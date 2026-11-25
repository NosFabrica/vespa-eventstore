#!/usr/bin/env python3
"""The topical-intent probe's decision for every case query, ON PRODUCTION.

    python3 benchmark/embedding/probe_decisions.py out.json benchmark/rank_cases.json benchmark/rank_cases_broad.json

READ ONLY: REQ only (AGENTS.md). An evaluation corpus holds only what its capture pulled, so
the accounts production holds that are named after a common word ("Coffee", "art") are missing
from it, and a probe run locally reads those topics as topical when production would not
(docs/embedding-experiment.md §10.2). This runs store/search/QueryIntent's probe against the
live relay instead, in its NIP-50 spelling:

    kinds [0, 31990], search `"<words>" observer:<hex> filter:rank:gte:30`, limit 50

and applies its rule: navigational when an app handler (31990), or a profile with at least
1 000 followers on the provider's card, is named exactly the query (EntityNames.isExactly,
mirrored below). `rankAb --intent-file out.json` with `_intent` 3 then ranks every topic
under production's decision.
"""
import asyncio, json, re, ssl, sys, unicodedata
import websockets

RELAY = "wss://search.brainstorm.world/"
OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"
PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"
FOLLOWERS = 1000


def fold(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s) if unicodedata.category(c) != "Mn").lower()


def tokens(s):
    return [t for t in re.split(r"[^\w]+|_", fold(s)) if t]


async def req(ws, sub, f):
    await ws.send(json.dumps(["REQ", sub, f]))
    out = []
    while True:
        try:
            m = json.loads(await asyncio.wait_for(ws.recv(), 90))
        except asyncio.TimeoutError:
            break
        if m[1] != sub:
            continue
        if m[0] == "EVENT":
            out.append(m[2])
        elif m[0] in ("EOSE", "CLOSED"):
            break
    await ws.send(json.dumps(["CLOSE", sub]))
    return out


async def main():
    out_path, case_paths = sys.argv[1], sys.argv[2:]
    cases = [c for p in case_paths for c in json.load(open(p)) if c.get("query") and c.get("skip") != "true"]
    found = {}
    async with websockets.connect(RELAY, ssl=ssl.create_default_context(), max_size=1 << 26) as ws:
        for i, c in enumerate(cases):
            key = c.get("id") or c["query"]
            words = " ".join(w for w in c["query"].replace('"', " ").split() if ":" not in w)
            if c.get("kinds") and all(k == 0 for k in c["kinds"]):
                found[key] = {"query": c["query"], "intent": c.get("intent"), "named": [], "profiles_only": True}
                continue
            evs = await req(ws, f"p{i}", {"kinds": [0, 31990], "search": f'"{words}" observer:{OBSERVER} filter:rank:gte:30', "limit": 50})
            named = []
            for e in evs:
                try:
                    m = json.loads(e["content"])
                except Exception:
                    continue
                if not isinstance(m, dict):
                    continue
                for k in ("name", "display_name", "displayName"):
                    v = m.get(k)
                    if isinstance(v, str) and tokens(words) and tokens(v) == tokens(words):
                        named.append({"kind": e["kind"], "pubkey": e["pubkey"], "name": v})
                        break
            found[key] = {"query": c["query"], "intent": c.get("intent"), "named": named}
        keys = sorted({n["pubkey"] for v in found.values() for n in v["named"] if n["kind"] == 0})
        followers = {}
        for j in range(0, len(keys), 300):
            for e in await req(ws, f"c{j}", {"kinds": [30382], "authors": [PROVIDER], "#d": keys[j:j + 300], "search": "include:spam", "limit": 300}):
                d = next((t[1] for t in e["tags"] if t[0] == "d"), None)
                f = next((t[1] for t in e["tags"] if t[0] == "followers"), None)
                if d and f:
                    followers[d] = float(f)
    decisions = {}
    for key, v in found.items():
        for n in v["named"]:
            n["followers"] = followers.get(n["pubkey"]) if n["kind"] == 0 else None
        notable = [n for n in v["named"] if n["kind"] == 31990 or (n["followers"] or 0) >= FOLLOWERS]
        decisions[key] = {**v, "topical": 0.0 if (v.get("profiles_only") or notable) else 1.0, "notable": notable}
    json.dump(decisions, open(out_path, "w"), ensure_ascii=False, indent=1)
    for label in ("topical", "navigational"):
        rows = [d for d in decisions.values() if d["intent"] == label]
        print(f"{label}: {len(rows)} queries, probe says topical on {sum(1 for d in rows if d['topical'] == 1.0)}")
    print("topical-labelled read navigational:", [(d["query"], [(n["name"], n["followers"]) for n in d["notable"]]) for d in decisions.values() if d["intent"] == "topical" and d["topical"] == 0.0])


asyncio.run(main())
