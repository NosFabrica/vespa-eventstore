#!/usr/bin/env python3
"""Capture real search result pages AND every event that links to them, for links_probe.py.

    python3 benchmark/capture_links.py /tmp/links_capture.json [--per-term 100]
    VESPA_URL=http://localhost:8080 ./gradlew :benchmark:exportLoad --args=/tmp/links_capture.json
    python3 benchmark/links_probe.py --capture /tmp/links_capture.json

The question links_probe.py answers: what does it cost to count, for every hit on a
search page, the events that LINK to it (a reply, reaction, repost, zap receipt,
quote, comment — anything whose `e`/`E`/`q` tag names the hit, or whose `a`/`A` tag
names its address) written by authors the observer's web of trust clears? That is the
input a "how much did my network engage with this" ranking signal would read, and its
cost is set by the size of each hit's link set — which is why the link sets here are
captured COMPLETE, gated and ungated alike, rather than generated: only the real
relay knows the shape of a popular note's tail.

READ ONLY — REQ and COUNT only, never an EVENT (see capture_staging.py for why the
filters carry `include:spam` / `observer:`). Writes a plain JSON array of events:

  - the observer's kind 10040 and the provider's 30382 cards for EVERY author in the
    capture (result authors and linkers), so a local TrustProjection rebuilds the
    exact lens the live relay ranks with;
  - each term's search page, ranked by the live relay through that lens;
  - every event linking to a page hit, UNRANKED (the gate is what the probe measures,
    so the capture must not pre-apply it).

Beside it, `<out>.meta.json` records the pages (term -> ordered hit ids) and, per hit,
the live relay's own COUNTs of its links with the lens and without — the oracle the
probe's local answers are checked against, and a production-scale latency reference.
"""
import argparse, asyncio, json, ssl, sys, time
import websockets

RELAY = "wss://search.brainstorm.world/"
OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"
PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"
# Content searches — notes are what links point at. Profile-style queries from
# rank_cases.json are left out: a kind 0 is "linked" by `p` mentions, a different signal.
TERMS = ["bitcoin", "nostr", "lightning", "coffee", "amethyst", "primal", "esposa", "music", "gm", "zap", "freedom", "privacy"]
# The tag names that make an event a link to another: NIP-10 / NIP-18 / NIP-25 / NIP-57
# lowercase `e`, the quote tag `q`, NIP-22's uppercase root `E`; `a`/`A` for addressables.
ID_TAGS = ["e", "E", "q"]
ADDR_TAGS = ["a", "A"]

ap = argparse.ArgumentParser()
ap.add_argument("out")
ap.add_argument("--per-term", type=int, default=100)
ap.add_argument("--terms", default=",".join(TERMS))
ap.add_argument("--popular", type=int, default=100, help="size of the 'popular' page (0 skips it)")
ap.add_argument("--no-oracle", action="store_true", help="skip the per-hit live COUNTs (two round trips per hit)")
ap.add_argument("--kinds", default="1", help="the search pages' kinds, comma-separated, or 'all' (rank_cases.json's profile searches recall every kind)")
ap.add_argument("--no-links", action="store_true", help="capture the pages and the lens only — what an order that reads no links needs")
a = ap.parse_args()


class Relay:
    def __init__(self, ws):
        self.ws, self.n = ws, 0

    async def _rt(self, verb, filt, budget):
        self.n += 1
        sub = f"s{self.n}"
        await self.ws.send(json.dumps([verb, sub, *(filt if isinstance(filt, list) else [filt])]))
        out, t, deadline = [], time.perf_counter(), time.time() + budget
        while time.time() < deadline:
            m = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=deadline - time.time()))
            if len(m) < 2 or m[1] != sub:
                continue
            if m[0] == "EVENT":
                out.append(m[2])
            elif m[0] in ("EOSE", "COUNT", "CLOSED"):
                if m[0] == "CLOSED":
                    print(f"  CLOSED {filt}: {m[2:]}", file=sys.stderr)
                if m[0] != "CLOSED":
                    await self.ws.send(json.dumps(["CLOSE", sub]))
                return out, m, (time.perf_counter() - t) * 1000
        raise TimeoutError(f"{verb} {filt}")

    async def req(self, filt, budget=120):
        return (await self._rt("REQ", filt, budget))[0]

    async def req_all(self, filt, page=5000):
        """Every match, paged backwards with `until` (the relay caps a REQ at 5000)."""
        out, seen, until = [], set(), None
        while True:
            f = dict(filt, limit=page, **({"until": until} if until is not None else {}))
            batch = [e for e in await self.req(f) if e["id"] not in seen]
            for e in batch:
                seen.add(e["id"])
            out += batch
            if len(batch) < page // 2 or not batch:  # a short page is the end (ties at `until` re-serve a few)
                return out
            until = min(e["created_at"] for e in batch)

    async def count(self, filters):
        _, m, ms = await self._rt("COUNT", filters, 60)
        return (m[2].get("count") if m[0] == "COUNT" else None), ms


def address(e):
    d = next((t[1] for t in e["tags"] if len(t) > 1 and t[0] == "d"), "")
    return f"{e['kind']}:{e['pubkey']}:{d}"


def link_filters(hit):
    """The NIP-01 filters whose union is exactly `hit`'s link set."""
    fs = [{f"#{t}": [hit["id"]]} for t in ID_TAGS]
    if 30000 <= hit["kind"] < 40000:
        fs += [{f"#{t}": [address(hit)]} for t in ADDR_TAGS]
    return fs


async def main():
    corpus, meta = {}, {"observer": OBSERVER, "provider": PROVIDER, "relay": RELAY, "captured_at": int(time.time()), "pages": {}, "oracle": {}}

    def add(events):
        n = 0
        for e in events:
            if e["id"] not in corpus:
                corpus[e["id"]] = e
                n += 1
        return n

    async with websockets.connect(RELAY, ssl=ssl.create_default_context(), max_size=64 << 20, open_timeout=60) as ws:
        r = Relay(ws)
        hits = {}
        for term in a.terms.split(","):
            kinds = {} if a.kinds == "all" else {"kinds": [int(k) for k in a.kinds.split(",")]}
            page = (await r.req({**kinds, "search": f"{term} observer:{OBSERVER}", "limit": a.per_term}))[: a.per_term]
            meta["pages"][term] = [e["id"] for e in page]
            add(page)
            hits.update({e["id"]: e for e in page})
            print(f"  page '{term}': {len(page)} hits", file=sys.stderr)

        # A search page is newest-leaning (recency_mult), so its hits are mostly young and
        # lightly linked. The cost question lives in the TAIL — a note with thousands of
        # reactions — so one more page holds the most-reacted notes of the recent window:
        # the targets of the newest 5000 reactions, by how many of those name them.
        if a.popular:
            tally = {}
            for e in await r.req({"kinds": [7], "search": "include:spam", "limit": 5000}):
                target = next((t[1] for t in reversed(e["tags"]) if len(t) > 1 and t[0] == "e"), None)
                if target and len(target) == 64:
                    tally[target] = tally.get(target, 0) + 1
            top = [i for i, _ in sorted(tally.items(), key=lambda kv: -kv[1])[: a.popular * 2]]
            page = []
            for i in range(0, len(top), 100):
                page += await r.req({"ids": top[i : i + 100], "search": "include:spam"})
            order = {i: n for n, i in enumerate(top)}
            page = sorted((e for e in page if e["kind"] == 1), key=lambda e: order[e["id"]])[: a.popular]
            meta["pages"]["popular"] = [e["id"] for e in page]
            add(page)
            hits.update({e["id"]: e for e in page})
            print(f"  page 'popular': {len(page)} hits", file=sys.stderr)

        # Every link, ungated. Chunked so one REQ never names more ids than a relay will take.
        hit_list = list(hits.values())
        for i in range(0, 0 if a.no_links else len(hit_list), 50):
            chunk = hit_list[i : i + 50]
            for t in ID_TAGS:
                got = await r.req_all({f"#{t}": [h["id"] for h in chunk], "search": "include:spam"})
                print(f"  links #{t} for hits {i}..{i + len(chunk)}: +{add(got)}", file=sys.stderr)
            addrs = [address(h) for h in chunk if 30000 <= h["kind"] < 40000]
            for t in ADDR_TAGS if addrs else []:
                print(f"  links #{t}: +{add(await r.req_all({f'#{t}': addrs, 'search': 'include:spam'}))}", file=sys.stderr)

        # The live relay's own answer per hit, gated and ungated — the oracle. All of a hit's
        # link filters ride ONE COUNT, which NIP-45 answers as their union, so an event
        # linking through two tags is counted once — the same set the probe counts.
        for i, h in enumerate([] if a.no_oracle else hit_list):
            fs = link_filters(h)
            gated, gated_ms = await r.count([dict(f, search=f"observer:{OBSERVER}") for f in fs])
            every, _ = await r.count([dict(f, search="include:spam") for f in fs])
            meta["oracle"][h["id"]] = {"gated": gated, "all": every, "gated_ms": round(gated_ms, 1)}
            if i % 200 == 0:
                print(f"  oracle {i}/{len(hit_list)}", file=sys.stderr)

        # The lens: the observer's 10040, then the provider's card for every PERSON present —
        # every author, and every zapper a receipt's `P` names: scoreLinks weighs a zap by
        # its zapper's trust, and a zapper who wrote nothing in the capture would otherwise
        # read as unranked and be gated out.
        add(await r.req({"kinds": [10040], "authors": [OBSERVER], "search": "include:spam"}))
        zappers = {t[1] for e in corpus.values() if e["kind"] == 9735 for t in e["tags"] if len(t) > 1 and t[0] == "P" and len(t[1]) == 64}
        authors = sorted({e["pubkey"] for e in corpus.values()} | zappers)
        cards = 0
        for i in range(0, len(authors), 500):
            cards += add(await r.req({"kinds": [30382], "authors": [PROVIDER], "#d": authors[i : i + 500], "limit": 5000, "search": "include:spam"}))
        print(f"  {cards} score cards for {len(authors)} authors", file=sys.stderr)

    events = list(corpus.values())
    with open(a.out, "w") as f:
        json.dump(events, f)
    with open(a.out + ".meta.json", "w") as f:
        json.dump(meta, f)
    kinds = {}
    for e in events:
        kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
    print(f"\nwrote {len(events)} events to {a.out} (+ .meta.json); kinds: {dict(sorted(kinds.items()))}", file=sys.stderr)


asyncio.run(main())
