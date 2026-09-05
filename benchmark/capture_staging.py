#!/usr/bin/env python3
"""Capture a real corpus from the staging relay, for StagingCorpusIT.

    python3 benchmark/capture_staging.py /tmp/staging_corpus.json
    STAGING_CORPUS=/tmp/staging_corpus.json ./gradlew :benchmark:test -Pintegration --tests '*StagingCorpusIT*'

READ ONLY — REQ and COUNT only, never an EVENT. Writes a plain JSON array of
events, which is all a fixture needs to be (see
benchmark/src/test/resources/search_vitor_pamplona_export.json). The output is
NOT committed: a useful capture runs to megabytes, staging deploys on its own
cadence, and the whole point of the relay is that you can re-pull it.

WHY THE ODD-LOOKING FILTERS. The relay gates every read through a web of trust
and has no house observer to lend, so a plain filter is REFUSED with a CLOSED
naming the three ways through: NIP-42, a NIP-50 `observer:<64-hex>` token, or
`include:spam` for the corpus unranked. The observer pubkey below is public
(CLAUDE.md), so the lens is simulated with the token rather than authenticated —
which is also what makes the captured 10040 + 30382 set worth having: fed into a
local Vespa they rebuild the same reputation tensors, and the same query ranks
the same way.
"""
import asyncio, json, ssl, sys, time
import websockets

RELAY = "wss://search-staging.brainstorm.world/"
OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"
PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"

async def req(ws, sub, filt, budget=45):
    """One REQ, collected until EOSE or budget. Returns the events."""
    await ws.send(json.dumps(["REQ", sub, filt]))
    out, deadline = [], time.time() + budget
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
        except asyncio.TimeoutError:
            break
        m = json.loads(raw)
        if m[0] == "EVENT" and m[1] == sub:
            out.append(m[2])
        elif m[0] == "EOSE" and m[1] == sub:
            break
        elif m[0] == "CLOSED" and m[1] == sub:
            print(f"  CLOSED {sub}: {m[2:]}", file=sys.stderr); break
    await ws.send(json.dumps(["CLOSE", sub]))
    return out

async def count(ws, sub, filt, budget=30):
    await ws.send(json.dumps(["COUNT", sub, filt]))
    deadline = time.time() + budget
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
        except asyncio.TimeoutError:
            return None
        m = json.loads(raw)
        if m[0] == "COUNT" and m[1] == sub:
            await ws.send(json.dumps(["CLOSE", sub]))
            return m[2].get("count")
    return None

async def main():
    ctx = ssl.create_default_context()
    async with websockets.connect(RELAY, ssl=ctx, max_size=8 * 1024 * 1024, open_timeout=45) as ws:
        corpus, seen = [], set()

        def add(events, label):
            fresh = 0
            for e in events:
                if e["id"] not in seen:
                    seen.add(e["id"]); corpus.append(e); fresh += 1
            print(f"  {label}: +{fresh} (total {len(corpus)})", file=sys.stderr)

        # Size the corpus first — COUNT is exact here and is the cheapest way.
        total = await count(ws, "c1", {"kinds": [1], "search": "include:spam"})
        print(f"  staging holds {total:,} kind-1 events" if total else "  COUNT unavailable", file=sys.stderr)

        # 1. The trust lens: the observer's kind 10040, then the provider's
        #    score cards. Without these every ranked search comes back empty.
        # `include:spam` asks for the corpus UNRANKED — the relay gates every
        # read through a web of trust and has no house observer to lend, so a
        # plain filter is refused (its CLOSED message says exactly this).
        add(await req(ws, "s10040", {"kinds": [10040], "authors": [OBSERVER], "search": "include:spam"}), "10040 provider list")
        add(await req(ws, "s30382", {"kinds": [30382], "authors": [PROVIDER], "limit": 2000, "search": "include:spam"}, budget=90), "30382 score cards")

        # 2. Real notes across several search terms, so a ranked search has
        #    something to rank and the corpus is not one topic.
        for term in ["bitcoin", "nostr", "lightning", "zap", "relay"]:
            # The observer token is how you name whose trust ranks the read
            # without holding their secret key (CLAUDE.md: the observer pubkey
            # is public, so simulate the lens rather than NIP-42).
            add(await req(ws, f"s_{term}", {"kinds": [1], "search": f"{term} observer:{OBSERVER}", "limit": 400}, budget=60), f"kind 1 '{term}'")

        # 3. Profiles, so kind-0 search fields are exercised too.
        add(await req(ws, "s0", {"kinds": [0], "search": f"nostr observer:{OBSERVER}", "limit": 300}, budget=60), "kind 0 profiles")

        # 4. A plain recency page — the shape a mirror pages with, and the one
        #    that carries the corpus's real dirt (CLAUDE.md warns about
        #    created_at in the year 2100).
        add(await req(ws, "srec", {"kinds": [1], "limit": 600, "search": "include:spam"}, budget=60), "kind 1 recent")

        out = sys.argv[1]
        with open(out, "w") as f:
            json.dump(corpus, f)
        kinds = {}
        for e in corpus:
            kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
        print(f"\nwrote {len(corpus)} events to {out}", file=sys.stderr)
        print(f"kinds: {dict(sorted(kinds.items()))}", file=sys.stderr)

asyncio.run(main())
