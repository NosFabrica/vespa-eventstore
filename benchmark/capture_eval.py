#!/usr/bin/env python3
"""Capture the search-evaluation dataset from the live relay (search.brainstorm.world).

    python3 benchmark/capture_eval.py benchmark/rank_cases.json /tmp/eval
    VESPA_URL=http://localhost:8080 ./gradlew :benchmark:exportLoad --args="/tmp/eval/corpus.json"
    ./gradlew :benchmark:rankAb --args="--observer 460c25…065c --run production=/tmp/eval/run_production.json \
        --configs production,baseline --pool /tmp/eval/pool.json"

READ ONLY: REQ only, never an EVENT (CLAUDE.md). For every topic in the case
file it pulls, under the canonical observer's lens:

  - production's ranked page (`<query> observer:<hex>`, limit 150), kept IN
    SERVED ORDER as `run_production.json`. That is production's ranking as a
    `rankAb --run`, scored against the same judgments as a local config, which
    is how "is this branch better than what the relay serves?" gets a paired
    answer (docs/search-evaluation.md §8.5);
  - an unranked slice (`<query> include:spam`, limit 60), so the local corpus
    holds candidates the lens hides, and a config that surfaces them can be
    caught doing so;
  - the observer's kind 10040 and the provider's 30382s for every author
    seen, so the local TrustProjection rebuilds the same lens.

All of it goes to `corpus.json`, a plain JSON array of events. Neither file is
committed: a capture runs to megabytes and is re-pullable, and the judgments
are keyed by event id, so they outlive any one capture.

The local cluster then RE-RANKS production's candidates. A local config can
only surface what the capture holds, so this compares orderings over the same
pool, not recall over 200M events. That is the right question for a ranking
change and the wrong one for a recall change, which needs a bigger slice.
"""
import asyncio, json, ssl, sys, time
import websockets

RELAY = "wss://search.brainstorm.world/"
OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"
PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"
PAGE, UNRANKED = 150, 60


async def req(ws, sub, filt, budget=90):
    """One REQ, collected in arrival order until EOSE or budget."""
    await ws.send(json.dumps(["REQ", sub, filt]))
    out, deadline = [], time.time() + budget
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
        except asyncio.TimeoutError:
            print(f"  TIMEOUT {sub}", file=sys.stderr)
            break
        m = json.loads(raw)
        if m[0] == "EVENT" and m[1] == sub:
            out.append(m[2])
        elif m[0] == "EOSE" and m[1] == sub:
            break
        elif m[0] == "CLOSED" and m[1] == sub:
            print(f"  CLOSED {sub}: {m[2:]}", file=sys.stderr)
            break
    await ws.send(json.dumps(["CLOSE", sub]))
    return out


async def main():
    cases, outdir = sys.argv[1], sys.argv[2]
    # Topic key = the case's `id`, else its query: the same key rankAb and the
    # judgments use. Per-case `observer`/`kinds` overrides are not captured
    # here (no case sets them yet); extend this when one does.
    topics = []
    for c in json.load(open(cases)):
        if c.get("skip") == "true":
            continue
        key = c.get("id") or c["query"]
        if key not in [t[0] for t in topics]:
            topics.append((key, c["query"]))
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    async with websockets.connect(RELAY, ssl=ssl.create_default_context(), max_size=32 << 20, open_timeout=45) as ws:
        corpus, seen = [], set()
        run = {"_doc": f"production's served order per topic, observer:{OBSERVER}, captured {stamp} from {RELAY}"}

        def add(events, label):
            fresh = 0
            for e in events:
                if e["id"] not in seen:
                    seen.add(e["id"])
                    corpus.append(e)
                    fresh += 1
            print(f"  {label}: {len(events)} got, +{fresh} (total {len(corpus)})", file=sys.stderr)

        add(await req(ws, "l", {"kinds": [10040], "authors": [OBSERVER], "search": "include:spam"}), "10040")
        for i, (key, q) in enumerate(topics):
            page = await req(ws, f"q{i}", {"search": f"{q} observer:{OBSERVER}", "limit": PAGE})
            run[key] = page
            add(page, f"{q!r} lensed")
            add(await req(ws, f"p{i}", {"search": f"{q} include:spam", "limit": UNRANKED}), f"{q!r} unranked")
        authors = sorted({e["pubkey"] for e in corpus})
        for i in range(0, len(authors), 400):
            cards = {"kinds": [30382], "authors": [PROVIDER], "#d": authors[i:i + 400], "search": "include:spam", "limit": 400}
            add(await req(ws, f"t{i}", cards), f"30382 cards {i}")
        with open(f"{outdir}/corpus.json", "w") as f:
            json.dump(corpus, f)
        with open(f"{outdir}/run_production.json", "w") as f:
            json.dump(run, f)
        print(f"wrote {len(corpus)} events ({len(authors)} authors) and {len(topics)} production pages to {outdir}", file=sys.stderr)


asyncio.run(main())
