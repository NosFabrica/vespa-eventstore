#!/usr/bin/env python3
"""What the topical-intent probe costs on the live relay: one search versus its probe.

    python3 benchmark/embedding/probe_latency.py benchmark/rank_cases_broad.json [benchmark/rank_cases.json ...]

READ ONLY: REQ only (AGENTS.md). For every case query, under the canonical observer, it
times two REQs to EOSE on one socket:

  search  `<q> observer:<hex>`, limit 50: what a client sends today;
  probe   the same words over kinds 0 and 31990 at `filter:rank:gte:30`, limit 20, which is
          store/search/QueryIntent's probe expressed as a NIP-50 REQ.

The order alternates from query to query so neither always meets a warm cache. These are
wall-clock times seen from outside, network included, so read the RATIO and the probe's
absolute p50/p95. Inside the store the probe is one engine search with no network hop and
no reference expansion, so it is cheaper than this measures, never dearer.
"""
import asyncio, json, ssl, statistics, sys, time
import websockets

RELAY = "wss://search.brainstorm.world/"
OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"


async def timed(ws, sub, filt, budget=60):
    t0 = time.perf_counter()
    await ws.send(json.dumps(["REQ", sub, filt]))
    n = 0
    while True:
        left = budget - (time.perf_counter() - t0)
        if left <= 0:
            break
        try:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=left))
        except asyncio.TimeoutError:
            break
        if m[1] != sub:
            continue
        if m[0] == "EVENT":
            n += 1
        elif m[0] in ("EOSE", "CLOSED"):
            break
    await ws.send(json.dumps(["CLOSE", sub]))
    return time.perf_counter() - t0, n


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p * len(v)))]


async def main():
    queries = []
    for path in sys.argv[1:]:
        for c in json.load(open(path)):
            q = c.get("query", "")
            if q and c.get("skip") != "true" and q not in queries:
                queries.append(q)
    rows = []
    async with websockets.connect(RELAY, ssl=ssl.create_default_context(), max_size=32 << 20, open_timeout=45) as ws:
        for i, q in enumerate(queries):
            search = {"search": f"{q} observer:{OBSERVER}", "limit": 50}
            probe = {"kinds": [0, 31990], "search": f"{q} observer:{OBSERVER} filter:rank:gte:30", "limit": 20}
            pair = [("s", search), ("p", probe)] if i % 2 == 0 else [("p", probe), ("s", search)]
            got = {}
            for tag, f in pair:
                got[tag] = await timed(ws, f"{tag}{i}", f)
            rows.append((q, got["s"][0], got["p"][0], got["s"][1], got["p"][1]))
            print(f"  {q[:28]:28s} search {got['s'][0]*1000:7.0f} ms ({got['s'][1]:3d})   probe {got['p'][0]*1000:7.0f} ms ({got['p'][1]:2d})", file=sys.stderr)
    s = [r[1] * 1000 for r in rows]
    p = [r[2] * 1000 for r in rows]
    ratio = [r[2] / r[1] for r in rows if r[1] > 0]
    print(f"queries {len(rows)}")
    print(f"search  p50 {pct(s, .5):6.0f} ms  p95 {pct(s, .95):6.0f} ms  mean {statistics.mean(s):6.0f} ms")
    print(f"probe   p50 {pct(p, .5):6.0f} ms  p95 {pct(p, .95):6.0f} ms  mean {statistics.mean(p):6.0f} ms")
    print(f"probe / search  median {statistics.median(ratio):.2f}   p95 {pct(ratio, .95):.2f}")


asyncio.run(main())
