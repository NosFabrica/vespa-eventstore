#!/usr/bin/env python3
"""What an observer-gated NIP-45 COUNT costs — and what it would cost with the trust gate as a MATCH FILTER.

A gated count ranks every match to read the author's trust (the gated profile's
rank-score-drop-limit is the gate), so its cost is the full match set. The
prototype asks the cheap unranked grouping count instead, with the gate written as
`pubkey in (every author the observer trusts at the floor)` — the trusted set read
straight off the reputation documents. Its count must EQUAL the gated one; its cost is
an UPPER bound for a real design (an imported per-service "trusted" attribute would not
pay the inline set's per-term lookups, ~300-450 ms at 148k authors).

Read-only. Against a schema that keys trust cells by OBSERVER (pre service-keyed), pass
the observer as --lens; on a current schema pass the 10040's service key.

    python3 benchmark/gated_count_probe.py --observer <hex> [--lens <hex>] [--now <epoch>] [--vespa http://127.0.0.1:8080]
"""
import argparse, json, time, urllib.parse, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--vespa", default="http://127.0.0.1:8080")
ap.add_argument("--observer", required=True)
ap.add_argument("--lens", help="the key trust cells are stored under (default: --observer)")
ap.add_argument("--floor", type=float, default=2.0)
ap.add_argument("--now", type=int, help="the request clock (default: the newest kind-1 note)")
ap.add_argument("--reps", type=int, default=3)
a = ap.parse_args()
LENS = (a.lens or a.observer).lower()


def post(body):
    body = dict(body, timeout="600s")
    body["presentation.timing"] = True
    t = time.perf_counter()
    r = urllib.request.urlopen(urllib.request.Request(a.vespa + "/search/", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}), timeout=900)
    return json.load(r), (time.perf_counter() - t) * 1000


now = a.now or post({"yql": "select created_at from event where kind = 1 order by created_at desc limit 1", "hits": 1, "ranking": "unranked"})[0]["root"]["children"][0]["fields"]["created_at"]
authors = [g["value"] for g in post({"yql": "select * from event where kind = 1 | all(group(pubkey) max(2000) order(-count()) each(output(count())))", "hits": 0})[0]["root"]["children"][0]["children"][0]["children"]]
quiet = [g["value"] for g in post({"yql": "select * from event where kind = 1 | all(group(pubkey) max(400000) each(output(count())))", "hits": 0})[0]["root"]["children"][0]["children"][0]["children"] if 5 <= g["fields"]["count()"] < 60][:50]

# The trusted set, off the reputation documents themselves.
t0, trusted, seen, cont = time.perf_counter(), set(), 0, None
while True:
    q = {"fieldSet": "reputation:pubkey,influence_scores", "wantedDocumentCount": "5000", "concurrency": "4", "timeout": "120s"}
    if cont:
        q["continuation"] = cont
    d = json.load(urllib.request.urlopen(a.vespa + "/document/v1/reputation/reputation/docid/?" + urllib.parse.urlencode(q), timeout=300))
    for doc in d.get("documents", []):
        seen += 1
        f = doc.get("fields", {})
        cells = f.get("influence_scores")
        score = None
        if isinstance(cells, dict):
            c = cells.get("cells", cells.get("blocks", cells))
            if isinstance(c, dict):
                score = c.get(LENS)
            elif isinstance(c, list):
                score = next((cell.get("value") for cell in c if cell.get("address", {}).get("user") == LENS), None)
        if score is not None and score >= a.floor:
            trusted.add(f["pubkey"])
    cont = d.get("continuation")
    if not cont:
        break
print(f"reputation docs {seen}; trusted at floor {a.floor}: {len(trusted)} ({time.perf_counter() - t0:.0f}s); clock {now}")
trusted_in = "pubkey in (" + ", ".join(f'"{p}"' for p in sorted(trusted)) + ")"


def hexin(xs):
    return "pubkey in (" + ", ".join(f'"{x}"' for x in xs) + ")"


shapes = [
    ("global kind 1", "kind in (1)"),
    ("global kinds 1/6/7", "kind in (1, 6, 7)"),
    ("follow 300 k1/6/7", f"kind in (1, 6, 7) and {hexin(authors[200:500])}"),
    ("follow 1000 k1/6/7", f"kind in (1, 6, 7) and {hexin(authors[500:1500])}"),
    ("50 quiet authors", f"kind in (1, 6, 7) and {hexin(quiet)}"),
    ("#t:bitcoin kind 1", 'kind in (1) and tag_index contains "t:bitcoin"'),
    ("#p notifications", f'kind in (1, 6, 7, 9735) and tag_index contains "p:{a.observer}"'),
    ("kind 1, last 7 days", f"kind in (1) and created_at >= {now - 7 * 86400}"),
]
G = {"ranking.features.query(user_q)": "{%s:1.0}" % LENS, "ranking.features.query(min_rank)": str(a.floor), "ranking.matching.numThreadsPerSearch": "1"}


def med(xs):
    return sorted(xs)[len(xs) // 2]


print(f"{'shape':<22} {'ungated count':>20} | {'GATED (exact profile, 1 hit)':>30} | {'gate as filter (prototype)':>30} | same")
for name, w in shapes:
    w = f"{w} and expires_at > {now}"
    un = [post({"yql": f"select * from event where {w} | all(output(count()))", "hits": 0, "ranking": "unranked"}) for _ in range(a.reps)]
    ga = [post({"yql": f"select id from event where {w}", "hits": 1, "ranking": "recency_gated_exact", **G}) for _ in range(a.reps)]
    pr = [post({"yql": f"select * from event where {w} and {trusted_in} | all(output(count()))", "hits": 0, "ranking": "unranked"}) for _ in range(a.reps)]
    un_n = un[0][0]["root"]["children"][0]["fields"]["count()"]
    ga_n = ga[0][0]["root"]["fields"]["totalCount"]
    pr_n = (pr[0][0]["root"].get("children") or [{"fields": {"count()": 0}}])[0]["fields"]["count()"]
    ms = lambda runs: med([r[1] for r in runs])
    print(f"{name:<22} {un_n:>9} {ms(un):7.0f} ms | {ga_n:>9} {ms(ga):7.0f} ms           | {pr_n:>9} {ms(pr):7.0f} ms           | {'yes' if pr_n == ga_n else 'NO'}", flush=True)
