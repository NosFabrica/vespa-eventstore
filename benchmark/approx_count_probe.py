#!/usr/bin/env python3
"""PROTOTYPE: cheaper observer-gated NIP-45 COUNTs, against today's exact gated count.

Today a gated count runs the gated profile over EVERY match (the gate is a ranking pass), so
it costs the whole match set: ~5-6 s for a global kind-1 count on the 44M-doc relay corpus.
Two alternatives, both answering from the cheap unranked side:

  BY AUTHOR (exact). A gated count IS the sum, over the authors the lens trusts, of each
  author's match count. One grouping query (`group(pubkey)`) over the UNGATED match set gives
  every author's count; the lens's trusted set (which TrustProjection sees every card write
  for, so it could hold per service in memory) picks which to add. No sampling, no error.

  SAMPLED (approximate, NIP-45 `"approximate": true`). The exact ungated count times a trust
  ratio measured in K random time windows of the match set's own span — each window a small
  exact gated count and its ungated count, run concurrently.

Read-only. The trusted set is built here by visiting the reputation documents (the store
would keep it); its build time is reported separately and excluded from per-count times.

    python3 benchmark/approx_count_probe.py --lens <hex> [--vespa http://127.0.0.1:8080] [--windows 24]
"""
import argparse, concurrent.futures as cf, json, random, statistics, time, urllib.parse, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--vespa", default="http://127.0.0.1:8080")
ap.add_argument("--lens", required=True)
ap.add_argument("--floor", type=float, default=2.0)
ap.add_argument("--now", type=int)
ap.add_argument("--windows", type=int, default=24, help="sampled windows per count")
ap.add_argument("--window-share", type=float, default=0.004, help="each window's width as a share of the match span")
ap.add_argument("--reps", type=int, default=3)
ap.add_argument("--seed", type=int, default=7)
a = ap.parse_args()
LENS = a.lens.lower()


def post(body, timeout=900):
    body = dict(body, timeout="600s")
    body.setdefault("ranking.matching.numThreadsPerSearch", "1")
    t = time.perf_counter()
    r = urllib.request.urlopen(urllib.request.Request(a.vespa + "/search/", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}), timeout=timeout)
    return json.load(r), (time.perf_counter() - t) * 1000


def grouping_count(where):
    d, ms = post({"yql": f"select * from event where {where} | all(output(count()))", "hits": 0, "ranking": "unranked"})
    ch = d["root"].get("children")
    return (ch[0]["fields"]["count()"] if ch else 0), ms


G = {"ranking.features.query(user_q)": "{%s:1.0}" % LENS, "ranking.features.query(min_rank)": str(a.floor)}


def gated_count(where):
    d, ms = post({"yql": f"select id from event where {where}", "hits": 1, "ranking": "recency_gated_exact", **G})
    return d["root"]["fields"]["totalCount"], ms


now = a.now or post({"yql": "select created_at from event where kind = 1 order by created_at desc limit 1", "hits": 1, "ranking": "unranked"})[0]["root"]["children"][0]["fields"]["created_at"]
groups = post({"yql": "select * from event where kind = 1 | all(group(pubkey) max(1000000) order(-count()) each(output(count())))", "hits": 0})[0]["root"]["children"][0]["children"][0]["children"]
authors = [g["value"] for g in groups]
quiet = [g["value"] for g in groups if 5 <= g["fields"]["count()"] < 60][:50]

# The lens's trusted set — what the store would hold (TrustProjection sees every card write).
t0, trusted, cont = time.perf_counter(), set(), None
while True:
    q = {"fieldSet": "reputation:pubkey,influence_scores", "wantedDocumentCount": "5000", "concurrency": "4", "timeout": "120s"}
    if cont:
        q["continuation"] = cont
    d = json.load(urllib.request.urlopen(a.vespa + "/document/v1/reputation/reputation/docid/?" + urllib.parse.urlencode(q), timeout=300))
    for doc in d.get("documents", []):
        f = doc.get("fields", {})
        cells = (f.get("influence_scores") or {}).get("cells") or {}
        score = cells.get(LENS) if isinstance(cells, dict) else next((c.get("value") for c in cells if c.get("address", {}).get("user") == LENS), None)
        if score is not None and score >= a.floor:
            trusted.add(f["pubkey"])
    cont = d.get("continuation")
    if not cont:
        break
print(f"trusted set for lens {LENS[:8]} at floor {a.floor}: {len(trusted)} authors, built in {time.perf_counter() - t0:.1f}s (the store would maintain it); clock {now}\n")


def hexin(xs):
    return "pubkey in (" + ", ".join(f'"{x}"' for x in xs) + ")"


def by_author(where):
    d, ms = post({"yql": f"select * from event where {where} | all(group(pubkey) max(1000000) each(output(count())))", "hits": 0, "ranking": "unranked"})
    ch = d["root"].get("children")
    groups = ch[0]["children"][0]["children"] if ch and ch[0].get("children") else []
    t = time.perf_counter()
    n = sum(g["fields"]["count()"] for g in groups if g["value"] in trusted)
    return n, ms + (time.perf_counter() - t) * 1000, len(groups)


pool = cf.ThreadPoolExecutor(16)


def sampled(where, rnd):
    t = time.perf_counter()
    u, _ = grouping_count(where)
    # The match set's time span. Grouped by kind and folded here: a ROOT-level min()/max()
    # throws a NullPointerException in Vespa 8.731's grouping (inside a group it works).
    d, _ = post({"yql": f"select * from event where {where} | all(group(kind) each(output(min(created_at), max(created_at))))", "hits": 0, "ranking": "unranked"})
    ch = d["root"].get("children")
    spans = [g["fields"] for g in ch[0]["children"][0]["children"]] if ch and ch[0].get("children") else []
    if not spans or u == 0:
        return 0, (time.perf_counter() - t) * 1000, 0.0
    lo, hi = int(min(s["min(created_at)"] for s in spans)), int(max(s["max(created_at)"] for s in spans))
    width = max(1, int((hi - lo) * a.window_share))
    starts = [rnd.randint(lo, max(lo, hi - width)) for _ in range(a.windows)]

    def window(s):
        w = f"{where} and created_at >= {s} and created_at < {s + width}"
        return gated_count(w)[0], grouping_count(w)[0]

    res = list(pool.map(window, starts))
    g, un = sum(r[0] for r in res), sum(r[1] for r in res)
    if un == 0:  # every window empty: fall back to the exact gated count
        return gated_count(where)[0], (time.perf_counter() - t) * 1000, 0.0
    return round(u * g / un), (time.perf_counter() - t) * 1000, un / u


shapes = [
    ("global kind 1", "kind in (1)"),
    ("global kinds 1/6/7", "kind in (1, 6, 7)"),
    ("follow 300 k1/6/7", f"kind in (1, 6, 7) and {hexin(authors[200:500])}"),
    ("follow 1000 k1/6/7", f"kind in (1, 6, 7) and {hexin(authors[500:1500])}"),
    ("50 quiet authors", f"kind in (1, 6, 7) and {hexin(quiet)}"),
    ("#t:bitcoin kind 1", 'kind in (1) and tag_index contains "t:bitcoin"'),
    ("#p notifications", f'kind in (1, 6, 7, 9735) and tag_index contains "p:{authors[3]}"'),
    ("kind 1, last 7 days", f"kind in (1) and created_at >= {now - 7 * 86400}"),
]
rnd = random.Random(a.seed)
print(f"{'shape':<22} {'EXACT gated (today)':>24} | {'BY AUTHOR (exact)':>30} | {'SAMPLED (approximate)':>40}")
for name, w in shapes:
    w = f"{w} and expires_at > {now}"
    ex = [gated_count(w) for _ in range(a.reps)]
    ba = [by_author(w) for _ in range(a.reps)]
    sa = [sampled(w, rnd) for _ in range(a.reps)]
    exact = ex[0][0]
    ba_ok = all(r[0] == exact for r in ba)
    errs = [abs(r[0] - exact) / exact * 100 if exact else 0.0 for r in sa]
    print(
        f"{name:<22} {exact:>10} {statistics.median(r[1] for r in ex):9.0f} ms | "
        f"{ba[0][0]:>10} {statistics.median(r[1] for r in ba):8.0f} ms {'=' if ba_ok else 'DIFF':>4} ({ba[0][2]} authors) | "
        f"{sa[0][0]:>10} {statistics.median(r[1] for r in sa):8.0f} ms  err {statistics.median(errs):5.2f}% (max {max(errs):5.2f}%, sampled {sa[0][2] * 100:4.1f}%)",
        flush=True,
    )
