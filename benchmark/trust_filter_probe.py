#!/usr/bin/env python3
"""PROTOTYPE A/B: the observer trust gate as a RANKING pass (today) vs as a MATCH filter.

Today a gated read runs the `recency_gated` / `recency_gated_exact` profile: every match is
ranked to read the author's trust cell off the imported reputation tensor, and a below-floor
author is dropped by rank-score-drop-limit. The prototype (schema in docs/trust-gate-filter.md)
adds `trusted_services` — the service keys under which the author clears the default floor —
to the reputation document and imports it into `event`, so the same gate is
`author_trusted_services contains "<lens>"` in the WHERE clause of an ordinary query.

For every shape this asserts the two return the IDENTICAL answer (count, or page ids in
NIP-01 order) and times both. Read-only; needs a Vespa carrying the prototype schema.

    python3 benchmark/trust_filter_probe.py --lens <hex> [--vespa http://127.0.0.1:8081] [--now <epoch>]
"""
import argparse, json, statistics, time, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--vespa", default="http://127.0.0.1:8081")
ap.add_argument("--lens", required=True, help="the key trust cells / trusted_services are stored under")
ap.add_argument("--floor", type=float, default=2.0)
ap.add_argument("--now", type=int)
ap.add_argument("--reps", type=int, default=5)
a = ap.parse_args()
# `documentid` rather than `id`: a copy fed from `vespa visit` may omit the event-id field,
# and the document id carries the same hex (id:event:event::<id>), so it orders identically.
F = "documentid, pubkey, created_at, kind"
SLACK = 64


def post(body):
    body = dict(body, timeout="600s")
    body["presentation.timing"] = True
    body.setdefault("ranking.matching.numThreadsPerSearch", "1")
    t = time.perf_counter()
    r = urllib.request.urlopen(urllib.request.Request(a.vespa + "/search/", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}), timeout=900)
    return json.load(r), (time.perf_counter() - t) * 1000


now = a.now or post({"yql": "select created_at from event where kind = 1 order by created_at desc limit 1", "hits": 1, "ranking": "unranked"})[0]["root"]["children"][0]["fields"]["created_at"]
groups = post({"yql": "select * from event where kind = 1 | all(group(pubkey) max(400000) order(-count()) each(output(count())))", "hits": 0})[0]["root"]["children"][0]["children"][0]["children"]
authors = [g["value"] for g in groups]
quiet = [g["value"] for g in groups if 5 <= g["fields"]["count()"] < 60][:50]
GATE = {"ranking.features.query(user_q)": "{%s:1.0}" % a.lens, "ranking.features.query(min_rank)": str(a.floor)}
FILTER = f'author_trusted_services contains "{a.lens}"'
exp = f"expires_at > {now}"


def hexin(xs):
    return "pubkey in (" + ", ".join(f'"{x}"' for x in xs) + ")"


def timed(body):
    runs = [post(body) for _ in range(a.reps)]
    return runs[0][0], statistics.median(r[1] for r in runs), statistics.median(r[0].get("timing", {}).get("querytime", 0) * 1000 for r in runs)


def page_ids(root, limit):
    hits = [c["fields"] for c in root.get("children", []) if c.get("fields", {}).get("documentid")]
    hits.sort(key=lambda f: (-f["created_at"], f["documentid"]))
    return [h["documentid"] for h in hits[:limit]]


shapes = [
    ("global kind 1", "kind in (1)"),
    ("global kinds 1/6/7", "kind in (1, 6, 7)"),
    ("follow 300 k1/6/7", f"kind in (1, 6, 7) and {hexin(authors[200:500])}"),
    ("follow 1000 k1/6/7", f"kind in (1, 6, 7) and {hexin(authors[500:1500])}"),
    ("50 quiet authors", f"kind in (1, 6, 7) and {hexin(quiet)}"),
    ("#t:bitcoin kind 1", 'kind in (1) and tag_index contains "t:bitcoin"'),
    ("#p notifications", f'kind in (1, 6, 7, 9735) and tag_index contains "p:{authors[3]}"'),
]
bad = 0
print(f"clock {now}; lens {a.lens[:8]}; floor {a.floor}; {a.reps} reps, medians (wall ms / engine ms)\n")

print("== COUNT: gated profile (exact, 1 hit)  vs  grouping count + filter   [ungated grouping for reference]")
for name, w in shapes:
    w = f"{w} and {exp}"
    g, gms, geng = timed({"yql": f"select id from event where {w}", "hits": 1, "ranking": "recency_gated_exact", **GATE})
    f, fms, feng = timed({"yql": f"select * from event where {w} and {FILTER} | all(output(count()))", "hits": 0, "ranking": "unranked"})
    u, ums, _ = timed({"yql": f"select * from event where {w} | all(output(count()))", "hits": 0, "ranking": "unranked"})
    gn = g["root"]["fields"]["totalCount"]
    fn = (f["root"].get("children") or [{"fields": {"count()": 0}}])[0]["fields"]["count()"]
    un = (u["root"].get("children") or [{"fields": {"count()": 0}}])[0]["fields"]["count()"]
    same = gn == fn
    bad += not same
    print(f"  {name:<22} gated {gn:>9} {gms:8.1f} / {geng:7.1f} | filter {fn:>9} {fms:8.1f} / {feng:7.1f} | x{gms / max(fms, 0.01):5.1f} | ungated {un:>9} {ums:7.1f} | {'same' if same else 'DIFF'}")

for label, window in (("PAGES (unwindowed, as shipped)", None), ("PAGES inside a 1-hour window (the speculative case)", 3600), ("DEEP PAGES: until = clock - 180 days", -180 * 86400)):
    print(f"\n== {label}: gated profile  vs  recency profile + filter")
    for name, w in shapes:
        for limit in (50, 500):
            ww = f"{w} and {exp}"
            if window and window > 0:
                ww += f" and created_at >= {now - window}"
            deep = window is not None and window < 0
            if deep:
                ww += f" and created_at <= {now + window}"
            gprof = "recency_gated_exact" if deep else "recency_gated"
            fprof = "unranked" if deep else "recency"
            order = " order by created_at desc"
            g, gms, geng = timed({"yql": f"select {F} from event where {ww} limit {limit + SLACK}", "hits": limit + SLACK, "ranking": gprof, **GATE})
            f, fms, feng = timed({"yql": f"select {F} from event where {ww} and {FILTER}{order} limit {limit + SLACK}", "hits": limit + SLACK, "ranking": fprof})
            gp, fp = page_ids(g["root"], limit), page_ids(f["root"], limit)
            gdeg = g["root"]["coverage"].get("degraded", {}).get("match-phase", False)
            fdeg = f["root"]["coverage"].get("degraded", {}).get("match-phase", False)
            # A match-phase cut is exact only on a FULL page; a short degraded one the client reruns.
            comparable = not ((gdeg and len(gp) < limit) or (fdeg and len(fp) < limit))
            if not gp and not fp:
                # Empty on both sides proves nothing either way: reported, never counted as a pass.
                print(f"  {name:<22} lim{limit:<4} empty on both sides — not compared")
                continue
            same = gp == fp
            bad += comparable and not same
            flag = "same" if same else ("DIFF" if comparable else "short-cut (client reruns)")
            print(f"  {name:<22} lim{limit:<4} gated {len(gp):>4} {gms:8.1f} / {geng:7.1f}{' MP' if gdeg else '   '} | filter {len(fp):>4} {fms:8.1f} / {feng:7.1f}{' MP' if fdeg else '   '} | x{gms / max(fms, 0.01):5.1f} | {flag}")

print("\n" + ("every comparable answer IDENTICAL" if bad == 0 else f"{bad} comparable answer(s) DIFFER"))
