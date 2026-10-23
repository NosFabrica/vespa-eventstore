#!/usr/bin/env python3
"""PROTOTYPE: count, for every hit on a search page, the links to it from the observer's web of trust.

The ranking idea this measures: today a search hit scores text x the AUTHOR's trust. The
alternative reads how much the observer's network ENGAGED with the hit — the events that
link to it (`e`/`E`/`q` naming its id: replies, reactions, reposts, zap receipts, quotes,
NIP-22 comments) whose author clears the observer's trust floor. Before that can be a
ranking input, it has to be cheap enough to compute for a whole result page per search.
Four ways to get the numbers, each timed per PAGE and checked against the others and
against the live relay's own gated COUNTs (captured by capture_links.py):

  PER_HIT      one gated count per hit — the store's NIP-45 COUNT shape (recency_gated_exact,
               one hit so totalCount is the post-gate number), issued concurrently.
  GROUPED      ONE gated query over every hit's link keys at once
               (`tag_index in ("e:<h1>", "q:<h1>", "E:<h1>", ...)`), grouped by tag_index:
               each page key's group count is that hit's trusted links. Relies on grouping
               seeing only hits that survived the gate's rank-score-drop-limit — asserted
               here against PER_HIT, not assumed. The grouping carries a `filter(regex())`
               naming the page's keys: without it, group(tag_index) builds a group for EVERY
               tag of every link (a reply's `p` list, its thread root, ...) — 6,137 groups for
               the 26 that matter on the heaviest page, 117 ms against 17 ms with the filter.
               NOT EXACT: grouping an array attribute counts ELEMENTS, so a link naming the hit
               twice (NIP-22's `E`+`e` on a top-level comment, `e`+`q`, a duplicated `e`) is
               counted twice. The probe reports the overcount.
  GROUPED_KIND the same, each group split by kind — the reaction / reply / repost / zap mix a
               ranking would actually weigh.
  BY_AUTHOR    one UNGATED query grouped by tag_index then pubkey; the trusted set (what
               TrustProjection could hold in memory, docs/gated-count.md) picks which authors
               to add. Exact, and skips ranking entirely.
  LOAD         the link documents themselves, gated (attribute-only `idtimetag` summary), counted
               client-side — what "load all the links" costs when the documents, not just the
               numbers, are wanted.

Plus UNGATED_GROUPED (GROUPED on `unranked`) as the floor: what the gate itself costs.

Read-only against whatever Vespa holds the capture. Median of --reps per page, single
match thread (as the store's own aggregations run).

    python3 benchmark/links_probe.py --capture /tmp/links_capture.json [--vespa http://127.0.0.1:8080]
"""
import argparse, concurrent.futures as cf, http.client, json, statistics, threading, time, urllib.parse

ap = argparse.ArgumentParser()
ap.add_argument("--vespa", default="http://127.0.0.1:8080")
ap.add_argument("--capture", required=True, help="the events file capture_links.py wrote (its .meta.json beside it)")
ap.add_argument("--floor", type=float, default=2.0, help="the trust floor (store's DEFAULT_MIN_RANK)")
ap.add_argument("--sizes", default="20,50,100", help="page sizes: the top-N of each captured page")
ap.add_argument("--reps", type=int, default=5)
ap.add_argument("--concurrency", type=int, default=16, help="PER_HIT fan-out width")
a = ap.parse_args()

meta = json.load(open(a.capture + ".meta.json"))
events = {e["id"]: e for e in json.load(open(a.capture))}
# Cells are keyed by the SERVICE that signed the 30382 (docs/service-keyed-trust.md): the
# observer's 10040 names one provider, so the lens is that provider's key.
LENS = meta["provider"]
ID_TAGS = ["e", "E", "q"]
url = urllib.parse.urlparse(a.vespa)
local = threading.local()


def post(body):
    """POST /search/ on a per-thread keep-alive connection: a fresh TCP handshake per
    query would be charged to PER_HIT n times and to the one-query shapes once."""
    body = dict(body, timeout="600s")
    body.setdefault("ranking.matching.numThreadsPerSearch", "1")
    body.setdefault("grouping.defaultMaxGroups", "-1")
    body.setdefault("grouping.defaultMaxHits", "-1")
    if not hasattr(local, "c"):
        local.c = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=900)
    t = time.perf_counter()
    local.c.request("POST", "/search/", json.dumps(body), {"Content-Type": "application/json"})
    r = local.c.getresponse()
    d = json.loads(r.read())
    ms = (time.perf_counter() - t) * 1000
    if r.status != 200 or "errors" in d["root"]:
        raise RuntimeError(f"{r.status}: {d['root'].get('errors')}\n{body['yql']}")
    if not d["root"].get("coverage", {}).get("full", True):
        raise RuntimeError(f"degraded coverage: {d['root']['coverage']}")
    return d, ms


GATE = {"ranking": "recency_gated_exact", "ranking.features.query(user_q)": "{%s:1.0}" % LENS, "ranking.features.query(min_rank)": str(a.floor)}


def page_filter(hits):
    """Keep only the page's own keys as groups (see GROUPED above). Hex ids need no escaping."""
    return 'filter(regex("^[' + "".join(ID_TAGS) + ']:(' + "|".join(hits) + ')$", tag_index))'


def keys(hit):
    return [f"{t}:{hit}" for t in ID_TAGS]


def where(hits):
    return "tag_index in (" + ", ".join(f'"{k}"' for h in hits for k in keys(h)) + ")"


def groups_of(d):
    ch = d["root"].get("children")
    return ch[0]["children"][0].get("children", []) if ch and ch[0].get("children") else []


def fold(counts_by_key, hits):
    """Per-hit totals from per-key counts. An event naming a hit through TWO tag names
    (`e` and `q`) is in both groups — the overlap PER_HIT's single OR clause cannot
    have; the oracle comparison below reports whether it ever happens."""
    return {h: sum(counts_by_key.get(k, 0) for k in keys(h)) for h in hits}


def per_hit(hits, pool):
    def one(h):
        d, _ = post({"yql": f"select id from event where {where([h])}", "hits": 1, **GATE})
        return h, d["root"]["fields"]["totalCount"]

    return dict(pool.map(one, hits))


def grouped(hits, gated=True, by_kind=False):
    inner = "each(output(count()) all(group(kind) each(output(count()))))" if by_kind else "each(output(count()))"
    body = {"yql": f"select * from event where {where(hits)} limit 0 | all(group(tag_index) {page_filter(hits)} {inner})", "hits": 0}
    body.update(GATE if gated else {"ranking": "unranked"})
    d, _ = post(body)
    wanted = {k for h in hits for k in keys(h)}
    by_key = {g["value"]: g["fields"]["count()"] for g in groups_of(d) if g["value"] in wanted}
    if by_kind:
        mix = {}
        for g in groups_of(d):
            if g["value"] in wanted:
                for kg in (g.get("children") or [{}])[0].get("children", []):
                    mix[int(kg["value"])] = mix.get(int(kg["value"]), 0) + kg["fields"]["count()"]
        return fold(by_key, hits), mix
    return fold(by_key, hits)


def by_author(hits, trusted):
    yql = f"select * from event where {where(hits)} limit 0 | all(group(tag_index) {page_filter(hits)} each(group(pubkey) each(output(count()))))"
    d, _ = post({"yql": yql, "hits": 0, "ranking": "unranked"})
    wanted = {k for h in hits for k in keys(h)}
    by_key = {}
    for g in groups_of(d):
        if g["value"] in wanted:
            authors = (g.get("children") or [{}])[0].get("children", [])
            by_key[g["value"]] = sum(ag["fields"]["count()"] for ag in authors if ag["value"] in trusted)
    return fold(by_key, hits)


def load(hits):
    d, _ = post({"yql": f"select * from event where {where(hits)}", "hits": 1_000_000, "summary": "idtimetag", **GATE})
    page = set(hits)
    out = {h: 0 for h in hits}
    for c in d["root"].get("children", []):
        named = {v.split(":", 1)[1] for v in c["fields"].get("tag_index", []) if v[:2] in ("e:", "E:", "q:")} & page
        for h in named:  # an event counts once per hit it links to, however many tags name it
            out[h] += 1
    return out, len(d["root"].get("children", []))


def trusted_set():
    """The lens's trusted authors at the floor, visited off the reputation documents —
    the set docs/gated-count.md has TrustProjection keep in memory; built once here."""
    out, cont, t = set(), None, time.perf_counter()
    while True:
        q = {"fieldSet": "reputation:pubkey,influence_scores", "wantedDocumentCount": "5000", "timeout": "120s"}
        if cont:
            q["continuation"] = cont
        c = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=300)
        c.request("GET", "/document/v1/reputation/reputation/docid/?" + urllib.parse.urlencode(q))
        d = json.loads(c.getresponse().read())
        for doc in d.get("documents", []):
            f = doc.get("fields", {})
            cells = (f.get("influence_scores") or {}).get("cells") or {}
            score = cells.get(LENS) if isinstance(cells, dict) else next((x.get("value") for x in cells if x.get("address", {}).get("user") == LENS), None)
            if score is not None and score >= a.floor:
                out.add(f["pubkey"])
        cont = d.get("continuation")
        if not cont:
            return out, (time.perf_counter() - t) * 1000


def timed(fn, reps):
    res, ms = None, []
    for _ in range(reps):
        t = time.perf_counter()
        res = fn()
        ms.append((time.perf_counter() - t) * 1000)
    return res, statistics.median(ms)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]


trusted, build_ms = trusted_set()
print(f"lens {LENS[:8]} (observer {meta['observer'][:8]}), floor {a.floor}: {len(trusted)} trusted authors (visited in {build_ms:.0f} ms — the store would keep this set)")
pages = {t: [h for h in ids if h in events] for t, ids in meta["pages"].items()}
print(f"{len(pages)} captured pages: " + ", ".join(f"{t} {len(ids)}" for t, ids in pages.items()) + "\n")

pool = cf.ThreadPoolExecutor(a.concurrency)
# Warm every path once (dictionary pages, attribute reads, the JIT'd rank program).
for ids in pages.values():
    per_hit(ids[:20], pool), grouped(ids[:100]), by_author(ids[:100], trusted), load(ids[:100])

oracle = meta["oracle"]
mismatch = {"PER_HIT": 0, "GROUPED": 0, "BY_AUTHOR": 0, "LOAD": 0}
excess = {"GROUPED": 0, "BY_AUTHOR": 0, "LOAD": 0}
checked = exact_total = 0
STRATS = ["PER_HIT", "GROUPED", "GROUPED_KIND", "BY_AUTHOR", "LOAD", "UNGATED_GROUPED"]
for n in [int(s) for s in a.sizes.split(",")]:
    rows = {s: [] for s in STRATS}
    links_gated, links_all, kinds = [], [], {}
    detail = []
    for term, ids in pages.items():
        hits = ids[:n]
        if len(hits) < n:
            continue
        ph, ms_ph = timed(lambda: per_hit(hits, pool), a.reps)
        gr, ms_gr = timed(lambda: grouped(hits), a.reps)
        (grk, mix), ms_grk = timed(lambda: grouped(hits, by_kind=True), a.reps)
        ba, ms_ba = timed(lambda: by_author(hits, trusted), a.reps)
        (ld, docs), ms_ld = timed(lambda: load(hits), a.reps)
        ug, ms_ug = timed(lambda: grouped(hits, gated=False), a.reps)
        for s, ms in zip(STRATS, [ms_ph, ms_gr, ms_grk, ms_ba, ms_ld, ms_ug]):
            rows[s].append(ms)
        # Every answer against PER_HIT (the store's own COUNT semantics), and PER_HIT against
        # the live relay — the first size only, so each hit is checked once.
        for name, got in [("GROUPED", gr), ("BY_AUTHOR", ba), ("LOAD", ld)]:
            mismatch[name] += sum(1 for h in hits if got[h] != ph[h])
            excess[name] += sum(got[h] - ph[h] for h in hits)
        exact_total += sum(ph.values())
        if n == int(a.sizes.split(",")[0]):
            for h in hits:
                if h in oracle and oracle[h]["gated"] is not None:
                    checked += 1
                    mismatch["PER_HIT"] += oracle[h]["gated"] != ph[h]
        links_gated.append(sum(ph.values()))
        links_all.append(sum(ug.values()))
        for k, v in mix.items():
            kinds[k] = kinds.get(k, 0) + v
        detail.append((term, sum(ph.values()), sum(ug.values()), max(ph.values()), ms_ph, ms_gr, ms_grk, ms_ba, ms_ld, ms_ug))

    print(f"=== page = top {n} hits, {len(detail)} pages; trusted links per page: median {statistics.median(links_gated):.0f}, max {max(links_gated)} (all links: median {statistics.median(links_all):.0f}, max {max(links_all)})")
    print(f"{'page':<10} {'trusted':>8} {'all':>7} {'top hit':>8} | " + " ".join(f"{s:>12}" for s in STRATS))
    for d in detail:
        print(f"{d[0]:<10} {d[1]:>8} {d[2]:>7} {d[3]:>8} | " + " ".join(f"{x:>10.1f}ms" for x in d[4:]))
    print(f"{'median':<10} {'':>8} {'':>7} {'':>8} | " + " ".join(f"{statistics.median(rows[s]):>10.1f}ms" for s in STRATS))
    print(f"{'p90':<10} {'':>8} {'':>7} {'':>8} | " + " ".join(f"{pct(rows[s], 0.9):>10.1f}ms" for s in STRATS))
    total = sum(kinds.values()) or 1
    print("trusted link mix by kind: " + ", ".join(f"{k}: {v} ({100 * v / total:.0f}%)" for k, v in sorted(kinds.items(), key=lambda kv: -kv[1])) + "\n")

live = [v["gated_ms"] for v in oracle.values() if v.get("gated_ms")]
print(f"correctness: PER_HIT vs the live relay's gated COUNT: {mismatch['PER_HIT']} of {checked} hits differ (a link that landed between capture and COUNT shows here)")
for k in excess:
    print(f"  {k} vs PER_HIT: {mismatch[k]} hit-pages differ, net {excess[k]:+d} links over {exact_total} ({100 * excess[k] / max(1, exact_total):+.1f}%)")
print(f"live relay reference: one gated COUNT per hit over the wire, median {statistics.median(live):.0f} ms, p90 {pct(live, 0.9):.0f} ms ({len(live)} hits, serial)")
