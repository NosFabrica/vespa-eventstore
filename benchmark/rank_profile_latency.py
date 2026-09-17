#!/usr/bin/env python3
"""Per-rank-profile matching latency from proton — the live A/B instrument.

    # WATCH a live deployment for 10 minutes and accumulate every window
    python3 benchmark/rank_profile_latency.py \
        --metrics http://vespa:19092/metrics/v2/values?consumer=Vespa --watch 600

    # ONE window (whatever proton last published), or a saved dump
    python3 benchmark/rank_profile_latency.py --metrics http://vespa:19092/metrics/v2/values?consumer=Vespa
    curl -s http://vespa:19092/metrics/v2/values?consumer=Vespa > m.json
    python3 benchmark/rank_profile_latency.py --json m.json

    # A/B a candidate profile against the one it would replace
    python3 benchmark/rank_profile_latency.py --metrics … --watch 900 \
        --compare text=cand_text_b --compare search=cand_search_b

WHY THIS EXISTS. A rank-profile change is the one performance change this store
can put in front of PRODUCTION traffic without risking an answer: the profile is
chosen per query, adding one is a live config change (no refeed, no reindex),
and nothing selects it until `EventYql.profileOf` does or a `sort:` token names
it. So the candidate can run beside the shipped profile on the real corpus — and
then the question is how to read the result. `searchTrace` answers it with
synthetic queries; this answers it from the queries users actually sent, which
is the only thing that settles "does it work in production".

Named in docs/telemetry.md §9 as the gap: per-rank-profile latency comes from
each node's metrics proxy, not from the store's own counters, so it needs a
separate reader. benchmark/attribute_memory.py is the model for the shape.

WHAT PROTON PUBLISHES, verified against a live 320k-document node (2026-09-17)
rather than taken from the metric reference:

    service    vespa.searchnode
    dimensions rankProfile, documenttype
    metrics    content.proton.documentdb.matching.rank_profile.query_latency
               …rank_profile.query_setup_time
               …rank_profile.rerank_time
    each with  .count .sum .average .max        (sum/max in SECONDS)

FOUR THINGS THAT MAKE A NAIVE READER WRONG. Every one was measured here, and
each is why this file is longer than a jq one-liner:

  1. THE VALUES ARE A WINDOW, NOT A COUNTER. /metrics/v2/values publishes the
     last completed snapshot interval (~60s by default). Measured: 18 queries
     across three profiles reported as count 6/6/5 in one read, and a read 75s
     later with no traffic in between returned NOTHING AT ALL. So windows are
     ACCUMULATED here, never subtracted — a delta between two reads of a
     windowed metric is meaningless.

  2. ABSENT IS NOT ZERO. A profile that saw no traffic in a window is missing
     from the payload rather than present with 0. Counting it as a zero-latency
     sample would make an idle profile look like the fastest thing in the
     cluster.

  3. A MEAN OVER A PERIOD IS Σsum/Σcount, NOT the mean of `.average`. Averaging
     the per-window averages weights a window holding one query the same as a
     window holding a thousand — which, on traffic that arrives in bursts, is
     most of the error.

  4. WINDOWS MUST BE DEDUPED. Polling faster than the snapshot interval returns
     the SAME window again, and adding it twice doubles that window's weight.
     Each service carries a `timestamp`, so (hostname, service, timestamp) is
     the window identity and a repeat is dropped. Polling SLOWER than the
     interval silently skips windows instead, so --interval defaults below the
     default snapshot and the report prints how many distinct windows it saw:
     if that number is far under the elapsed time divided by the interval, the
     sample is missing traffic and the comparison is not yet trustworthy.

WHAT THIS IS NOT, AND THE NUMBER THAT PROVES IT MATTERS. `query_latency` is the
CONTENT NODE's matching-and-ranking time: the container's query phase, the fill
phase, the store's own page assembly and the network are all outside it. That
sounds like a virtue — the part a rank profile can change, without the noise of
the parts it cannot — and it is NOT. Measured on the 320k corpus, one window,
13 queries per arm, `text` against the two-phase candidate:

    proton rank_profile.query_latency    56.1 ms -> 50.0 ms    -11%
    container querytime                  84    ms -> 57    ms   -32%
    container summaryfetchtime (hits=0)  46    ms ->  8    ms   -83%
    client wall p50                     194    ms -> 81    ms   -58%

THE PROFILE-DIMENSIONED METRIC SEES A FIFTH OF THE SAVING. Most of it lands in
phases proton does not dimension by rankProfile — including, surprisingly, a
`summaryfetchtime` that differs by 38 ms at `hits=0`, where there is no summary
to fetch at all. So an operator who watches this reader alone after a deploy
will conclude the change did almost nothing.

Read it for ATTRIBUTION — which profile served which traffic, which nothing else
can tell you from live queries — and read the SIZE of the change off the
container's `querytime`/`searchtime` (per query via `presentation.timing`, or the
container's own undimensioned latency metrics) or off `searchTrace`'s client-side
p50. This reader answers "was the candidate actually used, and is it directionally
better on real traffic"; it does not answer "by how much".

PER CONTENT NODE, finally: on a multi-node cluster each node matches its own
share, so rows are summed across nodes and the node count is printed beside them.
"""
import argparse
import json
import sys
import time
import urllib.request
from collections import defaultdict

SERVICE = "vespa.searchnode"
PREFIX = "content.proton.documentdb.matching.rank_profile."

# The three timers proton dimensions by rankProfile, and the order they are
# reported in: total first, then the two parts of it that a profile change moves.
TIMERS = ("query_latency", "rerank_time", "query_setup_time")


class Accumulator:
    """Σcount / Σsum / max per (rankProfile, documenttype), over distinct windows.

    Windows rather than samples, because that is what the endpoint serves: see
    trap 1 and trap 4 in the module docstring. `seen` is the dedupe set, and its
    size is the number of distinct windows the report rests on.
    """

    def __init__(self):
        self.rows = defaultdict(lambda: defaultdict(float))
        self.seen = set()
        self.hosts = set()

    def add(self, payload):
        """Fold every window in [payload] that has not been counted already."""
        fresh = 0
        for node in payload.get("nodes", []) or []:
            host = node.get("hostname", "?")
            for svc in node.get("services", []) or []:
                if svc.get("name") != SERVICE:
                    continue
                window = (host, svc.get("name"), svc.get("timestamp"))
                if window in self.seen:
                    continue
                self.seen.add(window)
                self.hosts.add(host)
                fresh += 1
                for metric in svc.get("metrics", []) or []:
                    dims = metric.get("dimensions", {}) or {}
                    profile = dims.get("rankProfile")
                    if not profile:
                        continue
                    key = (profile, dims.get("documenttype", "?"))
                    values = metric.get("values", {}) or {}
                    for timer in TIMERS:
                        count = values.get(f"{PREFIX}{timer}.count")
                        if not count:
                            # Trap 2: no traffic for this profile in this
                            # window. Contributes nothing — not a zero sample.
                            continue
                        row = self.rows[key]
                        row[f"{timer}.count"] += count
                        row[f"{timer}.sum"] += values.get(f"{PREFIX}{timer}.sum", 0.0)
                        row[f"{timer}.max"] = max(row.get(f"{timer}.max", 0.0), values.get(f"{PREFIX}{timer}.max", 0.0))
        return fresh


# A metrics proxy is always local or inside the cluster, so it must NOT be
# routed through whatever HTTP proxy the environment advertises: urllib honours
# http_proxy for http:// URLs, and an environment that proxies egress will hang
# or refuse the request. An empty ProxyHandler pins the direct route.
_DIRECT = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def fetch(url, timeout=15):
    with _DIRECT.open(url, timeout=timeout) as response:
        return json.loads(response.read())


def mean_ms(row, timer):
    """Σsum/Σcount in milliseconds, or None when the profile saw no traffic.

    Trap 3: this is the query-weighted mean. The per-window `.average` values
    are deliberately never read.
    """
    count = row.get(f"{timer}.count", 0)
    if not count:
        return None
    return row[f"{timer}.sum"] / count * 1000.0


def print_rows(acc, min_queries):
    rows = {k: v for k, v in acc.rows.items() if v.get("query_latency.count", 0) >= min_queries}
    if not rows:
        print(
            f"\nNo rank profile saw {min_queries}+ queries in {len(acc.seen)} window(s). "
            "Either the cluster is idle, or --watch has not yet covered a window with traffic.",
        )
        return
    print(
        f"\nPER-RANK-PROFILE matching latency — {len(acc.seen)} window(s), "
        f"{len(acc.hosts)} content node(s), query-weighted means",
    )
    print(f"\n{'rankProfile':<26} {'doctype':<10} {'queries':>8} {'mean ms':>9} {'max ms':>9} {'rerank ms':>10} {'setup ms':>9}")
    print("-" * 88)
    for (profile, doctype), row in sorted(rows.items(), key=lambda kv: -(mean_ms(kv[1], "query_latency") or 0)):
        def cell(timer, field="mean"):
            if field == "max":
                value = row.get(f"{timer}.max", 0.0) * 1000.0
            else:
                value = mean_ms(row, timer)
            return f"{value:9.1f}" if value is not None else " " * 9

        print(
            f"{profile:<26} {doctype:<10} {row['query_latency.count']:8.0f} "
            f"{cell('query_latency')} {cell('query_latency', 'max')} "
            f"{cell('rerank_time'):>10} {cell('query_setup_time')}",
        )


def print_compare(acc, pairs, min_queries):
    if not pairs:
        return
    print(f"\n{'baseline':<22} {'candidate':<22} {'base ms':>9} {'cand ms':>9} {'delta':>8}   queries (base/cand)")
    print("-" * 100)
    for baseline, candidate in pairs:
        # Summed across document types: a profile serves whichever doctypes the
        # queries named, and the comparison is about the profile.
        def fold(profile):
            count = sum(r.get("query_latency.count", 0) for (p, _dt), r in acc.rows.items() if p == profile)
            total = sum(r.get("query_latency.sum", 0.0) for (p, _dt), r in acc.rows.items() if p == profile)
            return count, (total / count * 1000.0 if count else None)

        base_n, base_ms = fold(baseline)
        cand_n, cand_ms = fold(candidate)
        if base_ms is None or cand_ms is None or min(base_n, cand_n) < min_queries:
            print(
                f"{baseline:<22} {candidate:<22} "
                f"{'—':>9} {'—':>9} {'—':>8}   {base_n:.0f}/{cand_n:.0f}  "
                f"(need {min_queries}+ on both; a profile with no traffic is absent, not zero)",
            )
            continue
        delta = (cand_ms - base_ms) / base_ms * 100.0
        print(f"{baseline:<22} {candidate:<22} {base_ms:9.1f} {cand_ms:9.1f} {delta:+7.0f}%   {base_n:.0f}/{cand_n:.0f}")
    print(
        "\nThe two arms are only comparable if they served the same TRAFFIC MIX. "
        "One term's match set dwarfs another's, so a candidate reached only by "
        "`sort:` while the baseline serves everything is comparing terms, not "
        "profiles — drive both arms over the same queries.",
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--metrics", help="URL of a metrics endpoint (…:19092/metrics/v2/values?consumer=Vespa — that consumer carries the full set)")
    ap.add_argument("--json", help="a saved metrics dump instead of --metrics")
    ap.add_argument("--watch", type=int, default=0, metavar="SECONDS", help="poll for this long, accumulating distinct windows")
    ap.add_argument(
        "--interval",
        type=int,
        default=20,
        metavar="SECONDS",
        help="poll interval; keep it BELOW proton's snapshot interval (~60s) so no window is skipped (default 20)",
    )
    ap.add_argument("--compare", action="append", default=[], metavar="BASE=CAND", help="print an A/B row for two profiles; repeatable")
    ap.add_argument("--min-queries", type=int, default=1, help="hide rows thinner than this many queries (default 1)")
    args = ap.parse_args()

    if not args.metrics and not args.json:
        sys.exit("give --metrics <url> or --json <dump>")
    if args.json and args.watch:
        sys.exit("--watch needs --metrics; a saved dump is a single window")

    pairs = []
    for spec in args.compare:
        if "=" not in spec:
            sys.exit(f"--compare wants BASE=CAND, got {spec!r}")
        pairs.append((spec.split("=", 1)[0].strip(), spec.split("=", 1)[1].strip()))

    acc = Accumulator()
    if args.json:
        acc.add(json.load(open(args.json)))
    elif not args.watch:
        acc.add(fetch(args.metrics))
    else:
        deadline = time.monotonic() + args.watch
        failures = 0
        while True:
            # A failed poll is a poll, not the end of the run: the endpoint is
            # one hop away and a watch is meant to outlive a blip. Failures are
            # counted and reported, because enough of them mean windows were
            # skipped and the comparison is not yet trustworthy.
            try:
                fresh = acc.add(fetch(args.metrics))
                note = f"  (+{fresh} new)" if fresh else ""
            except Exception as exc:  # noqa: BLE001 — every transport failure is the same decision
                failures += 1
                note = f"  (read failed: {type(exc).__name__})"
            remaining = deadline - time.monotonic()
            print(
                f"  +{args.watch - max(0, int(remaining)):4d}s  {len(acc.seen):3d} window(s){note}",
                file=sys.stderr,
                flush=True,
            )
            if remaining <= 0:
                break
            time.sleep(min(args.interval, max(1, remaining)))
        if failures:
            print(f"\n{failures} poll(s) failed — windows may have been skipped.", file=sys.stderr)
        expected = max(1, args.watch // 60)
        if len(acc.seen) < expected:
            print(
                f"\nWARNING: {len(acc.seen)} window(s) over {args.watch}s, expected ~{expected}. "
                "Windows were skipped, so this sample is missing traffic.",
                file=sys.stderr,
            )

    print_rows(acc, args.min_queries)
    print_compare(acc, pairs, args.min_queries)
    print(
        "\nquery_latency is the CONTENT NODE's matching+ranking time, and it UNDER-REPORTS\n"
        "a rank-profile change: measured 56.1 -> 50.0 ms here (-11%) for a change worth\n"
        "-58% client-side, because most of the saving lands in phases proton does not\n"
        "dimension by rankProfile. Use these rows for ATTRIBUTION (which profile served\n"
        "what, on live traffic); take the SIZE from presentation.timing or searchTrace.",
    )


if __name__ == "__main__":
    main()
