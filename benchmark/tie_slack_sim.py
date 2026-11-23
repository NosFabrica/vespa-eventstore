#!/usr/bin/env python3
"""SIMULATION: what each tie-slack rule costs a recency page, by feed rate and page size.

A limit'd recency page overfetches `slack` hits so the boundary second's tie group arrives
whole (VespaEventIndex.recallSummaries); when the events at or after the boundary second reach
`limit + slack`, the page pays a second, unbounded `[T,T]` query. The model:

  - a feed is a Poisson process at RATE events/s; one REQ per second, at a uniformly random
    point in the current second, newest-first, `created_at` in whole seconds (so the current
    second is partly filled);
  - `over` is the number of boundary-second events past the page's last slot; the page pays
    the extra query iff `over >= slack`;
  - cost per REQ: 800 us per extra query plus 4.4 us per hit asked for (the container CPU the
    benchmark/README.md replay measured, "The tie slack is learned per shape").

Rules compared: `flat` (64, before 10fbce5), `page` (the page's width clamped to 8..64,
10fbce5) and `adaptive` (TieSlackMemory.next: cold at 64, back to 64 on a cut, raised at once
to 2 * over + 8, narrowed by a quarter of the gap). It is a model, not a measurement: real feeds
are burstier than Poisson, which favours the wider rules.

  python3 benchmark/tie_slack_sim.py
"""
import math
import random

MIN, MAX = 8, 64
QUERY_US, HIT_US = 800.0, 4.4
READS = 20_000


def poisson(lam: float) -> int:
    if lam < 30:
        limit, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= random.random()
            if p <= limit:
                return k
            k += 1
    return max(0, int(round(random.gauss(lam, math.sqrt(lam)))))


def adaptive_next(slack: int, over: int, cut: bool) -> int:
    if cut:
        return MAX
    target = max(MIN, min(MAX, 2 * over + MIN))
    if target >= slack:
        return target
    return max(target, slack - max(1, (slack - target) // 4))


def run(rate: float, limit: int, rule: str, seed: int = 1):
    random.seed(seed)
    slack, cuts, hits, reads = MAX, 0, 0, 0
    for _ in range(READS):
        seconds = [poisson(rate * random.random())] + [poisson(rate) for _ in range(300)]
        seen = 0
        for count in seconds:
            if seen + count >= limit:
                over = seen + count - limit
                break
            seen += count
        else:
            continue  # the feed ran out before a full page: no boundary, nothing to learn
        reads += 1
        s = {"flat": MAX, "page": max(MIN, min(MAX, limit))}.get(rule, slack)
        cut = over >= s
        cuts += cut
        hits += limit + s
        if rule == "adaptive":
            slack = adaptive_next(slack, over, cut)
    return 100.0 * cuts / reads, hits / reads, (cuts * QUERY_US + hits * HIT_US) / reads


if __name__ == "__main__":
    print("| ev/s | limit | flat: extra / us | page: extra / us | adaptive: extra / hits / us |")
    print("|---|---|---|---|---|")
    for rate, limit in [(0.1, 10), (1, 10), (1, 20), (5, 10), (5, 20), (20, 10), (20, 20), (50, 10), (50, 20), (50, 100)]:
        f, p, a = (run(rate, limit, r) for r in ("flat", "page", "adaptive"))
        print(f"| {rate:g} | {limit} | {f[0]:.1f}% / {f[2]:.0f} | {p[0]:.1f}% / {p[2]:.0f} | {a[0]:.1f}% / {a[1]:.1f} / {a[2]:.0f} |")
