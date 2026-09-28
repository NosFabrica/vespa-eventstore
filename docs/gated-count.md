# Cheaper observer-gated COUNTs — by author (exact), not sampled (2026-09-28)

A gated NIP-45 COUNT runs the gated rank profile over every match — the gate is a ranking
pass — so it costs the whole match set: 5.3 s for a global kind-1 count on the 44M-doc relay
corpus. Time windows cannot help a count. Two cheaper answers were prototyped
(`benchmark/approx_count_probe.py`, read-only, lens = the canonical observer, floor 2.0):

- **BY AUTHOR (exact).** A gated count IS the sum, over the authors the lens trusts, of each
  author's match count. One grouping query (`group(pubkey)`) over the UNGATED match set returns
  every author's count; the lens's trusted set picks which to add.
- **SAMPLED (approximate).** The exact ungated count times a trust ratio measured in 24 random
  time windows of the match's own span (exact gated and ungated counts per window,
  concurrent) — NIP-45's `"approximate": true`.

| gated COUNT | exact (today) | by author | sampled |
|---|---:|---:|---:|
| global kind 1 (5.7M) | 5,313 ms | **1,363 ms** | 1,791 ms, err 7.7% (max 18%) |
| follow 300 (1.0M) | 913 ms | **135 ms** | 500 ms, err 4.9% |
| follow 1000 (1.1M) | 1,093 ms | **178 ms** | 375 ms, err 17.8% |
| `#t:bitcoin` (110k) | 121 ms | 67 ms | 83 ms, err 3.1% (max 15%) |
| `#p` notifications (75k) | 130 ms | 38 ms | 125 ms, err 0.2% |
| kind 1, last 7 days (54k, 12.9k authors) | 78 ms | 125 ms | 60 ms, err 17% |
| 50 quiet authors | 5 ms | 6 ms | 38 ms, err 34% (max 57%) |

By author matched the exact count on every shape and rep, 4–7× faster on large counts; it
loses only where a modest match set spans many authors (it pays per group returned).
Sampling is REJECTED: 0.2–34% median error (57% worst) — trust ratios drift over time and
windows are clustered samples — and no faster than the exact alternative where it matters.

**What shipping by-author needs.** A choice rule — the ungated grouping count first (10–280
ms); by author above ~250k matches, today's gated count below (projected: global 3.2×, follow
lists ~3.5×, `#t`/`#p` unchanged, worst case +20%). And the lens's trusted set in the store —
better a pubkey → score map, so an explicit `filter:rank:gte:N` floor works too. TrustProjection
sees every card write, so it can hold it per service in memory (~20–40 MB per service at
staging's 242k cards); the prototype built it by visiting every reputation document (19–22 s),
which is only fit for a cache warmed off the request path. See also `docs/trust-gate-filter.md`
(the schema alternative, measured and not shipped).
