# Cheaper observer-gated COUNTs — by author (exact), not sampled (2026-09-28)

> **2026-11-26:** by author was built and measured: it lost to the gate on every shape of a real
> slice and ships off. See "Built and measured" at the end.

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

## Built and measured: by author is not worth turning on (2026-11-26)

The design above was built, behind a setting that was off by default, and measured through the
store's own COUNT path on a real slice of the live relay. **On that slice it never beat the
gate on a single shape**, and the choice rule (ungated count first, by author above 250k) was
slower than the gate on every shape. **The code was not merged**: it was exact, but slower
everywhere, and it assumed a single writer of trust data that nothing checks. This section is the
record, so the idea is not re-proposed without new evidence; the names below are what the build
called things, and none of them exist in the repo.

### What was built (not merged)

- **`VESPA_GATED_COUNT_BY_AUTHOR`** (an `EngineTuning.gatedCountByAuthor`): unset, every gated
  COUNT ranks its match set as before. A number `N` turns it on: a gated count whose UNGATED
  match set holds at least `N` documents is answered by author; `0` answers every eligible count
  by author without the ungated count first.
- **`trust/RankMirror`**: the rank cells of up to 16 services held in the process, per service an
  open-addressing table of pubkey (four longs) -> rank (one byte), ~33 bytes a slot. It is a
  MIRROR of the engine, not a derivation: `TrustProjection` writes every trust change through
  `MirroredReputationIndex`, which applies the write to the engine and then, once acked, to the
  tables, under one mutex (so two writes cannot land in one order in the engine and the other in
  the tables). A half-done service walk is half mirrored, exactly as the engine ranks it. A write
  that throws drops every table, since it may or may not have landed.
- **Warm-up, off the request path.** A service is held only once a count asks for its lens. That
  count is answered through the gate and queues the service; one background visit of every
  reputation document fills the queued tables. A write during the visit marks its subject as
  touched for that service, and the visit's (possibly stale) value for that subject is skipped.
  `VespaEventStore.warmGatedCounts(observers)` warms a lens up front (a deployment's default
  observer at boot; the bench). `gatedCountMirrorBytes()` reports each table's heap.
- **`trust/GatedCountByAuthor`**, called from `TrustProjection.count`. It answers only a count on
  the recency-gated pair, termless (`EventYql.walksGated`), with a lowercase hex `rankKey`, a
  finite floor above 0, no per-key weights, no `acceptsPartial`, and its lens warm. It runs the
  ungated count first (unless the threshold is 0): 0 is the answer outright, and below the
  threshold the gate answers. Otherwise one `countByAuthor` grouping of the ungated match set,
  summed over the authors whose cell is at least the floor. In every other case it returns null
  and the gate answers as before.
- **No new port member.** It is `TrustProjection`'s own `count`, which already decorates
  `EventIndex.count`. `MeteredEventIndex` meters its ungated count and grouping as the two engine
  calls they are, and `UnionCount` reaches it once per filter. `PortDecoratorsTest` now also holds
  every `ReputationIndex` decorator (the mirror, the read-only backstop) to the whole trust port.
- **One writer only.** The mirror sees this process's writes and nothing else, so `open()` keeps
  none on a read-only store or one opened as `WriterTopology.SHARED`. Turning it on asserts that
  this process is the only writer of trust data. Production's relay is (the sync forwards its
  writes to it), but nothing checks that assertion.

### Why it answers what the gate answers

`recency_gated_exact` admits a document when `sum(query(user_q) *
attribute(author_influence_scores)) >= query(min_rank)`, with `user_q = {rankKey: 1}`. That sum
is the author's cell under the lens's rank service, or 0 when the author has none. The by-author
rule reads the same cell, for the same service, with the same comparison, and only where the
floor is above 0, so an author with no cell is out on both sides. The grouping and the ranked
count compile from the same `filterClauses`, so they match the same documents: kinds, authors,
tags, windows and the expiry cutoff. The profile exempts no kind and no author, the observer's
own events included, so neither does this. `include:spam` alone is not gated and never reaches
it; `include:spam` with a floor is gated at that floor and does. An `observer:` token and a
connection's observer arrive as the same `rankKey`.

It stays on the gate for: text (`sort:recent` with terms); the termless trust sorts (`rank_desc`
and the rest gate the same way, but they compute text features this was not measured against);
an observer whose lens names no service; an upper-case rank key; and any count while its lens is
cold.

**Parity evidence.**

- `GatedCountByAuthorTest`: 13 lens shapes × 11 COUNT shapes, compared against an in-memory index
  whose COUNT applies the profile's gate rule. The lens shapes are token, connection, token over
  connection, a lens-less observer, `gte:3`, `gte:50`, `gt:50`, `gte:101`, `gte:0`,
  `include:spam` with and without a floor, a connection with a floor, and no observer. The COUNT
  shapes include an expired note, a NIP-09-deleted note, the observer's own notes below the floor,
  a second lens, limits (clamp and 0), and overlapping and disjoint multi-filters. 90 of them were
  answered by author, and all 143 agree.
- The same test covers the fallback (a cold count goes through the gate and queues the warm-up),
  staying current (a new card, a supersession below the floor, a retracted rank and a NIP-09
  deletion each move the count at once), a failed write dropping the mirror, and a card written
  mid-visit winning over the visit's stale value. Each of these failed with its mechanism removed
  (the floor test, the touched set, the cell and removal hooks, the invalidation), and passed with
  it restored.
- `FilterMatrixIT`, in the build, counted every case a second time, through a store with
  `gatedCountByAuthor = 0` opened on the same engine, and required the two to agree. It then
  wrote two cards through that store and required both stores to move together. The run here:
  10,305 cases, 616 gated counts answered by author, 0 failing; the whole integration gate was
  green (`SearchCountIT` included).
- The bench asserts that the gated, by-author and rule stores give equal answers on every shape
  and every rep of the real slice: 12 shapes × 8 reps warm, and once each cold. They always did.

### The corpus and the method

The corpus is a read-only slice of `search.brainstorm.world` pulled on 2026-11-26 with
`relaySlice` (with `SLICE_KINDS`, `SLICE_TAGGED` and `SLICE_LENSED` modes the build added and
that were not merged either) and loaded through the store (`exportLoad`, made to stream
JSON-Lines). It holds 1,053,040 events in a single-node
Vespa 8.763.13 on a 4-vCPU, 15 GB host:

- 840,101 kind-1 notes by 30,637 authors. Most are the canonical observer's lens walked back
  17 days (520k new after dedup), and 743,637 (88.5%) pass the gate at floor 2.
- 157,453 of the canonical provider's 30382s, which make 157,447 reputation documents.
- All 2,169 of the relay's 10040s.
- 100,000 `#p:<observer>` events (kinds 1/6/7/9735) and 50,000 `#t:bitcoin` notes.

The disk ran out before the newest unranked window could be loaded. It was 1.28M notes by 352k
authors in 16 hours, 98% of them untrusted.

The build's `gatedCountBench` opened three stores on the same engine: `gated` (off),
`byauthor` (0) and `rule` (250,000). It also times `ungated`, the same filter with no observer
(the plain count, the rule's first step). Each shape ran 1 untimed rep and then 7 timed reps, the
four modes shuffled within each rep. The table gives p50 / p95 in ms, the lens is the canonical
observer at the default floor, and "authors" is how many groups the by-author grouping returns.
"Lens cold" is the rule store's first count, before its mirror was warm, so it was answered
through the gate.

| shape | ungated matches | gated count | authors | gated | by author | rule (250k) | ungated count | lens cold |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| global kind 1 | 840,101 | 743,637 | 30,637 | **125** / 142 | 564 / 592 | 596 / 689 | 64 | 416 |
| global kind 1, `filter:rank:gte:50` | 840,101 | 152,306 | 30,637 | **109** / 141 | 544 / 552 | 579 / 677 | 57 | 209 |
| follow 300 (most active trusted) | 386,501 | 386,501 | 300 | **65** / 79 | 77 / 84 | 135 / 144 | 59 | 95 |
| follow 1000 (most active trusted) | 565,721 | 565,721 | 1,000 | **112** / 143 | 145 / 155 | 225 / 238 | 93 | 208 |
| follow 1000 by activity [500,1500) | 158,340 | 154,971 | 1,000 | **43** / 48 | 55 / 57 | 76 / 81 | 39 | 127 |
| `#t:bitcoin` kind 1 | 60,959 | 11,239 | 1,737 | **15** / 18 | 33 / 36 | 25 / 27 | 9 | 51 |
| `#p` notifications 1/6/7/9735 | 100,000 | 94,717 | 8,000 | **47** / 51 | 125 / 133 | 61 / 67 | 13 | 103 |
| kind 1, last hour | 51,359 | 1,590 | 17,517 | **15** / 21 | 253 / 270 | 25 / 27 | 9 | 34 |
| kind 1, last 6 hours | 75,566 | 9,750 | 18,701 | **17** / 24 | 270 / 285 | 26 / 33 | 13 | 17 |
| 50 quiet authors | 1,217 | 907 | 50 | **5** / 6 | 6 / 8 | 10 / 10 | 5 | 31 |
| multi: follow 300 + `#t` (overlapping) | — | 393,420 | 300 | **423** / 462 | 435 / 447 | 482 / 506 | — | 1,436 |
| multi: global kind 1 + `#p` 6/7/9735 (disjoint) | 893,418 | 794,973 | 30,637 | **95** / 128 | 530 / 565 | 572 / 593 | 56 | 169 |

The overlapping multi-filter is dominated by `UnionCount`'s id walk of the smaller filter, which
by-author does not touch.

**The first count after an engine restart** was measured once per mode, with `docker restart`
before each pass (single samples, ms). Global kind 1 took 1,525 gated, 622 by author and 932 by
rule; `gte:50` took 213 / 485 / 712; follow 1000 took 212 / 153 / 247; the last hour took 34 /
286 / 44. The by-author global figure is not cold: choosing the shapes runs that same grouping
first. The only cold cost that stands out is the gate's first touch of the imported tensor.

**Where the by-author time goes**, from raw engine queries on global kind 1. The grouping costs
352 ms of engine query time for 30,637 groups (6.2 MB of JSON), about 11 µs a group. The store
then spends ~140 ms decoding and summing it. The gated count costs 112 ms at the profile's default
match threads (215 ms on one thread), and the plain ungated count 60 ms. The grouping pays per
AUTHOR as well as per match, and here an author has ~27 notes, so the per-group cost alone is
several times the gate's.

### Heap and warm-up

| | measured |
|---|---|
| mirror, the canonical provider (157,447 cells) | **8.65 MB** of arrays (2^18 slots × 33 B); used heap after GC grew 9.6 MB |
| warm-up (one visit of 157,447 reputation documents, one cell each) | **8.3 s** |

Production's 768k reputation documents, each with many cells, would make the visit longer
(unmeasured). The heap grows with the lens's own cards only, so a 150k-card personalized service
is ~4-9 MB.

### Does the gate's cost depend on the cells? Yes — and it still does not flip the verdict

The slice's reputation documents carry ONE cell each, but production's carry one per enrolled
service that scored the subject. To see what that does, 200 synthetic service cells were added to
13,650 of the documents (the feed was then stopped for disk). For 941 matched pairs of authors
with equal note counts, about 60k notes a side, single match thread, the gate's engine cost was:

| reputation document | gated | ungated | gate cost |
|---|---:|---:|---:|
| 201 cells | 69 ms | 16 ms | **0.88 µs a match** |
| 1 cell | 35 ms | 16 ms | 0.32 µs a match |

So the gate costs ~2.8× more on a production-shaped document. (On the 44M-doc corpus above a gated
count cost ~0.9 µs a match all in.) On the same 941 authors at the store's thread count, the wall times were:

| | gated | ungated count | by-author grouping (engine only) |
|---|---:|---:|---:|
| 201 cells | 41 ms | 20 ms | 31 ms |
| 1 cell | 32 ms | 18 ms | 30 ms |

By author wins by ~10 ms there, before the store decodes its groups. The rule then adds the
ungated count's 20 ms and loses again.

### Verdict and threshold

**Not worth shipping turned on.** On this slice every shape was faster through the gate. By author
cost 1.2× (follow lists, quiet authors) to 17× (a recent window over many authors) more at p50,
and the 250k rule cost 2.0-6.0× more on the shapes it sent by author, and 1.3-2× more on the rest,
where it pays the ungated count and then the gate anyway. The data gives no threshold: by author lost at every match-set size measured, so the best
setting is OFF (unset).

The earlier projection (3-5× faster) came from a Python prototype on a corpus where a gated count
cost ~0.9 µs a match all in. Here the gate itself costs 0.18-0.32 µs a match on one match thread,
and less at the store's thread count (global kind 1: 61 ms over the ungated count's 64 ms, for
840k matches). The grouping still costs ~11 µs an author.

How production differs, and which way each difference pushes:

- **Toward by-author: cells per reputation document.** Production has 768k reputation documents,
  each carrying a cell per scoring service; here each carries one. That makes the gate ~2.8× more
  expensive per match (measured above). On follow-list shapes, with many notes per author, it is
  roughly enough to make the grouping itself break even. It is not enough on global or windowed
  shapes, where groups are many.
- **Away from by-author: authors per match set.** The newest 16 hours of the live relay held 352k
  kind-1 authors for 1.28M notes. A production global count groups millions of authors; this
  slice's global count grouped 30.6k. The grouping's cost is per author, and in production every
  content node returns its own groups for the container to merge (five nodes today).
- **Unknown: scale per node.** A production node ranks ~189M events against a 4.4 GiB trust copy,
  and its cache behaviour is not this host's. A gated count's absolute cost there should be
  re-measured with `observer:` COUNTs against the live relay (read-only) before this is revisited.

If it is revisited, measure first: gated `observer:` COUNTs against the live relay (read-only)
at production cell counts. The only shape with a chance is a follow list's COUNT, few authors and
many notes each, answered by author with no ungated count first and chosen by shape rather than
by match count. Rebuilding it means the mirror, the warm-up and the parity checks described
above; neither the code, the bench nor the raw engine probes behind these tables are in the
repo.
