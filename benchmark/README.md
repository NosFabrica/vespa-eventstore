# `:benchmark` — event-store performance harness

A head-to-head benchmark of this framework's Vespa-backed `IEventStore` against
**Quartz's own SQLite `EventStore`**, on one identical, reproducible corpus. It
measures the two things that matter for a relay — **insertion** and **query**
speed — and, crucially, the **store-layer I/O amplification** that is the real
lever for optimizing *this* framework.

Not published to Maven Central (it pulls in the SQLite driver and a fixed corpus
generator that have no place in the released artifacts).

## Search-ranking harness (`rankAb` + `RankRegressionIT`)

Two tools guard SEARCH QUALITY, born of the 2026-07 lesson that an inert
`{prefix:true}userInput(...)` annotation can ship for months when nothing
measures what the emitted query actually does:

- **`RankRegressionIT`** (test, `@Tag("integration")`) — stands up a real Vespa
  in Docker, deploys the bundled schema, feeds the canonical regression corpus
  (Ode/ODELL/Odessa, VitorPamplona in all three spellings, José, 中村太郎,
  BitcoinMemeTreasury, a bio-only ODELL mention, a #bitcoin note…) and asserts
  where each expected doc lands and through which match TIER it arrived
  (`searchScored`). This is also what proves the schema — the second-phase
  `fieldMatch` features included — actually deploys. Run with `-Pintegration`.

- **`ObserverGateIT`** (test, `@Tag("integration")`) — the observer gate
  against a real Vespa: proves both gated profiles (`recency_gated` with its
  match-phase, `recency_gated_exact`) deploy, that the gate drops exactly the
  authors below `min_rank` (unranked ones included) while keeping newest-first
  order on both variants, that an explicit floor moves the cut, and that the
  lens-less query still recalls everything. Also prints the median latency of
  the dominant relay shape ("kind 1, limit 50") anonymous vs gated over a
  10k-note corpus — informational; run it against a staging cluster for real
  numbers. Run with `-Pintegration`.

- **`rankAb`** (main) — A/Bs ranking knobs against a LIVE Vespa with
  per-request `ranking.features.query(...)` overrides: no deploy, no reindex,
  no re-feed. The YQL comes from `EventYql` itself, so it is byte-identical to
  production's. Cases live in `benchmark/rank_cases.json` (`query` → expected
  doc-id prefix); **add a case every time someone reports a bad search**.

  ```bash
  ./gradlew :benchmark:rankAb --args="--vespa http://localhost:8080"
  ./gradlew :benchmark:rankAb --args="--configs baseline,near_off --profile text"
  ```
## Targeted benches (gradle tasks against a live Vespa)

Beyond the head-to-head suite (`:benchmark:run`), these tasks each own one
performance surface, expect `VESPA_URL` (default `http://localhost:8080`), and
carry their own CORRECTNESS gates — a wrong result set fails the run, so every
timing is also a proof:

| task | surface | corpus | gate |
|---|---|---|---|
| `visitBench` | full-corpus visit transports (paged serial / streamed slices) — the negentropy walk | feeds 2M synthetic docs once (`BENCH_VISIT_DOCS`) | every transport walks the identical doc set |
| `queryBench` | filter recall + count shapes: limit sweeps, since/until windows, authors, exact counts, the recency planner A/B | reuses visitBench's corpus (its linear layout makes every expectation exact) | window widths, orderings, and counts must match closed-form values |
| `multiFilterBench` | store-level multi-filter REQs vs the serialized sum (A/B `VESPA_QUERY_FANOUT`) | reuses visitBench's corpus | multi-filter result equals the deduped single-filter union |
| `searchBench` | NIP-50: rare/common terms, multi-word, trigram/fuzzy, search+filters, profile directory, `text` vs `text2` rerank sweep | feeds its own 200k-doc zipfian text band once (`BENCH_SEARCH_DOCS`) | 37 planted sentinel docs must be recalled exactly |
| `corpusLoad` | setup: loads the deterministic `NostrCorpus` a workload bench samples its filters from | 30k mixed events (`BENCH_SIZE`) | — |
| `dedupProbe` | the bulk-dedup existence check: full-summary vs summary-free variants at mirror hit rates, chunk × fan-out curves, REQ latency under dedup load | reuses a `corpusLoad` corpus (ids sampled off the live store) | every variant must return the identical member set |
| `transportProbe` | read-transport isolation: JDK h1 / OkHttp h1 / OkHttp h2c on identical queries across body sizes | any loaded store | — |

The workload benches (`corpusLoad`, `BENCH_MIXED`, `BENCH_THROUGHPUT`,
`BENCH_CONDPUT`, ingest sweeps) WRITE into the store. The exactness benches
(`queryBench`, `visitBench`, `multiFilterBench`) assume the pristine visit
corpus; snapshot the engine before dirtying it (`docker commit vespa
vespa-2m-snapshot`) and restore to re-run them.

## Did Quartz already have a benchmark to copy?

No. The closest thing in Amethyst's Quartz is
`nip01Core/store/sqlite/ParallelInsertTest.kt` — a **concurrency-correctness**
test (≈1,600 `TextNoteEvent`s across 8 coroutines, asserting "every insert is
visible" and no `SQLITE_MISUSE`), with **no timing and no throughput assertion**.
The `fs/` and `sqlite/` suites are parity/correctness, not performance. So there
was no events-per-second harness or standard event list to replicate — this
module builds one, modelled on a general relay's kind distribution, and runs it
against both stores through the shared `IEventStore` interface.

## What it measures

1. **Round-trip amplification** — how many engine calls (`get`/`search`/`count`
   reads, `put`/`remove` writes) `NostrSemanticsStore` makes per event, for the
   per-event `insert()` path vs the bulk `batchInsert()` path. This count is
   **engine-independent** (a property of the store's own logic — each call is one
   Vespa network round-trip or one SQLite statement), so it is the cleanest lens
   for optimizing the framework. Measured with `CountingEventIndex`.
2. **Insert throughput** — `insert()` and `batchInsert()`, events/sec.
3. **Query throughput** — the shapes a relay actually serves: author timeline,
   kind scan, tag mentions, id lookup, profile lookup, count, NIP-50 search.

### Backends

| Backend | What it is | Meaningful for |
|---|---|---|
| **SQLite (memory / disk)** | Quartz's `EventStore(dbName=…)`, real embedded DB | the honest apples-to-apples comparison |
| **Real Vespa** (`BENCH_VESPA_URL`) | `VespaEventStore.open(url)` against a running cluster | real engine numbers (ingest + BM25 search) |
| **Vespa/InMemory ref** | this store over the O(n)-scan in-memory reference engine | round-trip counts only — **not** a throughput proxy |

> The in-memory reference engine answers every filter with a linear scan, so its
> *wall-clock* is not representative of Vespa. Read its **round-trip counts**, not
> its throughput. Real Vespa numbers require `BENCH_VESPA_URL`.

## Running it

```bash
# SQLite + in-memory reference only (no external services):
./gradlew :benchmark:run

# Add real Vespa (stand one up first — see below):
BENCH_VESPA_URL=http://localhost:8080 ./gradlew :benchmark:run
```

Stand up a local Vespa with Docker, deploy the bundled schema, wait for it to serve:

```bash
docker run --detach --name vespa --publish 8080:8080 --publish 19071:19071 vespaengine/vespa
# VespaEventStore.open(url) auto-deploys the bundled app package on first contact.
```

Tunables (env): `BENCH_SIZE` (corpus events, default 30000), `BENCH_BATCH`
(batchInsert chunk, 500), `BENCH_QUERIES` (reps per query shape, 2000),
`BENCH_REF_SIZE` (cap for the O(n²) in-memory reference, 8000), `BENCH_SEED`.
Add `BENCH_THROUGHPUT=1` (with `BENCH_CONCURRENCY=1,8,32`) for the concurrent
read table (§3b), `BENCH_CONCURRENT_INGEST=1` (with `BENCH_CI_SIZE` /
`BENCH_CI_CONC`) for the concurrent-writer A/B (§2b), `BENCH_CONDPUT=1` (with
`BENCH_CP_ADDRS` / `BENCH_CP_OPS` / `BENCH_CP_OLDPCT`) for the draft-churn
conditional-put A/B (§2c), and `BENCH_SCALE_CURVE=1` (with `BENCH_CURVE_SIZES=…`)
for the [scale study](#scale-study-25k--400k).

## Results

Measured on a 4-core / 16 GB Linux box, JDK 21, corpus = 30,000 events
(seed 42), single-node Vespa 8.x in Docker. Relative numbers matter more than
absolutes; re-run on your own hardware. Kind mix per run is printed at the top
(~53% notes, ~26% reactions, plus reposts, metadata/contacts/relay-lists that
supersede, deletions, and addressable long-form).

### 1. Round-trip amplification — the headline finding

| path | reads/event | round-trips/event |
|---|---|---|
| `insert()` | 1.30 | **2.34** |
| `batchInsert()` | 0.05 | **0.05** |

**The bulk path issues ≈47× fewer engine round-trips per event.** The per-event
path pays, per event, up to four reads before the write — a dup probe (`get`), a
NIP-09 deletion probe (`search`), a NIP-62 vanish probe (`search`), and a
replaceable/addressable supersession probe (`search`) — averaging ~1.3 reads
across this corpus (most kind-1 notes skip the supersession/guard probes).
`batchInsert()` reads the whole working set in a handful of chunked queries and
pipelines the writes, amortizing all of it to ~0.05 round-trips/event.

This is engine-independent, and it dominates real-Vespa ingest (below).

### 2. Insert throughput (events/sec)

Steady-state — every ingest path is **warmed before timing** (see `Warmup`), so
these are not JVM/JIT warmup numbers (the ingest harnesses feed + discard junk
first; cold Vespa reads ~1.5× slower).

| backend | `insert()` | `batchInsert(500)` |
|---|---:|---:|
| SQLite (memory) | 13,371 | 11,149 |
| SQLite (disk) | 5,266 | **21,078** |
| Real Vespa (1 node) | **~75** | **1,107** |

- **On-disk SQLite batching is ~4×** (5.3k → 21k): the transaction amortizes
  `fsync`. In-memory SQLite barely differs (`batch ≈ insert` — no `fsync` to
  amortize; the two land within run-to-run noise).
- **Real Vespa: `insert()` ≈ 75/s vs `batchInsert()` = 1,107/s — a ~15× wall-clock
  gap on one node.** The per-event path is latency-bound (~2.3 sequential HTTP
  round-trips of admission probes per event, ~4–9 ms each); `batchInsert`
  pipelines the whole working set (round-trips/event → ~0.05), so warming the
  engine lifts it (765 cold → 1,107 warm) while the sequential per-event path
  can't pipeline and stays flat. On a multi-node cluster the feed client
  pipelines much further. **Rule: never ingest into Vespa via per-event
  `insert()`; always `batchInsert`.**

### 2b. Concurrent-writer ingest (`BENCH_CONCURRENT_INGEST=1`)

The write-side mirror of §3b, and geode's `publishThroughputConcurrent`: N
publishers each `batchInsert` a disjoint corpus slice at once (60k events/level,
id-disjoint per level), aggregate events/sec as N rises. The hypothesis was that
SQLite's **single writer** would bottleneck and this store's lock-free feed
client would scale past it. **It doesn't — on one box SQLite wins outright:**

| writers | SQLite ev/s | Vespa ev/s | SQLite p50 | Vespa p50 |
|---:|---:|---:|---:|---:|
| 1 | 10,670 | 862 | 47 ms | 542 ms |
| 4 | 12,246 | 943 | 165 ms | 1,979 ms |
| 8 | 12,480 | 946 | 317 ms | 4,245 ms |
| 16 | **13,210** | 1,079 | 580 ms | 7,167 ms |
| 32 | 13,135 | 1,014 | 1,127 ms | 15,251 ms |

Both results run against the hypothesis, and both are worth knowing:

- **SQLite's single writer is not the bottleneck it looks like.** Aggregate EPS
  holds ~11–13k across every writer count (a gentle rise to ~13k by 16), **0
  rejects** — concurrent batches are *coalesced by group commit* behind the lock,
  not blocked. More writers only raise per-batch latency linearly (47 → 1,127 ms)
  while total throughput stays flat. In-memory SQLite ingest is just fast.
- **The Vespa store barely scales with publishers (~1.2× from 1→32) and is ~13×
  slower in absolute terms.** Two reasons: (1) the feed client *already*
  pipelines a single stream — hundreds of writes in flight over HTTP/2 — so one
  `batchInsert` already saturates what this container's proton can absorb
  (~850–1,080 EPS), leaving little for external concurrency to add; (2) one
  shared-core container is **proton-bound, not lock-bound**, so latency balloons
  (p50 15 s at 32 writers) as every batch queues against that fixed ceiling.

So on a single box SQLite out-ingests single-node Vespa ~10× whether writers are
serial or concurrent — consistent with §2 and the ingest curve, and with geode's
own choice of SQLite + group commit for a single-box relay. Vespa's write scaling
is a **multi-node** property (proton sharded across content nodes); it does not
show on one container, and the feed client already extracts the available
single-node concurrency from one stream. **Net: concurrent publishers are not a
reason to pick single-node Vespa; corpus size, read concurrency at scale, and
search relevance are.**

### 2c. Draft-churn supersession — server-side conditional put (`BENCH_CONDPUT=1`)

Clients re-save replaceable drafts constantly (every ~2 s per open editor), so a
relay sees a flood of replaceable/addressable saves where **most are stale
resends** of a version it already holds. The default path pays a supersession
search per save to find the incumbent; Vespa can instead enforce NIP-01
newest-wins **server-side** with an address-keyed conditional put — one write,
no read, stale versions rejected by the engine. This is the opt-in
`VESPA_ADDRESS_KEYED` mode (address as docid + test-and-set condition; see
[`docs/server-side-constraints.md`](../docs/server-side-constraints.md) §2).

A/B on the identical churn stream (5,000 draft addresses, 60,000 saves, 69%
stale resends below the running head):

| arm | throughput | wall |
|---|---:|---:|
| A) read-then-supersede (default) | 599 ev/s | 100.2 s |
| B) conditional put (address-keyed) | **1,940 ev/s** | 30.9 s (50,478 stale rejected server-side) |

**B/A = 3.24×.** The win scales with the stale fraction: every stale resend
costs the read path a supersession search but costs the conditional put one
rejected write with no read. The mode is off by default — it is a docid-scheme
split (regular events by id, replaceables by address) worth enabling only where
replaceable/draft churn dominates. `BENCH_ADDR_SMOKE=1` (with
`VESPA_ADDRESS_KEYED=1`) is the live end-to-end correctness smoke for it.

> Note: through the production trust-projection decorator, supersession stays
> read-based so the projection's write reactions still see both versions; the
> conditional-put fast path engages on the undecorated index.

### 3. Query throughput (queries/sec; higher is better)

| query | SQLite (mem) | Real Vespa (1 node) |
|---|---:|---:|
| profile (kind 0) | **35,206** | 294 |
| tag-mentions (`#p`) | **8,908** | 196 |
| id-lookup (16 ids) | **4,061** | 82 |
| count(reactions) | 3,763 | 211 |
| kind-scan (notes, limit 200) | 959 | 63 |
| author-timeline (limit 50) | 404 | 162 |
| NIP-50 search | **94** | 45 |

**At 30k events on a single node, SQLite wins every single-query shape.** That is
expected and important: SQLite is an in-process embedded DB with no network hop,
so a point read (`profile`) is ~35k/s. Single-node Vespa pays a 4–9 ms HTTP
round-trip per query, and this box runs Vespa as one container sharing cores with
the client, so its absolute numbers are conservative. **Vespa's value is not
single-node small-corpus latency** — it is concurrency (next section),
horizontal scale, corpus sizes past SQLite's ceiling (the [scale
study](#scale-study-25k--400k) — author timelines cross ~70k, search ~300k), and
search **relevance quality** (trust-ranked BM25, trigram typo-recall). At this
scale the new SQLite FTS5 is even ~2× faster on NIP-50 search — the workload the
Vespa store exists to do *better* by ranking, once trust data and scale enter.

Practical read: **don't reach for Vespa until you outgrow SQLite.** Below a
hundred-k events on one box, Quartz's SQLite store is faster and simpler; the
Vespa store earns its keep under **concurrent load** (next) and **at scale**.

### 3b. List-shaped REQs and concurrency — where Vespa's parallelism shows

Every query above uses a single-value filter, but real relay traffic is
dominated by **list-shaped** REQs: a follow-feed carries the observer's whole
contact list as `authors`, a thread fetch carries dozens of `ids`, a
notification subscription carries a wide `#p` value list. The suites (latency,
real-Vespa, concurrent, parity) all run four such shapes, drawn from a shared
`BenchWorkload` whose value windows rotate per rep so no single hot list is
re-served from cache:

| query (per rep) | SQLite (mem) q/s | Real Vespa (1 node) q/s |
|---|---:|---:|
| follow-feed — 300 authors, kinds 1/6/7, limit 500 | **21.7** | 14.0 |
| contact-sync — 100 authors, kinds 0/3/10002, limit 300 | **118** | 72 |
| tag-list — `#p` ×100, kinds 1/7, limit 300 | **82** | 52 |
| ids-set — 100 ids | **883** | 102 |

(client and Vespa sharing cores, 30k corpus, seed 42 — same-run pairs, so compare
across the row, not against the earlier tables. Vespa is a single shared-core
container here, so single-query latency is its worst case.)

**Single-query at 30k, SQLite wins the list shapes too** — no network hop beats
one HTTP round-trip, even on a 300-author `IN`. But a relay never serves one
subscription at a time: **every connected client holds a follow-feed open**, and
that is where Vespa's HTTP/2 parallelism earns its place. Driving the real Vespa
store from rising client counts (events/sec, Vespa-only, same box):

| query (events/query) | conc 1 | conc 8 | conc 32 | bytes/event |
|---|---:|---:|---:|---:|
| follow-feed(a300) (500) | 6,589 | **55,596** | 54,815 | ~5,800 |
| kind-scan (200) | 11,881 | 45,063 | 42,937 | ~4,500 |
| ids-set(100) (100) | 8,978 | 34,538 | 34,473 | ~6,800 |

The follow-feed goes from 14 q/s single-threaded to **~55k events/sec at
concurrency 8–32** — clearing the 50k events/sec relay target — because each of
the in-flight REQs overlaps the others' round-trips over one multiplexed HTTP/2
connection. That parallelism, not single-query latency, is the read-side case for
the Vespa store; SQLite's in-process reads don't overlap the same way.

## Scale study: 25k → 400k

The full curves — six metrics × five checkpoints, both engines — are in
[`docs/scale-curve.html`](../docs/scale-curve.html) (interactive, log–log). One
prefix-stable seed-42 corpus was delta-ingested into both engines and measured
at each checkpoint (`BENCH_SCALE_CURVE`, this box; Vespa a single shared-core
container, so its absolute numbers are conservative — the *shape* is the point).
Both engines are **warmed before timing** (steady-state, not JIT warmup — this
is why the Vespa ingest row is now flat instead of starting low at 25k). Parity
holds **127/127** on both engines at every checkpoint.

The pattern: **SQLite's multi-row read shapes erode with the table while Vespa
stays flat**, so the two engines cross as the corpus grows.

| metric (q/s unless noted) | 25k | 50k | 100k | 200k | 400k | crossover |
|---|---:|---:|---:|---:|---:|---|
| author-timeline — SQLite | 518 | 223 | 87.3 | 43.2 | 22.0 | **≈ 70k** |
| author-timeline — Vespa | 192 | 188 | 156 | 150 | 134 | |
| NIP-50 search — SQLite | 132 | 61.0 | 28.7 | 14.9 | 7.5 | **≈ 300k** |
| NIP-50 search — Vespa | 32.8 | 33.8 | 22.8 | 14.1 | 8.2 | |
| follow-feed(a300) — SQLite | 38.7 | 28.6 | 19.5 | 16.8 | 13.3 | none ≤400k |
| follow-feed(a300) — Vespa | 14.0 | 13.3 | 13.5 | 13.3 | 12.8 | (converge) |
| id-lookup(16) — SQLite | 1,129 | 1,540 | 1,901 | 2,419 | 3,615 | none (20–48×) |
| id-lookup(16) — Vespa | 55 | 68 | 70 | 80 | 75 | |
| id-lookup(256) — SQLite | 286 | 316 | 244 | 258 | 281 | none (~18×) |
| id-lookup(256) — Vespa | 14.7 | 15.1 | 15.2 | 15.8 | 15.9 | |
| batch ingest ev/s — SQLite | 15,563 | 13,469 | 11,902 | 10,407 | 7,584 | none (7–13×) |
| batch ingest ev/s — Vespa | 1,189 | 1,220 | 1,221 | 1,023 | 1,099 | |

> The two id-lookup rows are from the later run that added the 256-id shape;
> SQLite point reads are sub-millisecond and noisy on this shared box (±30%),
> so read their *level* (20–48× ahead, no crossover), not the point-to-point wiggle.

Read:

- **Author timelines cross ≈70k.** SQLite falls 24× (518 → 22 q/s) as the
  single-author index walk lengthens; Vespa's fast-search attribute holds ~150
  q/s flat. Past ~70k, Vespa is the faster engine for this shape.
- **NIP-50 search crosses ≈300k.** SQLite's FTS5 decays 132 → 7.5 q/s over the
  growing match set; Vespa flattens and overtakes near 400k. (Both fall in
  absolute terms; Vespa just falls less — trust-ranked relevance is the reason to
  run it here, not raw speed. The crossover point is noisy run-to-run: reads swing
  ±20–40% on this shared box.)
- **follow-feed, id-lookup, and ingest never cross ≤400k on this box.**
  SQLite's in-process reads pay no HTTP hop, and this single-node container's
  follow-feed sits at ~14 q/s single-threaded. The follow-feed curves *converge*
  (SQLite 39 → 13 falling to meet Vespa's flat ~13 at 400k) but do not cross —
  its real Vespa win is **concurrency**, not single-query throughput (§3b: ~55k
  events/sec at conc 8–32). Ingest is now flat ~1,100 ev/s across all sizes
  (warm), 7–13× under SQLite.
- **id-lookup is flat on Vespa at every corpus size** — that flatness is the
  tell. A 16-id REQ holds ~55–80 q/s and a 256-id REQ ~15 q/s **independent of
  25k→400k**, because the cost tracks the *number of matched documents fetched*,
  not the table size. Isolated on the 400k store (single-threaded client): a
  256-id query spends ~7 ms matching (parse + `id in (…)` dictionary probes,
  17 KB of query text and all) and ~16 ms fetching the ~238 matched docs' summary
  — the fetch is the lever, not the id encoding. The query text *is* double-parsed
  (jdisc JSON → YQL AST, then messagebus protobuf → proton), but that cost is
  per-**term**, not per-byte: at a fixed 256 terms, shrinking each id 64→16 hex
  (a collision-safe 64-bit fold, 17 KB → 4.9 KB) moved the query only ~0.7 ms
  (6.7 → 6.0 ms, at the box's noise floor), while the 256 AST nodes / messagebus
  items / dictionary probes stay fixed. So **pre-hashing ids buys < 1 ms of the
  ~24–27 ms query (~3%)** — and the id is already a 32-byte hash Vespa re-hashes
  into its `dictionary { hash }` attribute, so folding it shorter only reintroduces
  collision risk on an exact-match key. The real levers for a large id-set are
  parallel `document/v1` GETs by primary key (what `ID_GET_FANOUT` already does
  ≤32 ids) and a leaner summary — both attack the fetch, not the id string.

Extrapolating the eroding SQLite curves, more shapes flip past ~1M. Practical
read: **a search/timeline-serving relay starts to benefit from this store in the
low hundreds-of-thousands of events; a point-lookup or small-corpus cache stays
better on SQLite.** The [store-migration A/B](#new-sqlite-store-quartz-72b8df7f6d-same-box-ab)
below isolates what amethyst PR #3663 changed in SQLite itself (old pin
`0d534a3149` → `72b8df7f6d`), run twice on this box with Vespa as the control.

Runs written with `BENCH_JSON=` are machine-readable and diff with
`compare_results.py` as a relative reference.

## New SQLite store (Quartz `72b8df7f6d`): same-box A/B

The bump from `0d534a3149` → `72b8df7f6d` pulls in amethyst PR #3663
("SQLite query scaling: FTS contentless, tag-merge, pooled statements"). To
attribute its effect **without** cross-box noise, the scale curve
(`BENCH_SCALE_CURVE`) was run **twice on this same box, back to back** — once
per Quartz pin, identical prefix-stable corpus, seed 42. **Vespa's store code
is unchanged between the two runs, so its curve is the box control**: Vespa
ingest matched to within 8% across every checkpoint (e.g. 400k: 1,177 → 1,174
ev/s), confirming the box did not drift — so every SQLite delta below is the
Quartz change, not the hardware.

**SQLite, old → new Quartz, same box (events/sec ingest; q/s queries):**

| shape (SQLite mem) | 25k | 50k | 100k | 200k | 400k | net |
|---|---:|---:|---:|---:|---:|---|
| **batch ingest** | 6,380→**11,165** | 3,775→**12,590** | 1,684→**12,181** | 769→**10,475** | 306→**8,912** | **+75% → +2,815% (29× at 400k)** |
| **NIP-50 search** | 47.8→**114.1** | 27.2→**55.5** | 13.1→**29.5** | 6.5→**15.5** | 3.4→**7.0** | **~2.2× at every size** |
| author-timeline | 347→450 | 263→275 | 97→81 | 42→44 | 24→22 | flat (±noise) |
| follow-feed(a300) | 33→36 | 34→32 | 25→25 | 19→18 | 16→16 | flat |
| id-lookup | 22.7k→25.2k | 30.3k→35.4k | 32.2k→24.8k | 28.8k→32.3k | 25.5k→29.6k | flat (noisy) |

**Two curves moved, three did not:**

- **Batch ingest no longer collapses.** The old store fell **21×** across this
  range (6,380 → 306 ev/s) as the table grew; the new store *rises then holds*
  at **~9–12k ev/s** (a shallow −20% 50k→400k). This is the FTS-contentless
  win: every `batchInsert` that supersedes a replaceable event or applies a
  NIP-09 deletion issued FTS5 deletes that were **O(n) column scans** on the
  old contentless-less table; the new schema seeks by rowid (**O(log n)**), so
  the per-batch delete cost stops scaling with the corpus. It is the single
  biggest change the bump makes.
- **NIP-50 (FTS5) search is ~2.2× faster at every scale** (400k: 3.4 → 7.0
  q/s). The contentless index is smaller and hotter in page cache; BM25 scoring
  still grows with the match set, so the *slope* is unchanged — the whole curve
  just lifts ~2.2×.
- **author-timeline, follow-feed, id-lookup: unchanged** (every point within
  the ±8–15% run noise this box shows). The tag-merge heap executor and pooled
  statements did not move the single-author, author-list, or point-id paths at
  these scales.

The **per-event `insert()`** path (not on the curve, but in the 30k and 300k
full suites) tells the same ingest story from the admission side: it degrades
**−3.8× memory / −1.7× disk** across 30k → 300k on this box (11,555 → 3,029 and
7,838 → 4,577 ev/s), versus the old store's documented **−12× / −9×**. In
absolute terms at 300k the new per-event insert is **3,029 vs the old
reference's 507 ev/s (≈6×)** — so the README's old headline "SQLite per-event
insert collapses 9–12× by 300k" no longer holds; the same O(log n) FTS deletes
that fix batch ingest also stop the per-event admission SELECTs from collapsing.

The **current SQLite-vs-Vespa crossovers** are the fresh
[scale study](#scale-study-25k--400k) above — author timelines ≈70k, NIP-50
search ≈140k, and feeds / point-reads / ingest never crossing ≤400k on this box.
Those supersede an older `BENCH_SIZE=300000` head-to-head that used to sit here:
this box runs its single-node Vespa container slower than the earlier reference
box, so the absolute Vespa figures shifted and are no longer directly
comparable. The box-**independent** result is the SQLite-side change measured
above (Vespa held as the control): sustained ingest stops collapsing, and FTS5
search lifts ~2.2× at every size.

So the new store **buys SQLite headroom on exactly the two axes it used to lose
first at scale — sustained ingest and full-text search.** A relay that ingests
heavily or serves search stays on SQLite meaningfully longer than the old
"~150k ingest / ~40k search" crossovers implied; the feed and timeline
crossovers are set by the fresh curve above.

> **tag-list caveat.** The `tag-list(p100)` shape (100 `#p` values, the query
> the tag-merge executor targets) is **not** on the scale curve, so it has no
> clean same-box A/B here. In the 30k suite the new store measured **90 q/s**
> vs the old README's 331 (a *cross-box* figure). The same-box control for the
> closest list shape — `follow-feed(a300)` — is unchanged, so part of that drop
> is box difference; but tag-merge does rewrite the tag path specifically, so a
> real small-corpus cost (it opens one cursor per value and heap-merges, paying
> per-stream overhead that only amortizes on large tag sets / large corpora) is
> plausible and **unconfirmed same-box**. Flagged honestly rather than claimed.

Regenerated graph: `docs/scale-curve.html` (both engines, this box, new
Quartz). Machine-readable curves: `results-curve-reference.csv` is now the
**new-Quartz** baseline; the old-Quartz curve is preserved in git history
(commit that added it) for anyone reproducing the A/B.

## Query-path optimizations (applied)

Profiling the live Vespa (via `presentation.timing` and payload sizing) showed
per-query latency is dominated by **server-side matching plus fixed overhead**
(2–7 ms) and, for large result sets, **payload transfer + client-side parse**;
network RTT and summary-fetch are small. Single-node throughput is also very
noisy (±15–50 % run to run — even a query whose code never changed swings that
much), so only structural changes are reliably attributable. Two landed:

1. **Trim the returned summary to the reconstruction fields** (`EventYql`): a
   served event needs only `id/pubkey/created_at/kind/tags/content/sig/owner`, so
   the query selects those instead of `*`. That drops the BM25 index fields —
   `search_text` (a full copy of note content), `name`, `about`, the `_gram`
   views — from every hit: **~35 % fewer bytes** on a 200-hit note scan, more on
   long-form content.
2. **Pure-id lookups use the document API's direct key get** (`VespaEventIndex`)
   instead of a `/search/` over the id attribute: **≈35–55 % faster per id**
   (raw `get` 3.8 ms vs search-by-id 5.9 ms). Capped at one concurrent get wave
   so the bulk-insert dedup preload (500-id chunks) stays on the single-search
   path and **ingest is unaffected**.

3. **Multi-value tag lists compile to the `in` operator** (`EventYql`): a NIP-01
   tag filter with N values used to build an OR-chain of `tag_index contains`;
   it now builds `tag_index in ("p:a", "p:b", …)`. `tag_index` is a fast-search
   attribute, so `in` resolves the whole list through one dictionary-backed
   iterator where the OR tree paid a per-term iterator plus the merge. A/B on
   the live corpus (same values, both forms, identical hit counts — semantics
   are equal; `MockYql` parses the new grammar so the wire tests still pin it):

   | `#p` values | or-chain ms | `in` ms | speedup |
   |---:|---:|---:|---:|
   | 10 | 6.6 | 5.6 | 1.17× |
   | 50 | 12.6 | 9.8 | 1.29× |
   | 100 | 13.2 | 10.7 | 1.23× |
   | 200 | 24.7 | 10.6 | **2.32×** |

   The `in` form is essentially flat in list width; the OR-chain grows
   linearly. Single values and `tagsAll` (AND semantics — no `in` form) keep
   `contains`.

4. **The bulk-dedup preload became a summary-free existence check**
   (`EventIndex.existingIds` — `select id` under the attribute-only `dedup`
   document-summary): 2.2–2.3× the old full-summary path at the production
   shape, ~2.2× less engine occupancy for the same offered dedup work. Full
   A/B, curves, and the rejected alternatives in the
   [dedup section](#dedup--existence-check-ab-dedupprobe--the-mirror-relays-hottest-query).

5. **TCP_NODELAY on the read transport** (`VespaHttp`): OkHttp never sets it,
   so every multi-segment POST risked a ~40ms Nagle/delayed-ACK stall —
   deterministic behind a userspace proxy, tail-shaped on a direct link. A
   500-id existence query dropped 26.2ms mean / 78ms p95 → 10.1 / 11.3
   direct, and 55.6 → 10.7 through docker-proxy. Benefits every read with a
   large body (follow-feed author lists included). Details in the
   [transport section](#the-read-transport-okhttp-needed-tcp_nodelay-transportprobe).

Everything is gated by the parity harness (now **127/127**, including the
list-shaped specs) and the engine test suite. Measured id-lookup gain from (2):
**+34–65 %** across runs. Server-side matching and the NIP-50 ranking cost are
inherent to the engine and were left alone (search relevance is a feature, not
overhead to cut).

The in-memory reference engine also compiles a query's list constraints to hash
sets once per scan (was per-doc linear membership — a 300-author filter over a
30k corpus was ~9M string compares per query). Same semantics; it just stops
punishing exactly the list shapes under study, in the benchmark and in every
store test that runs on the reference.

## Dedup / existence-check A/B (`dedupProbe`) — the mirror relay's hottest query

A relay that continuously mirrors upstream relays re-sees the network: at
steady state **~99% of offered events are already stored** (observed live:
11.9M rejected vs 171k accepted per cycle on a 52.9M-event store, with the
`dedup` ingest stage at 88s — 30–60× the actual write). The bulk path's stage
B answers "which of these 500 ids do I hold?" and used to answer it with the
regular recall query — materializing ~500 **full document summaries** to read
one field (`id`) off them. This A/B measured every alternative that can answer
set membership exactly.

Setup: 1M-event `NostrCorpus` (934k live docs after supersession/deletions) on
a single-node Vespa container (4-core host), client addressing the container
IP directly, production client with TCP_NODELAY (see the transport section
below — every number here includes that fix). Chunks are 500 ids at 99%
stored / 1% novel, hit ids sampled off the live store via the visit walk, so
membership has a known ground truth — **the probe `require`s every timed
chunk's member set to equal it**; a wrong member fails the run (it would be a
wrong write decision in stage B). Coverage is checked on every response; a
degraded answer is refused, same as production.

### One 500-id chunk, serial (ms/chunk; engine timing via `presentation.timing`)

| variant | ms/chunk | p95 | bytes/chunk | engine query ms | engine summary ms |
|---|---:|---:|---:|---:|---:|
| `prod` — store `search(EventQuery(ids))`, the old stage B | 29.2 | 38.9 | ~353k | 5.3 | 6.0 |
| `prod-new` — store `existingIds()` (shipped) | **12.7** | **14.4** | ~83k | 3.4 | 1.5 |
| raw full-summary query (JDK client) | 25.1 | 31.7 | 353,248 | 5.3 | 6.0 |
| raw `select id`, default summary class | 12.9 | 16.2 | 83,413 | 3.8 | 1.7 |
| raw `select id` + `dedup` summary class (the shipped wire shape) | 12.0 | 14.9 | 83,413 | 3.4 | 1.5 |
| raw grouping `all(group(id) each(output(count())))` | 15.2 | 20.2 | 99,892 | 8.1 | 0.0 |
| document-API gets ×500 (32 concurrent) | 144.3 | 217.5 | 584,238 | — | — |

**2.3× serial at the production constants — and ~3.9× against the branch's
starting point**, because the transport fix composes: before TCP_NODELAY the
old path measured ~50ms/chunk through the same client. What the old path paid
per chunk: ~353KB of response (full NIP-01 summaries — tags, content, sig —
for ~495 documents nobody reads) plus the client-side decode of all of it
(`prod` vs raw-full gap). The existence check transfers ~83KB, skips document
reconstruction entirely, and at the engine drops summary fill to attribute
reads.

- **Winner: `select id` under the attribute-only `dedup` document-summary**
  (event.sd ships it). Every field in the class is an attribute, so Vespa
  serves it from memory (the docsum attribute combiner) — the disk summary
  store never runs. On this cache-warm 934k corpus the default-class
  `select id` measures the same, but that equality is page-cache luck; the
  dedicated class is the structural guarantee for a 52.9M-event store under
  memory pressure, and it costs one deploy (no re-feed, no reindex).
- **Grouping** (matched ids as group values, zero summary stage) is exact and
  needs no schema change, but pays ~2.5x the engine query time in container-
  side grouping work (8.1ms vs 3.4ms). Rejected as the default; it remains
  the right shape where a redeploy is impossible.
- **Document-API gets do collapse at this width**: 144ms/chunk at real 32-way
  concurrency, 12× the winner — confirming `ID_GET_FANOUT`'s KDoc. They are
  also **wrong under address-keying** (a replaceable lives at its address
  docid, not its id), which is why `existingIds` stays on the search stack:
  the `id` attribute carries the event id whatever the docid is.

### Aggregate throughput (ids/s), chunk-size × fan-out

Fanned work runs with REAL concurrency (blocking sends on `Dispatchers.IO` —
an earlier revision of this probe serialized every "fanned" request on one
thread and published a flat fan-out curve; that number was a harness artifact,
caught in review and re-measured).

full-summary (the old path):

| chunk | f=1 | f=2 | f=4 | f=8 | f=16 |
|---:|---:|---:|---:|---:|---:|
| 100 | 13,742 | 28,522 | 36,224 | 46,470 | 55,053 |
| 500 | 25,043 | 49,727 | 73,719 | 89,310 | 81,333 |
| 2000 | 30,043 | 55,971 | 86,763 | 91,194 | 98,134 |

existence (`dedup` class, the shipped path):

| chunk | f=1 | f=2 | f=4 | f=8 | f=16 |
|---:|---:|---:|---:|---:|---:|
| 100 | 27,434 | 54,814 | 74,963 | 98,816 | 111,591 |
| 500 | 53,853 | 99,883 | 159,753 | 205,087 | 215,139 |
| 1000 | 65,866 | 115,049 | 189,580 | 231,013 | 256,542 |
| 2000 | 73,740 | 138,499 | 179,713 | 244,112 | 245,056 |

At the production shape (`CHECK_CHUNK=500`, `QUERY_FANOUT=4`) the existence
check is **2.2×** the old path (159.8k vs 73.7k ids/s); at that rate the
observed 88s dedup stage costs ~40s of engine time. Reading the curve:

- **Fan-out scales for this shape** — near-linear to f=4, still +28% to f=8,
  then the 4-core node saturates (~215–256k ids/s). `QUERY_FANOUT=4` leaves
  ~25% on the table for existence chunks on THIS box, but the cap is shared
  with far heavier query shapes (the summary-stage 504 risk it exists for),
  so it stays 4; a deployment can re-run this curve and raise
  `VESPA_QUERY_FANOUT` for its own hardware.
- **Chunk width still helps**: 500→1000/2000 buys another ~15–25% at high
  fan-out. `CHECK_CHUNK` stays 500 (the shape every other number here was
  measured at); `VESPA_DEDUP_CHUNK` widens stage B's chunks alone for
  deployments that value sync speed over per-query latency.

### REQ latency under continuous dedup load

An earlier revision published dramatic starvation numbers (87ms vs 41ms p50)
that review traced to a client-side thread convoy in the harness, not the
engine. Measured properly (load on its own IO-backed scope, REQ latency on an
uncontended path), a continuous fanout-4 dedup load on this 4-core node moves
a 50-hit author-timeline REQ only mildly:

| dedup load | REQ p50 ms | REQ p99 ms |
|---|---:|---:|
| idle | 6.5 | 7.3 |
| full-summary chunks (old stage B) | 9.0 | 13.8 |
| existence chunks (shipped) | 8.3 | 13.3 |
| grouping chunks | 7.5 | 14.3 |

The per-query differences are small at this load level — the real read
protection is that the existence check finishes the same offered dedup work
in **~2.2× less engine time**, so a sync cycle occupies the engine for less
than half as long. (The production incident this investigates — REQs starved
for 20s at 1087% engine CPU — involved ingest writes, trust projection and
dedup together at full tilt; this probe isolates the dedup component only.)

### The read transport: OkHttp needed TCP_NODELAY (`transportProbe`)

The A/B initially showed the production read stack (OkHttp, h2c prior
knowledge) far slower than a plain JDK HTTP/1.1 client on the identical
query. `transportProbe` isolated it to the one structural difference between
the clients: **OkHttp never sets TCP_NODELAY** (verified against okhttp-jvm
5.4.0 — no reference to the option), while the JDK client always enables it.
With Nagle on, a multi-segment POST (a 500-id existence query is a ~35KB
body) can stall a delayed-ACK quantum (~40ms) mid-upload — deterministic
behind a userspace proxy hop (docker-proxy), intermittent and tail-shaped on
a direct link. VespaHttp now sets TCP_NODELAY via a delegating socket
factory. Measured, 500-id existence query (mean/p95 ms):

| route | okhttp-h2c before | okhttp-h2c + nodelay | jdk-h1 (reference) |
|---|---:|---:|---:|
| through docker-proxy | 55.6 / 59.8 | **10.7 / 12.1** | 10.8 / 12.9 |
| direct to container IP | 26.2 / 78.3 | **10.1 / 11.3** | 11.6 / 14.2 |

The fix benefits every read the store makes (search, counts, guard and
version queries), not just dedup — any REQ with a large id/author list is a
multi-segment POST. It is also why "benchmarking through `-p 8080:8080`" no
longer needs a caveat: with nodelay the proxy hop costs ~0.5ms, not 45.

### Address-keying and the id paths (analysis, verified against the code)

`VESPA_ADDRESS_KEYED` is opt-in and default-off. What it changes for id work:

- **Stage B dedup is identical in both modes** — 500-id chunks always exceed
  `ID_GET_FANOUT`, so they ride the search stack either way, and the `id`
  attribute resolves docs at any docid. Nothing in this A/B moves.
- **Point reads get slower under address-keying**: `isPureIdLookup` is gated
  on `!addressKeyed`, so even a single-id REQ routes through `/search/`
  (~35–55% slower than the direct get, per the optimization notes above).
- **Stage D gets cheaper**: supersession becomes one conditional put per event
  (engine-side test-and-set, no read at all) — already the cheapest exact
  "do I hold this address and is mine newer" (§2c measured it at 1,940 ev/s
  vs 1,569 for read-then-supersede on draft churn). In default mode stage D's
  version reads are per-(kind, author-chunk) queries that return few docs —
  a different shape from stage B's 99%-hit id sets, and not the bottleneck
  the ingest stage timings point at (`dedup 88.4s  versions 10.5s`).

### Bloom filters and caches — why not (here)

At 99% duplicates the useful filter would prove *presence* cheaply, and a
Bloom filter proves only *absence* — `mightContain=false` (the exact "it's
new" answer) fires on ~1% of this traffic, so it saves ~1% of the work.
`GuardBloom` stays what it is: a guard-owner gate, whose no-false-negatives
property serves a differently-shaped question. An exact presence cache (ids
confirmed stored, invalidated through the write decorator the way the trust
projection observes deletions) would genuinely remove round trips, but it must
hold full 32-byte ids to stay exact (~1.7GB at 52.9M events, or an eviction
policy plus every deletion-style invalidation edge). With the summary-free
check at ~12ms/500 ids, the store-side complexity was not worth it; revisit
only if dedup is still the top line after this lands.

## Latency tails (p50/p95/p99 — now on every row)

Every suite records per-operation latency and prints the tail next to the
throughput number. Two immediate findings on the 30k corpus:

- **Single-stream tails are tight**: p99 runs 1.4–1.6× p50 across the query
  shapes (follow-feed p50 14.4ms → p99 22.3ms) — no pathological outliers on
  a quiet store.
- **Concurrency buys throughput with queueing, and the tail pays**: at
  concurrency 128 author-timeline reaches 1,892 q/s but p99 hits 117ms
  (vs 41.5ms at concurrency 32 for only ~26% less throughput); kind-scan's
  p99 reaches 448ms at 128. **For latency-sensitive serving the sweet spot on
  this box is concurrency ~32–64** — beyond it, added clients mostly deepen
  the queue. This is exactly the trade `numthreadspersearch=1` makes
  (throughput over per-query latency), now visible per shape.

## Bulk-ingest sweep (`BENCH_INGEST_SWEEP=1`) — chunk × streams

Single-node ingest is throttle-floor-bound, so which knob raises it —
bigger `batchInsert` chunks or more parallel streams? The grid (fresh
id-band events per cell, real store stack, per-chunk commit tails):

| chunk | 1 stream | 2 streams | 4 streams |
|---:|---:|---:|---:|
| 250 | 688 | 1,169 | 1,241 |
| 500 | 1,294 | 1,704 | 1,573 |
| 1000 | 1,719 | **1,868** | **1,947** |
| 2000 | 1,642 | 1,665 | 1,865 |

(events/sec; single 4-core node, shared with the client.)

**Both knobs matter, and they compound: chunk 1000 × 2 streams ≈ 1,850–1,950
events/sec — ~45% over the 500×1 default and 2.8× over small 250-event
chunks.** The curve plateaus at ~1,900–2,000 (the engine/throttle ceiling on
this box); 2,000-event chunks add nothing and double the per-chunk commit
latency (p50 ~1.2s). Practical rule for syncers: **feed `batchInsert` in
~1,000-event chunks from ~2 parallel streams**; going wider or larger only
deepens latency. (Parallel streams are safe: the store's guard/plan reads
overlap outside the writer lock, commits serialize behind it — and if the
streams are split by owner, this is exactly the multi-lane ingest shape from
docs/multi-node-consistency.md.)

## Ingest stage profile (`BENCH_INGEST_PROFILE=1`) — where the ceiling actually is

The profile mode books each `batchInsert`'s wall time into the store's own
stage counters and prints the split plus the feed-client gauge. On the live
400k corpus (batch 1000): **the write stage — the pipelined `putAll` awaiting
engine acks — is 69% of wall time** (10.2s of ~15s per 20k events); the read
stages (dedup, guards, versions) barely register. Average per-put latency
inside a burst is ~56ms versus ~6ms for a lone put: the puts queue.

The obvious suspect — the feed client's throttle window — was then swept and
**acquitted**: connections 32→64, streams 128→256, and every combination
land within noise (1,306–1,322 ev/s, write stage unchanged), and more
connections only raise per-put latency (57→92ms) while throughput holds.
That is the signature of a server-side ceiling, and `docker stats` confirms
it: during ingest the engine runs ~1.5 cores while the client JVM
(serialization + guard reads) takes much of the rest — the shared 4-core box
is the bottleneck, not the client pipeline.

Practical conclusions:

- **Client-side ingest tuning is exhausted.** Chunk ~1,000 × 2 streams (the
  sweep's sweet spot) is the ceiling this box allows; no feed-window knob
  moves it. The knobs are now overridable anyway
  (`VESPA_FEED_CONNECTIONS` / `VESPA_FEED_STREAMS` /
  `VESPA_FEED_INFLIGHT_FACTOR`) for hosts with more cores, where the
  defaults' small-core compromise no longer applies.
- **The remaining ingest levers are topology**, exactly as docs/scaling.md
  lays out: separate client hardware (stop sharing cores with proton), more
  engine cores, owner-lane parallel writers, multi-node. The fbench study
  reached the same conclusion for reads.

## Mixed read/write load (`BENCH_MIXED=1`)

A relay never serves REQs from a quiet store — queries arrive while syncs
feed it. `MixedLoadBench` measures the interference directly: a read-only
baseline (16 readers looping follow-feed + author-timeline), a write-only
baseline (2 writers streaming `batchInsert(500)` of fresh id-band-disjoint
events), then both at once, then reads again immediately after. Measured on
the 4-core box (client and Vespa sharing cores), ~35k-doc corpus:

| mode | ingest ev/s | query q/s | query ev/s |
|---|---:|---:|---:|
| read-only | — | 449 | 64,443 |
| write-only | 802 | — | — |
| **mixed** | **556 (−31%)** | **191 (−57%)** | **30,704 (−52%)** |
| post-ingest reads | — | 458 | 65,997 |

Both sides pay for concurrency: ingest keeps ~70% of its throughput, queries
keep ~half. Recovery is immediate — the post-ingest read pass is back at the
baseline, so the "flush shadow" seen after a full 30k ingest does not appear
at this write volume. On this box the interference is largely CPU contention
(three processes on four cores); separate client hardware would soften it,
which is exactly what this bench exists to verify per deployment.

## Schema & index study (vs SQLite's indexes) — A/B'd on live Vespa

A review of the `event.sd` schema against Quartz's SQLite indexing, with each
candidate change A/B-tested on a live single-node Vespa: fresh container per
variant, the identical 30,793-doc corpus fed raw both times, the same
rotating-window query shapes timed, and a **repeated baseline (v0 run twice)**
to calibrate run noise (±8–9% on this class of box — anything inside that band
is not a result).

### Index coverage: SQLite ↔ this schema

| SQLite index (`EventIndexesModule`) | Vespa equivalent |
|---|---|
| `UNIQUE (id)` | `id` fast-search attribute |
| `(created_at DESC, id ASC)` | `created_at` fast-search attribute (+ `order by`) |
| `(pubkey, created_at DESC)` | `pubkey` fast-search attribute |
| `(kind, created_at DESC)` | `kind` fast-search attribute |
| `(kind, pubkey, created_at DESC)` | posting-list intersection of the two |
| `event_tags (tag_hash[, kind, pubkey_hash], created_at DESC)` | `tag_index` fast-search array |
| `pubkey_owner_hash` | `owner` fast-search attribute |

Coverage is complete. The structural difference: SQLite's composite indexes
end in `created_at DESC` so newest-first order falls out of the index walk;
Vespa intersects independent per-field posting lists, then sorts on the
`created_at` attribute. Every filter the store issues hits a fast-search
attribute — there are no unindexed scans.

### CORRECTNESS: attribute matching was case-insensitive (now `match: cased`)

Probing the live engine showed Vespa string attributes match **uncased by
default**: a stored `t:MixedCase` matched `#t:["mixedcase"]` — and even a
query for `T:MIXEDCASE`. NIP-01 tag values compare by exact bytes and SQLite's
`tag_hash` is case-sensitive, so this was a real-Vespa-only spec divergence
(the same class as the empty-`sig` bug: the in-memory reference and the mock
compare strings exactly, and the old all-lowercase corpus could never catch
it). Fixed with `match: cased` on `id`/`pubkey`/`owner`/`tag_index`; the
corpus now gives ~20% of notes a Capitalized hashtag and the parity battery
asserts the exact value matches while the lowercased form matches **nothing**,
on both stores — so the gate holds on every future run.

### `dictionary: hash` (shipped) and `rank: filter` (rejected)

These four fields are only ever equality/`in`-matched — never range- or
prefix-scanned — so the default B-tree term dictionary buys nothing; `hash`
makes each term lookup O(1) in the number of unique terms (which grows with
every event: each id is unique). Cased match is a prerequisite, so the
correctness fix and this optimization ship together. Measured **at 30k docs:
noise-level** (B-tree depth is tiny; follow-feed −3%, ids-set −6%, both inside
the band) with **no cost** — feed rate unchanged (721 vs 710–727 docs/s). The
win this buys is scale headroom, priced at zero.

`rank: filter` on the filter attributes (compact rank-free posting lists) was
the other candidate: **no read-side win beyond noise, and the two variants
carrying it produced the two slowest feeds** (648 and 521 docs/s vs 710–727)
— a write-side cost with no benefit at any tested shape. Not shipped.

| variant | follow-feed | ids-set | tag-list | contact-sync | feed docs/s |
|---|---:|---:|---:|---:|---:|
| v0 baseline | 25.3 ms | 11.7 ms | 19.4 ms | 9.1 ms | 727 |
| v0 repeat (noise) | 25.2 ms | 11.2 ms | 22.6 ms | 10.8 ms | 710 |
| v1 hash-dict + cased | 24.5 ms | 11.0 ms | 19.6 ms | 9.0 ms | 721 |
| v2 rank:filter | 30.6 ms | 11.8 ms | 17.6 ms | 11.0 ms | 648 |
| v3 both | 26.0 ms | 11.6 ms | 28.3 ms | 9.0 ms | 521 |

### Is the store reading more from disk than the filters need?

The question was whether the many non-filter columns (search fields, grams)
slow queries down by inflating what is read per hit. How Vespa actually lays
this out:

- **Matching and sorting never touch the disk blob.** Every field the filters
  use is a fast-search attribute — in-memory posting lists and value arrays.
  The extra columns are irrelevant to match cost.
- **Summary fill reads the whole document blob per hit** from the document
  store (disk, page-cached), no matter how few fields the query selects: a
  document's fields are stored together in one compressed chunk. The
  `SUMMARY_FIELDS` select-list trim cuts network transfer and client parse
  (~35% fewer bytes measured), not the disk read itself.
- The blob does carry dead weight on the read path: `search_text` duplicates
  each note's `content`, and the profile/tier fields double a kind-0. Roughly
  2× blob size for notes. Shrinking it would mean deriving the search fields
  inside the schema (synthetic fields) — impossible here because extraction is
  per-kind Kotlin logic — or splitting search fields into a separate document
  type (a real option at scale, at the cost of dual writes). At benchmark
  scale the blobs sit in page cache and summary fill is CPU-bound decompress +
  copy, so this is documented as the known trade, not changed.

## Concurrent throughput and GC (`BENCH_THROUGHPUT=1`)

The latency suite issues one query at a time, so its throughput is capped by a
single round trip (~250 q/s). A relay's load is *concurrent*, so `ThroughputBench`
drives the store from N coroutines for a fixed duration and reports events/sec,
queries/sec, GC time, and **bytes allocated per event** (from the JVM beans) at
rising concurrency — the real metric, and the GC lens.

The current headline concurrency numbers are in [§3b](#3b-list-shaped-reqs-and-concurrency--where-vespas-parallelism-shows)
(fresh 30k run, conc 1/8/32 — follow-feed clears the 50k target at conc 32).
The figures below are a deeper GC-focused run at higher concurrency (@128) on a
~15k-doc corpus, `numthreadspersearch=1`, kept for the allocation findings:

| query (events/query) | events/sec @128 | bytes/event |
|---|---:|---:|
| kind-scan (200) | **~66,000–76,000** | ~9,200 |
| author-timeline (50) | ~27,000 | ~12,800 |
| id-lookup (16 ids) | ~6,000 q/s | ~49,000 |

**Bulk reads clear the 50k events/sec target**, and because the client and Vespa
are fighting over the same 4 cores here, separate hardware would go higher.
Throughput measured immediately after a heavy ingest drops (~48k) while proton
flushes in the background — the steady-state number is the representative one.

**How much higher on separate hardware?** `vespa-fbench` run *inside* the
container (no JVM client parsing on the same cores) isolates Vespa's own ceiling:

| query | Vespa Q/s | events/sec | JVM-client events/sec |
|---|---:|---:|---:|
| kind-scan (200) | 527 | **~105,000** | ~78,000 |
| author-timeline (50) | 1,578 | **~79,000** | ~24,000 |

QPS plateaus (16→64 clients) while p99 climbs 50→185 ms — Vespa is CPU-bound, not
thread-starved. The **author-timeline 24k → 79k gap proves the client-side
response parse/reconstruction was the shared-host limiter**, which is exactly what
the GC work below attacks.

### GC findings

Two optimizations roughly **halved per-event allocation on bulk reads**
(kind-scan 9,230 → 4,843 bytes/event, author-timeline 12,800 → 8,479):

- **Trimmed summary fields (above) directly cut allocation** — fewer response
  bytes to parse per hit.
- **`getByIds` short-circuits the single-id REQ** past the `mapBounded` fan-out
  (coroutineScope + Semaphore + async all allocate per call).
- **Direct reconstruction via Quartz's `EventFactory.create`** (applied): the
  store used to rebuild every result with `Event.fromJson(toEventJson())` — a
  serialize+parse round trip whose only purpose is to recover the typed subclass.
  `toEvent()` now calls Quartz's own by-kind factory straight from the stored
  fields — no JSON — which covers every kind and returns a base `Event` for
  unknown ones. Reconstruction alone, microbenchmarked over 40k events:
  **313 ns / 120 bytes per event, vs 5,317 ns / 3,892 bytes for the round trip
  (~17× faster, ~32× less garbage)**. `EventDocConversionTest` pins the factory
  to `fromJson`'s exact subclass and serialization across kinds. This cut
  kind-scan end-to-end from ~9,200 toward ~6,000 bytes/event.
- **Streaming response decode** (applied): the recall path parsed each response
  into a full immutable `JsonElement` tree, then walked it. It now decodes hits
  straight into flat `@Serializable` DTOs (allocating the target objects, not a
  wrapper per field); the count/grouping paths still build the tree. `get()`
  stays lossless (the DTO carries the search columns too). Measured on top of the
  above: **kind-scan 6,181 → 4,843 (−22%)**, author-timeline 9,887 → 8,479 (−14%).
- Remaining floor for tiny results (id-lookup ~45 KB/event): with the tree gone,
  what's left is the JDK HttpClient's per-request allocation plus the query
  objects — inherent to a 1-doc round trip, not JSON.

### Insert-side latency

The per-event `insert()` path ran its three independent admission reads — dedup,
NIP-09 tombstone, NIP-62 vanish — strictly in series. They now fire concurrently
and are checked in the original precedence, so a per-event insert pays one round
trip's latency for the guards instead of three (the bulk `batchInsert` path,
which already batches these, is untouched). All 135 store+engine tests, including
the full NIP-09/40/62 semantics suite, still pass.

### Vespa config studied

`numthreadspersearch=1` (one matching thread per query): for concurrent serving
this keeps cores saturated with *independent* queries instead of splitting one
query across cores. Measured: bulk throughput ~66k → ~76k events/sec and it kept
scaling to concurrency 128 instead of plateauing. Config-only, so results/parity
are unchanged. A latency-critical (few big queries) deployment
would raise it instead.

### The ingest ceiling is NOT indexing or durability (config A/Bs)

Single-node Vespa tops out ~1,000 events/sec on this box (§2, §2b) — ~10× under
in-memory SQLite. Two A/Bs, feeding 60k docs straight through `VespaEventIndex`
(`BENCH_SCHEMA_INGEST=1`, no store logic — pure engine write path), rule out the
two obvious suspects:

| A/B | variant A | variant B | Δ |
|---|---|---|---|
| **schema** | full index (attrs + BM25 + trigram): **1,101** ev/s | summary-only (every field stored, *nothing* indexed): **1,153** ev/s | **+5%** |
| **durability** | `sync-transactionlog=true` (WAL fsync): **1,033** ev/s | `sync-transactionlog=false`: **1,022** ev/s | **~0%** |

So **dropping every index buys 5%, and disabling the WAL fsync buys nothing** on
local SSD. It is not our client, either: the official `vespa-feed-client` run
*inside* the container gets the same rate (~860 ev/s cold, ~1,500–1,800 warm).
During a feed the container pegs **~380% of its 4 cores** — it is CPU-bound. A
per-process profile shows exactly where that CPU goes:

| process | role | %CPU (of 400%) |
|---|---|---|
| **`container.0`** (jdisc) | HTTP/2 + JSON parse + schema-validate + marshal to internal doc protocol | **110–170%** |
| `vespa-feed-client` | client-side JSON + HTTP (runs elsewhere in prod) | ~100% |
| **`vespa-proton-bin`** | **the actual insert: index + document store + WAL** | **~30–60%** |
| config-server / cluster-controller / distributor | admin + bucket routing | ~60% |

**The insert engine (proton) is the *lightest* major consumer.** The weight is
the stateless jdisc **container** tier marshalling every document — HTTP/2 →
JSON parse → validate each field against the schema → re-serialize into Vespa's
internal document/messagebus binary → route to proton → marshal the ack back.
That is *why* the schema and durability A/Bs came back flat: proton — which does
the indexing and the fsync — was never the bottleneck; it has headroom. The cost
is the document-API pipeline in front of it. In-process in-memory SQLite pays
none of that per-document marshalling, which is the entire ~10× gap — not the
schema, not durability, not the write.

Why the fsync was free when Vespa's own guide quotes a **60× win** from disabling
it: that figure is on **network-attached storage (EBS/NAS)**, where an fsync is a
network round trip. On local SSD the fsync is cheap *and* proton's group commit
amortizes it across the ~100 in-flight ops, so the cost that dominates on NAS is
noise here. `sync-transactionlog=false` is therefore a real lever **only on cloud
block storage** (a relay's durability is weak anyway — clients resend unacked
events), and noise on local disk.

### Config levers, reviewed

| lever | measured / status | verdict for this store |
|---|---|---|
| drop indexes (summary-only schema) | +5% ingest | **no** — trivial write win, and it removes all query ability |
| `sync-transactionlog=false` | 0% local SSD; up to 60× on EBS/NAS (Vespa docs) | **only on network storage** — legit for relay-grade durability there |
| `visibility-delay > 0` | not run | **trap** — batches index commits for feed throughput but delays visibility; the store's single-writer dedup/supersession does query-then-write and needs read-your-writes, so this breaks correctness |
| `numthreadspersearch` | 1 (above) | already throughput-optimal for concurrent serving; raise only for latency-critical single-query deployments |
| `paged` attributes | not run | **scale** lever, not speed — mmap/disk-backed attributes let the corpus grow past RAM (the corpus-past-SQLite story), at read cost |
| match-phase early-terminate | not run | **trap** — approximate; caps `totalCount` and can miss the newest, breaking exact-newest-N REQ semantics |
| feeding concurrency / feed-client conns | 1.0 / 8×256 | already maxed (services.xml + `VespaFeed`) |

**Bottom line: no single-node config knob meaningfully speeds ingest here** — the
cost is the jdisc container tier's per-document marshalling, not a tunable, and
not the write engine. The real ingest lever is **horizontal scale of the
*stateless container* tier**: in a real deployment the container and content
pools are separate node sets scaled independently, and feed CPU lives in the
container tier — so a feed-heavy relay adds container nodes while proton (which
has headroom here) stays on the content nodes. Our single all-in-one dev
container collapses container + content + admin (+ the client, in the profiled
run) onto 4 cores, so the container tier starves everything while proton idles at
~half a core — Vespa's worst-case shape, and why single-node ingest understates
it. Harness: `SchemaIngestProbe` (`BENCH_SCHEMA_INGEST=1`)
feeds against whatever schema is deployed, so the schema and `sync-transactionlog`
variants are reproducible by deploying the corresponding app package first.

### NIP-50 two-phase ranking: MEASURED at 489k — real but modest; not the default

The corpus-size precondition from the study below was finally met (489k docs;
"nostr" matches 100k), so the two-phase profile was built and gated properly:
`text2` in the schema (phase 1 = `relevance()` minus its two expensive
within-band tie-breakers — band-exact by construction; phase 2 = the full
tuned `relevance()` over the top 1,000 per node, `ranking.rerankCount`
overridable per query), judged by the new `BENCH_RANK_QUALITY` harness
(overlap@10/50 + Kendall-τ vs the single-phase ground truth, per term class,
with latency tails).

**Quality: PASSED.** Every single-word class — common, rare, short — is
IDENTICAL (overlap 1.00, τ 1.00). Multi-word terms show exactly the predicted
proximity effect at the window edge: 2 of 4 terms at overlap@10 0.80 with
τ ≥ 0.95 and membership ≥ 0.86@50. (The typo class matched nothing on kind-1
content — its gram net covers profile/title fields, so that class needs
kind-0 queries to bite.)

**Performance: MISSED the ≥2× bar.** Consistent wins — 14 of 16 terms faster,
median ~10–25%, best −29% ("bitcoin" 135.6 → 95.7ms p50), multi-word tails
tightened most (p99 320 → 270ms) — but nowhere near 2×. The instructive
finding is WHY: search at this scale is **match-bound, not rank-bound**. The
first phase still pays full matching (weakAnd + gram OR-trees over 100k
postings), and that, not the ranking math, is the floor. The scale curve's
search degradation is therefore mostly *match-set growth*, and the next real
lever would be match-phase approximation — a recall trade, a different and
heavier decision than reranking.

Verdict per the pre-declared criteria: **`text` stays the default**
(single-phase, exact tuned order). `text2` remains in the schema — inert
unless a query names it — so any operator can re-run
`BENCH_RANK_QUALITY=1` on their corpus and flip if a 10–25% median win with
the measured multi-word top-10 drift is the right trade for them. The
harness is permanent: no future ranking change ships without it.

### The original study (30k, 2026-07): two-phase studied, not shipped

Search is the slowest query (ranking-bound: `relevance()` runs over every match —
~2,700 hits for a common term). A two-phase profile (cheap bm25-sum first-phase,
then `relevance()` reranking the top 200) was measured against the current
single-phase `text` profile on the corpus. It **reordered results** — the top-50
overlapped 49–50/50 but the top-10 changed for 4 of 5 terms — while the speedup
was **marginal and noisy at this scale** (6→4 ms one term, 5→7 ms another). So it
trades the tuned relevance for a speedup that only materializes on far larger
match sets. **Not shipped**: it needs a corpus large enough for the win to appear
*and* a relevance-quality regression harness (representative signed data) to
prove the reranking is acceptable — search relevance is a headline feature, not
overhead to cut blind.

## CI correctness gate

`VespaParityIT` (`:benchmark`) stands up a real Vespa via testcontainers, ingests
the corpus, and asserts this store matches Quartz's SQLite store on the whole
NIP-01 battery — the gate the in-memory reference can't provide (only a real
Vespa exercises YQL, the streaming decode, the get fast path, and reconstruction
end to end; the empty-`sig` bug was real-Vespa-only). It is tagged `integration`,
excluded from the fast default build, run by a dedicated CI job with
`-Pintegration`, and self-skips where no Docker daemon is present.

## Correctness parity (SQLite ↔ this store)

The benchmark is also a **correctness gate**: `ParityCheck` loads the identical
corpus into Quartz's SQLite store and this store, then asserts they return the
**same event ids, in the same newest-first order, with the same counts** across a
battery of NIP-01 filters (kind scans, author timelines, id sets, `#p`/`#e` tags,
`since`/`until` windows, replaceable/deletion effects, and the list-shaped
follow-feed / id-set / wide-tag filters). The corpus advances
`created_at` by a positive step per event, so timestamps are distinct and the
newest-first order is a total order — the comparison is exact, not best-effort.
It runs SQLite ↔ in-memory reference always, and SQLite ↔ **real Vespa** when
`BENCH_VESPA_URL` is set. NIP-50 `search` is excluded (relevance ordering is
engine-defined; FTS5 and BM25 legitimately differ).

Current status: **127/127 checks agree** across SQLite, the in-memory reference,
and real Vespa.

### A NIP-09 divergence the parity gate surfaced

Building this harness turned up a spec-compliance difference. NIP-09 says:

> "Publishing a deletion request event against a deletion request has no effect."

Given N (note), D1 (kind 5 deleting N), D2 (kind 5 deleting D1):

- **This store keeps `[D1, D2]`** — D2 targeting the deletion D1 is a no-op, per
  spec. (`Deletions.applyDeletion` skips kind-5/kind-62 targets.)
- **Quartz's SQLite store keeps `[D2]`** — it has no such guard and erases D1,
  **violating NIP-09**. (Confirmed in `sqlite/DeletionRequestModule.kt`: the
  delete-by-id path has no kind-5/62 exclusion.)

So on this edge case **this store is correct and SQLite is not**. The corpus
therefore has its deletions target real *non-deletion* events (what clients
actually delete), keeping the parity gate a clean pass; the divergence is a
SQLite bug to report upstream, not something to "fix" by breaking this store.

## Where to hammer performance (framework optimization targets)

1. **Push ingest onto `batchInsert`.** The single biggest win, already available:
   ~47× fewer round-trips (§1). Any syncer/relay path still calling `insert()` in a
   loop against Vespa is leaving ~7× wall-clock on the table. Consider making the
   per-event `insert()` documentation warn against bulk use, or coalescing bursts.
2. **Trim the per-event read fan-out — DONE (`GuardOwners`).** The store now
   tracks the owners with any stored tombstone/vanish (two grouping queries at
   load, maintained on every guard written, self-disabling at the engine's
   group cap) and skips both admission probes for everyone else; the bulk path
   queries guards only for flagged owners. Measured: **reads/event 3.26 →
   1.73** on the benchmark corpus (whose 4%-deletion mix flags many of its
   400 authors — realistic deleter densities sit near 1.1), round-trips/event
   4.26 → 2.80, parity 127/127. Single-stream insert latency is UNCHANGED
   (counterbalanced A/B, all runs within 2%) — the probes ran concurrently
   with the dup get, so this win is engine READ CAPACITY, not per-op latency:
   it pays under concurrent load, exactly where the mixed bench showed reads
   and writes fighting. The cache is now scoped by `WriterTopology`; these
   numbers were measured with the load-once cache, which is what
   `open(writers = SINGLE_WRITER)` gives — reproduce them with that. The
   default `SHARED` is identical per insert (same skip, same reads/event) but
   rebuilds the cache every `guardRefreshSeconds` (300) so a second writer's
   tombstone can't be ignored forever; a harness run longer than one interval
   pays that scan's read load concurrently with whatever it is measuring.
3. **Real-Vespa single-node ingest is COMPUTE-bound, not throttle-bound**
   (see the ingest stage profile above): the feed-window knobs are exhausted;
   chunk ~1000 × 2 streams is this box's ceiling and the remaining levers are
   topology (docs/scaling.md).

## BUG found while benchmarking — now fixed (correctness, real Vespa)

`EventDoc.fromSummary` read `fields.getValue("sig")`, which **throws** when `sig`
is absent. Real Vespa **omits empty-string fields from query summaries**, so any
stored event with `sig == ""` — i.e. **every unsigned rumor**, which this store is
explicitly designed to hold (NIP-59 inner events, drafts) — came back from
`search()` as a dropped hit, and the query returned *nothing*. `count()`
(grouping) is unaffected, which masked it. `MockVespaEngine` never caught this
because it echoes empty fields back verbatim; only a real Vespa omits them.

**Fixed** in this branch — `sig` is now tolerant like `content`/`owner` already
were (`fields["sig"]?.jsonPrimitive?.content ?: ""`), with a regression test in
`EventDocTest` that reconstructs a rumor whose empty `sig` the summary dropped.

The corpus here signs its events (128-hex fake signature) so the benchmark
measures the common signed path; the fix is what makes *rumor* queries work
against real Vespa.
