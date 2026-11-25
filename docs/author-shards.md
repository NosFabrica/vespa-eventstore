# Author shards: the corpus split by author across content clusters (PROTOTYPE)

**Status: prototype, off by default.** `EngineTuning.authorShards` (1..256, unset = 1 = the
bundled package, byte for byte), which `EngineTuning.fromEnv` reads from `VESPA_AUTHOR_SHARDS`.
The variable is read nowhere else: a store opened without a tuning from the environment ignores
it, so the deployed package and the client always get the same count. Verified on a single-host Vespa with two
clusters; not yet run on a multi-host deployment or at production scale.

## The problem

`reputation` is a `global` document type, because `event.sd`'s `author_ref` imports its
tensors into every event's ranking, and Vespa only resolves a reference to a global document.
A global document is copied to **every node** of its content cluster. With personalized
service keys (one per user, `docs/service-keyed-trust.md` §1) the reputation data grows as
users × scored subjects, so every node carries that whole product, and adding nodes does
not reduce it.

## The design

Global means global *within a content cluster*, not within the deployment. So:

- **K content clusters**, `content` (shard 0, the bundled cluster's name), `content1` …
  `content{K-1}`.
- **Shard key: the pubkey's middle byte**, byte 16 of 32 (hex characters 32..33). The ends of a
  key are not uniform: vanity keys are ground for a prefix (an npub that spells a word) or a
  suffix. 256 buckets.
- **A bucket map decides which cluster owns which bucket** (`AuthorShards.BucketMap`,
  `EngineTuning.authorBuckets` / `VESPA_AUTHOR_BUCKETS`, written `00-99=0,9a-ff=1`). An even
  split of K (`authorShards`) is the map `b * K / 256`. An explicit map allows uneven splits
  (3 + 2 nodes take 60/40) and moves of single buckets, so a new cluster takes only the buckets
  given to it. Every sharded package records its map in `services.xml` as a comment, which is
  how the deploy guard reads what each active cluster keeps.
  The selection requires lowercase hex THROUGH that byte (`^[0-9a-f]{32}(…)`), never "anything,
  then hex": it is evaluated by Java's regex when the container routes and by the content node's
  when garbage collection re-reads it, and the two disagree on what `.` matches (`\r`,
  `\u2028`, …). A key the router sent to shard 0 that the collector read as another shard's
  would be deleted. ASCII classes mean the same in both.
- **Each cluster holds its authors' events AND their reputation documents**, the latter still
  `global`, so the import resolves locally and each node carries ~1/K of the trust tensors.
- **Routing is server-side**, through each cluster's `<document selection=…>`:
  - `event`: the cluster's pubkey test on the `event.pubkey` field, ANDed with the NIP-40
    expiry test it already had.
  - `reputation`: the same test on `id.specific`, the subject. Routing by id matters: a cell
    update that creates a document carries no `pubkey` field.
  - Anything not lowercase hex through that byte goes to shard 0, in both types alike. This
    catch-all is not optional: the trust ledger keeps non-hex marker documents among the
    reputations, and Vespa answers **200 and stores nothing** for a document that no cluster's
    selection accepts (measured).

`VespaApp.zipBytes(accessLog, authorShards)` writes this into the bundled package by copying
the content cluster K times (`VespaApp.shardContent`). `AuthorShards.selection` is the
expression to paste into a hand-written multi-host `services.xml`.

## What Vespa does with K clusters (measured, Vespa 8, 2 clusters, one host)

| operation | without a `cluster` parameter | what the store does |
|---|---|---|
| put (`/document/v1`, batch handler) | routed by selection; lands in exactly one cluster | nothing |
| partial update, `create=true` | routed by the reputation selection on the id | nothing |
| update without create, conditional put, remove by id | succeed; the remove takes the document out of the cluster that holds it | nothing |
| get by id | finds the document in whichever cluster holds it | nothing |
| **visit** | **400 "Please specify one of the content clusters"** | walks every cluster: the streamed and paged walks (`VespaVisits.pages`) all clusters at once, with the slice budget split between them; the resumable page walk one after another, its token `<cluster count>/<cluster index>:<token>` (a token without the prefix, or from a walk under another shard count, is refused); the reputation walks one after another |
| visit naming a cluster that does not exist | **400 "no content cluster 'X', only 'a', 'b'"**, every deployed cluster named | `AuthorClusters.verify`: the deployment's clusters checked against the client's, once, before any visit or scoped query |
| search hits | fanned out to every cluster and merged: sorting, `offset`, `limit` and `totalCount` all correct | nothing |
| `select … from sources content1 …` + `model.restrict=event` | searches that cluster only (without `model.restrict` it also reads `reputation`, which the cluster holds) | a query whose every match is by known authors goes to their clusters only (below) |
| the imported trust tensor in a rank profile | resolved inside each cluster from its own global copy | nothing |
| **grouping** | **one `group:root` per cluster, not merged** | `GroupingResults.mergeRoots`, in `queryRoot`, before any reader |

Without the merge, a grouping COUNT over the two-cluster fixture returned 24 of 60 documents,
because the reader took the first cluster's count. Without visits naming a cluster, every visit
was refused with the 400 above. `AuthorShardsIT` fails both ways when either fix is reverted.

### Author-scoped reads

`pubkey` matches `cased`, and the query builder writes authors as lowercase 64-hex, so a query
with an `authors` (or `authorWeights`) clause can only match documents stored under those exact
keys, which live in those keys' shards. `EventYql.authorScopeOf` carries that set on the query
(`VespaQuery.authorScope`), and `queryBody` rewrites `from event` to `from sources <their
clusters>` with `model.restrict=event`. A scope that spans every shard is left unrewritten.
Follow feeds, profiles and one-author lookups therefore stop paying the fan-out. Their answers are
pinned unchanged by `AuthorShardsIT` (recall, COUNT, pages, the gate) and by `FilterMatrixIT`'s
author cases on two clusters. The gain in latency is NOT measured.

A scoped query is only right if the client maps authors to clusters the way the deployment does,
which is why `AuthorClusters.verify` runs before the first one.

### The cluster check

A client configured for a different shard count than the deployment fails silently in the
places it names clusters. A visit over fewer clusters skips the rest, so a reindex or a trust
walk never sees a shard. A scoped query under another count reads the wrong cluster.
`AuthorClusters.verify` asks the document API for a cluster one past the client's last. The
refusal names every deployed cluster, and an answer with documents means the deployment has
more. Either way a mismatch throws before anything relies on it. Writes and unscoped queries
route server-side and are right whatever the client's count. So is `visitIds`, which pages
through search rather than visits.

### The grouping merge, and its one limit

Groups are matched by id under grouplists matched by label, at every depth. `count()` and
`sum(…)` add, `max(…)` and `min(…)` take the extreme, and any other output (`avg`, `summary()`)
keeps the first cluster's value. Every grouping the store itself runs is exact under this
merge:
- The author-keyed ones (newest versions, version census, per-author counts) never meet across
  clusters, because a pubkey lives in one.
- The rest are uncapped counts and sums.

**The limit is a consumer's `max(n)`.** Each cluster applies it before answering. A group that
is n+1st in every cluster would be missing from the merge, and the merged list could hold up to
K×n groups in first-seen order. A group's `relevance` is its rank within one cluster's list, not
a value, so it can't be used to re-sort. So `VespaEventIndex.group` (behind `EngineReads.group`,
used by pipelines such as vespa-relay's statistics) REFUSES a pipeline with a group limit when
K > 1, rather than return an answer that only looks right. The `max(expr)` aggregator is not a
limit and passes. Roots merge only when their ids match exactly: a second grouping in one query is
`group:root:1`, a different tree.

**A count of distinct values does not add either.** An `output(count())` beside a `group(F)`
counts the distinct values of F. Each cluster counts its own, so a value present in two clusters
counts twice: vespa-relay's `distinct(kind)` read 16 for 8 kinds on two clusters. It adds
correctly only when the values are split by cluster: F is `pubkey`, or the count sits in an
`each(…)` under a `group(pubkey)`. `ShardedGrouping` (behind `VespaEventIndex.group`, sharded only)
handles the rest:
- **`all(group(F) output(count()))`**, the shape statistics use, is answered EXACTLY. It runs as
  `all(group(F) output(count()) each(output(count())))`, the groups merge by value across
  clusters, and their number replaces the count. The groups are dropped, so the answer has the
  shape Vespa gives the original pipeline: a bare root holding `count()` (`AuthorShardsIT`
  compares the two). The sharded answer is the more accurate one. Vespa's own count is
  2–4% off at ~55 k pubkeys, and for a multi-value field it is wrong outright: `tag_index` read 1
  in a cluster holding 2 values, where the store reads the corpus's 3. It costs one group per
  distinct value, as the matching `each(output(count()))` statistic already does.
- **It is capped at 100,000 distinct values** (`ShardedGrouping.DISTINCT_CAP`). Each cluster
  returns at most one more than that, and a merged list past the cap is refused, not answered.
  Without the cap, a count over a field with millions of values (`tag_index` on production)
  would build every value in the container and the client, where one cluster pays almost
  nothing for the same question.
- **Every other group-level count over a non-author field is refused**, naming the field and the
  pipeline to count instead.
- **So is an `order(…)` over groups from several clusters.** The merge lists groups cluster by
  cluster as it meets them, so "the most common kind first" could name the wrong kind. Groups
  inside an `each(…)` under a `group(pubkey)` all come from one cluster, and their order stands.

One cluster is untouched: the pipeline goes to Vespa as it came.

## What was verified

- `AuthorShardsIT` (2 clusters):
  - Placement: every event and reputation document sits in its author's cluster and nowhere
    else, including cell-update-born documents and a non-hex marker.
  - The gate: a trusted author in each shard passes, a low one in each drops.
  - Pages, COUNT, the per-author and distinct-tag groupings.
  - A distinct count: one kind in both clusters reads 1, not the 2 a summed merge gives, in
    Vespa's own response shape. Three tag values read 3, where Vespa itself answers 1 per
    cluster for that array field. A non-rewritable distinct count is refused.
  - Id walks, a streamed visit over both clusters at once, the page walk across the cluster
    boundary (and a foreign token refused), and the reputation walk.
  - Author-scoped recall, COUNT, pages and the gate.
  - The deploy guard against the real config server: a one-cluster package over the loaded
    two-cluster corpus is refused, and nothing is deployed.
  - A client configured for three clusters refuses to visit or scope.
- `FilterMatrixIT` with `-DitAuthorShards=2`: 10,305 REQ+COUNT cases, 0 failing, authors split
  3/4. The fixture's seven authors were all `bbb…` and would have sat in one shard; their middle
  bytes now differ, and `ItAuthorShards.requireSpread` refuses a fixture that reaches only one
  cluster.
- `VespaParityIT` with `-DitAuthorShards=2`: parity with SQLite on quartz's conformance
  battery, 200 authors split 91/109. Quartz's corpus keys all carry `00` at the middle byte, so
  `ItAuthorShards.spreadAuthors` rewrites that byte, and everywhere the key appears, for both
  stores.
- All three pass on the default single cluster too.

```bash
./gradlew :benchmark:test -Pintegration --tests '*AuthorShardsIT'
./gradlew :benchmark:test -Pintegration -DitAuthorShards=2 --tests '*VespaParityIT' --tests '*FilterMatrixIT'
```

## Measured: more shards than hosts (2026-11-24)

`./gradlew :benchmark:shardScaleProbe` (`ShardScaleProbe.kt`), one 4-core / 15 GB host running
Vespa AND the benchmark client, a fresh Vespa per configuration. The corpus is synthetic and
loaded at the engine level: 400,000 events by 40,000 authors with hash keys (Zipf-skewed
activity), 70% notes with hashtags and mentions, plus 40,000 reputation documents carrying 960k
cells from 40 services. The read mix is closed loop over 8 classes, 60 s at concurrency 1 and
60 s at 16, through a client configured for K. Raw rows: `benchmark/shard_scale_results.jsonl`.

| K (clusters on the one host) | q/s, 1 client | q/s, 16 clients | search processes' RSS, total |
|---|---|---|---|
| 1 | 38.8 | 84.4 | 866 MB |
| 2 | 36.4 | 76.8 (−9%) | 1,223 MB |
| 4 | 25.9 | 44.4 (−47%) | 1,927 MB |
| 8 | 20.9 | 35.9 (−57%) | 3,151 MB |
| 8, 1 match thread per process | 23.1 | 36.9 | 3,000 MB |
| 4, 1 match thread per process | 24.3 | 37.7 | 1,792 MB |

p50 latency at one client, ms (K = 1 / 2 / 4 / 8):

| class | K=1 | K=2 | K=4 | K=8 |
|---|---|---|---|---|
| global feed, kind 1, limit 50 | 11.2 | 17.4 | 26.4 | 32.7 |
| gated global feed | 27.3 | 38.1 | 98.5 | 106.7 |
| follow feed, 300 authors (spans every shard) | 14.3 | 16.0 | 21.3 | 30.8 |
| one author's profile (scoped to one cluster) | 8.4 | 9.1 | 11.1 | 13.6 |
| hashtag feed | 7.8 | 9.5 | 12.1 | 18.0 |
| notifications (`#p`) | 8.5 | 10.1 | 13.0 | 18.4 |
| NIP-50 search, gated | 98.6 | 95.0 | 107.5 | 133.4 |
| COUNT, all kind 1 | 23.7 | 16.5 | 14.9 | 16.3 |

What it says, on this host and corpus:

- **Over-sharding costs throughput.** The host was CPU-bound at every K (380–389% of 400%),
  so the per-cluster work is paid out of query capacity: K=2 lost 9%, and K=4 and K=8 lost
  about half.
- **Limit'd reads pay the most.** Each cluster must find its OWN top N, so a page of 50 from
  K clusters finds K×50 and the container throws most of them away. The gated feed is worst
  (27 → 99 ms at K=4), since the gate makes each cluster scan deeper for its 50.
- **Whole-match-set work gets faster.** COUNT (24 → 15 ms) and, at 16 clients with 2–4
  clusters, search split across processes like extra match threads.
- **An author-scoped read gets slower too, just less.** A one-author profile (8.4 → 13.6 ms
  at K=8) reaches one cluster but still pays the host's contention. A 300-author follow feed
  spans every shard, so scoping cannot narrow it.
- **Each search process has a baseline of ~350–400 MB here.** That baseline, not the data,
  is most of the memory growth at this corpus size.
- **Retuning threads did not recover it.** One match thread per process helped the gated feed
  at K=8 slightly and halved search throughput.

Not measured: separate hosts, where the clusters stop sharing cores; production-sized slices,
where each cluster's fixed cost is a smaller share of a query (the K×N top-N amplification is
not a fixed cost and stays); the reputation memory saving itself, since 960k cells are small
next to the processes' baseline here.

**Operator guidance from this**: do not deploy many more clusters than hosts to avoid a later
re-split. Keep K at the number of hosts (or host groups) that need the reputation split, grow
a cluster by adding nodes, and plan a K change as the migration below.

## Moving buckets (measured, then built)

Vespa never moves a document between clusters, and `garbage-collection="true"` makes a cluster
delete whatever its selection stops covering. A move onto a NEW cluster is therefore a copy, a
check, a switch of who serves, and a delete. Each Vespa behavior it relies on was checked
first (Vespa 8, 2 clusters, one host):

| behavior | measured |
|---|---|
| two clusters whose selections overlap | a write lands in **both**, events and reputation updates alike |
| `?route=<cluster>` on a put or a remove | reaches **that cluster only**, whatever its selection says; a route that does not exist fails slowly (504) |
| `condition=false&create=true` on a put | writes only where the document is **absent**; a present one is left as it was |
| reading during the overlap with `from event` | the moving document shows **twice**; `from sources <serving>` + `model.restrict` does not |
| a deploy that narrows a cluster, `garbage-collection-interval="30"` | the moved event and reputation document dropped **13 s** after the deploy |

The default collection interval is an hour, and shortening it adds scanning load in
production. So the move removes the old copies itself (`route=<source>`), and never relies on
collection.

`BucketMove` (engine) runs the steps. Its KDoc has the detail; in order:

1. **Switch every store to the overlap map** (`BucketMap.overlapTo`). Each moving bucket keeps
   its owner and is also written to its new cluster, which serves nothing yet. A cluster that
   only receives is **pending**: no query or visit reads it. Then deploy the overlap package. It
   only widens what clusters keep, so the guard lets it through over data. Every new write now
   lands in both clusters.
2. **`copy()`**: the moving buckets' events and reputation documents copied into the new
   cluster inside Vespa, by a document-API visit with `destinationCluster`. No document passes
   through the tool. The copy overwrites, so a newer write that already landed there can be
   replaced by the older version, or a delete undone. Step 3 is what makes it exact.
3. **`diff(repair = true)` until clean.** A write racing the copy can leave the new cluster
   short or stale. The source is complete and receives every write, so it is the reference. Each
   candidate difference is read again from both clusters before it counts, because two
   snapshots taken seconds apart under live writes always disagree a little.
4. **Switch every store to the final map.** The new cluster serves. The old owner still holds
   copies, so a read over every cluster can meet a document twice. A sharded client drops
   repeated hits by id, and asks again for more when a page shrank below its limit, so a
   reader never mistakes it for the end. COUNTs over-report for this window.
5. **`finish { … }`**, in this order:
   - the moved documents are removed from the old owner (a document-API `DELETE` by
     selection), while the overlap package still sends it every write, so any copy a read
     meets there is current;
   - the caller deploys the final package inside the block. It narrows the old owner; the store's
     guard refuses this, and the move passes `allowNarrowing` only after step 3 is clean;
   - a second removal takes what was written to the old owner between the two.

   `remaining()` then confirms none are left. The first cut removed AFTER the narrowing deploy.
   For the whole cleanup, the old owner held copies that no write reached any more: a delete in
   that window was served again from the old copy, and a moved author was gated on stale trust.
   Now only the few documents written between the first removal and the deploy can go stale,
   and only until the second removal. `finish` refuses to start without a clean diff, and a
   `diff()` once it has begun is refused: with the source emptied, it would take every moved
   document for an extra and delete it from its new owner.

`AuthorShardsMoveIT` runs it end to end: 2 → 3 clusters on one host, 300 authors with hash keys,
1,200 events, and a writer adding events, deleting some and changing reputation cells (in the
moving buckets too) throughout: 449 writes in the last recorded run. Against an in-test oracle it checks:
- every phase: reads serve every live event once, never twice, and nothing deleted;
- after the move: each event and reputation document sits only in its new owner's cluster;
- every moved author's cell equals the last value written during the move;
- the trust gate reads the moved authors' trust in their new cluster;
- the store's own guard refused the narrowing deploy.

With the first cut's put-if-absent copy, one repair pass fixed 10 reputation documents. In
each, a live cell update had created a partial document in the new cluster before the copy
reached it: the race step 3 exists for. The copy inside Vespa finishes before the writer
can race it reliably (~40 writes in), so the IT now damages the new cluster itself after the copy.
It removes one moved event, adds one the source never held, and plants a cell the source never
had. The first diff must report exactly those (`missing=1 differing=1 extra=1`), and each must
be repaired. With the extra-document repair switched off, the IT fails.

**How fast** (`./gradlew :benchmark:moveSpeedProbe`, one 4-core host, a fresh Vespa per run,
400,000 events by 40,000 authors plus their reputation documents, one cluster split in two,
~218,700 documents moving, no live writes):

| step | first cut | pipelined writes | inside Vespa (now) |
|---|---|---|---|
| `copy` | 85.6 s (2,554 docs/s) | 86.0 s (2,543 docs/s) | **20.1 s (10,880 docs/s)** |
| `diff` | 60.9 s | 26.7 s | 27.5 s |
| `cleanup` | 79.0 s (2,768 docs/s) | 79.1 s (2,764 docs/s) | **39.1 s (5,593 docs/s)** |
| all three | 225.5 s | 191.8 s | **86.7 s (2.6× faster)** |

- **The first attempt, writes pipelined across pages instead of awaited per page, changed
  nothing for copy and cleanup.** Two reference rates in the same run said why. The host feeds
  ~5,700 docs/s through the store's own feed path, and visits the moving buckets at ~13,600 docs/s
  with no writes. So neither Vespa's writes nor its reads were the limit: the tool's own round
  trip per document was, reading each one out and putting it back.
- **The copy and cleanup now run inside Vespa**: a visit with `destinationCluster`, and a
  `DELETE` by selection. The copy's put-if-absent guard is gone with it. The diff, which always
  runs after the copy, repairs whatever that lets a racing write undo (`AuthorShardsMoveIT`
  checks it under live writes).
- **The diff got 2.3× faster** from fewer, larger batches: each batch re-reads both clusters, so
  it now sizes them from the copy's count against a fixed memory budget.
- At the copy's 10,880 docs/s, a bucket of production (~945 M events, 1/256 per bucket, ~3.7 M
  documents) copies in ~6 minutes on hardware like this. That is a projection from one host,
  not a measurement.

**Limits of this tool:**
- It only moves onto new clusters. A move onto a cluster that already serves would be read
  twice while it fills.
- It holds a batch of buckets' destination ids in memory during `diff`, sized to a fixed budget.
- Only the diff's repairs go through the client, one document at a time.
- It has run on one host with a small corpus, never on production scale. Plan a staging dry
  run to learn how long a real bucket takes.

## Knowing when: capacity telemetry and the bucket report

- `StoreTelemetry.headroom` (`EngineResources.Usage`) now reports, per node and per document
  type, the documents, memory, disk and transaction-log disk. `peakTrustMemoryShare` is the
  largest share of a node's document memory that is `reputation`. Divide `peakTrustMemoryBytes`
  by `TrustHealth.servicesNamed` for the cost per customer. The store hands back the facts;
  the threshold is the operator's.
- `./gradlew :benchmark:bucketReport` (read-only) walks every reputation document and every
  event's author. It prints events and reputation documents per bucket, how evenly the middle
  byte spreads them (min/mean/max), how many reputation documents score a subject with no
  stored event, and a bucket map that splits the EVENTS evenly for each cluster count in
  `BENCH_SPLITS`. On production the event walk takes hours: run it off-peak.
- A bucket cannot be split below one author. One very prolific author, a bot say, makes a hot
  bucket that no map can balance. On the synthetic corpus the report read bucket `ff` at 8.6×
  the mean, because the most prolific Zipf author's key falls there. Read the report's
  min/mean/max before trusting an even split of authors to be an even split of load.

## The trust copy's size (measured 2026-11-24)

`./gradlew :benchmark:trustFootprintProbe`, a fresh one-cluster Vespa per row, 100,000
subjects each scored by ~60% of 60 services: 3.6 M cells, read off the reputation document
type's own metrics.

| cells carry | memory | per cell | disk | transaction log |
|---|---|---|---|---|
| influence only (`int8`) | 78 MB | 21 B | 33 MB | 39 MB |
| + follower counts, `float` (today's schema) | 150 MB | 41 B | 67 MB | 75 MB |
| + follower counts, `bfloat16` | 139 MB | 38 B | 65 MB | 57 MB |

- **The follower counts are half of the TA memory**, and their number type is not what costs.
  A mapped tensor pays per cell ADDRESS, and the schema stores every (subject, service) address
  twice, once per tensor. Moving the follower counts alone to `bfloat16` would save 7% for a
  schema migration: not worth it.
- **One mixed tensor, measured** (`./gradlew :benchmark:tensorLayoutProbe`, the same 3.6 M
  cells in each layout, a minimal one-type application per fresh Vespa, 2026-11-25):

  | layout | document type memory | per cell | of which the attribute | disk | transaction log |
  |---|---|---|---|---|---|
  | two tensors, `int8` + `float` (today) | 149.6 MB | 41.5 B | 16.7 B | 66.5 MB | 69.1 MB |
  | one `tensor<float>(user{}, v[2])` | 130.7 MB | 36.3 B (−13%) | 12.8 B | 54.7 MB | 61.1 MB |
  | one `tensor<bfloat16>(user{}, v[2])` | 104.2 MB | 28.9 B (−30%) | 9.7 B | 44.1 MB | 49.6 MB |

  - **Not the ~2× guessed above.** The attribute, where the duplicated address lives, is only
    ~40% of the type's memory. The rest is the document store and metadata, which hold the
    cells' values whatever the layout.
  - **The mixed float tensor saves 13%**, because its two floats cost more than today's `int8`
    plus one float.
  - **Mixed `bfloat16` saves 30% of memory and a third of disk.** That pushes the author-shard
    threshold out by ~1.4×. It holds every rank 0..100 exactly, and a follower count to ~0.4%.
    That is enough for the log scale ranking reads it on, but it is not exact.
  - **The cost**: every rank expression reading `influence_scores` or `follower_counts` must be
    rewritten to `attribute(trust){v:0}` / `{v:1}`, the projection must write one field, and every
    reputation document must be rebuilt from the cards. That is a schema migration, decided
    against the shard split it postpones, not a quick win. Attribute numbers move ±10% between
    runs with Vespa's background compaction; the type totals were read once memory settled.
- **Churn**: 5 rounds of ~600 k cell rewrites (a provider re-publishing) left document disk flat,
  while the transaction log grew from 75 MB to 130 MB and memory 13%. Here disk plus log is
  about 1× memory; production's 38 GiB of disk against 4.4 GiB of memory is about 9×.
  Something accumulates on production that this run did not reproduce. Telemetry now splits
  document disk from transaction-log disk per document type (`EngineResources.DocumentTypeUsage`),
  which will say which one.

## Measured: one cluster, before and after (2026-11-24)

The question this work has to answer before anything turns it on is whether the one-cluster
store (production today) got slower. `./gradlew :benchmark:oneClusterAbProbe` (identical file
in both trees) ran against `main` as the branch left it (`23843fc`) and against this branch,
with ONE corpus loaded once (400,000 events, 40,000 authors, 960 k trust cells). Then 12
measurement runs alternated the two client versions against that same engine, ABBA-ordered,
each 45 s at one client and 45 s at sixteen. Raw rows: `benchmark/one_cluster_ab_results.jsonl`.

| | `main` | branch | difference | exact permutation p |
|---|---|---|---|---|
| q/s, 1 client (n = 6 + 6) | 38.2 (sd 2.1) | 37.9 (sd 1.4) | −0.7% | 0.83 |
| q/s, 16 clients | 85.5 (sd 3.5) | 86.4 (sd 2.8) | +1.0% | 0.66 |

At one client, every class's median latency is within ±1% between versions (global, gated
global, follow, profile, hashtag, notifications, search), and COUNT within +2.4%. At this
spread and sample, a throughput difference of about 5% or more would have shown. Reading the
code agrees: on one cluster the new path adds a null check and one small object per query
that names authors, and nothing else.

An earlier design gave each run its own fresh Vespa (load, then measure) and read the branch
8% slower at sixteen clients (p = 0.07, 5 pairs). The interleaved runs show that was the engine
still flushing and compacting after its load, not the client. A per-run fresh engine is too
noisy for an A/B at this size.

## Rehearsed on real data: `docs/growth-simulation.md`

A slice of the production relay (2,141 provider lists, 150,000 real cards, 202,051 events) went
through the real store. Users were added with 150,000 cards each, and the store was split from
one cluster to two while users kept enrolling. Trust re-derived with 0 drift, events matched a
reference exactly, and cleanup left nothing behind.

The run also found:
- under enrolment, the move's repair step does most of the TA copying;
- the transaction log, not the documents, is what grows on disk;
- the lensed feed gets ~3× slower on two clusters sharing one host;
- vespa-relay's `distinct(kind)` statistic doubled on two clusters. The store now answers it
  exactly (above).

## What is NOT known yet

- **Cost per query.** Every unscoped read fans out to K clusters, and the container waits for the
  slowest. Nothing here measured latency or throughput at K > 1, or on more than one host:
  neither the fan-out's cost, nor what scoping and the concurrent walks save.
- **Balance.** Middle bytes split AUTHORS evenly. Event volume per shard depends on how
  prolific each shard's authors are. With many authors per bucket this should average out, but
  it hasn't been measured on the production corpus.
- **The memory saving itself.** The expected per-node `reputation` footprint is 1/K. Read it off
  `/state/v1/metrics` on a real split before relying on it.
- **Garbage collection's re-evaluation of the regex on the content node** was not observed: no
  test here waited out a GC interval. Routing was observed, and the selections use only ASCII
  classes, a counted repeat and alternation.
- Ranking statistics are per cluster (bm25 corpus statistics, `perfect_share()`'s document
  counts), as they already are per node. The rank cases were not re-run on K > 1.

## The deploy guard

With `garbage-collection="true"`, a cluster's selection is also what its nodes **keep**. A
package under which some cluster keeps fewer buckets would make the engine delete every
document in the buckets it lost, and nothing re-feeds them.

`SchemaDeployer.checkShardChange` runs on every deploy path that replaces a serving package
(`deployIfChanged`, and `open()` / `deploySchema` with `IF_CHANGED`). It compares, cluster by
cluster, the buckets the active package keeps with those the new one keeps.

| change | over data |
|---|---|
| schema only, same map | goes through |
| only widens what clusters keep (the start of a move) | goes through |
| narrows any cluster (a new even K, a cut-over) | **refused** |
| narrows, with `allowNarrowing = true` | goes through: the move tool, after its diff is clean |

When nothing is deployed (the config server answers 404 `NOT_FOUND`), any package goes
through: there is nothing to lose. When the active package cannot be read, the guard never
takes it for a compatible one. The first cut did, for a one-cluster package, and would have let
one replace a sharded deployment during a config-server hiccup. It asks the container instead,
which names the content clusters it serves when asked to visit one that does not exist. One
served cluster to a one-cluster package goes through: a single cluster keeps every bucket either
way, so production's boot behaves as before. Anything else is decided by the document count, and
goes through only over an empty deployment. Moving to a new layout on real data is either the move above
or a fresh deployment fed from the old one, for example by NIP-77 sync.
