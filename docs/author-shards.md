# Author shards: the corpus split by author across content clusters (PROTOTYPE)

**Status: prototype, off by default.** `VESPA_AUTHOR_SHARDS` / `EngineTuning.authorShards`
(1..256, unset = 1 = the bundled package, byte for byte). Verified on a single-host Vespa with two
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
  suffix. 256 buckets; shard `i` owns buckets `b` with `b * K / 256 == i` (contiguous runs).
- **Each cluster holds its authors' events AND their reputation documents**, the latter still
  `global`, so the import resolves locally and each node carries ~1/K of the trust tensors.
- **Routing is server-side**, through each cluster's `<document selection=…>`:
  - `event`: the cluster's pubkey test on the `event.pubkey` field, ANDed with the NIP-40
    expiry test it already had.
  - `reputation`: the same test on `id.specific`, the subject. Routing by id matters: a cell
    update that creates a document carries no `pubkey` field.
  - Anything not lowercase hex at that byte goes to shard 0, in both types alike. This
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
| **visit** | **400 "Please specify one of the content clusters"** | walks each cluster in turn (`VespaVisits`, `VespaReputationIndex`); a resumable page walk's token is `<cluster index>:<token>` |
| search hits | fanned out to every cluster and merged: sorting, `offset`, `limit` and `totalCount` all correct | nothing |
| the imported trust tensor in a rank profile | resolved inside each cluster from its own global copy | nothing |
| **grouping** | **one `group:root` per cluster, not merged** | `GroupingResults.mergeRoots`, in `queryRoot`, before any reader |

Without the merge, a grouping COUNT over the two-cluster fixture returned 24 of 60 documents,
because the reader took the first cluster's count. Without visits naming a cluster, every visit
was refused with the 400 above. `AuthorShardsIT` fails both ways when either fix is reverted.

### The grouping merge, and its one limit

Groups are matched by id under grouplists matched by label, at every depth. `count()` and
`sum(…)` add, `max(…)` and `min(…)` take the extreme, and any other output (`avg`, `summary()`)
keeps the first cluster's value. Every grouping the store itself runs is exact under this
merge:
- The author-keyed ones (newest versions, version census, per-author counts) never meet across
  clusters, because a pubkey lives in one.
- The rest are uncapped counts and sums.

**The limit is a consumer's `max(n)`.** Each cluster applies it before answering, so a group
that is n+1st in every cluster is missing from the merge, and the merged list keeps the order
groups were first seen in instead of re-sorting. A group's `relevance` is its rank within one
cluster's list, not a value, so it can't be used to re-sort. This matters for
`EngineReads.group` pipelines such as vespa-relay's statistics.

## What was verified

- `AuthorShardsIT` (2 clusters):
  - Placement: every event and reputation document sits in its author's cluster and nowhere
    else, including cell-update-born documents and a non-hex marker.
  - The gate: a trusted author in each shard passes, a low one in each drops.
  - Pages, COUNT, the per-author and distinct-tag groupings.
  - Id walks, the page walk across the cluster boundary, and the reputation walk.
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

## What is NOT known yet

- **Cost per query.** Every read now fans out to K clusters, and the container waits for the
  slowest. Nothing here measured latency or throughput at K > 1, or on more than one host.
- **Balance.** Middle bytes split AUTHORS evenly. Event volume per shard depends on how
  prolific each shard's authors are. With many authors per bucket this should average out, but
  it hasn't been measured on the production corpus.
- **The memory saving itself.** The expected per-node `reputation` footprint is 1/K. Read it off
  `/state/v1/metrics` on a real split before relying on it.
- **Garbage collection's re-evaluation of the regex on the content node** (C++) was not
  observed: no test here waited out a GC interval. Routing (Java) was observed, and the
  selections use only character classes and alternation.
- Ranking statistics are per cluster (bm25 corpus statistics, `perfect_share()`'s document
  counts), as they already are per node. The rank cases were not re-run on K > 1.

## Not a migration

With `garbage-collection="true"`, a cluster's selection is also what its nodes **keep**.
Deploying a different shard count over a corpus fed under another one makes each cluster delete
the documents the new selections move away, and nothing re-feeds them. Changing K means feeding
a fresh deployment, for example from a NIP-77 sync off the old one. `VespaApp.shardContent`
says so where the package is built.
