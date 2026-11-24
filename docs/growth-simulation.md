# Growth simulation: new users on real data, and the author split they force

## What this is for

Every content node holds a full copy of the trust assertions (TAs): the `reputation` documents
the trust projection builds from kind 30382 cards (`docs/service-keyed-trust.md`). Production
enrolls customers with **personalized service keys**: each new user brings a new trust service,
and that service publishes a card for every author it scores, ~150 k of them. So the TA copy
grows with **users × scored authors** on every node at once, and adding nodes does not shrink it.

`docs/author-shards.md` prepares the answer: split the authors across content clusters so each
node holds only its own authors' TAs, and move buckets of authors onto a new cluster while the
store serves (`BucketMove`). This simulation exists to answer, before that is ever run in
production, the questions only real data and real growth can:

1. **What does one more user cost** each node, in TA documents, memory and disk, and how long
   does enrolling one take through the real write path?
2. **Can the store split from one cluster into two in the middle of that growth**, with users
   still enrolling, notes still arriving, deletions still landing and the relay still serving,
   without losing, duplicating or mis-ranking anything?
3. **What does the split cost while it runs** (time per step, read latency), and does the source
   cluster actually give its memory back afterwards?
4. **Does anything a consumer reads change meaning on two clusters?** It checks vespa-relay's
   statistics pipelines in particular.

It is a rehearsal, not a benchmark. It runs on one host, so every cluster shares the same cores,
and on a slice rather than the whole corpus. Read its timings as "how long, on this host, for
this much data". Read its verification results as pass/fail.

## How to run it

```bash
# 1. a real slice of the production relay, READ-ONLY (plain NIP-01, `include:spam` for the
#    unranked corpus; never its Vespa, never a write). ~2 minutes.
./gradlew :benchmark:relaySlice          # -> benchmark/build/relay-slice/

# 2. a FRESH Vespa (the simulation deploys the bundled package and refuses a used one)
docker run --detach --name vespa-sim -p 8080:8080 -p 19071:19071 vespaengine/vespa

# 3. the simulation. ~1-2 hours with the defaults below.
SIM_USERS_BEFORE=4 SIM_USERS_DURING=1 SIM_USERS_AFTER=2 SIM_CARDS_PER_USER=150000 \
  BENCH_HEAP=5g ./gradlew :benchmark:growthSimulation
# -> progress lines, then REPORT {json}, also written to benchmark/build/growth-sim/report.json
```

| setting | default | what it is |
|---|---|---|
| `SLICE_SERVICES` / `SLICE_CARDS` / `SLICE_EVENTS` | 3 / 150000 / 200000 | template services, cards per template, newest events pulled |
| `SIM_DIR` | `build/relay-slice` | the slice to load |
| `SIM_USERS_BEFORE` / `_DURING` / `_AFTER` | 4 / 1 / 2 | users enrolled on one cluster, during the split, and on two clusters |
| `SIM_CARDS_PER_USER` | 150000 | cards each new user's service publishes (cloned from the largest template) |
| `SIM_SPLIT_MAP` | `00-7f=0,80-ff=1` | the two-cluster bucket map to split into |
| `SIM_NOTES_PER_SEC` | 20 | background notes by real authors; every tenth write deletes an earlier one |
| `SIM_SETTLE_MINUTES` | 10 | how long to keep sampling memory after the split |

## What it does, step by step

1. **Load (one cluster).** The slice goes through the real store: its events, every kind 10040
   the relay holds (every enrolled user's provider list), and the template services' cards. The
   trust projection builds reputation documents from them as production would.
2. **Grow.** A new user is a kind 10040 naming a NEW service key, followed by that service's
   cards: one per subject the largest real template scored, ranks jittered by ±5. Both go through
   `batchInsert`, so the projection writes cells as the cards land. After each user the
   simulation records the reputation type's documents, memory, disk and transaction log per search
   process, the time enrolment took, and the read latencies while it ran.
3. **Split, while users keep arriving.**
   - Every store restarts on the overlap map. This is a rolling restart: traffic pauses for the
     reopen.
   - The overlap package deploys, and the store's guard lets it through, because it only widens
     what each cluster keeps.
   - `BucketMove.copy()` runs while `SIM_USERS_DURING` users enroll.
   - `diff(repair = true)` runs until clean.
   - Stores restart on the final map.
   - The store's guard is shown to refuse the narrowing deploy, which then goes through only with
     `allowNarrowing`.
   - `cleanup()` removes the moved buckets' documents from the source, and `remaining()` confirms
     none are left.
4. **Grow again** on two clusters.
5. **Verify.**
   - **Trust:** `verifyTrust()` re-derives every reputation document from its stored cards and
     compares it with what is stored. Expect `drift = 0`.
   - **Events:** an in-memory reference store (`NostrSemanticsStore` over `InMemoryEventIndex`,
     the executable specification) was fed the same events and background writes. 38 NIP-01
     filters over the event kinds (every kind, single authors, 300-author follow sets, a hashtag,
     the newest 500) must return the same ids from both. Expect `mismatches = 0`.
   - **Statistics:** vespa-relay's `StatsYql` shapes, where `distinct(field)` must equal the
     number of groups `countsBy(field)` returns. Checked on one cluster before the split and on
     two after.
6. **After.** Memory per search process is sampled for `SIM_SETTLE_MINUTES`, to see whether the
   source cluster releases what it no longer holds.

Background traffic runs from load to verify: notes by real authors from the slice, a deletion of
an earlier note every tenth write, and a reader cycling relay-shaped REQs. Some of those REQs go
through a real user's trust lens (`observer:`), from a 10040 in the slice that names the template
service.

## Reading the report

- `arrivals[]`: one row per user.
  - `seconds`: from its 10040 to the projection settling.
  - `cardsPerSecond`
  - `trust`: per search process (`searchnode` is cluster `content`, `searchnode2` is `content1`):
    reputation `documents` (total, including removed ones not yet pruned), `active`, `memory`,
    `disk` and `tlog`, in bytes.
  - `reads`: p50/p95 per REQ class during that arrival.
- `split`: seconds per step; documents copied, repaired (per pass) and cleaned up; reads during
  the split.
- `verify.trust.drift`, `verify.events.mismatches`: must be 0.
- `statsBeforeSplit` / `verify.statsPipelines`: `agree` per field.
- `afterSplitMemory[]`: the samples after the split.

## Results: the first run (2026-11-24, one 4-core / 15 GB host)

The slice, pulled read-only that day:
- **2,141 kind 10040 lists naming 2,007 trust services.** The September notes counted 341
  named services, so enrolment grew about 6× in two months: the growth this was built for.
- **The largest template service's 150,000 cards** (capped), plus two smaller services
  (15,737 and 36 cards).
- **202,051 events** (190 k notes, 6.7 k profiles, 3.0 k reactions, 1.5 k reposts, 597 follow
  lists). They cover only the newest ~1.7 hours: the relay aggregates a lot.

Run: 4 users on one cluster, 1 during the split, 2 after, each 150,000 cards, 20 background
notes/s. Raw report: `benchmark/growth_simulation_report.json`.

### Every check passed

| check | result |
|---|---|
| `verifyTrust` after everything: 162,374 reputation documents re-derived from their cards | **0 drift** |
| 38 NIP-01 reads against the in-memory reference (every event kind, authors, follow sets, hashtag, newest 500) | **0 mismatches** |
| the moved buckets' documents left in the source after cleanup | **0** |
| the store's own guard, offered the narrowing deploy | **refused it** (the move's verified deploy then went through) |

### What a new user costs

| user | topology | enrolment | cards/s | reputation memory (`searchnode`, `searchnode2`) | transaction log |
|---|---|---|---|---|---|
| (load) | one cluster | 148 s | | 49.7 MB | 21.4 MB |
| 1 | one cluster | 61 s | 2,442 | 53.3 MB | 34.1 MB |
| 2 | one cluster | 54 s | 2,793 | 65.3 MB | 45.1 MB |
| 3 | one cluster | 47 s | 3,164 | 64.3 MB | 67.4 MB |
| 4 | one cluster | 51 s | 2,970 | 58.0 MB | 86.9 MB |
| 5 | during the split | **225 s** | 667 | 59.7 + 23.8 MB | 128 + 6 MB |
| 6 | two clusters | 58 s | 2,608 | 52.1 + 49.3 MB | 149 + 25 MB |
| 7 | two clusters | 56 s | 2,697 | 56.2 + 56.0 MB | 163 + 31 MB |

- **Enrolment takes ~50–60 s per 150,000-card user** through the real write path on this host,
  and 4.5× that while a split is copying on the same machine.
- **The reputation documents do not grow with users: their CELLS do.** Every user's service
  scores the same ~160 k subjects, so each one adds a cell per subject, not a document. Allocated
  memory moves non-monotonically (49.7 → 53.3 → 65.3 → 64.3 → 58.0 MB) because Vespa compacts in
  the background. The steadier measure is the probe's ~21 B per cell per tensor pair (below),
  about 3 MB per 150 k-card user per node.
- **The transaction log grows ~13–20 MB per user and was never pruned** during the run (21 →
  163 MB on the source), while document disk stayed flat (10.1 MB until the split). That matches
  the trust-size probe and is the leading explanation for production's 38 GiB of TA disk against
  4.4 GiB of memory. The store's telemetry now splits the two per document type, so production
  can confirm it.

### The split

| step | time | documents |
|---|---|---|
| stores restart on the overlap map | 0.2 s | |
| overlap package deploys | 20 s | |
| copy | 291 s | 403,573 events, 6,254 reputation documents |
| check and repair, 2 passes | 494 s | **74,977 reputation documents repaired**, then clean |
| stores restart on the final map | 0.1 s | |
| final package deploys | 3.6 s | |
| cleanup | 262 s | 388,289 events, 81,231 reputation documents removed from the source |

About 18 minutes for ~490 k documents on one shared host.

**The repair, not the copy, moved the trust data.** User 5 enrolled while the copy ran. Its cell
updates reached both clusters and CREATED partial reputation documents in the new one before the
copy got there. The copy writes only where a document is absent, so it skipped them, and the
repair replaced all 74,977 with the source's complete documents. That is correct, and it is what
step 3 exists for. But under enrolment the cost of copying TAs sits in `diff`, which holds a
batch's destination in memory and reads each candidate back. Size the move window for that, or
pause enrolment during the copy.

### Reads

| REQ class (p50 / p95 ms) | one cluster (users 1–4) | during the split | two clusters (users 6–7) |
|---|---|---|---|
| global kind 1, limit 50 | 21–34 / 72–94 | 35 / 101 | 34–36 / 111–129 |
| one author | 12–17 / 43–58 | 16 / 63 | 13 / 53–60 |
| hashtag | 13–15 / 39–53 | 21 / 90 | 24–26 / 98–105 |
| lensed feed (a real user's trust) | 20–25 / 52–81 | 37 / 242 | **62–73 / 176–187** |
| lensed search | 172–264 / 306–396 | 297 / 521 | 176–189 / 293–311 |
| COUNT, last hour | 12–18 / 37–47 | 20 / 47 | 12–13 / 51–52 |

- On one host, two clusters made the lensed feed about **3× slower** and hashtag feeds about
  2×. This is the over-sharding cost measured in `docs/author-shards.md`: each cluster finds its
  own top 50 under the gate, on the same cores.
- One-author reads, COUNT and lensed search held. On separate hosts the clusters stop sharing
  cores, which this run cannot show.
- During the split, 7 reads failed (6 lensed, 1 global; 1 had failed before it). The failure
  type was not recorded. The likeliest cause is the simulation's own restarts, which close the
  store under reads in flight, as a process stopping in a rolling restart would.

### Memory after the split

Reputation documents split evenly: 81,145 and 81,231. Allocated memory did not halve. Each
cluster's reputation document type held ~50 MB, against ~58 MB for the single cluster before,
and nothing changed over 10 minutes of sampling. Per attribute (each node's
`/state/v1/custom/component/documentdb/reputation/subdb/ready/attribute`):

| cluster | `influence_scores` | `follower_counts` | `pubkey` |
|---|---|---|---|
| `content` (source) | 5.6 MB | 7.6 MB | 11.7 MB |
| `content1` (new) | 5.3 MB | 7.1 MB | 11.6 MB |

**The trust tensors did split**: 25.7 MB across the two for ~1.2 M cells, near-equal halves. The
rest of each ~50 MB is FIXED per document type per cluster (the `pubkey` attribute, document
metadata, caches), and at this slice's size it hides the saving. At production's 4.4 GiB per
node the tensors dominate and the split should show. That is an expectation, not a measurement,
and the first production split should confirm it from this per-attribute view, not from the
allocated total.

### What it found for consumers

**vespa-relay's `distinct(field)` statistic is wrong on two clusters for any field whose values
span clusters.**
- `all(group(kind) output(count()))` returned 8 on one cluster and **16** on two (8 kinds). Each
  cluster counts its own distinct values, and the merge adds them.
- For `pubkey` the count disagrees with the number of groups on BOTH topologies (56,152 against
  54,042 on one cluster, 55,167 against 54,048 on two). Vespa estimates large distinct counts,
  so that one is not the split.
- A sharded store should refuse a group-level `count()` the way it refuses `max(n)`, or
  vespa-relay should count `countsBy` groups instead. Not fixed yet.

## What this does not show

- Separate hosts. Every cluster shared four cores, so the read costs above are worst-case for
  the topology and the timings are for this host.
- Production scale. 202 k events and ~1.2 M trust cells, against production's ~945 M events and
  4.4 GiB of TAs per node. The move tool's throughput here (~1,400 events/s copied) would need
  days at production size.
- Long-running behavior: transaction-log pruning, lid-space compaction on the source, and
  memory over hours rather than 10 minutes.

