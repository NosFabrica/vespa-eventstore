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

RESULTS_PENDING
