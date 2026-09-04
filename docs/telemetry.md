# Telemetry — what the store spends, and what measuring it costs

**Status: PROPOSAL** (2026-09-04). Nothing here is built. `IngestStats` is what
exists; §7 says how it decomposes into this rather than being replaced by it.

This document is in two halves, for the same reason `attribute-memory.md` is.
First **where the model should come from** (§§1–4) — the argument that telemetry
belongs at the seams the architecture already has, rather than at hand-placed
call sites. Second **what it costs to run** (§§5–6), measured, because a library
that ships always-on instrumentation owes its embedder that number before it
ships it. §§7–9 say what it replaces, what was rejected, and what it leaves out.

**§10 is the implementer's index**: every quantity an operator dashboard shows,
which altitude produces it, and the two additions to the model that auditing a
mocked page against §§1–9 turned up. **§11 turns a latency model into a resource
model** — denominators, bounded heavy hitters, and the one causal edge worth
having — and is honest about the part no counter can answer. Start at §10 if you
are building rather than reviewing.

## 1. The gap

The store can say a great deal about a write and nothing at all about a read.

| path | instrumented? |
| --- | --- |
| ingest stages (`dedup`, `guards`, `versions`, `supersede`, `write`) | yes — `IngestStats` |
| writer-lock wait / hold, and who holds it now | yes — `IngestStats`, `heldNow()` |
| trust projection (`proj.fetch.derive`, `proj.fetch.maxrank`, `proj.write`) | yes |
| **REQ / COUNT / NIP-50 search** | **no** |
| **round trips to the engine** | **no** |
| **engine-side cost: docs matched, coverage, rank profile** | **no** |

On a relay the reads *are* the load, and the slowest thing this store serves is
a ranked search over a common word (`bitcoin` 2.7–4.4 s, `nostr` 19.2 s on the
production relay — `benchmark/README.md`). None of it is visible from inside the
library today.

## 2. What is actually scarce

`IngestStats` measures wall time, and only wall time. But the performance
arguments already written down in this repo are conducted in three different
currencies, and conflating them is why the flat stage map has to be read so
carefully:

| resource | why it is the binding constraint | measured today |
| --- | --- | --- |
| **serialized time** in a critical section | every write queues behind one mutex; a slow holder stalls all ingest | yes — this is all `IngestStats` does |
| **round trips** to the engine | `batchInsert` exists *because* of this ("~47× fewer round trips"; "never ingest in a loop over `insert()`") | **no** |
| **engine work** — docs matched, coverage, rank profile | the ranked-search cost that dominates a relay's CPU | **no** |

Round trips are this codebase's native unit of cost. The bulk-insert design is
justified in them, `benchmark/README.md` reports them per event, and CLAUDE.md
states a rule in them. There is no counter for them anywhere in the library.
That absence says more about the shape of the current model than any complaint
about its data structures: the model measures what was easy to time from inside
the write path, not what the store is known to be short of.

## 3. Measure at the seams that already exist

### 3.1 The port decorator

`TrustProjection` is a decorator on `EventIndex`, and its KDoc states exactly
why that placement was chosen: it observes `put`/`remove` at the seam, so
**every** deletion style — supersession, kind-5, NIP-62 vanish, the orphan sweep
— updates trust tensors *with zero deletion-specific code*.

That argument transfers verbatim to metering. A `MeteredEventIndex` at the same
seam sees every `get`, `put`, `search`, `count` and `visit`, by whatever route
reached it, with **no instrumentation at any call site**. The stage list stops
being a hand-maintained artifact that drifts from the code and becomes a
consequence of the architecture: a new read path is counted the day it is
written, because it must go through the port to reach the engine at all.

This is the whole design. Everything below is detail.

### 3.2 The caller's intent, carried ambiently

A decorator at the port sees `search(query)`; it does not see that this
particular search is the dedup probe inside a `batchInsert`. That second
dimension has to come from the caller, and it must not be threaded through
twenty signatures to get there.

Quartz's `StoreQueryContext` already rides `coroutineContext` through
`NostrSemanticsStore` to carry the observer into `query()`, `count()` and
`rawQuery()`. The same mechanism carries the same kind of thing — the caller's
intent, ambient, set once. An `Activity` element names the operation in the
store's own vocabulary, as a closed set:

```
Insert  BatchInsert  Query  Count  Delete  Snapshot
Drain   Reconcile    Sweep  GuardRefresh   Backfill
```

Set at each public entry point, read by the decorator. Nothing in between has to
know it exists.

### 3.3 Composition

```kotlin
// today
NostrSemanticsStore(TrustProjection(VespaEventIndex, VespaReputationIndex))

// metered at two depths: the outer counts what the STORE asked for, the
// inner what actually reached the engine. Their difference is the trust
// projection's own traffic — a number nothing can show today.
NostrSemanticsStore(
    Metered(STORE, TrustProjection(Metered(ENGINE, VespaEventIndex), reputations)),
)
```

The two-depth placement is not a trick to be clever with. It answers a question
that has come up twice already in this repo's history — how much of the engine's
load the projection adds — and no restructuring of a flat stage map can produce
it, because the flat map has no notion of *which layer asked*.

### 3.4 Critical sections nest; the model must too

The locks stay explicitly instrumented, because contention is not a port call.
But they become **scopes that stack**, not flat counters. Today they do not, and
the work runs inside them:

```
lock.ingest.hold  ⊃  lock.ingest.trust.{wait,hold}  ⊃  dedup, guards, versions, supersede, write
lock.gate.hold    ⊃  proj.fetch.derive, proj.write
```

Three consequences of getting this right:

- **Totals stop double-counting.** `dump()` currently sorts container and
  contents into one list, so `lock.*.hold` sorts to the top and reads as the
  biggest "stage" when it is the thing the others happen inside.
- **Unaccounted time becomes computable**: `hold` minus the sum of its timed
  children — the gate held doing something nobody instrumented. That is the
  signal that finds the *next* `proj.fetch`, instead of discovering after 24
  minutes of held gate that two call sites shared a name.
- **`heldNow()` becomes correct.** See §7.2 — it currently is not.

### 3.5 The honest limit

A port decorator sees `EventQuery`, not the rank profile: the profile is chosen
when `EventYql` compiles the query, and `totalCount` and coverage live in the
response. Engine-level detail must therefore be published from *inside*
`VespaEventIndex`.

That is the correct split rather than a compromise. Port-level metering is
engine-agnostic and works for `InMemoryEventIndex`, which has no rank profiles
at all; engine-level detail is Vespa's and belongs with the client that speaks
to it.

## 4. The model

Two dimensions and a containment relation:

- **what** — the port call (`get`, `put`, `search`, `count`, `visit`)
- **who** — the `Activity` it happened under
- **containment** — which lock scope it ran inside

Two things do not fit this shape and are not counters at the port: the store's
own admission **outcomes** (`duplicate:`, `replaced:`, `blocked:` — decided
above the port, so a refused event never reaches it) and **gauges** (queue
depth, in-flight, current lock holder — instantaneous, with no cumulative
form). Both are part of the model; §10.1 defines them.

Everything the current dotted-string convention encodes falls out as a
dimension. `proj.fetch.derive` is `search` under `Drain`; `dedup` is
`existingIds` under `BatchInsert`. The name-splitting treadmill — one `timed()`
label per distinguishable caller — stops, because the distinction is now a
dimension instead of a substring.

**Cardinality is closed, and must stay closed.** Activities and port calls are
both fixed sets; their product is ~11 × 6 = 66 slots, plus the lock scopes.
Every memory figure in §6 depends on that. Keying a counter by search term,
observer pubkey, filter shape or document id turns a fixed ~100 KiB (§11.5)
into an unbounded leak. §11.2 is how those dimensions get answered anyway,
in bounded memory, without becoming keys. This is the one invariant of the design that a reviewer should
enforce without negotiation.

## 5. What it costs — CPU

**Measured 2026-09-04**, 4 cores, OpenJDK 21.0.10, `tsc` clocksource, 20M
ops/round, median of 7 rounds after 3 warm-ups. Harness in §5.3.

| primitive | uncontended | 2 threads | 4 threads | 8 threads |
| --- | ---: | ---: | ---: | ---: |
| `System.nanoTime()` ×1 | 22.4 ns | | | |
| **nanoTime pair** (timing one interval) | **47–54 ns** | | | |
| `System.currentTimeMillis()` ×1 | 24.1 ns | | | |
| `AtomicLong.addAndGet` | 5.7–7.7 ns | 31–35 | **29–37** | 31–43 |
| `LongAdder.add` | 9.8–10.0 ns | 5.3 | **2.4** | 2.5–2.6 |
| `AtomicLongArray` histogram record | 5.9 ns | | 2.5 | |
| `ConcurrentHashMap.get` (interned key) | 3.1–5.8 ns | | | |
| **full per-request instrumentation** | **96–97 ns** | | 24–26 | |

Ranges are two runs on the same box — one fuller harness and the condensed §5.3
one, which covers a subset of the rows. Where they differ, both ends are given:
this is the ±8–15 % run-to-run noise the rest of the repo's benchmarks report on
this class of machine, and none of it moves a conclusion. Treat the **ratios**
as the result, not the absolute figures; re-run §5.3 on your own hardware before
using any of these as a planning number.

The full case is the realistic budget for one `/search/`: a nanoTime pair, one
map lookup, three counters, one histogram record. Two thirds of it is the clock,
not the counters.

Read the 4-thread figure correctly: it is wall time per op divided across four
threads, so it means ~26 ns of *one core's* time per request. The latency added
to each individual request stays ~100 ns.

**Against what it measures:**

- **Reads.** A Vespa round trip is milliseconds. 97 ns is ~0.01 % of a 1 ms
  admission probe and ~0.002 % of a 4 s ranked search. At staging's ~500 req/s
  of the store's own reads that is 0.005 % of one core; at 30k req/s, 0.3 %.
- **Writes.** `SearchExtractors.extract` alone costs **9,527 ns/event**
  (measured 2026-09-01, `benchmark/README.md`). Metering is per port call, not
  per event, so it disappears — and even charged once per event it is ~1 % of
  the derivation the write path already pays.

### 5.1 `LongAdder`, not `AtomicLong`, on the read path

This is the one measurement that changes a decision. `AtomicLong` is *faster
uncontended* (~6–8 ns vs ~10) and **stays flat-to-worse as threads are added**
(~30–37 ns at four, up to 43 at eight) because every thread CASes one cache
line. `LongAdder` goes the other way — 2.4 ns at four threads, a ~12× gap —
because it stripes. That ordering held across both runs; only the absolute
figures moved.

For the write path this is irrelevant and `IngestStats`'s existing `AtomicLong`
is the right call: two volatile writes against critical sections measured in
seconds. For the read path, where many container threads book concurrently at
500–30,000 req/s, it is the difference between free and a contended cache line
on the hot path. **Read-side counters are `LongAdder`; the write path keeps what
it has.**

### 5.2 The clock is host-dependent

22 ns here because this box's clocksource is `tsc`, where `nanoTime` is a vDSO
read. On a VM fallen back to `xen` or `hpet` it becomes a syscall in the
0.5–1.5 µs range, so a timing pair costs ~2–3 µs — a ~30× swing. Still under 1 %
of a millisecond round trip, so it does not change the verdict, but an operator
diagnosing unexpected overhead should check it first:

```bash
cat /sys/devices/system/clocksource/clocksource0/current_clocksource
```

### 5.3 Reproducing §5

Single-file source launch, no build wiring, JDK 11+:

```bash
java MetricCost.java     # the harness below, saved under that name
```

```java
// MetricCost.java — median ns/op over 7 rounds of 20M ops, after 3 warm-ups.
// Sinks are accumulated and printed so C2 cannot delete the work.
import java.util.concurrent.*; import java.util.concurrent.atomic.*; import java.util.*;
public class MetricCost {
    static final int OPS = 20_000_000, ROUNDS = 7, WARMUP = 3;
    static long sink; static final AtomicLong ATOMIC = new AtomicLong();
    static final LongAdder ADDER = new LongAdder();
    static final LongAdder[] HIST = new LongAdder[24];
    static final ConcurrentHashMap<String, long[]> MAP = new ConcurrentHashMap<>();
    static final String[] KEYS = {"search","recency_gated","recency","unranked",
                                  "spliced_member","sort_followers","text","text2"};
    static { for (int i=0;i<24;i++) HIST[i]=new LongAdder();
             for (String k: KEYS) MAP.put(k, new long[8]); }
    static int bucket(long n){ return Math.min(63-Long.numberOfLeadingZeros(Math.max(n,1)),23); }
    interface Op { void run(int i); }
    static void bench(String name, Op op) {
        double[] t = new double[ROUNDS];
        for (int r=0;r<WARMUP+ROUNDS;r++){ long t0=System.nanoTime();
            for (int i=0;i<OPS;i++) op.run(i);
            if (r>=WARMUP) t[r-WARMUP]=(double)(System.nanoTime()-t0)/OPS; }
        Arrays.sort(t); System.out.printf("  %-44s %7.2f ns/op%n", name, t[t.length/2]);
    }
    static void threaded(String name, int threads, Op op) throws Exception {
        int per = OPS/threads; double[] t = new double[ROUNDS];
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int r=0;r<WARMUP+ROUNDS;r++){
            CountDownLatch go=new CountDownLatch(1), done=new CountDownLatch(threads);
            for (int i=0;i<threads;i++) pool.submit(() -> { try { go.await();
                for (int j=0;j<per;j++) op.run(j); } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); } finally { done.countDown(); } });
            long t0=System.nanoTime(); go.countDown(); done.await();
            if (r>=WARMUP) t[r-WARMUP]=(double)(System.nanoTime()-t0)/(per*(long)threads); }
        pool.shutdown(); Arrays.sort(t);
        System.out.printf("  %-44s %7.2f ns/op  (%d threads)%n", name, t[t.length/2], threads);
    }
    public static void main(String[] a) throws Exception {
        bench("nanoTime pair", i -> { long t0=System.nanoTime(); sink+=System.nanoTime()-t0; });
        bench("AtomicLong.addAndGet", i -> ATOMIC.addAndGet(1));
        bench("LongAdder.add", i -> ADDER.add(1));
        bench("ConcurrentHashMap.get", i -> { sink += MAP.get(KEYS[i&7])[0]; });
        for (int t : new int[]{2,4,8}) {
            threaded("AtomicLong.addAndGet", t, i -> ATOMIC.addAndGet(1));
            threaded("LongAdder.add", t, i -> ADDER.add(1));
        }
        Op full = i -> { long t0=System.nanoTime(); long[] s=MAP.get(KEYS[i&7]);
            long e=System.nanoTime()-t0; ADDER.add(1); ADDER.add(e); ADDER.add(s[0]+1);
            HIST[bucket(e)].add(1); };
        bench("full per-request instrumentation", full);
        threaded("full per-request instrumentation", 4, full);
        System.out.println("sink=" + sink + " " + ADDER.sum());
    }
}
```

## 6. What it costs — memory

**Measured 2026-09-04**, same box. Cardinality as §4: 28 keyed slots (11 rank
profiles + 7 activity classes + 10 ingest stages), each with 6 scalar counters
and a 24-bucket latency histogram. Figures are **per whole store**, measured as
a heap delta over 400 replicas to keep GC noise off the number, before and after
forcing contention.

| design | fresh | after contention |
| --- | ---: | ---: |
| A — `LongAdder` everywhere | 31.4 KiB | 31.5 KiB |
| **B — `LongAdder` scalars + `AtomicLongArray` histograms** | **13.3 KiB** | **13.4 KiB** |
| C — `AtomicLongArray` everywhere | 9.1 KiB | 9.1 KiB |
| slow-query ring, 256 distinct records | 45.6 KiB | |

**Ship design B.** It buys `LongAdder`'s contention behaviour where the counters
are hot and scalar, and pays `AtomicLongArray`'s smaller footprint for the
histograms, whose 24 buckets already spread contention across three cache lines
without striping.

Total steady state is **~60 KiB**, and it does not grow with traffic: counters
are fixed-width and the ring is bounded. Against a 1536m default container heap
and proton's 75 GiB resident at 176.7M documents
(`docs/attribute-memory.md`), it is noise.

Displaying an actionable p99 needs finer buckets than the 24 measured here,
which raises this to **~80 KiB** — see §10.3 for the arithmetic and why the
recording cost is unchanged.

### 6.1 `LongAdder` inflation scales with the host, not the traffic

A `LongAdder` is one word until threads collide, then it allocates a padded cell
array. Measured on this 4-core box:

| state | bytes each |
| --- | ---: |
| never contended | 36.1 |
| after heavy contention | 92.8 |

The ceiling is a function of **core count**, not request rate: the cell array
grows toward the next power of two ≥ `ncpu`, at ~128 B per padded cell. On a
64-core relay host a genuinely hot adder can reach several KiB. Two consequences
for the design:

- It is why the histograms are `AtomicLongArray` in design B. 24 `LongAdder`s
  per slot on a big host is the one way this budget stops being noise.
- Only *contended* adders inflate, so the realistic figure stays near the
  measured one. But size the budget from core count when planning for a large
  container, not from the 4-core number above.

### 6.2 What is bounded, and by what

| structure | bound | enforced by |
| --- | --- | --- |
| counters | activities × port calls, fixed at compile time | the closed `Activity` set (§4) |
| histograms | 24 buckets, log-spaced | fixed array |
| slow-query ring | 256 entries, overwriting | ring buffer, not a list |
| live lock holder | one per lock scope, popped on release | scope stack (§7.2) |

## 7. What this replaces

### 7.1 `IngestStats` decomposes rather than dies

Every stage name it books re-expresses as either a lock scope or a port call
under an activity:

| today | becomes |
| --- | --- |
| `dedup` | `existingIds` under `BatchInsert` |
| `guards` | the NIP-09/62 guard probes under `BatchInsert` — a `GuardOwners` bloom check plus whatever queries it does not eliminate, so metering it at the port also shows how many probes the cache actually saved |
| `write` | `putAll` under `BatchInsert` |
| `proj.fetch.derive` | `search` under `Drain` |
| `proj.fetch.maxrank` | `search` under `Backfill` |
| `proj.write` | `updateCells` under `Drain` |
| `lock.*.wait` / `lock.*.hold` | lock scopes, stacked, with unaccounted time |

Its structural debts go with it. Four `ConcurrentHashMap`s keyed by the same
string (`stages`, `calls`, `maxima`, `lastSeen`) become one map to one record —
three `computeIfAbsent` lookups per `timed()` call collapse to one, and
`snapshot()` stops reading three maps at three instants and reporting a
`meanNanos` whose numerator and denominator may not correspond. `statusLine()`
is destructive (it consumes the per-stage delta via `lastSeen.put`, so two
callers corrupt each other — `WriterLockStatsTest` documents working around it)
and is superseded by callers diffing two snapshots.

The name also stops lying. `IngestStats` books `lock.gate.hold` and
`proj.fetch.*` and holds the live lock holder; none of that is ingest.

### 7.2 A defect this fixes

`IngestStats.held` is a single `@Volatile` field with non-stacking begin/end,
justified in its KDoc as *"One field because the store serialises every write
behind ONE mutex, so there is never more than one holder."*

That was true when it was written (`77bed35`). The trust-gate split (`1a0ddec`)
lands **after** it in the history and makes it false — `lockedForWrite` now
nests two mutexes:

```kotlin
locked(LOCK_INGEST) {
    if (touchesTrust(event)) lockedOn(trustGate, LOCK_INGEST_TRUST) { body() } else body()
}
```

The inner `beginHold` overwrites the outer's `Held`, and the inner `endHold`
sets the field to `null` — so the outer lock reports as *not held* while it is
still held, and any later `annotateHold` no-ops on the null. In today's call
shapes the inner block is the tail of the outer body, so the window is narrow
and nothing has been observed to mislead. It is still a field that no longer
models the system it describes, and it will report wrongly the moment work is
added after the inner block. A scope stack removes the failure mode rather than
the symptom.

### 7.3 A performance contract that becomes executable

`InMemoryEventIndex` is described in CLAUDE.md as the **executable
specification** of `EventQuery` semantics. Metering it makes it the executable
specification of *cost* as well: with round trips counted at the port,
CLAUDE.md's rule — "Never ingest in a loop over `insert()`" — becomes a unit test
asserting round-trips-per-event for `batchInsert`, with no Vespa and no Docker.

Today that rule is a comment, and the ~47× claim behind it is a benchmark
memory. It should be a gate.

## 8. Studied and rejected

- **A tracing library (OpenTelemetry, Micrometer).** Gives containment and
  attribution for free and is the textbook answer. Rejected for the reason
  `BackgroundFailures` already states about logging: *a library has no business
  picking a framework for its embedder*. This store is published to Maven
  Central and embedded in a relay; a transitive tracing dependency is the
  embedder's decision. The snapshot type is a plain value — an embedder who
  wants OpenTelemetry can bridge it in ten lines.
- **Keeping per-call-site timers (the status quo, extended to reads).** Requires
  a hand-placed `timed()` at every read path, which drifts the moment someone
  adds one, and pays the name-splitting treadmill forever: every new
  distinguishable caller is a new string. The decorator gets both properties
  structurally.
- **Sampling (time 1 in N requests).** The standard way to make instrumentation
  free. Rejected because the thing being hunted here is *the pathological call*
  — one 24-minute hold among ordinary ones — and sampling is precisely the
  technique that loses it. At 97 ns there is nothing to buy.
- **A Prometheus client dependency.** Same argument as tracing. A text-format
  renderer over the snapshot is ~100 lines with no dependency, and an embedder
  wanting the real client can feed it the snapshot.
- **Per-store instances instead of process-wide statics.** Correct in principle,
  and it would fix the test-isolation problem `IngestStats.reset()` exists for.
  Deferred, not rejected: the ambient `Activity` element and the port decorators
  are already per-store, so only the lock scopes remain global, and moving them
  is a smaller change once the rest is in place.

## 9. What this does not measure

- **Vespa's own resource use** — memory and disk against the feed-block limits,
  per-field attribute memory, transaction-log size, per-rank-profile latency
  from proton. That comes from each node's metrics proxy on `:19092`, is a
  separate reader, and needs an integration test because the metric names only
  exist on a real Vespa. `benchmark/attribute_memory.py` already consumes that
  endpoint and is the model for it.
- **The size of that metrics payload.** Not measured here; it is one
  `curl … | wc -c` against a live deployment, and it decides whether the scrape
  interval can be 15 s or must be longer.
- **Anything per-observer or per-term** *as a counter key*. Deliberately — see
  the cardinality invariant in §4. The slow-query ring (§10.5) is the one place
  a term is retained at all, and it is a bounded sample rather than a key: its
  size is fixed by the ring, not by how many distinct terms exist.

## 10. What the operator page needs

An operator dashboard ("Eventstore Pulse") was mocked before this document, and
auditing one against the other is what produced this section: §§1–9 describe the
*architecture* well and leave several of the page's actual *quantities*
unnamed. Two are gaps in the model itself; the rest are line items the model can
carry once someone says so. This section closes both, so an implementer works
from one document rather than a document plus a screenshot.

### 10.1 Two additions to the model

**Outcomes are a third altitude.** §3 has two — the port decorator and the Vespa
client — and neither can see a *refused* event. `duplicate:`, `replaced:` and
`blocked:` are decided in `NostrSemanticsStore.insertLocked`, above the port,
and an event rejected there never reaches `EventIndex` at all. The decorator
sees the `existingIds` probe and whatever `putAll` followed; inferring the
rejection from the gap between them is fragile and breaks the first time a batch
path changes.

So the store books its own outcomes where the decision is made. The key space is
already closed, which is what makes this safe under §4 — Quartz's
`RejectionReason` (`EXPIRED`, `DUPLICATE`, `DELETED`, `VANISHED`, `REPLACED`,
`INSERT_FAILED`) plus this store's `UNSTORABLE_TEXT`, and `admitted`. Counted
per `Activity`, so a mirror's duplicate rate and a live relay's are separable.

This altitude is worth more than the page that prompted it. "81 % of what this
node is offered is already stored" is the number that tells an operator to
narrow a sync, and nothing in the store can currently say it.

**Gauges are a metric kind, not a counter.** Everything in §§1–9 is cumulative
and diffable. Three of the page's most useful figures are not, because they have
no meaningful cumulative form:

| gauge | owner | why it cannot be a counter |
| --- | --- | --- |
| pending trust work (subjects, services) | `DirtLedger` | a queue depth; "total ever queued" answers nothing |
| feed operations in flight | `VespaFeed` (`client.stats()`) | an instantaneous window |
| who holds the write lock, and for how long | lock scope stack (§3.4) | already a gauge — `heldNow()` |

Gauges are **pulled at snapshot time**, so they cost nothing until read. Two
rules follow. They are never diffed between snapshots — a consumer that treats a
queue depth as a rate gets nonsense. And the owner must expose the value
*safely*: `DirtLedger.pending` is a plain `private var` today, so the metrics
layer must not reach into it — the ledger publishes a count through a volatile
read or its own atomic, or the gauge is a data race.

### 10.2 Every quantity the page shows, and where it comes from

Altitudes: **P** port decorator · **E** Vespa client · **O** store outcomes
(§10.1) · **G** gauge (§10.1) · **X** existing code · **M** metrics proxy,
deferred (§9).

| page region | quantity | where | note |
| --- | --- | --- | --- |
| health strip | REQ / COUNT / search rate | P | per `Activity` |
| | ingest rate | P | `putAll` under `BatchInsert` |
| | admitted / duplicate / replaced | **O** | §10.1 |
| | ranked-search p99 | P | §10.3 |
| | feed in-flight, latency, retries | X + **G** | `VespaFeed.statusLine()` exists |
| | degraded responses | E | `SearchCoverage` already parsed |
| | pending trust work | **G** | §10.1 |
| | content memory vs. feed-block limit | M | separate reader |
| hero | engine time per rank profile | **E** | §10.4 |
| reads table | queries, rate, engine share | P + E | |
| | p50 / p99 | P | §10.3 |
| | docs matched per query | **E** | `SearchRootFields.totalCount`, already parsed — free |
| | trust-descent rungs | **E** | count rungs walked per search in `TrustDescent` |
| | degraded count | E | |
| ingest panel | stage split, calls, mean, max | P | §7.1 |
| | writer lock wait / hold / unaccounted | P | §3.4 |
| engine resources | memory, disk, tlog, doc counts | M | separate reader |
| attribute memory | per-field resident bytes | M | `benchmark/attribute_memory.py` is the model |
| background workers | failures, consecutive, last message | X | `BackgroundFailures` exists |
| | drain cycle duration | P | `Drain` activity |
| slow queries | the log itself | **E** | §10.5 |

Two panels — engine resources and attribute memory — are **M** throughout. They
need the metrics-proxy reader §9 defers, and no amount of store-side work
produces them. An implementer should expect the page to land in two stages, and
the first stage is worth shipping without them.

### 10.3 Percentiles, and what that costs

§6 budgets a 24-bucket log2 histogram. That is fine for a *distribution* and too
coarse for a **p99 anyone will act on**: adjacent octaves are 2× apart, so a 4 s
search reports as "between 2.1 s and 4.2 s".

Percentiles worth displaying need sub-buckets. **8 linear sub-buckets per
octave** bounds the relative error of a reported percentile at **1/(2·8) ≈ 6 %**,
which is well inside the run-to-run noise of the thing being measured. Covering
1 µs to 16 s is 24 octaves, so **192 buckets** per histogram.

Histograms go on **read slots only** — roughly 15 (the rank profiles plus a few
activity classes). Ingest stages do not need percentiles; total, calls and max
already answer the question they are asked (§7.1).

```
15 slots × 192 buckets × 8 B  ≈  22.5 KiB
```

That replaces the 5.4 KiB the 24-bucket design implied, so **§6's ~60 KiB
becomes ~80 KiB**. Still flat in traffic, still noise against a 1536m heap. The
recording cost is unchanged from §5 — the bucket index is one
`numberOfLeadingZeros` plus a shift and an add either way.

### 10.4 Engine time per request

The hero panel splits engine time by rank profile, and the store cannot compute
that: it knows wall time around the round trip, which includes the network and
the client's own JSON parse.

Vespa will report its own split if the query asks, via `presentation.timing`.
That gives query time apart from summary-fetch time — the difference between
"the match phase is expensive" and "we asked for 2,500 summaries", which is a
different fix in each case and is invisible from the client side.

**Unverified against a live Vespa.** The exact field names and their meaning
must be pinned by an integration test before anything renders them, for the
reason the rest of this repo pins engine behaviour that way: only a real Vespa
executes it, and `MockVespaEngine` will happily return whatever shape the test
author imagined. Whether the flag costs the engine anything measurable is the
same question and the same test.

### 10.5 The slow-query log

The one place this design retains a query string, and therefore the one place
that needs an explicit privacy answer.

- **Threshold, not sampling.** Reads slower than a configured wall time are
  captured; everything else costs one comparison. §8 rejects sampling for the
  counters, and the same argument applies here in reverse — the pathological
  query is the entire point, so capture is triggered by the property that makes
  it interesting.
- **Bounded by the ring, not by the term space.** 256 entries, overwriting.
  This is what keeps it inside the §4 cardinality invariant: the term is
  retained as part of a bounded *sample*, never used as a counter key. §9 says
  the same from the other direction.
- **What is captured**: timestamp, rank profile, wall / engine / summary-fetch
  time, hits served, docs matched, descent rungs, coverage verdict, and the
  query's terms.
- **Observer keys are truncated.** A NIP-50 `observer:` term is a pubkey — it
  identifies a person. Truncate it to a prefix, enough to correlate two slow
  queries from one lens and not enough to be a log of who searched for what.
- **Off unless a threshold is set.** An operator opting in to retaining user
  queries should have to say so, and the default should not decide that for
  them.

### 10.6 What the library will not keep

The page has a 60 s / 15 min / 1 h selector. The library provides none of that,
deliberately: it exposes **cumulative counters and instantaneous gauges**, and a
window is the difference between two snapshots.

Retaining history is the consumer's job — Prometheus, or a ring of snapshots in
the relay. Keeping a time series inside the store would put an unbounded,
retention-policy-shaped decision inside a library whose entire memory budget
(§6, §10.3) rests on being flat in traffic. It is the same argument §8 makes for
not choosing a tracing framework: the embedder owns that, and the snapshot is a
plain value they can feed to whatever they already run.

## 11. From a latency model to a resource model

§10 made an operator page buildable. It did not make the model a good answer to
*"where are my resources going"*, which is a different question: §§1–10 measure
**elapsed time**, and elapsed time around a network round trip is mostly
waiting, not consuming. Three additions close most of that distance. The fourth
thing — actual CPU — cannot be closed by counters at all, and §11.4 says so
rather than implying otherwise.

### 11.1 Denominators, and the rule about ratios

A total does not normalize. Twenty-four minutes of held gate means nothing until
you know how many events went through it, and this repo already reasons in
ratios everywhere — 456 B/doc, 9,527 ns/event, round trips per event, 3.2 GB/h.

**The rule: expose counters, never ratios.** A pre-divided rate cannot be
re-windowed. To get a ratio over any window you must sum the numerator and the
denominator separately and divide once, at the end; a library that divides
first has destroyed the information needed to do that, and averaging its
per-scrape ratios gives a subtly wrong answer that looks plausible. So the
snapshot carries denominators as ordinary counters beside the costs, and the
consumer divides.

The denominators worth carrying, all of which the model already has or gains in
§10.1:

| denominator | where it comes from |
| --- | --- |
| events offered / admitted | outcomes altitude (§10.1) |
| queries served | port decorator, per `Activity` |
| hits served | port decorator (page size actually returned) |
| round trips | port decorator |
| documents in the corpus | engine, for the per-doc figures `attribute-memory.md` uses |

**Alignment is the constraint that makes them usable.** A numerator and its
denominator must be counted at the same altitude and under the same `Activity`,
or the quotient is nonsense — round-trips-per-event needs both the round trips
and the admitted events booked under the *same* `BatchInsert`, not a global
total of each. This is the part an implementer gets wrong by default, because
each counter looks correct on its own.

The canonical denominator differs per cost family, and naming it is what makes a
panel readable rather than merely populated:

| cost | read it per |
| --- | --- |
| ingest stage time, lock hold | event admitted |
| round trips | event admitted (the "~47×" claim, watchable in production) |
| engine time, docs matched | query served, and hit served |
| trust projection time | subject settled |
| attribute memory | document (as `attribute-memory.md` already does) |

### 11.2 Heavy hitters: the forbidden dimension, bounded

§4 forbids keying counters by observer or search term, and is right to — the key
space is unbounded. But *"which observer or term is costing me most"* is the
resource question a multi-tenant relay operator most wants answered, and
refusing it outright is a gap rather than an answer.

A **weighted Space-Saving sketch** answers it in fixed memory. Capacity `m`
entries; anything holding more than `1/m` of total weight is guaranteed to be
present, and every entry carries its own error bound, so the summary can say how
much it might be overstating. Weighted by **cost** — engine milliseconds, or
docs matched — not by frequency: the question is who is expensive, not who is
chatty, and one `nostr`-shaped query outweighs a thousand id lookups.

**Measured 2026-09-04**, same box, 64-hex (pubkey-shaped) keys, 5M ops/round,
median of 5 rounds after 2 warm-ups:

| traffic shape | K=64 | K=256 |
| --- | ---: | ---: |
| skewed — 80 % of weight from 20 heavy keys (realistic) | **62.9 ns** | 135.0 ns |
| churning — 1M distinct keys, nearly every add evicts (worst case) | **159.5 ns** | 601.3 ns |
| retained memory per sketch | **7.7 KiB** | 45.2 KiB |

**Ship K=64.** The jump to 601 ns at K=256 under churn is the O(m) minimum scan
on eviction, so `K` is not a free knob — it is quadratic-ish in the case that
matters least and costs the most. K=64 tracks the twenty-odd heavy observers a
relay actually has, at a cost comparable to the ~97 ns of §5.

Two properties make even the worst case irrelevant here:

- **It only sits on the search path.** Only a read carrying an observer or terms
  feeds a sketch; id lookups, dedup probes and guard checks never touch it. That
  is the low-frequency, high-cost path — hundreds of queries per second, not
  millions.
- **Concurrency can therefore be a plain lock.** The measurements above are
  single-threaded throughput in the millions of ops/second, four orders of
  magnitude above real search traffic, so a `synchronized` sketch will never be
  the contended thing. Per-thread sketches merged at snapshot are available if
  that is ever wrong, but shipping striping first would be solving a problem
  nobody has.

**Privacy is the same answer as §10.5, and it is not optional.** A top-K by
observer is a ranked list of who searched the most — truncate the key to a
prefix, and keep the whole feature behind explicit configuration. An operator
choosing to rank their users should have to say so.

### 11.3 Causality: attribute the wait to whoever caused it

You can see that `proj.fetch.derive` is slow and that `lock.ingest.wait` spiked,
and nothing in §§1–10 links the two. §8 rejects a tracing dependency, which is
the textbook fix, so the link has to come from somewhere else.

It is already there. In `lockedOn`, `requested` is captured *before*
`mutex.withLock`, and `heldNow()` is readable at that instant — so a writer
about to queue can see **exactly what it is about to wait behind**, for the cost
of one volatile read (~1 ns, against a wait measured in seconds).

That turns `lock.*.wait` from a scalar into an attribution:

```
lock.ingest.wait  41.2 s total
    38.4 s  behind  proj.fetch.derive   (Drain)
     2.1 s  behind  write               (BatchInsert)
     0.7 s  behind  supersede           (BatchInsert)
```

Which is the actionable form. "Ingest waited 41 s" prompts a question; "ingest
waited 38 s behind the trust drain's contact-card recall" names the fix.

**The honest limitation: this is first-holder attribution.** A waiter samples
the holder it queues behind, and over a long wait the lock may change hands
several times — all of that wait is charged to the head of the queue.
Re-sampling would cost a read per poll and still miss handovers. For the case
this exists to catch (one pathological holder stalling everyone) it is exactly
right; for a uniformly busy queue it over-attributes to whoever happened to be
first, and a reader should know that before drawing conclusions from a flat
distribution.

**The companion signal**: a hold exceeding a threshold gets recorded the way a
slow query does (§10.5) — stage, activity, duration, and its `annotateHold`
detail. The slow-query log finds expensive reads; this finds expensive
*critical sections*, which is the thing that actually stalls a relay's ingest.

This is not per-request causality and does not pretend to be. It is the one
causal edge that matters in a store where every write serialises through two
mutexes, bought for a volatile read instead of a dependency.

### 11.4 What no counter will tell you

Wall time is not CPU, and none of the above changes that. A store blocked on
Vespa for 90 % of its wall time is consuming almost no CPU, so every store-side
duration in this document is **latency and contention, not consumption**. (The
engine-side figures are different: Vespa's reported query time *is* the engine's
work.)

The obvious repair does not work here. `ThreadMXBean.getCurrentThreadCpuTime()`
measures **200 ns** on this box (7.4× `nanoTime`, so a pair is ~400 ns —
affordable at batch granularity, not per port call). But this store is
coroutines end to end, and a `suspend` function may resume on a different
dispatcher thread: a start/end delta then subtracts one thread's lifetime
counter from another's, which is meaningless and can be negative. Per-activity
CPU accounting is only sound around sections that provably do not suspend, which
is not where the interesting time goes.

So **true CPU attribution belongs to a profiler** — JFR or async-profiler, both
cheap enough to leave running. This does not contradict §8's rejection of
sampling: that rejects sampling *the counters*, where the pathological call is
the whole point and would be sampled away. Sampling is the right tool for the
question counters cannot express at all, which is "which code burned the CPU".
The two compose — this model says *which activity* is expensive, a profiler says
*why* — and an operator hunting CPU should be told to reach for the second
rather than left to infer it from the first.

### 11.5 What this does to the budget

| | |
| --- | ---: |
| §6 counters, gauges, slow-query ring | ~60 KiB |
| §10.3 percentile-grade histograms (replaces the 24-bucket figure) | ~+17 KiB |
| §11.2 three sketches at K=64 (observer, term, filter shape) | ~+23 KiB |
| **total** | **~100 KiB** |

Still flat in traffic, still bounded by closed key sets, still noise against a
1536m heap. The per-request cost is unchanged for everything except a search
carrying an observer or terms, which additionally pays the ~63 ns of §11.2.
