# Locks and blocking in this store — what each one guards, and what could replace it

An audit of every mutex, monitor, atomic and thread-blocking call in `:engine`
and `:store` (2026-09-04), asked one question per site: is the exclusion
load-bearing, and if so what would a lock-free version have to prove? The
verdicts below are the record; the last section is the design for the two
locks that are worth removing, so that work starts from the argument and not
from scratch.

Terminology, because "blocking" means two different things here:

- A **coroutine `Mutex`/`Semaphore`** (`kotlinx.coroutines.sync`) SUSPENDS a
  waiter. No thread parks; the dispatcher runs other work. Every writer lock in
  the store is this kind.
- A **JVM monitor** (`synchronized`) or a **blocking I/O call** parks a thread.
  The store's monitors are all nanosecond-scale and uncontended; the blocking
  I/O is confined to boot and to one walk that runs on `Dispatchers.IO`.

## Inventory and verdicts

| site | primitive | guards | verdict |
|---|---|---|---|
| `NostrSemanticsStore.writes` | coroutine `Mutex` | query-then-write atomicity for event documents: dedup GET → guard probes → put, supersession, kind-5/62 sweeps | **load-bearing, but coarser than the invariant needs** — every constraint is scoped to one OWNER (see below); a per-owner scheme or an engine-conditioned fast path is the real win |
| `NostrSemanticsStore.trustGate` | coroutine `Mutex` | a reputation document's derive+write against a live card's cell update; the dirt marker's rewrite against a write-ahead | **load-bearing** — the derive reads N cards and writes one document, which no engine condition can express; slicing (`GATE_SLICE`) is the fairness knob |
| `DirtLedger.pending` / `inherited` | was plain fields | the in-memory work ledger | **was wrong since the gate split; now lock-free** (`AtomicReference` with CAS union / take) — see "What the audit found" |
| `GuardOwners.loadLock` | coroutine `Mutex` | one corpus walk at a time (load and rebuild) | **fine** — it serialises a multi-minute scan, which is the point; a CAS "in progress" flag would only turn a waiter into a spinner |
| `GuardOwners.swapLock` | JVM monitor | a note landing either fully before or fully after a bloom swap | **fine, replaceable** — held for two set-adds, never across I/O, and notes arrive under `writes` anyway; the lock-free shape is below for the record |
| `MaxRankCache.known` | was `synchronized(HashMap)` | the per-subject `max_rank` cache | **now lock-free** (`ConcurrentHashMap`) — every caller already runs under the trust gate, so the monitor was pure cost; `putIfAbsent` on fills keeps the one unsound direction (stale-high) impossible |
| `InMemoryEventIndex.docs` | was `synchronized(LinkedHashMap)` | a reader scanning while a writer mutates | **now lock-free** (`ConcurrentHashMap`; every recall sorts by a total order, so iteration order was never relied on) — this is also the bulk mixed path's replay snapshot, not only a test double |
| `Concurrency.mapBounded` / `forEachBounded` | coroutine `Semaphore`, `Mutex` | fan-out width; a serialised accumulator | **fine** — both suspend; the accumulator mutex could be a single-consumer `Channel`, which is the same thing spelled longer |
| `IngestStats`, `BackgroundFailures`, `GuardBloom`, `SchemaFallbacks`, `ProviderMap.cached`, `VespaEventIndex.nextUrl` / `trustDescent` | atomics / `@Volatile` | counters, flags, a cache | **already lock-free** |
| `VespaHttp.send` | `suspendCancellableCoroutine` over OkHttp `enqueue` | every read | **non-blocking** |
| `VespaEventIndex` / `VespaReputationIndex` feed ops | `CompletableFuture.await()` (kotlinx) | every write | **non-blocking** |
| `VespaVisits.streamOnce` | `call.execute()` + `readUtf8Line()` on `Dispatchers.IO` | the streamed JSON-Lines visit | **thread-blocking, deliberately** — a streamed body is a pull-based `BufferedSource`; it is offloaded, bounded by `VESPA_VISIT_CONCURRENCY`, and a guard child aborts the socket on cancellation. The async alternative (JDK `HttpClient` + `BodySubscribers.fromLineSubscriber` bridged to a `Channel`) is a rewrite of the walk for no measured gain |
| `SchemaDeployer` | JDK `HttpClient.send` + `Thread.sleep` | boot-time deploy and readiness poll | **thread-blocking at boot only**, inside the non-suspending `VespaEventStore.open()`; a `suspend` `open` (or an `openAsync`) using `sendAsync().await()` and `delay()` is a public-API change, not a fix |
| `VespaFeed.close` | `FeedClient.close(true)` | graceful shutdown | **blocking at shutdown only** |
| tests, benchmarks | `runBlocking` | entry points | expected |

Nothing in the serving path parks a thread. The lock-wait a client can feel is
coroutine suspension on `writes` — which is why the rest of this note is about
that one lock.

## What the audit found (and fixed)

**The trust-gate split left the ledger unprotected.** `DirtLedger.pending` was
a plain field, safe because every entry point used to run under the ONE writer
lock. After the split a kind-1 insert runs `guarded()` under `writes` alone and
the background drain runs under `trustGate` alone; two read-modify-writes of a
plain field under different locks is a lost update. The path that mattered: a
kind-1 read `pending` before a drain cleared it and wrote it back after; the
next card for that subject then found it "already pending", computed an empty
write-ahead delta, persisted nothing — and a crash before the following drain
would have been permanent drift with no marker naming it. The ledger is now an
`AtomicReference<Dirt>` moved only by CAS union (an op adds its work) and CAS
take (a drain swaps the whole snapshot out), which is correct under any
interleaving and costs no lock. `DirtLedgerTest` pins both interleavings.

**The drain lost work re-queued for a subject it was already deriving.** This
one predates the split. A round subtracted its snapshot at the end
(`pending - snapshot`), so a card written for subject A AFTER A's slice had
been derived — but before the round finished — added an A that was already in
`pending`, and the subtraction removed it with the marker rewritten clean. A's
newer card was served with the old rank until a reconcile. The round now TAKES
its snapshot first, so anything added mid-round, in the snapshot or not, is
what the next round finds; the marker keeps covering the snapshot until the
round completes, and a failed round puts its snapshot back.

**The reindex was the fifth write entry point.** `reindexFullTextSearch` re-puts
pages through the projection, whose `putAll` applies card cells inline; it took
`writes` only. It now queues for the trust gate when a page carries trust
kinds, like every other write that touches reputation.

## Why `writes` cannot simply go away

`docs/multi-node-consistency.md` already states the invariant: query-then-write
is atomic against other writers in this process, and an acked put is visible
to search. Three admission decisions rest on it, and they differ in what the
engine could take over:

| decision | cross-document? | engine can enforce it? |
|---|---|---|
| dedup (`get(id)` then put) | no | already safe lock-free — a raced duplicate is an idempotent re-put of identical bytes (the bulk planner runs it outside the lock today) |
| replaceable/addressable supersession | no (one address) | **yes** — `putIfNewer` under address-keying is a server-side test-and-set (`docs/server-side-constraints.md`); the read-then-supersede default is not |
| NIP-09 tombstone / NIP-62 vanish guard | **yes** — the kind-5 is one document, the event it must block another | **no** — Vespa conditions see only the document they address |

So a fully lock-free insert exists exactly for events whose owner has no stored
tombstone or vanish AND whose supersession the engine conditions — and
`GuardOwners` already answers the first half per owner, while `supersedesViaPut`
answers the second. What remains locked is the guard, which is per owner.

## The two designs worth doing

### 1. Owner-scoped exclusion instead of a process-wide one

Every constraint the store enforces is scoped to one OWNER (`EventDoc.owner`:
the gift-wrap recipient for kind 1059, else the author): a replaceable address
is `(kind, pubkey[, d])`, NIP-09 erases same-owner targets only, NIP-62 sweeps
one owner's history. Different owners' writes commute — the multi-lane argument
in `docs/multi-node-consistency.md`, applied inside one process.

Shape: `N` striped coroutine mutexes keyed by `hash(owner)`, plus one
reader-writer arrangement for the paths that span owners:

- per-event `insert` takes SHARED on the store plus its owner's stripe;
- `batchInsert` (both bulk shapes), `transaction`, `delete(filters)`,
  `deleteExpiredEvents` and the reindex take EXCLUSIVE — they touch arbitrary
  owners and the bulk commits are designed around one atomic diff;
- kotlinx has no RW mutex; `Semaphore(N)` with 1 permit for shared and `N` for
  exclusive is the standard construction and suspends like `Mutex` does.

What it buys: the per-event path is measured at ~143 inserts/s with p50
6.6 ms (`benchmark/README.md` §2bb) — the lock is held across the dup GET,
the guard probes and the put, so one process serialises client EVENTs at
roughly `1 / p50` regardless of engine capacity. Striping lets independent
owners overlap and the ceiling becomes the engine's. What it costs: a batch
now waits for in-flight singles (bounded by one insert's latency), and the
`GuardOwners` note hooks, which today land under `writes`, must stay ordered
after the kind-5's put within the same stripe (they do — same coroutine).

Not done here because it changes the store's concurrency contract and needs
the ingest benchmarks re-run under the relay's real mix; it is a PR of its
own, and the `lock.ingest.wait` stage in the status line is what will say
whether it was needed.

### 2. A lock-free fast path for unguarded owners

With striping in place, the remaining lock on the hot path is the stripe, and
even that is unnecessary for the common event: no stored tombstone or vanish
for the owner (the bloom says so) and engine-side supersession (or a
non-replaceable kind). The one hazard is the kind-5 that arrives WHILE such an
insert is in flight: the fast insert checked the bloom, the kind-5's sweep
query runs before the put lands, the covered event survives. The RCU-shaped
answer: the kind-5 path notes its owner in the bloom FIRST (over-flagging only
costs a probe), then waits for the owner's in-flight fast inserts to drain (a
per-stripe counter), then sweeps under the stripe. Fast inserts touch one
counter and no lock; the rare kind-5 pays a wait bounded by one insert. This
is an extension of design 1, not an alternative to it, and it only pays off
once design 1 has shown the stripe itself to be contended.

### The trust gate stays a lock

A derive reads every card about a subject and writes one document; a live
card write updates a cell of the same document. No engine condition covers
"the cards I read are still the cards there are", so the derive+write must be
atomic against card writes for that subject. What is NOT necessary is one gate
for all subjects: the same striping (by subject) applies, and the drain's
slices are already per subject. That is the follow-up if `lock.ingest.trust`
ever shows cards queueing behind the drain the way kind-1s used to; today the
split has taken the client-facing path out from behind it entirely.

### `GuardOwners.swapLock`, for the record

A lock-free swap is possible — publish `State(blooms, noting)` as one
`AtomicReference`; a note adds to the state it read and retries if the state
changed; a rebuild folds the buffer into the fresh blooms, publishes, then
folds the buffer AGAIN (catching notes that landed between the first fold and
the publish, which the retry alone cannot) — and it removes a monitor that is
held for two set-adds by a caller that already holds `writes`. Not worth its
subtlety until something measures it.
