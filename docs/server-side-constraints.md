# Server-side constraints in Vespa (vs SQLite triggers) — what the insert probes can and cannot shed

The per-event `insert()` path pays ~3.26 engine reads per event (dup probe,
NIP-09 tombstone probe, NIP-62 vanish probe, supersession probe — the first
three now run concurrently, so ~2 round trips of latency per insert). SQLite
pays none of that as *communication*: its UNIQUE constraints and triggers run
in-process, and Quartz's store enforces admission inside the same address
space. The question: does Vespa have constraint/trigger equivalents that move
these checks server-side?

Vespa has **no triggers and no cross-document constraints** — but it has three
mechanisms that between them cover most of the admission logic. Mapping:

| SQLite mechanism | Admission rule | Vespa equivalent | Status |
|---|---|---|---|
| `expires_at` sweep (client) | NIP-40 expiry | **GC selection** (`selection="event.expires_at > now()"`) | **SHIPPED** — validated live |
| `UNIQUE (id)` | duplicate insert | docid identity + **test-and-set** insert-if-absent | **MEASURED, NOT SHIPPED** — slower (below) |
| `UNIQUE (kind, pubkey[, d_tag])` + replace | replaceable/addressable supersession | **address-keyed docid + test-and-set condition** | **SHIPPED, opt-in** (`VESPA_ADDRESS_KEYED`) — 3.2× on draft churn (below) |
| trigger-like module (`DeletionRequestModule`) | NIP-09 tombstone block | **document processor** (custom Java in the container) | roadmap |
| trigger-like module (`RightToVanishModule`) | NIP-62 vanish block | **document processor** | roadmap |

## 1. SHIPPED: NIP-40 expiry as a garbage-collection selection

`services.xml` now declares:

```xml
<documents garbage-collection="true">
  <document type="event" mode="index" selection="event.expires_at &gt; now()" />
```

The engine itself periodically (default hourly) removes every event whose
`expires_at` has passed — a standing server-side constraint, no client
involvement. Validated on the live engine: with a 45s test interval, an
already-expired probe document was removed by Vespa within one cycle while a
never-expiring document (expires_at = Long.MAX_VALUE, always written) stayed.

The store's own guards remain, deliberately:
- the **read-time filter** (`expires_at > now`) stays because expiry must be
  exact-to-the-second and GC is periodic;
- the **insert-time block** on already-expired events stays because it costs
  no round trip — the expiration tag is on the event being inserted;
- the store's explicit sweep becomes a redundancy rather than the only
  mechanism — and the engine now also self-cleans events that expire while no
  store instance is running.

## 2. Test-and-set: single-document conditions (Vespa's CHECK constraint)

Every Vespa write (put/update/remove) accepts a `condition` — a selection
evaluated **server-side against the existing document with the same docid**,
atomically. Two admission rules fit inside it:

- **Insert-if-absent (the dup probe) — built, measured, NOT shipped.** A put
  with `create=true` and an always-false condition creates the document when
  absent and answers 412/`conditionNotMet` when present — one round trip that
  both writes and reports "duplicate", replacing the separate `get`. The full
  implementation (EventIndex.putIfAbsent through the feed client, the mock,
  the reference, the store) passed all tests and 127/127 parity, and the
  round-trip counter confirmed 3.25 → 2.25 engine reads/event. But the
  interleaved wall-clock A/B on a live engine (2,000 fresh + 2,000 duplicate
  per-event inserts per arm, code versions alternated against the same
  corpus) went the other way:

  | path | get-probe (shipped) | test-and-set | delta |
  |---|---:|---:|---|
  | fresh insert | 79.0 / 87.0 ev/s | 67.9 / 67.4 ev/s | **15–20% slower** |
  | duplicate re-insert | 310.5 / 323.3 ev/s | 218.1 / 192.9 ev/s | **~35% slower** |

  Two causes: Vespa's conditional writes pay a read-for-write check even when
  the document is absent, and duplicates LOSE their cheap early exit — the
  dup `get` rides the concurrent guard wave (1 round trip total, no write
  attempt), while the conditional path pays guards + a full write. Fewer
  engine *ops* is not fewer *milliseconds* here. Reverted; the probe tool
  (`BENCH_INSERT_PROBE`) and the duplicate-reinsert benchmark case remain for
  re-testing on future engine versions or multi-node clusters (where a
  client↔cluster round trip costs more and the trade could flip).
- **Supersession by address-keyed conditional put — SHIPPED, opt-in
  (`VESPA_ADDRESS_KEYED=1`).** By default every doc's id is the event id, so
  replacing kind 0/3/10002/3xxxx searches for the incumbent + removes + puts
  (the read-then-supersede path). Under the flag, replaceable/addressable
  events instead use their NIP-01 address `kind:pubkey[:dtag]` as the **docid**,
  and supersession becomes a single conditional put:

  ```
  condition = "event.created_at < X or (event.created_at == X and event.id > 'newid')"
  ```

  — the NIP-01 newest-wins rule with the lexical tiebreak, enforced atomically
  by the engine with no client read; stale versions come back `conditionNotMet`
  and are rejected for free. Regular events stay id-keyed; id lookups route
  through the `id`-attribute search so both addressing schemes resolve. The
  refactor it costs (a docid-scheme split touching dedup, deletion-by-id, and
  reconstruction) is contained behind [`EventIndex.putIfNewer`]: the default
  implementation is read-then-supersede (what the store and reference use), and
  `VespaEventIndex` overrides it with the conditional put when the flag is on.

  **Measured (draft-churn A/B, `BENCH_CONDPUT`, this box):** 5,000 addressable
  draft addresses, 60,000 saves, 69% of them stale resends below the running
  head (the "clients re-saving every 2s" flood).

  | arm | throughput | wall |
  |---|---:|---|
  | A) read-then-supersede (default) | 599 ev/s | 100.2 s |
  | B) conditional put (address-keyed) | **1,940 ev/s** | 30.9 s — 50,478 stale rejected server-side |

  **B/A = 3.24×.** The win is exactly the stale flood: each stale resend costs
  the read path a supersession search, but costs the conditional put one
  rejected write with no read. It is a real refactor, worth enabling only where
  replaceable/draft churn dominates ingest (~9% of this corpus mix; the batch
  path already amortizes the read path's searches).

  Scope note: the fast path engages on the raw `VespaEventIndex`. Through the
  production trust-projection decorator, supersession deliberately stays
  read-based so the projection's write reactions still see both the superseded
  and the new version (the atomic conditional put never exposes the removed
  doc); see `TrustProjection`. `BENCH_ADDR_SMOKE` is the live end-to-end
  correctness smoke for the flag (dual id+address addressing, reject-stale).

The limitation that shapes everything: **a condition can only see the one
document it addresses.** No condition can ask "does a kind-5 by this author
target this id" — that is a cross-document question.

## 3. Document processors: the actual trigger equivalent (roadmap)

Vespa's container runs user Java on the **write path**: a `DocumentProcessor`
chain sees every fed document before it reaches the content node, can look
other documents up (`DocumentAccess` is injectable; searches are available via
the same container's search chains), can mutate, and can **reject**. That is
where the NIP-09/NIP-62 admission belongs if it should leave the client:

- the client's `insert()` becomes one round trip (the put);
- the tombstone/vanish/dup lookups still happen, but as **container-local**
  hops (sub-ms, same box or same cluster network) instead of client round
  trips, and centrally — every feeder gets the semantics, not just this
  library.

Cost and why it is roadmap rather than shipped: the admission logic
(`NostrSemanticsStore`'s guard reads and precedence rules) would need a port to a
container bundle (Java, deployed inside the app package), CI that stands up
the container with the bundle, and a versioning story between the bundle and
this library. The measured ceiling it attacks: per-event insert round trips
2 → 1 client-side (the bulk `batchInsert` path is already at 0.05
round-trips/event and would not benefit).

## What this does NOT change

The batch path already solved this problem for bulk ingest (chunked guard
reads amortized across 500 events). These mechanisms matter for the
*per-event* path — a relay accepting one EVENT at a time from a websocket —
where the guard-read round trip is the latency floor. Order of value after
measurement: GC selection (shipped, free) → **guard-owner cache (SHIPPED —
`GuardOwners`)**: the client-side realization of the docproc idea's read
savings, skipping both admission probes for owners with no stored
tombstone/vanish. Measured reads/event 3.26 → 1.73 (→ ~1.1 at realistic
deleter densities) with single-stream latency unchanged — the win is engine
read capacity under load.

That cache only maintains itself for a writer that sees all of its owners'
guards, so it is scoped by `WriterTopology` (an argument to `open()`, since no
store can detect a sibling feeder) and it is **opt-in**: the default is
`SHARED_STRICT`, which caches nothing. The numbers above are therefore what a
deployment gains by asserting `SINGLE_WRITER` (no staleness window while the
assertion holds), not what it gets for free. `SHARED` keeps the savings for a
multi-writer deployment at the price of a bounded window — see
docs/multi-node-consistency.md, and note the bound is the REBUILD duration on a
large corpus, not the configured interval. The reasoning behind the default:
skipping a probe never makes anything more correct, so every failure of the
cache is one-directional (an erased event admitted and served) and nothing
re-sweeps it afterwards; that is not a trade to make on a caller's behalf.
`GUARD_OWNERS_DISABLE=1` still forces `SHARED_STRICT` over any argument.
The docproc bundle remains the right tool when the goal is ENFORCEMENT
for multi-feeder deployments (it is the only mechanism that guards writes this
store never sees, with no staleness window at all); as a pure performance lever
it is now largely superseded. Address-keyed replaceables are now **shipped opt-in**
(`VESPA_ADDRESS_KEYED`, 3.24× on draft churn, §2); the test-and-set dup fold is
measured out at single-node scale (table above).
