# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Vespa-backed implementation of Quartz's `IEventStore` (Quartz is the Nostr library from Amethyst) that filters and ranks everything — REQs, COUNTs, and NIP-50 search — through each connecting user's NIP-85 web of trust. Kotlin 2.4 / JDK 21, Gradle multi-module. Published to Maven Central as `com.nosfabrica.vespa.eventstore:{store,engine}`.

## Commands

```bash
./gradlew build            # compile + unit tests + spotlessCheck (the CI gate)
./gradlew test             # unit tests only — needs NO external services, no Docker
./gradlew spotlessApply    # fix formatting; run this before committing

# Single test class / single test method:
./gradlew :store:test --tests "com.nosfabrica.vespa.eventstore.NostrSemanticsStoreTest"
./gradlew :engine:test --tests "*EventYqlTest.someMethodName"

# Integration tests (real Vespa via testcontainers — needs a Docker daemon;
# excluded from the default build, self-skip without Docker):
./gradlew :benchmark:test -Pintegration
```

- First build installs git hooks (`.git-hooks` → `.git/hooks`): pre-commit runs `spotlessCheck`, pre-push runs `test`.
- Spotless enforces ktlint plus the MIT license header (`.spotless/copyright.kt`) on every `.kt` file — new files without the header fail the build; `spotlessApply` inserts it.
- Dependency versions live in `gradle/libs.versions.toml`. Quartz comes from JitPack **pinned by commit** (`quartz = "<sha>"`); bumps are deliberate and the comment there records what the new pin contains. The published library version is `app`.
- Benchmarks (against a live Vespa): `./gradlew :benchmark:run`, plus targeted tasks (`searchBench`, `queryBench`, `visitBench`, `multiFilterBench`, `rankAb`, …) — see `benchmark/README.md`, which is also the repo's performance knowledge base (measured numbers, config A/Bs, why decisions were made).
- Local Vespa for manual testing: `docker run --detach --name vespa --publish 8080:8080 --publish 19071:19071 vespaengine/vespa`; `VespaEventStore.open(url)` auto-deploys the bundled schema on first contact.

## Architecture

Three modules, layered strictly bottom-up:

- **`:engine`** — the engine layer: the ports and shared helpers at the package root (`EventIndex`, `ReputationIndex`, `InMemoryEventIndex`, `ScoredHit`), document shapes (`doc/`), the `EventQuery` → YQL compiler (`query/`), and the Vespa client (`client/` — `VespaEventIndex` reads via OkHttp h2c, writes via Vespa's official feed client; `VespaReputationIndex`) plus the bundled Vespa application package.
- **`:store`** — Nostr semantics on top: `NostrSemanticsStore` (the `IEventStore` implementation), the NIP-85 trust projection (`trust/`), per-kind search extraction (a thin wrapper over Quartz's `SearchFieldExtractor` in `mapping/SearchExtractors`), and `VespaEventStore.open()` — the public front door.
- **`:benchmark`** — not published. Perf harness + the parity/rank-regression integration tests (the CI correctness gates).

The stack `open()` assembles: `NostrSemanticsStore( TrustProjection( VespaEventIndex + VespaReputationIndex ) )`. Consumers only ever see the Quartz `IEventStore` interface.

### The engine port and its executable spec

`EventIndex` (`:engine`, `EventIndex.kt`) is the seam everything hangs on: get/put/remove + `EventQuery` recall, with a hard contract — read-your-writes per document, and an **acked put is visible to search**. That contract is what makes the store's query-then-write logic sound. There are two implementations: the real Vespa client, and `InMemoryEventIndex`, which is the **executable specification** of `EventQuery` matching semantics — store tests run against it with no Vespa. `MockVespaEngine` (testFixtures, a Jetty h2c server) additionally exercises the real HTTP clients' wire format.

**Known blind spot**: the in-memory reference and the mock can miss real-Vespa-only divergences (e.g. Vespa omits empty-string fields from summaries; string attributes match uncased unless `match: cased`). Anything touching YQL, summaries, or the schema needs the integration gate (`-Pintegration`): `VespaParityIT` asserts exact result parity with Quartz's SQLite store (127/127 checks), `RankRegressionIT` pins search-ranking quality against a canonical corpus, `ObserverGateIT` pins the observer gate engine-side (trust-gated recall, both gated profiles), `OrphanSweepIT` pins the orphan-score sweep (the `distinctAuthors` grouping decides what gets deleted) — the mock cannot rank or gate.

### Nostr semantics (`NostrSemanticsStore`)

All writes serialize behind one `Mutex` (`withWriteLock`), making query-then-write atomic within the process. `insertLocked` enforces: dedup, replaceable/addressable supersession (NIP-01 tiebreak: same `created_at` → lowest id wins), NIP-09 deletions (same-owner only; deleting a deletion is a no-op — Quartz's SQLite store gets this wrong, we don't), NIP-40 expiration, NIP-62 vanish, ephemeral-never-stored. Rejections return typed `"duplicate:"`/`"replaced:"`/`"blocked:"` reasons (`Rejections.kt`). The store **never verifies signatures** — it deliberately holds unsigned rumors; verification is the caller's ingress job.

The per-event `insert()` path pays admission-probe round trips; `batchInsert()` amortizes them (~47× fewer round trips). Never ingest in a loop over `insert()`.

### Trust projection (`store/trust/`)

`TrustProjection` maintains per-pubkey `reputation` parent documents (which the event schema imports for ranking) as an `EventIndex` **decorator**: it observes `put`/`remove` on the index, so every deletion style (supersession, kind-5, vanish, sweep) updates trust tensors with zero deletion-specific code. Work is declared as `Dirt` in the crash-safe `DirtLedger` — settled inline, or (default in `open()`) drained by a background worker; `awaitTrustProjection()` is the read-your-writes barrier. `TrustReconciler` is the startup/ops repair for drift no write trigger can see, and owns the one trust-side deletion: `sweepOrphanScores()` drops the 30382s signed by services no 10040 names (never automatic; it refuses outright when no 10040 is readable, since that state is indistinguishable from a corpus mirrored before its provider lists).

### The schema ships with the code

`engine/app/` (`schemas/event.sd`, `schemas/reputation.sd`, `services.xml`) is the single source of truth; the build zips it into the `:engine` jar as `vespa-app.zip` and `open(autoDeploy = true)` deploys it to a fresh Vespa. Schema and query builder can therefore never drift — **a schema change and the Kotlin that depends on it must land together**, and needs the integration tests since only a real Vespa executes the schema.

### Search

Only kinds Quartz parses as `SearchableEvent` are searchable. The per-kind decomposition lives UPSTREAM in Quartz (`SearchFieldExtractor`/`IndexableFields`, beside the kinds themselves); `mapping/SearchExtractors` is this store's thin wrapper applying Vespa sanitization and the join/weighting policy (the kind table is in README). Search-string extensions (`observer:`, `sort:rank`, `filter:rank:gte:N`, `include:spam`, and the `-word` / `"exact phrase"` term syntax) are interpreted by the store; ranking profiles live in `event.sd`. When changing ranking: cases live in `benchmark/rank_cases.json` (add one for every reported bad search), A/B with `./gradlew :benchmark:rankAb` against a live Vespa (no redeploy needed), and `RankRegressionIT` must stay green.

## Conventions

- Comment culture: this codebase documents **why** (invariants, contracts, measured trade-offs) in KDoc and inline comments, densely. Match it — and update the comment when you change the behavior it explains.
- CI (`.github/workflows/build.yml`) runs three jobs on PRs to `main`: `spotlessCheck`, `build` (unit), and `integration` (`:benchmark:test -Pintegration`). All three must pass.
- Design docs in `docs/` (`scaling.md`, `multi-node-consistency.md`, `server-side-constraints.md`, `attribute-memory.md`, `embedded-vespa-study.md`) record operator guidance and studied-but-rejected alternatives — check them before re-proposing one. `attribute-memory.md` is the per-field RAM budget of `event.sd` — read it before adding an attribute or reaching for `paged`.
