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

## The live staging system (real data to pull into a local environment)

**`https://search-staging.brainstorm.world/`** runs this store at production scale:
SearchOverTrust (`github.com/NosFabrica/vespa-relay`), a Nostr relay whose REQ / COUNT /
NIP-50 paths are `NostrSemanticsStore` over a real multi-node Vespa. It is the source most
bug reports and test fixtures here come from (the "sot… export" notes in
`benchmark/rank_cases.json`, `benchmark/src/test/resources/search_vitor_pamplona_export.json`
— a plain JSON array of events, which is all a capture needs to be).

Use it as a **data source and a sanity oracle**, never as a test gate — it deploys on its own
cadence, so its schema and Quartz pin can lag this repo, and that mismatch is precisely why CI
stays hermetic (testcontainers). **Read only: never feed it, and never point a test at it.**

- **Wire**: `wss://search-staging.brainstorm.world/`, plain NIP-01, so any Nostr client works.
  NIP-11 doc: `curl -H 'Accept: application/nostr+json' https://search-staging.brainstorm.world/`
  — supports NIPs 1, 9, 11, 40, 42, 45, 50, 62, 77, 86; `default_limit` 500, `max_limit` 5000,
  20 filters per REQ. Page a corpus backwards with `until`, or walk it with NIP-77. AUTH is
  offered but not required, and `COUNT` answers exactly, so it is also the cheapest way to size
  a slice before pulling it (measured 2026-08-14: 211.8M events, 132.4M kind 1s).
- **Observer key**: `460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c` — a
  **public** key (no secret ships with it), so simulate the observer through the search
  extension rather than NIP-42: `Filter(search = "bitcoin observer:460c25…065c")`. It is already
  the canonical observer in `SearchPrefixLadderIT` and `rank_cases.json`. Running a query with
  and without the lens is the fastest way to see the gate and the trust ranking do their work;
  `sort:recent`, `filter:rank:gte:N` and `include:spam` are live there too.
- **Reproducing that lens locally**: its kind 10040 names one provider,
  `7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377`
  (`wss://scores.brainstorm.world`), for both `rank` and `followers`. Pull the 10040, then that
  provider's 30382s (242k of them), feed them alongside the events, and `TrustProjection` rebuilds
  the same reputation tensors — the same query then ranks the same way against a local Vespa.
- The corpus is real, and therefore dirty: it holds notes with `created_at` in the year 2100 that
  sort ahead of everything under an unranked filter. Capture what a report describes, not a tidied
  version of it — that raw shape is the point.

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

### Search expansion (`store/search/`)

A NIP-50 search matches a LABEL's value or a Trusted List's TITLE, not the
record those are about — so a reader searching "podcaster" gets the list and
none of the people on it. `SearchReferenceExpansion` splices the record in
behind the pointer that named it: NIP-32 labels (1985), NIP-85 assertions
(30382-30385) and Tapestry Trusted Lists (30392-30395).

- **Only a read carrying TERMS**, which is what makes it safe to run
  unconditionally. `include:spam` alone is not a term, and neither is
  `observer:` — so a mirror's paging, a NIP-77 catch-up and this store's own
  provider-list read are untouched. There is no flag on the read to say so.
- **A kind-restricted search still reaches its pointers.** A client hunting
  people sends `kinds:[0]`, which recalls no 30392 — so the searching queries
  are also re-run against the pointer kinds that CONVERT into the asked-for
  ones (`SearchReferences.convertibleInto`: pubkey pointers when kind 0 is
  asked, id pointers for any kind, coordinate pointers for addressable kinds;
  `SearchReferenceExpansion.companions`). The pointers are served with the
  page — the client needs them to know what to process — and their subjects
  are admitted under the ORIGINAL query's kinds. Declaration kinds are
  companion-fetched from enrolled signers only (plus the reader): an explicit
  `kinds:[30392]` ask serves strangers' lists as plain NIP-01 hits, but a
  companion is the store's own addition and only adds what the gate would
  unpack. Labels' companion is ungated, so an anonymous search converts
  labels and nothing else. Two refinements: attribution prefers a lens that
  asked for the pointer's kind OUTRIGHT over one it merely converts into
  (else an early filter captures a later filter's hit and reads its subjects
  through the wrong lens), and the declaration companion waives the DEFAULT
  trust floor, mirroring `include:spam` (its authors are already exactly the
  enrolled signers, and the canonical provider is an unranked service key —
  an explicit `filter:rank:` floor survives). The gate read is lazy: resolved
  at most once per read, and only when a declaration companion is buildable
  or a declaration pointer is met — `ProviderMap` never caches an empty pass,
  so eager resolution would bill a query to every observer search on a
  10040-less relay.
- **An UNRESTRICTED search fetches the declaration companion too.** A read with
  no `kinds` admits every pointer, which is not the same as recalling one
  inside the caller's `limit`: a declaration is signed by a NIP-85 service key
  that nobody follows and no reputation tensor ranks, so it competes for a slot
  from the bottom of the trust curve. Measured on staging — searching
  `"Verified Human"` under a reader whose own provider signed the Trusted List
  of that name put the 30392 at rank 80, so a page of 40 held five kind-30000
  re-publications of it, none of the reader's own list, and unpacked nothing,
  while the same search as `kinds:[0]` returned all seventeen member profiles.
  The LABEL companion stays kind-restricted: it is ungated and unconstrained,
  so on an unrestricted read it would only re-fetch what the ranking placed
  below the page, on every searching read the relay serves.
- **`SearchReferences` dispatches on the KIND, never `is UserTrustedListEvent`**:
  a consumer may force its own Quartz over ours, and a pin without those kinds
  hands back a base `Event` whose `is` checks all go false — silently.
- **The gate.** A declaration only unpacks for a reader whose own kind-10040
  named its signer FOR THAT KIND (`trust/Delegations`, derived in `ProviderMap`'s
  cached pass, so a 10040 write invalidates it with no code of its own). Both
  delegation shapes count: NIP-85's `<kind>:<metric>` and the Tapestry ADR's
  generic bare `<kind>`, which NIP-85's parser refuses. Labels are ungated by
  design. A store built without `TrustProjection` admits no declaration at all.
- **Admission is the engine's job**: the subject lookup is the finding query
  with its terms stripped and the subject keys intersected in, so the index
  applies the same predicate it applied to the hits. No second matcher.
- **The confidence rides on the KEY, not on the query** (`EventQuery.authorWeights`
  / `idWeights` -> `dotProduct(pubkey|id, {member: 0..100})`, read back in the
  profile as `rawScore`). That is what makes one lookup carry a whole list at the
  publisher's own resolution: confidence used to be a query-level rank feature, so
  members were GROUPED by it and each group cost a round trip — which is also why
  it was quantized to quarters (`BUCKETS`, now the addressable shape's alone: a
  coordinate is (kind, author, d) and has no single attribute to weight). The
  operator is measured, not assumed — on a single-value `fast-search` attribute
  `weightedSet` recalls the same rows and leaves `rawScore` at 0, so only
  `dotProduct` carries the number; a ZERO weight still recalls its document, so a
  publisher's honest 0 needs no offset. `SplicedMemberWeightsIT` is that proof.
- **Placement is the ENGINE's number, on ONE scale.** A scored member is fetched
  under `spliced_member` (`event.sd` §13): its own rung (the affiliation band,
  widened by its confidence, times its own trust) OR a floor at a share of the
  POINTER that found it — whichever is higher. The floor is what the rung cannot
  express: the member matched none of the query's words, so only the pointer knows
  the query, and a 4,000 x wot ceiling cannot reach a 130,000 x wot title match
  from below (measured on staging: a `Verified Human` list at #10 on its title, the
  member it is 87% sure of and the reader ranks 100 at #40, under 27 mirror pages
  from one rank-30 bot). The floor is a SPAN of rungs, not a share of an arbitrary
  number — `w_subject_floor_span` defaults to `w_near_tier / w_name_tier`, so a
  member lands within one rung of its pointer however doubted, ordered inside that
  span by confidence. Nothing is computed in the store: the two placements it
  tried both broke (multiplying the pointer's banded relevance by confidence
  ejected members into the gap below; clamping each member to its pointer made the
  SIGNER's trust the ceiling for everyone the list names — a service key nobody
  follows pinned sixteen members scored 65..100 to one number).
- **A reason never ranks below what it explains — by LIFTING, not clamping.** The
  pointer takes the max of its own relevance and the best of its scored members,
  so no subject can pass it, a tie resolves pointer-first (the pointer is
  appended before its subjects and the sort is stable), and the block's position
  is decided by the people on the list rather than by whoever signed it. An
  unscored reference (a label, an assertion) expresses no doubt and so takes the
  lifted score too, keeping it adjacent to its pointer.
- **Which needs the per-hit relevance**, which is why `recallOrdered` returns a
  `Page` (hits plus a NULLABLE parallel score list — the unscored page is the hot
  one, and wrapping every hit of a plain recall to carry a null was two copies
  and an allocation per event). **The scores cost nothing**: on a ranked query
  `recallSummaries` goes through `rankedHits` anyway, one `recallRoot` call, and
  the ranked path differs only in keeping the `relevance` Vespa already returned.
  A page with no scores at all — the in-memory reference, a recency-ordered read,
  two ranking profiles concatenated — keeps the pointer's own order rather than
  inventing a constant, and a ladder with no member rung (`sort:rank:asc` and its
  siblings) degrades to exactly that.
- Caps (`SearchExpansionLimits`) arrive through `open()`: a deployment's budget
  is the operator's call, applying it is the store's.

### Search

Only kinds Quartz parses as `SearchableEvent` are searchable. The per-kind decomposition lives UPSTREAM in Quartz (`SearchFieldExtractor`/`IndexableFields`, beside the kinds themselves); `mapping/SearchExtractors` is this store's thin wrapper applying Vespa sanitization and the join/weighting policy (the kind table is in README). Search-string extensions (`observer:`, `sort:rank`, `filter:rank:gte:N`, `include:spam`, and the `-word` / `"exact phrase"` term syntax) are interpreted by the store; ranking profiles live in `event.sd`. When changing ranking: cases live in `benchmark/rank_cases.json` (add one for every reported bad search), A/B with `./gradlew :benchmark:rankAb` against a live Vespa (no redeploy needed), and `RankRegressionIT` must stay green.

## Conventions

- Comment culture: this codebase documents **why** (invariants, contracts, measured trade-offs) in KDoc and inline comments, densely. Match it — and update the comment when you change the behavior it explains.
- CI (`.github/workflows/build.yml`) runs three jobs on PRs to `main`: `spotlessCheck`, `build` (unit), and `integration` (`:benchmark:test -Pintegration`). All three must pass.
- Design docs in `docs/` (`scaling.md`, `multi-node-consistency.md`, `server-side-constraints.md`, `attribute-memory.md`, `embedded-vespa-study.md`) record operator guidance and studied-but-rejected alternatives — check them before re-proposing one. `attribute-memory.md` is the per-field RAM budget of `event.sd` — read it before adding an attribute or reaching for `paged`.
