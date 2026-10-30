# AGENTS.md

The one rules file for coding agents in this repository: Codex reads it directly, Claude Code
through `CLAUDE.md`, which only points here. Change a rule here, never in `CLAUDE.md`, so the two
cannot drift apart again (they had: each held sections the other lacked).

## What this is

A Vespa-backed implementation of Quartz's `IEventStore` (Quartz is the Nostr library from Amethyst) that filters and ranks everything — REQs, COUNTs, and NIP-50 search — through each connecting user's NIP-85 web of trust. Kotlin 2.4 / JDK 21, Gradle multi-module. Published to Maven Central as `com.nosfabrica.vespa.eventstore:{store,engine}`.

## What goes where

Three repos, one direction of dependency: quartz ← vespa-eventstore ← its consumers (vespa-relay).

- **Anything that knows a Vespa schema field, YQL, a rank profile, or what a search extension
  MEANS lives in this store.** A consumer that writes YQL, names `created_at` / `tag_index` /
  `pubkey` outside a grouping pipeline it hands to `EngineReads.group`, parses `sort:` /
  `observer:` / `include:spam` itself (`ReadShape.of` answers that), re-implements the
  supersession tiebreak (`EngineReads.newestVersions`), the provider map (`lensOf`), or a
  `VESPA_*` parse (`EngineTuning.fromEnv`) holds a copy that will drift — vespa-relay's did, and
  disagreed with the store on `sort:bogus` and on which rank provider is a reader's lens.
- **A consumer's production path reaching through `.engine` or `.store` is a missing facade
  member.** Add it to the facade (`VespaEventStore`, `LensedEventStore`, `EngineReads`, a root
  DTO); the two hatches stay for ops and benchmarks. A capability both handles answer goes on
  `LensedEventStore`, so `(store as? LensedEventStore)` is the consumer's only type test.
- **A Nostr rule this store needs that quartz lacks goes upstream to quartz FIRST** (filter
  matching, tag codecs, kind classification, NIP grammar). A local shim is allowed while the
  quartz change is in flight, and its KDoc names that change; when quartz already has the rule,
  call it (`FilterMatcher.match`, `MemberTagFields.score`). Build against a local quartz with
  `-PquartzDir` (below) to land both together.
- **Policy that is the consumer's stays the consumer's**: wording of a notice, which deploy failure
  to survive, chart shapes and bucket decoders, editorial caps. The store hands back facts
  (`TrustChain`, `SchemaDeployException.serving`, a raw grouping root), never the decision.
- **Skills that describe quartz come from quartz.** `.claude/skills/` still holds copies of
  quartz's own `nostr-expert` and `event-store-semantics` and of the `searchable-events`,
  `nip85-trusted-assertions` and `quartz-integration` skills. Quartz now ships those last three
  as its distributed skill, `quartz/agent-skill/quartz/` (`SKILL.md` plus
  `references/nip85-trusted-assertions.md` and `references/search-and-stores.md`), but that skill
  describes APIs newer than the 2.8.0 this store pins. At the first quartz bump that includes it,
  delete all five copies and install that skill (its `README.md` has the commands). Fix a
  quartz fact in quartz's skill, not in a copy here.

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
- Dependency versions live in `gradle/libs.versions.toml`. Quartz comes from **Maven Central**, a release (`com.vitorpamplona.quartz:quartz`, `quartz = "2.8.0"`) — not JitPack, because this store is published to Central with quartz as an `api` dependency; bumps are deliberate and the comment there records what the new pin contains. Nothing else here comes from JitPack either (`jitpack.yml` only lets JitPack build this store for vespa-relay). The published library version is `app`.
- **Building against a local quartz**: `./gradlew -PquartzDir=../quartz build` includes that
  checkout as a composite build and substitutes it for the pinned artifact in every module (like
  quartz's own `-PcryptoKmpDir`; `settings.gradle.kts`). For a change that needs quartz and this
  store together — the published POM still names the pin, so the quartz release lands first.
- Benchmarks (against a live Vespa): `./gradlew :benchmark:run`, plus targeted tasks (`searchBench`, `queryBench`, `visitBench`, `multiFilterBench`, `rankAb`, `searchTrace` — the per-clause-family latency split for one NIP-50 term, …) — see `benchmark/README.md`, which is also the repo's performance knowledge base (measured numbers, config A/Bs, why decisions were made).
- Local Vespa for manual testing: `docker run --detach --name vespa --publish 8080:8080 --publish 19071:19071 vespaengine/vespa`; `VespaEventStore.open(url)` auto-deploys the bundled schema on first contact.

## The live system (real data to pull into a local environment)

**`https://search.brainstorm.world/`** runs this store at production scale:
SearchOverTrust (`github.com/NosFabrica/vespa-relay`), a Nostr relay whose REQ / COUNT /
NIP-50 paths are `NostrSemanticsStore` over a real multi-node Vespa. It is the source most
bug reports and test fixtures here come from (the "sot… export" notes in
`benchmark/rank_cases.json`, `benchmark/src/test/resources/search_vitor_pamplona_export.json`
— a plain JSON array of events, which is all a capture needs to be).

Use it as a **data source and a sanity oracle**, never as a test gate — it deploys on its own
cadence, so its schema and Quartz pin can lag this repo, and that mismatch is precisely why CI
stays hermetic (testcontainers). **Read only: never feed it, and never point a test at it.**

- **Wire**: `wss://search.brainstorm.world/`, plain NIP-01, so any Nostr client works.
  NIP-11 doc: `curl -H 'Accept: application/nostr+json' https://search.brainstorm.world/`
  — supports NIPs 1, 9, 11, 40, 42, 45, 50, 62, 77, 86; `default_limit` 5000, `max_limit` 5000,
  20 filters per REQ. Page a corpus backwards with `until`, or walk it with NIP-77. AUTH is
  offered but not required, and `COUNT` answers exactly, so it is also the cheapest way to size
  a slice before pulling it (measured 2026-08-14, when it was still served as
  `search-staging.brainstorm.world`: 211.8M events, 132.4M kind 1s). Older notes across the repo
  that say "staging" or `search-staging` mean this same system under its previous host name.
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

Three modules, layered strictly bottom-up — and inside each, packages that are layered too.
`ModuleBoundariesTest` (in `:store`) asserts the whole order below, so a new package is a
deliberate decision (it fails until the layer table names it) rather than a drift:

- **`:engine`** — the index port and its Vespa binding. Reading order is the layer order:
  `text/` + `async/` + `app/` + `tuning/` (shared leaves: near-text derivation, bounded fan-out,
  the bundled Vespa application package and its deployer, the process-wide `ProcessKnob`s) → `doc/` (document shapes) → `query/` (the
  `EventQuery` → YQL compiler) → the package root (`EventIndex`, `ReputationIndex`, `ScoredHit`,
  `Ranked` — the PORT) → `metrics/` (`MeteredEventIndex`, the cost ledger, `IngestStats`,
  `DegradedReads`) → `memory/` + `client/` (the two implementations: the in-memory executable
  spec, and `VespaEventIndex` reading via OkHttp h2c / writing via Vespa's feed client,
  `VespaReputationIndex`).
- **`:store`** — relay policy on top. `runtime/` and `mapping/` are leaves that import no other
  store package (`WriterTopology` + background-worker failure accounting; the Quartz `Filter` →
  `EventQuery` mapping, per-kind search extraction, Vespa text rules) → `ingest/` (the write
  path: bulk inserts, the guard caches, NIP-09/62 enforcement, the rejection vocabulary),
  `trust/` (the NIP-85 projection), `search/` (reference expansion) → the package root, which is
  the FACADE: exactly the types a CONSUMER names, and nothing else. Today that is
  `VespaEventStore.open()` (the front door), `NostrSemanticsStore` (the `IEventStore`
  implementation), `LensedEventStore` (the capabilities beyond `IEventStore` that both of them
  answer), the values a consumer passes in — `EngineTuning` (every operator setting, with the
  strict `fromEnv`), `SchemaDeploy` — and the values it hands back or throws — `RejectedException`,
  `EngineReads`, `TrustHealth`, `TrustChain` (`lensOf`), `ReadShape` (the search grammar's verdict
  on a filter), `StoreTelemetry`, `SchemaDeployment` / `SchemaDeployException`, `LiveGate` (the
  observer gate a relay applies to a subscription's live events), `ScoredSearch`
  (`scoredProfileSearch`'s hits with their own engine scores, plus the rank service that scored them).
  A new type here has to earn it by being named from outside; the machinery
  behind one goes in a leaf (`TrustHealth` is a DTO, while the registries it reads stay
  `internal` in `trust/`; `LiveGate`'s rank-cell cache is `trust/TrustCells`). Nothing below the root imports the two COMPOSED types
(`NostrSemanticsStore`, `VespaEventStore`) — that is the cycle the layering exists to prevent.
A value the store hands back or throws is not that: `ingest/EventAdmission` throws
`RejectedException` because it is the store's public vocabulary for a rejection, and a value
travelling up is the opposite of a package reaching up.
- **`:benchmark`** — not published. `harness/` (backends, corpora, result plumbing, the parity
  and rank-quality batteries), `bench/` (the timed suites), `probe/` (targeted A/Bs), `load/`
  (corpus loaders and dumps), plus the parity/rank-regression integration tests — the CI
  correctness gates.

The stack `open()` assembles: `NostrSemanticsStore( TrustProjection( VespaEventIndex + VespaReputationIndex ) )`.

Two rules the compiler cannot hold, so tests do (`:store`, and they read the SOURCE — the test
task declares both source trees as inputs so a violation added in `:engine` cannot leave them
cached): `ModuleBoundariesTest` — the layer order above, no package unnamed by it, no facade
import from below, and a test named after a class lives in that class's package.
`PortDecoratorsTest` — every `EventIndex` decorator overrides every port member. That second one
matters more than it sounds: several port members have DEFAULT bodies that are correct-but-slow
answers for an engine that cannot do better, so a decorator that inherits one does not fail to
decorate it, it ANSWERS with it. Adding a member to `EventIndex` means adding it to
`TrustProjection` and `MeteredEventIndex` in the same commit.

Consumers get the Quartz `IEventStore` surface (`VespaEventStore` delegates it), plus two deliberate
escape hatches for ops and benchmarks: `store` (the concrete `NostrSemanticsStore`, for capabilities
beyond the interface) and `engine` (read-only, un-metered, NOT trust-projected — what is really
stored, and what a rank profile does). Both are documented at their declaration; prefer the
`IEventStore` surface for real reads.

**Where the seam actually is.** It is NOT "Nostr-free engine / Nostr-aware store" — `:engine` knows
Nostr, deliberately: `EventIndex.putIfNewer` implements the NIP-01 supersession rule *and its
tiebreak* (it has to, to be engine-atomic), `EventDoc` computes NIP-01 addresses and gates tags on
`isIndexableTagName`, and `doc/TrustKeys` + `EventQuery.rankKey`/`followersKey` carry the NIP-85
concepts the rank profiles in `event.sd` are written around. The real seam is **the index port and its Vespa binding**
(what a document is, how a query compiles, how it travels) **versus relay policy** (write
serialization, deletion / expiration / vanish enforcement, the trust projection, the search grammar,
page assembly). So when hunting a rule: supersession and address computation are engine; anything a
*relay* decides is store.

### The engine port and its executable spec

`EventIndex` (`:engine`, `EventIndex.kt`) is the seam everything hangs on: get/put/remove + `EventQuery` recall, with a hard contract — read-your-writes per document, and an **acked put is visible to search**. That contract is what makes the store's query-then-write logic sound. There are two implementations: the real Vespa client, and `InMemoryEventIndex`, which is the **executable specification** of `EventQuery` matching semantics — store tests run against it with no Vespa. `MockVespaEngine` (testFixtures, a Jetty h2c server) additionally exercises the real HTTP clients' wire format.

**Known blind spot**: the in-memory reference and the mock can miss real-Vespa-only divergences (e.g. Vespa omits empty-string fields from summaries; string attributes match uncased unless `match: cased`). Anything touching YQL, summaries, or the schema needs the integration gate (`-Pintegration`): `VespaParityIT` asserts exact result parity with Quartz's SQLite store (127/127 checks), `RankRegressionIT` pins search-ranking quality against a canonical corpus, `ObserverGateIT` pins the observer gate engine-side (trust-gated recall, both gated profiles), `OrphanSweepIT` pins the orphan-score sweep (the `distinctAuthors` grouping decides what gets deleted), `FilterMatrixIT` runs ~10k REQ+COUNT pairs through `open()` — NIP-01 shapes × lens modes × `sort:` × search shapes — against an in-test oracle (served set == oracle, COUNT == served), `NewestVersionsIT` holds the newest-version grouping to the in-memory reference, `EngineGroupIT` runs vespa-relay's statistics pipelines through `EngineReads.group` — the mock cannot rank, gate or run a grouping beyond a count.

### Recency planning (`RecencyStrategy`, `engine/client/`)

A limit'd newest-first read would otherwise make the engine walk its whole match set to keep the newest few — match time, not summaries or parsing, is nearly all of a feed REQ's cost at scale. `VESPA_RECENCY_STRATEGY` picks how that is bounded; the default `speculative` runs the read inside a `since` window anchored at its newest end, where a full page proves itself (everything outside is strictly older — true under the trust gate too) and a short one widens, falling back to the shipped `match_phase` behavior for narrow reads (`match_phase` is also the escape hatch). `RecencyWindowMemory` remembers per read shape where the last page was found (or that the shape is narrow), so repeated shapes resolve in one query (`VESPA_RECENCY_MEMORY=0` disables it). Pages are resolved ID-FIRST: every recency query (overfetch, tie windows, speculative attempts) projects to the attribute-only `idtime` summary, and one `id in (…)` query then reads exactly the page's documents (`VespaEventIndex.hydrate`; `VESPA_ID_FIRST=0` disables it) — on a corpus larger than memory, document reads were the load tail. All strategies serve the identical page; `./gradlew :benchmark:recencyStrategyProbe` asserts that against any loaded Vespa and prints the cost per strategy, and `recency.speculative.*` `IngestStats` stages count live outcomes. Internal existence probes opt out by stamping `RANK_UNRANKED` (a window can only add a round trip to them). Numbers and rejected alternatives: `benchmark/README.md`, "Recency strategies".

### Nostr semantics (`NostrSemanticsStore`)

All writes serialize behind one `Mutex` (`withWriteLock`), making query-then-write atomic within the process. `insertLocked` enforces: dedup, replaceable/addressable supersession (NIP-01 tiebreak: same `created_at` → lowest id wins), NIP-09 deletions (same-owner only; deleting a deletion is a no-op — Quartz's SQLite store gets this wrong, we don't), NIP-40 expiration, NIP-62 vanish, ephemeral-never-stored. Rejections return typed `"duplicate:"`/`"replaced:"`/`"blocked:"` reasons (`Rejections.kt`). The store **never verifies signatures** — it deliberately holds unsigned rumors; verification is the caller's ingress job.

The per-event `insert()` path pays admission-probe round trips; `batchInsert()` amortizes them (~47× fewer round trips). Never ingest in a loop over `insert()`. The same holds one layer down, and is handled there: the trust projection reacts per STORE COMMIT, not per port call — the store wraps each trust-gated ingest commit in `TrustProjection.commit`, every put/remove inside it records its reaction, and the commit applies them in one crash-ledger bracket and one pipelined cell write. Per-call reaction made a 150,000-card assistant run (one `putIfNewer` per card on the address-keyed path) take ~18 minutes (`./gradlew :benchmark:cardRunProbe`). A new trust-gated write path should run inside a commit too; a call outside one opens its own.

### Trust projection (`store/trust/`)

`TrustProjection` maintains per-pubkey `reputation` parent documents (which the event schema imports for ranking) as an `EventIndex` **decorator**: it observes `put`/`remove` on the index, so every deletion style (supersession, kind-5, vanish, sweep) updates trust tensors with zero deletion-specific code. Cells are keyed by the SERVICE that signed the 30382, never by the observer: a card is one cell update, a removal one cell remove, and the observer's kind 10040 is resolved to service keys per query (`NostrSemanticsStore.lensed`, `EventQuery.rankKey`/`followersKey`) — so a 10040 write costs nothing unless it names a service no list named before, whose stored cards are then walked into cells (`docs/service-keyed-trust.md`). That walk is the one deferred reaction, declared as `Dirt` in the crash-safe `DirtLedger` — settled inline, or (default in `open()`) drained by a background worker; `awaitTrustProjection()` is the read-your-writes barrier. `TrustKeyingMigration` re-keys a store fed before this model, once, at `open()`. `TrustReconciler` is the startup/ops repair for drift no write trigger can see, and owns the one trust-side deletion: `sweepOrphanScores()` drops the 30382s signed by services no 10040 names (never automatic; it refuses outright when no 10040 is readable, since that state is indistinguishable from a corpus mirrored before its provider lists).

### Operator settings (`EngineTuning`)

Every operator setting this library reads — the `VESPA_*` variables and `GUARD_OWNERS_DISABLE` —
is a field of `EngineTuning` (store root), documented there with its variable, range and the
code that reads it; `EngineTuning.VARIABLES` lists them. `open(tuning = EngineTuning.fromEnv(env))`
parses STRICTLY (every bad variable named in one exception); a field left null is read the old,
lenient way, so `open()` without a tuning is unchanged. Per-store settings travel as values
(`ClientTuning` into the engine client, `guardOwnersDisabled` into the store); the few read as
constants from places no store instance reaches (fan-outs, the match-phase depth, the dedup chunk,
the trust walk's page and slice) are `ProcessKnob`s, pinned once per process by `open()`. **A new
knob goes on `EngineTuning`** (field, `fromEnv`, `VARIABLES`) — never a bare `System.getenv` in the
class that reads it, which is how ~25 lenient reads accumulated.

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
  **Never pooled across observers**: a read carrying two `observer:` lenses
  resolves a gate PER observer and unpacks a declaration only if every lens that
  could have found it enrolled the signer. One observer's service key may not
  unpack for another, and `accepts` deliberately ignores the terms, so "which
  filter found it" is not a question the page can answer — being unanimous is
  the only rule that does not depend on the filter order.
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
- **Lookups are batched by the QUERY, and only the floor can split them.** A
  per-key weight never forces two pointers apart — each key carries its own
  number — so every row that would send the same query shares one round trip:
  all unscored references (a label, an assertion: no weights, no floor), and all
  scored ones when `subjectFloorSpan` is null. Only `query(pointer_rel)` varies
  per pointer, so a page of N *differently-ranked Trusted Lists* costs N, and a
  page of N labels costs ONE. That distinction is load-bearing and was measured:
  an early cut gave every row its own lookup and turned a page of twenty labels
  from 1 round trip into 20 — labels being most of a relay's searching traffic.
  `a page of labels costs one lookup, however long the page` is the guard.
- **A member is placed by what was SAID about it, not by who signed the list**
  (`event.sd` §13.1). Most specific answer first: (a) the list's own score for
  that member, (b) the member's own rank under this observer when the list
  scored nobody, (c) neither — and only then the signer, through the floor
  below. Under (a)/(b) the score is the band the POINTER earned with its words
  (`query(pointer_text)`, carried out of the pointer's match-features) times the
  trust curve over that member's number, which is the same shape as every
  organic hit on the page. The signer's own rank — the one thing
  `query(pointer_rel)` carries — then cannot move a scored member at all, and
  that is the point: a NIP-85 provider is a key the reader APPOINTED in their
  10040 and nobody follows (measured on staging: rank 0 for a reader who has not
  enrolled it, 26 for one who has), while `TrustRecompute` has always read that
  appointment as delegation — it stores the provider's asserted rank AS the
  observer's trust, at face value. Ranking now agrees with the write path.
- **Placement is the ENGINE's number, on ONE scale.** A scored member is fetched
  under `spliced_member` (`event.sd` §13): its own rung (the affiliation band,
  widened by its confidence) OR — where §13.1 does not apply — a floor at a
  share of the POINTER that found it, whichever is higher, and **neither carries
  the member's own trust**. The floor is what the rung cannot
  express: the member matched none of the query's words, so only the pointer knows
  the query, and a 4,000 x wot ceiling cannot reach a 130,000 x wot title match
  from below (measured on staging: a `Verified Human` list at #10 on its title, the
  member it is 87% sure of and the reader ranks 100 at #40, under 27 mirror pages
  from one rank-30 bot). The floor is a SPAN of rungs, not a share of an arbitrary
  number — `w_subject_floor_span` defaults to `w_near_tier / w_name_tier`, so a
  member lands within one rung of its pointer however doubted, ordered inside that
  span by confidence.
- **`wot_mult()` IS NOT IN THE MEMBER RUNG, and removing it is what made the floor
  work.** The floor is a share of `query(pointer_rel)`, which is the pointer's
  FINISHED relevance and so already carries the SIGNER's trust — and a Trusted
  List is signed by a NIP-85 service key nobody follows. Multiplying the rung by
  the MEMBER's trust put the two sides of the `max()` in different units: the
  floor's side times 1, the rung's times up to 251,189, so the floor never bound.
  Measured on the production corpus (page span 610..2.39e5): with the trust term,
  members scored 2.19e8..1.00e9 — four orders of magnitude above the page, ordered
  by trust rather than by the publisher, and the SAME whether the list matched on
  its title or barely at all; without it, 1.70e5..2.39e5 behind a title match and
  2.85e3..4.00e3 behind a weak one. Putting `wot_mult()` on the FLOOR instead was
  measured and is worse — once the floor wins the max the score is
  `wot_mult x floor`, so trust decides the order again and members reach 6.02e10.
  A member's trust still decides ADMISSION (the gate runs on the lookup); it does
  not decide placement. `SplicedMemberWeightsIT` pins that a reader who trusts a
  member at 90 sees it on the same rung as one they have never heard of. Nothing is computed in the store: the two placements it
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

Only kinds Quartz parses as `SearchableEvent` are searchable. The per-kind decomposition lives UPSTREAM in Quartz (`SearchFieldExtractor`/`IndexableFields`, beside the kinds themselves); `mapping/SearchExtractors` is this store's thin wrapper applying Vespa sanitization and the join/weighting policy (the kind table is in README). Search-string extensions (`observer:`, `sort:rank`, `filter:rank:gte:N`, `include:spam`, and the `-word` / `"exact phrase"` term syntax) are interpreted by the store; ranking profiles live in `event.sd`. `sort:links` (prototype) is the one ordering the store does itself: a second step over the page's head scores each hit's inbound links per trusted PERSON (`EventIndex.scoreLinks`: a grouping, a zap-receipts read — a zap's person is its `P` zapper, not the LNURL server that signed it — and a zapper-trust read on `reputation.sd`'s `trust_cell` profile; signed, per the store's `LinkWeights`) and `search/LinkRerank` rescales the engine's scores by them, capped at ×3 so a body match cannot overpower a primary-field one. Like any port member, adding to it means `TrustProjection` and `MeteredEventIndex` in the same commit. The text ladder is BANDS, not a score, and WHICH FIELD ANSWERED decides the band: `event.sd` §12.2 puts a multi-word query no single naming field answered — `Verified` in a name, `human` in a bio — one rung below the token band (`w_split_tier`), where a prefix/typo hit on the whole query already sits, so crossing it takes ~1.93x the trust delta instead of ~1.14x. Single-word queries cannot reach that rung by construction (`naming_coverage()` is 1.0 at one word), which is why every calibrated pin holds. Recency is weighted per QUERY shape: a search whose kinds are exactly `[1]` ranks at `mapping/FilterMapping`'s `NOTE_RECENCY_WEIGHT` (30, blind-judged on real note searches), every other at event.sd's 0.1 — the same 30 everywhere broke profile search. The perfect-match rung on a ONE-word query is damped by how common the word is as a name (`event.sd` `perfect_share()`: documents carrying it in a naming field over documents indexed, measured on a production sample, confirmed on held-out topics, and off on an index under 100 000 documents, whose statistics are too small to trust). On a multi-word query, a THIN split — a naming field carrying under half the typed words, the rest elsewhere — scores on the body rung, not the split rung (`w_split_min_coverage`), and a body holding the typed words adjacent and in order gets a weak-rung band (`w_phrase_pop`, read by `body_phrase()` through the rank-only `rank(words, phrase)` item `EventYql` adds on the `search` profile only — drop that item and the feature silently reads 0, since the word groups interleave synthetic joined pairs between the words; elsewhere the item is not ranking-neutral, it moves `bm25(search_text)`. The run it needs is the phrase's Vespa TOKEN count, `query(phrase_terms)` from `EventYql.phraseTokens`, never the word count: Vespa splits `don't` in two). When changing ranking: **`docs/search-evaluation.md` is the protocol** — what to run, what to read, what counts as a win. In short: cases live in `benchmark/rank_cases.json` (add one for every reported bad search, with its `intent`), graded judgments in `benchmark/search_judgments.json` (grow them with `rankAb --pool` + `--merge`, blind, with provenance); A/B with `./gradlew :benchmark:rankAb` against a live Vespa (no redeploy needed) and read its verdict block — paired per-topic nDCG@10 / success@k with a randomization test, Holm across a sweep, and the minimum detectable effect; a change ships when no pinned case is LOST, the nDCG@10 CI's low end clears −0.01, every listed per-topic loss is explained in the PR, and the age / author-share / latency guardrails hold. `RankRegressionIT` (and the other ranking ITs) must stay green.

## Conventions

- Comment culture: this codebase documents **why** (invariants, contracts, measured trade-offs) in KDoc and inline comments, densely. Match it — and update the comment when you change the behavior it explains.
- CI (`.github/workflows/build.yml`) runs three jobs on PRs to `main`: `spotlessCheck`, `build` (unit), and `integration` (`:benchmark:test -Pintegration`). All three must pass.
- **A green gate is not a run gate.** Locally, `:benchmark:test -Pintegration`
  reports `BUILD SUCCESSFUL` **in 6 seconds with every IT skipped** when Docker
  is not up — each one aborts on `Assumption failed: Docker not available`. Judge
  it by the counts, never the exit code:
  ```bash
  grep -ho 'tests="[0-9]*" skipped="[0-9]*"' benchmark/build/test-results/test/*.xml
  ```
  A real run is ~10 minutes with 20 of 21 executing (`StagingCorpusIT` needs a
  `STAGING_CORPUS` export). Don't run it concurrently with a unit suite — they
  contend for the daemon and report compile failures that aren't real.
- **A new test must be verified to fail without its fix.** Revert the change,
  watch the test go red, restore. Tests written here have passed identically with
  and without the behaviour they guarded — an assertion that only checks "past
  the first batch" is satisfied by the bug too. This is also how dead code gets
  shipped: `EngineResources` merged parsed, tested, and constructed nowhere,
  because the tests drove the parser seam rather than the wiring.
- **Say only what was measured, and say the sample.** A width theory was written
  into `VespaEventIndex` as fact ("Width does"), shipped, and contradicted by its
  own counters within twenty minutes: 90% of production cuts were on windows of a
  day or less, not the wide ones four hand-run queries had suggested. Four
  queries are an anecdote. When the mechanism is not understood, write that down
  instead of inventing one.
- Design docs in `docs/` (`scaling.md`, `multi-node-consistency.md`, `server-side-constraints.md`, `attribute-memory.md`, `embedded-vespa-study.md`, `recency-ranking.md`, `telemetry.md`, `locking.md` — every lock and blocking call, its verdict, and the owner-striped design that would replace the writer lock, `trust-gate-filter.md` — the observer gate as an imported match filter, measured and not shipped, `gated-count.md` — cheaper gated COUNTs: by author (exact) wins, sampling rejected, `search-evaluation.md` — how a search change is judged: topics, graded judgments, the measures, the statistics and the ship rule) record operator guidance and studied-but-rejected alternatives — check them before re-proposing one. `attribute-memory.md` is the per-field RAM budget of `event.sd` — read it before adding an attribute or reaching for `paged`.
