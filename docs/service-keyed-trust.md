# Service-keyed trust tensors — the plan, and what the prototype measured

The review that led here (2026-09-05): a kind-30382 insert on staging could wait
14 seconds or more on the trust gate, and the reason is not the insert. The
reputation tensors are keyed by OBSERVER, so the projection materializes every
observer's kind-10040 pointer at write time — a card fans out to every observer
naming its signer, and any 10040 write (a re-sign included) re-derives every
subject the named service ever scored. This document records the design that
removes that, the prototype that proves it works against real staging data, the
numbers, the build-and-test plan, and a review of how the relay syncs the NIP-85
kinds against it.

## 1. What NIP-85 says, and what the projection did with it

Under NIP-85 a score is a property of the pair `(service key, subject)`: a
kind-30382 is signed by the service, addressed at `(30382, service, subject)`,
and supersedes by NIP-01. The observer's kind 10040 is a POINTER — "for
`30382:rank`, read service P's cards" — and nothing else. Consumers resolve the
pointer when they read (Amethyst: `UserCardsCache.rankFlow` picks the card whose
author equals the 10040 entry's pubkey).

The store instead stored the RESOLVED pointer: `influence_scores{observer}`. Three
costs followed, all measured or read off the code:

| operation | what the observer-keyed projection did | cost |
|---|---|---|
| one card via `insert()` | subject dirt → `deriveBatch`: fetch EVERY 30382 about the subject, fold, rewrite the whole parent | one `#d` query returning up to one card per scoring service (341 on staging) |
| a 10040 write, even a re-sign with identical entries | `opDirt` + `removeDirt` both dirty every named service → `recomputeWalk` over every card the service signed, twice (put and remove each re-stamp the entry) | 2 × O(cards of service) derives; the baseline below measured 115 s for a 178k-card service on a local single node; the code's own staging measurement is 1.9 s per 50 subjects, so ~1.9 h per walk |
| the mirror's steady state: a provider re-publishing its corpus | the bulk planner supersedes through `removeDocs(old)` → subject dirt → one derive per superseded card | O(cards) derives behind every re-publish |

Every derive holds the trust gate for a 500-subject slice (`GATE_SLICE`, ~19 s on
staging), and every 30382, 10040, kind 5 and kind 62 insert queues on that gate.
That is the 14 seconds.

**The shape of the corpus makes this worse, not better.** Measured on staging
2026-09-05 through the relay (read-only, `include:spam`):

| what | count |
|---|---|
| kind 10040 lists | 371 |
| distinct `30382:rank` / `30382:followers` services named | 341 |
| observers per service | 1 (personalized keys: a service per user) |
| kind 30382 cards, total | 84.8 M |
| cards from named services | 28.0 M |
| cards per named service | 150 k – 280 k |
| kind 30383 / 30384 / 30385 | 9.2 M / 104 k / 25 k |

So a subject is scored by hundreds of services, a 10040 names one service per
dimension, and a "provider swap" is one user's list moving from their old
personalized key to a new one that carries ~300k cards.

## 2. The design

**Key the tensors by the signing service.** `influence_scores{service}` and
`follower_counts{service}` on the reputation parent; a cell is the newest card
at `(service, subject)`, both tags. The observer's 10040 is resolved AT QUERY
TIME: the store hands `user_q = {<rank service>: 1.0}` and a new
`followers_q = {<followers service>: 1.0}` to the profiles (`EventQuery.rankKey`
/ `followersKey`, filled by `NostrSemanticsStore.lensed()` off the cached
`ProviderMap`, the same pass the search gate already reads per query).

What each write becomes:

| write | reaction |
|---|---|
| a card by a service some 10040 names | ONE tensor update on its subject: `add` the cells it carries, `remove` the dimension it dropped (retraction) — atomic, no read, `max_rank` raised in the same update as before |
| a card by a service nobody names | nothing (as today: dead storage until a list names the signer) |
| a 10040 write | drop the provider-map cache; for each service the list names that NO stored list named before, queue a **service walk**: its card ids stream off the search index (`visitIds`, cursor-paged), each page fetched by id and applied as cells under the gate — O(cards of that service), once ever per service |
| a 10040 re-sign, or a swap to an already-named service | nothing at all |
| a card removal (kind 5, vanish, sweep) | a tensor `remove` of its cell, no read, no derive |
| supersession | one reaction, the winner's cells; the versions it replaced need no hook (same key) — `TrustProjection.putIfNewer` replays the read-then-supersede on the inner index itself |

What stays: the `DirtLedger` (crash insurance and the deferred drain), the
`TrustReconciler` (verify/repair through the exact derive, now keyed by service),
the orphan sweep, `max_rank` and the trust descent (the max over service cells
is still an upper bound for any lens — a removal can leave it stale-high, which
costs a rung and never correctness).

**Policy decisions the design forces, and what the prototype chose:**

- *Multi-provider per dimension.* NIP-85 prescribes no merge; Amethyst holds one
  slot per metric. The prototype resolves the FIRST entry per dimension in the
  10040 (`TrustProviders.Lens`); every named service still projects, only the
  lens picks. `sum` over a one-key tensor is the value, so the rank profiles
  keep `sum(query(user_q) * attribute(...))` unchanged. A `max` reduce is the
  alternative if a merge is ever wanted.
- *A card lands whole.* A service named for `rank` only still writes its
  followers cell; the lens decides which dimension a query reads. The old
  per-dimension gate at write time is gone (it existed to stop a rank provider's
  followers tag from overwriting the follower provider's cell — impossible when
  cells are keyed by the service).
- *Emptied parents stand.* A cell `remove` cannot know it took the last cell, so
  a parent may hold empty tensors until a derive (verify with repair) drops it.
  `TrustReconciler.matches` treats an empty parent as absent.
- *Expired cards.* A cell path never re-reads, so an expired card's cell stays
  until the NIP-40 sweep removes the event (which removes the cell). The derive
  still excludes expired cards, so a repair drops it earlier.
- *Only named services project.* The reputation type is global and
  memory-resident (`docs/attribute-memory.md`), and on staging 33.8 M of 51.9 M
  cards were by services nobody names. Projecting everything would remove the
  service walk entirely at that memory price; not taken.

## 3. What the prototype measured

The prototype is the design above, complete enough to run: schema, port, client,
projection, ledger, reconciler, query resolution, and the unit tests rewritten
to the new semantics (store + engine: all green). It lives on the worktree
branch `claude/trust-score-injester-perf-proto`, a 20-file diff against `main`.

The harness is `:benchmark:trustProbe` (`TrustProbe.kt`, committed on this
branch): the 371 real 10040s and two real providers' 30382 corpora captured
read-only from staging (177,848 cards of the Brainstorm key, whose lists the
canonical observer `460c25…065c` names; 234,272 of a second key `48ec01…`), fed
through the real write path into a fresh single-node Vespa (Docker, 8 GB), then
the four operations a relay actually sees, with fresh cards inserted on a clock
during each walk to read the gate wait a client pays.

| operation | observer-keyed (main) | service-keyed (prototype) |
|---|---|---|
| bulk ingest, 412k cards + 371 lists, batch 1000 | 324 s to settled (`proj.write` 104 s, `proj.fetch.maxrank` 91 s) | 315 s to settled (`proj.write` 90 s, `proj.fetch.maxrank` 95 s) |
| single card insert, drain idle (client path) | median 32 ms, max 78 ms | median 30 ms, max 68 ms |
| observer re-signs an UNCHANGED 10040 | settled after **114.7 s** (two full walks: 733 derive slices); card inserts during it median 218 ms, max 508 ms | settled in **0.0 s**; nothing to walk |
| observer SWAPS providers (7d7ffd → 48ec01) | **NOT settled after 240 s** (2278 derive slices and counting, both services re-walked); card inserts median 226 ms, max 442 ms | settled in **0.0 s** (48ec01 was already named by another list, so its cells existed; a never-named service costs one index-driven walk) |
| the provider re-publishes its 177,848 cards (every one a supersession) | fed in 266.5 s (667 ev/s), projection settled at **333.6 s** — 628 derive slices behind the supersessions, the gate held 95 s, trust inserts waited 26 s | fed in 132.9 s, projection settled with the feed, **0 derives** |
| `sort:rank` under the observer after the swap | orders by the new provider's ranks | the same order |

Every number is a local single node; the gate holds scale with the corpus
(staging: 84.8 M cards, ~38 ms per derived subject). Locally a re-sign is two
minutes; on staging it is the two-hour walk the review found, and the swap is
that twice.

One thing the design had to get right that the first cut did not, recorded
because it is the trap the real PR must not fall back into: the first service
walk paged the document API (`visitDocsPage`), which is a scan of the WHOLE
event corpus per service (the relay measured 75 s per such walk at 319 M
events) — with 341 named services it held the gate for 255 s during the bulk
ingest and doubled its settle time (568 s). The walk now streams ids off the
search index (`visitIds`, the cursor the old `recomputeWalk` used) and fetches
each page by id under the gate: `proj.fetch.page` 0.1 s over the whole run.

**Status (2026-09-05): landed on this branch.** Everything in §4 is done —
the engine, the store, the tests in §5 (`ProviderMapTest`, `TrustLensTest`,
`TrustKeyingMigrationTest`, `ServiceKeyedTrustIT` are the new ones) and the
docs — with two deliberate departures from the plan below: the tensor
dimension keeps its `user{}` spelling (a rename is a field type change that
needs a validation override on a global document type; the label is
invisible to every caller), and the migration is automatic rather than an
operator step (`TrustKeyingMigration`, §6). The probe's numbers in §3 are the
prototype's; the landed code is the prototype plus the renames, the
migration and the test coverage.

## 4. Build plan

One PR for the store, in this order, each step green on its own:

1. **Engine.** `reputation.sd` comment (shape unchanged: `tensor<int8>(user{})`,
   keys become service pubkeys — rename the dimension to `service{}` here, and
   in every `query(user_q) tensor<float>(user{})` input); `event.sd`:
   `query(followers_q)` input on `text_relevance`, `verified_followers()` reads
   it; `EventQuery.rankKey/followersKey`; `EventYql` emits `user_q`/`followers_q`
   from them (`{}` when unresolved — the gate stays closed, never the observer's
   own key); `ReputationCells.dropInfluence/dropFollowers` and
   `ReputationIndex.removeCells` (Vespa tensor `remove` by address; a pure
   retraction never creates a document; in-memory spec updated).
2. **Store.** `ProviderMap`: `TrustProviders.lenses` (first entry per
   dimension). `TrustRecompute`: `derive` keyed by signer, `applyCards` (the hot
   path), `projectServices` (index-driven walk). `TrustProjection`:
   `supersedesViaPut` forwarded, `putIfNewer` reacts once, `put/putAll` →
   `react`, removals → `unreact` (cell remove), `freshServicesOf` computed
   BEFORE the write lands. `DirtLedger.drain` walks services with
   `projectServices`. `TrustReconciler.reconcile` samples the service's own
   cell; rebuilds through the walk. `NostrSemanticsStore.lensed()` on the three
   query sites. Rename `ReputationCells.observer` → `key`.
3. **Docs.** This file; `README.md` §"Where trust comes from" (cells are per
   service, the lens resolves per query); `docs/attribute-memory.md` (per-doc
   size is now the number of scoring services); `docs/locking.md` (the trust
   gate's remaining holds: one cell page per walk slice).
4. **Relay follow-ups** (`vespa-relay`, separate PR): none required for
   correctness. See §6 for what gets cheaper and one sweep to widen.

## 5. Test plan

**Unit (no Vespa)** — rewritten in the prototype, all green:

- `TrustProjectionTest`: cells keyed by service, lens resolution
  (`scores land keyed by the service key…`, `switching providers…` asserts the
  lens moves and the newly named service's card becomes a cell, `dropping a
  shared provider detaches only that observer` on the lens), retraction in the
  same update, emptied parents, bulk == sequential on cells, deferred mode
  queues the WALK and applies cards inline, the marker write-ahead accounting.
- `DirtLedgerTest`: the mid-walk card keeps its newer value; a bulk write-ahead
  during a round keeps the marker covering the service being walked.
- `TrustReconcilerTest`: `reconcile catches a service whose cells sit under
  another key`; a rank-mapped service whose cards assert only followers is
  clean; verify treats an emptied parent as absent.
- `EventYqlTest`: `user_q`/`followers_q` carry the resolved keys; an unresolved
  lens emits `{}`.
- Add: `ProviderMapTest` for `Lens` (first entry wins, per dimension, private
  entries invisible), `NostrSemanticsStoreTest` for `lensed()` (a context
  observer and an `observer:` token both resolve; a store without the
  projection resolves nothing).

**Integration (`-Pintegration`, real Vespa)** — the schema executes only there:

- `ObserverGateIT`, `RankRegressionIT`, `MemberTrustIT`, `SplicedMemberWeightsIT`,
  `MaxRankRaiseIT`, `SearchCountIT`: seed reputation documents by SERVICE key
  and query through a 10040 (or set `rankKey` directly at the engine level).
- New `ProviderSwapIT`: two providers, one observer; assert the page under the
  observer follows the provider the CURRENT 10040 names, before and after a
  swap, with no reputation document rewritten (count feed operations through a
  counting `ReputationIndex`).
- New `RetractionIT`: a card losing its rank tag removes the cell in Vespa
  (tensor `remove` by address), and a pure retraction creates no document.
- New `ServiceWalkIT`: a 10040 naming a service whose 300k cards are already
  stored projects them in O(cards) — assert the walk's `proj.fetch.page` call
  count equals the id pages, and that a card inserted mid-walk keeps its value.
- `VespaParityIT` is unaffected (recall is unchanged); run it anyway.

**Performance gate** — `trustProbe` against a fresh Vespa with the captured
corpora, before and after; the re-sign and swap rows must read 0 walks.

**Parity oracle** — `store.verifyTrust()` after the probe: the exact derive
(keyed by service) must call the cell path's tensors clean.

## 6. Migration and rollout

- The schema change is additive on `event.sd` (a query input) and semantic on
  `reputation.sd` (same shape); deploy as usual, no validation override.
- Existing reputation documents carry observer keys and are WRONG under the
  new lens, and `VespaEventStore.open()` repairs that itself:
  `TrustKeyingMigration` runs once in the background — a reconcile (every
  named service samples as unprojected and is walked into cells; a relay that
  reconciles at boot has already done this half), then a sweep that removes
  every cell whose key no 10040 names (the old observer keys, and services no
  list names any more), then a marker document so the next boot reads one
  document and returns. `awaitTrustKeying()` is the barrier; a run that finds
  reputation documents but no readable 10040 refuses and retries, since that
  is an engine still serving its corpus. Until the walk of a given service
  lands, a lens resolving to it serves an EMPTY page (the gate fails closed),
  and `backgroundStatus()` names a migration that keeps failing.
- Cost on staging's shape: one cell write per card of every named service
  (28 M) plus one page per reputation document for the sweep — the same order
  as the relay's existing boot reconcile, once.
- The `max_rank` backfill marker survives; a sweep can leave `max_rank` high
  on a document, which is an upper bound still.
- `DirtLedger` markers from the old process name subjects: the new derive
  re-derives them keyed by service — safe.
- `sweepOrphanScores()` is unchanged.

## 7. The sync review: 10040, 30382, 30383, 30384 in `vespa-relay`

Read against the relay's `router.conf.example`, `SyncEngine`, `VisitPool`,
`RetractionAudit`, `IngestPipeline` and `RelayMain` (clone at 2026-09-05).

**Where each kind comes from.**

- **10040**: the `profileViaOutbox` stream (`kinds: [0, 10002, 10040]` from every
  monitor-certified relay; negentropy over the past week, re-fetch monthly), and
  the monitor's own 10040 source, which reads every delegation tag (38 named
  tags, `30382:rank` … `30395`) to discover provider relays and bind
  `(relay, provider)` asks. A 10040 re-fetched from another relay is a duplicate
  at the store (no write, so no reaction) — only a NEW version reaches the
  projection.
- **30382–30385**: two streams. `assertions` is the narrow one — one ask per
  `(provider relay, service key)`, `ownedKinds` = the eight declaration kinds,
  `deleteMissing = true` with a daily negentropy reconcile: records the
  provider's relay no longer serves are DELETED by id in chunks of 500
  (`store.delete(ids)`). `contentViaOutbox` is the wide one (every kind worth
  searching, the eight declaration kinds included), from every certified relay.
- **Ingest**: `IngestPipeline` batches of `SYNC_INGEST_BATCH` (default 1000, two
  workers) through `batchInsert`, after a by-id dedup probe; its supersession
  pre-drop covers replaceable NON-addressable kinds only, so 30382 supersession
  is the store's bulk planner's job (versions read, `removeDocs`, `putAll`).
- **Boot**: `RelayMain` runs `reconcileTrust()` in the background unless
  `TRUST_RECONCILE_ON_START=false`; `SyncMain` opens the store without it.

**What the service-keyed projection changes for each path.**

| relay path | today (observer-keyed) | with service-keyed cells |
|---|---|---|
| a provider re-publishes 300k cards (the `assertions` stream's normal catch-up) | bulk cells inline, then one derive per superseded card behind `removeDocs` — hours of gate slices | one conditional put + one cell update per card; no derive |
| `deleteMissing` retracts N cards | N derives (each a `#d` fetch) | N cell removes, pipelined |
| a user re-signs their 10040 (Amethyst re-signs on any edit) | two full walks of the named service | nothing |
| a user swaps to a new personalized key | walk the old service AND the new one, whole-document rewrites | one index-driven cell walk of the new key if nobody named it yet; the lens moves at once |
| kind 5 / vanish of a card | derive | cell remove |
| `reconcileTrust()` at boot | per-service sample (341 queries) + derive walks for unprojected services | same sample, cell walks |

**Findings on the sync itself.**

1. **The `assertions` stream deletes by id, 500 per call.** Correct for the
   store either way; with cells it becomes cheap. Keep `dryRun` first on a new
   deployment, as the config says — the reconcile is the licence, and there is
   still no size guard.
2. **The wide `contentViaOutbox` stream mirrors 30382–30385 from every relay,
   so most stored cards are by services nobody names** (33.8 M of 51.9 M on the
   operations page; 56.8 M of 84.8 M on staging today). Under either design
   they project nothing and cost storage plus the bulk planner's version reads
   on every re-mirror. `sweepOrphanScores()` covers 30382 only; 30383–30385
   (9.3 M) have no sweep. Widen the sweep to the four assertion kinds, keyed by
   signer against the 10040s' per-kind delegations (`Delegations` already knows
   `30383:*` entries), and consider dropping the assertion kinds from the wide
   stream: the narrow stream reconciles them from their source of truth.
3. **Personalized keys mean the swap IS the common case.** With one service per
   observer, every time a provider rotates a user's key (or a user changes
   provider) the store has to bring ~300k cards into cells for a service nobody
   named before. Under the new design that is one index-driven walk, gated per
   1000-card page — the relay should expect the lens to serve nothing for that
   observer until the walk lands (minutes, not hours), and the `assertions`
   stream's `(relay, provider)` ask for the new key is what brings the cards in
   first. Nothing in the relay needs to change for it; `observer_stats.html`
   already shows per-observer card counts on both sides, which is the right
   place to see the walk catching up.
4. **Ordering.** Lists before cards is the healthy order for the projection
   (a 10040 naming a service with no stored cards costs one empty index query).
   The profile stream's weekly negentropy and the assertions stream's daily one
   make that the steady state; a fresh mirror that pulls cards first pays one
   walk per service at the first reconcile, exactly what `reconcileTrust()`
   does today.
5. **`dropSuperseded` in `IngestPipeline` skips addressable kinds on purpose**
   (the comment says the store owns that), so nothing there needs to change;
   the store's bulk planner already keys 30382 winners by address.

## 8. Open questions

- The tensor dimension's `user{}` spelling — see the status note above; a
  rename needs a validation override and buys a label.
- Whether to project every card regardless of a 10040 naming its signer, which
  deletes the service walk at a global-document memory cost; measure on
  staging's numbers before deciding.
- The relay's `observer_stats` page counts cards per observer's provider; it
  reads events, not tensors, so it is unaffected — confirm.
- `GATE_SLICE` becomes the walk's page size (1000 cards per hold); the old
  500-subject derive slice no longer exists on any write path.
