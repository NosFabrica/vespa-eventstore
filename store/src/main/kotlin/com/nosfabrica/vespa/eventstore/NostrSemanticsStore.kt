/*
 * Copyright (c) 2026 NosFabrica
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nosfabrica.vespa.eventstore

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.async.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.async.forEachBounded
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.ingest.BulkMixedInsert
import com.nosfabrica.vespa.eventstore.ingest.BulkRecordInsert
import com.nosfabrica.vespa.eventstore.ingest.Deletions
import com.nosfabrica.vespa.eventstore.ingest.EventAdmission
import com.nosfabrica.vespa.eventstore.ingest.GuardOwners
import com.nosfabrica.vespa.eventstore.ingest.Rejections
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.nosfabrica.vespa.eventstore.mapping.toEventQuery
import com.nosfabrica.vespa.eventstore.runtime.DEFAULT_GUARD_REFRESH_MILLIS
import com.nosfabrica.vespa.eventstore.runtime.WriteLocks
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import com.nosfabrica.vespa.eventstore.search.PageAssembly
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.nosfabrica.vespa.eventstore.search.SearchReferenceExpansion
import com.nosfabrica.vespa.eventstore.search.SearchReferences
import com.nosfabrica.vespa.eventstore.search.SubjectKeys
import com.nosfabrica.vespa.eventstore.search.isRanked
import com.nosfabrica.vespa.eventstore.trust.Delegations
import com.nosfabrica.vespa.eventstore.trust.Enrolment
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.sync.Mutex
import kotlin.coroutines.coroutineContext

/**
 * The Nostr-semantics layer: a Quartz [IEventStore] backed by the search
 * engine itself — one copy of the data, queryable with full NIP-01 filters
 * plus NIP-50 search. It is engine-agnostic (any [EventIndex] works, including
 * the in-memory one); [VespaEventStore.open] assembles it over Vespa.
 *
 * [EventAdmission] enforces the Nostr write rules: dedup ("duplicate:"),
 * replaceable/addressable supersession with the NIP-01 tiebreak — same
 * created_at, LOWEST id wins — ("replaced:"), NIP-09 deletions and NIP-62
 * vanishes ("blocked:", enforcement in [Deletions], keyed on the event's
 * OWNER — the gift-wrap recipient for kind 1059, else the author),
 * already-expired events rejected and due expirations swept (NIP-40), and
 * ephemeral kinds accepted WITHOUT storing (an observable wrapper still
 * broadcasts them live).
 *
 * NIP-50: only kinds implementing SearchableEvent are searchable, via
 * [SearchExtractors]. Filters arrive with `search` verbatim; this store
 * interprets the `sort:`/`filter:rank:`/`include:spam`/`observer:` extensions
 * and the `-word` / `"exact phrase"` term syntax, and ignores extensions it
 * doesn't know. A resolved observer also trust-gates PLAIN recall — the
 * observer gate, see [toExpiryQuery].
 *
 * Correctness rests on two properties: all writes serialize behind one
 * [Mutex], so query-then-write is atomic against other writers in this
 * process; and [EventIndex] guarantees an acked put is visible to search.
 * There are no cross-document transactions — [transaction] buffers and applies
 * sequentially without rollback. Both properties are about writes THIS process
 * makes; when another process feeds the same index, say so with [writers] —
 * the guard-owner cache is the one piece of state that would otherwise trust
 * this process's writes to be all of them.
 *
 * Events are NOT verified here; verification is the ingest path's job, once,
 * before insert.
 */
class NostrSemanticsStore(
    private val index: EventIndex,
    override val relay: NormalizedRelayUrl? = null,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
    /**
     * Whether anything ELSE writes [index] — an assertion only the caller can
     * make, since the store cannot see a second feeder. The guard-owner cache is
     * self-maintaining only for a writer that sees all of its owners' guards, so
     * the default [WriterTopology.SHARED_STRICT] caches nothing and probes every
     * insert. See [WriterTopology].
     */
    writers: WriterTopology = WriterTopology.SHARED_STRICT,
    /** Guard-cache rebuild cadence under [WriterTopology.SHARED]; 0 disables the refresher. */
    guardRefreshMillis: Long = DEFAULT_GUARD_REFRESH_MILLIS,
    /** Events removed per sweep round (see [Deletions]). Internal — a test seam, like [nowSecs]. */
    internal val sweepPage: Int = 10_000,
    /**
     * How much of a searching read's answer may be events it POINTS AT rather
     * than events it matched — see [SearchReferenceExpansion]. Always applied,
     * because it only ever engages on a read carrying TERMS: a plain NIP-01
     * recall, a mirror's paging, a NIP-77 catch-up and this store's own
     * provider-list read all carry none, and are left exactly as they were.
     * [SearchExpansionLimits.Off] turns it off outright.
     */
    private val searchExpansion: SearchExpansionLimits = SearchExpansionLimits.Default,
    /**
     * HOW MANY HITS ONE AUTHOR MAY HOLD ON A RANKED PAGE, or null — the
     * default — for no cap at all.
     *
     * Ranking answers "how good is this document"; nothing in it answers "how
     * many of these does one page need". A single Wikipedia-mirror bot took 27
     * of the top 50 for `Verified Human` (measured 2026-09-01), and
     * `rank_cases.json`'s `jack` row has the same shape from a different
     * author — 13 of 40 slots, one account's identical kind-20 pictures. Both
     * are one document being right many times, which is a DIVERSITY problem
     * and not a relevance one: no rung can fix it, because every one of those
     * documents genuinely earned its rung.
     *
     * OFF BY DEFAULT, and this is a deliberate refusal rather than caution. A
     * cap DROPS events a filter matched, so a page can come back shorter than
     * the limit the client asked for — a NIP-01 answer with rows missing, which
     * is the operator's call to make and not a library's. The store adds no
     * cap of its own anywhere else either (see [count]); bounding a read is the
     * FILTER's job.
     *
     * RANKED PAGES ONLY when it is on. A recency-ordered recall is a mirror
     * paging a corpus or a NIP-77 catch-up, where dropping an author's events
     * is data loss rather than an editorial choice; only a relevance page is
     * making a judgement that a cap can participate in.
     */
    private val maxHitsPerAuthor: Int? = null,
    /**
     * WHERE THIS STORE'S RESOURCES GO — see docs/telemetry.md.
     *
     * The store owns the ledger rather than receiving one, because it books the
     * altitude nothing below it can see: ADMISSION OUTCOMES. A duplicate never
     * reaches [EventIndex], so a decorator down there observes a dedup probe
     * and the absence of a write, which is not the same statement. The port
     * calls themselves are counted by `MeteredEventIndex` wrapped around
     * [index], which `VespaEventStore.open` installs.
     */
    val metrics: CostLedger = CostLedger(),
) : IEventStore {
    /** The two writer mutexes and their wait/hold accounting — see [WriteLocks]. */
    private val locks = WriteLocks()

    // Owners with any stored tombstone/vanish; everyone else's inserts skip the
    // NIP-09/62 guard probes entirely (see GuardOwners for the safety argument).
    private val guards = GuardOwners(index, writers, guardRefreshMillis)

    private val deletions = Deletions(index, relay, sweepPage)

    /** The per-event write rules — shared verbatim with [BulkMixedInsert]'s replay. */
    private val admission = EventAdmission(index, deletions, guards)

    /**
     * WHO THIS READER ASKED TO COMPUTE WHAT — the gate the search expansion
     * checks before unpacking a Trusted List or a NIP-85 assertion.
     *
     * Read off [TrustProjection] rather than derived here, and the type check is
     * the honest statement of the dependency: the projection is the decorator
     * every write already passes through, so its [ProviderMap] is invalidated by
     * every 10040 put and remove with no write-path code of its own. A second
     * cache in this class would need its own hook on four separate write entry
     * points and would still be a second answer to one question.
     *
     * A store assembled WITHOUT the projection — a bare index in a test — has no
     * Map to read and so admits no declaration. Labels are unaffected: NIP-32 is
     * ungated by design, and this gate never applied to them.
     */
    private suspend fun delegations(): Delegations = (index as? TrustProjection)?.recompute?.delegations() ?: Delegations.NONE

    /**
     * THE LENS, RESOLVED: the reputation tensors are keyed by SERVICE key, so
     * a query carrying an observer is handed the service their kind 10040
     * names per dimension ([EventQuery.rankKey], [EventQuery.followersKey])
     * off the projection's cached provider map — the same pass the write
     * side and the search gate read, invalidated by every 10040 write. An
     * observer with no stored list resolves to no key and ranks as trusting
     * nobody, which is what an observer with no cells ranked as before. A
     * store assembled without the projection has no map and resolves nothing.
     */
    private suspend fun lensed(queries: List<EventQuery>): List<EventQuery> {
        if (queries.none { it.observer != null }) return queries
        val recompute = (index as? TrustProjection)?.recompute ?: return queries
        val providers = recompute.providerMap()
        return queries.map { q ->
            val lens = providers.lensOf(q.observer ?: return@map q)
            q.copy(rankKey = lens.rank, followersKey = lens.followers)
        }
    }

    /**
     * The read path's page assembly — ordering, splicing, narrowing, the cap.
     * Handed the enrolment supplier rather than the projection: on a relay
     * holding no 10040s that read is never cached, so it resolves at most once
     * per observer, and only on the paths that consult the gate.
     */
    private val pages = PageAssembly(searchExpansion, maxHitsPerAuthor, { observer -> if (observer == null) Enrolment.NONE else delegations().of(observer) })

    /** How the page assembly reads and recalls a stored document. */
    private val docPage =
        PageAssembly.Projection(
            newestFirst = EventDoc.NEWEST_FIRST,
            keys = DOC_KEYS,
            idOf = EventDoc::id,
            kindOf = EventDoc::kind,
            authorOf = EventDoc::pubkey,
            pointerOf = { if (it.kind in SearchReferences.KINDS) it.toEvent() else null },
            search = { index.search(it) },
            searchRanked = { index.searchRanked(it) },
        )

    /** The same, off a raw row — the projection the wire-serving path uses. */
    private val rawPage =
        PageAssembly.Projection(
            newestFirst = RAW_NEWEST_FIRST,
            keys = RAW_KEYS,
            idOf = RawEvent::id,
            kindOf = RawEvent::kind,
            authorOf = RawEvent::pubKey,
            pointerOf = { if (it.kind in SearchReferences.KINDS) it.toEvent() else null },
            search = { index.rawSearch(it) },
            searchRanked = { index.rawSearchRanked(it) },
        )

    private val bulkRecords = BulkRecordInsert(index, relay, guards)

    private val bulkMixed = BulkMixedInsert(index, relay, nowSecs, guards, sweepPage)

    /**
     * Trust-relevant writes take BOTH gates; everything else takes only
     * [writes].
     *
     * CONSERVATIVE BY CONSTRUCTION — the question asked is "could this write
     * change a reputation document?", and anything that might answers yes:
     * a contact card (30382) and a provider list (10040) are the two kinds
     * [TrustProjection.insuranceFor] books work for, and a deletion or a
     * request-to-vanish can REMOVE one, which is trust work through
     * `TrustProjection.remove`. A kind-1 note, a reaction, a repost and a zap
     * are none of those, and they are the overwhelming majority of what a
     * relay is asked to store.
     */
    private fun touchesTrust(event: Event): Boolean =
        event.kind == ContactCardEvent.KIND ||
            event.kind == TrustProviderListEvent.KIND ||
            event is DeletionEvent ||
            event is RequestToVanishEvent

    private suspend fun <T> lockedForWrite(
        event: Event,
        body: suspend () -> T,
    ): T = locks.gated(touchesTrust(event), WriteLocks.INGEST, body = body)

    override suspend fun insert(event: Event) = withActivity(Activity.Insert) { insertOne(event) }

    private suspend fun insertOne(event: Event) = lockedForWrite(event) { admission.admit(event) }

    /**
     * Run [body] under this store's TRUST writer lock. For the trust
     * reconciler's mutating batches: its repairs derive from a read of the
     * corpus, and racing a live insert would let a derivation from pre-write
     * state land after the insert's own recompute. NOT reentrant (a plain
     * [Mutex]): never call from a path that already holds the lock.
     *
     * Booked under [LOCK_GATE], separately from ingest's own acquisitions: the
     * callers are the projection drain and the reconciler, and telling their
     * hold apart from ingest's is the entire point of the split.
     *
     * ON [trustGate], NOT [writes], since 2026-09-04: this is
     * reputation-document work, so holding it no longer stalls a kind-1 insert
     * that has no reputation work to do. Writes that DO touch reputation still
     * queue for it — see [touchesTrust].
     */
    internal suspend fun <T> withWriteLock(body: suspend () -> T): T = locks.underGate(WriteLocks.GATE) { body() }

    /**
     * [lockedForWrite] for a BATCH that must stay whole: the trust gate is
     * taken when ANY event in it touches reputation.
     *
     * The bulk path writes reputation state inline — `TrustProjection.putAll`
     * ends in `reputations.updateCells` for the cards in the batch — so a batch
     * carrying one card mutates the same documents the drain re-derives.
     * Missing this is how the trust-gate split first shipped: `insert` took
     * both locks and `batchInsert` took neither, which left the mirror's bulk
     * card ingest racing the drain with no mutual exclusion at all. The
     * single-lock design could not have this bug; the split has to earn its
     * exclusion at every write entry point, and there are several.
     */
    private suspend fun <T> lockedForBatch(
        events: List<Event>,
        body: suspend () -> T,
    ): T = locks.gated(events.any { touchesTrust(it) }, WriteLocks.INGEST, body = body)

    /**
     * Batches take a BULK path — the per-event path costs 3–5 index round
     * trips each, which caps ingest in the low thousands per second. Two
     * shapes, by whether the batch mutates via deletions:
     *
     *  - PURE RECORDS: [BulkRecordInsert] chunks the read checks and pipelines
     *    one [EventIndex.putAll]; its dedup reads run outside the writer lock
     *    so parallel relays overlap them (a raced duplicate is an idempotent
     *    re-put); guards and supersession stay under it.
     *  - CONTAINS kind 5/62: [BulkMixedInsert] batch-reads the working set and
     *    replays the per-event rules in memory, order preserved, then writes
     *    the diff.
     *
     * Sub-[BULK_MIN] batches aren't worth the setup and just loop [EventAdmission.admit].
     *
     * A PURE-RECORD BATCH IS SPLIT BY TRUST. The events that touch reputation
     * (cards, provider lists — see [touchesTrust]) commit under the trust gate;
     * everything else commits under [writes] alone, first. The two halves
     * cannot interact: a card's or a list's supersession address is
     * `(kind, pubkey, d)`, which no other kind in the batch can share; dedup is
     * per id; the guard probes are per owner and read-only. So the split
     * changes no outcome (they are merged back by position) and it takes the
     * mirror's 999 notes out from behind the drain slice their one card has to
     * wait for — and holds the gate for one small commit instead of the whole
     * write stage. A MIXED batch (any kind 5/62) stays whole: [BulkMixedInsert]
     * replays it in order because a deletion may target an event earlier in
     * the same batch, and a kind 5 by id may be pointing at a card.
     */
    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> =
        withActivity(Activity.BatchInsert) {
            val outcomes = batchInsertUnder(events)
            // Every write path already reports a typed outcome per event; this
            // just keeps the tally. "81% of what this node is offered is
            // already stored" is what tells an operator to narrow a sync.
            bookOutcomes(Activity.BatchInsert, outcomes)
            outcomes
        }

    /** [batchInsert]'s body; split because `withActivity` cannot express a non-local return. */
    private suspend fun batchInsertUnder(events: List<Event>): List<IEventStore.InsertOutcome> {
        if (events.any { it is DeletionEvent || it is RequestToVanishEvent }) {
            return lockedForBatch(events) { if (events.size < BULK_MIN) events.map { admission.tryAdmit(it) } else bulkMixed.run(events) }
        }
        // Bulk-or-loop is decided on the batch the CALLER sent, not on a
        // half: a 30-event batch split 15/15 must not fall to the per-event
        // loop on both sides for having been split.
        val bulk = events.size >= BULK_MIN
        val trustAt = BooleanArray(events.size) { touchesTrust(events[it]) }
        val trustCount = trustAt.count { it }
        if (trustCount == 0) return insertRecords(events, trust = false, bulk = bulk)
        if (trustCount == events.size) return insertRecords(events, trust = true, bulk = bulk)
        val plainOut = insertRecords(events.filterIndexed { i, _ -> !trustAt[i] }, trust = false, bulk = bulk)
        val trustOut = insertRecords(events.filterIndexed { i, _ -> trustAt[i] }, trust = true, bulk = bulk)
        var p = 0
        var t = 0
        return events.indices.map { i -> if (trustAt[i]) trustOut[t++] else plainOut[p++] }
    }

    /**
     * One run of plain records (no kind 5/62) — the bulk path when [bulk], a
     * loop otherwise — under [writes], and under the trust gate first when
     * [trust] (every event in the run touches reputation, or none does).
     */
    private suspend fun insertRecords(
        events: List<Event>,
        trust: Boolean,
        bulk: Boolean,
    ): List<IEventStore.InsertOutcome> {
        if (!bulk) return locks.gated(trust, WriteLocks.INGEST) { events.map { admission.tryAdmit(it) } }
        // PLANNED OUTSIDE THE LOCKS, as before: the plan is reads only.
        val plan = bulkRecords.plan(events)
        return locks.gated(trust, WriteLocks.INGEST) { bulkRecords.commit(plan) }
    }

    /**
     * Tally a batch's typed outcomes onto the outcomes altitude.
     *
     * TALLIED LOCALLY FIRST, then booked once per distinct reason. A batch is
     * overwhelmingly one reason — a mirroring relay's is ~99% duplicates — so
     * per-event booking paid two `computeIfAbsent` lookups 20,000 times to
     * reach a handful of counters (measured: 63.2 ns/event against 33.3 ns
     * batched).
     */
    private fun bookOutcomes(
        activity: Activity,
        outcomes: List<IEventStore.InsertOutcome>,
    ) {
        if (outcomes.isEmpty()) return
        val tally = HashMap<String, Long>(4)
        for (o in outcomes) {
            val reason =
                when (o) {
                    is IEventStore.InsertOutcome.Accepted -> CostLedger.ADMITTED

                    is IEventStore.InsertOutcome.Rejected -> Rejections.reasonOf(o.reason)

                    // A transient engine failure, not a semantic verdict. Kept
                    // separate from the rejection reasons so a broken engine can
                    // never read as a corpus full of duplicates.
                    else -> OUTCOME_FAILED
                }
            tally[reason] = (tally[reason] ?: 0L) + 1L
        }
        tally.forEach { (reason, n) -> metrics.outcome(activity, reason, n) }
    }

    /** No rollback: buffered inserts apply in order; the first rejection propagates and aborts the rest. */
    override suspend fun transaction(body: IEventStore.ITransaction.() -> Unit) =
        withActivity(Activity.BatchInsert) {
            val buffered = ArrayList<Event>()
            object : IEventStore.ITransaction {
                override fun insert(event: Event) {
                    buffered += event
                }
            }.body()
            lockedForBatch(buffered) { buffered.forEach { admission.admit(it) } }
        }

    // ---- queries ------------------------------------------------------------

    /**
     * Map a filter to an [EventQuery] stamped with the request's clock (one
     * [cutoffSecs] per request, so sibling filters can't disagree about an
     * event expiring on the boundary — nor, since it is also the RECENCY
     * ranking instant, about how old the same document is) and the ranking
     * observer. An explicit
     * `observer:` search token wins over the connection [observer] — scores
     * are public, so a client may rank through any lens.
     *
     * THE OBSERVER GATE: supplying an observer opts the whole request into
     * that lens — including plain recall. A non-search query with a resolved
     * observer keeps its NIP-01 recency order but drops authors below the
     * trust floor ([EventQuery.minRank] if the query set one via
     * `filter:rank:…`, else [DEFAULT_MIN_RANK]). `include:spam` opts out of
     * the DEFAULT floor only — an explicit floor survives it. Queries that
     * chose a profile (`sort:`) or carry terms already gate through their own
     * profile; reads that never resolve an observer are untouched — recall
     * without a lens is never gated.
     */
    private fun Filter.toExpiryQuery(
        cutoffSecs: Long,
        observer: String? = null,
    ): EventQuery? =
        toEventQuery()?.let {
            // nowSecs: the same instant the expiry cutoff uses. recallOrdered
            // MERGES sibling filters' hits by engine score, and with recency in
            // the profile a score is a function of the instant it was asked at
            // — so letting each filter stamp its own clock would put one merge
            // on two scales, which is the exact thing cutoffSecs exists to stop.
            // It also puts the store's injectable clock in charge of ranking.
            val q = it.copy(notExpiredAt = cutoffSecs, nowSecs = it.nowSecs ?: cutoffSecs, observer = it.observer ?: observer)
            val floor = q.minRank ?: DEFAULT_MIN_RANK.takeUnless { q.includeSpam }
            // Phrases count as search text (they gate through the search
            // profiles); an exclusion-only (notSearch) query is plain recall.
            val termless = q.search == null && q.phrases.isEmpty()
            when {
                q.observer != null && termless && q.ranking == null && floor != null -> {
                    q.copy(ranking = EventYql.RANK_RECENCY_GATED, minRank = floor)
                }

                // `sort:recent` with nothing to gate through AND nothing to
                // search IS a plain NIP-01 filter — the gated profile would
                // order it identically while forgoing the recency profile and
                // the count-probe planner (both key on a ranking-free query),
                // and would rank every match to say so. Handed back to the
                // plain path. A `sort:recent` carrying TERMS keeps the profile:
                // there, ranking-free means the RELEVANCE profiles, which is
                // the order the caller just asked us not to use.
                q.observer == null && termless && q.ranking == EventYql.RANK_RECENCY_GATED -> {
                    q.copy(ranking = null, minRank = null)
                }

                else -> {
                    q
                }
            }
        }

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    /**
     * NOTE ON [T]: the reference expansion adds rows the caller's own recall
     * never returned — the subjects a matched label, assertion or Trusted List
     * nominates — so a SEARCHING read can serve more events than it matched.
     * What it cannot do is serve a KIND the filters did not name: `PageAssembly.asked`
     * holds the page to the caller's own kinds, so `query<MetadataEvent>(
     * kinds=[0], search=…)` is still all kind 0 and the unchecked cast below —
     * the interface's own idiom — stays honest. A read that named NO kinds
     * admits everything by definition, and there a mixed page is what was
     * asked for.
     */
    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> = withActivity(Activity.Query) { queryUnder(filters) }

    /** [query]'s body; split for the reason [batchInsertUnder] is. */
    @Suppress("UNCHECKED_CAST")
    private suspend fun <T : Event> queryUnder(filters: List<Filter>): List<T> {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = lensed(filters.mapNotNull { it.toExpiryQuery(cutoff, observer) })
        // Reconstruct via Quartz's by-kind factory straight from the stored
        // fields, skipping the serialize+parse round trip; see [toEvent].
        return pages.serve(queries, filters, docPage).map { it.toEvent() } as List<T>
    }

    /**
     * Raw read path: recall matches as Quartz [RawEvent]s, skipping the
     * per-hit tag parse and the Event object model — a relay serving REQs
     * straight to the wire re-serializes each event anyway (see
     * [EventIndex.rawSearch]). Same recall, expiry, dedup, and ordering as
     * [query]; only the projection differs.
     */
    override suspend fun rawQuery(
        filters: List<Filter>,
        onEach: (RawEvent) -> Unit,
    ) = withActivity(Activity.Query) {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = lensed(filters.mapNotNull { it.toExpiryQuery(cutoff, observer) })
        pages.serve(queries, filters, rawPage).forEach(onEach)
    }

    override suspend fun <T : Event> query(
        filter: Filter,
        onEach: (T) -> Unit,
    ) = query<T>(filter).forEach(onEach)

    override suspend fun <T : Event> query(
        filters: List<Filter>,
        onEach: (T) -> Unit,
    ) = query<T>(filters).forEach(onEach)

    /**
     * NIP-45 COUNT == the REQ's MATCHED feed, exactly: the same observer
     * (ranking gates included), the same per-filter limits, the same
     * cross-filter id dedup — the number a client could verify by running the
     * REQ and counting what it MATCHED. One deliberate exception: the search
     * expansion's ADDED rows — converted pointers and their spliced subjects —
     * are not counted, so a searching COUNT can undercount the frames its REQ
     * serves. Counting them would cost the full ranked recall plus the
     * expansion's subject lookups on a path whose whole point is to be cheaper
     * than the REQ, to count rows that exist to be *extra*; a client sizing a
     * slice wants the match set, and that is what it gets.
     *
     * A GATED count is therefore a recall, not a grouping: the trust floor
     * reads the observer's cell in the author's reputation tensor, which no YQL
     * `count()` can express, so an observer's count costs a full ranked pass
     * over the match set. Only the ungated shapes reach the cheap engine count
     * below. Bounding that cost is the FILTER's job, as with every other query
     * here — the store adds no cap of its own (see [VespaEventStore.open]).
     */
    override suspend fun count(filter: Filter): Int = count(listOf(filter))

    override suspend fun count(filters: List<Filter>): Int = withActivity(Activity.Count) { countUnder(filters) }

    /** [count]'s body; split for the reason [batchInsertUnder] is. */
    private suspend fun countUnder(filters: List<Filter>): Int {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries =
            lensed(filters.mapNotNull { it.toExpiryQuery(cutoff, observer) })
                // A present limit <= 0 is the "matches nothing" sentinel on the
                // feed, so it contributes nothing to the count either.
                .filterNot { (it.limit ?: 1) <= 0 }
        if (queries.isEmpty()) return 0
        if (queries.size == 1) {
            val q = queries[0]
            return when {
                // Ranked/searching: only the search path applies the observer's
                // trust floor and spam gate, so the count must be of what the
                // feed would SERVE, not of what the filter matches. That used to
                // mean recalling the page and taking its size, which fetched a
                // full document summary per counted event to produce an integer
                // — 76.0s for a "bitcoin" COUNT on the production relay against
                // 4.9s for the same search (2026-09-01). [EventIndex.count]
                // answers the same gated number from the engine's own
                // `totalCount` with zero hits; the clamp below is unchanged, so
                // the ANSWER is identical (STORE-C01: a count honours its
                // filter's limit).
                q.isRanked() || q.limit != null -> {
                    val served = index.count(q.copy(limit = null))
                    q.limit?.let { minOf(served, it) } ?: served
                }

                // Plain, unbounded: the engine's exact grouping count.
                else -> {
                    index.count(q)
                }
            }
        }
        // Multi-filter: the feed dedups across filters, so collect each
        // filter's SERVED ids and count the union. Plain unbounded filters
        // stream ids through the visit (no doc materialization).
        //
        // Fanned out at [QUERY_FANOUT], like the recall path: a COUNT summarizes
        // the same feed a REQ serves and a relay allows as many filters here as
        // there (20), so running them one after another made COUNT several times
        // slower than the REQ it describes, for identical work. [forEachBounded]
        // serializes the fold, which is what lets the id set stay a plain
        // HashSet while the queries overlap.
        val ids = HashSet<String>()
        queries.forEachBounded(
            QUERY_FANOUT,
            produce = { q ->
                // Every PLAIN filter streams ids, limited or not — the engine
                // honours a limit on the id walk itself, so the relay's
                // `limit: 100000` no longer turns a count into a full-summary
                // recall (measured: 40 s for two window filters over 51k
                // events, against 0.2 s counted singly). Only a ranked filter
                // has to recall through the search path, where its ids ARE
                // the ranking.
                if (!q.isRanked()) {
                    val seen = ArrayList<String>()
                    index.visitIds(q) { page ->
                        page.forEach { seen += it.id }
                        true
                    }
                    seen
                } else {
                    index.rawSearch(q).map { it.id }
                }
            },
        ) { found -> ids += found }
        return ids.size
    }

    /**
     * Every distinct value of [tagName] at position [valueIndex] across
     * [filter]'s matches, optionally narrowed by [where] — which sees the
     * WHOLE tag, so a positional condition on another element is expressible
     * (NIP-65's write marker, NIP-85's relay position).
     *
     * It rides the tags-only visit projection ([EventIndex.visitTags]) BY
     * DEFAULT, not a grouping over `tag_index`: that field is a derived, lossy
     * view (single-letter names, first values only), so a grouping never sees a
     * multi-character name and cannot apply a positional condition — it would
     * return a SUPERSET.
     *
     * WHERE THE SUPERSET IS THE ANSWER, though, the grouping is exactly right,
     * and [unconditional] is how a caller says so: a one-letter tag read at
     * position 1 with no condition on the rest of it. Then the engine
     * aggregates and the corpus is never walked — measured at ~1s against
     * ~157s for every relay url in 3.27M NIP-65 lists. Any other shape takes
     * the walk and gets the exact set.
     *
     * Empty values are skipped, and expiry is honored like [count].
     */
    suspend fun distinctTagValues(
        filter: Filter,
        tagName: String,
        valueIndex: Int = 1,
        /**
         * Whether the caller's [where] actually looks at the tag. It cannot be
         * inferred — a lambda is opaque — and guessing wrong would answer a
         * SUPERSET silently, so the default is the safe one and only a caller
         * that knows it has no positional condition opts in.
         *
         * BEFORE [where], not after: `where` is function-typed, so a parameter
         * added past it captures every trailing-lambda call site as this
         * Boolean instead. The compiler caught it; the ordering keeps it caught
         * for good.
         */
        unconditional: Boolean = false,
        where: (List<String>) -> Boolean = { true },
    ): Set<String> = withActivity(Activity.Query) { distinctTagValuesUnder(filter, tagName, valueIndex, unconditional, where) }

    /** [distinctTagValues]'s body; split for the reason [batchInsertUnder] is. */
    private suspend fun distinctTagValuesUnder(
        filter: Filter,
        tagName: String,
        valueIndex: Int,
        unconditional: Boolean,
        where: (List<String>) -> Boolean,
    ): Set<String> {
        val q = filter.toExpiryQuery(nowSecs()) ?: return emptySet()
        // THE FAST PATH, and every one of these conditions is load-bearing.
        // `tag_index` is lossy in three ways at once — single-letter names,
        // first values only, nothing of the rest of the tag — so a grouping
        // over it answers this question and no other. Drop any condition and
        // the answer silently widens.
        if (unconditional && valueIndex == 1 && tagName.length == 1) {
            index.distinctTagIndexValues(q, tagName)?.let { return it }
        }
        val out = HashSet<String>()
        index.visitTags(q) { page ->
            for (tags in page) {
                for (tag in tags) {
                    if (tag.size > valueIndex && tag[0] == tagName && where(tag)) {
                        tag[valueIndex].takeIf(String::isNotEmpty)?.let(out::add)
                    }
                }
            }
            true
        }
        return out
    }

    /**
     * (created_at, id) pairs straight off the docs — no Event materialization.
     * Plain filters walk the corpus through the engine's visit, so a
     * negentropy session sees the COMPLETE match set even when it dwarfs the
     * search page limit. Searching or limit'd filters keep the search path,
     * since their semantics live there.
     *
     * [maxEntries] returns at most `maxEntries + 1` — one over, so the caller
     * can tell "at budget" from "over budget" — and STOPS the walk there: a
     * caller sizing a sync window only needs to learn the set exceeds its
     * budget, not scan a 10M corpus to prove it.
     *
     * That cap counts UNIQUE ids, which is why cross-filter dedup runs INLINE
     * rather than over the collected list at the end. Capping RAW hits would
     * stop the walk while the deduped union was still under budget, handing back
     * a partial set indistinguishable from a complete one.
     *
     * [onProgress] fires after every page — the walk is the longest silent phase
     * a mirror has.
     */
    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
        onProgress: ((collected: Int) -> Unit)?,
    ): List<IdAndTime> = withActivity(Activity.Snapshot) { snapshotUnder(filters, maxEntries, onProgress) }

    /** [snapshotIdsForNegentropy]'s body; split for the reason [batchInsertUnder] is. */
    private suspend fun snapshotUnder(
        filters: List<Filter>,
        maxEntries: Int?,
        onProgress: ((collected: Int) -> Unit)?,
    ): List<IdAndTime> {
        val all = ArrayList<IdAndTime>()
        // Only a multi-filter snapshot can repeat an id — one query never
        // returns a doc twice — so the single-filter walk pays nothing here.
        val seen = if (filters.size > 1) HashSet<String>() else null
        val cap = maxEntries?.plus(1)

        fun collect(
            id: String,
            createdAt: Long,
        ) {
            if (seen == null || seen.add(id)) all += IdAndTime(createdAt, id)
        }

        // Exclude already-expired events (NIP-40), exactly as query/count do —
        // otherwise a peer keeps trying to reconcile events we refuse to serve.
        val cutoff = nowSecs()
        for (q in filters.mapNotNull { it.toExpiryQuery(cutoff) }) {
            // Already over budget: the filters left can only add to the union.
            if (cap != null && all.size >= cap) break
            if (q.search == null && q.limit == null) {
                index.visitIds(q) { page ->
                    page.forEach { collect(it.id, it.createdAt) }
                    onProgress?.invoke(all.size)
                    cap == null || all.size < cap
                }
            } else {
                index.search(q).forEach { collect(it.id, it.createdAt) }
                onProgress?.invoke(all.size)
            }
        }
        // A page (or a search, which has no page hook to stop on) can carry
        // the count past the cap — trim to the sentinel the contract promises.
        return if (cap != null && all.size > cap) all.subList(0, cap) else all
    }

    // ---- deletes ------------------------------------------------------------

    override suspend fun delete(filter: Filter) = delete(listOf(filter))

    /**
     * BOTH LOCKS, unconditionally — a sweep is defined by a filter, so what it
     * will delete is not known until it runs, and it may well be cards. A
     * removal does not write reputation inline (it queues projection work and the drain
     * re-derives), but `ProjectionLedger.insuring` PERSISTS that mark as a reputation
     * document, and the drain loads and rewrites the same marker under the
     * trust gate. Racing it would drop work the drain had already snapshotted.
     *
     * Cheap where it matters: a sweep is periodic, not the client write path
     * the gate split exists to keep clear.
     */
    override suspend fun delete(filters: List<Filter>) = withActivity(Activity.Delete) { deleteUnder(filters) }

    /** [delete]'s body; split for the reason [batchInsertUnder] is. */
    private suspend fun deleteUnder(filters: List<Filter>) {
        // An EMPTY filter deletes NOTHING (STORE-F10, the reference's deliberate
        // asymmetry with query): as a query it means "everything", and a stray
        // one here would sweep the corpus 10k at a time until the round cap
        // threw, half-wiped.
        val queries = filters.filterNot { it.isEmpty() }.mapNotNull { it.toEventQuery() }
        if (queries.isEmpty()) return
        locks.gated(trust = true, WriteLocks.SWEEP, WriteLocks.SWEEP_TRUST) {
            queries.forEach { deletions.sweep(it) }
        }
    }

    override suspend fun deleteExpiredEvents() =
        withActivity(Activity.Delete) {
            // expiresBefore is strict (<): +1 makes "expires exactly now" due, per NIP-40.
            // Both locks, for the reason on [delete]: NIP-40 expiry does not ask
            // what kind it is reaping, so it can reap cards.
            locks.gated(trust = true, WriteLocks.SWEEP, WriteLocks.SWEEP_TRUST) { deletions.sweep(EventQuery(expiresBefore = nowSecs() + 1)) }
        }

    // ---- full-text reindex --------------------------------------------------

    /**
     * Re-derive the search fields for every stored event. Which kinds are
     * searchable — and how [SearchExtractors] decomposes them — is baked into
     * this build, so docs indexed under old code can be stale until this runs.
     * It also clears fields for kinds that LOST searchability, and it is the
     * RE-FEED that backfills the near-tier prefix/fuzzy attributes on a corpus
     * fed before they existed — a Vespa reindex cannot, because fed fields
     * only change on a put.
     *
     * ORDER MATTERS on an upgraded deployment: deploy the bundled schema
     * BEFORE running this — a serving schema that predates the near fields
     * rejects the backfill puts outright, failing loudly instead of
     * backfilling nothing.
     *
     * THIS DOES NOT BACKFILL `search_text_gram`, and the symmetry with the near
     * tier is exactly inverted. That column is DERIVED BY VESPA
     * (`indexing: input search_text | index`), not fed, so nothing this method
     * compares can see it missing: `columnsChanged` reads the search columns,
     * which are identical, and `nearStale` reads the near arrays, which are
     * identical too — so no document is re-put and the walk reports success
     * having repaired nothing. The corpus keeps working, minus body
     * partial-word reach, with no error anywhere. VERIFIED on Vespa 8
     * (2026-08-15): deploying the column onto a populated cluster leaves it
     * empty for every existing document while `search_text` keeps serving.
     *
     * Two things do repair it, and an operator must pick one:
     *   - Vespa REINDEXING, which the deploy response asks for by name
     *     ("Non-document field 'search_text_gram' added; this may be populated
     *     by reindexing"). Triggered on the config server at
     *     `POST …/reindex?clusterId=content&documentType=event`. It is
     *     asynchronous — it goes `pending` and is dispatched by a maintenance
     *     job, not on the call — so do not treat the 200 as completion.
     *   - A full RE-FEED. A plain put re-derives the column at index time, even
     *     with byte-identical content (verified). This method cannot be that
     *     re-feed, by the drift check above.
     */
    override suspend fun reindexFullTextSearch() =
        withActivity(Activity.Reconcile) {
            var cursor: String? = null
            do {
                val progress = reindexFullTextSearch(cursor)
                cursor = progress.cursor
            } while (!progress.done)
        }

    /**
     * Resumable batch: one page of the engine's document walk, with the walk's
     * continuation carried in the opaque [FtsReindexProgress.cursor]. O(page)
     * memory and O(corpus) total.
     */
    override suspend fun reindexFullTextSearch(
        resumeFrom: String?,
        batchSize: Int,
    ): FtsReindexProgress {
        val (progress, trustDocs) =
            locks.underWrites(WriteLocks.REINDEX) {
                val page = index.visitDocsPage(EventQuery(), resumeFrom, batchSize)
                // ONE pipelined write per page: serial awaited puts pay per-op ack
                // latency — hours of it on a churny reindex.
                val changed = ArrayList<EventDoc>()
                for (doc in page.docs) {
                    val fields = SearchExtractors.extract(doc.toEvent())
                    // The near-tier arrays are FED data derived from the search
                    // columns at put time, so identical columns can still hide a
                    // stale or MISSING near tier (a corpus fed before those fields
                    // existed). storedNearFields is the visit's evidence of what the
                    // engine holds (null = no evidence). Checked second, since a
                    // changed column already forces the re-put.
                    val columnsChanged = fields != doc.search
                    val nearStale = !columnsChanged && doc.storedNearFields?.let { it != fields.nearFieldsWritten() } == true
                    if (columnsChanged || nearStale) changed += doc.copy(search = fields)
                }
                // A page can carry cards, and the projection applies their
                // cells INLINE on putAll — the same documents the drain
                // re-derives — so those queue for the trust gate like every
                // other write that touches reputation. The gate comes BEFORE
                // [writes] (see [trustGate]), and this page was read under
                // [writes] alone, so they are handed out and re-taken in order
                // below rather than stalling the page (and every writer behind
                // it) on a drain slice. Values agree either way — the card is
                // the newest for its address — so this is the split's rule
                // kept at its fifth entry point, not a repair.
                val (trust, plain) = changed.partition { it.kind in TrustProjection.TRUST_KINDS }
                if (plain.isNotEmpty()) index.putAll(plain)
                FtsReindexProgress(cursor = page.continuation, processedThisBatch = page.docs.size, done = page.continuation == null) to trust
            }
        if (trustDocs.isNotEmpty()) {
            locks.gated(trust = true, WriteLocks.REINDEX, WriteLocks.REINDEX_TRUST) {
                // [writes] was released to take the gate in order, so a
                // supersession may have landed since the page was read, and
                // re-putting the page's copy of a replaced card would roll the
                // newer version back. Events are immutable, so an id that is
                // STILL stored is exactly the doc the page holds — near-tier
                // evidence included, which a re-read would not carry.
                val alive = index.existingIds(trustDocs.map { it.id })
                val still = trustDocs.filter { it.id in alive }
                if (still.isNotEmpty()) index.putAll(still)
            }
        }
        return progress
    }

    /**
     * Rebuild the guard-owner cache from the corpus NOW — the explicit barrier
     * for the staleness [WriterTopology.SHARED] otherwise bounds by its refresh
     * interval. Worth calling after a known foreign write (a sync round that
     * mirrored kinds 5/62 from upstream), or to make a test deterministic
     * instead of interval-bound. Costs one distinct-author scan per guard kind;
     * union-only, so it can never unflag an owner.
     */
    suspend fun refreshGuardOwners() = withActivity(Activity.GuardRefresh) { guards.refresh() }

    override fun close() {
        // Before the index its background walks read through goes away.
        guards.close()
        index.close()
    }

    private companion object {
        /** The outcome key for an insert that failed on the ENGINE rather than on a rule. */
        const val OUTCOME_FAILED = "failed"

        /** Batches this size or larger take the bulk path; smaller ones aren't worth its setup. */
        const val BULK_MIN = 16

        /** [EventDoc.NEWEST_FIRST] for the raw read path — the same order over [RawEvent]s. */
        val RAW_NEWEST_FIRST = compareByDescending(RawEvent::createdAt).thenBy(RawEvent::id)

        /** NIP-01's parameterized-replaceable range — the kinds an `a` tag can name. */
        private val ADDRESSABLE = 30_000..39_999

        /** How the expansion reads a subject out of a document. */
        private val DOC_KEYS =
            SubjectKeys<EventDoc>(
                idOf = { it.id },
                authorOf = { it.pubkey },
                addressOf = { if (it.kind in ADDRESSABLE) "${it.kind}:${it.pubkey}:${it.dTagOrEmpty()}" else null },
            )

        /**
         * The same, off a raw row. The address costs a tag parse, so it is paid
         * only for the addressable kinds — the raw path exists to skip exactly
         * that parse on the hits, and a spliced addressable is a rounding error
         * beside them.
         */
        private val RAW_KEYS =
            SubjectKeys<RawEvent>(
                idOf = { it.id },
                authorOf = { it.pubKey },
                addressOf = { raw -> if (raw.kind in ADDRESSABLE) raw.toEvent<Event>().let { "${it.kind}:${it.pubKey}:${it.tags.dTag()}" } else null },
            )
    }
}
