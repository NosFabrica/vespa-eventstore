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
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.forEachBounded
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.ingest.BulkMixedInsert
import com.nosfabrica.vespa.eventstore.ingest.BulkRecordInsert
import com.nosfabrica.vespa.eventstore.ingest.GuardOwners
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.nosfabrica.vespa.eventstore.mapping.VespaText
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.nosfabrica.vespa.eventstore.mapping.toEventQuery
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.nosfabrica.vespa.eventstore.search.SearchReferenceExpansion
import com.nosfabrica.vespa.eventstore.search.SearchReferences
import com.nosfabrica.vespa.eventstore.search.SubjectKeys
import com.nosfabrica.vespa.eventstore.trust.Delegations
import com.nosfabrica.vespa.eventstore.trust.Enrolment
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip01Core.store.owner
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip40Expiration.isExpired
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.coroutineContext

/**
 * The Nostr-semantics layer: a Quartz [IEventStore] backed by the search
 * engine itself — one copy of the data, queryable with full NIP-01 filters
 * plus NIP-50 search. It is engine-agnostic (any [EventIndex] works, including
 * the in-memory one); [VespaEventStore.open] assembles it over Vespa.
 *
 * [insertLocked] enforces the Nostr write rules: dedup ("duplicate:"),
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
) : IEventStore {
    private val writes = Mutex()

    // Owners with any stored tombstone/vanish; everyone else's inserts skip the
    // NIP-09/62 guard probes entirely (see GuardOwners for the safety argument).
    private val guards = GuardOwners(index, writers, guardRefreshMillis)

    private val deletions = Deletions(index, relay, sweepPage)

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

    private val bulkRecords = BulkRecordInsert(index, relay, guards)

    private val bulkMixed = BulkMixedInsert(index, relay, nowSecs, guards)

    /** A writer-lock label's two [IngestStats] stage names, interned at construction. */
    private class LockStage(
        name: String,
    ) {
        val wait = "$name.wait"
        val hold = "$name.hold"
    }

    /**
     * Take [writes], booking the WAIT and the HOLD under separate [IngestStats]
     * stages named for [stage].
     *
     * Every other stage timer starts once the lock is already held, so a writer
     * starved by another holder would otherwise show up as fast stages and a
     * stalled pipeline with nothing naming the reason. The deferred trust
     * projection makes that real: it re-derives off the ingest path but INSIDE
     * this lock (DirtLedger.drain's gate), so `proj.fetch` and an ingest commit
     * contend for one mutex while both look cheap individually. `lock.*.wait`
     * makes that visible; `lock.*.hold` attributes it.
     */
    private suspend fun <T> locked(
        stage: LockStage,
        body: suspend () -> T,
    ): T {
        val requested = System.nanoTime()
        var acquired = 0L
        try {
            return writes.withLock {
                acquired = System.nanoTime()
                body()
            }
        } finally {
            // Booked AFTER release: recording inside would put two map lookups
            // and two atomic adds in the critical section this exists to
            // measure, and `hold` would stop short of the actual release.
            // acquired == 0 means the lock was never taken (cancelled while
            // waiting) — nothing to attribute.
            if (acquired != 0L) {
                val released = System.nanoTime()
                IngestStats.add(stage.wait, acquired - requested)
                IngestStats.add(stage.hold, released - acquired)
            }
        }
    }

    override suspend fun insert(event: Event) = locked(LOCK_INGEST) { insertLocked(event) }

    /**
     * Run [body] under this store's single writer lock. For the trust
     * reconciler's mutating batches: its repairs derive from a read of the
     * corpus, and racing a live insert would let a derivation from pre-write
     * state land after the insert's own recompute. NOT reentrant (a plain
     * [Mutex]): never call from a path that already holds the lock.
     *
     * Booked under [LOCK_GATE], separately from ingest's own acquisitions: the
     * callers are the projection drain and the reconciler, and telling their
     * hold apart from ingest's is the entire point of the split.
     */
    internal suspend fun <T> withWriteLock(body: suspend () -> T): T = locked(LOCK_GATE) { body() }

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
     * Sub-[BULK_MIN] batches aren't worth the setup and just loop [insertLocked].
     */
    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
        if (events.size < BULK_MIN) return locked(LOCK_INGEST) { events.map { tryInsertLocked(it) } }
        return if (events.any { it is DeletionEvent || it is RequestToVanishEvent }) {
            locked(LOCK_INGEST) { bulkMixed.run(events) }
        } else {
            val plan = bulkRecords.plan(events)
            locked(LOCK_INGEST) { bulkRecords.commit(plan) }
        }
    }

    private suspend fun tryInsertLocked(event: Event): IEventStore.InsertOutcome =
        try {
            insertLocked(event)
            IEventStore.InsertOutcome.Accepted
        } catch (e: RejectedException) {
            // Only a SEMANTIC rejection becomes a Rejected outcome. A transient
            // engine failure must PROPAGATE — swallowing it would silently DROP
            // a valid event and let the sync cursor advance past it.
            IEventStore.InsertOutcome.Rejected(e.message ?: Rejections.INSERT_FAILED)
        }

    /** No rollback: buffered inserts apply in order; the first rejection propagates and aborts the rest. */
    override suspend fun transaction(body: IEventStore.ITransaction.() -> Unit) {
        val buffered = ArrayList<Event>()
        object : IEventStore.ITransaction {
            override fun insert(event: Event) {
                buffered += event
            }
        }.body()
        locked(LOCK_INGEST) { buffered.forEach { insertLocked(it) } }
    }

    private suspend fun insertLocked(event: Event) {
        if (event.kind.isEphemeral()) return
        if (event.isExpired()) throw RejectedException(Rejections.EXPIRED)
        // Text the engine refuses is a property of the event, so it is settled
        // here with the other no-I/O checks rather than surfacing as a feed
        // exception three round trips later. See [VespaText].
        if (VespaText.firstIllegalField(event) != null) throw RejectedException(Rejections.UNSTORABLE_TEXT)
        // The admission reads — dedup, NIP-09 tombstone, NIP-62 vanish — are
        // independent, so fire them together and check in the original
        // precedence (duplicate > deleted > vanished). The dup GET deliberately
        // stays a read: folding it into a conditional put was A/B-measured
        // 15-35% slower (see docs/server-side-constraints.md). Guard probes run
        // only when this owner HAS a stored tombstone/vanish (GuardOwners).
        val owner = event.owner()
        val probeDeleted = guards.mightBeDeleted(owner)
        val probeVanished = guards.mightHaveVanished(owner)
        if (!probeDeleted && !probeVanished) {
            // The common case reads just the dup get — skip the fan-out
            // machinery, which allocates per call.
            if (index.get(event.id) != null) throw RejectedException(Rejections.DUPLICATE)
        } else {
            coroutineScope {
                val existing = async { index.get(event.id) }
                val deleted = if (probeDeleted) async { deletions.isDeleted(event) } else null
                val vanished = if (probeVanished) async { deletions.isVanished(event) } else null
                if (existing.await() != null) throw RejectedException(Rejections.DUPLICATE)
                if (deleted?.await() == true) throw RejectedException(Rejections.DELETED)
                if (vanished?.await() == true) throw RejectedException(Rejections.VANISHED)
            }
        }
        when {
            event is DeletionEvent -> {
                deletions.applyDeletion(event)
                index.put(event.toDoc())
                guards.noteDeletionStored(event.pubKey)
            }

            event is RequestToVanishEvent -> {
                deletions.applyVanish(event)
                index.put(event.toDoc())
                guards.noteVanishStored(event.pubKey)
            }

            // Replaceable/addressable newest-wins in ONE call: false == a
            // same-or-newer version holds the address, so this insert is
            // REPLACED (see EventIndex.putIfNewer).
            event.kind.isReplaceable() || event.kind.isAddressable() -> {
                if (!index.putIfNewer(event.toDoc())) throw RejectedException(Rejections.REPLACED)
            }

            else -> {
                index.put(event.toDoc())
            }
        }
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

    /** Whether this query recalls through a rank profile (its trust gates apply engine-side). */
    private fun EventQuery.isRanked(): Boolean = search != null || phrases.isNotEmpty() || ranking != null

    /**
     * Whether the ENGINE's hit order is the serving order. Ranked queries keep
     * relevance order (NIP-50) — except the observer gate's profile, whose
     * order is defined as NIP-01 recency: the engine's created_at score order
     * is re-sorted client-side (engine score ties are arbitrary). That covers
     * `sort:recent` SEARCHES too — the point of the token is that its hits
     * come back in the same `created_at desc, id asc` order a plain filter's do.
     */
    private fun EventQuery.keepsEngineOrder(): Boolean = isRanked() && ranking != EventYql.RANK_RECENCY_GATED && ranking != EventYql.RANK_RECENCY_GATED_EXACT

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    /**
     * NOTE ON [T]: the reference expansion adds rows the caller's own recall
     * never returned — the subjects a matched label, assertion or Trusted List
     * nominates — so a SEARCHING read can serve more events than it matched.
     * What it cannot do is serve a KIND the filters did not name: [servedKinds]
     * holds the page to the caller's own kinds, so `query<MetadataEvent>(
     * kinds=[0], search=…)` is still all kind 0 and the unchecked cast below —
     * the interface's own idiom — stays honest. A read that named NO kinds
     * admits everything by definition, and there a mixed page is what was
     * asked for.
     */
    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = filters.mapNotNull { it.toExpiryQuery(cutoff, observer) }
        val expansion = expansionOf(queries)
        val recalled =
            recallOrdered(
                queries + companionsOf(expansion, queries),
                EventDoc.NEWEST_FIRST,
                EventDoc::id,
                { index.search(it) },
                { index.searchRanked(it) },
                expansion != null,
            )
        val page =
            spliced(expansion, recalled, DOC_KEYS, { if (it.kind in SearchReferences.KINDS) it.toEvent() else null }, { index.searchRanked(it) })
                .diverse(queries.all { it.keepsEngineOrder() }, EventDoc::pubkey)
        // Reconstruct via Quartz's by-kind factory straight from the stored
        // fields, skipping the serialize+parse round trip; see [toEvent].
        return page.asked(servedKinds(expansion, queries), EventDoc::kind).map { it.toEvent() } as List<T>
    }

    /**
     * THE KINDS THE CALLER ASKED FOR, or null where the page needs no
     * narrowing at all — nothing expanded, or some filter left its kinds open
     * and so admits every kind by definition.
     *
     * A kind-restricted search recalls MORE than its own kinds on purpose: the
     * pointer families that convert into them are fetched as companion queries
     * ([SearchReferenceExpansion.companions]), because a label, assertion or
     * Trusted List is the only route to the subjects it names. That is a recall
     * device, and it stops at recall. A REQ that asked for `kinds:[0]` asked a
     * NIP-01 question with a NIP-01 answer, and a 30382 on that page is a kind
     * the client said it did not want — it has no parser for it, it did not
     * budget a slot for it, and on a relay it is a protocol violation rather
     * than a bonus. So the pointer does its job (it names subjects, and those
     * subjects ARE of an asked-for kind) and is then dropped from the answer.
     *
     * The union across filters, not per filter: a REQ ORs its filters and
     * answers with one page, so a pointer kind ANY filter named is a kind the
     * client asked for — `kinds:[0]` beside `kinds:[30392]` serves both, and
     * the 30392 arrives as the plain NIP-01 hit it is.
     *
     * KINDS ONLY, not the whole filter. The rest of what a filter says is
     * already applied by the engine — a subject is looked up under the finding
     * query with its terms stripped, so it passed the same authors, tags,
     * window and trust floor the hits did (see [SearchReferenceExpansion]) —
     * and re-deciding admission here would be a second answer to a question the
     * index already answered. The kind is the one constraint the companion
     * queries deliberately step outside of, so it is the one this restores.
     */
    private fun servedKinds(
        expansion: SearchReferenceExpansion?,
        queries: List<EventQuery>,
    ): Set<Int>? {
        if (expansion == null) return null
        if (queries.any { it.kinds.isEmpty() }) return null
        return queries.flatMapTo(HashSet()) { it.kinds }
    }

    /**
     * The page with no author holding more than [maxHitsPerAuthor] rows —
     * the same list, not a copy, whenever the cap is off or nothing exceeds it,
     * which is every read on a default store.
     *
     * STABLE and FIRST-WINS: the rows an author keeps are the ones the ranking
     * put highest, and everything else stays exactly where it was. A spliced
     * member cannot be dropped by this in practice — a person has one profile,
     * so one row — which is the right asymmetry: the cap exists to stop one
     * author's BULK from taking a page, not to ration the people a list names.
     */
    private fun <R> List<R>.diverse(
        /**
         * Read off the QUERIES, not off whether the page came back with scores.
         * The two differ on an engine that does not rank — the in-memory
         * reference reports a null score per hit — and which pages an operator
         * capped must not depend on which engine answered them.
         */
        ranked: Boolean,
        authorOf: (R) -> String,
    ): List<R> {
        val cap = maxHitsPerAuthor ?: return this
        if (!ranked || size <= cap) return this
        val seen = HashMap<String, Int>()
        var dropped = false
        val kept = ArrayList<R>(size)
        for (hit in this) {
            val n = seen.merge(authorOf(hit), 1, Int::plus)!!
            if (n <= cap) kept.add(hit) else dropped = true
        }
        return if (dropped) kept else this
    }

    /**
     * The page narrowed to [kinds] — the same list, not a copy, whenever there
     * is nothing to narrow (see [servedKinds]) or nothing fell outside, which
     * is every plain recall and every search whose expansion added only
     * asked-for kinds.
     */
    private fun <R> List<R>.asked(
        kinds: Set<Int>?,
        kindOf: (R) -> Int,
    ): List<R> {
        if (kinds == null || all { kindOf(it) in kinds }) return this
        return filter { kindOf(it) in kinds }
    }

    /**
     * The reference expansion for this read, or null where nothing can ever
     * expand: the feature switched off, or no query carrying TERMS. Terms, not
     * "carries a search field" — every anonymous read on a lens-requiring
     * relay stamps `include:spam`, and a mirror's paging carries it too, so
     * gating on the field would put all of that traffic behind an expansion
     * none of it asked for.
     *
     * Created BEFORE the recall, because the expansion now shapes it: a
     * searching read also recalls the pointer kinds it would otherwise miss
     * ([SearchReferenceExpansion.companions]) — the ones a kind-restricted
     * query cannot return at all, and the enrolled signers' declarations an
     * unrestricted one ranks below its own page. Without that, the lists,
     * assertions and labels whose text matched are never returned, and neither
     * this store nor the client ever learns there was anything to unpack.
     *
     * The GATE READ STAYS LAZY. [ProviderMap] never caches an empty pass, so
     * on a relay holding no 10040s the delegations read is one small engine
     * query every time it is asked — which is why the enrolment goes in as a
     * supplier the expansion resolves at most once, and only on the paths
     * that consult the gate: building a declaration companion, or meeting a
     * declaration pointer in the page. An anonymous read resolves to
     * [Enrolment.NONE] without ever querying, as before, and a termless one
     * never reaches the gate at all. An UNRESTRICTED-kind read with an
     * observer now does resolve it — it builds a declaration companion like
     * any other searching read — which is one small query per such read on a
     * relay with no 10040s to cache, the price of the reader's own lists
     * reaching a page they were ranked off.
     */
    private fun expansionOf(queries: List<EventQuery>): SearchReferenceExpansion? {
        if (!searchExpansion.enabled) return null
        val searching = queries.filter { it.search != null || it.phrases.isNotEmpty() }
        if (searching.isEmpty()) return null
        // An ANONYMOUS read can unpack no declaration at all, and asking the
        // gate to say so would cost a provider-list query on a relay that holds
        // no Treasure Maps — where the answer is never cached, by design. Labels
        // are ungated, so the expansion still runs; it just runs with nothing
        // enrolled.
        //
        // PER OBSERVER, never pooled: the supplier takes the lens's own observer
        // and the expansion memoizes per key, so one filter's `observer:` can
        // never unpack a declaration only the filter beside it enrolled. The
        // resolution is the same one either way — `delegations()` caches the
        // parsed Maps, and `of()` is a pure fold over that cache — so a
        // two-lens read costs a second fold, not a second query.
        return SearchReferenceExpansion(
            searching,
            { observer -> if (observer == null) Enrolment.NONE else delegations().of(observer) },
            searchExpansion,
        )
    }

    /**
     * [SearchReferenceExpansion.companions] minus any query the caller already
     * sent: a REQ can legitimately carry the very filter a companion would
     * duplicate (`kinds:[1]` beside `kinds:[1985]`, same terms), and running
     * the identical query twice buys nothing the id-dedup doesn't already
     * guarantee.
     */
    private suspend fun companionsOf(
        expansion: SearchReferenceExpansion?,
        queries: List<EventQuery>,
    ): List<EventQuery> = expansion?.companions()?.filterNot { it in queries } ?: emptyList()

    /**
     * Recall every query concurrently (bounded), dedup across queries, and
     * order the result — ONE order over the union, not one order per filter.
     *
     * NIP-50 asks for relevance order and NIP-01 for recency, and a REQ can
     * carry both kinds of filter at once, so there are three cases:
     *
     *  - **No query ranked.** The union is sorted `created_at desc, id asc` —
     *    the NIP-01 order, applied across filters.
     *  - **Every query ranked, by the SAME profile.** The union is merged on the
     *    engine's relevance, ties broken by recency: the filters are several
     *    ways of asking one question, and their answers belong in one order.
     *    It costs the scores, which is why [EventIndex.searchRanked] exists.
     *  - **Mixed, or ranked by different profiles.** Each query's hits keep
     *    their own order and the runs are concatenated. A relevance score and a
     *    timestamp share no scale, and neither do two profiles' scores, so
     *    interleaving them would be inventing a comparison. The honest floor,
     *    not a good answer — a client that wants one order should ask one
     *    question.
     *
     * Dedup is by id and keeps the BEST copy: sorting before [distinctBy] means
     * an event that answered two filters survives at its higher score.
     */
    private suspend fun <R> recallOrdered(
        queries: List<EventQuery>,
        newestFirst: Comparator<R>,
        idOf: (R) -> String,
        searchOne: suspend (EventQuery) -> List<R>,
        searchRankedOne: suspend (EventQuery) -> List<Ranked<R>>,
        /**
         * Whether the CALLER needs the per-hit relevance kept — the splice does,
         * to place a subject by the confidence its pointer expressed.
         *
         * IT COSTS NOTHING ON A RANKED QUERY, which is the only kind it applies
         * to: `recallSummaries` already goes through `rankedHits` there, one
         * `recallRoot` call, and the ranked path differs only in keeping the
         * `relevance` Vespa already returned. What the single-query fast path
         * avoids is wrapping every hit of an ORDINARY REQ — and an ordinary REQ
         * is recency-ordered, so `keepsEngineOrder()` sends it down the
         * score-free branch regardless of this flag.
         */
        wantScores: Boolean = false,
    ): Page<R> {
        if (queries.isEmpty()) return Page(emptyList(), null)
        // One filter is the ordinary REQ and never needs a score: its engine
        // order IS the answer, and asking for scores would wrap every hit on
        // the hottest read a relay serves.
        if (queries.size == 1) {
            if (wantScores && queries[0].keepsEngineOrder()) return Page.of(searchRankedOne(queries[0]))
            val hits = searchOne(queries[0])
            return Page(if (queries[0].keepsEngineOrder()) hits else hits.sortedWith(newestFirst), null)
        }
        if (queries.none { it.keepsEngineOrder() }) {
            return Page(
                queries
                    .mapBounded(QUERY_FANOUT) { searchOne(it) }
                    .flatten()
                    .distinctBy(idOf)
                    .sortedWith(newestFirst),
                null,
            )
        }
        // [EventYql.profileOf], not `ranking`: the field is null for every
        // ordinary search and the profile is picked from the query's shape, so
        // two filters where only one carries `observer:` read as "same profile"
        // by the field while running on `search` and `text` — two scales,
        // interleaved.
        if (queries.all { it.keepsEngineOrder() } && queries.mapTo(HashSet()) { EventYql.profileOf(it) }.size == 1) {
            val scored = queries.mapBounded(QUERY_FANOUT) { searchRankedOne(it) }.flatten()
            // An engine that does not rank (the in-memory reference) reports a
            // null rather than a fabricated constant; its hits are already
            // newest-first, so recency is the merge that keeps them coherent.
            if (scored.any { it.score == null }) {
                return Page(
                    scored
                        .map { it.hit }
                        .distinctBy(idOf)
                        .sortedWith(newestFirst),
                    null,
                )
            }
            return Page.of(
                scored
                    .sortedWith(compareByDescending<Ranked<R>> { it.score }.thenBy(newestFirst) { it.hit })
                    .distinctBy { idOf(it.hit) },
            )
        }
        val results = queries.mapBounded(QUERY_FANOUT) { searchOne(it) }
        val ordered = queries.zip(results).flatMap { (q, hits) -> if (q.keepsEngineOrder()) hits else hits.sortedWith(newestFirst) }
        // Two scales already, which is why these runs are concatenated rather
        // than merged — so there is no coherent score to carry out of here.
        return Page(ordered.distinctBy(idOf), null)
    }

    /**
     * ONE ORDERED PAGE, AND THE RELEVANCE BEHIND IT — [scores] index-aligned
     * with [hits], or NULL for the pages that have none: a recency-ordered
     * recall, two ranking profiles concatenated, an engine that does not rank.
     *
     * A nullable parallel list rather than a `List<Ranked<R>>` because the
     * unscored page is the hot one. A plain NIP-01 recall, a mirror's paging and
     * a NIP-77 catch-up all land here, and wrapping every hit of those in a
     * score-carrying object — then unwrapping it again in [spliced] — would be
     * two copies of the page and an allocation per event to carry a null.
     */
    private class Page<R>(
        val hits: List<R>,
        val scores: List<Double?>?,
        /**
         * The TEXT band behind each score — `Ranked.textScore`, index-aligned
         * with [hits] and null wherever [scores] is. The splice places a
         * Trusted List's member by the band the LIST earned times what the list
         * says about that MEMBER, so it needs the pointer's text apart from the
         * signer's trust that [scores] multiplies in.
         */
        val texts: List<Double?>? = null,
    ) {
        companion object {
            fun <R> of(ranked: List<Ranked<R>>) = Page(ranked.map { it.hit }, ranked.map { it.score }, ranked.map { it.textScore })
        }
    }

    /**
     * THE ORDERED PAGE, PLUS WHAT IT POINTS AT — a label's subject behind the
     * label, a Trusted List's members behind the list.
     *
     * Runs after [recallOrdered] rather than inside it, but over the [Page] it
     * produced rather than over a bare list: a scored member is placed by the
     * relevance the ENGINE gave it on the member rung, and its pointer rises to
     * sit just above the best of them, so the placement needs both the finished
     * order AND the scores that produced it. Splicing inside the recall would
     * have to answer that question once per ordering case; here it is answered
     * once.
     *
     * Returns the page's hits UNTOUCHED — the same list, not a copy — whenever
     * [expansion] is null, which is every plain recall this store serves
     * (see [expansionOf] for what qualifies). There is no cheaper early-out
     * left to take here: since the conversion recall, EVERY searching read can
     * splice — a kind-restricted one reaches its pointers through the
     * companion queries — so "can serve no pointer kind" no longer exists as
     * a shape.
     */
    private suspend fun <R> spliced(
        expansion: SearchReferenceExpansion?,
        page: Page<R>,
        keys: SubjectKeys<R>,
        pointerOf: (R) -> Event?,
        recall: suspend (EventQuery) -> List<Ranked<R>>,
    ): List<R> {
        val hits = page.hits
        if (expansion == null || hits.isEmpty()) return hits
        val expanded = expansion.expand(hits, page.scores, page.texts, keys, pointerOf, recall)

        // THE POINTER'S OWN ORDER FIRST, always — the sort below is a stable
        // re-sort of it, so a tie between a subject and its own pointer resolves
        // the only way it can read: the reason above the result. It is also the
        // answer whenever there are no scores to sort by.
        val placed = ArrayList<Placed<R>>(hits.size)
        hits.forEachIndexed { i, hit ->
            val pointer = page.scores?.get(i)
            // THE POINTER RISES TO ITS BEST MEMBER — it does not hold them down.
            //
            // "A reason cannot rank below the thing it explains" is still the
            // invariant, and this is the direction that satisfies it without
            // deciding the page. The other direction — clamping each member to
            // its pointer's score — made the SIGNER's trust the ceiling for
            // everyone the list names, and a trust service is a key nobody
            // follows: on the staging relay a `Verified Human` list signed by a
            // service scored 26 pinned sixteen members scored 65..100 to one
            // number (550 x wot(26)), so they came back in the publisher's tag
            // order, three orders of magnitude below their own relevance,
            // beneath organic hits from authors trusted 7. Member trust and
            // publisher confidence — the two things `event.sd` §13 computes —
            // could not move a member at all.
            //
            // Lifting instead keeps the pill row's reading exactly: the raised
            // score is a MAX over the pointer's own members, so no subject can
            // pass it, and a tie between the pointer and its best member
            // resolves to the pointer because the stable sort sees it first.
            //
            // Only SCORED members lift, which is why a label is untouched by
            // this: it expresses no confidence, none of its subjects is fetched
            // under a member profile, `lifted` collapses to `pointer`, and the
            // placement is bit-identical to before.
            //
            // A LOOP, not filterNotNull().maxOrNull(): this runs for every hit
            // of every scored page, and the overwhelming majority of them carry
            // no subjects at all — a throwaway ArrayList per row to reduce an
            // empty list is the wrong price for a page of 500.
            val lifted =
                if (pointer == null) {
                    null
                } else {
                    var best: Double = pointer
                    for (score in expanded.scores[i]) {
                        if (score != null && score > best) best = score
                    }
                    best
                }
            if (expanded.fresh[i]) placed.add(Placed(hit, lifted))
            expanded.subjects[i].forEachIndexed { j, subject ->
                val own = expanded.scores[i][j]
                placed.add(
                    Placed(
                        subject,
                        when {
                            // NO RUNG, SO NO MOVE — it sits WITH its pointer.
                            // A reference that expressed no confidence (a
                            // NIP-32 label, a NIP-85 assertion) was never
                            // fetched under the member profile, so it takes the
                            // pointer's score and a stable sort puts it right
                            // behind it. That is the placement those two
                            // families have always had, and it is right:
                            // neither claim is probabilistic, so there is no
                            // doubt for a rung to express.
                            //
                            // The LIFTED score, not the raw one, because
                            // adjacency is the whole point of this branch. On a
                            // list mixing scored and unscored members the raw
                            // pointer score would strand the unscored ones
                            // where the block used to be — on the staging
                            // numbers, ~340x below the siblings they were named
                            // beside. For a label, which has no scored member
                            // to lift anything, `lifted` IS `pointer` and this
                            // is bit-identical to before.
                            own == null -> lifted

                            // An UNSCORED pointer on a scored member is still a
                            // null, exactly as it was under the ceiling: it is
                            // the signal that this page cannot be sorted at all
                            // and must keep the pointer's own order. Answering
                            // `own` here would let one row's missing score turn
                            // a fallback page into a sorted one.
                            pointer == null -> null

                            // THE ENGINE'S OWN NUMBER, unclamped — see the
                            // lift above for what used to happen here.
                            else -> own
                        },
                    ),
                )
            }
        }
        // ONE SCALE, THE ENGINE'S. Hits carry the relevance their rank profile
        // gave them; members carry the relevance a MEMBER profile gave them on
        // the same ladder (event.sd §13), so the two sort together without this
        // class doing arithmetic on either. Nothing to normalize, nothing to
        // shift: a number computed here could only ever be a guess about a
        // scale the engine owns, and the guess is what broke.
        //
        // A page missing any score cannot be sorted at all — the in-memory
        // reference reports null rather than fabricating a constant, and a
        // recency-ordered read has no relevance to give — so it keeps the
        // pointer's own order.
        if (placed.any { it.score == null }) return placed.map { it.row }
        return placed.sortedByDescending { it.score }.map { it.row }
    }

    /** A row and the relevance the ENGINE placed it by — its rank profile's for a hit, the member profile's for a subject. */
    private class Placed<R>(
        val row: R,
        val score: Double?,
    )

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
    ) {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = filters.mapNotNull { it.toExpiryQuery(cutoff, observer) }
        val expansion = expansionOf(queries)
        val ordered =
            recallOrdered(
                queries + companionsOf(expansion, queries),
                RAW_NEWEST_FIRST,
                RawEvent::id,
                { index.rawSearch(it) },
                { index.rawSearchRanked(it) },
                expansion != null,
            )
        spliced(expansion, ordered, RAW_KEYS, { if (it.kind in SearchReferences.KINDS) it.toEvent() else null }, { index.rawSearchRanked(it) })
            .diverse(queries.all { it.keepsEngineOrder() }, RawEvent::pubKey)
            .asked(servedKinds(expansion, queries), RawEvent::kind)
            .forEach(onEach)
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

    override suspend fun count(filters: List<Filter>): Int {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries =
            filters
                .mapNotNull { it.toExpiryQuery(cutoff, observer) }
                // A present limit <= 0 is the "matches nothing" sentinel on the
                // feed, so it contributes nothing to the count either.
                .filterNot { (it.limit ?: 1) <= 0 }
        if (queries.isEmpty()) return 0
        if (queries.size == 1) {
            val q = queries[0]
            return when {
                // Ranked/searching: only the search path applies the observer's
                // trust floor and spam gate, so the count must recall what the
                // feed would serve.
                q.isRanked() -> index.rawSearch(q).size

                // Plain with a limit: the feed caps at the limit, so the count
                // does too — the cheap exact engine count, clamped.
                q.limit != null -> minOf(index.count(q.copy(limit = null)), q.limit ?: 0)

                // Plain, unbounded: the engine's exact grouping count.
                else -> index.count(q)
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
                if (!q.isRanked() && q.limit == null) {
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
     * It rides the tags-only visit projection ([EventIndex.visitTags]), NOT a
     * grouping over `tag_index`: that field is a derived, lossy view
     * (single-letter names, first values only), so a grouping never sees a
     * multi-character name and cannot apply a positional condition — it would
     * return a SUPERSET.
     *
     * Empty values are skipped, and expiry is honored like [count].
     */
    suspend fun distinctTagValues(
        filter: Filter,
        tagName: String,
        valueIndex: Int = 1,
        where: (List<String>) -> Boolean = { true },
    ): Set<String> {
        val q = filter.toExpiryQuery(nowSecs()) ?: return emptySet()
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

    override suspend fun delete(filters: List<Filter>) {
        locked(LOCK_SWEEP) { filters.mapNotNull { it.toEventQuery() }.forEach { deletions.sweep(it) } }
    }

    override suspend fun deleteExpiredEvents() {
        // expiresBefore is strict (<): +1 makes "expires exactly now" due, per NIP-40.
        locked(LOCK_SWEEP) { deletions.sweep(EventQuery(expiresBefore = nowSecs() + 1)) }
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
    override suspend fun reindexFullTextSearch() {
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
    ): FtsReindexProgress =
        locked(LOCK_REINDEX) {
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
            if (changed.isNotEmpty()) index.putAll(changed)
            FtsReindexProgress(cursor = page.continuation, processedThisBatch = page.docs.size, done = page.continuation == null)
        }

    /**
     * Rebuild the guard-owner cache from the corpus NOW — the explicit barrier
     * for the staleness [WriterTopology.SHARED] otherwise bounds by its refresh
     * interval. Worth calling after a known foreign write (a sync round that
     * mirrored kinds 5/62 from upstream), or to make a test deterministic
     * instead of interval-bound. Costs one distinct-author scan per guard kind;
     * union-only, so it can never unflag an owner.
     */
    suspend fun refreshGuardOwners() = guards.refresh()

    override fun close() {
        // Before the index its background walks read through goes away.
        guards.close()
        index.close()
    }

    private companion object {
        /**
         * Writer-lock stage labels, named for the CALLER rather than the
         * operation: these exist to say which side of the contention a stall is
         * on ("ingest waited 40s while the gate held 40s"). Built once per
         * label, since `insert()` takes this lock per event and a String
         * allocation is not what a measurement should cost.
         */
        val LOCK_INGEST = LockStage("lock.ingest")
        val LOCK_GATE = LockStage("lock.gate")
        val LOCK_SWEEP = LockStage("lock.sweep")
        val LOCK_REINDEX = LockStage("lock.reindex")

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
