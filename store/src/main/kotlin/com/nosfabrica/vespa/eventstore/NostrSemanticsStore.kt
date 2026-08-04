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

import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.client.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.ingest.BulkMixedInsert
import com.nosfabrica.vespa.eventstore.ingest.BulkRecordInsert
import com.nosfabrica.vespa.eventstore.ingest.GuardOwners
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.nosfabrica.vespa.eventstore.mapping.VespaText
import com.nosfabrica.vespa.eventstore.mapping.owner
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.nosfabrica.vespa.eventstore.mapping.toEventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
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
 * sequentially without rollback.
 *
 * Events are NOT verified here; verification is the ingest path's job, once,
 * before insert.
 */
class NostrSemanticsStore(
    private val index: EventIndex,
    override val relay: NormalizedRelayUrl? = null,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
    /** Events removed per sweep round (see [Deletions]). Internal — a test seam, like [nowSecs]. */
    internal val sweepPage: Int = 10_000,
) : IEventStore {
    private val writes = Mutex()

    // Owners with any stored tombstone/vanish; everyone else's inserts skip the
    // NIP-09/62 guard probes entirely (see GuardOwners for the safety argument).
    private val guards = GuardOwners(index)

    private val deletions = Deletions(index, relay, sweepPage)

    private val bulkRecords = BulkRecordInsert(index, relay, guards)

    private val bulkMixed = BulkMixedInsert(index, relay, nowSecs, guards)

    override suspend fun insert(event: Event) = writes.withLock { insertLocked(event) }

    /**
     * Run [body] under this store's single writer lock. For the trust
     * reconciler's mutating batches: its repairs derive from a read of the
     * corpus, and racing a live insert would let a derivation from pre-write
     * state land after the insert's own recompute. NOT reentrant (a plain
     * [Mutex]): never call from a path that already holds the lock.
     */
    internal suspend fun <T> withWriteLock(body: suspend () -> T): T = writes.withLock { body() }

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
        if (events.size < BULK_MIN) return writes.withLock { events.map { tryInsertLocked(it) } }
        return if (events.any { it is DeletionEvent || it is RequestToVanishEvent }) {
            writes.withLock { bulkMixed.run(events) }
        } else {
            val plan = bulkRecords.plan(events)
            writes.withLock { bulkRecords.commit(plan) }
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
        writes.withLock { buffered.forEach { insertLocked(it) } }
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
            // The common-case insert reads just the dup get — skip the fan-out
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
     * Map a filter to an [EventQuery] stamped with the request's expiry cutoff
     * (one [cutoffSecs] per request, so sibling filters can't disagree about
     * an event expiring on the boundary) and the ranking observer. An explicit
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
            val q = it.copy(notExpiredAt = cutoffSecs, observer = it.observer ?: observer)
            val floor = q.minRank ?: DEFAULT_MIN_RANK.takeUnless { q.includeSpam }
            // Phrases count as search text (they gate through the search
            // profiles); an exclusion-only (notSearch) query is plain recall.
            if (q.observer != null && q.search == null && q.phrases.isEmpty() && q.ranking == null && floor != null) {
                q.copy(ranking = EventYql.RANK_RECENCY_GATED, minRank = floor)
            } else {
                q
            }
        }

    /** Whether this query recalls through a rank profile (its trust gates apply engine-side). */
    private fun EventQuery.isRanked(): Boolean = search != null || phrases.isNotEmpty() || ranking != null

    /**
     * Whether the ENGINE's hit order is the serving order. Ranked queries keep
     * relevance order (NIP-50) — except the observer gate's profile, whose
     * order is defined as NIP-01 recency: the engine's created_at score order
     * is re-sorted client-side (engine score ties are arbitrary).
     */
    private fun EventQuery.keepsEngineOrder(): Boolean = isRanked() && ranking != EventYql.RANK_RECENCY_GATED && ranking != EventYql.RANK_RECENCY_GATED_EXACT

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = filters.mapNotNull { it.toExpiryQuery(cutoff, observer) }
        val ordered = recallOrdered(queries, NEWEST_FIRST, EventDoc::id) { index.search(it) }
        // Reconstruct via Quartz's by-kind factory straight from the stored
        // fields, skipping the serialize+parse round trip; see [toEvent].
        return ordered.map { it.toEvent() } as List<T>
    }

    /**
     * Recall every query concurrently (bounded), dedup across queries, and
     * order the result. NIP-50: a searching query's hits stay in the engine's
     * RELEVANCE order; plain queries keep NIP-01 recency — PER QUERY, so a
     * plain filter riding beside a searching one is still served newest-first.
     */
    private suspend fun <R> recallOrdered(
        queries: List<EventQuery>,
        newestFirst: Comparator<R>,
        idOf: (R) -> String,
        searchOne: suspend (EventQuery) -> List<R>,
    ): List<R> {
        val results =
            when (queries.size) {
                0 -> return emptyList()
                1 -> listOf(searchOne(queries[0]))
                else -> queries.mapBounded(QUERY_FANOUT) { searchOne(it) }
            }
        if (queries.none { it.keepsEngineOrder() }) {
            val combined = results.flatten()
            val unique = if (queries.size > 1) combined.distinctBy(idOf) else combined
            return unique.sortedWith(newestFirst)
        }
        val ordered = queries.zip(results).flatMap { (q, hits) -> if (q.keepsEngineOrder()) hits else hits.sortedWith(newestFirst) }
        return if (queries.size > 1) ordered.distinctBy(idOf) else ordered
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
    ) {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = filters.mapNotNull { it.toExpiryQuery(cutoff, observer) }
        recallOrdered(queries, RAW_NEWEST_FIRST, RawEvent::id) { index.rawSearch(it) }.forEach(onEach)
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
     * NIP-45 COUNT == the REQ's feed, EXACTLY: the same observer (ranking
     * gates included), the same per-filter limits, the same cross-filter id
     * dedup — the number a client could verify by running the REQ and counting
     * what arrives. Anything else makes COUNT lie about the feed it summarizes.
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
        val ids = HashSet<String>()
        for (q in queries) {
            if (!q.isRanked() && q.limit == null) {
                index.visitIds(q) { page ->
                    page.forEach { ids += it.id }
                    true
                }
            } else {
                index.rawSearch(q).forEach { ids += it.id }
            }
        }
        return ids.size
    }

    /**
     * The distinct authors (pubkeys) matching [filter], via a server-side
     * grouping — for callers that need the author set out of a huge match set
     * without reconstructing every event. Honors expiry like [count].
     */
    suspend fun distinctAuthors(filter: Filter): Set<HexKey> = filter.toExpiryQuery(nowSecs())?.let { index.distinctAuthors(it) } ?: emptySet()

    /**
     * Every distinct `d` tag (addressable subject) across [filter]'s matches,
     * via a document visit — the STREAMING walk, for a set too big to want in
     * one response (e.g. the hundreds of thousands of subjects one WoT
     * provider scores).
     */
    suspend fun distinctDTags(filter: Filter): Set<String> {
        val q = filter.toExpiryQuery(nowSecs()) ?: return emptySet()
        val out = HashSet<String>()
        index.visitIds(q, withDTag = true) { page ->
            page.forEach { it.dTag?.takeIf(String::isNotEmpty)?.let(out::add) }
            true
        }
        return out
    }

    /**
     * Every distinct value of [tagName] at position [valueIndex] across
     * [filter]'s matches, optionally narrowed by [where] — which sees the
     * WHOLE tag, so a positional condition on another element is expressible
     * (NIP-65's write marker, NIP-85's relay position).
     *
     * It rides the tags-only visit projection ([EventIndex.visitTags]), NOT a
     * grouping over `tag_index`, deliberately: `tag_index` is a derived, lossy
     * view (single-letter names, first values only), so a grouping never sees
     * a multi-character name and cannot apply a positional condition — it
     * would return a SUPERSET. Full tag fidelity round-trips only through the
     * stored `tags` field, so this streams exactly that field and nothing else.
     *
     * Empty values are skipped, and expiry is honored like [count]. Searching
     * or limit-carrying filters fall back to the search path, keeping their
     * semantics.
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
     * (created_at, id) pairs straight off the docs — no Event materialization
     * and no result cap. Plain filters walk the corpus through the engine's
     * visit, so a negentropy session sees the COMPLETE match set even when it
     * dwarfs the search page limit. Searching or limit'd filters keep the
     * search path, since their semantics live there.
     *
     * [onProgress] fires after every page: the walk is the longest silent
     * phase a mirror has, and the page loop already knows the count.
     */
    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
        onProgress: ((collected: Int) -> Unit)?,
    ): List<IdAndTime> {
        val all = ArrayList<IdAndTime>()
        // A single-filter cap can stop the walk early: the caller only needs
        // to learn the set exceeds the cap, not scan a 10M corpus to prove it.
        // (Multi-filter needs the full set for cross-filter dedup.)
        val cap = maxEntries?.takeIf { filters.size == 1 }?.plus(1)
        // Exclude already-expired events (NIP-40), exactly as query/count do —
        // otherwise a peer keeps trying to reconcile events we refuse to serve.
        val cutoff = nowSecs()
        for (q in filters.mapNotNull { it.toExpiryQuery(cutoff) }) {
            if (q.search == null && q.limit == null) {
                index.visitIds(q) { page ->
                    page.forEach { all += IdAndTime(it.createdAt, it.id) }
                    onProgress?.invoke(all.size)
                    cap == null || all.size < cap
                }
            } else {
                index.search(q).forEach { all += IdAndTime(it.createdAt, it.id) }
                onProgress?.invoke(all.size)
            }
        }
        val unique = if (filters.size > 1) all.distinctBy { it.id } else all
        return if (maxEntries != null && unique.size > maxEntries + 1) unique.subList(0, maxEntries + 1) else unique
    }

    // ---- deletes ------------------------------------------------------------

    override suspend fun delete(filter: Filter) = delete(listOf(filter))

    override suspend fun delete(filters: List<Filter>) {
        writes.withLock { filters.mapNotNull { it.toEventQuery() }.forEach { deletions.sweep(it) } }
    }

    override suspend fun deleteExpiredEvents() {
        // expiresBefore is strict (<): +1 makes "expires exactly now" due, per NIP-40.
        writes.withLock { deletions.sweep(EventQuery(expiresBefore = nowSecs() + 1)) }
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
        writes.withLock {
            val page = index.visitDocsPage(EventQuery(), resumeFrom, batchSize)
            // ONE pipelined write per page: serial awaited puts pay per-op ack
            // latency — hours of it on a churny reindex.
            val changed = ArrayList<EventDoc>()
            for (doc in page.docs) {
                val fields = SearchExtractors.extract(doc.toEvent())
                // The near-tier arrays are FED data derived from the search
                // columns at put time, so identical columns can hide a stale or
                // MISSING near tier (a corpus fed before the near fields
                // existed). storedNearFields is the visit's evidence of what
                // the engine actually holds (null = no evidence); any drift
                // forces the re-put that backfills them. Checked second: a
                // changed column already forces the re-put.
                val columnsChanged = fields != doc.search
                val nearStale = !columnsChanged && doc.storedNearFields?.let { it != fields.nearFieldsWritten() } == true
                if (columnsChanged || nearStale) changed += doc.copy(search = fields)
            }
            if (changed.isNotEmpty()) index.putAll(changed)
            FtsReindexProgress(cursor = page.continuation, processedThisBatch = page.docs.size, done = page.continuation == null)
        }

    override fun close() = index.close()

    private companion object {
        // Runs at least this long take the bulk path; smaller ones aren't
        // worth the setup and stay on the per-event path.
        const val BULK_MIN = 16

        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)

        /** [NEWEST_FIRST] for the raw read path — the same order over [RawEvent]s. */
        val RAW_NEWEST_FIRST = compareByDescending(RawEvent::createdAt).thenBy(RawEvent::id)
    }
}
