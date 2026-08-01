/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store

import com.vitorpamplona.quartz.eventstore.store.ingest.BulkMixedInsert
import com.vitorpamplona.quartz.eventstore.store.ingest.BulkRecordInsert
import com.vitorpamplona.quartz.eventstore.store.ingest.GuardOwners
import com.vitorpamplona.quartz.eventstore.store.mapping.SearchExtractors
import com.vitorpamplona.quartz.eventstore.store.mapping.VespaText
import com.vitorpamplona.quartz.eventstore.store.mapping.addressOrNull
import com.vitorpamplona.quartz.eventstore.store.mapping.owner
import com.vitorpamplona.quartz.eventstore.store.mapping.toDoc
import com.vitorpamplona.quartz.eventstore.store.mapping.toEvent
import com.vitorpamplona.quartz.eventstore.store.mapping.toEventQuery
import com.vitorpamplona.quartz.eventstore.vespa.QUERY_FANOUT
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.mapBounded
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
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
import com.vitorpamplona.quartz.nip50Search.SearchableEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.coroutineContext

/**
 * The Nostr-semantics layer: a Quartz [IEventStore] backed by the search engine
 * itself — ONE copy of the data, queryable with full NIP-01 filters plus NIP-50
 * search, and wrappable in `ObservableEventStore` like any other store. It is
 * engine-agnostic (any [EventIndex] works, including the in-memory one);
 * [VespaEventStore.open] is the front door that assembles it over Vespa.
 *
 * It enforces Nostr semantics in [insertLocked]:
 *
 *  - duplicates rejected ("duplicate:");
 *  - replaceables/addressables: strictly-older versions (created_at, then
 *    LOWEST id wins ties) are deleted on insert, and an insert that lost that
 *    comparison is rejected ("replaced:");
 *  - kind 5: targets erased (e-tags by id, a-tags — including replaceable
 *    addresses — up to the deletion's created_at, same-owner only per NIP-09),
 *    the kind 5 kept as a tombstone, and covered inserts rejected ("blocked:");
 *  - kind 62 covering [relay]: the owner's strictly-older events erased, the
 *    request kept, covered inserts rejected ("blocked:");
 *  - deletion/vanish enforcement keys on the event's OWNER: the gift-wrap
 *    recipient for kind 1059, else the author. Recipients control the wraps
 *    addressed to them;
 *  - ephemeral kinds are accepted WITHOUT storing (persistence is a no-op per
 *    NIP-01; an observable wrapper still broadcasts them live); already-expired
 *    events rejected; [deleteExpiredEvents] sweeps due NIP-40 expirations via
 *    the derived `expires_at` attribute;
 *  - NIP-50: only kinds implementing [SearchableEvent] are searchable, via
 *    [SearchExtractors], which decomposes each kind's indexable content into the
 *    schema's per-kind search fields. [reindexFullTextSearch] re-derives them
 *    after extractor/Quartz upgrades. Per the [IEventStore] contract, filters
 *    arrive with `search` VERBATIM — this store interprets the
 *    `sort:`/`filter:rank:`/`include:spam`/`observer:` extensions itself and
 *    ignores ones it doesn't know.
 *
 * Correctness rests on two properties. First, all writes serialize behind one
 * [Mutex], so query-then-write is atomic against other writers in this process.
 * Second, [EventIndex] guarantees an acked put is visible to search (see its
 * contract). There are no cross-document transactions: [transaction] buffers and
 * applies sequentially without rollback, which relay semantics never needed.
 *
 * Events are NOT verified here. Verification is the ingest path's job
 * (syncer/relay), once, before insert.
 */
class NostrSemanticsStore(
    private val index: EventIndex,
    override val relay: NormalizedRelayUrl? = null,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
    /**
     * Events removed per round of a [sweep] (delete, NIP-40 expiry, NIP-62
     * vanish). A sweep re-runs its query until it comes back empty, so this
     * bounds how many matches are held at once, NOT how many get deleted.
     *
     * It is explicit because nothing else caps a query any more: without it a
     * vanish over a prolific author, or an expiry pass on a large corpus, would
     * materialize every doomed event in one list. Internal — a test seam, like
     * [nowSecs]; 10k is the page size this path ran with historically.
     */
    internal val sweepPage: Int = 10_000,
) : IEventStore {
    private val writes = Mutex()

    // Owners with any stored tombstone/vanish; everyone else's inserts skip the
    // NIP-09/62 guard probes entirely (see GuardOwners for the safety argument).
    private val guards = GuardOwners(index)

    private val bulkRecords = BulkRecordInsert(index, relay, guards)

    private val bulkMixed = BulkMixedInsert(index, relay, nowSecs, guards)

    override suspend fun insert(event: Event) = writes.withLock { insertLocked(event) }

    /**
     * Batches take a BULK path — the per-event path costs 3–5 index round-trips
     * each (dup probe, tombstone probe, vanish probe, supersession), which caps
     * ingest in the low thousands per second, useless against a million-event
     * sync. Two shapes, by whether the batch mutates via deletions:
     *
     *  - PURE RECORDS: [BulkRecordInsert] chunks the read checks and pipelines
     *    one [EventIndex.putAll]; its dedup reads run outside the writer lock
     *    so parallel relays overlap them (guards and supersession stay under it).
     *  - CONTAINS kind 5/62: [BulkMixedInsert] batch-reads the working set and
     *    replays the per-event rules in memory (order against neighbours — a
     *    deletion targeting an earlier event — preserved), then writes the diff.
     *    A per-event fallback here would collapse ingest on the deletion-heavy
     *    outbox streams (~98% kind 5).
     *
     * Sub-[BULK_MIN] batches aren't worth the setup and just loop [insertLocked].
     */
    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
        if (events.size < BULK_MIN) return writes.withLock { events.map { tryInsertLocked(it) } }
        return if (events.any { it is DeletionEvent || it is RequestToVanishEvent }) {
            writes.withLock { bulkMixed.run(events) }
        } else {
            // Pure records. The dedup reads run OUTSIDE the writer lock so
            // parallel relays' batches overlap them (a raced duplicate is an
            // idempotent re-put); the guard and supersession reads take the
            // lock with the writes — query-then-write stays atomic, so a
            // deletion committed by a neighbouring batch still blocks this one.
            val plan = bulkRecords.plan(events)
            writes.withLock { bulkRecords.commit(plan) }
        }
    }

    private suspend fun tryInsertLocked(event: Event): IEventStore.InsertOutcome =
        try {
            insertLocked(event)
            IEventStore.InsertOutcome.Accepted
        } catch (e: RejectedException) {
            // Only a SEMANTIC rejection (duplicate, replaced, blocked by a
            // deletion/vanish) becomes a Rejected outcome. A transient engine
            // failure (a 5xx that outlived its retries, an IO error) must
            // PROPAGATE — swallowing it as "Rejected" would silently DROP a
            // valid event and let the sync cursor advance past it. This matches
            // the bulk path, which already throws on engine failures.
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
        // Accepted but never persisted (NIP-01): an ObservableEventStore wrapper
        // still broadcasts the insert to live subscribers.
        if (event.kind.isEphemeral()) return
        if (event.isExpired()) throw RejectedException(Rejections.EXPIRED)
        // Text the engine refuses is a property of the event, so it is settled
        // here with the other no-I/O checks rather than surfacing as a feed
        // exception three round trips later. See [VespaText].
        if (VespaText.firstIllegalField(event) != null) throw RejectedException(Rejections.UNSTORABLE_TEXT)
        // The three admission reads — dedup, NIP-09 tombstone, NIP-62 vanish — are
        // independent, so fire them together: a per-event insert pays ONE round
        // trip's latency for the guards, not three in series. The results are then
        // checked in the original precedence (duplicate > deleted > vanished), so
        // which rejection wins is unchanged.
        //
        // The dup GET deliberately stays a read. Folding it into the write as a
        // conditional put (create-if-nonexistent + always-false test-and-set) was
        // built and A/B-measured against a live engine: it cut engine reads 3.25
        // to 2.25/event but was 15-20% SLOWER on fresh inserts (Vespa's
        // conditional writes pay a read-for-write check) and ~35% slower on
        // duplicates, which lose this wave's 1-round-trip early exit and pay a
        // full write attempt instead. See docs/server-side-constraints.md.
        // Guard probes run only when this owner HAS a stored tombstone (NIP-09)
        // or vanish (NIP-62) — gated INDEPENDENTLY (GuardOwners): a prolific
        // deleter's events skip the vanish probe unless they also vanished.
        val owner = event.owner()
        val probeDeleted = guards.mightBeDeleted(owner)
        val probeVanished = guards.mightHaveVanished(owner)
        if (!probeDeleted && !probeVanished) {
            // The common-case insert reads just the dup get — skip the fan-out
            // machinery (coroutineScope + async allocate per call; the same
            // shortcut the client's single-id read path takes).
            if (index.get(event.id) != null) throw RejectedException(Rejections.DUPLICATE)
        } else {
            coroutineScope {
                val existing = async { index.get(event.id) }
                val deleted = if (probeDeleted) async { isDeleted(event) } else null
                val vanished = if (probeVanished) async { isVanished(event) } else null
                if (existing.await() != null) throw RejectedException(Rejections.DUPLICATE)
                if (deleted?.await() == true) throw RejectedException(Rejections.DELETED)
                if (vanished?.await() == true) throw RejectedException(Rejections.VANISHED)
            }
        }
        when {
            event is DeletionEvent -> {
                applyDeletion(event)
                index.put(event.toDoc())
                guards.noteDeletionStored(event.pubKey)
            }

            event is RequestToVanishEvent -> {
                applyVanish(event)
                index.put(event.toDoc())
                guards.noteVanishStored(event.pubKey)
            }

            // Replaceable/addressable newest-wins in ONE call: address-keyed Vespa
            // rejects a stale version server-side (conditionNotMet, no read); the
            // default resolves it with the same (created_at, then lowest id) rule
            // the old supersede()+put did. False == a same-or-newer version holds
            // the address, so this insert is REPLACED.
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
     * (NIP-40 — one [cutoffSecs] per request, so sibling filters can't disagree
     * about an event expiring on the boundary) and, for searches, the ranking
     * observer. An explicit `observer:` search token wins over the connection
     * [observer] — a client may ask to rank through any lens (scores are
     * public), so the query's own choice takes precedence over the
     * authenticated default.
     */
    private fun Filter.toExpiryQuery(
        cutoffSecs: Long,
        observer: String? = null,
    ): EventQuery? = toEventQuery()?.let { it.copy(notExpiredAt = cutoffSecs, observer = it.observer ?: observer) }

    /** Whether this query's results carry engine ranking order (NIP-50) instead of NIP-01 recency. */
    private fun EventQuery.isRanked(): Boolean = search != null || ranking != null

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = filters.mapNotNull { it.toExpiryQuery(cutoff, observer) }
        val ordered = recallOrdered(queries, NEWEST_FIRST, EventDoc::id) { index.search(it) }
        // Reconstruct via Quartz's by-kind factory straight from the stored fields,
        // skipping the toEventJson()->fromJson() serialize+parse round trip that was
        // the hot path's biggest allocator; see [toEvent].
        return ordered.map { it.toEvent() } as List<T>
    }

    /**
     * Recall every query concurrently (bounded), dedup across queries, and
     * order the result. NIP-50: a searching query's hits stay in the engine's
     * RELEVANCE order "instead of the usual created_at ordering" — re-sorting
     * would undo the rank profile. Plain queries keep NIP-01 recency — PER
     * QUERY, so a plain filter riding beside a searching one in the same REQ
     * is still served newest-first (an all-plain request keeps the combined
     * newest-first order it always had).
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
        if (queries.none { it.isRanked() }) {
            val combined = results.flatten()
            val unique = if (queries.size > 1) combined.distinctBy(idOf) else combined
            return unique.sortedWith(newestFirst)
        }
        val ordered = queries.zip(results).flatMap { (q, hits) -> if (q.isRanked()) hits else hits.sortedWith(newestFirst) }
        return if (queries.size > 1) ordered.distinctBy(idOf) else ordered
    }

    /**
     * Raw read path: recall matches as Quartz [RawEvent]s, skipping the per-hit
     * tag parse and the Event object model that [query] builds. A relay serving
     * REQs straight to the wire never needs the parsed tags — it re-serializes
     * each event to JSON — so on the Vespa client this hands the stored tag
     * string through untouched (see [EventIndex.rawSearch]). Same recall, expiry,
     * dedup, and ordering as [query]; only the projection differs.
     */
    override suspend fun rawQuery(
        filters: List<Filter>,
        onEach: (RawEvent) -> Unit,
    ) {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = filters.mapNotNull { it.toExpiryQuery(cutoff, observer) }
        // Same recall, dedup and (per-query, NIP-50-aware) ordering as [query].
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
                // feed would serve (bounded by the same limit the feed honors).
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
        // stream ids through the visit (no doc materialization); limit'd and
        // ranked filters recall exactly the page the feed would serve.
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
     * The distinct authors (pubkeys) matching [filter], via a server-side grouping
     * query ([EventIndex.distinctAuthors]) — for callers that need the author set
     * out of a huge match set without reconstructing every event (the orphan-score
     * sweep over millions of 30382s). Honors expiry like [count].
     */
    suspend fun distinctAuthors(filter: Filter): Set<HexKey> = filter.toExpiryQuery(nowSecs())?.let { index.distinctAuthors(it) } ?: emptySet()

    /**
     * Every distinct `d` tag (addressable subject) across [filter]'s matches, via
     * a document visit — the STREAMING walk, for a set too big to want in one
     * response, e.g. the hundreds of thousands of subjects one WoT provider
     * scores. A `search` would return them all too (no hit cap by default), but
     * materialized at once, and truncated outright if a deployment does set a
     * cap. The sync uses this to find every scored author to fetch content for.
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
     * (created_at, id) pairs straight off the docs — no Event materialization
     * and no result cap. Plain filters walk the corpus through the engine's
     * visit ([com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex.visitIds]), so a negentropy
     * session (or a sync reconcile diff) sees the COMPLETE match set even when it
     * dwarfs the search page limit. Searching or limit'd filters keep the search
     * path, since their semantics live there.
     *
     * [onProgress] (now on the [IEventStore] contract) fires after every page:
     * the walk is the longest silent phase a mirror has — minutes of visit
     * requests on a large corpus — and the page loop already knows the count.
     */
    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
        onProgress: ((collected: Int) -> Unit)?,
    ): List<IdAndTime> {
        val all = ArrayList<IdAndTime>()
        // A single-filter cap can stop the walk early: the caller only needs to
        // learn the set exceeds the cap, not scan a 10M corpus to prove it.
        // (Multi-filter needs the full set for cross-filter dedup, so no break.)
        val cap = maxEntries?.takeIf { filters.size == 1 }?.plus(1)
        // Exclude already-expired events (NIP-40), exactly as query/count do.
        // Otherwise the negentropy set offers ids a plain REQ would never serve,
        // and a peer keeps trying to reconcile events we refuse to return.
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
        writes.withLock { filters.mapNotNull { it.toEventQuery() }.forEach { sweep(it) } }
    }

    override suspend fun deleteExpiredEvents() {
        // expiresBefore is strict (<): +1 makes "expires exactly now" due, per NIP-40.
        writes.withLock { sweep(EventQuery(expiresBefore = nowSecs() + 1)) }
    }

    /**
     * Remove every match, [sweepPage] at a time, until the query comes back
     * empty. No offset: each round re-runs the SAME query, and the removes
     * shrink the match set, so the next round naturally sees the next batch.
     * (Offset paging would be wrong here — deleting under an offset skips rows.)
     */
    private suspend fun sweep(q: EventQuery) {
        // The read is paged; the caller's own limit, if any, still decides
        // whether one page is the whole job (below), so read q.limit, not this.
        // Plain sweeps stamp RANK_UNRANKED: a sweep wants ANY page, so the
        // recency planner's count probes (up to 3 per round, serial under the
        // writer lock) answer a question it never asked — the explicit ranking
        // opts out of isBareRecencyScan while compiling to identical YQL.
        val paged =
            if (q.search == null && q.ranking == null) {
                q.copy(limit = q.limit ?: sweepPage, ranking = EventYql.RANK_UNRANKED)
            } else {
                q.copy(limit = q.limit ?: sweepPage)
            }
        var rounds = 0
        var lastPage: Set<String>? = null
        while (rounds++ < MAX_SWEEP_ROUNDS) {
            val page = index.search(paged)
            if (page.isEmpty()) return
            val ids = page.mapTo(HashSet()) { it.id }
            // An acked remove is visible to search (the EventIndex contract), so
            // a page identical to the one just removed means the deletes are not
            // landing. Fail NOW: silently returning would report a vanish/delete
            // as enforced while the events are still stored and served.
            check(ids != lastPage) { "sweep is not shrinking: ${ids.size} matches for $q survived their own removal" }
            // The docs are already in hand — removeDocs lets the projection
            // react without a get per id (see TrustProjection.removeDocs).
            index.removeDocs(page)
            // A limit'd delete is satisfied by its first page.
            if (q.limit != null) return
            lastPage = ids
        }
        // The backstop for a set that shrinks but never drains (or a query
        // matching more than MAX_SWEEP_ROUNDS pages). Loud, not silent: the
        // caller (a vanish, an expiry pass, an admin delete) must not believe
        // the sweep completed.
        error("sweep did not drain after $MAX_SWEEP_ROUNDS rounds of ${paged.limit} for $q")
    }

    // ---- Nostr semantics -------------------------------------------------------

    /**
     * NIP-09: a kind 5 authored by this event's OWNER, e/a-tagging it, with
     * created_at >= the event's, blocks the insert. Both target styles (e-tag
     * and a-tag) are time-guarded.
     */
    private suspend fun isDeleted(event: Event): Boolean {
        // NIP-09/NIP-62: a deletion request against a deletion request or a
        // request to vanish has no effect — they are immune to kind-5 tombstones.
        if (event is DeletionEvent || event is RequestToVanishEvent) return false
        val owner = event.owner()

        suspend fun deletionExists(
            tagKey: String,
            value: String,
        ): Boolean = index.search(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = listOf(owner), tags = mapOf(tagKey to listOf(value)), since = event.createdAt, limit = 1)).isNotEmpty()
        if (deletionExists("e", event.id)) return true
        val address = event.addressOrNull() ?: return false
        return deletionExists("a", address)
    }

    /** NIP-62: a stored vanish request by this event's OWNER covering [relay] blocks their events up to its time. */
    private suspend fun isVanished(event: Event): Boolean {
        val vanishes = index.search(EventQuery(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(event.owner()), since = event.createdAt))
        return vanishes.any { doc -> (doc.toEvent() as? RequestToVanishEvent)?.shouldVanishFrom(relay) == true }
    }

    /**
     * NIP-09 enforcement: erase this kind 5's targets — by id when the doc's
     * OWNER is the deletion author (a recipient deletes gift-wraps sent to
     * them), by address (addressable AND replaceable kinds) up to the
     * deletion's created_at, same author only. The event itself is stored
     * after, as the tombstone.
     */
    private suspend fun applyDeletion(ev: DeletionEvent) {
        // Deletions routinely carry dozens of e-tags: resolve them with
        // bounded-concurrent gets (light doc-API reads) and remove the victims
        // in ONE pipelined removeDocs — which also hands the trust projection
        // its batch react instead of a re-read per id.
        val byId =
            ev
                .deleteEventIds()
                .distinct()
                .mapBounded(TARGET_GET_FANOUT) { index.get(it) }
                .filterNotNull()
                // NIP-09/NIP-62: kind 5 against a kind 5 or a kind 62 has no effect.
                .filter { it.kind != DeletionEvent.KIND && it.kind != RequestToVanishEvent.KIND }
                .filter { it.owner == ev.pubKey }
        if (byId.isNotEmpty()) index.removeDocs(byId)
        for (address in ev.deleteAddresses()) {
            if (address.pubKeyHex != ev.pubKey) continue
            if (!address.kind.isAddressable() && !address.kind.isReplaceable()) continue
            val victims =
                index
                    .search(EventQuery(kinds = listOf(address.kind), authors = listOf(address.pubKeyHex), until = ev.createdAt))
                    // Replaceable kinds have ONE address regardless of the a-tag's d part.
                    .filter { !address.kind.isAddressable() || it.dTagOrEmpty() == address.dTag }
            if (victims.isNotEmpty()) index.removeDocs(victims)
        }
    }

    /**
     * NIP-62 enforcement: when the request covers [relay], erase the owner's
     * history "until its created_at" — INCLUSIVE, per the spec. The request
     * itself is only stored after this runs, so it survives its own sweep.
     */
    private suspend fun applyVanish(ev: RequestToVanishEvent) {
        if (!ev.shouldVanishFrom(relay)) return
        sweep(EventQuery(owners = listOf(ev.pubKey), until = ev.createdAt))
    }

    // ---- full-text reindex ----------------------------------------------------

    /**
     * Re-derive the search fields for every stored event. Which kinds are
     * searchable — and how [SearchExtractors] decomposes them — is baked into
     * this build, so docs indexed under old code can be stale (or missing from
     * search) until this runs. It also clears fields for kinds that LOST
     * searchability.
     */
    override suspend fun reindexFullTextSearch() {
        var cursor: String? = null
        do {
            val progress = reindexFullTextSearch(cursor)
            cursor = progress.cursor
        } while (!progress.done)
    }

    /**
     * Resumable batch: one page of the engine's document walk
     * ([EventIndex.visitDocsPage]), with the walk's continuation carried in the
     * opaque [FtsReindexProgress.cursor]. O(page) memory and O(corpus) total —
     * the previous shape re-listed the ENTIRE corpus per batch (one unbounded
     * search response, sorted), which cannot finish on a large index.
     */
    override suspend fun reindexFullTextSearch(
        resumeFrom: String?,
        batchSize: Int,
    ): FtsReindexProgress =
        writes.withLock {
            val page = index.visitDocsPage(EventQuery(), resumeFrom, batchSize)
            // ONE pipelined write per page: serial awaited puts pay per-op ack
            // latency — on a churny reindex (extractor upgrade touching most
            // docs) that is hours of pure latency the feed client exists to hide.
            val changed = ArrayList<EventDoc>()
            for (doc in page.docs) {
                val fields = SearchExtractors.extract(doc.toEvent())
                if (fields != doc.search) changed += doc.copy(search = fields)
            }
            if (changed.isNotEmpty()) index.putAll(changed)
            FtsReindexProgress(cursor = page.continuation, processedThisBatch = page.docs.size, done = page.continuation == null)
        }

    override fun close() = index.close()

    private companion object {
        // Runaway backstop, not a delete cap: a sweep that spins this long
        // throws (see sweep) instead of looping forever — the non-shrinking
        // case is caught earlier, after ONE repeated page. Only meaningful
        // because the rounds are page-sized (see sweepPage) — with an
        // unbounded read the loop always finishes in one round and this is dead.
        const val MAX_SWEEP_ROUNDS = 10_000

        // Runs at least this long take the bulk path; smaller ones aren't
        // worth the setup and stay on the per-event path.
        const val BULK_MIN = 16

        // Concurrent doc-API gets when resolving a kind 5's e-tag targets.
        // Gets are light (no summary stage to overrun), so this floats above
        // QUERY_FANOUT; the real client's own id fan-out uses 32.
        const val TARGET_GET_FANOUT = 16

        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)

        /** [NEWEST_FIRST] for the raw read path — the same created_at desc, id asc order over [RawEvent]s. */
        val RAW_NEWEST_FIRST = compareByDescending(RawEvent::createdAt).thenBy(RawEvent::id)
    }
}
