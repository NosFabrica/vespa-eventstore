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

import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.QUERY_FANOUT
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.SearchFields
import com.vitorpamplona.quartz.eventstore.vespa.mapBounded
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
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
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
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
 * Quartz [IEventStore] backed by the search engine itself: ONE copy of the
 * data, queryable with full NIP-01 filters plus NIP-50 search, and wrappable in
 * `ObservableEventStore` like any other store.
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
 *    after extractor/Quartz upgrades.
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
class NostrEventStore(
    private val index: EventIndex,
    override val relay: NormalizedRelayUrl? = null,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
) : IEventStore {
    private val writes = Mutex()

    // Owners with any stored tombstone/vanish; everyone else's inserts skip the
    // NIP-09/62 guard probes entirely (see GuardOwners for the safety argument).
    private val guards = GuardOwners(index)

    private val bulkInsert = BulkInsert(index, relay, guards)

    override suspend fun insert(event: Event) = writes.withLock { insertLocked(event) }

    /**
     * Batches take a BULK path — the per-event path costs 3–5 index round-trips
     * each (dup probe, tombstone probe, vanish probe, supersession), which caps
     * ingest in the low thousands per second, useless against a million-event
     * sync. Two shapes, by whether the batch mutates via deletions:
     *
     *  - PURE RECORDS: [BulkInsert] chunks the read checks and pipelines one
     *    [EventIndex.putAll]; its dedup + guard reads run outside the writer lock
     *    so parallel relays overlap them.
     *  - CONTAINS kind 5/62: [bulkMixedInsert] batch-reads the working set and
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
            // A batch that CONTAINS deletions/vanishes. Run-splitting on every
            // kind 5/62 would leave nothing to batch (an outbox stream is ~98%
            // kind 5), collapsing ingest to per-event speed. Instead: batch-read
            // the working set, replay the exact per-event rules in memory, then
            // write the net diff back in bulk.
            writes.withLock { bulkMixedInsert(events) }
        } else {
            // Pure records. The read checks (dedup + guards) run OUTSIDE the
            // writer lock so parallel relays' batches overlap them; only the
            // supersession read+resolve and the writes take the lock
            // (query-then-write stays atomic — commit sees every prior commit).
            val plan = bulkInsert.plan(events)
            writes.withLock { bulkInsert.commit(plan) }
        }
    }

    /**
     * The bulk path for a batch that contains deletions/vanishes: batch-read the
     * working set those events touch, REPLAY the sequential [insertLocked] rules
     * against an in-memory snapshot (zero I/O, so intra-batch ordering — a
     * deletion targeting an earlier event, a tombstone blocking a later one — is
     * preserved for free), then write the net diff back in bulk. Correct BY
     * CONSTRUCTION: the replay runs the same code the per-event path does.
     *
     * Turns the per-event path's O(events × round trips) into O(working-set
     * reads) + one pipelined write — the difference between weeks and hours on a
     * deletion-heavy sync. Runs under the writer lock (query-then-write stays
     * atomic); moving the preload reads out of it is a further refinement.
     */
    private suspend fun bulkMixedInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
        val snapshot = InMemoryEventIndex()
        preloadWorkingSet(snapshot, events)
        val before = snapshot.search(EventQuery()).mapTo(HashSet()) { it.id }
        // A throwaway store over the snapshot replays the exact per-event rules.
        val replay = NostrEventStore(snapshot, relay, nowSecs)
        val outcomes =
            events.map { e ->
                try {
                    replay.insert(e)
                    IEventStore.InsertOutcome.Accepted
                } catch (ex: RejectedException) {
                    IEventStore.InsertOutcome.Rejected(ex.message ?: Rejections.INSERT_FAILED)
                }
            }
        val after = snapshot.search(EventQuery())
        val afterIds = after.mapTo(HashSet()) { it.id }
        val removed = before.filter { it !in afterIds }
        val added = after.filter { it.id !in before }
        if (removed.isNotEmpty()) index.removeAll(removed)
        if (added.isNotEmpty()) index.putAll(added)
        added.forEach { if (it.kind == DeletionEvent.KIND || it.kind == RequestToVanishEvent.KIND) guards.noteGuardStored(it.pubkey) }
        return outcomes
    }

    /**
     * Load every stored doc [bulkMixedInsert]'s replay could read — a SUPERSET
     * of what [insertLocked]'s queries touch for this batch: existing ids (dup +
     * deletion e-tag targets), the owners' tombstones/vanishes (guards +
     * immunity), the records' address versions (supersession), each deletion's
     * a-tag targets, and each vanish owner's history (the sweep). All fanned out
     * as independent reads; the replay then needs no further I/O.
     */
    private suspend fun preloadWorkingSet(
        snapshot: InMemoryEventIndex,
        events: List<Event>,
    ) {
        val deletions = events.filterIsInstance<DeletionEvent>()
        val vanishes = events.filterIsInstance<RequestToVanishEvent>()
        val owners = events.map { it.owner() }.distinct()
        val batchIds = events.map { it.id }
        val batchAddresses = events.mapNotNull { it.addressOrNull() }.distinct()
        val records = events.filter { it !is DeletionEvent && it !is RequestToVanishEvent }
        // Only owners that provably HAVE a stored tombstone/vanish can guard this
        // batch (GuardOwners) — for everyone else both probes come back empty, so
        // skip them. A content batch touches ~500 owners but almost none are
        // flagged, turning ~3 heavy queries/owner-chunk into zero. This is the same
        // gate the per-event path (insertLocked) and the pure-record bulk path
        // (BulkInsert.plan) already apply; the mixed path was missing it.
        val guardedOwners = guards.filterFlagged(owners)

        val queries =
            buildList {
                // Existing docs by id: batch ids + every deletion's e-tag targets.
                (batchIds + deletions.flatMap { it.deleteEventIds() }).distinct().chunked(PRELOAD_CHUNK).forEach { add(EventQuery(ids = it)) }

                // Guards + immunity: the owners' stored tombstones (targeting this
                // batch's ids/addresses) and their vanishes — only for flagged owners.
                guardedOwners.chunked(PRELOAD_CHUNK).forEach { auth ->
                    if (batchIds.isNotEmpty()) add(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = auth, tags = mapOf("e" to batchIds)))
                    if (batchAddresses.isNotEmpty()) add(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = auth, tags = mapOf("a" to batchAddresses)))
                    add(EventQuery(kinds = listOf(RequestToVanishEvent.KIND), authors = auth))
                }

                // Supersession: existing versions of every record address (same
                // shapes as currentVersions — replaceable by (kind, authors),
                // addressable by (kind, author, d-tags), empty-d stays broad).
                for ((kind, evs) in records.filter { it.kind.isReplaceable() && !it.kind.isAddressable() }.groupBy { it.kind }) {
                    evs
                        .map { it.pubKey }
                        .distinct()
                        .chunked(PRELOAD_CHUNK)
                        .forEach { add(EventQuery(kinds = listOf(kind), authors = it)) }
                }
                for ((ka, evs) in records.filter { it.kind.isAddressable() }.groupBy { it.kind to it.pubKey }) {
                    val ds = evs.mapNotNull { it.tags.dTag() }.distinct()
                    ds.chunked(PRELOAD_CHUNK).forEach { add(EventQuery(kinds = listOf(ka.first), authors = listOf(ka.second), tags = mapOf("d" to it))) }
                    if (evs.any { it.tags.dTag().isNullOrEmpty() }) add(EventQuery(kinds = listOf(ka.first), authors = listOf(ka.second)))
                }

                // Deletion a-tag targets: the author's events of that kind.
                deletions
                    .flatMap { it.deleteAddresses() }
                    .filter { it.kind.isAddressable() || it.kind.isReplaceable() }
                    .map { it.kind to it.pubKeyHex }
                    .distinct()
                    .forEach { (kind, author) -> add(EventQuery(kinds = listOf(kind), authors = listOf(author))) }

                // Vanish sweep: the owner's whole history (the replay filters by until).
                vanishes.map { it.pubKey }.distinct().forEach { add(EventQuery(owners = listOf(it))) }
            }

        com.vitorpamplona.quartz.eventstore.vespa.IngestStats.timed("preload") {
            // Every preload query feeds a WRITE decision (dedup, guards, supersession,
            // vanish scope), so none of them carries a limit.
            queries.mapBounded(QUERY_FANOUT) { index.search(it) }.forEach { page -> page.forEach { snapshot.put(it) } }
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
        coroutineScope {
            val existing = async { index.get(event.id) }
            // Guard probes run only when this owner HAS a stored tombstone or
            // vanish (GuardOwners) — for everyone else both probes provably
            // come back empty, so the common-case insert reads just the dup get.
            val probeGuards = guards.mightHaveGuards(event.owner())
            val deleted = if (probeGuards) async { isDeleted(event) } else null
            val vanished = if (probeGuards) async { isVanished(event) } else null
            if (existing.await() != null) throw RejectedException(Rejections.DUPLICATE)
            if (deleted?.await() == true) throw RejectedException(Rejections.DELETED)
            if (vanished?.await() == true) throw RejectedException(Rejections.VANISHED)
        }
        when {
            event is DeletionEvent -> {
                applyDeletion(event)
                index.put(event.toDoc())
                guards.noteGuardStored(event.pubKey)
            }

            event is RequestToVanishEvent -> {
                applyVanish(event)
                index.put(event.toDoc())
                guards.noteGuardStored(event.pubKey)
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
     * Map a filter to an [EventQuery] stamped with the current expiry cutoff
     * (NIP-40) and, for searches, the ranking observer. An explicit `observer:`
     * search token wins over the connection [observer] — a client may ask to
     * rank through any lens (scores are public), so the query's own choice takes
     * precedence over the authenticated default.
     */
    private fun Filter.toExpiryQuery(observer: String? = null): EventQuery? = toEventQuery()?.let { it.copy(notExpiredAt = nowSecs(), observer = it.observer ?: observer) }

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
        val observer = coroutineContext[ObserverContext]?.pubkey
        val queries = restoreSearches(filters).mapNotNull { it.toExpiryQuery(observer) }
        val docs = searchDocs(queries)
        // NIP-50: a searching query's results stay in the engine's RELEVANCE
        // order "instead of the usual created_at ordering" — re-sorting here
        // would undo the rank profile. Plain filters keep NIP-01 recency.
        val ranked = queries.any { it.search != null || it.ranking != null }
        val ordered = if (ranked) docs else docs.sortedWith(NEWEST_FIRST)
        // Reconstruct via Quartz's by-kind factory straight from the stored fields,
        // skipping the toEventJson()->fromJson() serialize+parse round trip that was
        // the hot path's biggest allocator; see [toEvent].
        return ordered.map { it.toEvent() } as List<T>
    }

    /** Search every query concurrently (bounded), deduped by id — the shared recall for query and multi-filter count. */
    private suspend fun searchDocs(queries: List<EventQuery>): List<EventDoc> =
        when (queries.size) {
            0 -> emptyList()
            1 -> index.search(queries[0])
            else -> queries.mapBounded(QUERY_FANOUT) { index.search(it) }.flatten().distinctBy { it.id }
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
        val observer = coroutineContext[ObserverContext]?.pubkey
        val queries = restoreSearches(filters).mapNotNull { it.toExpiryQuery(observer) }
        val raw =
            when (queries.size) {
                0 -> return
                1 -> index.rawSearch(queries[0])
                else -> queries.mapBounded(QUERY_FANOUT) { index.rawSearch(it) }.flatten().distinctBy { it.id }
            }
        // NIP-50 relevance order is preserved for searching queries, exactly as in [query].
        val ranked = queries.any { it.search != null || it.ranking != null }
        val ordered = if (ranked) raw else raw.sortedWith(RAW_NEWEST_FIRST)
        ordered.forEach(onEach)
    }

    override suspend fun <T : Event> query(
        filter: Filter,
        onEach: (T) -> Unit,
    ) = query<T>(filter).forEach(onEach)

    override suspend fun <T : Event> query(
        filters: List<Filter>,
        onEach: (T) -> Unit,
    ) = query<T>(filters).forEach(onEach)

    override suspend fun count(filter: Filter): Int = restoreSearches(listOf(filter)).single().toExpiryQuery()?.let { index.count(it) } ?: 0

    /**
     * The distinct authors (pubkeys) matching [filter], via a server-side grouping
     * query ([EventIndex.distinctAuthors]) — for callers that need the author set
     * out of a huge match set without reconstructing every event (the orphan-score
     * sweep over millions of 30382s). Honors expiry like [count].
     */
    suspend fun distinctAuthors(filter: Filter): Set<HexKey> = restoreSearches(listOf(filter)).single().toExpiryQuery()?.let { index.distinctAuthors(it) } ?: emptySet()

    /**
     * Every distinct `d` tag (addressable subject) across [filter]'s matches, via
     * a document visit — the STREAMING walk, for a set too big to want in one
     * response, e.g. the hundreds of thousands of subjects one WoT provider
     * scores. A `search` would return them all too (no hit cap by default), but
     * materialized at once, and truncated outright if a deployment does set a
     * cap. The sync uses this to find every scored author to fetch content for.
     */
    suspend fun distinctDTags(filter: Filter): Set<String> {
        val q = restoreSearches(listOf(filter)).single().toExpiryQuery() ?: return emptySet()
        val out = HashSet<String>()
        index.visitIds(q, withDTag = true) { page ->
            page.forEach { it.dTag?.takeIf(String::isNotEmpty)?.let(out::add) }
            true
        }
        return out
    }

    override suspend fun count(filters: List<Filter>): Int {
        // Multi-filter counts need cross-filter id dedup (engine count can't),
        // but they don't need the events materialized — recall the docs and
        // count distinct ids, skipping the per-doc Event reconstruction.
        if (filters.size == 1) return count(filters[0])
        val queries = restoreSearches(filters).mapNotNull { it.toExpiryQuery() }
        return searchDocs(queries).size
    }

    /**
     * Undo Quartz's relay-side NIP-50 extension stripping. `LiveEventStore`
     * strips `key:value` tokens from every REQ's search before the store sees
     * it. That is the right default for stores that would match them as text,
     * but THIS store honors `sort:`/`filter:rank:`/`include:spam`. The relay
     * backend carries the pre-strip filters in [OriginalFilters] — the same list
     * in the same order, only `search` differs — so each filter's original
     * search string is restored before mapping. Direct callers (no context
     * element) are untouched.
     */
    private suspend fun restoreSearches(filters: List<Filter>): List<Filter> {
        val originals = coroutineContext[OriginalFilters]?.filters ?: return filters
        if (originals.size != filters.size) return filters
        return filters.mapIndexed { i, f ->
            val original = originals[i].search
            if (original != null && original != f.search) f.copy(search = original) else f
        }
    }

    /**
     * (created_at, id) pairs straight off the docs — no Event materialization
     * and no result cap. Plain filters walk the corpus through the engine's
     * visit ([com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex.visitIds]), so a negentropy
     * session (or a sync reconcile diff) sees the COMPLETE match set even when it
     * dwarfs the search page limit. Searching or limit'd filters keep the search
     * path, since their semantics live there.
     */
    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
    ): List<IdAndTime> {
        val all = ArrayList<IdAndTime>()
        // A single-filter cap can stop the walk early: the caller only needs to
        // learn the set exceeds the cap, not scan a 10M corpus to prove it.
        // (Multi-filter needs the full set for cross-filter dedup, so no break.)
        val cap = maxEntries?.takeIf { filters.size == 1 }?.plus(1)
        // Exclude already-expired events (NIP-40), exactly as query/count do.
        // Otherwise the negentropy set offers ids a plain REQ would never serve,
        // and a peer keeps trying to reconcile events we refuse to return.
        for (q in filters.mapNotNull { it.toExpiryQuery() }) {
            if (q.search == null && q.limit == null) {
                index.visitIds(q) { page ->
                    page.forEach { all += IdAndTime(it.createdAt, it.id) }
                    cap == null || all.size < cap
                }
            } else {
                index.search(q).forEach { all += IdAndTime(it.createdAt, it.id) }
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

    /** Remove every match, page by page, until the query comes back empty. */
    private suspend fun sweep(q: EventQuery) {
        var rounds = 0
        while (rounds++ < MAX_SWEEP_ROUNDS) {
            val page = index.search(q)
            if (page.isEmpty()) return
            index.removeAll(page.map { it.id })
            // A limit'd delete is satisfied by its first page.
            if (q.limit != null) return
        }
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
        return vanishes.any { doc ->
            (Event.fromJsonOrNull(doc.toEventJson()) as? RequestToVanishEvent)?.shouldVanishFrom(relay) == true
        }
    }

    /**
     * NIP-09 enforcement: erase this kind 5's targets — by id when the doc's
     * OWNER is the deletion author (a recipient deletes gift-wraps sent to
     * them), by address (addressable AND replaceable kinds) up to the
     * deletion's created_at, same author only. The event itself is stored
     * after, as the tombstone.
     */
    private suspend fun applyDeletion(ev: DeletionEvent) {
        for (id in ev.deleteEventIds()) {
            val doc = index.get(id) ?: continue
            // NIP-09/NIP-62: kind 5 against a kind 5 or a kind 62 has no effect.
            if (doc.kind == DeletionEvent.KIND || doc.kind == RequestToVanishEvent.KIND) continue
            if (doc.owner == ev.pubKey) index.remove(id)
        }
        for (address in ev.deleteAddresses()) {
            if (address.pubKeyHex != ev.pubKey) continue
            if (!address.kind.isAddressable() && !address.kind.isReplaceable()) continue
            index
                .search(EventQuery(kinds = listOf(address.kind), authors = listOf(address.pubKeyHex), until = ev.createdAt))
                // Replaceable kinds have ONE address regardless of the a-tag's d part.
                .filter { !address.kind.isAddressable() || it.dTagOrEmpty() == address.dTag }
                .forEach { index.remove(it.id) }
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
     * Resumable batch: docs are walked in id order from [resumeFrom]
     * (exclusive). This is reference-grade paging: each call re-lists the ids
     * through [EventIndex.search]. The real Vespa client will page with a visit.
     */
    override suspend fun reindexFullTextSearch(
        resumeFrom: String?,
        batchSize: Int,
    ): FtsReindexProgress =
        writes.withLock {
            val batch =
                index
                    .search(EventQuery())
                    .sortedBy { it.id }
                    .filter { resumeFrom == null || it.id > resumeFrom }
                    .take(batchSize)
            for (doc in batch) {
                val fields = Event.fromJsonOrNull(doc.toEventJson())?.let(SearchExtractors::extract) ?: SearchFields.NONE
                if (fields != doc.search) index.put(doc.copy(search = fields))
            }
            FtsReindexProgress(cursor = batch.lastOrNull()?.id, processedThisBatch = batch.size, done = batch.size < batchSize)
        }

    override fun close() = index.close()

    private companion object {
        // Page-sized rounds; a page of results per round means a runaway sweep
        // still terminates loudly rather than spinning forever.
        const val MAX_SWEEP_ROUNDS = 10_000

        // Runs at least this long take the bulk path; smaller ones aren't
        // worth the setup and stay on the per-event path.
        const val BULK_MIN = 16

        // Ids/authors/d-tags per preload query — well under the engine's page cap.
        const val PRELOAD_CHUNK = 500
        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)

        /** [NEWEST_FIRST] for the raw read path — the same created_at desc, id asc order over [RawEvent]s. */
        val RAW_NEWEST_FIRST = compareByDescending(RawEvent::createdAt).thenBy(RawEvent::id)
    }
}
