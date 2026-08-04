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
package com.nosfabrica.vespa.eventstore.ingest

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.RejectedException
import com.nosfabrica.vespa.eventstore.Rejections
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.client.EventIndex
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.addressOrNull
import com.nosfabrica.vespa.eventstore.mapping.owner
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent

/**
 * The bulk path for a batch that CONTAINS deletions/vanishes (pure-record
 * batches take [BulkRecordInsert]). Run-splitting on every kind 5/62 would
 * collapse ingest to per-event speed (an outbox stream is ~98% kind 5), so
 * instead: batch-read the working set, REPLAY the sequential
 * [NostrSemanticsStore.insert] rules against an in-memory snapshot (preserving
 * intra-batch ordering for free), then write the net diff in bulk. Correct by
 * construction — the replay runs the same code the per-event path does. Runs
 * under the writer lock, so query-then-write stays atomic.
 */
internal class BulkMixedInsert(
    private val index: EventIndex,
    private val relay: NormalizedRelayUrl?,
    private val nowSecs: () -> Long,
    private val guards: GuardOwners,
) {
    /** Preload, replay, write the diff. Callers hold the store's writer lock across the whole run. */
    suspend fun run(events: List<Event>): List<IEventStore.InsertOutcome> {
        val snapshot = InMemoryEventIndex()
        preloadWorkingSet(snapshot, events)
        val beforeDocs = snapshot.search(EventQuery())
        val before = beforeDocs.mapTo(HashSet()) { it.id }
        // A throwaway store over the snapshot replays the exact per-event rules.
        val replay = NostrSemanticsStore(snapshot, relay, nowSecs)
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
        // The removed DOCS (not just ids) were preloaded into the snapshot, so
        // removeDocs lets the projection react without re-reading each one.
        val removed = beforeDocs.filter { it.id !in afterIds }
        val added = after.filter { it.id !in before }
        if (removed.isNotEmpty()) index.removeDocs(removed)
        if (added.isNotEmpty()) index.putAll(added)
        added.forEach {
            when (it.kind) {
                DeletionEvent.KIND -> guards.noteDeletionStored(it.pubkey)
                RequestToVanishEvent.KIND -> guards.noteVanishStored(it.pubkey)
            }
        }
        return outcomes
    }

    /**
     * Load every stored doc [run]'s replay could read — a SUPERSET of what the
     * per-event insert's queries touch: existing ids, the owners'
     * tombstones/vanishes, address versions, deletion a-tag targets, and each
     * vanish owner's history. Fanned out as independent reads; the replay then
     * needs no further I/O.
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
        // Only owners with a provably stored tombstone/vanish (GuardOwners) can
        // guard this batch; everyone else's probes come back empty, so skip
        // them — turning ~3 heavy queries/owner-chunk into zero. Gated per
        // guard kind, same as the per-event path and BulkRecordInsert.
        val flaggedDeleters = guards.filterFlaggedDeleters(owners)
        val flaggedVanishers = guards.filterFlaggedVanishers(owners)

        val queries =
            buildList {
                // Existing docs by id: batch ids + every deletion's e-tag targets.
                (batchIds + deletions.flatMap { it.deleteEventIds() }).distinct().chunked(PRELOAD_CHUNK).forEach { add(EventQuery(ids = it)) }

                // Guards + immunity: the owners' stored tombstones (targeting this
                // batch's ids/addresses) and their vanishes — only for flagged owners.
                flaggedDeleters.chunked(PRELOAD_CHUNK).forEach { auth ->
                    if (batchIds.isNotEmpty()) add(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = auth, tags = mapOf("e" to batchIds)))
                    if (batchAddresses.isNotEmpty()) add(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = auth, tags = mapOf("a" to batchAddresses)))
                }
                flaggedVanishers.chunked(PRELOAD_CHUNK).forEach { auth ->
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

                // Deletion a-tag targets: grouped by kind and chunked by author
                // like the other preload shapes — never one query per
                // (kind, author) pair, which was thousands of round trips under
                // the writer lock on a 10k mixed batch.
                deletions
                    .flatMap { it.deleteAddresses() }
                    .filter { it.kind.isAddressable() || it.kind.isReplaceable() }
                    .map { it.kind to it.pubKeyHex }
                    .distinct()
                    .groupBy({ it.first }, { it.second })
                    .forEach { (kind, authors) ->
                        authors.distinct().chunked(PRELOAD_CHUNK).forEach { add(EventQuery(kinds = listOf(kind), authors = it)) }
                    }

                // Vanish sweep: the owner's whole history (the replay filters by until).
                vanishes.map { it.pubKey }.distinct().forEach { add(EventQuery(owners = listOf(it))) }
            }

        IngestStats.timed("preload") {
            // Every preload query feeds a WRITE decision (dedup, guards, supersession,
            // vanish scope), so none of them carries a limit.
            queries.mapBounded(QUERY_FANOUT) { index.search(it) }.forEach { page -> page.forEach { snapshot.put(it) } }
        }
    }

    private companion object {
        // Ids/authors/d-tags per preload query. Not a result cap — these queries
        // carry no limit — just how wide one round trip is built.
        const val PRELOAD_CHUNK = 500
    }
}
