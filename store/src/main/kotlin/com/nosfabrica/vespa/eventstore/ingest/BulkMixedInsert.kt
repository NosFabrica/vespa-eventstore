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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.async.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.async.mapBounded
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.ingest.Rejections
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.owner
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent

/**
 * The bulk path for a batch that CONTAINS deletions/vanishes (pure-record
 * batches take [BulkRecordInsert]). Run-splitting on every kind 5/62 would
 * collapse ingest to per-event speed (an outbox stream is ~98% kind 5), so
 * instead: batch-read the working set, REPLAY the sequential
 * [EventAdmission] rules against an in-memory snapshot (preserving
 * intra-batch ordering for free), then write the net diff in bulk. Correct by
 * construction — the replay runs the same code the per-event path does. Runs
 * under the writer lock, so query-then-write stays atomic.
 *
 * ONE ERASE IS NOT REPLAYED IN MEMORY: a vanish's sweep of the owner's stored
 * history. That history is unbounded, so it is never preloaded; the replay
 * sweeps what the snapshot holds, and [run] then applies the vanish to the
 * real index through [Deletions.applyVanish] — the per-event path's own paged
 * sweep — before it writes the batch's survivors (see [run] for why that order
 * is exact).
 */
internal class BulkMixedInsert(
    private val index: EventIndex,
    private val relay: NormalizedRelayUrl?,
    private val nowSecs: () -> Long,
    private val guards: GuardOwners,
    /** The store's own sweep page size: a vanish in the batch sweeps the snapshot, and then the index, through it. */
    private val sweepPage: Int,
) {
    /** The vanish erase against the REAL index — the per-event path's paged [Deletions.sweep]. */
    private val erase = Deletions(index, relay, sweepPage)

    /** Preload, replay, write the diff. Callers hold the store's writer lock across the whole run. */
    suspend fun run(events: List<Event>): List<IEventStore.InsertOutcome> {
        val snapshot = InMemoryEventIndex()
        preloadWorkingSet(snapshot, events)
        val beforeDocs = snapshot.search(EventQuery())
        val before = beforeDocs.mapTo(HashSet()) { it.id }
        // The exact per-event rules, over the snapshot — the SAME class the
        // per-event path runs, so the two cannot drift. Its guards are the
        // snapshot's own (default SHARED_STRICT: no cache, probe every time),
        // which is what a replay wants — the real store's cache answers for
        // the real index.
        val replay = EventAdmission(snapshot, Deletions(snapshot, relay, sweepPage), GuardOwners(snapshot))
        val outcomes = events.map { replay.tryAdmit(it) }
        val after = snapshot.search(EventQuery())
        val afterIds = after.mapTo(HashSet()) { it.id }
        // The removed DOCS (not just ids) were preloaded into the snapshot, so
        // removeDocs lets the projection react without re-reading each one.
        val removed = beforeDocs.filter { it.id !in afterIds }
        val added = after.filter { it.id !in before }
        if (removed.isNotEmpty()) index.removeDocs(removed)
        // The vanishes' erase of what the snapshot never held. Per owner, the
        // newest ADMITTED vanish covering this relay decides it: each one
        // erases the owner's stored history up to its own created_at, so for
        // documents stored BEFORE this batch the union is the newest one's
        // window, whatever the batch order. Applied after the removes and
        // BEFORE the puts, which is what makes it exact: the index then holds
        // no event of this batch, and the replay already decided every one of
        // them (an owner's event inside a window was erased or VANISHED there;
        // the surviving request itself, which its own window covers, is put
        // after). Paged by [sweepPage], so neither the read nor the remove is
        // ever the whole history, and a deployment that caps unlimited hits
        // cannot cut it short: the preload query this replaced came back
        // truncated there, and the vanish was reported Accepted over the rest.
        outcomes
            .zip(events)
            .mapNotNull { (outcome, e) -> (e as? RequestToVanishEvent)?.takeIf { outcome is IEventStore.InsertOutcome.Accepted && it.shouldVanishFrom(relay) } }
            .groupBy { it.pubKey }
            .forEach { (_, vanishes) -> erase.applyVanish(vanishes.maxBy { it.createdAt }) }
        if (added.isNotEmpty()) index.putAll(added)
        added.forEach {
            when (it.kind) {
                DeletionRequestEvent.KIND -> guards.noteDeletionStored(it.pubkey)
                RequestToVanishEvent.KIND -> guards.noteVanishStored(it.pubkey)
            }
        }
        return outcomes
    }

    /**
     * Load every stored doc [run]'s replay could read — a SUPERSET of what the
     * per-event insert's queries touch: existing ids, the owners'
     * tombstones/vanishes, address versions and deletion a-tag targets. Fanned
     * out as independent reads; the replay then needs no further I/O. NOT a
     * vanish owner's history: [run] erases that from the index, paged, after
     * the replay — nothing the replay DECIDES reads it (dedup, guards and
     * supersession each have their own preload above).
     */
    private suspend fun preloadWorkingSet(
        snapshot: InMemoryEventIndex,
        events: List<Event>,
    ) {
        val deletions = events.filterIsInstance<DeletionRequestEvent>()
        val owners = events.map { it.owner() }.distinct()
        val batchIds = events.map { it.id }
        val records = events.filter { it !is DeletionRequestEvent && it !is RequestToVanishEvent }
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

                // Guards: the flagged owners' stored tombstones targeting THEIR
                // OWN records in this batch (NIP-09 is same-owner-only, and a
                // kind 5/62 is immune, so nobody else's ids can be covered), in
                // the record path's shape — see [tombstoneQueries]. This sent the
                // WHOLE batch's ids and addresses, unchunked, in every owner
                // chunk's query: one YQL per chunk as wide as the batch, under
                // both locks. Then their vanishes.
                val flagged = flaggedDeleters.toHashSet()
                addAll(tombstoneQueries(records.filter { it.owner() in flagged }.groupBy { it.owner() }, PRELOAD_CHUNK))
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

                // Deletion a-tag targets, exactly the set the replay can touch
                // (Deletions.applyDeletion): SAME-OWNER addresses only — a
                // cross-author kind 5 is stored but inert, and preloading the
                // addresses it names pulled a whole foreign (kind, author)
                // corpus into the snapshot under both locks (a kind 5 naming
                // one provider's 30382 address fetched that provider's 242k
                // cards, then deleted none of them); narrowed by non-empty d,
                // the same pushdown the per-event path and putIfNewer apply;
                // and no newer than the deletion itself, which is the replay's
                // own `until`. Grouped by kind and chunked by author like the
                // other preload shapes — never one query per (kind, author).
                val ownAddresses =
                    deletions.flatMap { del ->
                        del
                            .deleteAddresses()
                            .filter { it.pubKeyHex == del.pubKey && (it.kind.isAddressable() || it.kind.isReplaceable()) }
                            .map { it to del.createdAt }
                    }
                // Replaceable kinds, and addressable ones with an empty d (no
                // "d:" pair to match on): by (kind, author), up to the newest
                // deletion naming that pair.
                ownAddresses
                    .filter { (a, _) -> !a.kind.isAddressable() || a.dTag.isEmpty() }
                    .groupBy({ (a, _) -> a.kind to a.pubKeyHex }, { (_, at) -> at })
                    .entries
                    .groupBy({ it.key.first }, { it.key.second to it.value.max() })
                    .forEach { (kind, authorsAt) ->
                        authorsAt.chunked(PRELOAD_CHUNK).forEach { chunk ->
                            add(EventQuery(kinds = listOf(kind), authors = chunk.map { it.first }, until = chunk.maxOf { it.second }))
                        }
                    }
                // Addressable with a d: by (kind, author, d-tags).
                ownAddresses
                    .filter { (a, _) -> a.kind.isAddressable() && a.dTag.isNotEmpty() }
                    .groupBy({ (a, _) -> a.kind to a.pubKeyHex }, { (a, at) -> a.dTag to at })
                    .forEach { (ka, dsAt) ->
                        val until = dsAt.maxOf { it.second }
                        dsAt.map { it.first }.distinct().chunked(PRELOAD_CHUNK).forEach { ds ->
                            add(EventQuery(kinds = listOf(ka.first), authors = listOf(ka.second), tags = mapOf("d" to ds), until = until))
                        }
                    }
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
