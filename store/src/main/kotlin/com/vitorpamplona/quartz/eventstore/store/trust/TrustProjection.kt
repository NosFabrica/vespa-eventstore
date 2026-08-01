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
package com.vitorpamplona.quartz.eventstore.store.trust

import com.vitorpamplona.quartz.eventstore.vespa.IngestStats
import com.vitorpamplona.quartz.eventstore.vespa.QUERY_FANOUT
import com.vitorpamplona.quartz.eventstore.vespa.client.DocRef
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.ReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationCells
import com.vitorpamplona.quartz.eventstore.vespa.mapBounded
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * Maintains the `reputation` parent documents — the per-pubkey trust tensors the
 * schema imports into every event's ranking. It works as an [EventIndex]
 * DECORATOR: it wraps the index the store writes through, so every mutation that
 * touches trust data triggers a recompute.
 *
 * Observing the index (not the events) is the whole trick. The store's semantic
 * machinery — supersession, kind-5, vanish, sweeps, admin deletes — all funnels
 * into [put]/[remove] calls here, so every deletion style updates the tensors
 * with ZERO deletion-specific code.
 *
 * This class decides WHEN the tensors change; HOW a subject is re-derived lives
 * in [TrustRecompute], and the startup repair for drift no write can reach (a
 * corpus mirrored before its provider lists) is [TrustReconciler].
 *
 * Recomputes run inline with the store's single-writer insert, so ranking is
 * read-your-writes consistent with the event corpus.
 */
class TrustProjection(
    private val inner: EventIndex,
    private val reputations: ReputationIndex,
) : EventIndex {
    /** The recompute engine the write triggers below drive; [TrustReconciler] shares it. */
    internal val recompute = TrustRecompute(inner, reputations)

    override suspend fun get(id: String): EventDoc? = inner.get(id)

    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query)

    // MUST delegate, not ride the interface default: the default would call this
    // decorator's search() (parsed EventDocs) and lose the inner client's raw
    // passthrough — the whole point of the raw path (see EventIndex.rawSearch).
    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = inner.rawSearch(query)

    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) = inner.visitIds(query, withDTag, onPage)

    override suspend fun count(query: EventQuery): Int = inner.count(query)

    // The author/kind aggregates below MUST forward to inner, not ride the
    // interface default: the default reconstructs the whole match set through
    // this decorator's search() just to project one field, where the real client
    // answers server-side (a grouping) or streams (a visit). scanAuthors in
    // particular backs the guard-owner Bloom preload, which walks the ENTIRE
    // corpus — materializing that as documents is the difference between a
    // paged walk and an OOM.
    override suspend fun distinctAuthors(query: EventQuery): Set<String> = inner.distinctAuthors(query)

    override suspend fun scanAuthors(query: EventQuery): Set<String> = inner.scanAuthors(query)

    override suspend fun countDistinctAuthors(query: EventQuery): Int = inner.countDistinctAuthors(query)

    override suspend fun countByKind(query: EventQuery): Map<Int, Int> = inner.countByKind(query)

    override fun close() {
        inner.close()
        reputations.close()
    }

    // NOTE — this decorator deliberately does NOT forward supersedesViaPut or
    // override putIfNewer, so it rides the read-then-supersede default (which
    // routes through this put()/remove(), firing react() for BOTH the superseded
    // old version and the new one). The engine's address-keyed conditional put
    // (VespaEventIndex under VESPA_ADDRESS_KEYED) is a single atomic op that never
    // exposes the removed old doc, so a 10040 that drops a service would leave that
    // service's stored scores un-reattributed, and a bulk card load would lose the
    // zero-read putAll cell update below. The conditional-put fast path therefore
    // engages only on an undecorated index; through the trust projection,
    // supersession stays read-based to keep the tensors consistent.
    override suspend fun put(doc: EventDoc) {
        inner.put(doc)
        react(doc)
    }

    /**
     * The bulk path writes ranking with ZERO reads. The store's supersession
     * guarantees every card reaching this putAll is the NEWEST version of its
     * (service, subject) address, so its rank/followers can be applied as a
     * tensor-cell UPDATE ([ReputationIndex.updateCells]) directly. Measured on an
     * 11M-card load, re-deriving parents from re-fetched cards was 44% of the
     * entire ingest wall clock.
     *
     * Semantics note (many services -> ONE observer cell): the cell holds the
     * latest-arriving mapped card's value, where the full derivation held an
     * arbitrary one. Equally arbitrary, and an order of magnitude cheaper. A
     * RETRACTION (a card whose rank tag disappeared) can't be applied blindly,
     * because another service's card may still back the cell. So those rare
     * subjects take the exact recompute path; deletions and 10040 changes always
     * did.
     */
    override suspend fun putAll(docs: List<EventDoc>) {
        IngestStats.timed("write") { inner.putAll(docs) }
        // Provider lists first (ONE walk over the union): they change the service->observer map the scores are attributed through.
        recompute.recomputeSubjectsOf(docs.filter { it.kind == TrustProviderListEvent.KIND })
        val cards = docs.filter { it.kind == ContactCardEvent.KIND }
        if (cards.isEmpty()) return
        val serviceToObserver = recompute.providerMap()
        val updates = ArrayList<ReputationCells>(cards.size)
        val retracted = LinkedHashSet<String>()
        for (doc in cards) {
            val subject = subjectOf(doc) ?: continue
            val observer = serviceToObserver[doc.pubkey] ?: continue
            val card = Event.fromJsonOrNull(doc.toEventJson()) as? ContactCardEvent ?: continue
            val influence = card.rank()
            val followers = card.followerCount()?.toDouble()
            if (influence != null && followers != null) {
                updates += ReputationCells(subject, observer, influence, followers)
            } else {
                // A card MISSING either dimension can't take the zero-read cell
                // update. updateCells only ADDS cells, so a null dimension would
                // leave the OTHER tensor's prior cell stale (bulk would diverge
                // from the single-doc derive, which drops it). Any partial or
                // full retraction goes through the read-based recompute, which
                // rebuilds the subject's whole doc from the newest stored cards.
                retracted += subject
            }
        }
        IngestStats.timed("proj.write") { reputations.updateCells(updates) }
        if (retracted.isNotEmpty()) recompute.recomputeBatch(retracted.toList(), serviceToObserver, removeEmpties = true)
    }

    override suspend fun remove(id: String) {
        // The doomed doc says what the removal invalidates — read before deleting.
        val doc = inner.get(id)
        inner.remove(id)
        doc?.let { react(it) }
    }

    /**
     * Bulk remove: read the doomed docs (what each removal invalidates), delete
     * them all pipelined, then react ONCE for the whole set. Every removed
     * 30382's subject is re-derived in a single batch, not one recompute per doc.
     */
    override suspend fun removeAll(ids: List<String>) {
        val docs = ids.mapBounded(QUERY_FANOUT) { inner.get(it) }.filterNotNull()
        inner.removeAll(ids)
        recompute.recomputeSubjectsOf(docs.filter { it.kind == TrustProviderListEvent.KIND })
        val subjects = docs.filter { it.kind == ContactCardEvent.KIND }.mapNotNull { subjectOf(it) }.distinct()
        if (subjects.isNotEmpty()) recompute.recomputeBatch(subjects, recompute.providerMap(), removeEmpties = true)
    }

    private suspend fun react(doc: EventDoc) {
        when (doc.kind) {
            ContactCardEvent.KIND -> subjectOf(doc)?.let { recompute.recompute(it) }
            TrustProviderListEvent.KIND -> recompute.recomputeSubjectsOf(listOf(doc))
        }
    }
}
