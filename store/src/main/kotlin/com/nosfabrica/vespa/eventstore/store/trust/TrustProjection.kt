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
package com.nosfabrica.vespa.eventstore.store.trust

import com.nosfabrica.vespa.eventstore.store.mapping.toEvent
import com.nosfabrica.vespa.eventstore.vespa.IngestStats
import com.nosfabrica.vespa.eventstore.vespa.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.vespa.client.DocRef
import com.nosfabrica.vespa.eventstore.vespa.client.DocsPage
import com.nosfabrica.vespa.eventstore.vespa.client.EventIndex
import com.nosfabrica.vespa.eventstore.vespa.client.ReputationIndex
import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc
import com.nosfabrica.vespa.eventstore.vespa.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.vespa.mapBounded
import com.nosfabrica.vespa.eventstore.vespa.query.EventQuery
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
 * This class decides WHAT each mutation invalidates; the work itself is
 * declarative [DirtLedger.Dirt] settled through the ledger — inline by default
 * (read-your-writes, and what every unit test asserts), or DEFERRED to a
 * background drain when a signal is attached ([DirtLedger.deferTo]). Deferral
 * needs no event ordering: HOW a subject is re-derived ([TrustRecompute]) reads
 * the store's CURRENT state under the writer lock, so any drain schedule
 * converges — the price is only that ranking lags writes by the drain cycle.
 * The one order-sensitive projection write, the bulk zero-read cell update,
 * stays inline in BOTH modes for exactly that reason (and because it is already
 * the cheap path). The startup repair for drift no write can reach (a corpus
 * mirrored before its provider lists) is [TrustReconciler].
 *
 * Every trust-mutating op runs [DirtLedger.guarded]: the event write and the
 * projection write are separate acks, and dedup means a trigger fires once — a
 * crash between them used to be PERMANENT drift (the retry comes back
 * all-duplicates and never reaches this decorator). The ledger persists what
 * the op invalidates before it starts and repairs it at the next settle,
 * drain, or reconcile.
 */
class TrustProjection(
    private val inner: EventIndex,
    private val reputations: ReputationIndex,
    nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
) : EventIndex {
    /** The recompute engine the ledger's drains drive; [TrustReconciler] shares it. */
    internal val recompute = TrustRecompute(inner, reputations, nowSecs)

    /** The work ledger: crash marker + (optionally deferred) projection queue; [TrustReconciler] drains it at startup. */
    internal val dirt = DirtLedger(reputations, recompute)

    override suspend fun get(id: String): EventDoc? = inner.get(id)

    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query)

    override suspend fun existingIds(ids: List<String>): Set<String> = inner.existingIds(ids)

    // MUST delegate, not ride the interface default: the default would call this
    // decorator's search() (parsed EventDocs) and lose the inner client's raw
    // passthrough — the whole point of the raw path (see EventIndex.rawSearch).
    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = inner.rawSearch(query)

    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) = inner.visitIds(query, withDTag, onPage)

    override suspend fun visitTags(
        query: EventQuery,
        onPage: suspend (List<List<List<String>>>) -> Boolean,
    ) = inner.visitTags(query, onPage)

    override suspend fun count(query: EventQuery): Int = inner.count(query)

    // MUST forward like the walks above: the interface default re-lists the
    // ENTIRE corpus through this decorator's search() per page — the exact
    // O(corpus²) shape the visit-backed reindex replaced, resurrected one
    // layer up. Pure read; nothing to react to.
    override suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage = inner.visitDocsPage(query, resumeFrom, maxDocs)

    // The author/kind aggregates below MUST forward to inner, not ride the
    // interface default: the default reconstructs the whole match set through
    // this decorator's search() just to project one field, where the real client
    // answers server-side (a grouping) or streams (a visit). scanAuthors in
    // particular backs the guard-owner Bloom preload, which walks the ENTIRE
    // corpus — materializing that as documents is the difference between a
    // paged walk and an OOM.
    override suspend fun distinctAuthors(query: EventQuery): Set<String> = inner.distinctAuthors(query)

    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> = inner.countByAuthor(query)

    override suspend fun scanAuthors(query: EventQuery): Set<String> = inner.scanAuthors(query)

    override suspend fun countDistinctAuthors(query: EventQuery): Int = inner.countDistinctAuthors(query)

    override suspend fun countByKind(query: EventQuery): Map<Int, Int> = inner.countByKind(query)

    override fun close() {
        inner.close()
        reputations.close()
    }

    // NOTE — this decorator deliberately does NOT forward supersedesViaPut or
    // override putIfNewer, so it rides the read-then-supersede default (which
    // routes through this put()/remove(), recording dirt for BOTH the superseded
    // old version and the new one). The engine's address-keyed conditional put
    // (VespaEventIndex under VESPA_ADDRESS_KEYED) is a single atomic op that never
    // exposes the removed old doc, so a 10040 that drops a service would leave that
    // service's stored scores un-reattributed, and a bulk card load would lose the
    // zero-read putAll cell update below. The conditional-put fast path therefore
    // engages only on an undecorated index; through the trust projection,
    // supersession stays read-based to keep the tensors consistent.
    override suspend fun put(doc: EventDoc) {
        val work = opDirt(doc)
        dirt.guarded(work) {
            inner.put(doc)
            // The attribution cache must not outlive a 10040 change even while its
            // walk is deferred: later inline cell updates attribute through it.
            if (doc.kind == TrustProviderListEvent.KIND) recompute.invalidateProviders()
            Unit to work
        }
    }

    /**
     * The bulk path writes ranking with ZERO reads. The store's supersession
     * guarantees every card reaching this putAll is the NEWEST version of its
     * (service, subject) address, so its rank/followers can be applied as a
     * tensor-cell UPDATE ([ReputationIndex.updateCells]) directly — inline in
     * both ledger modes; deferring it would need an ordered durable queue where
     * everything else here needs none. Measured on an 11M-card load, re-deriving
     * parents from re-fetched cards was 44% of the entire ingest wall clock.
     *
     * Attribution is PER DIMENSION ([TrustProviders]): the card's rank tag
     * updates the influence cell of every observer naming its signer under
     * `30382:rank`, its followers tag the follower cell of those naming it
     * under `30382:followers` — a user may pick different services for the two,
     * and a rank provider's followers tag must not clobber the follower
     * provider's value. A shared provider (the NIP-85 norm) fans out to every
     * observer trusting it, exactly as [TrustRecompute]'s derive does; a cell's
     * null side leaves the other tensor untouched, which under per-dimension
     * ownership is correct — that cell belongs to the other provider's cards.
     *
     * Semantics note (many services -> one observer's cell): the cards are
     * applied in the same (created_at, then lowest-id-wins) order the full
     * derivation folds in, so WITHIN a batch the two paths agree; across
     * batches the cell holds the last-arriving batch's winner, which a full
     * derivation may order differently. Bounded arbitrariness, an order of
     * magnitude cheaper than reading. A RETRACTION (a card missing a tag its
     * signer is mapped for) can't be applied blindly, because another service's
     * card may still back the cell — those rare subjects become re-derive work,
     * as do the 10040s' service walks; deletions always did.
     */
    override suspend fun putAll(docs: List<EventDoc>) {
        dirt.guarded(putDirt(docs)) {
            IngestStats.timed("write") { inner.putAll(docs) }
            val listServices = ProviderMap.trustServicesOf(docs.filter { it.kind == TrustProviderListEvent.KIND }).toSet()
            if (listServices.isNotEmpty()) recompute.invalidateProviders()
            val cards = docs.filter { it.kind == ContactCardEvent.KIND }
            if (cards.isEmpty()) return@guarded Unit to DirtLedger.Dirt(emptySet(), listServices)
            val providers = recompute.providerMap()
            val updates = ArrayList<ReputationCells>(cards.size)
            val retracted = LinkedHashSet<String>()
            // Same fold order as the derive (newest wins, ties to the LOWEST id),
            // so a same-batch conflict between two services of one observer lands
            // the same cell a full re-derivation would.
            for (doc in cards.sortedWith(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id })) {
                val subject = subjectOf(doc) ?: continue
                val rankObservers = providers.rank[doc.pubkey].orEmpty()
                val followerObservers = providers.followers[doc.pubkey].orEmpty()
                if (rankObservers.isEmpty() && followerObservers.isEmpty()) continue
                val card = doc.toEvent() as? ContactCardEvent
                val influence = card?.boundedRank()
                val followers = card?.followerCount()?.toDouble()
                if ((rankObservers.isNotEmpty() && influence == null) || (followerObservers.isNotEmpty() && followers == null)) {
                    // A card MISSING a tag its signer is MAPPED for can't take the
                    // zero-read cell update. updateCells only ADDS cells, so the
                    // missing dimension's prior cell would linger (bulk would
                    // diverge from the derive, which drops it). Any such partial or
                    // full retraction becomes read-based re-derive work, which
                    // rebuilds the subject's whole doc from the newest stored
                    // cards. A card that fails reconstruction lands here too —
                    // freezing its cells on a parse regression would be silent
                    // drift.
                    retracted += subject
                    continue
                }
                // Union fan-out, one partial cell per observer: only the mapped
                // dimension(s) carry a value — an unmapped dimension stays null so
                // the other provider's cell survives.
                (rankObservers + followerObservers).forEach { observer ->
                    updates +=
                        ReputationCells(
                            subject,
                            observer,
                            influence.takeIf { observer in rankObservers },
                            followers.takeIf { observer in followerObservers },
                        )
                }
            }
            IngestStats.timed("proj.write") { reputations.updateCells(updates) }
            Unit to DirtLedger.Dirt(retracted, listServices)
        }
    }

    override suspend fun remove(id: String) {
        // The doomed doc says what the removal invalidates — read before deleting.
        val doc = inner.get(id)
        val work = removeDirt(listOfNotNull(doc))
        dirt.guarded(work) {
            // With the doc in hand, remove THROUGH it: the address-keyed client
            // resolves the docid locally instead of re-reading it per id.
            if (doc != null) inner.removeDocs(listOf(doc)) else inner.remove(id)
            if (doc?.kind == TrustProviderListEvent.KIND) recompute.invalidateProviders()
            Unit to work
        }
    }

    /**
     * Bulk remove by id: only TRUST docs (30382/10040) can invalidate the
     * projection, so read back just those — chunked kind-filtered searches,
     * never a get per id (a million-deletion sweep would pay a round trip per
     * doomed doc to learn it was a plain note). Then delete pipelined and
     * record ONE work set for the whole batch.
     */
    override suspend fun removeAll(ids: List<String>) {
        val docs =
            ids
                .chunked(REMOVE_CHUNK)
                .mapBounded(QUERY_FANOUT) { chunk -> inner.search(EventQuery(ids = chunk, kinds = TRUST_KINDS)) }
                .flatten()
        val work = removeDirt(docs)
        dirt.guarded(work) {
            inner.removeAll(ids)
            if (docs.any { it.kind == TrustProviderListEvent.KIND }) recompute.invalidateProviders()
            Unit to work
        }
    }

    /**
     * Bulk remove for callers that already HOLD the doomed docs (the store's
     * sweep just searched them; the mixed bulk path preloaded them): ZERO reads
     * — the docs themselves say what each removal invalidates.
     */
    override suspend fun removeDocs(docs: List<EventDoc>) {
        val work = removeDirt(docs)
        dirt.guarded(work) {
            inner.removeDocs(docs)
            if (docs.any { it.kind == TrustProviderListEvent.KIND }) recompute.invalidateProviders()
            Unit to work
        }
    }

    /** What ONE doc's write invalidates: a card its subject, a 10040 its trust services (either dimension), anything else nothing. */
    private fun opDirt(doc: EventDoc): DirtLedger.Dirt =
        when (doc.kind) {
            ContactCardEvent.KIND -> DirtLedger.Dirt(setOfNotNull(subjectOf(doc)), emptySet())
            TrustProviderListEvent.KIND -> DirtLedger.Dirt(emptySet(), ProviderMap.trustServicesOf(listOf(doc)).toSet())
            else -> DirtLedger.Dirt.NONE
        }

    /**
     * What a PUT of [docs] could leave stale, for the crash insurance. Card
     * subjects are recorded exactly while they fit [DIRT_SUBJECT_CAP] (repair =
     * one cheap batched re-derive); a bigger batch records the cards' SERVICES
     * instead — a few keys however large the batch, repaired by re-walking each
     * service (safe for puts: the cards exist in the store, so the walk reaches
     * every touched subject). 10040s always record their rank services, since
     * their blast radius is every subject those services ever scored.
     */
    private fun putDirt(docs: List<EventDoc>): DirtLedger.Dirt {
        val subjects = LinkedHashSet<String>()
        for (doc in docs) if (doc.kind == ContactCardEvent.KIND) subjectOf(doc)?.let(subjects::add)
        val services = LinkedHashSet(ProviderMap.trustServicesOf(docs.filter { it.kind == TrustProviderListEvent.KIND }))
        if (subjects.size > DIRT_SUBJECT_CAP) {
            docs.forEach { if (it.kind == ContactCardEvent.KIND) services += it.pubkey }
            subjects.clear()
        }
        return DirtLedger.Dirt(subjects, services)
    }

    /**
     * What a REMOVE of [docs] invalidates. Card subjects are recorded exactly
     * and NEVER coarsened to services: a removed card may have been its
     * subject's last, and a service re-walk enumerates subjects from stored
     * cards — it can never reach a subject with none left, whose parent doc
     * would linger as an orphan. The exact re-derive removes it.
     */
    private fun removeDirt(docs: List<EventDoc>): DirtLedger.Dirt {
        val subjects = LinkedHashSet<String>()
        for (doc in docs) if (doc.kind == ContactCardEvent.KIND) subjectOf(doc)?.let(subjects::add)
        val services = LinkedHashSet(ProviderMap.trustServicesOf(docs.filter { it.kind == TrustProviderListEvent.KIND }))
        return DirtLedger.Dirt(subjects, services)
    }

    private companion object {
        /** The only kinds whose removal can invalidate the projection. */
        val TRUST_KINDS = listOf(ContactCardEvent.KIND, TrustProviderListEvent.KIND)

        /** Ids per removeAll read-back query — round-trip width, not a result cap. */
        const val REMOVE_CHUNK = 500

        /**
         * Max subjects persisted per put insurance before coarsening to services.
         * Bounds the marker write (~64 bytes/subject) to noise against the batch
         * it brackets, while keeping the precise (and much cheaper) repair for
         * every normally-sized batch.
         */
        const val DIRT_SUBJECT_CAP = 5_000
    }
}
