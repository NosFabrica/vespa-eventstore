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
 * Maintains the `reputation` parent documents (per-pubkey trust tensors the
 * schema imports into ranking) as an [EventIndex] DECORATOR: every deletion
 * style — supersession, kind-5, vanish, sweeps — funnels into [put]/[remove]
 * here, so the tensors stay current with ZERO deletion-specific code.
 *
 * Each mutation's invalidation is declared as [DirtLedger.Dirt] — settled
 * inline (read-your-writes; what the unit tests assert) or deferred to a
 * background drain ([DirtLedger.deferTo]). Deferral needs no event ordering:
 * [TrustRecompute] re-derives from the store's CURRENT state under the writer
 * lock, so any drain schedule converges; only the order-sensitive bulk
 * zero-read cell update stays inline in both modes. Every trust-mutating op
 * runs [DirtLedger.guarded]: the event and projection writes are separate acks
 * and dedup fires a trigger only once, so a crash between them would be
 * PERMANENT drift (the retry comes back all-duplicates) — the ledger persists
 * what the op invalidates before it starts and repairs it at the next settle,
 * drain, or reconcile. Drift no write trigger can see is [TrustReconciler]'s
 * job.
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

    // MUST delegate, not ride the interface default, which would route through
    // this decorator's search() and lose the raw passthrough (see EventIndex.rawSearch).
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

    // MUST forward: the interface default re-lists the ENTIRE corpus through
    // this decorator's search() per page — O(corpus²). Pure read; nothing to react to.
    override suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage = inner.visitDocsPage(query, resumeFrom, maxDocs)

    // The aggregates below MUST forward to inner: the interface defaults
    // materialize the whole match set via search() where the real client groups
    // or streams server-side. scanAuthors backs the guard-owner Bloom preload
    // over the ENTIRE corpus — materializing that is an OOM, not a paged walk.
    override suspend fun distinctAuthors(query: EventQuery): Set<String> = inner.distinctAuthors(query)

    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> = inner.countByAuthor(query)

    override suspend fun scanAuthors(query: EventQuery): Set<String> = inner.scanAuthors(query)

    override suspend fun countDistinctAuthors(query: EventQuery): Int = inner.countDistinctAuthors(query)

    override suspend fun countByKind(query: EventQuery): Map<Int, Int> = inner.countByKind(query)

    override fun close() {
        inner.close()
        reputations.close()
    }

    // NOTE — deliberately does NOT forward supersedesViaPut or override
    // putIfNewer: the read-then-supersede default routes through this
    // put()/remove(), recording dirt for BOTH old and new versions. The engine's
    // atomic conditional put never exposes the removed old doc, so through this
    // decorator supersession must stay read-based to keep the tensors consistent
    // (the fast path engages only on an undecorated index).
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
     * The bulk path writes ranking with ZERO reads: supersession guarantees each
     * card here is the newest for its (service, subject) address, so
     * rank/followers apply as tensor-cell UPDATEs ([ReputationIndex.updateCells])
     * — inline in both ledger modes (deferring would need an ordered durable
     * queue). Measured: re-deriving from re-fetched cards was 44% of an 11M-card
     * ingest's wall clock.
     *
     * Attribution is PER DIMENSION ([TrustProviders]): rank tags update
     * influence cells for `30382:rank` observers, followers tags the follower
     * cells for `30382:followers` observers; a cell's null side leaves the other
     * tensor untouched (that cell belongs to the other provider's cards). Cards
     * apply in the derive's fold order, so WITHIN a batch the two paths agree;
     * across batches the cell holds the last batch's winner — bounded
     * arbitrariness, an order of magnitude cheaper than reading. A RETRACTION
     * (a card missing a tag its signer is mapped for) can't apply blindly —
     * another service's card may still back the cell — so those subjects become
     * re-derive work, as do the 10040s' service walks.
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
            // so a same-batch conflict lands the cell a full re-derivation would.
            for (doc in cards.sortedWith(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id })) {
                val subject = subjectOf(doc) ?: continue
                val rankObservers = providers.rank[doc.pubkey].orEmpty()
                val followerObservers = providers.followers[doc.pubkey].orEmpty()
                if (rankObservers.isEmpty() && followerObservers.isEmpty()) continue
                val card = doc.toEvent() as? ContactCardEvent
                val influence = card?.boundedRank()
                val followers = card?.followerCount()?.toDouble()
                if ((rankObservers.isNotEmpty() && influence == null) || (followerObservers.isNotEmpty() && followers == null)) {
                    // updateCells only ADDS cells, so a missing mapped dimension
                    // would leave its prior cell lingering (diverging from the
                    // derive, which drops it). Retractions — and cards that fail
                    // reconstruction, else a parse regression is silent drift —
                    // become read-based re-derive work instead.
                    retracted += subject
                    continue
                }
                // One partial cell per observer: an unmapped dimension stays null
                // so the other provider's cell survives.
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
     * projection, so read back just those via chunked kind-filtered searches —
     * never a get per id — then delete pipelined with ONE work set for the batch.
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
     * Bulk remove for callers that already HOLD the doomed docs: ZERO reads —
     * the docs themselves say what each removal invalidates.
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
     * Crash insurance for a PUT of [docs]. Card subjects are recorded exactly
     * while they fit [DIRT_SUBJECT_CAP]; a bigger batch records the cards'
     * SERVICES instead — few keys however large the batch, repaired by
     * re-walking each service (safe for puts: the stored cards make the walk
     * reach every touched subject). 10040s always record their services — their
     * blast radius is every subject those services ever scored.
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
     * What a REMOVE of [docs] invalidates. Card subjects are recorded exactly,
     * NEVER coarsened to services: a removed card may have been its subject's
     * last, and a service re-walk (enumerating from stored cards) can never
     * reach a subject with none left — only the exact re-derive removes its
     * orphaned parent doc.
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
         * Max subjects persisted per put insurance before coarsening to services
         * — bounds the marker write to noise against the batch it brackets while
         * keeping the precise, cheaper repair for normally-sized batches.
         */
        const val DIRT_SUBJECT_CAP = 5_000
    }
}
