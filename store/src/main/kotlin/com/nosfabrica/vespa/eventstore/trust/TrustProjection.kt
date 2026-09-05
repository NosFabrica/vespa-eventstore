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
package com.nosfabrica.vespa.eventstore.trust

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * Maintains the `reputation` parent documents (per-pubkey trust tensors the
 * schema imports into ranking, keyed by the SERVICE that signed each score)
 * as an [EventIndex] DECORATOR: every deletion style — supersession, kind-5,
 * vanish, sweeps — funnels into [put]/[remove] here, so the tensors stay
 * current with ZERO deletion-specific code.
 *
 * A card is ONE cell update on its subject, applied inline on every path
 * ([TrustRecompute.applyCards]); a removed card is one cell remove. The only
 * deferred reaction is a 10040 naming a service nobody named before, whose
 * stored cards become cells through a walk ([TrustRecompute.projectServices])
 * — declared as [DirtLedger.Dirt] and settled inline (read-your-writes; what
 * the unit tests assert) or by the background drain ([DirtLedger.deferTo]).
 * Every trust-mutating op runs [DirtLedger.guarded]: the event and projection
 * writes are separate acks and dedup fires a trigger only once, so a crash
 * between them would be PERMANENT drift (the retry comes back all-duplicates)
 * — the ledger persists what the op could leave stale before it starts, and
 * the exact derive repairs it at the next settle, drain, or reconcile. Drift
 * no write trigger can see is [TrustReconciler]'s job.
 *
 * The observer never appears in a cell: their kind 10040 is resolved to a
 * service key per query (NostrSemanticsStore.lensed), so a list re-signed or
 * re-pointed writes nothing here. See docs/service-keyed-trust.md.
 */
class TrustProjection(
    private val inner: EventIndex,
    private val reputations: ReputationIndex,
    nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
) : EventIndex {
    /** The stored `max_rank` per subject, so a cell can raise it in the same write — see [MaxRankCache]. */
    internal val maxRanks = MaxRankCache(reputations)

    /** The recompute engine the ledger's drains drive; [TrustReconciler] shares it. */
    internal val recompute = TrustRecompute(inner, reputations, nowSecs, maxRanks)

    /** The work ledger: crash marker + (optionally deferred) projection queue; [TrustReconciler] drains it at startup. */
    internal val dirt = DirtLedger(reputations, recompute)

    override suspend fun get(id: String): EventDoc? = inner.get(id)

    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query)

    override suspend fun existingIds(ids: List<String>): Set<String> = inner.existingIds(ids)

    // MUST delegate, not ride the interface default, which would route through
    // this decorator's search() and lose the raw passthrough (see EventIndex.rawSearch).
    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = inner.rawSearch(query)

    // Same rule, and this is the production stack's only decorator: riding the
    // default would route through search()/rawSearch() above and hand the store
    // null scores for every hit, so a multi-filter REQ would silently fall back
    // to recency and the relevance merge would never run against Vespa.
    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = inner.searchRanked(query)

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = inner.rawSearchRanked(query)

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
    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> = inner.countByAuthor(query)

    override suspend fun scanAuthors(query: EventQuery): Set<String> = inner.scanAuthors(query)

    override fun close() {
        inner.close()
        reputations.close()
    }

    // The engine's atomic conditional put IS enough now: a card's cell is a
    // function of the newest version at its address alone (the same key is
    // overwritten), so the projection no longer needs to see the version it
    // replaced. Forwarding lets the bulk path skip its version-read stage.
    override val supersedesViaPut: Boolean get() = inner.supersedesViaPut

    /**
     * Supersession of a trust doc is ONE reaction: the winner's cells. The
     * versions it replaces need no hook — the cell is keyed by the address
     * they share, so the winner's `add` overwrites it and its `remove` of a
     * dropped tag retracts it — so the read-then-supersede default, which
     * would route each replaced version through [removeDocs] and pay a
     * ledger bracket per version, is replayed here on the INNER index with
     * the projection reacting once. Non-trust kinds go straight through.
     */
    override suspend fun putIfNewer(doc: EventDoc): Boolean {
        if (doc.kind !in TRUST_KINDS) return inner.putIfNewer(doc)
        // A 10040's fresh services must be judged against the map BEFORE the
        // write lands, else a first-ever map read would already hold the new
        // list and the walk that projects its service would never run.
        val fresh = if (doc.kind == TrustProviderListEvent.KIND) freshServicesOf(listOf(doc)) else emptySet()
        var stored = false
        dirt.guarded(opDirt(doc) + DirtLedger.Dirt(emptySet(), fresh)) {
            stored = if (inner.supersedesViaPut) inner.putIfNewer(doc) else supersedeReading(doc)
            if (!stored) return@guarded Unit to DirtLedger.Dirt.NONE
            Unit to react(listOf(doc), fresh)
        }
        return stored
    }

    /** [EventIndex.putIfNewer]'s read-then-supersede, against [inner] alone (see [putIfNewer]). */
    private suspend fun supersedeReading(doc: EventDoc): Boolean {
        val address =
            doc.addressOrNull() ?: run {
                inner.put(doc)
                return true
            }
        val dTag = doc.dTagOrEmpty()
        val q =
            if (doc.kind.isAddressable() && dTag.isNotEmpty()) {
                EventQuery(kinds = listOf(doc.kind), authors = listOf(doc.pubkey), tags = mapOf("d" to listOf(dTag)))
            } else {
                EventQuery(kinds = listOf(doc.kind), authors = listOf(doc.pubkey))
            }
        val existing = inner.search(q).filter { it.addressOrNull() == address }
        val incumbent = existing.minWithOrNull(EventDoc.NEWEST_FIRST)
        if (incumbent != null && EventDoc.NEWEST_FIRST.compare(doc, incumbent) >= 0) return false
        if (existing.isNotEmpty()) inner.removeDocs(existing)
        inner.put(doc)
        return true
    }

    override suspend fun put(doc: EventDoc) {
        val fresh = if (doc.kind == TrustProviderListEvent.KIND) freshServicesOf(listOf(doc)) else emptySet()
        dirt.guarded(opDirt(doc) + DirtLedger.Dirt(emptySet(), fresh)) {
            inner.put(doc)
            Unit to react(listOf(doc), fresh)
        }
    }

    /**
     * The bulk path: cards land as tensor-cell UPDATES with ZERO reads, one
     * update per card (its service's cell on its subject). Supersession
     * guarantees each card here is the newest for its (service, subject)
     * address, and the cell is keyed by exactly that service, so the update
     * IS the derivation — within a batch the fold order settles a same-address
     * conflict the way a derive would. A card that lost a tag drops that
     * dimension's cell in the same pass (a tensor `remove`), so the one thing
     * left for the read-based derive is a card that fails to reconstruct.
     */
    override suspend fun putAll(docs: List<EventDoc>) {
        val lists = docs.filter { it.kind == TrustProviderListEvent.KIND }
        val fresh = if (lists.isEmpty()) emptySet() else freshServicesOf(lists)
        dirt.guarded(putDirt(docs) + DirtLedger.Dirt(emptySet(), fresh)) {
            IngestStats.timed("write") { inner.putAll(docs) }
            Unit to react(docs, fresh)
        }
    }

    /**
     * What a batch of stored trust docs changes, applied: the provider map
     * dropped after any 10040, the cards' cells written, and the WORK left —
     * [fresh] services to walk (their cards were stored while nobody named
     * them, so their cells do not exist yet) and the subjects whose card
     * could not be applied as a cell.
     */
    private suspend fun react(
        docs: List<EventDoc>,
        fresh: Set<String>,
    ): DirtLedger.Dirt {
        if (docs.any { it.kind == TrustProviderListEvent.KIND }) recompute.invalidateProviders()
        val cards = docs.filter { it.kind == ContactCardEvent.KIND }
        if (cards.isEmpty()) return DirtLedger.Dirt(emptySet(), fresh)
        val retracted = recompute.applyCards(cards, recompute.providerMap())
        return DirtLedger.Dirt(retracted, fresh)
    }

    /** The services [lists] name that the CURRENT map does not — the ones whose stored cards have no cells yet. Read before the lists are written. */
    private suspend fun freshServicesOf(lists: List<EventDoc>): Set<String> {
        val named = ProviderMap.trustServicesOf(lists)
        if (named.isEmpty()) return emptySet()
        val before = recompute.providerMap()
        return named.filterNot(before::maps).toSet()
    }

    override suspend fun remove(id: String) {
        // The doomed doc says what the removal invalidates — read before deleting.
        val doc = inner.get(id)
        if (doc != null) removeDocs(listOf(doc)) else inner.remove(id)
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
                // `complete`, like every read that feeds a write: a short answer
                // here is a removed card whose subject is never dirtied, so its
                // cells linger after a kind-5 — refusing beats silent drift.
                .mapBounded(QUERY_FANOUT) { chunk -> inner.search(EventQuery(ids = chunk, kinds = TRUST_KINDS, complete = true)) }
                .flatten()
        if (docs.isEmpty()) return inner.removeAll(ids)
        val byId = docs.associateBy { it.id }
        val plain = ids.filter { it !in byId }
        dirt.guarded(removeDirt(docs)) {
            if (plain.isNotEmpty()) inner.removeAll(plain)
            inner.removeDocs(docs)
            Unit to unreact(docs)
        }
    }

    /**
     * Bulk remove for callers that already HOLD the doomed docs: ZERO reads —
     * the docs themselves say what each removal invalidates.
     */
    override suspend fun removeDocs(docs: List<EventDoc>) {
        if (docs.none { it.kind in TRUST_KINDS }) return inner.removeDocs(docs)
        dirt.guarded(removeDirt(docs)) {
            // Timed for symmetry with putAll's `write`: this is a supersession's
            // other half — the sweep of the versions the winner replaced — on
            // the same bulk path inside the same writer lock, so leaving it
            // untimed reads as "the write is all there is".
            IngestStats.timed("remove") { inner.removeDocs(docs) }
            Unit to unreact(docs)
        }
    }

    /**
     * A removed card's cells go with it — a tensor `remove` of its service's
     * cell on its subject, no read, no derive. Exact for ranking: the store
     * holds one version per (service, subject) address, so the removed card
     * was the one backing the cell. What it leaves behind is bookkeeping, not
     * drift: a parent with no cells left, and a `max_rank` that may now read
     * high — an upper bound still, so the descent stays sound and only pays
     * a rung; both are tightened by a derive (the crash marker, a verify with
     * repair). Removals are rare beside supersession, which never comes
     * through here on the address-keyed engine and reacts once on the
     * read-then-supersede path ([putIfNewer]). A removed 10040 drops the
     * provider map; the cells its services own stay — no lens resolves to an
     * unnamed service, so they are dead weight for the orphan sweep.
     */
    private suspend fun unreact(docs: List<EventDoc>): DirtLedger.Dirt {
        if (docs.any { it.kind == TrustProviderListEvent.KIND }) recompute.invalidateProviders()
        val removals =
            docs
                .filter { it.kind == ContactCardEvent.KIND }
                .mapNotNull { doc -> subjectOf(doc)?.let { CellRemoval(it, doc.pubkey, influence = true, followers = true) } }
        if (removals.isNotEmpty()) IngestStats.timed("proj.write") { reputations.removeCells(removals) }
        return DirtLedger.Dirt.NONE
    }

    /** Crash insurance for ONE doc's write: a card its subject (re-derived exactly if the cell write is lost), a 10040 nothing — its walk is declared as work, not insured. */
    private fun opDirt(doc: EventDoc): DirtLedger.Dirt =
        when (doc.kind) {
            ContactCardEvent.KIND -> DirtLedger.Dirt(setOfNotNull(subjectOf(doc)), emptySet())
            else -> DirtLedger.Dirt.NONE
        }

    /**
     * Crash insurance for a PUT of [docs]. Card subjects are recorded exactly
     * while they fit [DIRT_SUBJECT_CAP]; a bigger batch records the cards'
     * SERVICES instead — few keys however large the batch, repaired by
     * re-walking each service (safe for puts: the stored cards make the walk
     * reach every touched subject).
     */
    private fun putDirt(docs: List<EventDoc>): DirtLedger.Dirt {
        val subjects = LinkedHashSet<String>()
        for (doc in docs) if (doc.kind == ContactCardEvent.KIND) subjectOf(doc)?.let(subjects::add)
        val services = LinkedHashSet<String>()
        if (subjects.size > DIRT_SUBJECT_CAP) {
            docs.forEach { if (it.kind == ContactCardEvent.KIND) services += it.pubkey }
            subjects.clear()
        }
        return DirtLedger.Dirt(subjects, services)
    }

    /** What a REMOVE of [docs] insures: card subjects exactly — a lost cell remove is repaired by the exact derive, which also drops an emptied parent. */
    private fun removeDirt(docs: List<EventDoc>): DirtLedger.Dirt {
        val subjects = LinkedHashSet<String>()
        for (doc in docs) if (doc.kind == ContactCardEvent.KIND) subjectOf(doc)?.let(subjects::add)
        return DirtLedger.Dirt(subjects, emptySet())
    }

    internal companion object {
        /** The only kinds whose write or removal can touch the projection — what the store's trust gate is taken for. */
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
