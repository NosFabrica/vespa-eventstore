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

import com.vitorpamplona.quartz.eventstore.vespa.IngestStats
import com.vitorpamplona.quartz.eventstore.vespa.QUERY_FANOUT
import com.vitorpamplona.quartz.eventstore.vespa.client.DocRef
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.ReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationCells
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationDoc
import com.vitorpamplona.quartz.eventstore.vespa.forEachBounded
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
 * Recompute, never cell surgery: a change re-derives the SUBJECT's whole
 * [ReputationDoc] from the stored kind-30382s about them —
 *
 *   subject's 30382s (d = subject) -> signer is a SERVICE key
 *   -> observer = the kind-10040 author whose `30382:rank` entry lists that
 *      service key (NIP-85: cells are keyed by the OBSERVER, never the signer)
 *   -> influence_scores{observer} = rank tag, follower_counts{observer} =
 *      followers tag; a version without a rank tag contributes nothing
 *      (the provider retracted the score).
 *
 * Idempotent and self-healing; when no cells are left the parent doc is removed.
 * A 10040 change (new provider, switched provider, vanished observer) recomputes
 * every subject its service keys had scored. So late-arriving or superseded
 * provider lists re-attribute stored scores automatically.
 *
 * Recomputes run inline with the store's single-writer insert, so ranking is
 * read-your-writes consistent with the event corpus. [rebuildAll] re-derives
 * everything (bootstrap after enabling the projection on an existing index).
 */
class TrustProjection(
    private val inner: EventIndex,
    private val reputations: ReputationIndex,
) : EventIndex {
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
        recomputeSubjectsOf(docs.filter { it.kind == TrustProviderListEvent.KIND })
        val cards = docs.filter { it.kind == ContactCardEvent.KIND }
        if (cards.isEmpty()) return
        val serviceToObserver = providers.get()
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
        if (retracted.isNotEmpty()) recomputeBatch(retracted.toList(), serviceToObserver, removeEmpties = true)
    }

    /**
     * The batched recompute behind [putAll], [recomputeSubjectsOf] and
     * [rebuildAll]. The touched subjects' score docs are fetched back in
     * CHUNKED, concurrency-BOUNDED queries: hundreds of subjects per round trip,
     * a few round trips in flight (unbounded fan-out measurably times the engine
     * out). Every parent is derived locally, and the results are written through
     * one pipelined [ReputationIndex.putAll].
     */
    private suspend fun recomputeBatch(
        subjects: List<String>,
        serviceToObserver: Map<String, String>,
        removeEmpties: Boolean,
    ) {
        // Derived per CHUNK, not per batch. A chunk's query returns every score
        // for its 50 subjects — complete, since the query carries no limit — so a
        // subject can be derived the moment its chunk lands, and those docs are
        // then free.
        //
        // Collecting first is what the batch size looks like it bounds and does
        // not: subjects are cheap (20k × 64 chars), while the docs they recall are
        // ~75x more numerous and orders of magnitude larger. Holding a whole
        // batch's recall meant ~1.5M docs live at once — hundreds of MB, on the
        // ingest path. Per chunk it is `QUERY_FANOUT × FETCH_CHUNK × recall`,
        // about a hundredth of that, and independent of the batch size.
        //
        // The derived docs DO accumulate across the batch, deliberately: they
        // carry a cell per MAPPED service only (a handful, where the recall spans
        // every service that ever scored the subject), so they are small, and one
        // pipelined write per batch beats one per chunk.
        val puts = ArrayList<ReputationDoc>(subjects.size)
        val removes = ArrayList<String>()
        IngestStats.timed("proj.fetch") {
            subjects.chunked(FETCH_CHUNK).forEachBounded(
                QUERY_FANOUT,
                // A partial score set derives a WRONG parent card, so this query
                // carries no limit.
                produce = { chunk -> chunk to inner.search(EventQuery(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to chunk))) },
            ) { (chunk, docs) ->
                // Serialized by forEachBounded, so these plain lists need no lock.
                val bySubject = HashMap<String, MutableList<EventDoc>>(chunk.size * 2)
                val wanted = chunk.toHashSet()
                docs.forEach { doc ->
                    subjectOf(doc)?.takeIf { it in wanted }?.let { bySubject.getOrPut(it) { mutableListOf() } += doc }
                }
                for (subject in chunk) {
                    val reputation = derive(subject, bySubject[subject].orEmpty(), serviceToObserver)
                    if (!reputation.isEmpty()) {
                        puts += reputation
                    } else if (removeEmpties) {
                        removes += subject
                    }
                }
            }
        }
        IngestStats.timed("proj.write") {
            reputations.putAll(puts)
            removes.mapBounded(QUERY_FANOUT) { reputations.remove(it) }
        }
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
        recomputeSubjectsOf(docs.filter { it.kind == TrustProviderListEvent.KIND })
        val subjects = docs.filter { it.kind == ContactCardEvent.KIND }.mapNotNull { subjectOf(it) }.distinct()
        if (subjects.isNotEmpty()) recomputeBatch(subjects, providers.get(), removeEmpties = true)
    }

    private suspend fun react(doc: EventDoc) {
        when (doc.kind) {
            ContactCardEvent.KIND -> subjectOf(doc)?.let { recompute(it) }
            TrustProviderListEvent.KIND -> recomputeSubjectsOf(listOf(doc))
        }
    }

    /** Re-derive [subject]'s whole parent doc from the stored 30382s about them. */
    suspend fun recompute(subject: String) = recompute(subject, providers.get())

    private suspend fun recompute(
        subject: String,
        serviceToObserver: Map<String, String>,
    ) {
        val docs = inner.search(EventQuery(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to listOf(subject))))
        val reputation = derive(subject, docs, serviceToObserver)
        if (reputation.isEmpty()) reputations.remove(subject) else reputations.put(reputation)
    }

    /** [subject]'s parent doc from its score docs — pure derivation, no I/O. */
    private fun derive(
        subject: String,
        docs: List<EventDoc>,
        serviceToObserver: Map<String, String>,
    ): ReputationDoc {
        val influence = LinkedHashMap<String, Int>()
        val followers = LinkedHashMap<String, Double>()
        for (doc in docs) {
            val card = Event.fromJsonOrNull(doc.toEventJson()) as? ContactCardEvent ?: continue
            val observer = serviceToObserver[card.pubKey] ?: continue
            card.rank()?.let { influence[observer] = it }
            card.followerCount()?.let { followers[observer] = it.toDouble() }
        }
        return ReputationDoc(subject, influence, followers)
    }

    /** service key -> observer (NIP-85 attribution), cached across a pass; see [ProviderMap]. */
    private val providers = ProviderMap(inner)

    /**
     * One or more 10040s appeared or disappeared. The provider map changed, so
     * every subject their rank services have scored needs re-attribution. The
     * subjects are enumerated through the engine's VISIT walk (d tags projected),
     * not a search: a provider with millions of stored scores is exactly where
     * pulling one giant response would blow up memory (and where any deployment
     * that DID set a hit cap would silently miss most of them). The subjects are then
     * re-derived in batches, with empties removed (a re-attribution can empty a
     * parent). A BATCH of 10040s does ONE walk over the union of their services,
     * not one walk per list.
     */
    private suspend fun recomputeSubjectsOf(listDocs: List<EventDoc>) {
        if (listDocs.isEmpty()) return
        providers.invalidate() // the map just changed; next providers.get() rebuilds
        val services = ProviderMap.rankServicesOf(listDocs)
        if (services.isEmpty()) return
        recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = services))
    }

    /** Re-derive every parent doc from scratch (bootstrap over an existing index). */
    suspend fun rebuildAll() = recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND)))

    /** What [reconcile] found: services examined, and the ones it had to re-derive. */
    data class Reconciliation(
        val services: Int,
        val rebuilt: List<String>,
    ) {
        fun isClean() = rebuilt.isEmpty()
    }

    /**
     * Re-derive the services whose scores are not projected under the observer
     * currently mapped to them.
     *
     * ## Why this is needed at all
     *
     * Derivation happens on WRITE: [putAll] projects the cards in the batch, and
     * a 10040 arriving re-walks the services it names. Both are triggers, and a
     * trigger only fires once. Dedup rejects an event the store already holds
     * BEFORE the projection sees it, so once a corpus is stored neither trigger
     * can fire again — a card skipped because its service had no 10040 yet stays
     * unprojected for as long as both events remain in the store.
     *
     * A mirror hits this as a matter of course: scores outnumber provider lists
     * by four orders of magnitude and arrive first, and the run that finally
     * writes the 10040 may be one in which every score is already a duplicate.
     * The result is a projection that is empty, correct-looking and unable to
     * repair itself — every ranked search returns nothing, with no error anywhere.
     *
     * ## What it checks
     *
     * A service is projected when its subjects carry a cell for ITS observer. So
     * for each mapped service this samples a few of its cards and asks whether
     * any of their subjects has that observer's cell. Checking the observer
     * rather than mere existence is what also catches a RE-MAPPED service: its
     * subjects have docs, but the cells belong to the previous observer.
     *
     * Sampling, not counting, because the alternative is reading every card. A
     * service whose sample happens to land on retracted subjects is re-derived
     * needlessly, which costs one walk and is idempotent. The opposite error —
     * calling an unprojected service clean — would need a sampled subject to be
     * projected while the rest are not, which is not the shape this failure takes.
     */
    suspend fun reconcile(samplesPerService: Int = DEFAULT_RECONCILE_SAMPLES): Reconciliation {
        val serviceToObserver = providers.get()
        if (serviceToObserver.isEmpty()) return Reconciliation(0, emptyList())

        val rebuilt = mutableListOf<String>()
        var examined = 0
        for ((service, observer) in serviceToObserver) {
            val sample =
                inner.search(
                    EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service), limit = samplesPerService),
                )
            // A service that has published nothing we hold has nothing to project.
            if (sample.isEmpty()) continue
            examined++
            val projected =
                sample.any { card ->
                    val subject = subjectOf(card) ?: return@any false
                    reputations.get(subject)?.influenceScores?.containsKey(observer) == true
                }
            if (!projected) {
                recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service)))
                rebuilt += service
            }
        }
        return Reconciliation(examined, rebuilt)
    }

    /**
     * Visit every score doc matching [query] and re-derive the subjects in
     * bounded batches, STREAMING. The subject buffer is flushed and cleared every
     * [RECOMPUTE_BATCH] distinct subjects rather than collecting the whole corpus
     * first. Otherwise a `rebuildAll()` or a large provider's 10040 change would
     * hold millions of subject strings in memory (an OOM on the exact
     * "scale-safe" path). A subject whose cards span a batch boundary is
     * re-derived (idempotent), which is cheaper than an unbounded dedup set.
     */
    private suspend fun recomputeWalk(query: EventQuery) {
        val map = providers.get()
        val buffer = LinkedHashSet<String>()

        suspend fun flush() {
            if (buffer.isNotEmpty()) {
                recomputeBatch(buffer.toList(), map, removeEmpties = true)
                buffer.clear()
            }
        }
        inner.visitIds(query, withDTag = true) { page ->
            page.forEach { ref -> ref.dTag?.let(buffer::add) }
            if (buffer.size >= RECOMPUTE_BATCH) flush()
            true // walk the whole corpus
        }
        flush()
    }

    /** The 30382's d tag is the SUBJECT the score is about. */
    private fun subjectOf(doc: EventDoc): String? =
        doc.tags
            .firstOrNull { it.size >= 2 && it[0] == "d" }
            ?.get(1)
            ?.takeIf { it.isNotEmpty() }

    private companion object {
        // Subjects per batched score-fetch query. Sized for DENSE subjects: a
        // real NIP-85 corpus scores each subject from dozens of service keys
        // (~50 observed), so 100 subjects already recall ~5k docs. Chunking
        // keeps each response bounded, and keeps the derivation correct on a
        // deployment that lowered the engine's hit cap (which truncates
        // silently, with no error to notice).
        const val FETCH_CHUNK = 50

        // Subjects per recompute round in a full walk (memory-bounded batches).
        const val RECOMPUTE_BATCH = 20_000

        // Cards sampled per service by [reconcile]. The question it answers is
        // "did this service's scores get projected at all", and the failure is
        // all-or-nothing per service, so a handful settles it; the cost is one
        // small query plus that many key lookups per mapped service.
        const val DEFAULT_RECONCILE_SAMPLES = 3
    }
}
