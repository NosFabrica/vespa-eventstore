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
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.ReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationDoc
import com.vitorpamplona.quartz.eventstore.vespa.forEachBounded
import com.vitorpamplona.quartz.eventstore.vespa.mapBounded
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * HOW a reputation parent doc is (re)derived — the engine [TrustProjection]'s
 * write triggers and [TrustReconciler]'s repairs both drive. It owns the
 * service->observer attribution map ([ProviderMap]) and the three shapes of
 * recompute: one subject, a batch of subjects, and a streaming walk over a
 * whole score corpus.
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
 */
internal class TrustRecompute(
    private val inner: EventIndex,
    private val reputations: ReputationIndex,
) {
    /** service key -> observer (NIP-85 attribution), cached across a pass; see [ProviderMap]. */
    private val providers = ProviderMap(inner)

    /** The current attribution map, rebuilding it once per pass (see [ProviderMap.get]). */
    suspend fun providerMap(): Map<String, String> = providers.get()

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

    /**
     * The batched recompute behind [TrustProjection.putAll], [recomputeSubjectsOf]
     * and the walks. The touched subjects' score docs are fetched back in
     * CHUNKED, concurrency-BOUNDED queries: hundreds of subjects per round trip,
     * a few round trips in flight (unbounded fan-out measurably times the engine
     * out). Every parent is derived locally, and the results are written through
     * one pipelined [ReputationIndex.putAll].
     */
    suspend fun recomputeBatch(
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
    suspend fun recomputeSubjectsOf(listDocs: List<EventDoc>) {
        if (listDocs.isEmpty()) return
        providers.invalidate() // the map just changed; next providerMap() rebuilds
        val services = ProviderMap.rankServicesOf(listDocs)
        if (services.isEmpty()) return
        recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = services))
    }

    /**
     * Visit every score doc matching [query] and re-derive the subjects in
     * bounded batches, STREAMING. The subject buffer is flushed and cleared every
     * [RECOMPUTE_BATCH] distinct subjects rather than collecting the whole corpus
     * first. Otherwise a full rebuild — or a large provider's 10040 change —
     * would hold millions of subject strings in memory (an OOM on the exact
     * "scale-safe" path). A subject whose cards span a batch boundary is
     * re-derived (idempotent), which is cheaper than an unbounded dedup set.
     */
    suspend fun recomputeWalk(
        query: EventQuery,
        onSubjects: ((Int) -> Unit)? = null,
    ) {
        val map = providers.get()
        val buffer = LinkedHashSet<String>()
        var derived = 0

        suspend fun flush() {
            if (buffer.isNotEmpty()) {
                recomputeBatch(buffer.toList(), map, removeEmpties = true)
                derived += buffer.size
                buffer.clear()
                // After the batch, not after the page: a subject is only actually
                // re-derived once its batch is written, and reporting on the page
                // would run ahead of the work.
                onSubjects?.invoke(derived)
            }
        }
        inner.visitIds(query, withDTag = true) { page ->
            page.forEach { ref -> ref.dTag?.let(buffer::add) }
            if (buffer.size >= RECOMPUTE_BATCH) flush()
            true // walk the whole corpus
        }
        flush()
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
    }
}

/** The 30382's d tag is the SUBJECT the score is about. */
internal fun subjectOf(doc: EventDoc): String? =
    doc.tags
        .firstOrNull { it.size >= 2 && it[0] == "d" }
        ?.get(1)
        ?.takeIf { it.isNotEmpty() }
