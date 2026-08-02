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

import com.vitorpamplona.quartz.eventstore.store.mapping.toEvent
import com.vitorpamplona.quartz.eventstore.vespa.IngestStats
import com.vitorpamplona.quartz.eventstore.vespa.QUERY_FANOUT
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.ReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationDoc
import com.vitorpamplona.quartz.eventstore.vespa.forEachBounded
import com.vitorpamplona.quartz.eventstore.vespa.mapBounded
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex

/**
 * HOW a reputation parent doc is (re)derived — the engine [TrustProjection]'s
 * write triggers and [TrustReconciler]'s repairs both drive. It owns the
 * service->observers attribution map ([ProviderMap]) and the three shapes of
 * recompute: one subject, a batch of subjects, and a streaming walk over a
 * whole score corpus.
 *
 * Recompute, never cell surgery: a change re-derives the SUBJECT's whole
 * [ReputationDoc] from the stored kind-30382s about them —
 *
 *   subject's 30382s (d = subject) -> signer is a SERVICE key
 *   -> observers, PER DIMENSION ([TrustProviders]): the `rank` tag credits the
 *      kind-10040 authors whose `30382:rank` entry lists that service key, the
 *      `followers` tag those whose `30382:followers` entry does — a user may
 *      pick different services for the two (NIP-85: cells are keyed by the
 *      OBSERVER, never the signer; a popular provider is named by many
 *      observers and scores every one)
 *   -> influence_scores{observer} = rank tag, follower_counts{observer} =
 *      followers tag; a version missing a tag contributes nothing to that
 *      dimension (the provider retracted the score).
 *
 * Already-expired cards (NIP-40) contribute nothing: the store never serves
 * them as records, so they must not keep scoring — the derive queries carry
 * the same expiry cutoff every read path applies.
 *
 * Idempotent and self-healing; when no cells are left the parent doc is removed.
 */
internal class TrustRecompute(
    private val inner: EventIndex,
    private val reputations: ReputationIndex,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    /** Per-dimension `service key -> observers` (NIP-85 attribution), cached across a pass; see [ProviderMap]. */
    private val providers = ProviderMap(inner, nowSecs)

    /** The current attribution maps, rebuilding them once per pass (see [ProviderMap.get]). */
    suspend fun providerMap(): TrustProviders = providers.get()

    /**
     * Drop the cached attribution map. Call after ANY 10040 write or removal —
     * the write paths do it inline (even when the service walk itself is
     * deferred), and [DirtLedger.drain] does it again for dirt inherited from a
     * crashed process, whose dying op may never have gotten this far.
     */
    fun invalidateProviders() = providers.invalidate()

    /**
     * The batched recompute behind every [DirtLedger] drain and the walks. The
     * touched subjects' score docs are fetched back in CHUNKED,
     * concurrency-BOUNDED queries: hundreds of subjects per round trip, a few
     * round trips in flight (unbounded fan-out measurably times the engine
     * out). Every parent is derived locally, and the results are written through
     * one pipelined [ReputationIndex.putAll].
     */
    suspend fun recomputeBatch(
        subjects: List<String>,
        serviceProviders: TrustProviders,
        removeEmpties: Boolean,
    ) {
        val derived = deriveBatch(subjects, serviceProviders)
        IngestStats.timed("proj.write") {
            reputations.putAll(derived.values.toList())
            if (removeEmpties) subjects.filter { it !in derived }.mapBounded(QUERY_FANOUT) { reputations.remove(it) }
        }
    }

    /**
     * The read side of [recomputeBatch], alone: what each subject's parent doc
     * SHOULD be, derived from the stored records — no writes. Subjects whose
     * derivation is empty are simply absent from the result. This is also what
     * [TrustReconciler]'s verify audits against.
     */
    suspend fun deriveBatch(
        subjects: List<String>,
        serviceProviders: TrustProviders,
    ): Map<String, ReputationDoc> {
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
        // carry a cell per MAPPED observer only (a handful, where the recall spans
        // every service that ever scored the subject), so they are small, and one
        // pipelined write per batch beats one per chunk.
        val derived = LinkedHashMap<String, ReputationDoc>(subjects.size * 2)
        val cutoff = nowSecs()
        IngestStats.timed("proj.fetch") {
            subjects.chunked(FETCH_CHUNK).forEachBounded(
                QUERY_FANOUT,
                // A partial score set derives a WRONG parent card, so this query
                // carries no limit.
                produce = { chunk -> chunk to inner.search(EventQuery(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to chunk), notExpiredAt = cutoff)) },
            ) { (chunk, docs) ->
                // Serialized by forEachBounded, so this plain map needs no lock.
                val bySubject = HashMap<String, MutableList<EventDoc>>(chunk.size * 2)
                val wanted = chunk.toHashSet()
                docs.forEach { doc ->
                    subjectOf(doc)?.takeIf { it in wanted }?.let { bySubject.getOrPut(it) { mutableListOf() } += doc }
                }
                for (subject in chunk) {
                    val reputation = derive(subject, bySubject[subject].orEmpty(), serviceProviders)
                    if (!reputation.isEmpty()) derived[subject] = reputation
                }
            }
        }
        return derived
    }

    /**
     * Visit every score doc matching [query] and re-derive the subjects in
     * bounded batches, STREAMING. The subject buffer is flushed and cleared every
     * [RECOMPUTE_BATCH] distinct subjects rather than collecting the whole corpus
     * first. Otherwise a full rebuild — or a large provider's 10040 change —
     * would hold millions of subject strings in memory (an OOM on the exact
     * "scale-safe" path). A subject whose cards span a batch boundary is
     * re-derived (idempotent), which is cheaper than an unbounded dedup set.
     *
     * The enumeration deliberately carries NO expiry cutoff: an expired card's
     * subject must still be re-derived so its stale cells DROP (the derive
     * fetch applies the cutoff — see [recomputeBatch]).
     *
     * [gate] wraps each mutating flush — identity for the write path, which
     * already holds the store's writer lock; [TrustReconciler] passes the real
     * lock so a minutes-long walk mutates in short locked bursts instead of
     * racing live inserts. The provider map is re-read INSIDE the gate on every
     * flush (cached, so it costs nothing when unchanged): a 10040 committed
     * mid-walk by a concurrent writer must not be overwritten by derivations
     * from a walk-start snapshot of the map.
     */
    suspend fun recomputeWalk(
        query: EventQuery,
        onSubjects: ((Int) -> Unit)? = null,
        gate: suspend (suspend () -> Unit) -> Unit = { it() },
    ) {
        val buffer = LinkedHashSet<String>()
        var derived = 0

        suspend fun flush() {
            if (buffer.isNotEmpty()) {
                gate { recomputeBatch(buffer.toList(), providers.get(), removeEmpties = true) }
                derived += buffer.size
                buffer.clear()
                // After the batch, not after the page: a subject is only actually
                // re-derived once its batch is written, and reporting on the page
                // would run ahead of the work.
                onSubjects?.invoke(derived)
            }
        }
        inner.visitIds(query, withDTag = true) { page ->
            page.forEach { ref -> ref.dTag?.takeIf { Hex.isHex64(it) }?.let(buffer::add) }
            if (buffer.size >= RECOMPUTE_BATCH) flush()
            true // walk the whole corpus
        }
        flush()
    }

    /** [subject]'s parent doc from its score docs — pure derivation, no I/O. */
    private fun derive(
        subject: String,
        docs: List<EventDoc>,
        serviceProviders: TrustProviders,
    ): ReputationDoc {
        val influence = LinkedHashMap<String, Int>()
        val followers = LinkedHashMap<String, Double>()
        // Folded OLDEST-first so the NEWEST card wins each (observer, dimension)
        // cell — deterministic, where the previous engine-order fold made a
        // rebuild able to change served scores with no event changing. Ties go
        // to the LOWEST id (sorted after it, so it overwrites), matching the
        // store's replaceable-winner rule.
        for (doc in docs.sortedWith(DERIVE_ORDER)) {
            // Direct by-kind reconstruction — no toEventJson()/fromJson round
            // trip; this runs once per fetched card across every recompute walk.
            val card = doc.toEvent() as? ContactCardEvent ?: continue
            // Each dimension attributes on its own map: the card's rank tag
            // counts only for observers who named its signer under `30382:rank`,
            // its followers tag only for those who named it under
            // `30382:followers` — a rank provider's followers tag must not
            // shadow the value the user's chosen follower provider asserts.
            // EVERY observer naming the service gets the cell — a shared
            // provider scores each of the users who trust it, not one winner.
            serviceProviders.rank[card.pubKey]?.let { observers ->
                card.rank()?.let { rank -> observers.forEach { influence[it] = rank } }
            }
            serviceProviders.followers[card.pubKey]?.let { observers ->
                card.followerCount()?.toDouble()?.let { count -> observers.forEach { followers[it] = count } }
            }
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

        /** Oldest first, ties iterated highest-id first — so the last (winning) write per cell is (newest, lowest id). */
        val DERIVE_ORDER: Comparator<EventDoc> = compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id }
    }
}

/**
 * The 30382's d tag is the SUBJECT the score is about — a pubkey, so only a
 * 64-hex value counts. Anything else can never join an event author in ranking
 * (the reputation import matches the author's hex pubkey exactly), and
 * admitting arbitrary strings would let a crafted card collide with the
 * projection's own bookkeeping ids ([DirtLedger]).
 */
internal fun subjectOf(doc: EventDoc): String? =
    doc.tags
        .firstOrNull { it.size >= 2 && it[0] == "d" }
        ?.get(1)
        ?.takeIf { Hex.isHex64(it) }
