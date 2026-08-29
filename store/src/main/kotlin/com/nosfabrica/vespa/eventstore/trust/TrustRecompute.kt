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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.forEachBounded
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex

/**
 * HOW a reputation parent doc is (re)derived — driven by [TrustProjection]'s
 * write triggers and [TrustReconciler]'s repairs. Owns the service->observers
 * attribution map ([ProviderMap]) and three recompute shapes: one subject, a
 * batch, and a streaming walk over a whole score corpus.
 *
 * Recompute, never cell surgery: a change re-derives the SUBJECT's whole
 * [ReputationDoc] from the stored 30382s about them. The signer is a SERVICE
 * key; PER DIMENSION ([TrustProviders]) the `rank` tag credits the observers
 * naming that service under `30382:rank`, the `followers` tag those under
 * `30382:followers` — cells are keyed by the OBSERVER, never the signer, and a
 * popular provider scores every observer naming it. A version missing a tag
 * contributes nothing to that dimension (retraction); already-expired cards
 * (NIP-40) contribute nothing — the derive queries carry the same expiry
 * cutoff every read path applies. Idempotent and self-healing; when no cells
 * are left the parent doc is removed.
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
     * The read-time gate off the same cached pass — who a reader has asked to
     * compute what. See [Delegations]; the write side's projection is above.
     */
    suspend fun delegations(): Delegations = providers.delegations()

    /**
     * Drop the cached attribution map after ANY 10040 write or removal — the
     * write paths do it inline (even with the walk deferred); [DirtLedger.drain]
     * repeats it for dirt inherited from a crashed process that may have died first.
     */
    fun invalidateProviders() = providers.invalidate()

    /**
     * The batched recompute behind every [DirtLedger] drain and the walks:
     * chunked, concurrency-bounded fetches (unbounded fan-out measurably times
     * the engine out), local derivation, one pipelined [ReputationIndex.putAll].
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
     * The read side of [recomputeBatch]: what each subject's parent doc SHOULD
     * be — no writes; empty derivations are absent from the result. Also what
     * [TrustReconciler]'s verify audits against.
     */
    suspend fun deriveBatch(
        subjects: List<String>,
        serviceProviders: TrustProviders,
    ): Map<String, ReputationDoc> {
        // Derived per CHUNK, not per batch: each chunk's query is complete, so
        // its docs are freed as soon as it lands. Holding a whole batch's recall
        // meant ~1.5M docs live (hundreds of MB) on the ingest path; per chunk it
        // is QUERY_FANOUT × FETCH_CHUNK × recall, independent of batch size. The
        // derived docs do accumulate — they are small, and one pipelined write
        // per batch beats one per chunk.
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
     * bounded batches, STREAMING: the buffer flushes every [RECOMPUTE_BATCH]
     * distinct subjects, else a full rebuild would hold millions of subject
     * strings (an OOM on the exact "scale-safe" path). A subject spanning a
     * batch boundary is re-derived twice — idempotent, cheaper than an
     * unbounded dedup set. The enumeration carries NO expiry cutoff: an expired
     * card's subject must still re-derive so its stale cells DROP (the derive
     * fetch applies the cutoff).
     *
     * [gate] wraps each mutating flush — identity on the write path (already
     * under the writer lock); [TrustReconciler] passes the real lock so a long
     * walk mutates in short bursts instead of racing live inserts. The provider
     * map is re-read INSIDE the gate each flush (cached, free when unchanged):
     * a 10040 committed mid-walk must not be overwritten by derivations from a
     * walk-start snapshot of the map.
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
                // Reported after the batch is written, not per page — the page
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
        // Folded OLDEST-first so the NEWEST card wins each cell — deterministic
        // (an engine-order fold let rebuilds change served scores with no event
        // changing). Ties go to the LOWEST id (sorted after, so it overwrites),
        // matching the store's replaceable-winner rule.
        for (doc in docs.sortedWith(DERIVE_ORDER)) {
            // Direct by-kind reconstruction — no JSON round trip; runs once per
            // fetched card across every recompute walk.
            val card = doc.toEvent() as? ContactCardEvent ?: continue
            // Per-dimension attribution: a rank provider's followers tag must
            // not shadow the chosen follower provider's value. EVERY observer
            // naming the service gets the cell — not one winner.
            serviceProviders.rank[card.pubKey]?.let { observers ->
                card.boundedRank()?.let { rank -> observers.forEach { influence[it] = rank } }
            }
            serviceProviders.followers[card.pubKey]?.let { observers ->
                card.followerCount()?.toDouble()?.let { count -> observers.forEach { followers[it] = count } }
            }
        }
        return ReputationDoc(subject, influence, followers)
    }

    private companion object {
        // Subjects per batched score-fetch, sized for DENSE subjects (~50
        // services each observed, so 100 subjects recall ~5k docs). Chunking
        // bounds each response and keeps the derivation correct under a lowered
        // engine hit cap, which truncates silently.
        const val FETCH_CHUNK = 50

        // Subjects per recompute round in a full walk (memory-bounded batches).
        const val RECOMPUTE_BATCH = 20_000

        /**
         * The serving order REVERSED — oldest first, ties iterated highest-id
         * first — so the last (winning) write per cell is exactly
         * [EventDoc.NEWEST_FIRST]'s first element: (newest, lowest id).
         */
        val DERIVE_ORDER: Comparator<EventDoc> = EventDoc.NEWEST_FIRST.reversed()
    }
}

/**
 * The 30382's d tag is the SUBJECT — a pubkey, so only 64-hex counts. Anything
 * else can never join ranking (the reputation import matches hex author keys),
 * and admitting arbitrary strings would let a crafted card collide with the
 * projection's bookkeeping ids ([DirtLedger]).
 */
internal fun subjectOf(doc: EventDoc): String? = doc.dTagOrEmpty().takeIf(Hex::isHex64)

/**
 * The card's rank tag clamped to the served 0..100 scale — providers are not
 * trusted to stay on-scale, and both bounds are load-bearing: a NEGATIVE score
 * sits below `include:spam`'s min_rank=0 floor and would silently drop the
 * author from the one query shape that promises not to drop anyone; an
 * OVER-SCALE score would distort wot_mult()'s calibrated 0..100 tier-crossing
 * thresholds. Clamped at EVERY read of the tag — the fast path, the bulk path,
 * and the derive must land the same cell value or [TrustReconciler] reads drift.
 */
internal fun ContactCardEvent.boundedRank(): Int? = rank()?.coerceIn(0, 100)
