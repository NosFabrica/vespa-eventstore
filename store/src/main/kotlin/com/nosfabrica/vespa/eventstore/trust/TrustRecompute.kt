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
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.forEachBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex

/**
 * HOW the reputation parents are written — the cell path every card takes
 * ([applyCards]), the walk that brings a newly named service's stored cards
 * in ([projectServices]), and the exact derive [TrustReconciler]'s repairs and
 * the crash marker fall back on. Owns the named-service map ([ProviderMap]).
 *
 * The REPAIR path, not the hot path: a derive rebuilds the SUBJECT's whole
 * [ReputationDoc] from the stored 30382s about them, and the write path only
 * asks for one when it cannot apply a change as a cell (a crash marker, a
 * reconcile, a removal by id it cannot resolve). Cells are keyed by the
 * SIGNING SERVICE: the newest card by each mapped service about the subject
 * holds that service's cell, both tags. A version missing a tag contributes
 * nothing to that dimension (retraction); already-expired cards (NIP-40)
 * contribute nothing — the derive queries carry the same expiry cutoff every
 * read path applies. Idempotent and self-healing; when no cells are left the
 * parent doc is removed.
 */
internal class TrustRecompute(
    private val inner: EventIndex,
    private val reputations: ReputationIndex,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
    /** Told what every whole-document write stored, so the cell path never reads a stale-high value — see [MaxRankCache]. */
    private val maxRanks: MaxRankCache? = null,
) {
    /** The named services and every observer's lens, cached across a pass; see [ProviderMap]. */
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
     * [recomputeBatch] under a gate, taken PER SLICE rather than once for the
     * whole batch.
     *
     * The batch sizes upstream ([DirtLedger.DRAIN_BATCH], [RECOMPUTE_BATCH],
     * [TrustReconciler.ORPHAN_BATCH], all 20,000) were chosen to bound MEMORY,
     * and every caller wrapped the whole of one in `gate { }` — so the store's
     * single write mutex was held for as long as 20,000 subjects took to
     * derive. Measured on staging 2026-09-04, one such call:
     *
     *     lockHeldBy  lock.gate.hold  656s and counting
     *                 "derive 20000 subject(s) in 400 chunk(s), fanout 4"
     *     proj.fetch.derive  ms=769980  calls=1
     *     lock.ingest.hold   ms=560          (ingest's own work: 0.56s)
     *     lock.ingest.wait   ms=2276280      (ingest queueing: 38 minutes)
     *
     * The derivation is per subject and each slice writes only what it
     * derived, so slicing changes no result — it only decides how long anyone
     * else waits. Ingest, the monitor's verdicts and the sweeps all queue on
     * this mutex, so the hold is the whole store's fairness knob.
     *
     * The reads are NOT hoisted out of the gate instead, deliberately: a
     * derive that read before a live insert and wrote after it would clobber
     * that insert's own recompute, with the dirt marker already cleared —
     * permanent drift, which is exactly what the gate exists to prevent.
     * Slicing keeps every subject's derive and write atomic against writers.
     */
    suspend fun recomputeBatchGated(
        subjects: List<String>,
        removeEmpties: Boolean,
        gate: suspend (suspend () -> Unit) -> Unit,
    ) {
        subjects.chunked(GATE_SLICE).forEach { slice ->
            // The provider map is read INSIDE the gate, per slice — cached, so
            // free when unchanged. Read once outside as an argument (as this
            // was), a 10040 committed mid-batch left every remaining slice
            // deriving under the pre-write map.
            gate { recomputeBatch(slice, providers.get(), removeEmpties) }
        }
    }

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
            maxRanks?.remember(derived.values)
            if (removeEmpties) {
                // Pipelined like the puts above: this used to be one feed round
                // trip per subject at QUERY_FANOUT, under the gate — an orphan
                // sweep that emptied 17k parents held it for ~17k/4 of them.
                val gone = subjects.filter { it !in derived }
                if (gone.isNotEmpty()) reputations.removeAll(gone)
                maxRanks?.forget(gone)
            }
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
        // SPLIT from the old shared `proj.fetch` (2026-09-04): this and
        // TrustProjection's max_rank raise both booked to that one name, so a
        // gate held for 24 minutes could not be attributed to either. The
        // annotation names the shape of THIS call — the chunk count is the
        // loop, the subject count is the work.
        IngestStats.annotateHold("derive ${subjects.size} subject(s) in ${(subjects.size + FETCH_CHUNK - 1) / FETCH_CHUNK} chunk(s), fanout $QUERY_FANOUT")
        IngestStats.timed("proj.fetch.derive") {
            subjects.chunked(FETCH_CHUNK).forEachBounded(
                QUERY_FANOUT,
                // A partial score set derives a WRONG parent card, so this query
                // carries no limit — and `complete`, so an engine that would
                // answer short (still opening buckets after a restart, or
                // capping hits) refuses instead: the batch aborts with its
                // dirt marker intact, and nothing is written or removed from
                // a fetch that missed cards. That is the write-side counterpart
                // of the read path's rounded-100 carve-out, and the failure
                // that removed 17k parents on staging (2026-09-04).
                produce = { chunk -> chunk to inner.search(EventQuery(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to chunk), notExpiredAt = cutoff, complete = true)) },
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
                recomputeBatchGated(buffer.toList(), removeEmpties = true, gate = gate)
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

    /**
     * THE HOT PATH: [cards] applied as tensor-cell writes, ONE update per card
     * and no read of any other card. A card's cell is keyed by its signing
     * service, and the store holds one version per (service, subject)
     * address, so the newest version IS the cell: `add` overwrites the key, a
     * tag the version lost is a `remove` in the same atomic update. Folded
     * oldest-first so that a same-address pair inside one batch lands the
     * newest, as a derive would. Only services some 10040 names are written
     * (the reputation type is global and memory-resident); a card by anyone
     * else is dead storage until a list names them, when the drain's service
     * walk ([projectServices]) applies it through this same path.
     *
     * Returns the subjects whose card could NOT be applied — a card that
     * fails to reconstruct — which the caller declares as derive work.
     */
    suspend fun applyCards(
        cards: List<EventDoc>,
        serviceProviders: TrustProviders,
    ): Set<String> {
        val updates = ArrayList<ReputationCells>(cards.size)
        val unapplied = LinkedHashSet<String>()
        for (doc in cards.sortedWith(DERIVE_ORDER)) {
            val subject = subjectOf(doc) ?: continue
            if (!serviceProviders.maps(doc.pubkey)) continue
            val card = doc.toEvent() as? ContactCardEvent
            if (card == null) {
                unapplied += subject
                continue
            }
            val influence = card.boundedRank()
            val followers = card.followerCount()?.toDouble()
            updates += ReputationCells(subject, doc.pubkey, influence, followers, dropInfluence = influence == null, dropFollowers = followers == null)
        }
        if (updates.isEmpty()) return unapplied
        // Each cell that overtakes its document's `max_rank` carries the new
        // value in the same update — the invariant the trust descent proves
        // pages with (TrustDescent). Its cost is a function of cache misses.
        IngestStats.annotateHold("cell update over ${updates.size} card(s)")
        val raised = IngestStats.timed("proj.fetch.maxrank") { maxRanks?.raise(updates) ?: updates }
        try {
            IngestStats.timed("proj.write") { reputations.updateCells(raised) }
        } catch (t: Throwable) {
            // The cache moved to the raised values BEFORE this write; a write
            // that failed leaves it reading high, and a stale-high entry skips
            // a raise the store needs. Forget them: the next cell reads again.
            maxRanks?.forget(raised.mapNotNull { u -> u.subject.takeIf { u.maxRank != null } })
            throw t
        }
        return unapplied
    }

    /**
     * A NEWLY NAMED service's stored cards become cells: its ids stream off
     * the search index ([EventIndex.visitIds] — cursor-paged, so a service
     * that bulk-published thousands of cards on one second costs one window
     * query, and never a document-API scan of the whole corpus, which is
     * O(corpus) per service), and each page's documents are fetched BY ID and
     * applied through [applyCards] inside one [gate] hold. By id, inside the
     * gate: a card superseded between the id listing and the fetch is simply
     * gone (its winner's own write applied the cell), so no page can land an
     * older cell after a newer one. The one O(cards) operation left in the
     * projection, and it runs once per service, the first time any 10040
     * names it — never on a re-signed or re-pointed list.
     */
    suspend fun projectServices(
        services: Collection<String>,
        onCards: ((Int) -> Unit)? = null,
        gate: suspend (suspend () -> Unit) -> Unit = { it() },
    ) {
        for (service in services) {
            var applied = 0
            inner.visitIds(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service)), withDTag = false) { page ->
                page.map { it.id }.chunked(PROJECT_PAGE).forEach { ids ->
                    gate {
                        IngestStats.annotateHold("project service ${service.take(8)}: ${ids.size} card(s)")
                        val docs = IngestStats.timed("proj.fetch.page") { inner.search(EventQuery(ids = ids, kinds = listOf(ContactCardEvent.KIND), complete = true)) }
                        applyCards(docs, providers.get())
                        applied += docs.size
                    }
                }
                onCards?.invoke(applied)
                true
            }
        }
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
            // Keyed by the SIGNING SERVICE. Only services some stored 10040
            // names are projected (memory: the reputation type is global), and
            // both tags land whatever dimension the service was named for — a
            // query reads the dimension it resolved the observer's lens to.
            if (!serviceProviders.maps(card.pubKey)) continue
            card.boundedRank()?.let { rank -> influence[card.pubKey] = rank }
            card.followerCount()?.toDouble()?.let { count -> followers[card.pubKey] = count }
        }
        return ReputationDoc(subject, influence, followers)
    }

    internal companion object {
        // Subjects per batched score-fetch, sized for DENSE subjects (~50
        // services each observed, so 100 subjects recall ~5k docs). Chunking
        // bounds each response and keeps the derivation correct under a lowered
        // engine hit cap, which truncates silently.
        const val FETCH_CHUNK = 50

        // Subjects per recompute round in a full walk (memory-bounded batches).
        const val RECOMPUTE_BATCH = 20_000

        /**
         * Cards per gate hold in a service walk — one by-id fetch, applied as
         * pipelined cell updates. The walk's total time barely moves with the
         * page (the updates pipeline either way); what the page decides is how
         * long a live trust write waits: measured at 1000 on a loaded single
         * node, a card insert during a 234k-card walk waited 762 ms median,
         * 1.1 s max — one page hold. `VESPA_TRUST_WALK_PAGE` overrides.
         */
        val PROJECT_PAGE: Int = System.getenv("VESPA_TRUST_WALK_PAGE")?.toIntOrNull()?.coerceAtLeast(1) ?: 250

        /**
         * Subjects per WRITE-GATE hold, which is a different question from
         * every batch size above it: those bound memory, this bounds how long
         * every other writer in the store waits. One [FETCH_CHUNK] of 50 took
         * ~1.9s on staging, so 500 is ~19s of hold against the ~13 minutes a
         * whole 20,000-subject batch took. Overridable per deployment.
         *
         * Lower is fairer and costs only the mutex round trip (microseconds
         * against seconds of work); higher approaches the old behaviour.
         */
        val GATE_SLICE: Int = System.getenv("VESPA_TRUST_GATE_SLICE")?.toIntOrNull()?.coerceAtLeast(1) ?: 500

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
