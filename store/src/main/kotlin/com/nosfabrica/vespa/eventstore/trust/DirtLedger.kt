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

import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * The trust projection's WORK LEDGER — crash safety and optional deferral for
 * every trust-mutating op. The unit is [Dirt], DECLARATIVE ("re-derive these
 * subjects, re-walk these services"): re-derivation is a pure function of the
 * store's CURRENT state under the writer lock, so dirt coalesces across ops
 * and every drain schedule converges to the same tensors.
 *
 * CRASH SAFETY: the event and projection writes are separate acks, and dedup
 * fires a write trigger exactly once, so a failure between them used to be
 * permanent drift (the retry comes back all-duplicates). [guarded] persists
 * what the op could invalidate BEFORE touching anything; the marker clears
 * only once the projection has caught up. A surviving marker IS the drift, named.
 *
 * DEFERRAL: with a drain signal attached ([deferTo]), the expensive reactions
 * leave [guarded] as pending work for a background [drain] — writes return
 * fast, storms coalesce into one re-derivation, and ranking lags by the drain
 * cycle ([drain] is also the explicit barrier). Without a signal, work settles
 * inline before returning — the read-your-writes behavior the unit tests assert.
 *
 * The marker is one [ReputationDoc] under [MARKER_KEY] — non-hex, so no card
 * can collide with it ([subjectOf] admits only 64-hex). Subjects ride the
 * influence cells, services the follower cells; small dirt persists as
 * pipelined cell ADDs, a bulk batch's as one marker-doc put ([DELTA_ADD_MAX]).
 *
 * Every entry point except [drain]'s gated parts runs under the store's single
 * writer lock — what makes the plain fields here safe; [drain] takes the same
 * lock per bounded batch through its gate.
 */
internal class DirtLedger(
    private val reputations: ReputationIndex,
    private val recompute: TrustRecompute,
) {
    /** Declarative projection work: subjects to re-derive exactly, services to re-walk. */
    internal data class Dirt(
        val subjects: Set<String>,
        val services: Set<String>,
    ) {
        fun isEmpty() = subjects.isEmpty() && services.isEmpty()

        operator fun plus(other: Dirt) = if (other.isEmpty()) this else Dirt(subjects + other.subjects, services + other.services)

        operator fun minus(other: Dirt) = Dirt(subjects - other.subjects, services - other.services)

        companion object {
            val NONE = Dirt(emptySet(), emptySet())
        }
    }

    /** Unhealed work: null until the persisted marker (a previous process's leftovers) has been read once. */
    private var pending: Dirt? = null

    /**
     * True while dirt inherited from a PREVIOUS process is unhealed: it may
     * have died between writing a 10040 and invalidating the provider-map
     * cache, so the heal must drop the cache even when the dirt names no
     * services. In-process dirt invalidates inline.
     */
    private var inherited = false

    /** The drain signal; null = settle work inline (the read-your-writes mode). */
    private var signal: (() -> Unit)? = null

    /**
     * Switch to DEFERRED mode: [guarded] leaves work pending and fires [onWork];
     * the owner runs [drain] on the signal — and once at startup too, since a
     * previous process's marker is only discovered by draining.
     */
    fun deferTo(onWork: () -> Unit) {
        signal = onWork
    }

    /**
     * Run [block] (the event write plus any cheap inline projection) bracketed
     * by the ledger. [insurance] is everything the op COULD leave stale if it
     * dies partway (persisted write-ahead); [block] returns the WORK actually
     * left, which the insurance must COVER — named directly, or reachable
     * through an insured service's walk. On failure the whole insurance becomes
     * pending: the retry is all-duplicates and can never repair it, so the next
     * settle or [drain] must. The marker may transiently OVER-cover until a
     * drain's final rewrite — deliberate: narrowing here would cost a doc-sized
     * write per batch, while over-coverage only costs a crash some redundant
     * (idempotent) re-derives.
     */
    suspend fun <T> guarded(
        insurance: Dirt,
        block: suspend () -> Pair<T, Dirt>,
    ): T {
        val before = load()
        val delta = insurance - before
        if (!delta.isEmpty()) {
            // Write-ahead: a small delta is pipelined cell ADDs (no read, no doc
            // rewrite; already-pending dirt persists nothing); a bulk batch's is
            // ONE marker-doc put — per-entry ops at batch size would rival the
            // event writes they insure.
            if (delta.subjects.size + delta.services.size <= DELTA_ADD_MAX) persistDelta(delta) else persist(before + insurance)
        }
        val work: Dirt
        val result: T
        try {
            val r = block()
            result = r.first
            work = r.second
        } catch (t: Throwable) {
            pending = before + insurance
            // Wake the drainer even though the op failed: its retry loop repairs
            // a transient failure's dirt without waiting for the next write.
            signal?.invoke()
            throw t
        }
        val queued = before + work
        pending = queued
        val deferred = signal
        if (deferred == null) {
            drain { it() } // settle inline: the caller holds the writer lock
        } else if (!queued.isEmpty()) {
            deferred()
        }
        return result
    }

    /**
     * Heal everything pending, in gated batches: snapshot, re-derive its
     * subjects (empties removed — also deletes a parent whose last card died
     * with a crashed removal), re-walk its services, then subtract the snapshot
     * and shrink the marker. Loops until a snapshot is empty, so work deferred
     * WHILE draining is picked up. Idempotent; throws with the marker intact if
     * a repair step fails. [gate] wraps each mutating batch — identity when the
     * caller already holds the writer lock; the background drainer and the
     * reconciler pass the store's lock so a long walk shares it with ingest.
     */
    suspend fun drain(gate: suspend (suspend () -> Unit) -> Unit) {
        while (true) {
            var snapshot = Dirt.NONE
            gate { snapshot = load() }
            if (snapshot.isEmpty()) return
            if (snapshot.services.isNotEmpty() || inherited) recompute.invalidateProviders()
            snapshot.subjects.chunked(DRAIN_BATCH).forEach { chunk ->
                // The gate is taken PER SLICE inside, not once for the whole
                // 20,000-subject chunk: that hold was measured at 13 minutes on
                // staging with ingest queueing behind it. See
                // [TrustRecompute.recomputeBatchGated].
                recompute.recomputeBatchGated(chunk, recompute.providerMap(), removeEmpties = true, gate = gate)
            }
            if (snapshot.services.isNotEmpty()) {
                recompute.recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = snapshot.services.toList()), gate = gate)
            }
            gate {
                val rest = load() - snapshot // work deferred mid-drain survives
                persist(rest)
                pending = rest
                inherited = false
            }
        }
    }

    /** The pending dirt, reading the persisted marker once per process. */
    private suspend fun load(): Dirt {
        pending?.let { return it }
        val stored = reputations.get(MARKER_KEY)?.let { Dirt(it.influenceScores.keys, it.followerCounts.keys) } ?: Dirt.NONE
        inherited = !stored.isEmpty()
        pending = stored
        return stored
    }

    /** Write-ahead append: one pipelined tensor-cell add per NEW dirt entry — no read, no doc rewrite. */
    private suspend fun persistDelta(delta: Dirt) {
        if (delta.isEmpty()) return
        val cells = ArrayList<ReputationCells>(delta.subjects.size + delta.services.size)
        delta.subjects.forEach { cells += ReputationCells(MARKER_KEY, it, 1, null) }
        delta.services.forEach { cells += ReputationCells(MARKER_KEY, it, null, 1.0) }
        reputations.updateCells(cells)
    }

    /** Rewrite the marker to exactly [dirt] (or remove it when clean). */
    private suspend fun persist(dirt: Dirt) {
        if (dirt.isEmpty()) reputations.remove(MARKER_KEY) else reputations.put(marker(dirt))
    }

    companion object {
        /**
         * The marker's document id — deliberately NOT 64-hex, so no event can
         * name it ([subjectOf] filters to 64-hex): it never collides with a
         * real subject's parent doc and never joins ranking.
         */
        const val MARKER_KEY = "projection-dirty"

        /** Subjects re-derived per gated drain batch (memory- and lock-hold-bounded). */
        private const val DRAIN_BATCH = 20_000

        /**
         * Largest write-ahead delta persisted as per-cell feed ADDs; bigger
         * takes one marker-doc put. Adds win for live traffic's small dirt; a
         * doc put wins for bulk, where per-entry ops rival the event writes.
         */
        private const val DELTA_ADD_MAX = 64

        /** The persisted form: subjects ride the influence cells, services the follower cells (values are ignored). */
        private fun marker(dirt: Dirt): ReputationDoc = ReputationDoc(MARKER_KEY, dirt.subjects.associateWith { 1 }, dirt.services.associateWith { 1.0 })
    }
}
