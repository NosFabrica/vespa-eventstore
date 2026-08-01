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

import com.vitorpamplona.quartz.eventstore.vespa.client.ReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationCells
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * The trust projection's WORK LEDGER — crash safety and (optionally) deferral
 * for every trust-mutating op.
 *
 * The unit of work is [Dirt], and it is DECLARATIVE: "re-derive these
 * subjects", "re-walk these services". Re-derivation is a pure function of the
 * store's CURRENT state executed under the writer lock, so dirt can be
 * accumulated, coalesced across ops, and drained at any later time — every
 * schedule converges to the same tensors. That one property is what makes both
 * jobs of this class sound:
 *
 *  - CRASH SAFETY. The event write and the projection write are separate acks,
 *    and dedup fires a write trigger exactly once, so a failure between them
 *    used to be permanent drift: the events are stored, and the retry comes
 *    back all-duplicates, which never reaches the projection. [guarded]
 *    persists what the op could invalidate (its INSURANCE) before touching
 *    anything, and the marker only clears once the projection has caught up.
 *    A marker that survives IS the drift, named.
 *
 *  - DEFERRAL. With a drain signal attached ([deferTo]), the expensive
 *    reactions — a 10040's service walk (minutes under the writer lock), a
 *    retraction's re-derive, a sweep's re-derives — leave [guarded] as PENDING
 *    work for a background [drain] instead of running inline. Inserts return
 *    after the event write plus one small pipelined marker update; a storm of
 *    updates about the same subjects coalesces into one re-derivation; and a
 *    superseded 10040's two walks collapse into one over the union. Ranking
 *    then lags writes by the drain cycle (bounded, and [drain] is also the
 *    explicit barrier) instead of being read-your-writes. Without a signal,
 *    [guarded] settles the work inline before returning — the read-your-writes
 *    behavior every projection unit test asserts.
 *
 * The marker is one [ReputationDoc] under [MARKER_KEY] — a non-hex id no card
 * can collide with, since [subjectOf] admits only 64-hex subjects. Dirty
 * subjects ride the influence cells, dirty services the follower cells, and
 * the write-ahead persist is a pipelined tensor-cell ADD (no read, no doc
 * rewrite), so an op whose dirt is already pending persists NOTHING.
 *
 * Every entry point except the gated parts of [drain] runs under the store's
 * single writer lock, which is what makes the plain fields here safe; [drain]
 * takes the same lock per bounded batch through its gate.
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

    /**
     * Unhealed work: null until the persisted marker has been read once (a
     * previous PROCESS may have crashed or shut down with work queued).
     */
    private var pending: Dirt? = null

    /**
     * True while dirt inherited from a PREVIOUS process is unhealed. That
     * process may have died between writing a 10040 and invalidating the
     * provider-map cache, so the heal must drop the cache even when the dirt
     * names no services. In-process dirt never needs this — the 10040 write
     * paths invalidate inline.
     */
    private var inherited = false

    /** The drain signal; null = settle work inline (the read-your-writes mode). */
    private var signal: (() -> Unit)? = null

    /**
     * Switch to DEFERRED mode: [guarded] leaves its work pending and fires
     * [onWork] instead of healing inline; the owner runs [drain] on the signal.
     * Fire it once at startup too — a marker left by the previous process is
     * only discovered by draining.
     */
    fun deferTo(onWork: () -> Unit) {
        signal = onWork
    }

    /**
     * Run [block] — the event write plus any cheap inline projection — bracketed
     * by the ledger. [insurance] is everything the op COULD leave stale if it
     * dies partway (persisted write-ahead); [block] returns the WORK actually
     * left to do, which must be a subset of the insurance: nothing in sync mode
     * beyond what it chose not to run inline, the expensive reactions in
     * deferred mode. On success the marker narrows from insurance to the
     * remaining pending work; on failure the whole insurance becomes pending —
     * the failed op's retry is all-duplicates and can never repair it, so the
     * next settle or [drain] must.
     */
    suspend fun <T> guarded(
        insurance: Dirt,
        block: suspend () -> Pair<T, Dirt>,
    ): T {
        val before = load()
        // Write-ahead, as pipelined cell ADDs: only the delta beyond what is
        // already pending costs anything — a storm about the same subjects
        // persists once.
        persistDelta(insurance - before)
        val work: Dirt
        val result: T
        try {
            val r = block()
            result = r.first
            work = r.second
        } catch (t: Throwable) {
            pending = before + insurance
            // In deferred mode, wake the drainer even though the op failed: its
            // retry loop is how a transient engine failure's dirt gets repaired
            // without waiting for the next successful write.
            signal?.invoke()
            throw t
        }
        pending = before + work
        val deferred = signal
        if (deferred == null) {
            drain { it() } // settle inline: the caller holds the writer lock
        } else {
            // Narrow the marker from insurance to the real pending work — skipped
            // in the common case where they are identical (singles, removals).
            if (work != insurance) persist(before + work)
            if (!(before + work).isEmpty()) deferred()
        }
        return result
    }

    /**
     * Heal everything pending, in gated batches: snapshot the pending dirt,
     * re-derive its subjects ([TrustRecompute.recomputeBatch], empties removed —
     * which also deletes a parent whose last card died with a crashed removal),
     * re-walk its services, then subtract the snapshot and shrink the marker.
     * Loops until a snapshot comes back empty, so work deferred WHILE draining
     * is picked up before returning. Idempotent; throws with the marker intact
     * (and the work still pending) if a repair step fails.
     *
     * [gate] wraps each mutating batch. Callers already holding the writer lock
     * pass identity; the background drainer and the reconciler pass the store's
     * lock so a minutes-long walk shares it with ingest instead of stalling it.
     */
    suspend fun drain(gate: suspend (suspend () -> Unit) -> Unit) {
        while (true) {
            var snapshot = Dirt.NONE
            gate { snapshot = load() }
            if (snapshot.isEmpty()) return
            if (snapshot.services.isNotEmpty() || inherited) recompute.invalidateProviders()
            snapshot.subjects.chunked(DRAIN_BATCH).forEach { chunk ->
                gate { recompute.recomputeBatch(chunk, recompute.providerMap(), removeEmpties = true) }
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
         * The marker's document id. Deliberately NOT 64-hex: [subjectOf] filters
         * subjects to 64-hex, so no event can name this id and the marker can
         * never collide with (or be clobbered by) a real subject's parent doc.
         * It also never joins ranking — the reputation import matches event
         * author pubkeys, which are hex.
         */
        const val MARKER_KEY = "projection-dirty"

        /** Subjects re-derived per gated drain batch (memory- and lock-hold-bounded). */
        private const val DRAIN_BATCH = 20_000

        /** The persisted form: subjects ride the influence cells, services the follower cells (values are ignored). */
        private fun marker(dirt: Dirt): ReputationDoc = ReputationDoc(MARKER_KEY, dirt.subjects.associateWith { 1 }, dirt.services.associateWith { 1.0 })
    }
}
