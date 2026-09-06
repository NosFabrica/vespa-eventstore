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
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

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
 * THE IN-MEMORY LEDGER IS LOCK-FREE, since 2026-09-04. It used to be plain
 * fields, safe because every entry point ran under the store's ONE writer
 * lock. The trust-gate split (NostrSemanticsStore.trustGate) ended that: a
 * kind-1 insert now runs [guarded] under the event lock alone, while the
 * background [drain] mutates the same ledger under the trust gate alone, and
 * two read-modify-writes of a plain field under different locks is a lost
 * update — a kind-1 that read `pending` before a drain cleared it wrote the
 * cleared dirt back, so the next card for that subject computed an empty
 * write-ahead delta against an over-covering memory, and a crash before the
 * following drain would have been permanent drift with no marker naming it.
 * So [pending] is an [AtomicReference] moved only by CAS (an op ADDS its
 * work; a finished round REMOVES exactly what it derived), which is correct
 * under any interleaving and costs no lock. The persisted marker is still
 * only ever written under the trust gate ([guarded]'s write-ahead by a trust
 * write, [drain]'s rewrite through its gate), which keeps disk and memory
 * agreeing.
 *
 * EVERY ENTRY IS STAMPED with the sequence number of the add that last named
 * it, and a round removes only entries whose stamp is still the one it
 * snapshotted. That is what makes a re-add during a round survive: a card
 * for subject A written AFTER A's slice was derived, but before the round
 * finished, re-stamps A, so the round's removal leaves it for the next one.
 * (A first version removed the snapshot as a set, `pending - snapshot`, and
 * lost exactly that card — A's newer rank was served only after a
 * reconcile.)
 *
 * ENTRIES ARE RETIRED ONE SLICE AT A TIME, as each slice's projection write
 * is acked ([retire]) — not once at the end of the round. Retiring per round
 * made a large ledger look FROZEN for as long as the round took: staging
 * inherited 139,524 subjects, a slice of 500 measured ~14s of derive, so the
 * backlog gauge sat at its starting value and the marker at full size for
 * over an hour per pass, and a crash anywhere in that hour re-derived every
 * subject including the ones written in its first minute. Per-slice
 * retirement is the same work, credited when it happens.
 *
 * A round still leaves its UNRETIRED snapshot in [pending] while it runs, on
 * purpose: a trust write's write-ahead is computed against `pending`, so the
 * marker keeps covering the in-flight remainder however the write-ahead is
 * persisted; and a round that fails leaves nothing to restore. A second
 * drain — the read-your-writes barrier, a verify — still returns only once
 * the work is visible, because it SKIPS what another drain already retired
 * (retired means written and acked) and re-derives the rest (idempotent, and
 * each slice serialised on the gate), so it can never wait on another round
 * while holding the gate. Skipping is what keeps two concurrent drains of
 * one ledger — the background loop's and [TrustReconciler]'s — from each
 * deriving the whole snapshot, which on staging split one process's derive
 * budget between two passes doing identical work.
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
     * The ledger's entries with the stamp of the add that last named each.
     * Immutable; every change is a CAS on [pending].
     */
    private class Stamped(
        val subjects: Map<String, Long>,
        val services: Map<String, Long>,
    ) {
        fun isEmpty() = subjects.isEmpty() && services.isEmpty()

        fun toDirt() = Dirt(subjects.keys, services.keys)

        /** [dirt]'s entries (re)stamped [stamp]; the same instance when there is nothing to add. */
        fun plus(
            dirt: Dirt,
            stamp: Long,
        ): Stamped = if (dirt.isEmpty()) this else Stamped(subjects + dirt.subjects.associateWith { stamp }, services + dirt.services.associateWith { stamp })

        /** Without the entries of [done] whose stamp is unchanged — a re-stamped entry was re-added since and stays. */
        fun minusUnchanged(done: Stamped): Stamped = Stamped(subjects.filterNot { (s, n) -> done.subjects[s] == n }, services.filterNot { (s, n) -> done.services[s] == n })

        companion object {
            val NONE = Stamped(emptyMap(), emptyMap())
        }
    }

    /**
     * Unhealed work: null until the persisted marker (a previous process's
     * leftovers) has been read once. Moved only by CAS — see the class KDoc for
     * why a plain field under two locks is not enough.
     */
    private val pending = AtomicReference<Stamped?>(null)

    /**
     * Subjects queued for re-derivation, right now — the backlog GAUGE behind
     * an operator page. Instantaneous: never diffed between snapshots the way a
     * counter is, because a queue depth has no cumulative form.
     *
     * Reads [pending] directly, which is safe precisely because the ledger went
     * lock-free: an `AtomicReference` is publication enough for a snapshot
     * thread that holds none of this ledger's locks. Back when this was a plain
     * `var` the gauge had to be mirrored into a volatile field by the owner;
     * that mirror is gone, and this is the better shape.
     */
    fun pendingSubjects(): Long = (pending.get()?.subjects?.size ?: 0).toLong()

    /** Services queued for a re-walk, right now — a gauge, like [pendingSubjects]. */
    fun pendingServices(): Long = (pending.get()?.services?.size ?: 0).toLong()

    /** The stamp source; every add takes the next one. */
    private val stamps = AtomicLong()

    /**
     * True while dirt inherited from a PREVIOUS process is unhealed: it may
     * have died between writing a 10040 and invalidating the provider-map
     * cache, so the heal must drop the cache even when the dirt names no
     * services. In-process dirt invalidates inline. Consumed by the drain
     * round that takes the inherited snapshot.
     */
    private val inherited = AtomicBoolean(false)

    /** The drain signal; null = settle work inline (the read-your-writes mode). */
    @Volatile
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
            add(insurance)
            // Wake the drainer even though the op failed: its retry loop repairs
            // a transient failure's dirt without waiting for the next write.
            signal?.invoke()
            throw t
        }
        // ADDED, never assigned: `before + work` written back would resurrect
        // whatever a concurrent drain removed between the read above and here.
        val queued = add(work)
        // Insurance persisted, no work left, nothing pending: the marker names
        // subjects nobody will ever drain — a batch of cards by a signer no
        // 10040 maps (a mirror's by-kind card ingest is mostly this) insured
        // every subject and then found no cells to write. No drain rewrites
        // the marker for work that does not exist, so it stood, and the next
        // boot inherited hundreds of subjects to re-derive to nothing and
        // dropped the provider cache for it. One remove here keeps the marker
        // honest: a surviving marker IS drift, named.
        if (!delta.isEmpty() && queued.isEmpty()) persist(Dirt.NONE)
        val deferred = signal
        if (deferred == null) {
            drain { it() } // settle inline: the caller holds the writer lock
        } else if (!queued.isEmpty()) {
            deferred()
        }
        return result
    }

    /** Union [dirt] into the ledger, freshly stamped, and return the result; a no-op CAS when [dirt] is empty. */
    private fun add(dirt: Dirt): Dirt {
        if (dirt.isEmpty()) return pending.get()?.toDirt() ?: Dirt.NONE
        val stamp = stamps.incrementAndGet()
        return pending.updateAndGet { (it ?: Stamped.NONE).plus(dirt, stamp) }!!.toDirt()
    }

    /**
     * Heal everything pending, in gated batches: snapshot, re-derive its
     * subjects (empties removed — also deletes a parent whose last card died
     * with a crashed removal), re-walk its services, then remove the snapshot
     * (only the entries nothing re-added meanwhile — see the class KDoc) and
     * shrink the marker. Loops until a snapshot is empty, so work deferred
     * WHILE draining is picked up. Idempotent; throws with the marker intact if
     * a repair step fails. [gate] wraps each mutating batch — identity when the
     * caller already holds the writer lock; the background drainer and the
     * reconciler pass the store's lock so a long walk shares it with ingest.
     */
    suspend fun drain(gate: suspend (suspend () -> Unit) -> Unit) {
        while (true) {
            load() // the marker a previous process left is only discovered by reading it
            // READ, not taken: the unretired remainder stays pending while it
            // is derived, so every write-ahead computed meanwhile still covers
            // it and a concurrent drain still sees it as work — class KDoc.
            val snapshot = pending.get() ?: Stamped.NONE
            if (snapshot.isEmpty()) return
            val dirt = snapshot.toDirt()
            if (dirt.services.isNotEmpty() || inherited.get()) recompute.invalidateProviders()
            // Sliced HERE at the gate's own slice size, rather than handing
            // [TrustRecompute.recomputeBatchGated] one 20,000-subject chunk to
            // slice internally, because the ledger has to see each slice land:
            // a slice is the unit that gets skipped (already retired elsewhere)
            // and the unit that gets retired (derived and acked).
            dirt.subjects.chunked(TrustRecompute.GATE_SLICE).forEach { slice ->
                // Another drain of this ledger may have retired these since the
                // snapshot — retired means written and acked, so re-deriving
                // them is pure duplicate work, and skipping keeps the barrier
                // honest either way. See the class KDoc.
                val todo = slice.filter { (pending.get() ?: Stamped.NONE).subjects.containsKey(it) }
                if (todo.isEmpty()) return@forEach
                // The gate is taken inside, around this slice alone: holding it
                // for a whole 20,000-subject batch measured 13 minutes on
                // staging with ingest queueing behind it.
                recompute.recomputeBatchGated(todo, removeEmpties = true, gate = gate)
                retire(Stamped(todo.associateWith { snapshot.subjects.getValue(it) }, emptyMap()), gate)
            }
            snapshot.services.keys.forEach { service ->
                if (!(pending.get() ?: Stamped.NONE).services.containsKey(service)) return@forEach
                // A service's cards become cells page by page — no derive: the
                // cell is a function of the newest card at its address alone.
                // One service per call so each retires on its own ack.
                recompute.projectServices(listOf(service), gate = gate)
                retire(Stamped(emptyMap(), mapOf(service to snapshot.services.getValue(service))), gate)
            }
            inherited.set(false)
        }
    }

    /**
     * Credit [done] — a slice this round has DERIVED AND WRITTEN — against the
     * ledger: drop the entries whose stamp is still the one the round
     * snapshotted (a re-stamped entry was re-added since and stays), then clear
     * exactly those entries from the persisted marker.
     *
     * The marker shrinks by tensor-cell REMOVES of the retired cells —
     * commutative, the mirror of [persistDelta]'s adds — never by rewriting the
     * document to this process's whole view of the ledger. Two processes share
     * this marker (the serving relay and the sync mirror each run a store), and
     * [pending] is process-local, so a blind whole-doc write by either erases
     * the other's write-ahead insurance: the marker would stop naming work that
     * is genuinely unhealed, which is the one thing it exists to do. Removes
     * compose; whole-doc writes do not.
     *
     * Runs under [gate] and re-reads [pending] there: a trust write's
     * write-ahead ([persistDelta]) touches this same document, and the gate is
     * the only thing ordering the two.
     */
    private suspend fun retire(
        done: Stamped,
        gate: suspend (suspend () -> Unit) -> Unit,
    ) {
        if (done.isEmpty()) return
        pending.updateAndGet { (it ?: Stamped.NONE).minusUnchanged(done) }
        gate {
            val live = pending.get() ?: Stamped.NONE
            // Nothing left at all: drop the document, so a SURVIVING marker is
            // always drift and never an emptied husk.
            if (live.isEmpty()) return@gate persist(Dirt.NONE)
            val cells = ArrayList<CellRemoval>(done.subjects.size + done.services.size)
            done.subjects.keys.forEach { if (!live.subjects.containsKey(it)) cells += CellRemoval(MARKER_KEY, it, influence = true, followers = false) }
            done.services.keys.forEach { if (!live.services.containsKey(it)) cells += CellRemoval(MARKER_KEY, it, influence = false, followers = true) }
            if (cells.isNotEmpty()) reputations.removeCells(cells)
        }
    }

    /** The pending dirt, reading the persisted marker once per process. */
    private suspend fun load(): Dirt {
        pending.get()?.let { return it.toDirt() }
        val stored = reputations.get(MARKER_KEY)?.let { Dirt(it.influenceScores.keys, it.followerCounts.keys) } ?: Dirt.NONE
        // Two first readers race harmlessly: both read the same marker, and
        // the loser's copy is dropped rather than overwriting work the winner
        // has since added. Inherited entries carry stamp 0, below every add.
        if (pending.compareAndSet(null, Stamped.NONE.plus(stored, 0L)) && !stored.isEmpty()) inherited.set(true)
        return pending.get()!!.toDirt()
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
