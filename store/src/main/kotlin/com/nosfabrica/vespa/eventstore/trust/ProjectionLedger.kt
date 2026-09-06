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
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/** Projection work, declarative: re-derive exactly these subjects, re-walk exactly these services. */
internal data class ProjectionWork(
    val toRederive: Set<String>,
    val toRewalk: Set<String>,
) {
    fun isEmpty() = toRederive.isEmpty() && toRewalk.isEmpty()

    operator fun plus(other: ProjectionWork) = if (other.isEmpty()) this else ProjectionWork(toRederive + other.toRederive, toRewalk + other.toRewalk)

    operator fun minus(other: ProjectionWork) = ProjectionWork(toRederive - other.toRederive, toRewalk - other.toRewalk)

    companion object {
        val NONE = ProjectionWork(emptySet(), emptySet())
    }
}

/** What [ProjectionLedger.insuring] hands back: the caller's own result, plus the work the op left behind. */
internal data class Outcome<T>(
    val result: T,
    val workLeft: ProjectionWork,
)

/** The sequence number of the queueing that last named an entry. */
@JvmInline
internal value class Stamp(
    val seq: Long,
)

/**
 * The trust projection's WORK LEDGER — crash safety and optional deferral for
 * every trust-mutating op. Re-derivation is a pure function of the store's
 * CURRENT state under the writer lock, so work coalesces across ops and every
 * drain schedule converges to the same tensors.
 *
 * CRASH SAFETY: the event and projection writes are separate acks, and dedup
 * fires a write trigger exactly once, so a failure between them used to be
 * permanent drift — the retry comes back all-duplicates and can never repair
 * it. [insuring] persists what the op could invalidate BEFORE touching
 * anything. A surviving marker IS the drift, named.
 *
 * The marker is one [ReputationDoc] under [MARKER_KEY], deliberately not
 * 64-hex so no card can collide with it ([subjectOf] admits only 64-hex).
 * Subjects ride its influence cells, services its follower cells.
 *
 * WHY [unfinished] IS AN ATOMIC AND NOT A FIELD. It used to be plain fields,
 * safe because every entry point ran under the store's ONE writer lock. The
 * trust-gate split (NostrSemanticsStore.trustGate) ended that: a kind-1 insert
 * runs [insuring] under the event lock alone while a background [drain]
 * mutates the same ledger under the trust gate alone, and two
 * read-modify-writes of a plain field under different locks is a lost update.
 * A kind-1 that read the ledger before a drain cleared it wrote the cleared
 * work back, so the next card for that subject computed an empty write-ahead
 * against an over-covering memory, and a crash before the following drain
 * would have been permanent drift with no marker naming it.
 *
 * WHY ENTRIES ARE STAMPED. A round retires only entries whose stamp is still
 * the one it snapshotted, which is what makes a re-add DURING a round survive:
 * a card for subject A written after A's slice was derived re-stamps A, so the
 * retirement leaves it for the next round. A first version retired the
 * snapshot as a set, `unfinished - snapshot`, and lost exactly that card — A's
 * newer rank was served only after a reconcile.
 *
 * WHY RETIREMENT IS PER SLICE. Retiring per ROUND made a large ledger look
 * frozen for as long as the round took: staging inherited 139,524 subjects, a
 * slice of 500 measured ~14s of derive, so the backlog gauge sat at its
 * starting value and the marker at full size for over an hour per pass — and a
 * crash anywhere in that hour re-derived every subject including the ones
 * written in its first minute. A ledger that takes longer to drain than the
 * process stays up never finishes.
 *
 * WHY A ROUND LEAVES ITS UNRETIRED REMAINDER PENDING. A trust write's
 * write-ahead is computed against the ledger, so the marker keeps covering the
 * in-flight remainder however that write-ahead is persisted, and a round that
 * fails leaves nothing to restore. A second drain — the read-your-writes
 * barrier, a verify — still returns only once the work is visible, because it
 * SKIPS what another drain already retired (retired means written and acked)
 * and re-derives the rest, idempotently. Skipping is what keeps two concurrent
 * drains of one ledger, the background loop's and [TrustReconciler]'s, from
 * each deriving the whole snapshot; on staging they were splitting one
 * process's derive budget between two passes doing identical work.
 *
 * NOT a mutex around [drain], deliberately: an inline drain runs while its
 * caller holds the writer lock, so blocking it on a round that needs that same
 * lock would deadlock.
 */
internal class ProjectionLedger(
    private val reputations: ReputationIndex,
    private val recompute: TrustRecompute,
) {
    /** The ledger's entries and the stamp each was last named by. Immutable; every change is a CAS. */
    private class StampedWork(
        val toRederive: Map<String, Stamp>,
        val toRewalk: Map<String, Stamp>,
    ) {
        fun isEmpty() = toRederive.isEmpty() && toRewalk.isEmpty()

        fun toWork() = ProjectionWork(toRederive.keys, toRewalk.keys)

        /** [work]'s entries (re)stamped [stamp]; the same instance when there is nothing to add. */
        fun plus(
            work: ProjectionWork,
            stamp: Stamp,
        ): StampedWork = if (work.isEmpty()) this else StampedWork(toRederive + work.toRederive.associateWith { stamp }, toRewalk + work.toRewalk.associateWith { stamp })

        /** Without [done]'s entries whose stamp is unchanged; a re-stamped entry was re-added since and stays. */
        fun minusRetired(done: StampedWork): StampedWork = StampedWork(toRederive.filterNot { (s, n) -> done.toRederive[s] == n }, toRewalk.filterNot { (s, n) -> done.toRewalk[s] == n })

        companion object {
            val NONE = StampedWork(emptyMap(), emptyMap())
        }
    }

    /** Work not yet healed. Null only until [adoptStoredMarkerOnce] has run; moved only by CAS. */
    private val unfinished = AtomicReference<StampedWork?>(null)

    /** The ledger as it stands right now, with no I/O and no locks held. */
    private fun pendingNow(): StampedWork = unfinished.get() ?: StampedWork.NONE

    /**
     * Subjects queued for re-derivation right now — the backlog GAUGE behind an
     * operator page. Instantaneous: never diffed between snapshots the way a
     * counter is, because a queue depth has no cumulative form. Reading the
     * atomic directly is publication enough for a snapshot thread holding none
     * of this ledger's locks.
     */
    fun pendingSubjects(): Long = pendingNow().toRederive.size.toLong()

    /** Services queued for a re-walk right now — a gauge, like [pendingSubjects]. */
    fun pendingServices(): Long = pendingNow().toRewalk.size.toLong()

    /** The stamp source; every queueing takes the next one. */
    private val stamps = AtomicLong()

    /**
     * True while work inherited from a PREVIOUS process is unhealed. That
     * process may have died between writing a 10040 and invalidating the
     * provider-map cache, so the heal must drop the cache even when the
     * inherited work names no services. In-process work invalidates inline.
     */
    private val crashLeftovers = AtomicBoolean(false)

    /** Set by [drainInBackground]; null means settle work inline, the read-your-writes mode. */
    @Volatile
    private var onWorkQueued: (() -> Unit)? = null

    /**
     * Switch to DEFERRED mode: [insuring] leaves work pending and fires
     * [onWork] instead of settling it, so writes return fast and storms
     * coalesce into one re-derivation. The owner runs [drain] on the signal —
     * and once at startup too, since a previous process's marker is only
     * discovered by draining.
     */
    fun drainInBackground(onWork: () -> Unit) {
        onWorkQueued = onWork
    }

    /**
     * Run [block] — the event write plus any cheap inline projection —
     * bracketed by the ledger.
     *
     * [couldInvalidate] is everything the op could leave stale if it dies
     * partway, persisted as a write-ahead before anything is touched;
     * [block]'s [Outcome.workLeft] is what it ACTUALLY left, which
     * [couldInvalidate] must cover, named directly or reachable through an
     * insured service's walk. On failure the whole insurance becomes pending.
     *
     * The marker may transiently OVER-cover until a drain narrows it —
     * deliberate: narrowing here would cost a doc-sized write per batch, while
     * over-coverage only costs a crash some redundant, idempotent re-derives.
     */
    suspend fun <T> insuring(
        couldInvalidate: ProjectionWork,
        block: suspend () -> Outcome<T>,
    ): T {
        val before = adoptStoredMarkerOnce()
        val delta = couldInvalidate - before
        if (!delta.isEmpty()) {
            // A small delta is pipelined cell adds — no read, no doc rewrite,
            // and already-pending work persists nothing. A bulk batch's is one
            // doc put: per-entry ops at batch size would rival the event writes
            // they insure.
            if (delta.toRederive.size + delta.toRewalk.size <= MAX_CELL_ADDS) addMarkerCells(delta) else mergeWholeMarker(before + couldInvalidate)
        }
        val outcome: Outcome<T>
        try {
            outcome = block()
        } catch (t: Throwable) {
            queue(couldInvalidate)
            // Wake the drainer even though the op failed: its retry loop repairs
            // a transient failure's work without waiting for the next write.
            onWorkQueued?.invoke()
            throw t
        }
        // ADDED, never assigned: `before + workLeft` written back would
        // resurrect whatever a concurrent drain retired since the read above.
        val queued = queue(outcome.workLeft)
        // Insurance persisted, no work left, nothing pending: the marker names
        // subjects nobody will ever drain — a batch of cards by a signer no
        // 10040 maps (a mirror's by-kind card ingest is mostly this) insured
        // every subject and then found no cells to write. No drain rewrites the
        // marker for work that does not exist, so it stood, and the next boot
        // inherited hundreds of subjects to re-derive to nothing and dropped the
        // provider cache for it. One remove here keeps the marker honest.
        if (!delta.isEmpty() && queued.isEmpty()) {
            clearMarkerCells(delta)
            dropMarkerIfEmpty()
        }
        val deferred = onWorkQueued
        if (deferred == null) {
            drain(WriteGate.DIRECT) // settle inline: the caller holds the writer lock
        } else if (!queued.isEmpty()) {
            deferred()
        }
        return outcome.result
    }

    /** Union [work] into the ledger, freshly stamped, and return the result; a no-op CAS when [work] is empty. */
    private fun queue(work: ProjectionWork): ProjectionWork {
        if (work.isEmpty()) return pendingNow().toWork()
        val stamp = Stamp(stamps.incrementAndGet())
        return unfinished.updateAndGet { (it ?: StampedWork.NONE).plus(work, stamp) }!!.toWork()
    }

    /**
     * Heal everything pending, in gated slices: snapshot, re-derive its
     * subjects (empties removed — which also deletes a parent whose last card
     * died with a crashed removal), re-walk its services, retiring each slice
     * as it lands. Loops until a snapshot is empty, so work queued WHILE
     * draining is picked up. Idempotent; throws with the marker intact if a
     * repair step fails.
     */
    suspend fun drain(gate: WriteGate) {
        while (true) {
            adoptStoredMarkerOnce()
            // READ, not taken: the unretired remainder stays pending while it
            // is derived — see the class KDoc.
            val snapshot = pendingNow()
            if (snapshot.isEmpty()) {
                TrustProgress.finish(DRAIN)
                return
            }
            val work = snapshot.toWork()
            // THE LONG PHASE, AND IT REPORTED NOTHING. A reconcile begins by
            // draining, and on an inherited ledger that is most of its runtime.
            // The registry only heard from the phases after it, so a page
            // watching a reconcile drew an empty panel for as long as the drain
            // took — which is exactly when someone is watching.
            TrustProgress.begin(DRAIN, "re-deriving queued subjects", work.toRederive.size.toLong())
            if (work.toRewalk.isNotEmpty() || crashLeftovers.get()) recompute.invalidateProviders()
            // Sliced HERE rather than inside recomputeBatchGated, because the
            // ledger has to see each slice land: a slice is the unit that gets
            // skipped and the unit that gets retired.
            work.toRederive.chunked(TrustRecompute.GATE_SLICE).forEach { slice ->
                val todo = slice.filter { pendingNow().toRederive.containsKey(it) }
                if (todo.isEmpty()) return@forEach
                recompute.recomputeBatchGated(todo, removeEmpties = true, gate = gate)
                retire(StampedWork(todo.associateWith { snapshot.toRederive.getValue(it) }, emptyMap()), gate)
                // Retired, not attempted: the fraction has to mean work that
                // landed, or it runs ahead of the writes it is reporting.
                TrustProgress.advance(DRAIN, (work.toRederive.size - pendingNow().toRederive.size).toLong(), work.toRederive.size.toLong())
            }
            snapshot.toRewalk.keys.forEach { service ->
                if (!pendingNow().toRewalk.containsKey(service)) return@forEach
                // A service's cards become cells page by page — no derive: the
                // cell is a function of the newest card at its address alone.
                // One service per call so each retires on its own ack.
                //
                // Named on the page: one service's walk is the single longest
                // thing this store does, and "walking 7d7ffd72's cards" is a
                // different answer from "still draining".
                TrustProgress.advance(DRAIN, 0, 0, phase = "walking service ${service.take(12)}'s cards into cells")
                recompute.projectServices(listOf(service), gate = gate)
                retire(StampedWork(emptyMap(), mapOf(service to snapshot.toRewalk.getValue(service))), gate)
            }
            crashLeftovers.set(false)
        }
    }

    /**
     * Credit [done] — a slice this round has derived AND written — against the
     * ledger, then clear exactly those entries from the persisted marker.
     *
     * Clearing is per-cell ([clearMarkerCells]), never a rewrite of the whole
     * document to this process's view. The serving relay and the sync mirror
     * each run a store against this one marker while [unfinished] is
     * process-local, so a blind whole-doc write by either erases the other's
     * write-ahead insurance — the marker would stop naming work that is
     * genuinely unhealed, which is the one thing it exists to do. Cell removes
     * compose; whole-document writes do not.
     */
    private suspend fun retire(
        done: StampedWork,
        gate: WriteGate,
    ) {
        if (done.isEmpty()) return
        unfinished.updateAndGet { (it ?: StampedWork.NONE).minusRetired(done) }
        gate.holding {
            // Re-read under the gate: a trust write's write-ahead
            // ([addMarkerCells]) touches this same document, and the gate is the
            // only thing ordering the two. An entry re-added since keeps its cell.
            val live = pendingNow()
            clearMarkerCells(
                ProjectionWork(
                    done.toRederive.keys
                        .filterNot { live.toRederive.containsKey(it) }
                        .toSet(),
                    done.toRewalk.keys
                        .filterNot { live.toRewalk.containsKey(it) }
                        .toSet(),
                ),
            )
            // Nothing left here: drop the document, so a surviving marker is
            // always drift and never an emptied husk. The marker legitimately
            // OVER-covers between a bulk write-ahead and the drain that narrows
            // it (see [insuring]), so this has to clear more than `done` — but
            // it clears the cells it has READ, one by one, rather than deleting
            // a document whose contents it never looked at. A peer's insurance
            // written after that read survives, where a blind delete took it.
            if (live.isEmpty()) {
                clearMarkerCells(storedMarker())
                dropMarkerIfEmpty()
            }
        }
    }

    /**
     * The stored marker, adopted into [unfinished] on the FIRST call of this
     * process and never read again — a running process's ledger is its memory,
     * not the document. Returns the ledger either way.
     */
    private suspend fun adoptStoredMarkerOnce(): ProjectionWork {
        unfinished.get()?.let { return it.toWork() }
        val stored = storedMarker()
        // Two first readers race harmlessly: both read the same marker, and the
        // loser's copy is dropped rather than overwriting work the winner has
        // since added. Inherited entries carry stamp 0, below every add.
        if (unfinished.compareAndSet(null, StampedWork.NONE.plus(stored, Stamp(0L))) && !stored.isEmpty()) crashLeftovers.set(true)
        return pendingNow().toWork()
    }

    /** Write-ahead append: one pipelined tensor-cell add per NEW entry — no read, no doc rewrite. */
    private suspend fun addMarkerCells(work: ProjectionWork) {
        if (work.isEmpty()) return
        val cells = ArrayList<ReputationCells>(work.toRederive.size + work.toRewalk.size)
        work.toRederive.forEach { cells += ReputationCells(MARKER_KEY, ServiceKey(it), 1, null) }
        work.toRewalk.forEach { cells += ReputationCells(MARKER_KEY, ServiceKey(it), null, 1.0) }
        reputations.updateCells(cells)
    }

    /** The mirror of [addMarkerCells]: drop exactly these entries' cells, leaving every other cell alone. */
    private suspend fun clearMarkerCells(work: ProjectionWork) {
        if (work.isEmpty()) return
        val cells = ArrayList<CellRemoval>(work.toRederive.size + work.toRewalk.size)
        work.toRederive.forEach { cells += CellRemoval(MARKER_KEY, ServiceKey(it), influence = true, followers = false) }
        work.toRewalk.forEach { cells += CellRemoval(MARKER_KEY, ServiceKey(it), influence = false, followers = true) }
        reputations.removeCells(cells)
    }

    /**
     * One doc put for a delta too big to be cells — MERGED with what is
     * stored, never this process's view alone.
     *
     * The merge is the multi-writer rule this file is built on ([retire]): a
     * whole-document write composes only if it carries every cell already
     * there, and the peer's write-ahead insurance is exactly the cell this
     * process has never heard of. The read costs one small get against a
     * write this branch only takes at bulk size.
     */
    private suspend fun mergeWholeMarker(work: ProjectionWork) {
        val stored = reputations.get(MARKER_KEY)
        val merged =
            ProjectionWork(
                work.toRederive + (stored?.influenceScores?.keys?.unwrap() ?: emptySet()),
                work.toRewalk + (stored?.followerCounts?.keys?.unwrap() ?: emptySet()),
            )
        if (merged.isEmpty()) reputations.remove(MARKER_KEY) else reputations.put(marker(merged))
    }

    /** The marker's cells as they are STORED right now — the only emptiness this class is allowed to act on. */
    private suspend fun storedMarker(): ProjectionWork = reputations.get(MARKER_KEY)?.let { ProjectionWork(it.influenceScores.keys.unwrap(), it.followerCounts.keys.unwrap()) } ?: ProjectionWork.NONE

    /**
     * Remove the marker IF the stored document has no cells left — the
     * emptied-husk cleanup, made safe for a second writer.
     *
     * A blind `remove` here was the one whole-document write left in this
     * class, and it fired on THIS process's ledger being empty: the serving
     * relay finishing its last slice deleted the sync mirror's freshly written
     * insurance along with its own spent cells, so the mirror's next crash left
     * drift that nothing named. Emptiness is now read off the document, so a
     * cell this process never saw is a cell it cannot erase.
     */
    private suspend fun dropMarkerIfEmpty() {
        val stored = reputations.get(MARKER_KEY) ?: return
        if (stored.influenceScores.isEmpty() && stored.followerCounts.isEmpty()) reputations.remove(MARKER_KEY)
    }

    companion object {
        /**
         * The marker's document id — deliberately NOT 64-hex, so no event can
         * name it ([subjectOf] filters to 64-hex): it never collides with a
         * real subject's parent doc and never joins ranking.
         */
        const val MARKER_KEY = "projection-dirty"

        /** Registry key — the operator reads this string on the trust page. */
        const val DRAIN = "trust-drain"

        /**
         * Largest write-ahead delta persisted as per-cell adds; bigger takes one
         * doc put. Adds win for live traffic's small deltas; a put wins for
         * bulk, where per-entry ops rival the event writes they insure.
         */
        internal const val MAX_CELL_ADDS = 64

        /** The persisted form: subjects ride the influence cells, services the follower cells (values are ignored). */
        private fun marker(work: ProjectionWork): ReputationDoc = ReputationDoc(MARKER_KEY, work.toRederive.associate { ServiceKey(it) to 1 }, work.toRewalk.associate { ServiceKey(it) to 1.0 })

        /**
         * THE MARKER IS NOT A REPUTATION DOCUMENT, and this is where that shows.
         * It borrows the reputation tensors as a plain string SET — subjects ride
         * the influence cells, services the follower cells, and both values are
         * ignored — so its cell keys are not [ServiceKey]s in the sense every
         * other cell in this doctype is. The wrapping at these four call sites is
         * that borrowing made explicit. While every key was a bare `String` the
         * type system had no way to say it, and nothing did.
         */
        private fun Set<ServiceKey>.unwrap(): Set<String> = mapTo(LinkedHashSet()) { it.hex }
    }
}
