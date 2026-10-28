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
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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

    /** The services queued for a re-walk right now, by name. */
    fun queuedRewalks(): Set<String> = pendingNow().toRewalk.keys

    /** The stamp source; every queueing takes the next one. */
    private val stamps = AtomicLong()

    /**
     * The newest re-walk stamp the provider cache has already been dropped for.
     *
     * The drop exists for ONE event: a 10040 naming a service no list named
     * before, whose stored cards then have to be walked. That is news the FIRST
     * time the service is queued and never again — but the condition was "this
     * round has services to re-walk", and a service whose walk keeps failing
     * stays queued forever. The drainer retries every 5 s, so a single poisoned
     * service dropped the map on a loop; `ProviderMap` never caches an empty
     * pass, so on a relay holding no 10040s that is a fresh engine query per
     * read in between, for as long as the failure lasts.
     *
     * Stamps answer it exactly: a re-queue takes a NEW stamp, a stuck entry
     * keeps its old one. Inherited work carries stamp 0, which is above the
     * -1 this starts at, so a restart still drops the map once.
     *
     * READS THE HIGHEST STAMP, so it depends on stamps becoming VISIBLE in the
     * order they were taken — a queueing that published stamp 7 after one that
     * published 8 would never invalidate. That holds because [queue] is only
     * reached through [insuring], and every write path into it holds the
     * store's `writes` mutex (a plain insert takes it alone, a trust write
     * takes the gate and then it), so stamp allocation and publication are one
     * critical section. The drain, which runs under the gate alone, never
     * queues.
     */
    private val invalidatedThrough = AtomicLong(-1L)

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
     * The write-ahead of ONE [insuring] bracket, which its block may EXTEND as
     * it learns what it is about to write — a commit scope does not know every
     * subject when it opens (TrustProjection.commit), only as each port call
     * arrives. Each extension persists only what is neither already covered by
     * this bracket nor pending in the ledger, so the calls of one commit that
     * the store pre-declared cost no marker I/O at all, and an undeclared one
     * costs one small write.
     *
     * SERIALIZED, because the calls of one commit run concurrently (the bulk
     * path's conditional puts, [com.nosfabrica.vespa.eventstore.engine.async.PUT_FANOUT]
     * wide) and every extension writes the SAME marker document: unserialized,
     * 32 callers each wrote their own cell to it and Vespa queued them behind
     * each other — 78 ms per marker op, measured, against 5 ms of cell write.
     */
    inner class Cover internal constructor() {
        private val lock = Mutex()

        // Mutable, and touched only under [lock] (or after the block, by the
        // one coroutine that ran it): a commit extends its cover once per call,
        // up to 20,000 calls on the address-keyed path, and an immutable union
        // per call copied the whole cover each time — O(n²) under the lock the
        // whole fan-out waits on.
        private val coveredSubjects = HashSet<String>()
        private val coveredServices = HashSet<String>()
        private val persistedSubjects = LinkedHashSet<String>()
        private val persistedServices = LinkedHashSet<String>()

        /** Calls of this commit that have extended the cover and not yet recorded what they wrote — see [retire]. */
        private val inFlight = AtomicLong()

        /** Everything this bracket insures — what a failure queues. */
        val covered: ProjectionWork get() = ProjectionWork(coveredSubjects.toSet(), coveredServices.toSet())

        /** What this bracket itself wrote to the marker — what a clean finish clears. */
        val persisted: ProjectionWork get() = ProjectionWork(persistedSubjects.toSet(), persistedServices.toSet())

        /**
         * Persist [work] before the write it insures; returns once the marker
         * holds it. With [call], also counts the caller as IN FLIGHT until it
         * calls [landed] — registered under the same lock [retire] decides
         * under, so a retirement can never see "nobody is about to write" while
         * a call it would uncover has already been told it is covered.
         */
        suspend fun extend(
            work: ProjectionWork,
            call: Boolean = false,
        ) {
            if (work.isEmpty() && !call) return
            lock.withLock {
                if (call) inFlight.incrementAndGet()
                val freshSubjects = work.toRederive.filterTo(LinkedHashSet()) { coveredSubjects.add(it) }
                val freshServices = work.toRewalk.filterTo(LinkedHashSet()) { coveredServices.add(it) }
                if (freshSubjects.isEmpty() && freshServices.isEmpty()) return@withLock
                val before = pendingNow().toWork()
                val delta = ProjectionWork(freshSubjects, freshServices) - before
                if (delta.isEmpty()) return@withLock
                // Cell adds, which the engine sends as one update of the marker
                // document at any size: no read, no doc rewrite, and
                // already-pending work persists nothing.
                addMarkerCells(delta)
                persistedSubjects += delta.toRederive
                persistedServices += delta.toRewalk
            }
        }

        /** The call registered by [extend] has recorded what it wrote. */
        fun landed() {
            inFlight.decrementAndGet()
        }

        /**
         * Drop [subjects] from this bracket — their cells are written and
         * acked, mid-commit — and clear the marker cells it wrote for them, so
         * a long commit's marker holds what is still unwritten rather than all
         * it ever insured. Without it, a commit that removes a service's cards
         * a page at a time (a kind 62 by a scoring service: 242,000 cards,
         * pages of 10,000) kept every page's subjects in the marker until the
         * commit ended — one document of every subject, cleared in one call —
         * where the per-call bracket it replaced held one page at a time.
         *
         * Refused — returns false, nothing cleared — unless the caller is the
         * only call of the commit in flight: a call that extended its cover
         * and has not yet written may be about to write one of these subjects,
         * and clearing its insurance first would leave that write naked.
         * Refusing costs only over-coverage until the commit ends.
         */
        suspend fun retire(subjects: Set<String>): Boolean {
            if (subjects.isEmpty()) return true
            return lock.withLock {
                if (inFlight.get() != 0L) return@withLock false
                coveredSubjects -= subjects
                val mine = subjects.filterTo(LinkedHashSet()) { persistedSubjects.remove(it) }
                // Never a cell some queued work still needs: pending is insured too.
                mine.removeAll(pendingNow().toRederive.keys)
                clearMarkerCells(ProjectionWork(mine, emptySet()))
                true
            }
        }
    }

    /**
     * Run [block] — the event write plus any cheap inline projection —
     * bracketed by the ledger.
     *
     * [couldInvalidate] is everything the op could leave stale if it dies
     * partway, persisted as a write-ahead before anything is touched; the
     * block may add to it through the [Cover] it is handed, before each write
     * it did not declare. [block]'s [Outcome.workLeft] is what it ACTUALLY
     * left, which the cover must hold, named directly or reachable through an
     * insured service's walk. On failure the whole cover becomes pending.
     *
     * The marker may transiently OVER-cover until a drain narrows it —
     * deliberate: narrowing here would cost a doc-sized write per batch, while
     * over-coverage only costs a crash some redundant, idempotent re-derives.
     */
    suspend fun <T> insuring(
        couldInvalidate: ProjectionWork,
        block: suspend (Cover) -> Outcome<T>,
    ): T {
        adoptStoredMarkerOnce()
        val cover = Cover()
        cover.extend(couldInvalidate)
        val outcome: Outcome<T>
        try {
            outcome = block(cover)
        } catch (t: Throwable) {
            queue(cover.covered)
            // Wake the drainer even though the op failed: its retry loop repairs
            // a transient failure's work without waiting for the next write.
            onWorkQueued?.invoke()
            throw t
        }
        // ADDED, never assigned: `before + workLeft` written back would
        // resurrect whatever a concurrent drain retired since the read above.
        val queued = queue(outcome.workLeft)
        // Insurance persisted, no work left, nothing pending: the marker names
        // subjects nobody will ever drain — a clean commit whose cells are all
        // written, or a batch of cards by a signer no 10040 maps, insured
        // because the provider map could not yet say so
        // (TrustProjection.insurableSigners), that found no cells to write. No
        // drain rewrites the marker for work that does not exist, so it stood, and the next boot
        // inherited hundreds of subjects to re-derive to nothing and dropped the
        // provider cache for it. One remove here keeps the marker honest.
        //
        // THE CELLS, NOT THE DOCUMENT. This used to go on to drop the emptied
        // document ([dropMarkerIfEmpty]: a GET, then a remove), so a commit's
        // bracket was four serial round trips to one document under the trust
        // gate — on a 3-node cluster replaying a sync gap, 63–90 ms each, ~15%
        // of the ingest lock (issue #183). The husk it removed is harmless:
        // [adoptStoredMarkerOnce] reads an empty marker as no work, and the
        // next commit's write-ahead updates it in place. Leaving it is also
        // the safer half for a second writer — the drop could still take a
        // peer cell that landed between its GET and its remove. A drain that
        // empties the ledger still drops it ([retire]); a commit no longer does.
        val persisted = cover.persisted
        if (!persisted.isEmpty() && queued.isEmpty()) clearMarkerCells(persisted)
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
     * Some units failed and stay queued; the rest of the round completed.
     *
     * Carries the first failure as its cause and the rest as suppressed, so
     * [BackgroundFailures]' cause-chain walk names a real engine error rather
     * than this wrapper.
     */
    class DrainIncomplete(
        val stuck: Int,
        causes: List<Throwable>,
    ) : IllegalStateException("$stuck queued unit(s) failed and stay queued: ${causes.first().message}", causes.first()) {
        init {
            causes.drop(1).forEach { addSuppressed(it) }
        }
    }

    /**
     * Heal everything pending, in gated slices: snapshot, re-derive its
     * subjects (empties removed — which also deletes a parent whose last card
     * died with a crashed removal), re-walk its services, retiring each slice
     * as it lands. Loops until a snapshot is empty, so work queued WHILE
     * draining is picked up. Idempotent.
     *
     * A failing unit costs ONLY ITSELF: it is skipped for the rest of the
     * call, stays queued, and every other unit still gets its turn. The
     * failures are then raised together as [DrainIncomplete] once the round
     * has nothing left it can do — a throw, because the caller reads that as
     * "retry after a backoff" and a clean return as "nothing is stuck".
     * Always with the marker intact, so a retry loses nothing.
     */
    suspend fun drain(gate: WriteGate) {
        // ONE UNIT'S FAILURE IS ONE UNIT'S. Every read a unit makes can fail
        // for reasons that say nothing about the other units — an engine that
        // truncated a sorted read, a node a hair short of its target, a
        // timeout under load. Before this set existed, the first such failure
        // left the loop, and every unit behind it went underived: 342 services
        // queued, one throwing, none of the other 341 walked. The queue looked
        // busy and the projection never moved.
        //
        // Failed units are not retired, so they stay queued for the next call.
        // Skipped for the REST OF THIS ONE, because a unit that just threw will
        // throw again on the next turn of this loop, and the loop would spin on
        // it at full speed.
        val poisoned = HashSet<String>()
        val failures = ArrayList<Throwable>()
        while (true) {
            adoptStoredMarkerOnce()
            // READ, not taken: the unretired remainder stays pending while it
            // is derived — see the class KDoc.
            val snapshot = pendingNow()
            if (snapshot.isEmpty()) {
                TrustProgress.finish(DRAIN)
                break
            }
            val work = snapshot.toWork()
            // THE LONG PHASE, AND IT REPORTED NOTHING. A reconcile begins by
            // draining, and on an inherited ledger that is most of its runtime.
            // The registry only heard from the phases after it, so a page
            // watching a reconcile drew an empty panel for as long as the drain
            // took — which is exactly when someone is watching.
            TrustProgress.begin(DRAIN, "re-deriving queued subjects", work.toRederive.size.toLong())
            // ONCE PER NEW SERVICE, not once per turn of the retry loop — see
            // [invalidatedThrough].
            val newestRewalk = snapshot.toRewalk.values.maxOfOrNull { it.seq } ?: -1L
            if (crashLeftovers.get() || newestRewalk > invalidatedThrough.get()) {
                recompute.invalidateProviders()
                invalidatedThrough.getAndUpdate { maxOf(it, newestRewalk) }
            }
            // Sliced HERE rather than inside recomputeBatchGated, because the
            // ledger has to see each slice land: a slice is the unit that gets
            // skipped and the unit that gets retired.
            var progressed = false
            // COUNTED OUT OF THIS ROUND'S OWN SNAPSHOT, which is the only set
            // the denominator beside it describes.
            //
            // This used to be `work.size - pendingNow().size` — the whole
            // ledger's depth against one round's work. Those are different
            // sets: every subject a live write queues DURING the drain joins
            // `pendingNow()` and not `work`, so under ingest the numerator
            // drifts and then goes NEGATIVE. `TrustProgress.render` reads a
            // non-positive `done` as "no denominator" and prints
            // "re-deriving queued subjects: -37, total unknown" — the fraction
            // disappears exactly when the store is busy enough to want it.
            var retired = 0
            work.toRederive.chunked(TrustRecompute.GATE_SLICE).forEach { slice ->
                // ONE read of the ledger for the whole slice, not one per
                // subject: `pendingNow()` is an atomic read and the partition
                // should also see ONE state, rather than letting two subjects
                // land in different buckets because a write arrived between them.
                val stillQueued = pendingNow().toRederive
                val live = slice.filterNot { it in poisoned }
                val (gone, todo) = live.partition { !stillQueued.containsKey(it) }
                // Retired since the snapshot by a concurrent drain: done work,
                // and this round's denominator still counts it.
                retired += gone.size
                if (todo.isEmpty()) return@forEach
                try {
                    recompute.recomputeBatchGated(todo, removeEmpties = true, gate = gate)
                    retire(StampedWork(todo.associateWith { snapshot.toRederive.getValue(it) }, emptyMap()), gate)
                    // Retired, not attempted: the fraction has to mean work that
                    // landed, or it runs ahead of the writes it is reporting.
                    retired += todo.size
                    progressed = true
                } catch (e: CancellationException) {
                    throw e
                } catch (t: Throwable) {
                    poisoned += todo
                    failures += t
                }
                TrustProgress.advance(DRAIN, retired.toLong(), work.toRederive.size.toLong())
            }
            snapshot.toRewalk.keys.forEach { service ->
                if (!pendingNow().toRewalk.containsKey(service) || service in poisoned) return@forEach
                // A service's cards become cells page by page — no derive: the
                // cell is a function of the newest card at its address alone.
                // One service per call so each retires on its own ack.
                //
                // Named on the page: one service's walk is the single longest
                // thing this store does, and "walking 7d7ffd72's cards" is a
                // different answer from "still draining".
                TrustProgress.advance(DRAIN, 0, 0, phase = "walking service ${service.take(12)}'s cards into cells")
                try {
                    recompute.projectServices(listOf(service), gate = gate)
                    retire(StampedWork(emptyMap(), mapOf(service to snapshot.toRewalk.getValue(service))), gate)
                    progressed = true
                } catch (e: CancellationException) {
                    throw e
                } catch (t: Throwable) {
                    poisoned += service
                    failures += t
                }
            }
            crashLeftovers.set(false)
            // NOTHING MOVED AND NOTHING CAN. Every unit this round could reach
            // either failed or was retired by someone else, so another turn
            // would re-snapshot the same work and fail it again at full speed.
            if (!progressed) break
        }
        // RAISED LAST, ON PURPOSE. The drainer treats a throw as "retry after a
        // backoff" and a return as "clean, clear the failure count" — so
        // swallowing these would both hide stuck units from the health page and
        // leave them waiting for the next unrelated write to wake the drainer.
        // Throwing HERE, rather than where the failure happened, is what buys
        // the isolation: every other unit has already had its turn.
        if (failures.isNotEmpty()) throw DrainIncomplete(poisoned.size, failures)
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
            // Nothing left here: drop the document, so a marker that survives
            // a drain is drift or a commit's emptied husk (see [insuring]),
            // never spent cells. The marker legitimately
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
        IngestStats.timed(MARKER_STAGE) { reputations.updateCells(cells) }
    }

    /** The mirror of [addMarkerCells]: drop exactly these entries' cells, leaving every other cell alone. */
    private suspend fun clearMarkerCells(work: ProjectionWork) {
        if (work.isEmpty()) return
        val cells = ArrayList<CellRemoval>(work.toRederive.size + work.toRewalk.size)
        work.toRederive.forEach { cells += CellRemoval(MARKER_KEY, ServiceKey(it), influence = true, followers = false) }
        work.toRewalk.forEach { cells += CellRemoval(MARKER_KEY, ServiceKey(it), influence = false, followers = true) }
        IngestStats.timed(MARKER_STAGE) { reputations.removeCells(cells) }
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
     * drift that nothing named.
     *
     * NARROWED, NOT CLOSED, and the difference matters to anyone reading this
     * for a guarantee. The cleanup above clears the cells it READ, so a peer
     * cell written after that read survives where a blind delete took it — but
     * one already in the document when this process read it is still cleared.
     * Closing it needs an ownership model (a cell carrying who wrote it, or a
     * conditional remove), which is a design change and not a guard. Until
     * then this is a smaller window, not the absence of one.
     */
    private suspend fun dropMarkerIfEmpty() =
        IngestStats.timed(MARKER_STAGE) {
            val stored = reputations.get(MARKER_KEY) ?: return@timed
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
         * The [IngestStats] stage every marker round trip books to — the
         * write-ahead, its cleanup, the husk check. Each is a feed round trip
         * on the commit's critical path, beside `proj.write`, and until this
         * was booked the two could not be told apart.
         */
        const val MARKER_STAGE = "proj.marker"

        /**
         * THE MARKER IS NOT A REPUTATION DOCUMENT, and this is where that shows.
         * It borrows the reputation tensors as a plain string SET — subjects ride
         * the influence cells, services the follower cells, and both values are
         * ignored — so its cell keys are not [ServiceKey]s in the sense every
         * other cell in this doctype is. The unwrapping at these call sites is
         * that borrowing made explicit. While every key was a bare `String` the
         * type system had no way to say it, and nothing did.
         */
        private fun Set<ServiceKey>.unwrap(): Set<String> = mapTo(LinkedHashSet()) { it.hex }
    }
}
