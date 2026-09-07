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
package com.nosfabrica.vespa.eventstore.runtime

import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * THE WRITE LADDER: the two mutexes every mutating path in this store goes
 * through, and the wait/hold accounting that says which side of a stall you
 * are on.
 *
 * Lifted out of `NostrSemanticsStore` so the order below is a property of ONE
 * small file rather than of a 1,600-line class: every two-lock path in the
 * store goes through [gated], and there is nowhere else a `withLock` can be
 * written without this file's author noticing. It holds no Nostr semantics —
 * WHICH writes are trust-relevant is the store's decision, handed in as
 * [gated]'s `trust` flag — so nothing here can drift with the protocol.
 *
 * The stages are labels for `IngestStats`, not locks: several share one mutex
 * (see [Stage.lock]), which is exactly what the wait attribution has to match
 * on. See docs/locking.md for every lock in this codebase and its verdict.
 */
internal class WriteLocks {
    private val writes = Mutex()

    /**
     * THE TRUST GATE, separate from [writes] since 2026-09-04.
     *
     * One mutex used to serialise every write in this store, and the hazard it
     * was documented against is recompute-versus-recompute: "repairs must not
     * race live inserts' recomputes". A plain kind-1 note has NO recompute —
     * `TrustProjection.insuranceFor` returns `ProjectionWork.NONE` for every kind but 30382 and
     * 10040 — so it was excluded against work it cannot conflict with. Measured
     * on staging: an ephemeral event, which takes the lock and returns without
     * storing anything, took 35-41 SECONDS to answer OK while the trust drain
     * held the lock re-deriving reputation documents.
     *
     * What a plain insert genuinely needs exclusion for is the DELETION race —
     * check `isDeleted`, then put, with a kind-5 landing in between would
     * resurrect a deleted event. That is event-document work and stays on
     * [writes]. Reputation-document work moves here.
     *
     * LOCK ORDER, where both are needed (a card insert does inline projection):
     * [trustGate] FIRST, then [writes] — never the reverse. The order was
     * writes-then-gate when the split shipped, and that leaked the drain's
     * stall back onto every plain writer: a card took [writes] and then waited
     * for the gate WHILE HOLDING IT, so for the length of a drain slice every
     * kind-1 in the process queued behind the card. Gate first means a card
     * waits for the drain holding nothing, and once it has the gate it takes
     * [writes] for one short hold. The drain and the reconciler only ever
     * hold the gate, so the pair cannot deadlock as long as every two-lock
     * path here goes through [gated].
     */
    private val trustGate = Mutex()

    /** A writer-lock label's two [IngestStats] stage names, interned at construction. */
    class Stage(
        name: String,
        /**
         * WHICH MUTEX this label takes. Several share one: `lock.gate`,
         * `lock.ingest.trust`, `lock.sweep.trust` and `lock.reindex.trust` are
         * all [trustGate]; `lock.ingest`, `lock.sweep` and `lock.reindex` are
         * all [writes]. The wait attribution matches waiter to holder by THIS,
         * because matching by label misses every cross-label contention — which
         * is most of it (docs/telemetry.md §15.1).
         */
        val lock: String,
    ) {
        val wait = "$name.wait"
        val hold = "$name.hold"
    }

    /**
     * Take [writes], booking the WAIT and the HOLD under separate [IngestStats]
     * stages named for [stage].
     *
     * Every other stage timer starts once the lock is already held, so a writer
     * starved by another holder would otherwise show up as fast stages and a
     * stalled pipeline with nothing naming the reason. The deferred trust
     * projection makes that real: it re-derives off the ingest path but INSIDE
     * this lock (ProjectionLedger.drain's gate), so `proj.fetch` and an ingest commit
     * contend for one mutex while both look cheap individually. `lock.*.wait`
     * makes that visible; `lock.*.hold` attributes it.
     */
    suspend fun <T> underWrites(
        stage: Stage,
        body: suspend () -> T,
    ): T = on(writes, stage, body)

    /** [underWrites], on a named mutex — see [trustGate] for why there are two. */
    private suspend fun <T> on(
        mutex: Mutex,
        stage: Stage,
        body: suspend () -> T,
    ): T {
        val requested = System.nanoTime()
        // WHAT THIS WRITER IS ABOUT TO QUEUE BEHIND, sampled before we block —
        // the one causal edge in this design. Keyed by the MUTEX, not the stage
        // label, because several labels share each mutex and a label match
        // silently attributes nothing (docs/telemetry.md §15.1).
        val blockedBy = IngestStats.holderOf(stage.lock)?.let { IngestStats.labelOf(it) }
        var acquired = 0L
        try {
            return mutex.withLock {
                acquired = System.nanoTime()
                // Live holder, for the question the cumulative stages cannot
                // answer: not "the gate was held for 24 minutes since boot"
                // but "the gate is held RIGHT NOW, by this, for this long".
                // Two volatile writes per critical section, against a section
                // that is measured in seconds.
                IngestStats.beginHold(stage.hold, lock = stage.lock)
                try {
                    body()
                } finally {
                    IngestStats.endHold(stage.hold)
                }
            }
        } finally {
            // Booked AFTER release: recording inside would put two map lookups
            // and two atomic adds in the critical section this exists to
            // measure, and `hold` would stop short of the actual release.
            // acquired == 0 means the lock was never taken (cancelled while
            // waiting) — nothing to attribute.
            if (acquired != 0L) {
                val released = System.nanoTime()
                val waited = acquired - requested
                IngestStats.add(stage.wait, waited)
                IngestStats.add(stage.hold, released - acquired)
                // Only when something was actually holding: an uncontended
                // acquire waited on nobody, and charging it to a phantom holder
                // would make the split lie about where contention is.
                if (blockedBy != null) IngestStats.addBlocked(stage.wait, blockedBy, waited)
            }
        }
    }

    /**
     * Take [trustGate] ALONE, under [stage] — reputation-document work that
     * never touches an event document: the projection drain and the trust
     * reconciler. A plain insert is not stalled by it (that is the whole point
     * of the split); a card insert queues for it through [gated].
     */
    suspend fun <T> underGate(
        stage: Stage,
        body: suspend () -> T,
    ): T = on(trustGate, stage, body)

    /**
     * THE ONE TWO-LOCK SHAPE: [trustGate] when [trust], then [writes] under
     * [stage]. Every path that needs both goes through here, which is what
     * makes the order (see [trustGate]) a property of the file rather than of
     * each call site.
     *
     * The gate wait is charged to its OWN stage, not to LOCK_GATE: `lock.gate.*`
     * is the drain's, and folding an insert's wait for the drain into the same
     * name would make "the drain is slow" and "a card is waiting for the
     * drain" one number. They have different remedies.
     */
    suspend fun <T> gated(
        trust: Boolean,
        stage: Stage,
        /** The stage the GATE wait is booked under — ingest's by default; a sweep or a reindex names its own, so a sweep waiting on the drain does not read as "a card is waiting". */
        gateStage: Stage = INGEST_TRUST,
        body: suspend () -> T,
    ): T = if (trust) on(trustGate, gateStage) { underWrites(stage) { body() } } else underWrites(stage) { body() }

    companion object {
        /** The [writes] mutex, as the wait attribution names it. */
        const val WRITE_LOCK = "writes"

        /** The [trustGate] mutex, as the wait attribution names it. */
        const val TRUST_GATE = "trustGate"

        /**
         * Writer-lock stage labels, named for the CALLER rather than the
         * operation: these exist to say which side of the contention a stall is
         * on ("ingest waited 40s while the gate held 40s"). Built once per
         * label, since `insert()` takes this lock per event and a String
         * allocation is not what a measurement should cost.
         */
        val INGEST = Stage("lock.ingest", WRITE_LOCK)
        val GATE = Stage("lock.gate", TRUST_GATE)

        /** A trust-relevant insert queueing for [trustGate] — see `NostrSemanticsStore.touchesTrust`. */
        val INGEST_TRUST = Stage("lock.ingest.trust", TRUST_GATE)
        val SWEEP = Stage("lock.sweep", WRITE_LOCK)
        val REINDEX = Stage("lock.reindex", WRITE_LOCK)

        /** A sweep's / a reindex page's wait for the trust gate, apart from ingest's — different holders, different remedies. */
        val SWEEP_TRUST = Stage("lock.sweep.trust", TRUST_GATE)
        val REINDEX_TRUST = Stage("lock.reindex.trust", TRUST_GATE)
    }
}
