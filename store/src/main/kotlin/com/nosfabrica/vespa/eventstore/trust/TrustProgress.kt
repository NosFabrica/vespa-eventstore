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

import java.util.concurrent.ConcurrentHashMap

/**
 * WHERE THE TRUST REPAIRS HAVE GOT TO, reported whether or not anyone asked.
 *
 * Every long trust operation already took an `onProgress`, and every caller
 * passed the argument out: `reconcile`, `verify`, `sweepOrphans`,
 * `recomputeWalk`. `rebuildAll` — the one documented as bounded only by the
 * corpus — had no hook at all. A nullable callback makes "report nothing" the
 * default forever, and it was taken every time.
 *
 * So this is a REGISTRY, not a callback: the operation writes here
 * unconditionally, the way [IngestStats] and the store's gauges already work,
 * and a reader takes a snapshot. The optional callbacks stay for callers that
 * want a stream; nothing depends on one existing.
 *
 * Costs a map write per batch, against operations that walk a corpus.
 */
internal object TrustProgress {
    /** One operation's position. [total] is 0 where the walk has no denominator to give. */
    class Step(
        val op: String,
        val phase: String,
        val done: Long,
        val total: Long,
        val startedMs: Long,
        val updatedMs: Long,
    ) {
        val finished: Boolean get() = phase == DONE

        /** Elapsed seconds SINCE THE LAST ADVANCE, never 0, so a rate can divide by it. */
        val elapsedSec: Long get() = ((updatedMs - startedMs) / 1000).coerceAtLeast(1)

        /**
         * Wall seconds since this step last MOVED — 0 while it is advancing,
         * and growing without bound while it is not.
         *
         * [elapsedSec] cannot say this and reading it as if it could cost an
         * afternoon: `updatedMs` only moves on an advance, so a step that
         * begins and never advances has `updatedMs == startedMs`, an elapsed
         * of 0, and a `coerceAtLeast(1)` that renders it as a confident "1s"
         * forever. A walk blocked on a read looked identical to one restarting
         * constantly, and I read it as the latter.
         */
        val stalledForSec: Long get() = stalledAtSec(System.currentTimeMillis())

        /** [stalledForSec] against a supplied clock, so the rendering rule is testable without waiting minutes for it. */
        fun stalledAtSec(nowMs: Long): Long = (nowMs - updatedMs) / 1000
    }

    const val DONE = "done"

    private val steps = ConcurrentHashMap<String, Step>()

    fun begin(
        op: String,
        phase: String,
        total: Long = 0,
    ) {
        val now = System.currentTimeMillis()
        steps[op] = Step(op, phase, 0, total, now, now)
    }

    fun advance(
        op: String,
        done: Long,
        total: Long = 0,
        phase: String? = null,
    ) {
        val prev = steps[op]
        val now = System.currentTimeMillis()
        steps[op] =
            Step(
                op = op,
                phase = phase ?: prev?.phase ?: "running",
                done = done,
                // A denominator learned later must not be lost by a caller that stops passing it.
                total = if (total > 0) total else prev?.total ?: 0,
                startedMs = prev?.startedMs ?: now,
                updatedMs = now,
            )
    }

    fun finish(op: String) {
        val prev = steps[op] ?: return
        steps[op] = Step(op, DONE, prev.done, prev.total, prev.startedMs, System.currentTimeMillis())
    }

    fun snapshot(): List<Step> = steps.values.sortedBy { it.op }

    /** After this long without an advance, a step is reported as STALLED rather than merely slow. */
    const val STALLED_AFTER_SEC = 120L

    /** Test seam: one test's walk must not leak into the next. */
    fun reset() = steps.clear()

    /**
     * The operator's line. A fraction and an ETA appear only where a
     * denominator is known — a walk without one says so rather than inventing
     * a percentage, which is the failure this exists to end.
     */
    fun line(): String = render(snapshot().filterNot { it.finished }, System.currentTimeMillis())

    /**
     * [line]'s rule, over a supplied set of live steps and clock.
     *
     * STALLED IS ABOUT THE PIPELINE, NOT THE STEP. These steps delegate: a
     * drain hands one service to the walk and cannot retire it until the walk
     * returns, so the drain's own `done` is FROZEN BY DESIGN for as long as a
     * service takes — twenty-four minutes, in one measured case. Reporting
     * that as "STALLED 1446s" beside a walk visibly advancing through 150,666
     * cards is the tool crying wolf, and the next person to read it wastes the
     * time I did.
     *
     * So a step is stalled only when NOTHING is moving. One live step
     * advancing means the work is progressing and the frozen ones are waiting
     * on it, which is not the same thing as stuck.
     */
    internal fun render(
        live: List<Step>,
        nowMs: Long,
    ): String {
        val anythingMoving = live.any { it.stalledAtSec(nowMs) < STALLED_AFTER_SEC }
        return live.joinToString("; ") { s ->
            val stalledFor = s.stalledAtSec(nowMs)
            val stalled = if (!anythingMoving && stalledFor >= STALLED_AFTER_SEC) " STALLED ${stalledFor}s" else ""
            if (s.total <= 0 || s.done <= 0) {
                "${s.op} ${s.phase}: ${s.done}, total unknown (${s.elapsedSec}s)$stalled"
            } else {
                val pct = (s.done * 100 / s.total).coerceAtMost(100)
                val rate = s.done.toDouble() / s.elapsedSec
                val eta = if (rate > 0) "${((s.total - s.done) / rate).toLong()}s" else "unknown"
                "${s.op} ${s.phase}: ${s.done}/${s.total} ($pct%), eta $eta$stalled"
            }
        }
    }
}
