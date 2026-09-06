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

        /** Elapsed seconds, never 0, so a rate can divide by it. */
        val elapsedSec: Long get() = ((updatedMs - startedMs) / 1000).coerceAtLeast(1)
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

    /** Test seam: one test's walk must not leak into the next. */
    fun reset() = steps.clear()

    /**
     * The operator's line. A fraction and an ETA appear only where a
     * denominator is known — a walk without one says so rather than inventing
     * a percentage, which is the failure this exists to end.
     */
    fun line(): String =
        snapshot()
            .filterNot { it.finished }
            .joinToString("; ") { s ->
                if (s.total <= 0 || s.done <= 0) {
                    "${s.op} ${s.phase}: ${s.done}, total unknown (${s.elapsedSec}s)"
                } else {
                    val pct = (s.done * 100 / s.total).coerceAtMost(100)
                    val rate = s.done.toDouble() / s.elapsedSec
                    val eta = if (rate > 0) "${((s.total - s.done) / rate).toLong()}s" else "unknown"
                    "${s.op} ${s.phase}: ${s.done}/${s.total} ($pct%), eta $eta"
                }
            }
}
