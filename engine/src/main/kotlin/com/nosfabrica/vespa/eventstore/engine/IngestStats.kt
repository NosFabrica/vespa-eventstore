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
package com.nosfabrica.vespa.eventstore.engine

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Process-wide wall-time accounting for the ingest pipeline's named stages —
 * answers "the writer is busy 100%, but WHERE?". Stages self-register on first
 * use; [gauge] renders per-stage seconds since its last call. Global on
 * purpose: store and projection are separate modules composed per process, and
 * threading a stats object through both APIs would couple them for what is
 * strictly observability.
 */
object IngestStats {
    private val stages = ConcurrentHashMap<String, AtomicLong>()
    private val lastSeen = ConcurrentHashMap<String, Long>()

    /** Add [nanos] of wall time to [stage]. */
    fun add(
        stage: String,
        nanos: Long,
    ) {
        // computeIfAbsent, NOT getOrPut: the latter compiles to get-then-put, so
        // concurrent first-touches of a new stage each construct their own
        // AtomicLong and every increment but the last put's is dropped (measured
        // at 8 threads racing one fresh key: 123 of 200 trials lost one).
        stages.computeIfAbsent(stage) { AtomicLong() }.addAndGet(nanos)
    }

    /** Time [body], booking its wall time under [stage]. */
    suspend fun <T> timed(
        stage: String,
        body: suspend () -> T,
    ): T {
        val t0 = System.nanoTime()
        try {
            return body()
        } finally {
            add(stage, System.nanoTime() - t0)
        }
    }

    /** One status-line snapshot: per-stage seconds SINCE THE LAST CALL, busiest first; empty when idle. */
    fun statusLine(): String {
        val parts =
            stages.entries
                .mapNotNull { (name, total) ->
                    val now = total.get()
                    val delta = now - (lastSeen.put(name, now) ?: 0L)
                    if (delta < 50_000_000) null else name to delta
                }.sortedByDescending { it.second }
        if (parts.isEmpty()) return ""
        return "stages " + parts.joinToString(" ") { (n, d) -> "$n %.1fs".format(d / 1e9) }
    }

    /** Every stage's cumulative seconds, no threshold — the full split for profiling. */
    fun dump(): String {
        if (stages.isEmpty()) return "stages (none)"
        return "stages " +
            stages.entries
                .map { (n, t) -> n to t.get() }
                .sortedByDescending { it.second }
                .joinToString(" ") { (n, ns) -> "$n %.2fs".format(ns / 1e9) }
    }
}
