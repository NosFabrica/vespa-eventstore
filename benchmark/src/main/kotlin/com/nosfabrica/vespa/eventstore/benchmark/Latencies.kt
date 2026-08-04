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
package com.nosfabrica.vespa.eventstore.benchmark

/**
 * Per-operation latency samples -> tail percentiles. Averages hide exactly
 * what a relay's SLO is made of — a p99 that moves independently of the mean
 * (GC pauses, proton flushes, the 5xx retry path all live in the tail) — so
 * every suite records individual operation times through one of these and
 * prints `p50/p95/p99` next to its throughput number.
 *
 * Thread-safe for the concurrent suites; recording is a synchronized list
 * append (sub-µs), far below the cost of the operations being measured.
 */
class Latencies {
    private val samples = ArrayList<Long>()

    @Synchronized
    fun record(nanos: Long) {
        samples.add(nanos)
    }

    /** Time [op] and record it; returns the op's value. */
    inline fun <T> timed(op: () -> T): T {
        val t0 = System.nanoTime()
        val r = op()
        record(System.nanoTime() - t0)
        return r
    }

    @Synchronized
    private fun sorted(): LongArray = samples.toLongArray().also { it.sort() }

    /** The [q] percentile in nanos, or null with no samples (for the JSON results). */
    fun percentileNanos(q: Double): Long? {
        val s = sorted()
        if (s.isEmpty()) return null
        return s[((s.size - 1) * q).toInt()]
    }

    /** "p50=1.2ms p95=3.4ms p99=9.9ms" (or "-" with no samples). */
    fun summary(): String {
        val s = sorted()
        if (s.isEmpty()) return "-"

        fun p(q: Double) = s[((s.size - 1) * q).toInt()]
        return "p50=${fmt(p(0.50))} p95=${fmt(p(0.95))} p99=${fmt(p(0.99))}"
    }

    companion object {
        /** Nanos -> a compact human unit: µs below 1 ms, ms above. */
        fun fmt(nanos: Long): String =
            if (nanos < 1_000_000) {
                String.format("%dµs", nanos / 1_000)
            } else {
                String.format("%.1fms", nanos / 1e6)
            }
    }
}
