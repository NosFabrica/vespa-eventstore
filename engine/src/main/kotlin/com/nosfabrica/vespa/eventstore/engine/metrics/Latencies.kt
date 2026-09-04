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
package com.nosfabrica.vespa.eventstore.engine.metrics

import java.util.concurrent.atomic.AtomicLongArray

/**
 * A LATENCY DISTRIBUTION, in fixed memory — the thing a mean and a max cannot
 * give you.
 *
 * `IngestStats` reports total/calls/max, which is right for a stage measured
 * in seconds where you are hunting ONE pathological call. It is not enough for
 * a p99 anyone will act on: max is the worst sample ever seen, and a mean over
 * a long-tailed distribution describes nothing that happened.
 *
 * SHAPE. Powers of two ([SUB_BUCKETS] linear steps inside each octave), which
 * bounds the relative error of a reported percentile at `1/(2·SUB_BUCKETS)` —
 * about **6 %** at 8 steps, comfortably inside the run-to-run noise of anything
 * being measured. A plain octave histogram would put a 4 s search in "between
 * 2.1 s and 4.2 s", which is why the sub-buckets are there.
 *
 * SCALE. Samples are recorded in MICROSECONDS, so the whole 1 µs … ~30 s range
 * costs [BUCKETS] longs (~1.6 KiB) rather than the ~2.3 KiB a nanosecond scale
 * would need for the same precision. Nothing this store measures at the port is
 * meaningfully sub-microsecond — every call crosses a network or a data
 * structure large enough to dwarf that — and anything faster lands in bucket 0
 * and reads back as 500 ns.
 *
 * Thread-safe and allocation-free on the write path: one [AtomicLongArray],
 * whose buckets spread contention across cache lines without the per-core cell
 * arrays a `LongAdder` per bucket would inflate on a large host (measured in
 * docs/telemetry.md §6.1).
 */
class Latencies {
    private val buckets = AtomicLongArray(BUCKETS)

    /** Record one sample given in NANOSECONDS. Values above the top bucket clamp into it rather than being lost. */
    fun record(nanos: Long) {
        buckets.incrementAndGet(bucketOf(nanos / 1_000L))
    }

    /** Total samples recorded. */
    fun count(): Long {
        var n = 0L
        for (i in 0 until BUCKETS) n += buckets.get(i)
        return n
    }

    /**
     * The [p]-th percentile in NANOSECONDS (p in 0.0..1.0), or 0 when nothing
     * has been recorded. Reports the containing bucket's midpoint, so the
     * answer carries the ~6 % bound above rather than pretending to a precision
     * the structure does not hold.
     */
    fun percentile(p: Double): Long {
        val total = count()
        if (total == 0L) return 0L
        val target = Math.max(1L, Math.ceil(p.coerceIn(0.0, 1.0) * total).toLong())
        var seen = 0L
        for (i in 0 until BUCKETS) {
            seen += buckets.get(i)
            if (seen >= target) return midpointMicros(i) * 1_000L
        }
        return midpointMicros(BUCKETS - 1) * 1_000L
    }

    /** A stable copy, for a snapshot that must not change under the reader. */
    fun copyOf(): LongArray = LongArray(BUCKETS) { buckets.get(it) }

    /** Test seam: forget every sample. */
    fun reset() {
        for (i in 0 until BUCKETS) buckets.set(i, 0L)
    }

    companion object {
        /** Linear steps per octave. 8 bounds the reported error at 1/16 ≈ 6 %. */
        const val SUB_BUCKETS = 8

        private const val SUB_BITS = 3

        /** Below this the buckets are linear (one per microsecond) — no octave to subdivide. */
        private const val LINEAR_LIMIT = SUB_BUCKETS * 2

        /** Top octave: 2^25 µs ≈ 33.5 s, past any read this store should survive. */
        private const val MAX_OCTAVE = 25

        /**
         * The first octave the sub-bucketed region covers. The linear region
         * already spans 0..[LINEAR_LIMIT]-1 = 0..15, i.e. everything below
         * 2^4, so the octaves start at 4 — starting at [SUB_BITS] would
         * double-cover 8..15 and put a value in two buckets at once.
         */
        private const val FIRST_OCTAVE = SUB_BITS + 1

        /** 16 linear + 8 per octave from [FIRST_OCTAVE] to [MAX_OCTAVE]. */
        const val BUCKETS = LINEAR_LIMIT + SUB_BUCKETS * (MAX_OCTAVE - FIRST_OCTAVE + 1)

        /** Which bucket a microsecond value falls in. */
        fun bucketOf(micros: Long): Int {
            if (micros < LINEAR_LIMIT) return if (micros < 0) 0 else micros.toInt()
            val octave = 63 - java.lang.Long.numberOfLeadingZeros(micros)
            if (octave > MAX_OCTAVE) return BUCKETS - 1
            val offset = ((micros ushr (octave - SUB_BITS)) and (SUB_BUCKETS - 1L)).toInt()
            return LINEAR_LIMIT + SUB_BUCKETS * (octave - FIRST_OCTAVE) + offset
        }

        /** The representative value (midpoint) of bucket [i], in microseconds. */
        fun midpointMicros(i: Int): Long {
            if (i < LINEAR_LIMIT) return i.toLong()
            val k = (i - LINEAR_LIMIT) / SUB_BUCKETS
            val offset = (i - LINEAR_LIMIT) % SUB_BUCKETS
            val octave = k + FIRST_OCTAVE
            val step = 1L shl (octave - SUB_BITS)
            val lo = (1L shl octave) + offset * step
            return lo + step / 2
        }
    }
}
