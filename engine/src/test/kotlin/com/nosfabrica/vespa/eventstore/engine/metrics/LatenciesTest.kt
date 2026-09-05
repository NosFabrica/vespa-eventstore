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

import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The histogram's PRECISION CLAIM, which is the only reason it exists in this
 * shape.
 *
 * docs/telemetry.md §10.3 justifies 8 sub-buckets per octave by asserting the
 * reported percentile lands within ~6 % of the true one. A plain octave
 * histogram — the obvious cheaper thing — would put a 4 s search in "between
 * 2.1 s and 4.2 s", which is why that number is load-bearing rather than
 * decorative. If it does not hold, the p99 column of an operator page is
 * decoration.
 */
class LatenciesTest {
    private val tolerance = 1.0 / (2 * Latencies.SUB_BUCKETS)

    /** True percentile of a sorted sample, by the same rank rule [Latencies] uses. */
    private fun exact(
        sorted: List<Long>,
        p: Double,
    ): Long {
        val rank = Math.max(1.0, Math.ceil(p * sorted.size)).toInt()
        return sorted[rank - 1]
    }

    @Test
    fun `every bucket round-trips its own midpoint`() {
        // The structural invariant under the percentile claim: whatever value a
        // bucket reports must itself land back in that bucket. A midpoint that
        // fell into a neighbour would make percentiles drift by a whole bucket.
        for (i in 0 until Latencies.BUCKETS) {
            val mid = Latencies.midpointMicros(i)
            assertEquals(i, Latencies.bucketOf(mid), "bucket $i reports $mid µs, which lands in ${Latencies.bucketOf(mid)}")
        }
    }

    @Test
    fun `buckets ascend, so a percentile scan is monotonic`() {
        var previous = -1L
        for (i in 0 until Latencies.BUCKETS) {
            val mid = Latencies.midpointMicros(i)
            assertTrue(mid > previous, "bucket $i midpoint $mid is not above $previous")
            previous = mid
        }
    }

    @Test
    fun `p50 and p99 land within the documented 6 percent of the true value`() {
        // A long-tailed sample spanning six orders of magnitude — an id lookup
        // beside a ranked search, which is the real spread this measures.
        val rng = Random(7)
        val samples =
            buildList {
                repeat(20_000) { add(rng.nextLong(50_000, 2_000_000)) } // 50 µs .. 2 ms
                repeat(2_000) { add(rng.nextLong(2_000_000, 200_000_000)) } // 2 ms .. 200 ms
                repeat(200) { add(rng.nextLong(1_000_000_000, 20_000_000_000)) } // 1 s .. 20 s
            }
        val h = Latencies()
        samples.forEach { h.record(it) }
        val sorted = samples.sorted()

        assertEquals(samples.size.toLong(), h.count())

        for (p in listOf(0.50, 0.75, 0.90, 0.99, 0.999)) {
            val truth = exact(sorted, p)
            val got = h.percentile(p)
            val error = abs(got - truth).toDouble() / truth
            assertTrue(
                error <= tolerance,
                "p$p: reported ${got / 1_000}µs vs true ${truth / 1_000}µs — ${"%.1f".format(error * 100)}% off, " +
                    "budget ${"%.1f".format(tolerance * 100)}%",
            )
        }
    }

    @Test
    fun `an empty histogram reports zero rather than a fabricated percentile`() {
        assertEquals(0L, Latencies().percentile(0.99))
        assertEquals(0L, Latencies().count())
    }

    @Test
    fun `samples past the top bucket clamp instead of being lost`() {
        val h = Latencies()
        h.record(60L * 60 * 1_000_000_000) // an hour
        assertEquals(1L, h.count(), "an absurd sample must still be counted")
        assertTrue(h.percentile(0.99) > 30L * 1_000_000_000, "it should report at least the top bucket")
    }

    @Test
    fun `sub-microsecond samples are counted, not dropped`() {
        // Storing microseconds means anything faster lands in bucket 0. It must
        // still COUNT — a dropped sample would bias every percentile above it.
        val h = Latencies()
        repeat(100) { h.record(200) }
        assertEquals(100L, h.count())
    }

    @Test
    fun `concurrent recording loses nothing`() {
        val h = Latencies()
        val threads = 8
        val each = 20_000
        val pool =
            java.util.concurrent.Executors
                .newFixedThreadPool(threads)
        val done = java.util.concurrent.CountDownLatch(threads)
        repeat(threads) { t ->
            pool.submit {
                try {
                    repeat(each) { h.record(((t + 1) * 1_000L * (it % 50 + 1))) }
                } finally {
                    done.countDown()
                }
            }
        }
        done.await()
        pool.shutdown()
        assertEquals((threads * each).toLong(), h.count())
    }
}
