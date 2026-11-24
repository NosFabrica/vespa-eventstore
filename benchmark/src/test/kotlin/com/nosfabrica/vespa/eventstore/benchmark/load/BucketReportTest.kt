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
package com.nosfabrica.vespa.eventstore.benchmark.load

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BucketReportTest {
    @Test
    fun `uniform buckets split into equal runs`() {
        val events = LongArray(257) { if (it < 256) 100 else 0 }
        assertEquals("00-7f=0,80-ff=1", BucketReport.suggest(events, 2).spec())
        assertEquals(5, BucketReport.suggest(events, 5).clusters)
    }

    /** A skewed corpus: the first buckets hold most events, so cluster 0 takes few buckets and the rest share the tail. */
    @Test
    fun `a skewed corpus splits by events, not by buckets`() {
        val events =
            LongArray(257) { b ->
                if (b < 8) {
                    10_000
                } else if (b < 256) {
                    10
                } else {
                    0
                }
            }
        val map = BucketReport.suggest(events, 2)
        val cluster0 = map.bucketsOf(0)
        assertTrue(cluster0.size in 3..8, "cluster 0 holds the heavy head: $cluster0")
        val share0 = cluster0.sumOf { events[it] }.toDouble() / events.sum()
        assertTrue(share0 in 0.35..0.65, "about half the events each: $share0")
    }

    @Test
    fun `every cluster gets a bucket, even when one bucket holds everything`() {
        val events = LongArray(257).also { it[0] = 1_000_000 }
        for (k in listOf(2, 16, 256)) {
            val map = BucketReport.suggest(events, k)
            assertEquals(k, map.clusters)
            for (c in 0 until k) assertTrue(map.bucketsOf(c).isNotEmpty(), "cluster $c of $k")
        }
    }
}
