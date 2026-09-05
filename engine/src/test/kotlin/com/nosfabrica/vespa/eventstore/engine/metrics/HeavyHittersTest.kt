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

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The SPACE-SAVING GUARANTEE, which is the whole reason this is allowed to
 * exist beside a cardinality rule that forbids keying counters by pubkey.
 *
 * The bargain is: fixed memory, and in exchange the answer is approximate in a
 * BOUNDED way — anything holding more than 1/capacity of total weight is
 * guaranteed present, and every entry says how much it might be overstating.
 * If that does not hold, "which observer is costing me most" is not an answer,
 * it is a guess with a confident face.
 */
class HeavyHittersTest {
    private fun key(i: Int) = "%064x".format(i)

    @Test
    fun `a genuine heavy hitter is never evicted, however long the tail`() {
        val sketch = HeavyHitters(capacity = 16)
        val rng = Random(11)
        // One whale, then a very long tail of one-offs — the shape that breaks
        // a naive "keep the last N" cache.
        repeat(50_000) {
            if (it % 10 == 0) sketch.add("whale", 100) else sketch.add(key(rng.nextInt(100_000)), 1)
        }
        val top = sketch.top(5)
        assertEquals("whale", top.first().key, "the heaviest key must survive an unbounded tail")
        assertTrue(top.first().weight >= 500_000, "and keep roughly its true weight, got ${top.first().weight}")
    }

    @Test
    fun `weight ranks by cost, not by how often a key appears`() {
        val sketch = HeavyHitters(capacity = 8)
        // The distinction the design turns on: one expensive search must
        // outrank a thousand cheap id lookups. A frequency sketch ranks these
        // the other way round.
        repeat(1_000) { sketch.add("chatty", 1) }
        repeat(10) { sketch.add("expensive", 1_000) }
        assertEquals("expensive", sketch.top(1).first().key)
    }

    @Test
    fun `every reported weight is an over-estimate bounded by its own error`() {
        // The formal guarantee: true <= reported, and reported - error <= true.
        // A reader subtracting `error` gets a floor they can trust.
        val sketch = HeavyHitters(capacity = 24)
        val rng = Random(5)
        val truth = HashMap<String, Long>()
        repeat(40_000) {
            val k = if (rng.nextInt(100) < 70) key(rng.nextInt(10)) else key(rng.nextInt(20_000))
            val w = rng.nextLong(1, 50)
            sketch.add(k, w)
            truth[k] = (truth[k] ?: 0) + w
        }
        for (hit in sketch.top(24)) {
            val real = truth[hit.key] ?: 0L
            assertTrue(hit.weight >= real, "${hit.key} reported ${hit.weight} below its true $real — must never under-count")
            assertTrue(
                hit.weight - hit.error <= real,
                "${hit.key}: floor ${hit.weight - hit.error} exceeds true $real, so the error bound is a lie",
            )
        }
    }

    @Test
    fun `the top of a skewed stream is exactly right`() {
        val sketch = HeavyHitters(capacity = 64)
        val rng = Random(3)
        val truth = HashMap<String, Long>()
        repeat(60_000) {
            // 80/20: twenty heavy keys, a 50k-key tail — a relay's real shape.
            val k = if (rng.nextInt(100) < 80) key(rng.nextInt(20)) else key(1_000 + rng.nextInt(50_000))
            sketch.add(k, 10)
            truth[k] = (truth[k] ?: 0) + 10
        }
        val expected =
            truth.entries
                .sortedByDescending { it.value }
                .take(10)
                .map { it.key }
                .toSet()
        val got = sketch.top(10).map { it.key }.toSet()
        assertEquals(expected, got, "the ten heaviest keys should be recovered exactly at this skew")
    }

    @Test
    fun `capacity is the hard bound on what it retains`() {
        val sketch = HeavyHitters(capacity = 12)
        repeat(10_000) { sketch.add(key(it), 1) }
        assertTrue(sketch.top(1_000).size <= 12, "a sketch must never grow past its capacity — that is the entire point")
    }

    @Test
    fun `zero and negative weights are ignored rather than corrupting the ranking`() {
        val sketch = HeavyHitters(capacity = 4)
        sketch.add("a", 0)
        sketch.add("b", -5)
        assertTrue(sketch.top(4).isEmpty())
    }

    @Test
    fun `concurrent adds lose no weight`() {
        val sketch = HeavyHitters(capacity = 32)
        val threads = 8
        val each = 5_000
        val pool =
            java.util.concurrent.Executors
                .newFixedThreadPool(threads)
        val done = java.util.concurrent.CountDownLatch(threads)
        repeat(threads) {
            pool.submit {
                try {
                    repeat(each) { sketch.add("shared", 2) }
                } finally {
                    done.countDown()
                }
            }
        }
        done.await()
        pool.shutdown()
        assertEquals((threads * each * 2).toLong(), sketch.top(1).first().weight)
    }
}
