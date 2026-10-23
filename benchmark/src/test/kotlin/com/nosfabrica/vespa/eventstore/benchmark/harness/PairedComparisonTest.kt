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
package com.nosfabrica.vespa.eventstore.benchmark.harness

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PairedComparisonTest {
    @Test
    fun `identical configs are exchangeable`() {
        val s = listOf(0.1, 0.5, 0.9, 0.3)
        val r = PairedComparison.compare(s, s)
        assertEquals(1.0, r.p)
        assertEquals(0.0, r.meanDelta)
        assertEquals(4, r.ties)
    }

    @Test
    fun `the exact test enumerates every sign assignment`() {
        // Five topics, every one improved by 0.1: only the all-plus and
        // all-minus assignments reach |sum| = 0.5, so p = 2 / 2^5.
        val base = List(5) { 0.5 }
        val r = PairedComparison.compare(base, base.map { it + 0.1 })
        assertEquals(2.0 / 32, r.p, 1e-12)
        assertEquals(5, r.wins)
        assertEquals(0.1, r.meanDelta, 1e-12)
        assertEquals(0.1, r.ciLow, 1e-9)
        assertEquals(0.1, r.ciHigh, 1e-9)
    }

    @Test
    fun `the sampled test agrees on a clear effect and is reproducible`() {
        val base = List(40) { (it % 7) / 10.0 }
        val cand = base.mapIndexed { i, v -> v + if (i % 4 == 0) -0.02 else 0.05 }
        val r = PairedComparison.compare(base, cand)
        assertTrue(r.p < 0.001, "p=${r.p}")
        assertTrue(r.ciLow > 0.0)
        assertEquals(30, r.wins)
        assertEquals(10, r.losses)
        assertEquals(r, PairedComparison.compare(base, cand))
    }

    @Test
    fun `holm adjusts step-down and keeps the input order`() {
        val adj = PairedComparison.holm(listOf(0.04, 0.01, 0.03))
        // sorted 0.01, 0.03, 0.04 → 0.03, 0.06, max(0.06, 0.04) = 0.06
        assertEquals(listOf(0.06, 0.03, 0.06), adj.map { Math.round(it * 1e9) / 1e9 })
    }

    @Test
    fun `minimum detectable delta shrinks with the square root of the topic count`() {
        val at25 = PairedComparison.minimumDetectable(0.1, 25)
        val at100 = PairedComparison.minimumDetectable(0.1, 100)
        assertEquals(0.056, at25, 0.001)
        assertEquals(at25 / 2, at100, 1e-12)
    }
}
