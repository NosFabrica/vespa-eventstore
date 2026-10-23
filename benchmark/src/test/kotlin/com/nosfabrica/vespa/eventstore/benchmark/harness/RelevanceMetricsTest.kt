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
import kotlin.math.ln
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * The measures `rankAb` reports, pinned against values worked by hand — so a
 * refactor cannot quietly change what "nDCG went up" means between two runs
 * someone is comparing.
 */
class RelevanceMetricsTest {
    private fun log2(v: Double) = ln(v) / ln(2.0)

    @Test
    fun `ndcg condenses unjudged hits away and builds the ideal from every judgment`() {
        // Served: 3, unjudged, 0, 2. Condensed: 3, 0, 2. The topic also judged
        // a grade-1 doc the engine never returned, so the ideal is 3, 2, 1, 0.
        val dcg = 7.0 + 0.0 / log2(3.0) + 3.0 / log2(4.0)
        val ideal = 7.0 + 3.0 / log2(3.0) + 1.0 / log2(4.0) + 0.0
        assertEquals(dcg / ideal, RelevanceMetrics.ndcg(listOf(3, null, 0, 2), listOf(3, 2, 1, 0))!!, 1e-12)
    }

    @Test
    fun `ndcg is undefined, not zero, for a topic with nothing relevant`() {
        assertNull(RelevanceMetrics.ndcg(listOf(0, null, 0), listOf(0, 0)))
        // ...and a perfect page is exactly 1.
        assertEquals(1.0, RelevanceMetrics.ndcg(listOf(3, 2, 1), listOf(1, 3, 2))!!, 1e-12)
    }

    @Test
    fun `ndcg cuts at k after condensing`() {
        // Eleven unjudged hits then the answer: condensed, it is rank 1.
        val page = List(11) { null } + listOf(3)
        assertEquals(1.0, RelevanceMetrics.ndcg(page, listOf(3), k = 10)!!, 1e-12)
        // which is exactly why judged@k must be read beside it
        assertEquals(0.0, RelevanceMetrics.judgedAt(page, 10)!!, 1e-12)
    }

    @Test
    fun `known-item measures read the uncondensed page`() {
        val page = listOf(null, 1, 3, 0)
        assertEquals(1.0 / 3, RelevanceMetrics.reciprocalRank(page, minGrade = 3), 1e-12)
        assertEquals(0.0, RelevanceMetrics.successAt(page, minGrade = 3, k = 2))
        assertEquals(1.0, RelevanceMetrics.successAt(page, minGrade = 3, k = 3))
        assertEquals(0.0, RelevanceMetrics.reciprocalRank(listOf(1, 2), minGrade = 3))
    }

    @Test
    fun `judged at k and author share`() {
        assertEquals(0.5, RelevanceMetrics.judgedAt(listOf(1, null, 0, null), 4)!!, 1e-12)
        assertNull(RelevanceMetrics.judgedAt(emptyList(), 10))
        assertEquals(0.75, RelevanceMetrics.maxAuthorShare(listOf("a", "a", "b", "a"))!!, 1e-12)
        assertNull(RelevanceMetrics.maxAuthorShare(emptyList()))
    }

    @Test
    fun `rbo is 1 for identical, 0 for disjoint, and top-weighted between`() {
        val a = listOf("a", "b", "c", "d")
        assertEquals(1.0, RelevanceMetrics.rbo(a, a), 1e-12)
        assertEquals(0.0, RelevanceMetrics.rbo(a, listOf("w", "x", "y", "z")), 1e-12)
        // Worked: X1=1, X2=1, X3=3 at p=0.9 → (0.1/0.9)(0.9 + 0.405 + 0.729) + 0.729.
        val expected = (0.1 / 0.9) * (0.9 + 0.81 / 2 + 0.729) + 0.729
        assertEquals(expected, RelevanceMetrics.rbo(listOf("a", "b", "c"), listOf("a", "c", "b")), 1e-12)
        // A swap at the top costs more than the same swap at the bottom.
        val top = RelevanceMetrics.rbo(a, listOf("b", "a", "c", "d"))
        val bottom = RelevanceMetrics.rbo(a, listOf("a", "b", "d", "c"))
        assert(top < bottom) { "top swap $top should score below bottom swap $bottom" }
    }

    @Test
    fun `rbo handles lists of unequal length and is symmetric`() {
        val short = listOf("a", "b")
        val long = listOf("a", "b", "c", "d")
        // The short list is a prefix of the long one: the extrapolation assumes
        // it would have continued in agreement, so RBO_ext is 1.
        assertEquals(1.0, RelevanceMetrics.rbo(short, long), 1e-12)
        val other = listOf("c", "a", "x")
        assertEquals(RelevanceMetrics.rbo(other, long), RelevanceMetrics.rbo(long, other), 1e-12)
    }
}
