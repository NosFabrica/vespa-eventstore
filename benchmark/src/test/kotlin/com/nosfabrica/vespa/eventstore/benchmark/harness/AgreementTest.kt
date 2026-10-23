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

class AgreementTest {
    @Test
    fun `identical grades agree perfectly`() {
        val r = Agreement.of(listOf(0 to 0, 1 to 1, 2 to 2, 3 to 3, 3 to 3))
        assertEquals(1.0, r.kappa, 1e-12)
        assertEquals(1.0, r.exact)
    }

    @Test
    fun `quadratic weighting worked by hand`() {
        // Marginals all 1 of 4, so E is 0.25 everywhere: Σ w·E = 0.25 · 40/9.
        // The two adjacent swaps cost Σ w·O = 2/9, so κ = 1 − (2/9)/(10/9) = 0.8.
        val r = Agreement.of(listOf(0 to 0, 1 to 1, 2 to 3, 3 to 2))
        assertEquals(0.8, r.kappa, 1e-12)
        assertEquals(0.5, r.exact, 1e-12)
        assertEquals(1.0, r.withinOne, 1e-12)
        assertEquals(1, r.matrix[2][3])
    }

    @Test
    fun `a far disagreement costs more than a near one`() {
        val near = Agreement.of(listOf(0 to 0, 1 to 1, 2 to 2, 3 to 2, 0 to 0, 3 to 3))
        val far = Agreement.of(listOf(0 to 0, 1 to 1, 2 to 2, 3 to 0, 0 to 0, 3 to 3))
        assertTrue(far.kappa < near.kappa, "far ${far.kappa} should be below near ${near.kappa}")
        assertTrue(Agreement.of(listOf(2 to 2, 2 to 2)).kappa.isNaN())
    }
}
