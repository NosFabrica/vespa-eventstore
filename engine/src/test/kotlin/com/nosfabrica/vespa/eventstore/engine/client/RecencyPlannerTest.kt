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
package com.nosfabrica.vespa.eventstore.engine.client

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The arithmetic [RecencyStrategy.SPECULATIVE] steers by. The strategy's
 * end-to-end behavior (same page, how many engine queries) is pinned in
 * VespaEventIndexTest and RecencyStrategyIT; this pins the two numbers that
 * decide WHEN it widens and when it gives up.
 */
class RecencyPlannerTest {
    @Test
    fun `a window with no rate projects past every horizon`() {
        // Zero, and one — a single event is not a rate (the Poisson discount).
        assertEquals(Long.MAX_VALUE, RecencyPlanner.projected(3_600, got = 0, limit = 200))
        assertEquals(Long.MAX_VALUE, RecencyPlanner.projected(3_600, got = 1, limit = 200))
        assertTrue(RecencyPlanner.projected(3_600, got = 1, limit = 200) > RecencyPlanner.NARROW_HORIZON)
    }

    @Test
    fun `a busy window projects close to its raw rate`() {
        // 100 of 500 in an hour: raw 5h, discounted by 10% of the count -> 5.56h.
        val projected = RecencyPlanner.projected(3_600, got = 100, limit = 500)
        assertEquals(20_000L, projected)
        assertTrue(projected < RecencyPlanner.NARROW_HORIZON)
    }

    @Test
    fun `the measured thin read gives up instead of speculating`() {
        // 50 quiet authors (2026-09-26): 1 event in the first hour of a limit-200
        // read. The raw rate projected 8 days and speculated; discounted, it stops.
        assertTrue(RecencyPlanner.projected(3_600, got = 1, limit = 200) > RecencyPlanner.NARROW_HORIZON)
        // Three events project ~6.6 days at a pessimistic ~1.27/h: still worth one more window.
        assertTrue(RecencyPlanner.projected(3_600, got = 3, limit = 200) < RecencyPlanner.NARROW_HORIZON)
    }

    @Test
    fun `widening never creeps`() {
        assertEquals(8 * 3_600L, RecencyPlanner.widen(3_600, got = 49, limit = 50), "a nearly-full window still jumps 8x")
        assertEquals(800 * 3_600L, RecencyPlanner.widen(3_600, got = 1, limit = 200), "a thin one jumps by four times its rate")
        assertEquals(800 * 3_600L, RecencyPlanner.widen(3_600, got = 0, limit = 200), "an empty one as if it held one")
    }
}
