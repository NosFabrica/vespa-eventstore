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
package com.nosfabrica.vespa.eventstore.trust

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * A drain delegating one service to a walk is FROZEN BY DESIGN — it cannot
 * retire the service until the walk returns. Calling that "STALLED" beside a
 * walk visibly advancing is the report crying wolf; the real signal is the
 * whole pipeline standing still.
 */
class TrustProgressStalledTest {
    private val now = 1_000_000_000L

    private fun step(
        op: String,
        frozenSec: Long,
        done: Long = 5,
    ) = TrustProgress.Step(op, "doing $op", done, 100, now - 3_600_000, now - frozenSec * 1000)

    @Test
    fun `a step waiting on one that is advancing is not called stalled`() {
        val line =
            TrustProgress.render(
                listOf(
                    step("trust-drain", frozenSec = TrustProgress.STALLED_AFTER_SEC * 12),
                    step("trust-service-walk", frozenSec = 0),
                ),
                now,
            )
        assertFalse(line.contains("STALLED"), "the walk is moving, so nothing is stuck: $line")
        assertTrue(line.contains("trust-drain"), "both steps are still reported: $line")
        assertTrue(line.contains("trust-service-walk"), "both steps are still reported: $line")
    }

    @Test
    fun `when nothing at all is moving every frozen step is called stalled`() {
        val line =
            TrustProgress.render(
                listOf(
                    step("trust-drain", frozenSec = TrustProgress.STALLED_AFTER_SEC * 12),
                    step("trust-service-walk", frozenSec = TrustProgress.STALLED_AFTER_SEC * 3),
                ),
                now,
            )
        assertTrue(line.contains("trust-drain"), line)
        assertEquals(2, Regex("STALLED").findAll(line).count(), "both are stuck and both must say so: $line")
    }

    @Test
    fun `a single frozen step with nothing beside it still reports stalled`() {
        val line = TrustProgress.render(listOf(step("trust-drain", frozenSec = TrustProgress.STALLED_AFTER_SEC + 5)), now)
        assertTrue(line.contains("STALLED"), "nothing else is running, so this one really is stuck: $line")
    }
}
