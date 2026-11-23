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

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlin.test.Test
import kotlin.test.assertEquals

/** How [TieSlackMemory] moves a shape's tie slack, and how much it remembers. */
class TieSlackMemoryTest {
    private var clock = 0L
    private val memory = TieSlackMemory(capacity = 2, ttlMillis = 1_000, clock = { clock })
    private val now = 1_800_000_000L

    private fun shape(kind: Int) = RecencyWindowMemory.shapeOf(EventQuery(kinds = listOf(kind), limit = 10), now, now)

    @Test
    fun `a cut goes straight back to the widest slack`() {
        assertEquals(VespaEventIndex.TIE_SLACK, TieSlackMemory.next(slack = VespaEventIndex.MIN_TIE_SLACK, over = 0, cut = true))
    }

    @Test
    fun `a page asking for more raises the slack at once, with headroom`() {
        assertEquals(2 * 12 + VespaEventIndex.MIN_TIE_SLACK, TieSlackMemory.next(slack = 10, over = 12, cut = false))
        assertEquals(VespaEventIndex.TIE_SLACK, TieSlackMemory.next(slack = 10, over = 40, cut = false), "never past the widest")
    }

    @Test
    fun `a quiet page narrows by a quarter of the gap, never below the least`() {
        // Target 8 from 64: a quarter of the 56 gap.
        assertEquals(50, TieSlackMemory.next(slack = VespaEventIndex.TIE_SLACK, over = 0, cut = false))
        assertEquals(VespaEventIndex.MIN_TIE_SLACK, TieSlackMemory.next(slack = 9, over = 0, cut = false), "at least one step")
        assertEquals(VespaEventIndex.MIN_TIE_SLACK, TieSlackMemory.next(slack = VespaEventIndex.MIN_TIE_SLACK, over = 0, cut = false))
        // One quieter page on a busy shape does not open it to a cut: target 28, but 64 -> 55.
        assertEquals(55, TieSlackMemory.next(slack = VespaEventIndex.TIE_SLACK, over = 10, cut = false))
    }

    @Test
    fun `a shape starts cold at the widest slack and remembers what it learned`() {
        assertEquals(VespaEventIndex.TIE_SLACK, memory.slackFor(shape(1)))
        memory.learn(shape(1), slack = VespaEventIndex.TIE_SLACK, over = 0, cut = false)
        assertEquals(50, memory.slackFor(shape(1)))
        assertEquals(VespaEventIndex.TIE_SLACK, memory.slackFor(shape(7)), "another shape is its own")
    }

    @Test
    fun `entries expire and the table is bounded`() {
        memory.learn(shape(1), slack = VespaEventIndex.TIE_SLACK, over = 0, cut = false)
        clock += 1_001
        assertEquals(VespaEventIndex.TIE_SLACK, memory.slackFor(shape(1)), "past the TTL a shape starts cold again")
        (1..3).forEach { memory.learn(shape(it), slack = VespaEventIndex.TIE_SLACK, over = 0, cut = false) }
        assertEquals(2, memory.size)
        assertEquals(VespaEventIndex.TIE_SLACK, memory.slackFor(shape(1)), "the least recently used shape was evicted")
    }
}
