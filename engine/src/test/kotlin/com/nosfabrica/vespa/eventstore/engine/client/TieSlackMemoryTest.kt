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
    private val wide = VespaEventIndex.TIE_SLACK
    private val least = VespaEventIndex.MIN_TIE_SLACK

    private fun shape(kind: Int) = RecencyWindowMemory.shapeOf(EventQuery(kinds = listOf(kind), limit = 10), now, now)

    @Test
    fun `a cut goes back to at least the widest slack, and learns a wider group exactly`() {
        assertEquals(wide, TieSlackMemory.next(slack = least, over = 0, cut = true))
        // The [T,T] read counted 60 past the page: 2 * 60 + 3, so the next read holds it.
        assertEquals(123, TieSlackMemory.next(slack = wide, over = 60, cut = true))
        assertEquals(TieSlackMemory.MAX_LEARNED_SLACK, TieSlackMemory.next(slack = wide, over = 5_000, cut = true), "never past what one avoided query is worth")
    }

    @Test
    fun `a page asking for more raises the slack at once, with headroom`() {
        assertEquals(2 * 12 + least, TieSlackMemory.next(slack = 10, over = 12, cut = false))
        assertEquals(TieSlackMemory.MAX_LEARNED_SLACK, TieSlackMemory.next(slack = 10, over = 400, cut = false), "never past the learned cap")
    }

    @Test
    fun `a quiet page narrows by a quarter of the gap, never below the least`() {
        // Target 3 from 64: a quarter of the 61 gap.
        assertEquals(49, TieSlackMemory.next(slack = wide, over = 0, cut = false))
        assertEquals(8, TieSlackMemory.next(slack = 9, over = 0, cut = false), "at least one step")
        assertEquals(least, TieSlackMemory.next(slack = least + 1, over = 0, cut = false))
        assertEquals(least, TieSlackMemory.next(slack = least, over = 0, cut = false))
        // One quieter page on a busy shape does not open it to a cut: target 23, but 64 -> 54.
        assertEquals(54, TieSlackMemory.next(slack = wide, over = 10, cut = false))
    }

    @Test
    fun `a shape starts cold at the slack it is given and remembers what it learned`() {
        assertEquals(wide, memory.slackFor(shape(1)).value)
        assertEquals(12, memory.slackFor(shape(1), cold = 12).value, "a cold shape starts from the caller's evidence")
        memory.learn(shape(1), memory.slackFor(shape(1)), over = 0, cut = false)
        assertEquals(49, memory.slackFor(shape(1), cold = 12).value, "and a learned one ignores it")
        assertEquals(wide, memory.slackFor(shape(7)).value, "another shape is its own")
    }

    @Test
    fun `entries expire and the table is bounded`() {
        memory.learn(shape(1), memory.slackFor(shape(1)), over = 0, cut = false)
        clock += 1_001
        assertEquals(wide, memory.slackFor(shape(1)).value, "past the TTL a shape starts cold again")
        (1..3).forEach { memory.learn(shape(it), memory.slackFor(shape(it)), over = 0, cut = false) }
        assertEquals(2, memory.size)
        assertEquals(wide, memory.slackFor(shape(1)).value, "the least recently used shape was evicted")
    }

    /**
     * Two pages of one shape in flight: both leased the same slack, one was
     * cut, the other narrows and lands LAST. Last-writer-wins reopened the busy
     * shape to the cut it had just learned; a narrowing now lands only on the
     * entry it leased.
     */
    @Test
    fun `a narrowing that lands after a concurrent cut does not undo it`() {
        val shape = shape(1)
        memory.learn(shape, memory.slackFor(shape), over = 0, cut = false) // 49, a learned entry
        val a = memory.slackFor(shape)
        val b = memory.slackFor(shape)
        memory.learn(shape, a, over = 80, cut = true)
        val raised = memory.slackFor(shape).value
        assertEquals(2 * 80 + least, raised)
        memory.learn(shape, b, over = 0, cut = false)
        assertEquals(raised, memory.slackFor(shape).value, "the stale narrowing is dropped")

        // A raise computed from an older lease never lowers what is stored either.
        val c = memory.slackFor(shape)
        memory.learn(shape, c, over = 85, cut = true)
        memory.learn(shape, b, over = 20, cut = false) // a raise from 49 to 43? no: target 43 < 49, a stale narrowing
        assertEquals(2 * 85 + least, memory.slackFor(shape).value)
    }

    @Test
    fun `a selective shape starts at its page width, a busy one wide`() {
        assertEquals(wide, TieSlackMemory.coldSlack(EventQuery(kinds = listOf(1)), limit = 10), "a bare kind can be a firehose")
        assertEquals(10, TieSlackMemory.coldSlack(EventQuery(kinds = listOf(1), tags = mapOf("p" to listOf("a".repeat(64)))), limit = 10))
        assertEquals(least, TieSlackMemory.coldSlack(EventQuery(authors = listOf("a".repeat(64))), limit = 1))
        assertEquals(wide, TieSlackMemory.coldSlack(EventQuery(authors = listOf("a".repeat(64))), limit = 500))
        assertEquals(least, TieSlackMemory.measuredSlack(events = 3, windowSecs = 3_600), "three events an hour hold no ties")
        assertEquals(2 * 20 + least, TieSlackMemory.measuredSlack(events = 72_000, windowSecs = 3_600), "twenty a second")
    }
}
