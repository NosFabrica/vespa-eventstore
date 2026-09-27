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

import com.nosfabrica.vespa.eventstore.engine.client.RecencyWindowMemory.Recall
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** What [RecencyWindowMemory] keys a shape by, how long it remembers, and how much. */
class RecencyWindowMemoryTest {
    private var clock = 0L
    private val memory = RecencyWindowMemory(capacity = 3, ttlMillis = 1_000, clock = { clock })
    private val now = 1_800_000_000L
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)

    @Test
    fun `a shape is the same shape whatever its window, limit or order of values`() {
        memory.remember(EventQuery(kinds = listOf(1, 7), authors = listOf(alice, bob), limit = 50), now, now, Recall.Window(3_600, 50))
        val sameShape = EventQuery(kinds = listOf(7, 1), authors = listOf(bob, alice.uppercase()), since = now - 10, until = now, limit = 500)
        assertEquals(Recall.Window(3_600, 50), memory.recall(sameShape, now, now))
    }

    @Test
    fun `different authors, tags or lens are different shapes`() {
        memory.remember(EventQuery(kinds = listOf(1), authors = listOf(alice), limit = 50), now, now, Recall.Narrow)
        assertNull(memory.recall(EventQuery(kinds = listOf(1), authors = listOf(bob), limit = 50), now, now))
        assertNull(memory.recall(EventQuery(kinds = listOf(1), authors = listOf(alice), tags = mapOf("t" to listOf("x")), limit = 50), now, now))
        val gated = EventQuery(kinds = listOf(1), authors = listOf(alice), limit = 50, ranking = EventYql.RANK_RECENCY_GATED, rankKey = bob, minRank = 2.0)
        assertNull(memory.recall(gated, now, now), "a gated read fills at its trusted rate, not the raw one")
    }

    @Test
    fun `a deep anchor is a different shape from the newest page`() {
        val q = EventQuery(kinds = listOf(1), limit = 50)
        memory.remember(q, now, now, Recall.Window(3_600, 50))
        assertNull(memory.recall(q, now - 90 * 86_400L, now), "a page 90 days back does not fill at the newest page's rate")
        assertEquals(Recall.Window(3_600, 50), memory.recall(q, now - 3_600, now), "within a day of the clock is the same bucket")
    }

    @Test
    fun `entries expire`() {
        val q = EventQuery(kinds = listOf(1), limit = 50)
        memory.remember(q, now, now, Recall.Narrow)
        clock += 1_001
        assertNull(memory.recall(q, now, now), "past the TTL a shape is re-probed")
    }

    @Test
    fun `the table is bounded, least recently used out first`() {
        val shapes = (1..4).map { EventQuery(kinds = listOf(it), limit = 50) }
        shapes.take(3).forEach { memory.remember(it, now, now, Recall.Narrow) }
        memory.recall(shapes[0], now, now) // touch kind 1: kind 2 is now the eldest
        memory.remember(shapes[3], now, now, Recall.Narrow)
        assertEquals(3, memory.size)
        assertNull(memory.recall(shapes[1], now, now), "the least recently used shape was evicted")
        assertEquals(Recall.Narrow, memory.recall(shapes[0], now, now))
    }

    @Test
    fun `age buckets double`() {
        assertEquals(0, RecencyWindowMemory.ageBucket(0))
        assertEquals(0, RecencyWindowMemory.ageBucket(86_399))
        assertEquals(1, RecencyWindowMemory.ageBucket(86_400))
        assertEquals(2, RecencyWindowMemory.ageBucket(3 * 86_400L))
        assertEquals(8, RecencyWindowMemory.ageBucket(180 * 86_400L))
    }
}
