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
import kotlin.test.assertNotEquals
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

    /**
     * A gated `sort:recent` search with TERMS is windowable, and its rate is the
     * term's: a rare word learning Narrow must not make a busy one under the same
     * observer skip its speculation (the shipped gated read it then pays is the
     * class KDoc's ~455x slower one), nor a phrase share a slot with plain words.
     */
    @Test
    fun `different search terms or phrases are different shapes`() {
        fun gated(
            search: String?,
            phrases: List<String> = emptyList(),
        ) = EventQuery(kinds = listOf(1), search = search, phrases = phrases, limit = 50, ranking = EventYql.RANK_RECENCY_GATED, rankKey = bob)
        memory.remember(gated("zyzzogeton"), now, now, Recall.Narrow)
        assertNull(memory.recall(gated("bitcoin"), now, now), "a busy term is not the rare term's shape")
        assertNull(memory.recall(gated("zyzzogeton", phrases = listOf("zyzzogeton facts")), now, now), "a phrase narrows the shape")
        assertNull(memory.recall(gated(null), now, now), "no terms is the gated feed, another shape")
        assertEquals(Recall.Narrow, memory.recall(gated("zyzzogeton"), now, now))
        val shape = RecencyWindowMemory.shapeOf(gated("zyzzogeton"), now, now)
        assertEquals(Recall.Narrow, memory.recall(shape), "a shape computed once answers like the query it came from")
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

    /**
     * The key FRAMES every value: a tag value, a tag name or a search term is free text, and
     * joined with ", " a one-value filter could key exactly like a two-value one — a cheap REQ
     * matching nothing then taught a busy shape Narrow.
     */
    @Test
    fun `values that join alike are different shapes`() {
        val now = 1_800_000_000L

        fun shape(q: EventQuery) = RecencyWindowMemory.shapeOf(q, now, now)
        assertNotEquals(
            shape(EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf("bitcoin, nostr")))),
            shape(EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf("bitcoin", "nostr")))),
        )
        assertNotEquals(
            shape(EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf("a"), "u" to listOf("b")))),
            shape(EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf("a], u=[b")))),
        )
    }

    /** The key no longer sorts or copies its lists, and is still blind to their order and to hex case. */
    @Test
    fun `a shape is the same whatever the order of its lists and the case of its keys`() {
        val now = 1_800_000_000L
        val authors = (1..1_000).map { it.toString(16).padStart(64, 'a') }

        fun shape(q: EventQuery) = RecencyWindowMemory.shapeOf(q, now, now)
        assertEquals(
            shape(EventQuery(kinds = listOf(1, 6), authors = authors, tags = mapOf("t" to listOf("x", "y"), "p" to listOf("z")))),
            shape(EventQuery(kinds = listOf(6, 1), authors = authors.reversed().map { it.uppercase() }, tags = mapOf("p" to listOf("z"), "t" to listOf("y", "x")))),
        )
        assertNotEquals(shape(EventQuery(kinds = listOf(1), authors = authors)), shape(EventQuery(kinds = listOf(1), authors = authors.drop(1))))
        assertNotEquals(shape(EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf("x")))), shape(EventQuery(kinds = listOf(1), tagsAll = mapOf("t" to listOf("x")))))
    }

    /** The tie slack lives in the window table: one LRU bound, and forgetting a window keeps the slack. */
    @Test
    fun `the tie slack shares the window table and outlives a forgotten window`() {
        val now = 1_800_000_000L
        val table = RecencyWindowMemory(capacity = 8, ttlMillis = 60_000)
        val slack = TieSlackMemory(table)
        val shape = RecencyWindowMemory.shapeOf(EventQuery(kinds = listOf(1), limit = 10), now, now)
        table.remember(shape, Recall.Window(3_600, 10))
        slack.learn(shape, slack.slackFor(shape), over = 0, cut = false)
        assertEquals(1, table.size, "one entry for both")
        val (window, lease) = table.recallWith(shape, VespaEventIndex.TIE_SLACK)
        assertEquals(Recall.Window(3_600, 10), window)
        assertEquals(49, lease.value)
        table.forget(shape)
        assertNull(table.recall(shape))
        assertEquals(49, slack.slackFor(shape).value, "the slack the shape learned is not the window's to drop")
    }
}
