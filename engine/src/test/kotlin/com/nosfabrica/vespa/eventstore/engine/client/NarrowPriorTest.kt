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
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** How [NarrowPrior] counts a family's votes, and what [RecencyWindowMemory.familyOf] keys a family by. */
class NarrowPriorTest {
    private var clock = 0L
    private val prior = NarrowPrior(capacity = 3, ttlMillis = 1_000, clock = { clock })
    private val now = 1_800_000_000L

    private fun mentions(vararg keys: String) = EventQuery(kinds = listOf(1), tags = mapOf("p" to keys.toList()), limit = 10)

    private fun family(q: EventQuery) = RecencyWindowMemory.familyOf(q, now, now)!!

    @Test
    fun `a family is its shape with the values counted, not named`() {
        assertEquals(family(mentions("a".repeat(64))), family(mentions("b".repeat(64))))
        assertEquals(family(mentions("a", "b", "c")), family(mentions("d", "e", "f", "g")), "3 and 4 values share a bucket")
        assertNotEquals(family(mentions("a")), family(mentions("a", "b")))
        assertNotEquals(family(mentions("a")), family(EventQuery(kinds = listOf(1), tags = mapOf("e" to listOf("a")), limit = 10)), "the tag name stays")
        assertNotEquals(family(mentions("a")), family(EventQuery(kinds = listOf(7), tags = mapOf("p" to listOf("a")), limit = 10)), "the kinds stay")
        val gated = mentions("a").copy(ranking = EventYql.RANK_RECENCY_GATED, rankKey = "f".repeat(64), minRank = 2.0)
        assertNotEquals(family(mentions("a")), family(gated), "a gated read fills at its trusted rate")
        assertEquals(family(gated), family(gated.copy(rankKey = "e".repeat(64), tags = mapOf("p" to listOf("z")))), "any observer, any value: one family")
        assertNotEquals(family(mentions("a")), RecencyWindowMemory.familyOf(mentions("a"), now - 30 * 86_400L, now), "a deep page is another family")
    }

    @Test
    fun `a read with no values to count, or with search terms, has no family`() {
        assertNull(RecencyWindowMemory.familyOf(EventQuery(kinds = listOf(1), limit = 10), now, now))
        assertNull(RecencyWindowMemory.familyOf(mentions("a").copy(search = "bitcoin", ranking = EventYql.RANK_RECENCY_GATED, minRank = 2.0), now, now))
    }

    @Test
    fun `a family votes narrow only with enough samples, almost all narrow`() {
        val f = family(mentions("a"))
        repeat(NarrowPrior.MIN_SAMPLES - 1) { prior.record(f, narrow = true) }
        assertFalse(prior.isNarrow(f), "below the sample floor")
        prior.record(f, narrow = true)
        assertTrue(prior.isNarrow(f))
        prior.record(f, narrow = false)
        assertFalse(prior.isNarrow(f), "8 of 9 is under 90%")
    }

    @Test
    fun `recent votes outweigh old ones`() {
        val f = family(mentions("a"))
        repeat(NarrowPrior.WINDOW) { prior.record(f, narrow = false) }
        repeat(NarrowPrior.WINDOW * 4) { prior.record(f, narrow = true) }
        assertTrue(prior.isNarrow(f), "a family that turned narrow is believed once its old busy votes decayed")
        repeat(4) { prior.record(f, narrow = false) }
        assertFalse(prior.isNarrow(f), "and a few busy values take it back")
    }

    @Test
    fun `votes expire, and the table is bounded`() {
        val f = family(mentions("a"))
        repeat(NarrowPrior.MIN_SAMPLES) { prior.record(f, narrow = true) }
        clock += 1_001
        assertFalse(prior.isNarrow(f), "expired")
        val families = listOf(1, 2, 3, 5).map { family(mentions(*Array(it) { i -> "k$i" })) }.distinct()
        assertEquals(4, families.size)
        families.forEach { fam -> prior.record(fam, narrow = true) }
        assertEquals(3, prior.size)
    }
}
