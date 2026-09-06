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
package com.nosfabrica.vespa.eventstore.search

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * WHAT MAY SHARE A QUERY — the grouping, asserted directly.
 *
 * This decides the round-trip count of every searching read that meets a
 * Trusted List, and it is wrong in two directions. Too eager, and members are
 * fetched under a pointer whose confidence is not theirs; too shy, and a
 * ranked page of lists becomes one engine query per list, on the read path
 * whose latency is the thing being optimized.
 *
 * It could only be observed through a whole page before — a count of queries
 * inferred from a spy index — which is why the rules below were argued in
 * comments and asserted nowhere.
 */
class SubjectBatchesTest {
    private val limits = SearchExpansionLimits.Default

    private fun key(n: Int) = n.toString(16).padStart(64, '0')

    /** One page's rows, all under lens 0, with the scores the caller would have read off them. */
    private fun group(
        rows: List<References>,
        rel: List<Double> = rows.map { 0.0 },
        text: List<Double> = rows.map { 0.0 },
        limits: SearchExpansionLimits = this.limits,
    ) = SubjectBatches(limits).group(rows, IntArray(rows.size), 0, { rel[it] }, { text[it] })

    @Test
    fun `references no pointer scored all share one query`() {
        val rows =
            listOf(
                References(eventIds = listOf(key(1), key(2))),
                References(eventIds = listOf(key(3))),
            )
        val batches = group(rows, rel = listOf(9.0, 3.0), text = listOf(2.0, 1.0)).ids

        assertEquals(1, batches.size, "an unscored lookup sends the caller's lens untouched, so nothing forces a split")
        assertEquals(listOf(key(1), key(2), key(3)), batches[0].keys.keys.toList())
        assertTrue(!batches[0].weighted)
    }

    /**
     * With the floor span on, the pointer's own relevance AND text band travel
     * with the query, so two pointers that earned different numbers cannot
     * share one — their members are placed by those numbers.
     */
    @Test
    fun `scored references split by the pointer scores their query carries`() {
        val rows =
            listOf(
                References(eventIds = listOf(key(1)), confidence = mapOf(key(1) to 90)),
                References(eventIds = listOf(key(2)), confidence = mapOf(key(2) to 40)),
                References(eventIds = listOf(key(3)), confidence = mapOf(key(3) to 70)),
            )
        // rows 0 and 2 earned the same pair; row 1 did not.
        val batches = group(rows, rel = listOf(9.0, 3.0, 9.0), text = listOf(2.0, 1.0, 2.0)).ids

        assertEquals(2, batches.size, "same relevance AND same text band is the same query")
        assertEquals(listOf(key(1), key(3)), batches[0].keys.keys.toList())
        assertEquals(listOf(key(2)), batches[1].keys.keys.toList())
        assertEquals(mapOf(key(1) to 90, key(3) to 70), batches[0].keys, "each key still carries ITS pointer's confidence")
    }

    /**
     * BOTH numbers are the identity, not just the relevance. Two pointers can
     * arrive at the same relevance having earned it differently — one on a
     * title match under a weakly-trusted signer, one on a weaker match under a
     * better-trusted one — and their members are placed by the TEXT half. A
     * shared query would hand one pointer's band to the other's members.
     */
    @Test
    fun `pointers that share a relevance but not a text band do not share a query`() {
        val rows =
            listOf(
                References(eventIds = listOf(key(1)), confidence = mapOf(key(1) to 90)),
                References(eventIds = listOf(key(2)), confidence = mapOf(key(2) to 90)),
            )
        val batches = group(rows, rel = listOf(9.0, 9.0), text = listOf(2.0, 1.0)).ids

        assertEquals(2, batches.size, "same relevance, different text band: two queries")
        assertEquals(2.0, batches[0].pointerText)
        assertEquals(1.0, batches[1].pointerText)
    }

    /**
     * The same page with the floor off is ONE query: `withWeightedMember` does
     * not send the relevance then, so splitting on it would buy identical
     * queries — the shy direction, and it costs a round trip per pointer.
     */
    @Test
    fun `with no floor span the same page is a single query`() {
        val rows =
            listOf(
                References(eventIds = listOf(key(1)), confidence = mapOf(key(1) to 90)),
                References(eventIds = listOf(key(2)), confidence = mapOf(key(2) to 40)),
            )
        val batches = group(rows, rel = listOf(9.0, 3.0), text = listOf(2.0, 1.0), limits = limits.copy(subjectFloorSpan = null)).ids

        assertEquals(1, batches.size, "identical queries must not be split by a number none of them sends")
        assertEquals(mapOf(key(1) to 90, key(2) to 40), batches[0].keys)
    }

    /**
     * A key two pointers name is fetched under the FIRST row that claims it —
     * page order, which on any sortable page is relevance order, and the same
     * pointer the placement will file it under.
     */
    @Test
    fun `a contested key belongs to the better-ranked pointer`() {
        val shared = key(7)
        val rows =
            listOf(
                References(eventIds = listOf(shared), confidence = mapOf(shared to 90)),
                References(eventIds = listOf(shared, key(8)), confidence = mapOf(shared to 10, key(8) to 10)),
            )
        val batches = group(rows, rel = listOf(9.0, 3.0), text = listOf(2.0, 1.0)).ids

        assertEquals(listOf(shared), batches[0].keys.keys.toList())
        assertEquals(90, batches[0].keys[shared], "under the confidence the winning pointer gave it")
        assertEquals(listOf(key(8)), batches[1].keys.keys.toList(), "the loser keeps only what nobody claimed")
    }

    /**
     * Scored and unscored halves of ONE row cannot share a lookup: a scored
     * member is ranked on the member rung by the number its list gave it, while
     * a reference expressing no confidence must come back with no member score
     * at all so the placement can hand it the pointer's own.
     */
    @Test
    fun `one row's scored and unscored references become separate queries`() {
        val rows = listOf(References(eventIds = listOf(key(1), key(2)), confidence = mapOf(key(1) to 90)))
        val batches = group(rows, rel = listOf(9.0), text = listOf(2.0)).ids

        assertEquals(2, batches.size)
        assertTrue(batches[0].weighted, "the scored half goes first")
        assertEquals(listOf(key(1)), batches[0].keys.keys.toList())
        assertTrue(!batches[1].weighted)
        assertEquals(listOf(key(2)), batches[1].keys.keys.toList())
    }

    @Test
    fun `ids and pubkeys are grouped apart`() {
        val rows = listOf(References(eventIds = listOf(key(1)), pubKeys = listOf(key(2))))
        val grouped = group(rows)

        assertEquals(
            listOf(key(1)),
            grouped.ids
                .single()
                .keys.keys
                .toList(),
        )
        assertEquals(
            listOf(key(2)),
            grouped.pubKeys
                .single()
                .keys.keys
                .toList(),
        )
    }

    /**
     * Address buckets run UNSCORED FIRST, then descending confidence, and the
     * order is load-bearing: the caller files found subjects with `putIfAbsent`.
     * An unscored reference is not a doubted one, and between two publishers the
     * reader delegated to, the generous reading wins. Sorting ascending handed
     * every contested member to the publisher that doubted it most.
     */
    @Test
    fun `address buckets run unscored first, then most confident`() {
        val rows =
            listOf(
                References(addresses = listOf("30023:a:low"), confidence = mapOf("30023:a:low" to 20)),
                References(addresses = listOf("30023:a:none")),
                References(addresses = listOf("30023:a:high"), confidence = mapOf("30023:a:high" to 95)),
            )
        val buckets = SubjectBatches(limits).addressBuckets(rows, IntArray(rows.size), 0)

        assertEquals(listOf(null, 1.0, 0.25), buckets.map { it.first }, "unscored, then 0.95 and 0.20 quantized, descending")
        assertEquals(listOf("30023:a:none"), buckets[0].second.toList())
        assertEquals(listOf("30023:a:high"), buckets[1].second.toList())
    }
}
