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

import com.nosfabrica.vespa.eventstore.engine.LinkScore
import com.nosfabrica.vespa.eventstore.engine.LinkTarget
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.trust.Enrolment
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The `sort:links` rerank: the token, the curve, and what [PageAssembly] does
 * with it — widen the recall, rescore the head by trusted links, cut back to
 * the asked size — against a scripted engine whose scores and link counts are
 * known. That the real engine's counts are gated is `TrustedLinksIT`'s job.
 */
class LinkRerankTest {
    private val rerank = LinkRerank()

    @Test
    fun `the token asks, its argument sets the strength, anything else is not a request`() {
        assertEquals(0.5, rerank.requested(listOf(Filter(search = "bitcoin sort:links"))))
        assertEquals(0.25, rerank.requested(listOf(Filter(search = "bitcoin sort:links:0.25 observer:${"e".repeat(64)}"))))
        assertEquals(0.5, rerank.requested(listOf(Filter(search = "bitcoin sort:links:lots"))), "an unreadable strength falls back to the default")
        assertEquals(0.5, rerank.requested(listOf(Filter(search = "bitcoin sort:links:-1"))))
        assertNull(rerank.requested(listOf(Filter(search = "bitcoin sort:rank"))))
        assertNull(rerank.requested(listOf(Filter(search = "bitcoin"), Filter(kinds = listOf(1)))))
    }

    @Test
    fun `the multiplier grows with diminishing returns up to the cap and mirrors for negative engagement`() {
        assertEquals(1.0, LinkRerank.multiplier(0.0, 0.5))
        assertEquals(2.0, LinkRerank.multiplier(3.0, 0.5), 1e-12)
        assertEquals(3.0, LinkRerank.multiplier(8.0, 0.5), 1e-12, "the ceiling is reached at 8")
        assertEquals(3.0, LinkRerank.multiplier(99.0, 0.5), 1e-12, "and nothing passes it")
        assertEquals(10.0, LinkRerank.multiplier(99.0, 0.5, maxBoost = 100.0), 1e-12, "the curve under a looser cap")
        assertEquals(0.5, LinkRerank.multiplier(-3.0, 0.5), 1e-12)
        assertEquals(1.0 / 3, LinkRerank.multiplier(-99.0, 0.5), 1e-12, "the floor is the ceiling's reciprocal")
        assertEquals(1.0, LinkRerank.multiplier(42.0, 0.0), "strength 0 is the engine's order")
        assertEquals(-1.0, LinkRerank.DEFAULT_WEIGHTS.of(LinkRerank.REPORT_KIND), "a report hurts by default")
        assertEquals(1.0, LinkRerank.DEFAULT_WEIGHTS.of(7))
    }

    /** A scripted engine: [scores] in engine order, [engagement] per hit id, and a record of what it was asked. */
    private class Engine(
        val scores: List<Pair<String, Double>>,
        val engagement: Map<String, Double>,
    ) {
        val limits = ArrayList<Int?>()
        val linkCalls = ArrayList<Pair<List<LinkTarget>, EventQuery>>()

        fun projection() =
            PageAssembly.Projection<String>(
                newestFirst = compareBy { it },
                keys = SubjectKeys(idOf = { it }, authorOf = { "author" }, addressOf = { null }),
                idOf = { it },
                kindOf = { 1 },
                authorOf = { "author" },
                pointerOf = { null },
                search = { q -> recall(q).map { it.hit } },
                searchRanked = { q -> recall(q) },
                scoreLinks = { targets, linkers, _ ->
                    linkCalls += targets to linkers
                    targets.mapNotNull { t -> engagement[t.id]?.let { t.id to LinkScore(it, emptyMap(), 1) } }.toMap()
                },
            )

        private fun recall(q: EventQuery): List<Ranked<String>> {
            limits += q.limit
            return scores.take(q.limit ?: scores.size).map { (id, s) -> Ranked(id, s) }
        }
    }

    private val lens = "5e".repeat(32)

    private fun query(
        rankKey: String? = lens,
        limit: Int = 2,
    ) = EventQuery(search = "zebra", observer = "c".repeat(64), rankKey = rankKey, minRank = 2.0, limit = limit)

    private fun pages() = PageAssembly(SearchExpansionLimits(enabled = false), null, { Enrolment.NONE }, rerank)

    private val engine = { Engine(listOf("a" to 10.0, "b" to 9.0, "c" to 8.0, "d" to 7.0), mapOf("d" to 99.0, "b" to 3.0)) }

    @Test
    fun `the recall widens to the window, trusted links reorder the head, and the page is cut back`() =
        runBlocking {
            val e = engine()
            val page = pages().serve(listOf(query()), listOf(Filter(search = "zebra sort:links", limit = 2)), e.projection())
            // d: 7 x min(3, (1+99)^0.5) = 21; b: 9 x 2 = 18; a stays 10; c stays 8.
            assertEquals(listOf("d", "b"), page)
            assertEquals(listOf<Int?>(LinkRerank.DEFAULT_WINDOW), e.limits, "one recall, widened to the window")
            val (targets, linkers) = e.linkCalls.single()
            assertEquals(listOf("a", "b", "c", "d").map { LinkTarget(it, "author") }, targets, "one call for the whole head, each hit with its author")
            assertEquals(EventYql.RANK_RECENCY_GATED, linkers.ranking)
            assertEquals(lens, linkers.rankKey)
            assertEquals(2.0, linkers.minRank, "the read's own floor gates the linkers")
        }

    @Test
    fun `the cap keeps engagement from overpowering a hit more than three times stronger`() =
        runBlocking {
            // d's 1,000 trusted people take it to 7 x 3 = 21 — past b (9), never past a (40).
            val e = Engine(listOf("a" to 40.0, "b" to 9.0, "c" to 8.0, "d" to 7.0), mapOf("d" to 1_000.0))
            assertEquals(listOf("a", "d", "b"), pages().serve(listOf(query(limit = 3)), listOf(Filter(search = "zebra sort:links", limit = 3)), e.projection()))
        }

    @Test
    fun `a hit that drew only reports falls below one that drew nothing`() =
        runBlocking {
            // a: 10 x (1+3)^-0.5 = 5, under b (9) and c (8).
            val e = Engine(listOf("a" to 10.0, "b" to 9.0, "c" to 8.0), mapOf("a" to -3.0))
            assertEquals(listOf("b", "c"), pages().serve(listOf(query()), listOf(Filter(search = "zebra sort:links", limit = 2)), e.projection()))
        }

    @Test
    fun `without the token nothing is widened or counted`() =
        runBlocking {
            val e = engine()
            assertEquals(listOf("a", "b"), pages().serve(listOf(query()), listOf(Filter(search = "zebra", limit = 2)), e.projection()))
            assertEquals(listOf<Int?>(2), e.limits)
            assertTrue(e.linkCalls.isEmpty())
        }

    @Test
    fun `no lens no signal, so the engine order is served, still cut to the asked size`() =
        runBlocking {
            val e = engine()
            assertEquals(listOf("a", "b"), pages().serve(listOf(query(rankKey = null)), listOf(Filter(search = "zebra sort:links", limit = 2)), e.projection()))
            assertTrue(e.linkCalls.isEmpty())
        }
}
