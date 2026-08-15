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
package com.nosfabrica.vespa.eventstore.engine

import com.nosfabrica.vespa.eventstore.engine.client.SchemaFallbacks
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The gated-profile compatibility net, on a serving schema that predates the
 * observer gate (`deployIfAbsent` never redeploys onto a running cluster, so
 * this is reachable in production). The net fails OPEN — the demoted query is
 * ungated — and these are the assertions the wire tests CANNOT make:
 * [MockVespaEngine] does not rank, so a demoted relevance query and a demoted
 * recency query return the same order there, and the difference only appears
 * on an engine that really scores. The contract is therefore pinned on the
 * demoted query itself.
 */
class SchemaFallbacksTest {
    /** Drive [SchemaFallbacks.withProfileFallback] with an engine that 400s the gated profile; return the query the RERUN used. */
    private fun demoted(q: EventQuery): EventQuery =
        runBlocking {
            val fallbacks = SchemaFallbacks()
            val attempts = mutableListOf<EventQuery>()
            fallbacks.withProfileFallback(q) { attempt ->
                attempts += attempt
                // The 400 a pre-gate schema answers, naming the missing profile.
                if (attempt.ranking == EventYql.RANK_RECENCY_GATED || attempt.ranking == EventYql.RANK_RECENCY_GATED_EXACT) {
                    throw IllegalArgumentException("400 Bad Request: Requested rank profile '${EventYql.RANK_RECENCY_GATED}' is undefined for document type 'event'")
                }
            }
            assertEquals(2, attempts.size, "the net must rerun exactly once")
            attempts.last()
        }

    /**
     * TERMLESS gated recall (the observer gate on a plain filter) demotes to a
     * RANKING-FREE query: that is what hands it back the recency profile and
     * the count-probe planner, both of which key on `ranking == null`.
     */
    @Test
    fun `a plain gated feed demotes to a ranking-free query`() {
        val q = demoted(EventQuery(kinds = listOf(1), limit = 50, ranking = EventYql.RANK_RECENCY_GATED, minRank = 2.0, observer = "ab".repeat(32)))
        assertNull(q.ranking, "ranking-free is what the recency profile and the planner key on")
        assertTrue("order by created_at desc" in EventYql.build(q)!!.yql, "still NIP-01 order")
    }

    /**
     * A `sort:recent` SEARCH must NOT take that demotion. Ranking-free WITH
     * terms selects a RELEVANCE profile, so the engine would return the
     * top-`limit` by relevance — and since the store re-sorts the page by date,
     * the result reads chronological while holding the wrong events, with
     * nothing anywhere reporting a fault. The unranked profile keeps both the
     * recall and `order by created_at desc`.
     */
    @Test
    fun `a sort recent search demotes to unranked so the page stays the NEWEST limit`() {
        val q = demoted(EventQuery(kinds = listOf(1), search = "vitor", limit = 50, ranking = EventYql.RANK_RECENCY_GATED, minRank = 2.0, observer = "ab".repeat(32)))
        assertEquals(EventYql.RANK_UNRANKED, q.ranking)
        assertEquals("vitor", q.search, "the demotion touches the order, never the recall")
        assertTrue("order by created_at desc" in EventYql.build(q)!!.yql, "the engine, not the client, must pick the newest page")
    }

    /** Phrases are search text too — a phrase-only `sort:recent` takes the same demotion. */
    @Test
    fun `a phrase-only sort recent search demotes to unranked as well`() {
        val q = demoted(EventQuery(kinds = listOf(1), phrases = listOf("new york"), limit = 50, ranking = EventYql.RANK_RECENCY_GATED, observer = "ab".repeat(32)))
        assertEquals(EventYql.RANK_UNRANKED, q.ranking)
        assertTrue("order by created_at desc" in EventYql.build(q)!!.yql)
    }

    /**
     * Drive [SchemaFallbacks.withNearFallback] against a schema missing
     * [missing] columns; return every query the net attempted.
     *
     * The engine answers the YQL parser's wording for a schema gap, and only
     * for a column the attempt actually carried — a demoted attempt that no
     * longer references the field must not keep 400ing, or the net would spin.
     */
    private fun columnAttempts(
        q: EventQuery,
        vararg missing: String,
    ): List<EventQuery> =
        runBlocking {
            val attempts = mutableListOf<EventQuery>()
            SchemaFallbacks().withNearFallback(q) { attempt ->
                attempts += attempt
                val sent = EventYql.build(attempt)!!.yql
                missing.firstOrNull { it in sent }?.let {
                    throw IllegalArgumentException("400 Bad Request: Field '$it' does not exist.")
                }
            }
            attempts
        }

    /**
     * `search_text_gram` shipped after the near columns, so a schema can carry
     * the near columns and lack this one. Demoting it must leave them alone —
     * stripping name/title prefix reach from a schema that has it would be a
     * silent recall outage in the opposite direction.
     */
    @Test
    fun `a schema missing only the body gram column keeps its near columns`() {
        val attempts = columnAttempts(EventQuery(search = "testin"), "search_text_gram")
        assertEquals(2, attempts.size, "one demotion, one rerun")
        assertTrue(attempts.last().nearMatching, "the near columns are a different schema generation")
        assertEquals(false, attempts.last().bodyGramMatching)
        assertEquals("testin", attempts.last().search, "the demotion touches the reach, never the recall")
    }

    /** The older gap still demotes on its own, with the body column untouched. */
    @Test
    fun `a schema missing only the near columns keeps the body gram column`() {
        val attempts = columnAttempts(EventQuery(search = "testin"), "name_near")
        assertEquals(2, attempts.size)
        assertEquals(false, attempts.last().nearMatching)
        assertTrue(attempts.last().bodyGramMatching)
    }

    /**
     * A schema old enough lacks BOTH, which needs two demotions — the single
     * retry the net had before this column existed would have thrown on the
     * second gap.
     */
    @Test
    fun `a schema missing both columns demotes twice rather than failing`() {
        val attempts = columnAttempts(EventQuery(search = "testin"), "name_near", "search_text_gram")
        assertEquals(3, attempts.size, "two gaps, two demotions")
        assertEquals(false, attempts.last().nearMatching)
        assertEquals(false, attempts.last().bodyGramMatching)
    }
}
