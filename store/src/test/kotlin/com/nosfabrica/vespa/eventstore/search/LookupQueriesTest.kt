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

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * THE SHAPE OF A SUBJECT LOOKUP, asserted on the query rather than through a
 * page.
 *
 * Every one of these is a transformation of the FINDING query, and that is the
 * whole safety argument of the expansion: a subject is served only because the
 * index applied the same predicate to it that it applied to the hits. So what
 * SURVIVES a transformation matters as much as what it changes — an observer
 * dropped here would serve un-lensed rows into a lensed read, and a `d` or an
 * `ids` list replaced instead of intersected would serve records the caller
 * explicitly excluded.
 *
 * These ran only through `SearchExpansionTest`'s whole-store paths before,
 * where a lost field shows up as a wrong page and has to be traced back.
 */
class LookupQueriesTest {
    private val observer = "0b".repeat(32)
    private val a = "11".repeat(32)
    private val b = "22".repeat(32)

    private fun finding(
        minRank: Double? = null,
        ids: List<String> = emptyList(),
        authors: List<String> = emptyList(),
        kinds: List<Int> = listOf(1),
        tags: Map<String, List<String>> = emptyMap(),
    ) = EventQuery(
        kinds = kinds,
        ids = ids,
        authors = authors,
        tags = tags,
        search = "satoshi",
        phrases = listOf("exact one"),
        notSearch = listOf("spam"),
        ranking = EventYql.RANK_SEARCH,
        limit = 25,
        observer = observer,
        minRank = minRank,
        notExpiredAt = 1_700_000_000L,
    )

    @Test
    fun `a lookup keeps what decides the visible corpus and drops what was searched for`() {
        val q = finding().forLookup()

        assertNull(q.search, "the terms are what a lookup is NOT")
        assertTrue(q.phrases.isEmpty())
        assertTrue(q.notSearch.isEmpty())
        assertNull(q.ranking, "the profile the terms selected goes with them")
        assertNull(q.limit, "a limit is the caller's budget for HITS; the expansion has its own caps")

        assertEquals(observer, q.observer, "the lens decides which corpus is visible and must survive")
        assertEquals(1_700_000_000L, q.notExpiredAt, "so does expiry: a lookup must not serve what the read refuses")
        assertEquals(listOf(1), q.kinds, "and the kinds, which is what holds the page to what was asked for")
    }

    /**
     * The floor travels twice for a reason the KDoc gives in full: on the member
     * profile `min_rank` stops being a gate, so an EXPLICIT floor has to arrive
     * as `memberFloor` to be honoured as one. The store's own default floor is
     * not explicit — a reader who never asked for a floor must not get a hard
     * gate they did not request.
     */
    @Test
    fun `only an explicitly asked-for floor becomes a member floor`() {
        assertEquals(42.0, finding(minRank = 42.0).forLookup().memberFloor, "an explicit floor gates the member profile too")
        assertNull(finding(minRank = DEFAULT_MIN_RANK).forLookup().memberFloor, "the store's default floor is not a request")
        assertNull(finding(minRank = null).forLookup().memberFloor)
        assertEquals(42.0, finding(minRank = 42.0).forLookup().minRank, "and the floor itself still travels as it always did")
    }

    @Test
    fun `a narrowed lookup intersects the caller's own constraints instead of replacing them`() {
        // ids: the read named two, the pointer names one of them and a stranger.
        assertEquals(listOf(a), finding(ids = listOf(a, b)).narrowIds(listOf(a, "33".repeat(32)))?.ids)
        assertNull(finding(ids = listOf(b)).narrowIds(listOf(a)), "nothing left to ask for is no query at all")
        assertEquals(listOf(a), finding(ids = emptyList()).narrowIds(listOf(a))?.ids, "an unconstrained read takes the chunk whole")

        // authors, via the profile shape — and `ids` must SURVIVE it.
        val profiles = finding(ids = listOf(a), kinds = emptyList()).narrowProfiles(listOf(a, b))
        assertEquals(listOf(0), profiles?.kinds, "a profile lookup asks for kind 0")
        assertEquals(listOf(a, b), profiles?.authors)
        assertEquals(listOf(a), profiles?.ids, "the read's own ids survive: the engine ANDs them")
        assertNull(finding(kinds = listOf(1)).narrowProfiles(listOf(a)), "a read that cannot serve kind 0 asks for no profiles")
    }

    @Test
    fun `an addressable lookup intersects the d tags the read already constrained`() {
        val asked = finding(kinds = listOf(30023), tags = mapOf("d" to listOf("keep", "also")))
        val q = asked.narrowAddresses(30023, a, listOf("keep", "unasked"))
        assertEquals(listOf("keep"), q?.tags?.get("d"), "a `d` the read excluded must not come back")
        assertEquals(listOf(a), q?.authors)
        assertNull(asked.narrowAddresses(30023, a, listOf("unasked")), "no overlap is no query")
        assertEquals(listOf(b), asked.narrowAddresses(30023, b, listOf("keep"))?.authors, "a read that named no authors admits any publisher")
        val authored = finding(kinds = listOf(30023), authors = listOf(a), tags = mapOf("d" to listOf("keep")))
        assertNull(authored.narrowAddresses(30023, b, listOf("keep")), "but one that named authors excludes the rest")
        assertNull(finding(kinds = listOf(1)).narrowAddresses(30023, a, listOf("keep")), "nor a kind it cannot serve")
    }

    /**
     * A weighted lookup carries the pointer's own relevance ONLY when the floor
     * span is on; with no span the profile's default leaves a subject on its
     * rung exactly as it was before the floor existed. Sending it anyway would
     * split batches that could have shared one query — see [SubjectBatchesTest].
     */
    @Test
    fun `the pointer's relevance travels only when the floor span asks for it`() {
        val floored = finding().forLookup().withWeightedMember("member", gamma = 1.5, pointerRelevance = 7.0, pointerText = 2.0, floorSpan = 0.5)
        assertEquals(7.0, floored.rankFeatures[EventYql.F_POINTER_REL])
        assertEquals(2.0, floored.rankFeatures[EventYql.F_POINTER_TEXT])
        assertEquals(0.5, floored.rankFeatures[EventYql.F_SUBJECT_FLOOR_SPAN])

        val unfloored = finding().forLookup().withWeightedMember("member", gamma = 1.5, pointerRelevance = 7.0, pointerText = 2.0, floorSpan = null)
        assertNull(unfloored.rankFeatures[EventYql.F_POINTER_REL], "no span, no floor: the relevance is simply not sent")
        assertEquals(1.0, unfloored.rankFeatures[EventYql.F_DOC_CONF], "the per-key confidences still travel")
    }

    /**
     * A finding query that ranks on no ladder has nothing a synthesized member
     * score could be comparable with, so the lookup stays exactly as it was —
     * and the splice falls back to the pointer's order, the same degradation an
     * unscored page gets.
     */
    @Test
    fun `an unranked finding query produces an unranked lookup`() {
        val plain = finding().forLookup()
        assertEquals(plain, plain.withMember(profile = null, confidence = 0.9, gamma = 1.0))
        assertEquals(plain, plain.withWeightedMember(profile = null, gamma = 1.0, pointerRelevance = 1.0, pointerText = 1.0, floorSpan = 0.5))
        assertEquals("member", plain.withMember("member", 0.9, 1.0).ranking, "a ranked one takes the member ladder")
    }
}
