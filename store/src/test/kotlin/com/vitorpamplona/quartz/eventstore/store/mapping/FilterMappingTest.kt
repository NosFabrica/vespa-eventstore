/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store.mapping

import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The NIP-50 `search` string -> [com.vitorpamplona.quartz.eventstore.vespa.EventQuery]
 * extension mapping: `sort:` picks the rank profile, `filter:rank:…` sets the
 * trust floor, and `include:spam` lowers the default floor to 0 (Brainstorm's
 * onlyRanked, inverted — the floor still rides the query because it anchors
 * the trust boost).
 */
class FilterMappingTest {
    private fun map(search: String?) = Filter(search = search).toEventQuery()!!

    @Test
    fun `plain terms get the default profile and the spam-filter floor`() {
        val q = map("vitor pamplona")
        assertEquals("vitor pamplona", q.search)
        assertNull(q.ranking, "null = EventYql's default (the search profile)")
        assertEquals(DEFAULT_MIN_RANK, q.minRank, "searches are onlyRanked by default")
    }

    @Test
    fun `include spam lowers the floor to zero instead of dropping it`() {
        val q = map("vitor include:spam")
        assertEquals("vitor", q.search)
        assertEquals(
            INCLUDE_SPAM_MIN_RANK,
            q.minRank,
            "0 keeps every hit but still anchors wot_mult's boost — an ABSENT floor " +
                "collapses the trust multiplier to a constant and the order degrades to pure text",
        )
        assertTrue(q.includeSpam, "the opt-out survives the mapping for the store's observer gate")
        assertFalse(map("vitor").includeSpam)
    }

    @Test
    fun `sort picks the rank profile`() {
        assertEquals(EventYql.RANK_DESC, map("vitor sort:rank").ranking)
        assertEquals(EventYql.RANK_DESC, map("vitor sort:rank:desc").ranking)
        assertEquals(EventYql.RANK_ASC, map("vitor sort:rank:asc").ranking)
        assertEquals(EventYql.RANK_FOLLOWERS, map("vitor sort:followers").ranking)
        assertEquals(EventYql.RANK_TEXT, map("vitor sort:text").ranking)
        assertNull(map("vitor sort:bogus").ranking, "unknown sort values are ignored")
    }

    @Test
    fun `sort without terms is a trust-ordered match-all, still spam-filtered`() {
        val q = map("sort:rank")
        assertNull(q.search)
        assertEquals(EventYql.RANK_DESC, q.ranking)
        assertEquals(DEFAULT_MIN_RANK, q.minRank)
    }

    @Test
    fun `filter rank raises the floor without changing the default order`() {
        val gte = map("vitor filter:rank:gte:5")
        assertEquals(5.0, gte.minRank)
        assertNull(gte.ranking, "a floor is just a floor: the default profile keeps its order")

        assertEquals(6.0, map("vitor filter:rank:gt:5").minRank, "gt on integer ranks = gte the next one")
        assertEquals(DEFAULT_MIN_RANK, map("vitor filter:rank:eq:5").minRank, "unknown comparators are ignored, leaving the default floor")
    }

    @Test
    fun `filter rank without terms leaves the profile to the store's observer gate`() {
        val q = map("filter:rank:gte:50")
        assertNull(q.search)
        assertEquals(50.0, q.minRank)
        assertNull(q.ranking, "the gated-recall profile is stamped in the store, once the out-of-band observer is known")
    }

    @Test
    fun `an explicit floor survives include spam and rides a chosen sort`() {
        val q = map("vitor sort:rank filter:rank:gte:10 include:spam")
        assertEquals(EventYql.RANK_DESC, q.ranking, "sort wins over the filter fallback")
        assertEquals(10.0, q.minRank, "the user asked for the floor; include:spam only drops the default")
    }

    @Test
    fun `observer token names the ranking lens and leaves the terms alone`() {
        val hex = "a".repeat(64)
        val q = map("vitor observer:$hex")
        assertEquals("vitor", q.search, "the observer token is an extension, not a search term")
        assertEquals(hex, q.observer)
    }

    @Test
    fun `observer token is lowercased and non-hex is ignored`() {
        assertEquals("a".repeat(64), map("vitor observer:${"A".repeat(64)}").observer)
        assertNull(map("vitor observer:not-a-key").observer, "a non-hex observer is dropped")
        assertNull(map("vitor").observer, "no token, no observer")
    }

    @Test
    fun `observer rides alongside a sort and a floor`() {
        val hex = "b".repeat(64)
        val q = map("vitor observer:$hex sort:rank filter:rank:gte:5")
        assertEquals(hex, q.observer)
        assertEquals(EventYql.RANK_DESC, q.ranking)
        assertEquals(5.0, q.minRank)
        assertEquals("vitor", q.search)
    }

    @Test
    fun `plain filters are never trust-gated`() {
        val none = Filter(kinds = listOf(1)).toEventQuery()!!
        assertNull(none.search)
        assertNull(none.ranking)
        assertNull(none.minRank, "NIP-01 recall — spam filtering belongs to search only")

        val extensionsOnly = map("language:en")
        assertNull(extensionsOnly.search, "an all-extensions query imposes no text constraint")
        assertNull(extensionsOnly.minRank, "…and no default floor either: nothing is ranked")
    }
}
