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
package com.nosfabrica.vespa.eventstore.mapping

import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The NIP-50 `search` string -> [com.nosfabrica.vespa.eventstore.engine.EventQuery]
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
    fun `minus terms become exclusions, not search words`() {
        val q = map("cat -dog")
        assertEquals("cat", q.search)
        assertEquals(listOf("dog"), q.notSearch)
        assertEquals(DEFAULT_MIN_RANK, q.minRank, "the positive term still ranks (and spam-filters) as usual")
    }

    @Test
    fun `an exclusion-only query is plain recall minus the words`() {
        val q = map("-spam -scam")
        assertNull(q.search, "no positive terms: nothing to rank")
        assertEquals(listOf("spam", "scam"), q.notSearch)
        assertNull(q.ranking)
        assertNull(q.minRank, "plain recall: the observer gate, not the search floor, decides")
    }

    @Test
    fun `minus edge cases — lone dash, unindexable word, mid-word hyphens`() {
        assertEquals("-", map("-").search, "a lone dash is a term (never-matching), not syntax")
        assertTrue(map("-").notSearch.isEmpty())
        assertEquals("cat", map("cat -⚡").search)
        assertTrue(map("cat -⚡").notSearch.isEmpty(), "an exclusion no index can hold excludes nothing")
        assertEquals("e-cash", map("e-cash").search, "mid-word hyphens are not exclusions")
        assertEquals(listOf("e-cash"), map("-e-cash").notSearch, "…but a leading dash flips the whole term")
        assertNull(map("-e-cash").search)
    }

    @Test
    fun `quoted spans become exact-phrase requirements`() {
        val q = map("pizza \"new york\"")
        assertEquals("pizza", q.search)
        assertEquals(listOf("new york"), q.phrases)
        assertEquals(DEFAULT_MIN_RANK, q.minRank)
    }

    @Test
    fun `a phrase-only query is still a ranked search`() {
        val q = map("\"new york\"")
        assertNull(q.search)
        assertEquals(listOf("new york"), q.phrases)
        assertEquals(DEFAULT_MIN_RANK, q.minRank, "phrases are search text: the default spam floor applies")
    }

    @Test
    fun `a minus before quotes excludes the phrase`() {
        val q = map("pizza -\"pineapple ham\"")
        assertEquals("pizza", q.search)
        assertTrue(q.phrases.isEmpty())
        assertEquals(listOf("pineapple ham"), q.notSearch)
    }

    @Test
    fun `quote edge cases — unclosed, empty, single word`() {
        assertEquals(listOf("new york"), map("pizza \"new york").phrases, "an unclosed quote runs to the end")
        assertEquals("pizza", map("pizza \"new york").search)
        val empty = map("pizza \"\"")
        assertEquals("pizza", empty.search)
        assertTrue(empty.phrases.isEmpty(), "empty quotes are nothing")
        assertEquals(listOf("vitor"), map("\"vitor\"").phrases, "a quoted word is the fuzzy opt-out, not a loose term")
        assertNull(map("\"vitor\"").search)
    }

    @Test
    fun `quotes are lifted before the extension pass — the closing quote survives`() {
        // Quartz's extension pass is quote-blind: parsed AFTER it, the span's
        // closing quote would vanish with the stripped `sort:rank"` token and
        // the then-unclosed quote would swallow `-spam` into the phrase —
        // flipping an exclusion into REQUIRED text. The audit's headline bug.
        val q = map("\"pizza sort:rank\" -spam")
        assertEquals(listOf("pizza sort:rank"), q.phrases, "the quoted span survives intact")
        assertEquals(listOf("spam"), q.notSearch, "…and the exclusion stays an exclusion")
        assertNull(q.search)
        assertNull(q.ranking, "a quoted sort token is text, not a sort order")
    }

    @Test
    fun `quotes protect extension-shaped tokens`() {
        val q = map("\"sort:rank\" pizza sort:followers")
        assertEquals(listOf("sort:rank"), q.phrases, "quoted: a phrase")
        assertEquals(EventYql.RANK_FOLLOWERS, q.ranking, "unquoted: still an extension")
        assertEquals("pizza", q.search)
    }

    @Test
    fun `dashed edge cases — double dash and dash-prefixed extension shapes`() {
        assertEquals(listOf("word"), map("--word").notSearch, "every leading dash strips")
        // `-sort:rank` is NOT an extension (Quartz keys are strictly a-z), so
        // it lands in the term scan and excludes the literal — there is no
        // `-extension` syntax.
        val q = map("-sort:rank pizza")
        assertEquals(listOf("sort:rank"), q.notSearch)
        assertNull(q.ranking)
        assertEquals("pizza", q.search)
    }

    @Test
    fun `exclusions ride beside extensions`() {
        val q = map("-nsfw sort:rank")
        assertEquals(listOf("nsfw"), q.notSearch)
        assertEquals(EventYql.RANK_DESC, q.ranking, "a sorted match-all, minus the word")
        assertEquals(DEFAULT_MIN_RANK, q.minRank, "an explicit sort is ranked, so the default floor applies")
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
