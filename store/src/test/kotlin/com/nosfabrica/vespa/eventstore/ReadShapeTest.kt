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
package com.nosfabrica.vespa.eventstore

import com.nosfabrica.vespa.eventstore.mapping.toEventQuery
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [ReadShape] must say what the store will DO with a filter, so it is held to
 * the compiled query rather than to a second reading of the grammar: for every
 * search string below, its observer and spam waiver are the query's, and
 * "ranked" is exactly the condition that stamps the default trust floor
 * (`minRank`) on a read with no explicit `filter:`.
 */
class ReadShapeTest {
    private val hex = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

    private val battery =
        listOf(
            null,
            "",
            "bitcoin",
            "\"exact phrase\"",
            "-spam",
            "-\"a phrase\"",
            // recognised sorts, with and without text
            "sort:rank",
            "sort:rank:asc",
            "sort:followers",
            "sort:text",
            "sort:recent",
            "nostr sort:recent",
            // unknown and malformed extensions: the store ignores them
            "sort:bogus",
            "sort:",
            "nostr sort:bogus",
            "filter:rank:gte:nope",
            "observer:npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq",
            "observer:${hex.take(63)}",
            "observer:${hex.uppercase()}",
            "observer:$hex",
            "observer:$hex bitcoin",
            "include:spam",
            "include:spam sort:rank",
            "include:spamx",
            "language:pt",
            "unknown:thing",
        )

    @Test
    fun `the shape agrees with the compiled query on every extension`() {
        for (search in battery) {
            val shape = ReadShape.of(Filter(kinds = listOf(1), search = search))
            val q = Filter(kinds = listOf(1), search = search).toEventQuery()!!
            assertEquals(q.observer, shape.observer, "observer for `$search`")
            assertEquals(q.includeSpam, shape.includeSpam, "include:spam for `$search`")
            assertEquals(q.minRank != null, shape.ranked, "ranked for `$search` (minRank=${q.minRank})")
        }
    }

    @Test
    fun `an unknown sort is not a ranked read, which is where the relay's copy disagreed`() {
        assertFalse(ReadShape.of("sort:bogus").ranked)
        assertNull(Filter(search = "sort:bogus").toEventQuery()!!.ranking, "the store serves it as plain recall")
        assertTrue(ReadShape.of("sort:recent").ranked)
        assertTrue(ReadShape.of("\"just a phrase\"").ranked, "a phrase is text")
        assertFalse(ReadShape.of("-only -exclusions").ranked, "exclusions alone are recall minus words")
    }

    @Test
    fun `a lens is declared by a usable observer or the spam waiver, never by an npub`() {
        assertEquals(hex, ReadShape.of("observer:${hex.uppercase()}").observer, "lowercased, as the store reads it")
        assertTrue(ReadShape.of("observer:$hex").declaresLens)
        assertTrue(ReadShape.of("include:spam").declaresLens)
        assertFalse(ReadShape.of("observer:npub1abc").declaresLens, "an npub is ignored by the store, so it declares nothing")
        assertFalse(ReadShape.of("bitcoin").declaresLens)
        assertFalse(ReadShape.of(null).declaresLens)
    }

    @Test
    fun `a filter that matches nothing still has a shape`() {
        assertEquals(ReadShape.of("observer:$hex sort:rank"), ReadShape.of(Filter(ids = emptyList(), search = "observer:$hex sort:rank")))
    }
}
