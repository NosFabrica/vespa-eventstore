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
package com.nosfabrica.vespa.eventstore.benchmark.bench

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * [SearchTrace.ablate] against the YQL [EventYql] really builds — the
 * ablations are only measurements if they strip what they say from the
 * production shape, so the shape comes from the builder, never a fixture.
 */
class SearchTraceTest {
    private fun built(term: String) = assertNotNull(EventYql.build(EventQuery(kinds = listOf(1), limit = 50, search = term))).yql

    @Test
    fun `a multi-word term is ablated inside its rank-only wrapper`() {
        val yql = built("book of daniel")
        assertTrue("rank(" in yql && "body_phrase" in yql, "precondition: a multi-word group travels as rank(words, phrase)")
        assertTrue("_gram" in yql, "precondition: the word group carries gram branches")

        val stripped = assertNotNull(SearchTrace.ablate(yql, listOf("_gram")), "the gram branches inside rank( must be found")
        assertFalse("_gram" in stripped)
        // The rank-only phrase rides along untouched: it matches nothing, so
        // keeping it is what keeps the variant's ranking the production one.
        assertTrue("body_phrase" in stripped && "rank(" in stripped)
    }

    @Test
    fun `a one-word term has no wrapper and ablates as before`() {
        val yql = built("bitcoin")
        assertFalse("rank(" in yql)
        val stripped = assertNotNull(SearchTrace.ablate(yql, listOf("_gram")))
        assertFalse("_gram" in stripped)
        assertEquals(stripped, SearchTrace.ablate(stripped, listOf("_gram")) ?: stripped, "a second pass finds nothing left to strip")
    }
}
