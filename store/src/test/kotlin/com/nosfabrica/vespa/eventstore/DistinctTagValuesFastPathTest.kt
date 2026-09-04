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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * WHEN THE ENGINE'S AGGREGATE MAY ANSWER, AND WHEN IT MAY NOT.
 *
 * `tag_index` is lossy three ways — single-letter names, first values only, and
 * nothing of the rest of the tag — so a grouping over it answers exactly one
 * question: every value of one short tag, unconditionally. Ask anything else
 * through it and the answer silently WIDENS, which on the monitor's relay
 * sources would mean dialling urls nobody named.
 *
 * The preconditions are therefore asserted here rather than trusted, because
 * the cost of getting them wrong is not an error — it is a bigger set that
 * looks right.
 */
class DistinctTagValuesFastPathTest {
    /** Records whether the aggregate was consulted, and answers a fixed set when it is. */
    private class SpyIndex(
        private val answer: Set<String>?,
    ) : EventIndex by InMemoryEventIndex() {
        var asked: Pair<String, EventQuery>? = null

        override suspend fun distinctTagIndexValues(
            query: EventQuery,
            tagName: String,
        ): Set<String>? {
            asked = tagName to query
            return answer
        }
    }

    private fun store(spy: SpyIndex) = NostrSemanticsStore(spy)

    @Test
    fun `a one-letter tag with no condition is answered by the engine`() =
        runBlocking {
            val spy = SpyIndex(setOf("wss://a.example", "wss://b.example"))
            val got =
                store(spy).distinctTagValues(
                    Filter(kinds = listOf(10002)),
                    tagName = "r",
                    unconditional = true,
                )
            assertEquals(setOf("wss://a.example", "wss://b.example"), got)
            assertEquals("r", spy.asked?.first, "the engine should have been asked")
        }

    @Test
    fun `a positional condition never reaches the aggregate`() =
        runBlocking {
            // The write-marker shape. `tag_index` cannot see a third element,
            // so an aggregate here would return read relays too.
            val spy = SpyIndex(setOf("wss://everything.example"))
            store(spy).distinctTagValues(
                Filter(kinds = listOf(10002)),
                tagName = "r",
                where = { it.getOrNull(2) == "write" },
            )
            assertEquals(null, spy.asked, "a conditional ask must walk, not aggregate")
        }

    @Test
    fun `a multi-character tag never reaches the aggregate`() =
        runBlocking {
            // `tag_index` holds single-letter names only; "30382:rank" is not
            // in it at all, so an aggregate would answer the empty set.
            val spy = SpyIndex(emptySet())
            store(spy).distinctTagValues(
                Filter(kinds = listOf(10040)),
                tagName = "30382:rank",
                unconditional = true,
            )
            assertEquals(null, spy.asked, "a multi-character tag must walk")
        }

    @Test
    fun `a value read past position 1 never reaches the aggregate`() =
        runBlocking {
            // NIP-85 service tags and relay hints carry the url at 2;
            // `tag_index` keeps the FIRST value only.
            val spy = SpyIndex(emptySet())
            store(spy).distinctTagValues(
                Filter(kinds = listOf(10040)),
                tagName = "p",
                valueIndex = 2,
                unconditional = true,
            )
            assertEquals(null, spy.asked, "a position-2 read must walk")
        }

    @Test
    fun `an index with no aggregate falls back to the walk`() =
        runBlocking {
            // Null is "I cannot", not "the answer is empty" — the caller must
            // still get the exact set, from the slower path.
            val spy = SpyIndex(null)
            val got =
                store(spy).distinctTagValues(
                    Filter(kinds = listOf(10002)),
                    tagName = "r",
                    unconditional = true,
                )
            assertEquals("r", spy.asked?.first, "the aggregate was offered the question")
            assertTrue(got.isEmpty(), "and the walk answered over an empty in-memory corpus")
        }
}
