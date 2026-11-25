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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class QueryIntentTest {
    private val observer = "a".repeat(64)
    private val lens = "b".repeat(64)

    /** An index whose ranked search answers [entities] (with [followers] beside each) and records every probe it was sent. */
    private class Probed(
        var entities: List<EventDoc> = emptyList(),
        var fail: Boolean = false,
        var followers: Double? = null,
    ) : EventIndex by InMemoryEventIndex() {
        val probes = ArrayList<EventQuery>()

        override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> {
            probes += query
            if (fail) error("engine down")
            return entities.map { Ranked(it, 1.0, followers = followers) }
        }
    }

    private fun entity(
        kind: Int,
        content: String,
    ) = EventDoc(id = "c".repeat(64), pubkey = "d".repeat(64), createdAt = 1, kind = kind, tags = emptyList(), content = content, sig = "e".repeat(128))

    private fun search(
        text: String,
        kinds: List<Int> = emptyList(),
    ) = EventQuery(search = text, kinds = kinds, observer = observer, rankKey = lens, minRank = 2.0, nowSecs = 1_000)

    @Test
    fun `a query no trusted entity is named reads topical, and the probe asks the right question`() =
        runBlocking {
            val index = Probed(listOf(entity(0, """{"name":"Satoshi Coffee Co."}""")))
            val out = QueryIntent(index).apply(listOf(search("coffee"))).single()
            assertEquals(1.0, out.topical)
            val probe = index.probes.single()
            assertEquals(listOf(0, 31990), probe.kinds)
            assertEquals(listOf("coffee"), probe.phrases, "one quoted phrase: named exactly needs no fuzzy reach")
            assertNull(probe.search)
            assertEquals(QueryIntent.PROBE_TRUST, probe.minRank)
            assertEquals(QueryIntent.PROBE_HITS, probe.limit)
            assertEquals(lens, probe.rankKey, "under the reader's own lens")
            assertEquals(EventYql.RANK_SEARCH, probe.ranking)
        }

    @Test
    fun `a query a trusted profile or app is named reads navigational, and is left as it was`() =
        runBlocking {
            for (named in listOf(entity(0, """{"display_name":"ODELL"}"""), entity(31990, """{"name":"odell"}"""))) {
                val q = search("odell")
                assertEquals(q, QueryIntent(Probed(listOf(named))).apply(listOf(q)).single())
            }
        }

    @Test
    fun `a profile named the query makes it navigational only when it is notable`() =
        runBlocking {
            val coffee = entity(0, """{"name":"Coffee ☕"}""")
            assertEquals(1.0, QueryIntent(Probed(listOf(coffee), followers = 124.0)).topicalOf(search("coffee")), "a few hundred followers: someone named Coffee")
            assertEquals(0.0, QueryIntent(Probed(listOf(coffee), followers = QueryIntent.PROBE_FOLLOWERS)).topicalOf(search("coffee")))
            assertEquals(0.0, QueryIntent(Probed(listOf(coffee))).topicalOf(search("coffee")), "an unknown count errs toward the ranking as it was")
            val app = entity(31990, """{"name":"olas"}""")
            assertEquals(0.0, QueryIntent(Probed(listOf(app), followers = 3.0)).topicalOf(search("olas")), "an app handler is always an entity")
        }

    @Test
    fun `reads the rung cannot move are not probed`() =
        runBlocking {
            val index = Probed()
            val intent = QueryIntent(index)
            val skipped =
                listOf(
                    search("coffee", kinds = listOf(0)),
                    search("coffee", kinds = listOf(1, 30023)),
                    search("coffee").copy(observer = null, rankKey = null),
                    search("coffee").copy(ranking = EventYql.RANK_DESC),
                    EventQuery(kinds = listOf(1), observer = observer, rankKey = lens),
                )
            for (q in skipped) assertNull(intent.topicalOf(q), "$q")
            assertTrue(index.probes.isEmpty())
            // A mixed read that can surface an app still asks.
            assertEquals(1.0, intent.topicalOf(search("coffee", kinds = listOf(1, 31923))))
        }

    @Test
    fun `an answer is reused per lens and text until it expires`() =
        runBlocking {
            var now = 0L
            val index = Probed()
            val intent = QueryIntent(index) { now }
            intent.topicalOf(search("Coffee"))
            intent.topicalOf(search("coffee"))
            assertEquals(1, index.probes.size, "same lens, same words")
            intent.topicalOf(search("coffee").copy(rankKey = "f".repeat(64)))
            assertEquals(2, index.probes.size, "another lens trusts other entities")
            now += QueryIntent.TTL_MILLIS
            intent.topicalOf(search("coffee"))
            assertEquals(3, index.probes.size, "expired")
        }

    @Test
    fun `a failing probe leaves the query as it was`() =
        runBlocking {
            val q = search("coffee")
            assertEquals(q, QueryIntent(Probed(fail = true)).apply(listOf(q)).single())
        }
}
