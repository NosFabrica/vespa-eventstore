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
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** The ORE read: profiles scored by their own text under one observer's lens, in one engine query. */
class ScoredProfileSearchTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private var seq = 0

    private fun id() = (++seq).toString().padStart(64, '0')

    /** Engine scores by author, standing in for Vespa's relevance (the in-memory index ranks nothing). */
    private val scores = HashMap<String, Double>()
    private val ranked = mutableListOf<EventQuery>()
    private val searched = mutableListOf<EventQuery>()
    private val inner = InMemoryEventIndex()
    private val scoring =
        object : EventIndex by inner {
            override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> {
                ranked += query
                return inner.search(query).map { Ranked(it, scores[it.pubkey]) }
            }

            override suspend fun search(query: EventQuery): List<EventDoc> {
                searched += query
                return inner.search(query)
            }
        }

    /**
     * [block]'s engine reads alone: run once first so the shared provider map (one kind-10040 read,
     * cached for every lensed read) is loaded, then counted from a clean slate.
     */
    private suspend fun <T> reads(block: suspend () -> T): T {
        block()
        ranked.clear()
        searched.clear()
        return block()
    }

    private val store = NostrSemanticsStore(TrustProjection(scoring, InMemoryReputationIndex()))

    private fun profile(
        author: String,
        name: String,
    ) = MetadataEvent(id(), author, 1_000L + seq, emptyArray(), """{"name":"$name"}""", "")

    private fun list10040(
        createdAt: Long = 1_000L,
        owner: String = observer,
    ) = TrustProviderListEvent(id(), owner, createdAt, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")

    @Test
    fun `an observer with no 10040 gets no service, no hits, and costs no engine query`() =
        runBlocking {
            val alice = "a1".repeat(32)
            // Someone else's list: a map holding none is never cached, and a live relay always holds some.
            store.insert(list10040(owner = "cc".repeat(32)))
            store.insert(profile(alice, "alice"))
            scores[alice] = 10.0
            val result = reads { store.scoredProfileSearch("alice", observer, limit = 100) }
            assertEquals(ScoredSearch(emptyList(), null), result)
            assertEquals(emptyList(), ranked + searched)
        }

    @Test
    fun `a 10040 is the whole lens - nothing asks whether its service's scores are mirrored yet`() =
        runBlocking {
            val alice = "a1".repeat(32)
            store.insert(list10040())
            store.insert(profile(alice, "alice"))
            scores[alice] = 10.0
            // No card at all: the one query runs under the service, and what it answers is the answer
            // (Vespa gates an unscored profile out; this stand-in index ranks nothing, so alice stays).
            val result = reads { store.scoredProfileSearch("alice", observer, limit = 100) }
            assertEquals(ScoredSearch(listOf(ScoredSearch.Hit(alice, 10.0)), service), result)
            assertEquals(listOf(service), ranked.map { it.rankKey })
            assertEquals(emptyList(), searched, "no other read: no card sample, no readiness probe")
        }

    @Test
    fun `an observer key is exactly 64 hex, not 64 hex and more`() =
        runBlocking {
            store.insert(list10040())
            assertFailsWith<IllegalArgumentException> { store.scoredProfileSearch("alice", observer + "0b", limit = 1) }
            assertFailsWith<IllegalArgumentException> { store.scoredProfileSearch("alice", observer.take(62), limit = 1) }
            Unit
        }

    @Test
    fun `non-finite engine scores are dropped, not served`() =
        runBlocking {
            val (nan, inf, ok) = listOf("a1", "a2", "a3").map { it.repeat(32) }
            store.insert(list10040())
            listOf(nan, inf, ok).forEach { store.insert(profile(it, "alice")) }
            scores += mapOf(nan to Double.NaN, inf to Double.POSITIVE_INFINITY, ok to 3.0)
            val result = store.scoredProfileSearch("alice", observer, limit = 100)
            assertEquals(listOf(ScoredSearch.Hit(ok, 3.0)), result.hits)
        }

    @Test
    fun `matching profiles come back with their engine scores, highest first`() =
        runBlocking {
            val (low, high, mid, other) = listOf("a1", "a2", "a3", "a4").map { it.repeat(32) }
            store.insert(list10040())
            // Inserted lowest-scored last, so the engine's recency order is not the answer.
            store.insert(profile(mid, "alice mid"))
            store.insert(profile(high, "alice high"))
            store.insert(profile(other, "bob"))
            store.insert(profile(low, "alice low"))
            scores += mapOf(low to 1.5, high to 900.0, mid to 42.0, other to 5000.0)
            val result = store.scoredProfileSearch("alice", observer, limit = 100)
            assertEquals(service, result.rankService)
            assertEquals(
                listOf(ScoredSearch.Hit(high, 900.0), ScoredSearch.Hit(mid, 42.0), ScoredSearch.Hit(low, 1.5)),
                result.hits,
            )
        }

    @Test
    fun `the read is one default-profile kind-0 search at trust floor 2, whatever extensions the text carries`() =
        runBlocking {
            store.insert(list10040())
            val stranger = "cc".repeat(32)
            reads { store.scoredProfileSearch("alice include:spam sort:recent filter:rank:gte:0 observer:$stranger", observer, limit = 7) }
            val q = ranked.single()
            assertEquals(listOf(0), q.kinds, "no pointer kinds: nothing is reached through expansion")
            assertEquals(listOf("alice"), q.search?.split(" "))
            assertEquals(2.0, q.minRank)
            assertEquals(null, q.ranking)
            assertEquals(false, q.includeSpam)
            assertEquals(observer, q.observer)
            assertEquals(service, q.rankKey)
            assertEquals(7, q.limit)
        }
}
