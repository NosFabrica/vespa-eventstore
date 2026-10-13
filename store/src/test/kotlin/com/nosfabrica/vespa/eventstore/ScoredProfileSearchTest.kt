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
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/** The ORE read: profiles scored by their own text under one observer's lens, with the lens state beside them. */
class ScoredProfileSearchTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private var seq = 0

    private fun id() = (++seq).toString().padStart(64, '0')

    /** Engine scores by author, standing in for Vespa's relevance (the in-memory index ranks nothing). */
    private val scores = HashMap<String, Double>()
    private val ranked = mutableListOf<EventQuery>()
    private val inner = InMemoryEventIndex()
    private val scoring =
        object : EventIndex by inner {
            override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> {
                ranked += query
                return inner.search(query).map { Ranked(it, scores[it.pubkey]) }
            }
        }
    private val store = NostrSemanticsStore(TrustProjection(scoring, InMemoryReputationIndex()))

    private fun profile(
        author: String,
        name: String,
    ) = MetadataEvent(id(), author, 1_000L + seq, emptyArray(), """{"name":"$name"}""", "")

    private fun list10040(createdAt: Long = 1_000L) = TrustProviderListEvent(id(), observer, createdAt, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")

    private fun card(
        subject: String,
        vararg assertion: Array<String> = arrayOf(arrayOf("rank", "80")),
    ) = UserAssertionEvent(id(), service, 1_000L + seq, arrayOf(arrayOf("d", subject), *assertion), "", "")

    @Test
    fun `an observer with no 10040 gets NO_LIST and no hits`() =
        runBlocking {
            val alice = "a1".repeat(32)
            store.insert(profile(alice, "alice"))
            scores[alice] = 10.0
            val result = store.scoredProfileSearch("alice", observer, limit = 100)
            assertEquals(ScoredSearch.Lens(ScoredSearch.LensStatus.NO_LIST, null, null), result.lens)
            assertEquals(emptyList(), result.hits)
        }

    @Test
    fun `a 10040 whose service has no cards yet gets NO_CARDS and no hits`() =
        runBlocking {
            val alice = "a1".repeat(32)
            store.insert(list10040())
            store.insert(profile(alice, "alice"))
            scores[alice] = 10.0
            val result = store.scoredProfileSearch("alice", observer, limit = 100)
            assertEquals(ScoredSearch.Lens(ScoredSearch.LensStatus.NO_CARDS, service, 1_000L), result.lens)
            assertEquals(emptyList(), result.hits)
        }

    @Test
    fun `the lens reports when the observer's resolved 10040 was created`() =
        runBlocking {
            store.insert(list10040(createdAt = 1_700_000_123L))
            assertEquals(1_700_000_123L, store.scoredProfileSearch("alice", observer, limit = 1).lens.listCreatedAt)
            store.insert(card("a1".repeat(32)))
            assertEquals(1_700_000_123L, store.scoredProfileSearch("alice", observer, limit = 1).lens.listCreatedAt, "READY reports it too")
            assertEquals(null, store.scoredProfileSearch("alice", "cc".repeat(32), limit = 1).lens.listCreatedAt, "no list, no time")
        }

    @Test
    fun `a stored card the projection never saw is NO_CARDS, not READY`() =
        runBlocking {
            val alice = "a1".repeat(32)
            store.insert(list10040())
            // Straight into the engine, as a card held before its service was named and never walked.
            inner.put(card(alice).toDoc())
            store.insert(profile(alice, "alice"))
            scores[alice] = 10.0
            val result = store.scoredProfileSearch("alice", observer, limit = 100)
            assertEquals(ScoredSearch.Lens(ScoredSearch.LensStatus.NO_CARDS, service, 1_000L), result.lens)
            assertEquals(emptyList(), result.hits)
        }

    @Test
    fun `a projected card that asserts no rank is NO_CARDS, not READY`() =
        runBlocking {
            val alice = "a1".repeat(32)
            store.insert(list10040())
            store.insert(card(alice, arrayOf("followers", "12")))
            store.insert(profile(alice, "alice"))
            scores[alice] = 10.0
            val result = store.scoredProfileSearch("alice", observer, limit = 100)
            assertEquals(ScoredSearch.Lens(ScoredSearch.LensStatus.NO_CARDS, service, 1_000L), result.lens)
        }

    @Test
    fun `non-finite engine scores are dropped, not served`() =
        runBlocking {
            val (nan, inf, ok) = listOf("a1", "a2", "a3").map { it.repeat(32) }
            store.insert(list10040())
            store.insert(card(ok))
            listOf(nan, inf, ok).forEach { store.insert(profile(it, "alice")) }
            scores += mapOf(nan to Double.NaN, inf to Double.POSITIVE_INFINITY, ok to 3.0)
            val result = store.scoredProfileSearch("alice", observer, limit = 100)
            assertEquals(listOf(ScoredSearch.Hit(ok, 3.0)), result.hits)
        }

    @Test
    fun `a READY lens returns matching profiles with their engine scores, highest first`() =
        runBlocking {
            val (low, high, mid, other) = listOf("a1", "a2", "a3", "a4").map { it.repeat(32) }
            store.insert(list10040())
            store.insert(card(high))
            // Inserted lowest-scored last, so the engine's recency order is not the answer.
            store.insert(profile(mid, "alice mid"))
            store.insert(profile(high, "alice high"))
            store.insert(profile(other, "bob"))
            store.insert(profile(low, "alice low"))
            scores += mapOf(low to 1.5, high to 900.0, mid to 42.0, other to 5000.0)
            val result = store.scoredProfileSearch("alice", observer, limit = 100)
            assertEquals(ScoredSearch.Lens(ScoredSearch.LensStatus.READY, service, 1_000L), result.lens)
            assertEquals(
                listOf(ScoredSearch.Hit(high, 900.0), ScoredSearch.Hit(mid, 42.0), ScoredSearch.Hit(low, 1.5)),
                result.hits,
            )
        }

    @Test
    fun `the read is one default-profile kind-0 search at trust floor 2, whatever extensions the text carries`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card("a1".repeat(32)))
            val stranger = "cc".repeat(32)
            store.scoredProfileSearch("alice include:spam sort:recent filter:rank:gte:0 observer:$stranger", observer, limit = 7)
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
