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

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A NIP-77 SNAPSHOT IS THE SET ITS REQ SERVES, WHOLE.
 *
 * Two ways it was not, both on a relay that stamped its REQ `default_limit` on
 * a NEG-OPEN: a limit'd filter left the id walk for ONE bounded search page, so
 * a reconcile covered the newest `default_limit` events and reported that as
 * the set; and the `observer:` token's gate ran with no lens resolved — the
 * empty tensor, "trusts nobody" — so an observer-declared reconcile was EMPTY.
 * What reaches the engine is pinned here; what comes back needs a real Vespa
 * (the in-memory reference cannot gate) and is FilterMatrixIT's snapshot column.
 */
class NegentropySnapshotTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private val authors = listOf(90, 50, 10, 1).mapIndexed { i, _ -> "a${i + 1}".repeat(32) }
    private val ranks = listOf(90, 50, 10, 1)

    private val walked = mutableListOf<EventQuery>()
    private val searched = mutableListOf<EventQuery>()
    private val inner = InMemoryEventIndex()
    private val recording =
        object : EventIndex by inner {
            override suspend fun search(query: EventQuery): List<EventDoc> {
                searched += query
                return inner.search(query)
            }

            override suspend fun visitIds(
                query: EventQuery,
                withDTag: Boolean,
                onPage: suspend (List<DocRef>) -> Boolean,
            ) {
                walked += query
                inner.visitIds(query, withDTag, onPage)
            }
        }
    private val store = NostrSemanticsStore(TrustProjection(recording, InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    /** Four authors at ranks 90/50/10/1 under [service], three notes each, and the observer's 10040 naming it. */
    private fun seed() =
        runBlocking {
            store.batchInsert(
                authors.zip(ranks).map { (a, r) -> UserAssertionEvent(id(), service, 1_000L + r, arrayOf(arrayOf("d", a), arrayOf("rank", r.toString())), "", "") } +
                    authors.flatMap { a -> (0 until 3).map { n -> TextNoteEvent(id(), a, 1_700_000_000L + seq, emptyArray(), "note $n", "") } },
            )
            store.insert(TrustProviderListEvent(id(), observer, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
        }

    private fun snapshot(filter: Filter): Set<String> = runBlocking { store.snapshotIdsForNegentropy(listOf(filter), null, null).map { it.id }.toSet() }

    private fun notesBy(minRank: Int): Set<String> =
        runBlocking {
            store
                .query<TextNoteEvent>(Filter(kinds = listOf(1), search = "include:spam"))
                .filter { n -> (ranks[authors.indexOf(n.pubKey)]) >= minRank }
                .map { it.id }
                .toSet()
        }

    @Test
    fun `an observer-declared snapshot walks through the resolved lens`() {
        seed()
        walked.clear()
        snapshot(Filter(kinds = listOf(1), search = "observer:$observer filter:rank:gte:50"))
        val q = walked.single()
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking)
        assertEquals(service, q.rankKey, "the token resolves to the service its 10040 names, not the empty lens")
        assertEquals(50.0, q.minRank)
    }

    @Test
    fun `every lens mode reaches the walk as the set its REQ serves`() {
        seed()

        fun walkOf(search: String): EventQuery {
            walked.clear()
            snapshot(Filter(kinds = listOf(1), search = search))
            return walked.single()
        }
        walkOf("observer:$observer").let {
            assertEquals(EventYql.RANK_RECENCY_GATED, it.ranking)
            assertEquals(2.0, it.minRank, "the default floor")
        }
        walkOf("observer:$observer include:spam").let { assertNull(it.ranking, "a waiver lifts the default floor: ungated") }
        walkOf("observer:$observer include:spam filter:rank:gte:50").let { assertEquals(50.0, it.minRank, "an explicit floor survives the waiver") }
        walkOf("include:spam").let { assertNull(it.ranking, "no observer, no gate") }
    }

    @Test
    fun `a limit'd plain filter walks instead of taking one search page`() {
        seed()
        walked.clear()
        searched.clear()
        val newest = snapshot(Filter(kinds = listOf(1), limit = 5, search = "include:spam"))
        assertEquals(5, newest.size)
        assertEquals(1, walked.size, "the walk honours the limit as the newest N")
        assertTrue(searched.none { it.kinds == listOf(1) }, "no bounded search page: $searched")
    }

    @Test
    fun `a trust sort reduces to the gated walk, and sort-text to a plain one`() {
        seed()
        listOf("sort:rank", "sort:rank:asc", "sort:followers").forEach { sort ->
            walked.clear()
            snapshot(Filter(kinds = listOf(1), search = "observer:$observer $sort filter:rank:gte:50"))
            val q = walked.single()
            assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking, sort)
            assertEquals(service, q.rankKey, sort)
            assertEquals(50.0, q.minRank, sort)
        }
        walked.clear()
        snapshot(Filter(kinds = listOf(1), search = "observer:$observer sort:text"))
        assertNull(walked.single().ranking, "sort:text reads no lens, so its set is ungated")
    }
}
