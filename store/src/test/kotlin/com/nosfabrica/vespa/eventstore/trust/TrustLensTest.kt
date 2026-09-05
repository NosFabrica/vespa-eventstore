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
package com.nosfabrica.vespa.eventstore.trust

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * THE LENS ON THE QUERY: a read carrying an observer reaches the engine with
 * the service keys their stored kind 10040 names (`EventQuery.rankKey` /
 * `followersKey`), resolved off the projection's provider map — the tensors
 * are keyed by service, so the observer's own key is never sent.
 */
class TrustLensTest {
    private val observer = "0b".repeat(32)
    private val rankService = "5e".repeat(32)
    private val followerService = "6e".repeat(32)

    private val seen = mutableListOf<EventQuery>()
    private val inner = InMemoryEventIndex()
    private val recording =
        object : EventIndex by inner {
            override suspend fun search(query: EventQuery): List<EventDoc> {
                seen += query
                return inner.search(query)
            }
        }
    private val store = NostrSemanticsStore(TrustProjection(recording, InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private fun list10040() =
        TrustProviderListEvent(
            "1".padStart(64, '0'),
            observer,
            1_000L,
            arrayOf(arrayOf("30382:rank", rankService, "wss://scores.example.com/"), arrayOf("30382:followers", followerService, "wss://followers.example.com/")),
            "",
            "",
        )

    /** The caller's own kind-1 query, as the engine saw it. */
    private fun captured(): EventQuery = seen.last { it.kinds == listOf(1) }

    @Test
    fun `an observer token resolves to the services the stored 10040 names`() =
        runBlocking {
            store.insert(list10040())
            store.query<Event>(Filter(kinds = listOf(1), search = "observer:$observer sort:rank"))
            val q = captured()
            assertEquals(observer, q.observer)
            assertEquals(rankService, q.rankKey)
            assertEquals(followerService, q.followersKey)
        }

    @Test
    fun `a connection observer resolves the same way`() =
        runBlocking {
            store.insert(list10040())
            withContext(StoreQueryContext(setOf(observer))) { store.query<Event>(Filter(kinds = listOf(1))) }
            val q = captured()
            assertEquals(observer, q.observer)
            assertEquals(rankService, q.rankKey)
        }

    /** No list: the observer stays on the query (the gate, the expansion) but the lens resolves to nothing — the engine ranks them as trusting nobody. */
    @Test
    fun `an observer with no stored 10040 resolves to no key`() =
        runBlocking {
            store.query<Event>(Filter(kinds = listOf(1), search = "observer:$observer sort:rank"))
            val q = captured()
            assertEquals(observer, q.observer)
            assertNull(q.rankKey)
            assertNull(q.followersKey)
        }

    @Test
    fun `a read without an observer carries no lens`() =
        runBlocking {
            store.insert(list10040())
            store.query<Event>(Filter(kinds = listOf(1)))
            val q = captured()
            assertNull(q.observer)
            assertNull(q.rankKey)
        }

    /** A store assembled without the projection has no map to resolve against. */
    @Test
    fun `a bare store resolves nothing`() =
        runBlocking {
            val bare = NostrSemanticsStore(recording, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            bare.insert(list10040())
            bare.query<Event>(Filter(kinds = listOf(1), search = "observer:$observer sort:rank"))
            assertNull(captured().rankKey)
        }
}
