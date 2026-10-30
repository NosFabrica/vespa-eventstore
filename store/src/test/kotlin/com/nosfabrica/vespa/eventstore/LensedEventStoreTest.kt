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

import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * [LensedEventStore] is the ONE test a consumer holding an `IEventStore` writes
 * to reach this store's capabilities, and it must hold for BOTH handles: the
 * front door `open()` returns, and the bare store a test builds. vespa-relay had
 * a type switch per call site and one reach through `VespaEventStore.store`
 * that only the front door could answer; each is now this cast.
 */
class LensedEventStoreTest {
    private val mock = MockVespaEngine()

    @AfterTest
    fun stopMock() = mock.stop()

    private val list =
        EventDoc(
            id = "1".repeat(64),
            pubkey = "a".repeat(64),
            createdAt = 1_700_000_000,
            kind = 10002,
            tags = listOf(listOf("r", "wss://one.example"), listOf("r", "wss://two.example")),
            content = "",
            sig = "e".repeat(128),
            search = SearchFields(),
        )

    @Test
    fun `the front door answers every capability through the interface`() =
        runBlocking {
            mock.inner.put(list)
            VespaEventStore.open(url = mock.url, autoDeploy = false, readOnly = true).use { front ->
                val held: IEventStore = front
                val lensed = assertNotNull(held as? LensedEventStore, "the front door is a LensedEventStore")
                assertEquals(
                    setOf("wss://one.example", "wss://two.example"),
                    lensed.distinctTagValues(Filter(kinds = listOf(10002)), "r", unconditional = true),
                    "distinctTagValues delegates to the store, no `.store` reach",
                )
                assertNotNull(lensed.liveGate(listOf(Filter(kinds = listOf(1), search = "nostr"))), "a searching subscription is gated")
            }
        }

    @Test
    fun `the bare store answers the same interface`() =
        runBlocking {
            val index = InMemoryEventIndex().also { it.put(list) }
            val held: IEventStore = NostrSemanticsStore(index)
            val lensed = assertNotNull(held as? LensedEventStore, "the bare store is a LensedEventStore")
            val page = lensed.reindexFullTextSearch(listOf(10002), null)
            assertTrue(page.done, "one page covers one document")
            assertEquals(
                setOf("wss://one.example", "wss://two.example"),
                lensed.distinctTagValues(Filter(kinds = listOf(10002)), "r"),
            )
        }
}
