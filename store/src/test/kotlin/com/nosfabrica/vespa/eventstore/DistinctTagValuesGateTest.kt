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
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * AN OBSERVER-DECLARED TAG SELECT IS THE GATED SET — the values a REQ through
 * that lens would serve, read through the services the observer's 10040 names.
 *
 * It used to resolve no lens: every other read path runs its queries through
 * `NostrSemanticsStore.lensed`, this one compiled the filter and sent it, so
 * the gate read the EMPTY tensor ("trusts nobody") and answered nothing — and
 * the one-letter fast path, a grouping that cannot gate, answered every
 * author's values instead. Over the wire, because the in-memory reference
 * cannot gate: [MockVespaEngine.belowFloorAuthors] models the drop-limit, and
 * an unresolved lens.
 */
class DistinctTagValuesGateTest {
    private val mock = MockVespaEngine()
    private val engine = VespaEventIndex(mock.url)
    private val store = NostrSemanticsStore(TrustProjection(engine, InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private val trusted = "a1".repeat(32)
    private val spam = "a2".repeat(32)

    @AfterTest
    fun tearDown() {
        engine.close()
        mock.stop()
    }

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun seed() =
        runBlocking {
            store.batchInsert(
                listOf(
                    AdvertisedRelayListEvent(id(), trusted, 1_700_000_000L, arrayOf(arrayOf("r", "wss://kept.example/", "write")), "", ""),
                    AdvertisedRelayListEvent(id(), spam, 1_700_000_000L, arrayOf(arrayOf("r", "wss://gated.example/", "write")), "", ""),
                ),
            )
            store.insert(TrustProviderListEvent(id(), observer, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
            mock.belowFloorAuthors = setOf(spam)
        }

    /**
     * Verified to fail both ways. Without the lens: "the walk ==> expected:
     * <[wss://kept.example/]> but was: <[]>". With the lens but the fast path
     * open to a gated select: "the fast path ==> … but was: <[]>" — empty only
     * because the mock answers no `tag_index` grouping; real Vespa's ungated
     * grouping would hand back both urls.
     */
    @Test
    fun `an observer-declared tag select returns the gated set, on the walk and the fast path`() =
        runBlocking {
            seed()
            val filter = Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), search = "observer:$observer")
            assertEquals(setOf("wss://kept.example/"), store.distinctTagValues(filter, "r", where = { it.getOrNull(2) == "write" }), "the walk")
            assertEquals(setOf("wss://kept.example/"), store.distinctTagValues(filter, "r", unconditional = true), "the fast path")
            // And a plain select stays the raw set: no observer, no gate. (The
            // walk: the mock answers no `tag_index` grouping, so its fast path
            // would read as empty here whatever the gate did.)
            assertEquals(setOf("wss://kept.example/", "wss://gated.example/"), store.distinctTagValues(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)), "r", where = { true }))
        }
}
