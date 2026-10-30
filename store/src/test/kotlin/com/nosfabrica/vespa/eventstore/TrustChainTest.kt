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

import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * [LensedEventStore.lensOf] follows the chain the way the store's READS do:
 * the FIRST `30382:rank` provider of the list, and that one's cards. The case
 * vespa-relay's sign-in notice got wrong is the first test: a list whose first
 * provider has signed nothing here, a second one that has — the ranked page is
 * empty, and the relay's "any of the first eight" check said nothing.
 */
class TrustChainTest {
    private val reader = "0b".repeat(32)
    private val first = "5e".repeat(32)
    private val second = "6f".repeat(32)
    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private val store = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), InMemoryReputationIndex()), nowSecs = { 10_000L })

    private fun list(vararg rank: String) = TrustProviderListEvent(id(), reader, 1_000L + seq, rank.map { arrayOf("30382:rank", it, "wss://s.example/") }.toTypedArray(), "", "")

    private fun card(by: String) = UserAssertionEvent(id(), by, 1_000L + seq, arrayOf(arrayOf("d", reader), arrayOf("rank", "87")), "", "")

    @Test
    fun `the lens is the first rank provider, and its cards are the ones asked about`() =
        runBlocking {
            store.batchInsert(listOf(list(first, second), card(second)))
            val lens = store.lensOf(reader)
            assertEquals(first, lens.storedRankService)
            assertEquals(first, lens.rankService, "the cached pass agrees once this process wrote the list")
            assertEquals(false, lens.rankServiceHasCards, "the second provider's card does not rank this reader's reads")
            assertEquals(TrustChain.Gap.NO_RANK_CARDS, lens.gap)

            store.batchInsert(listOf(card(first)))
            assertNull(store.lensOf(reader).gap, "with the first provider's card the chain is whole")
        }

    @Test
    fun `each missing link is named in the order a read fails`() =
        runBlocking {
            assertEquals(TrustChain.Gap.NO_PROVIDER_LIST, store.lensOf(reader).gap)
            store.batchInsert(listOf(TrustProviderListEvent(id(), reader, 1_000L + seq, arrayOf(arrayOf("30382:followers", first, "wss://s.example/")), "", "")))
            assertEquals(TrustChain.Gap.NO_RANK_SERVICE, store.lensOf(reader).gap, "a followers-only list ranks nothing")
            assertNull(store.lensOf(reader).rankServiceHasCards, "no service, no question")
        }

    @Test
    fun `a non-hex key is refused, not decoded`() =
        runBlocking {
            val lensed: LensedEventStore = store
            assertFailsWith<IllegalArgumentException> { lensed.lensOf("npub1abc") }
        }
}
