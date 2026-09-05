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

import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The two answers a stored set of 10040s gives: which services are NAMED (the
 * write side's gate — a card by a named service becomes a cell under that
 * service) and each observer's LENS (the read side's resolution — the one
 * service per dimension a query carries). Pure parsing, no store.
 */
class ProviderMapTest {
    private val observer = "0b".repeat(32)
    private val observer2 = "2b".repeat(32)
    private val service = "5e".repeat(32)
    private val service2 = "6e".repeat(32)

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun list(
        author: String,
        vararg entries: Pair<String, String>,
    ) = TrustProviderListEvent(id(), author, 1_000L + seq, entries.map { (type, key) -> arrayOf(type, key, "wss://scores.example.com/") }.toTypedArray(), "", "").toDoc()

    @Test
    fun `a list names its services and resolves its owner's lens per dimension`() {
        val p = ProviderMap.providersOf(listOf(list(observer, "30382:rank" to service, "30382:followers" to service2)))
        assertEquals(setOf(service), p.rankServices)
        assertEquals(setOf(service2), p.followerServices)
        assertTrue(p.maps(service) && p.maps(service2))
        assertEquals(TrustProviders.Lens(rank = service, followers = service2), p.lensOf(observer))
        assertEquals(TrustProviders.NO_LENS, p.lensOf(observer2), "an observer with no list resolves to nothing")
    }

    /** NIP-85 prescribes no merge across providers for one metric; the first entry per dimension is the lens, the rest still project. */
    @Test
    fun `the first entry per dimension is the lens, the rest are still named`() {
        val p = ProviderMap.providersOf(listOf(list(observer, "30382:rank" to service, "30382:rank" to service2)))
        assertEquals(service, p.lensOf(observer).rank)
        assertEquals(setOf(service, service2), p.rankServices, "the second provider's cards still become cells")
    }

    /** A popular provider is one set entry and one lens per list — never a copy per observer. */
    @Test
    fun `a shared provider is named once and resolved by every observer naming it`() {
        val p = ProviderMap.providersOf(listOf(list(observer, "30382:rank" to service), list(observer2, "30382:rank" to service)))
        assertEquals(setOf(service), p.rankServices)
        assertEquals(service, p.lensOf(observer).rank)
        assertEquals(service, p.lensOf(observer2).rank)
        assertEquals(listOf(service), ProviderMap.trustServicesOf(listOf(list(observer, "30382:rank" to service))))
    }

    /** Only `30382:rank` and `30382:followers` feed the tensors; other delegations name no service here. */
    @Test
    fun `other delegation kinds name no trust service`() {
        val p = ProviderMap.providersOf(listOf(list(observer, "30383:rank" to service, "30392" to service2, "30382:hops" to service2)))
        assertTrue(p.isEmpty())
        assertFalse(p.maps(service) || p.maps(service2))
        assertEquals(TrustProviders.NO_LENS, p.lensOf(observer))
    }
}
