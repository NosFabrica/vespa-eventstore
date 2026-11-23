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
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The NIP-40 sweep over the WIRE: [VespaEventIndex]'s cursor walk, paged at
 * ten ids so a few hundred events span dozens of walk pages, against
 * [MockVespaEngine], which reports `totalCount` as real Vespa does — the
 * whole match set, not the page. [ExpirySweepTest] runs the same sweep on the
 * in-memory reference, whose walk is one page and whose answers carry no
 * count, which is how a walk that refused every page past the first went
 * unseen.
 */
class ExpirySweepWireTest {
    private val mock = MockVespaEngine()
    private val engine = VespaEventIndex(mock.url, idPageSize = 10)
    private val realNow = System.currentTimeMillis() / 1000
    private var clock = realNow
    private val alice = "a1".repeat(32)
    private val service = "5e".repeat(32)
    private val observer = "0b".repeat(32)
    private var seq = 0

    @AfterTest
    fun stop() {
        engine.close()
        mock.stop()
    }

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(
        at: Long,
        expiresAt: Long?,
    ) = Event(id(), alice, at, 1, if (expiresAt == null) emptyArray() else arrayOf(arrayOf("expiration", "$expiresAt")), "n$seq", "")

    private fun card(
        subject: Int,
        expiresAt: Long,
    ) = UserAssertionEvent(id(), service, 2_000L + subject, arrayOf(arrayOf("d", "%064x".format(subject)), arrayOf("rank", "50"), arrayOf("expiration", "$expiresAt")), "", "")

    @Test
    fun `a sweep over many walk pages drains the whole backlog, both passes`() =
        runBlocking {
            val reputations = InMemoryReputationIndex()
            val store = NostrSemanticsStore(TrustProjection(engine, reputations, nowSecs = { clock }), nowSecs = { clock }, expirySweepPage = 5)
            val list = TrustProviderListEvent(id(), observer, 5_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")
            val due = (1..300).map { note(at = 1_000L + it, expiresAt = realNow + 50) } + (1..40).map { card(subject = it, expiresAt = realNow + 50) }
            val kept = (1..20).map { note(at = 3_000L + it, expiresAt = null) }
            store.insert(list)
            store.batchInsert(due + kept).forEach { assertEquals(IEventStore.InsertOutcome.Accepted, it) }
            assertTrue(reputations.docs.values.count { it.influenceScores.isNotEmpty() } == 40, "fixture: the cards projected")
            clock = realNow + 100

            store.deleteExpiredEvents()

            assertEquals((kept + list).mapTo(HashSet()) { it.id }, engine.search(EventQuery()).mapTo(HashSet()) { it.id }, "every due event gone, nothing else")
            assertTrue(reputations.docs.values.none { it.influenceScores.isNotEmpty() }, "and every swept card's cell with it")
        }
}
