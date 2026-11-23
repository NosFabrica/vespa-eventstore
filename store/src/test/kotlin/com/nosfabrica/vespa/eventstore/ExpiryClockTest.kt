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
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.ingest.Rejections
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * NIP-40 admission reads THE STORE'S CLOCK (`nowSecs`), the same clock the
 * expiry sweep reads its cutoff from — on every admission path: a single
 * insert, a small batch (the per-event loop) and a bulk batch (the planner's
 * stage A). The sweep's termination argument is "nothing already due at the
 * cutoff can be admitted after it", which holds only if the two agree on what
 * "now" is; admission used to read quartz's wall clock instead.
 */
class ExpiryClockTest {
    private val realNow = System.currentTimeMillis() / 1000
    private val alice = "a1".repeat(32)
    private var seq = 0

    private fun expiring(expiresAt: Long) = Event((++seq).toString(16).padStart(64, '0'), alice, 1_000L + seq, 1, arrayOf(arrayOf("expiration", "$expiresAt")), "e$seq", "")

    private val expired = IEventStore.InsertOutcome.Rejected(Rejections.EXPIRED)

    @Test
    fun `an event due by the store's clock is rejected though the wall clock has not reached it`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, nowSecs = { realNow + 1_000 })
            val due = realNow + 500

            assertEquals(Rejections.EXPIRED, assertFailsWith<RejectedException> { store.insert(expiring(due)) }.message, "a single insert")
            assertEquals(List(3) { expired }, store.batchInsert(List(3) { expiring(due) }), "a small batch")
            assertEquals(List(BULK) { expired }, store.batchInsert(List(BULK) { expiring(due) }), "a bulk batch")
            assertEquals(0, index.search(EventQuery()).size, "nothing was stored")
        }

    @Test
    fun `an event not yet due by the store's clock is admitted though the wall clock is past it`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, nowSecs = { realNow - 1_000 })
            val notYet = realNow - 500

            store.insert(expiring(notYet))
            assertTrue(store.batchInsert(List(3) { expiring(notYet) }).all { it == IEventStore.InsertOutcome.Accepted }, "a small batch")
            assertTrue(store.batchInsert(List(BULK) { expiring(notYet) }).all { it == IEventStore.InsertOutcome.Accepted }, "a bulk batch")
            assertEquals(1 + 3 + BULK, index.search(EventQuery()).size, "all stored")
        }

    private companion object {
        /** Mirrors NostrSemanticsStore's private BULK_MIN — the batch size that takes the bulk path. */
        const val BULK = 16
    }
}
