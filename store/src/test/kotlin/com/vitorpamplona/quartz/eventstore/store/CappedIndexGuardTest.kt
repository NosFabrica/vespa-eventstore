/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store

import com.vitorpamplona.quartz.eventstore.vespa.MockVespaEngine
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A deployment may set [VespaEventIndex.maxHits] to bound what caller recall
 * costs. That setting must NOT reach the store's own decision reads: dedup,
 * NIP-09/62 guards and supersession answer "may this write happen", and a
 * truncated answer there resurrects deleted events.
 *
 * The cap here is 1 — the most hostile value that still returns something — so
 * any decision query riding the caller-facing default would see exactly one
 * row and get the answer wrong.
 */
class CappedIndexGuardTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url, maxHits = 1)
    private val store = NostrEventStore(index, relay = "wss://sot.test/".normalizeRelayUrl())

    private val alice = "a1".repeat(32)

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(
        at: Long,
        eventId: String = id(),
    ) = Event(eventId, alice, at, 1, emptyArray(), "hello", "")

    private fun deletion(
        targetId: String,
        at: Long,
    ) = DeletionEvent(id(), alice, at, arrayOf(arrayOf("e", targetId)), "", "")

    @AfterTest
    fun tearDown() {
        index.close()
        mock.stop()
    }

    /**
     * The guarding tombstone is the OLDEST of alice's kind-5s, so a capped guard
     * query (newest-first, one row) returns a DIFFERENT tombstone and misses it.
     */
    @Test
    fun `a capped index still blocks a bulk-inserted event whose tombstone is not the newest`() =
        runBlocking {
            val targetId = id()

            // The guard: a tombstone at t=500 covering an event created at t=100.
            store.insert(deletion(targetId, at = 500))
            // Newer, unrelated tombstones — these are what a cap of 1 would return.
            repeat(20) { store.insert(deletion(id(), at = 1_000L + it)) }

            // Past BULK_MIN so the batched guard path actually engages.
            val batch = listOf(note(at = 100, eventId = targetId)) + (1..24).map { note(at = 2_000L + it) }
            val outcomes = store.batchInsert(batch)

            assertEquals(
                IEventStore.InsertOutcome.Rejected(Rejections.DELETED),
                outcomes[0],
                "the tombstone at t=500 guards this event; a truncated guard set would have missed it",
            )
            assertTrue(store.query<Event>(listOf()).none { it.id == targetId }, "and it must not be stored")
        }

    /** Dedup is a decision read too: a second copy of a stored id must still be rejected. */
    @Test
    fun `a capped index still detects duplicates in a batch`() =
        runBlocking {
            val first = (1..25).map { note(at = 3_000L + it) }
            store.batchInsert(first)

            val outcomes = store.batchInsert(first)
            assertTrue(
                outcomes.all { it == IEventStore.InsertOutcome.Rejected(Rejections.DUPLICATE) },
                "every event was already stored: $outcomes",
            )
        }
}
