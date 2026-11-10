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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.ingest.Rejections
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * Verdicts that need no I/O — ephemeral (accepted, never stored), NIP-40
 * expired, text the engine cannot store — are answered WITHOUT the writer
 * lock. They used to queue for it like any write: an ephemeral OK measured at
 * 35-41 s on staging while the lock's holder worked (see `WriteLocks`).
 *
 * A batch or event carrying a TRUST kind (a card, a provider list, a kind
 * 5/62) still takes its commit, which settles the projection's ledger even
 * when it writes nothing; these are plain kinds.
 *
 * Deterministic: the lock is held by a real insert parked inside its own dup
 * GET until the test releases it, so "did not wait" is "completed while the
 * holder was still parked", not a latency budget.
 */
class LocalVerdictLockTest {
    private val alice = "a1".repeat(32)
    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(at: Long = 1_000_000_000L) = Event(id(), alice, at, 1, emptyArray(), "hello", "")

    private fun ephemeral() = Event(id(), alice, 1_000_000_000L, 20_001, emptyArray(), "typing…", "")

    private fun expired() = Event(id(), alice, 100, 1, arrayOf(arrayOf("expiration", "200")), "old", "")

    private fun unstorable() = Event(id(), alice, 1_000_000_000L, 1, emptyArray(), "bad\u0016here", "")

    /** Parks the dup GET for [parkedId] until [release] completes: the insert asking it holds the writer lock meanwhile. */
    private class ParkingIndex(
        private val inner: InMemoryEventIndex,
        private val parkedId: String,
    ) : EventIndex by inner {
        val parked = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()

        override suspend fun get(id: String): EventDoc? {
            if (id == parkedId) {
                parked.complete(Unit)
                release.await()
            }
            return inner.get(id)
        }

        override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
    }

    /** Runs [body] while a plain insert holds the writer lock, then lets the holder finish; [before] runs first, uncontended. */
    private fun whileLockHeld(
        before: suspend (NostrSemanticsStore) -> Unit = {},
        body: suspend (NostrSemanticsStore, InMemoryEventIndex) -> Unit,
    ) = runBlocking {
        val holder = note()
        val inner = InMemoryEventIndex()
        val index = ParkingIndex(inner, holder.id)
        val store = NostrSemanticsStore(index)
        before(store)
        coroutineScope {
            val holding = async { store.insert(holder) }
            index.parked.await()
            // Far longer than any of these takes; without the fix they wait forever.
            withTimeout(5_000) { body(store, inner) }
            index.release.complete(Unit)
            holding.await()
        }
    }

    @Test
    fun `an ephemeral, an expired and an unstorable insert are answered while the writer lock is held`() =
        whileLockHeld { store, inner ->
            val ephemeral = ephemeral()
            store.insert(ephemeral)
            assertNull(inner.get(ephemeral.id), "an ephemeral event is accepted, never stored")
            assertEquals(Rejections.EXPIRED, assertFailsWith<RejectedException> { store.insert(expired()) }.message)
            assertEquals(Rejections.UNSTORABLE_TEXT, assertFailsWith<RejectedException> { store.insert(unstorable()) }.message)
        }

    @Test
    fun `a batch decided without I-O is answered while the writer lock is held`() =
        whileLockHeld { store, inner ->
            val accepted = IEventStore.InsertOutcome.Accepted
            val expiredOut = IEventStore.InsertOutcome.Rejected(Rejections.EXPIRED)
            // Below BULK_MIN: the per-event loop.
            val small = listOf(ephemeral(), expired(), ephemeral())
            assertEquals(listOf(accepted, expiredOut, accepted), store.batchInsert(small))
            // At BULK_MIN: the bulk records path, every event settled in its plan.
            val bulk = (1..16).map { if (it % 2 == 0) ephemeral() else expired() }
            assertEquals(bulk.map { if (it.kind == 1) expiredOut else accepted }, store.batchInsert(bulk))
            (small + bulk).forEach { assertNull(inner.get(it.id), "nothing in these batches is stored") }
        }

    /**
     * A bulk batch whose every event the lock-free plan already decided — the
     * mirror's re-offer of what it fed last time — has nothing to commit, so
     * it does not queue for the lock to commit nothing.
     */
    @Test
    fun `a bulk batch of stored duplicates is answered while the writer lock is held`() {
        val stored = (1..16).map { note(at = 1_000_000_000L + it) }
        whileLockHeld(before = { it.batchInsert(stored) }) { store, _ ->
            assertEquals(stored.map { IEventStore.InsertOutcome.Rejected(Rejections.DUPLICATE) }, store.batchInsert(stored))
        }
    }
}
