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
package com.nosfabrica.vespa.eventstore.ingest

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.RejectedException
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip23LongContent.LongFormContentEvent
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A supersession whose SWEEP fails after its winner landed: retried in place
 * ([com.nosfabrica.vespa.eventstore.engine.async.retryingSweep]), and when that
 * is not enough, owed ([OwedSweeps]) until the address is written again or its
 * winner re-offered — which comes back a duplicate and so never reached
 * supersession again. Before, the stale version stayed stored indefinitely:
 * two versions served, and a later deletion of the winner resurrected it.
 */
class OwedSweepsTest {
    private val relayUrl = "wss://sot.test/".normalizeRelayUrl()
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)
    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun profile(at: Long) = Event(id(), alice, at, 0, emptyArray(), "{\"at\":$at}", "")

    private fun article(at: Long) = LongFormContentEvent(id(), alice, at, arrayOf(arrayOf("d", "post")), "body $at", "")

    private fun note(at: Long) = Event(id(), bob, at, 1, emptyArray(), "note $at", "")

    /** An index whose removals fail while [failing] holds, or for the next [failTimes] calls. */
    private class FlakyRemovals(
        val inner: InMemoryEventIndex = InMemoryEventIndex(),
    ) : EventIndex by inner {
        val failing = AtomicBoolean(false)
        val failTimes = AtomicInteger(0)

        override suspend fun removeDocs(docs: List<EventDoc>) {
            check(!failing.get() && failTimes.getAndUpdate { maxOf(0, it - 1) } == 0) { "remove refused" }
            inner.removeDocs(docs)
        }

        override suspend fun removeAll(ids: List<String>) = removeDocs(ids.mapNotNull { inner.get(it) })

        // The port's read-then-supersede, so its sweep goes through [removeDocs] above.
        override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
    }

    private suspend fun stored(
        index: FlakyRemovals,
        kinds: List<Int>,
    ) = index.inner
        .search(EventQuery(kinds = kinds, authors = listOf(alice)))
        .map { it.id }
        .toSet()

    @Test
    fun `a sweep that fails once is retried inside the commit`() =
        runBlocking {
            val index = FlakyRemovals()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            store.batchInsert(listOf(profile(100), article(100)))
            val newer = listOf(profile(200), article(200))
            index.failTimes.set(1)

            val outcomes = store.batchInsert(newer)

            assertEquals(List(2) { IEventStore.InsertOutcome.Accepted }, outcomes)
            assertEquals(newer.map { it.id }.toSet(), stored(index, listOf(0, LongFormContentEvent.KIND)), "each address holds its winner alone")
        }

    @Test
    fun `a bulk batch whose sweep failed is swept when its winners are re-offered as duplicates`() =
        runBlocking {
            val index = FlakyRemovals()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            store.batchInsert(listOf(profile(100), article(100)))
            val newer = listOf(profile(200), article(200))
            index.failing.set(true)
            assertFailsWith<IllegalStateException> { store.batchInsert(newer) }
            assertEquals(4, stored(index, listOf(0, LongFormContentEvent.KIND)).size, "the winners landed, their sweep did not")
            index.failing.set(false)

            val outcomes = store.batchInsert(newer)

            assertTrue(outcomes.all { it is IEventStore.InsertOutcome.Rejected && it.reason.startsWith("duplicate") }, "the re-offer is a duplicate: $outcomes")
            assertEquals(newer.map { it.id }.toSet(), stored(index, listOf(0, LongFormContentEvent.KIND)), "and it finished the sweep")
        }

    @Test
    fun `a mixed batch whose sweep failed is swept when its winners are re-offered`() =
        runBlocking {
            val index = FlakyRemovals()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            store.batchInsert(listOf(profile(100), article(100)))
            val batch = listOf(profile(200), article(200), note(150), DeletionRequestEvent(id(), bob, 300, arrayOf(arrayOf("e", "dd".repeat(32))), "", ""))
            index.failing.set(true)
            assertFailsWith<IllegalStateException> { store.batchInsert(batch) }
            index.failing.set(false)

            store.batchInsert(batch)

            assertEquals(batch.take(2).map { it.id }.toSet(), stored(index, listOf(0, LongFormContentEvent.KIND)))
        }

    @Test
    fun `a single insert whose sweep failed is swept when it is re-offered as a duplicate`() =
        runBlocking {
            val index = FlakyRemovals()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            store.insert(profile(100))
            val newer = profile(200)
            index.failing.set(true)
            assertFailsWith<IllegalStateException> { store.insert(newer) }
            index.failing.set(false)

            val dup = assertFailsWith<RejectedException> { store.insert(newer) }

            assertTrue(dup.message!!.startsWith("duplicate"), "${dup.message}")
            assertEquals(setOf(newer.id), stored(index, listOf(0)), "the duplicate finished the sweep")
        }
}
