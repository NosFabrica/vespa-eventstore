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
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.addressTagOrNull
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.owner
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip23LongContent.LongFormContentEvent
import com.vitorpamplona.quartz.nip59Giftwrap.wraps.GiftWrapEvent
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The bulk path for batches carrying a kind 5/62 ([BulkMixedInsert]): what its
 * preload asks the engine, and that the replay over that preload ends exactly
 * where the per-event path does. Batches are sized past BULK_MIN.
 */
class BulkMixedInsertTest {
    private val relayUrl = "wss://sot.test/".normalizeRelayUrl()
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)
    private val stranger = "c3".repeat(32)

    private var t = 1_000_000L
    private var seq = 0

    private fun next() = t++

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(
        author: String,
        at: Long = next(),
        eventId: String = id(),
    ) = Event(eventId, author, at, 1, emptyArray(), "note $eventId", "")

    private fun article(
        author: String,
        d: String,
        at: Long = next(),
    ) = LongFormContentEvent(id(), author, at, arrayOf(arrayOf("d", d)), "body", "")

    private fun deletion(
        author: String,
        at: Long = next(),
        ids: List<String> = emptyList(),
        addresses: List<String> = emptyList(),
    ) = DeletionRequestEvent(id(), author, at, (ids.map { arrayOf("e", it) } + addresses.map { arrayOf("a", it) }).toTypedArray(), "", "")

    /** Every query the store sends the engine, in order. */
    private class RecordingIndex(
        private val inner: InMemoryEventIndex,
    ) : EventIndex by inner {
        val queries = CopyOnWriteArrayList<EventQuery>()

        override suspend fun search(query: EventQuery): List<EventDoc> {
            queries += query
            return inner.search(query)
        }

        override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
    }

    /** The same [prelude] + [batch] through batchInsert and through insert, one by one: outcomes and stored ids must agree. */
    private fun assertMixedMatchesSequential(
        prelude: List<Event>,
        batch: List<Event>,
        writers: WriterTopology,
    ): List<IEventStore.InsertOutcome> =
        runBlocking {
            assertTrue(batch.size >= 16 && batch.any { it is DeletionRequestEvent }, "the batch must take the bulk mixed path")
            val bulkIndex = InMemoryEventIndex()
            val bulkStore = NostrSemanticsStore(bulkIndex, relay = relayUrl, writers = writers)
            prelude.forEach { bulkStore.insert(it) }
            val bulk = bulkStore.batchInsert(batch)

            val seqIndex = InMemoryEventIndex()
            val seqStore = NostrSemanticsStore(seqIndex, relay = relayUrl, writers = writers)
            prelude.forEach { seqStore.insert(it) }
            val sequential =
                batch.map { ev ->
                    try {
                        seqStore.insert(ev)
                        IEventStore.InsertOutcome.Accepted
                    } catch (e: Exception) {
                        IEventStore.InsertOutcome.Rejected(e.message ?: "insert failed")
                    }
                }
            assertEquals(sequential, bulk, "outcomes diverge from the per-event path")
            assertEquals(seqIndex.search(EventQuery()).map { it.id }.toSet(), bulkIndex.search(EventQuery()).map { it.id }.toSet(), "stored documents diverge")
            bulk
        }

    /**
     * A tombstone by owner X can only cover X's own events (NIP-09), so the
     * preload's tombstone queries pair each owner chunk with that chunk's own
     * records' ids and addresses. They used to carry EVERY id and address in
     * the batch, unchunked, in every owner chunk's query — a deleter's chunk
     * asked about a stranger's events, and a 20,000-event batch's ids rode one
     * YQL under both locks. A gift wrap is keyed by its RECIPIENT, as the
     * record path keys it.
     */
    @Test
    fun `the tombstone preload asks each owner only about its own events`() =
        runBlocking {
            val index = RecordingIndex(InMemoryEventIndex())
            // SINGLE_WRITER, so the guard cache knows alice is the only deleter:
            // under SHARED_STRICT every owner is flagged and lands in one chunk.
            val store = NostrSemanticsStore(index, relay = relayUrl, writers = WriterTopology.SINGLE_WRITER)
            store.insert(deletion(alice, ids = listOf("ee".repeat(32))))
            val wrap = GiftWrapEvent(id(), stranger, next(), arrayOf(arrayOf("p", alice)), "sealed", "")
            val batch =
                (1..8).map { note(alice) } + (1..8).map { note(bob) } +
                    listOf(article(alice, "a-post"), article(bob, "b-post"), wrap, deletion(bob, ids = listOf("dd".repeat(32))))
            index.queries.clear()

            store.batchInsert(batch)

            val tombstoneQueries = index.queries.filter { it.kinds == listOf(DeletionRequestEvent.KIND) && it.tags.isNotEmpty() }
            assertTrue(tombstoneQueries.isNotEmpty(), "alice is a flagged deleter, so her tombstones must be preloaded")
            for (q in tombstoneQueries) {
                val owned = batch.filter { it.owner() in q.authors && it !is DeletionRequestEvent }
                val keys = owned.map { it.id }.toSet() + owned.mapNotNull { it.addressTagOrNull() }
                val asked = q.tags.values.flatten()
                assertTrue(keys.containsAll(asked), "a query for ${q.authors} asked about ${asked - keys} — events none of them own")
            }
            val asked = tombstoneQueries.flatMap { it.tags.values.flatten() }.toSet()
            assertTrue(wrap.id in asked, "the gift wrap is its recipient's to delete, so alice's query must name it")
        }

    /** The narrowed preload still blocks everything a stored tombstone covers: by id, by address, a gift wrap by its recipient. */
    @Test
    fun `stored tombstones still block their targets in a mixed batch`() {
        for (writers in listOf(WriterTopology.SINGLE_WRITER, WriterTopology.SHARED_STRICT)) {
            val target = note(alice, at = 400)
            val post = article(alice, "gone", at = 500)
            val wrap = GiftWrapEvent(id(), stranger, 600, arrayOf(arrayOf("p", alice)), "sealed", "")
            val tomb = deletion(alice, at = 700, ids = listOf(target.id, wrap.id), addresses = listOf(post.addressTag()))
            val batch =
                (1..12).map { note(bob) } +
                    listOf(target, post, wrap, note(alice), deletion(bob, ids = listOf("dd".repeat(32))))
            val outcomes = assertMixedMatchesSequential(prelude = listOf(tomb), batch = batch, writers = writers)
            for (blocked in listOf(target, post, wrap)) {
                assertEquals(IEventStore.InsertOutcome.Rejected(Rejections.DELETED), outcomes[batch.indexOf(blocked)], "$writers: ${blocked.kind} must stay deleted")
            }
        }
    }
}
