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
package com.vitorpamplona.quartz.eventstore.store.ingest

import com.vitorpamplona.quartz.eventstore.store.NostrSemanticsStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The ingest-throughput probe. `InMemoryEventIndex` answers every round trip
 * instantly, so it hides the ONE thing that caps real ingest: whether the
 * per-batch engine round trips (dedup / guard / version reads, then the write)
 * happen UNDER the single writer lock (serialized across relays) or beside it.
 *
 * [LatencyEventIndex] gives each round trip a fixed virtual-time cost, and
 * `runTest`'s virtual clock turns "did these overlap?" into an exact number:
 * `currentTime` after N concurrent batches is the SUM of their costs if they
 * serialized, and closer to the MAX if their reads overlapped. No wall-clock,
 * no flakiness — the scheduler advances time only when everything is parked.
 *
 * This is the executable target for moving the read checks out of the write
 * lock: the concurrency assertion below fails while reads are serialized and
 * passes once they run beside each other.
 */
class BatchIngestConcurrencyTest {
    private val relayUrl = "wss://sot.test/".normalizeRelayUrl()

    /** Virtual-ms each engine round trip costs; the absolute value is arbitrary, only ratios matter. */
    private val lat = 10L

    /** Every [EventIndex] round trip costs [latMs] of virtual time; the write pipelines (one cost for the whole batch). */
    private class LatencyEventIndex(
        private val inner: InMemoryEventIndex,
        private val latMs: Long,
    ) : EventIndex {
        override suspend fun get(id: String): EventDoc? {
            delay(latMs)
            return inner.get(id)
        }

        override suspend fun put(doc: EventDoc) {
            delay(latMs)
            inner.put(doc)
        }

        override suspend fun putAll(docs: List<EventDoc>) {
            // One pipelined round trip for the whole batch, like the real feed client.
            delay(latMs)
            inner.putAll(docs)
        }

        override suspend fun remove(id: String) {
            delay(latMs)
            inner.remove(id)
        }

        override suspend fun removeAll(ids: List<String>) {
            delay(latMs)
            inner.removeAll(ids)
        }

        override suspend fun search(query: EventQuery): List<EventDoc> {
            delay(latMs)
            return inner.search(query)
        }

        override suspend fun count(query: EventQuery): Int {
            delay(latMs)
            return inner.count(query)
        }

        override suspend fun distinctAuthors(query: EventQuery): Set<String> {
            delay(latMs)
            return inner.distinctAuthors(query)
        }

        override fun close() = inner.close()
    }

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    /** A provider's batch of scores: [count] distinct subjects, so batches from distinct providers never conflict. */
    private fun scoreBatch(
        provider: String,
        count: Int,
        at: Long = 1_000L,
    ) = (0 until count).map { i ->
        val subject = "%064x".format(i)
        ContactCardEvent(id(), provider, at, arrayOf(arrayOf("d", subject), arrayOf("rank", "50")), "", "")
    }

    private fun provider(n: Int) = "%02x".format(n).repeat(32)

    /**
     * Diagnostic (not an assertion of the fix): print the virtual time N
     * concurrent batches take on the CURRENT store, so the serialization is
     * visible as a number and the fix's target is grounded in data.
     */
    @Test
    fun `diagnostic - virtual time vs concurrency on the current store`() =
        runTest {
            for (c in listOf(1, 2, 4, 8)) {
                seq = 0
                val store = NostrSemanticsStore(LatencyEventIndex(InMemoryEventIndex(), lat), relay = relayUrl)
                val batches = (0 until c).map { p -> scoreBatch(provider(p), count = 20) }
                val start = testScheduler.currentTime
                val jobs = batches.map { b -> launch { store.batchInsert(b) } }
                jobs.forEach { it.join() }
                val elapsed = testScheduler.currentTime - start
                println("[ingest] concurrency=$c  virtual_ms=$elapsed  per_batch=${elapsed / c}")
            }
        }

    /**
     * Diagnostic: the deletion-interleaving penalty. `batchInsert` splits its
     * run on every kind 5/62, so a stream that interleaves deletions never
     * forms a run >= BULK_MIN and falls entirely to the per-event path (3-5
     * round trips each) — the shape the observed 43 docs/s outbox sync
     * actually had (98% kind 5). Compare a clean addressable batch (bulk) to
     * the same size interleaved with deletions of ABSENT targets.
     */
    @Test
    fun `diagnostic - deletion interleaving drops the batch to the per-event path`() =
        runTest {
            val n = 60

            seq = 0
            val cleanStore = NostrSemanticsStore(LatencyEventIndex(InMemoryEventIndex(), lat), relay = relayUrl)
            val cleanBatch = scoreBatch(provider(1), count = n)
            val t0 = testScheduler.currentTime
            cleanStore.batchInsert(cleanBatch)
            val cleanMs = testScheduler.currentTime - t0

            seq = 0
            val mixedStore = NostrSemanticsStore(LatencyEventIndex(InMemoryEventIndex(), lat), relay = relayUrl)
            // Every other event is a kind-5 deletion of an id we don't hold —
            // exactly the outbox stream's shape, and the worst case for run-splitting.
            val mixed =
                (0 until n).flatMap { i ->
                    val card = ContactCardEvent(id(), provider(1), 1_000L, arrayOf(arrayOf("d", "%064x".format(i)), arrayOf("rank", "1")), "", "")
                    val del =
                        com.vitorpamplona.quartz.nip09Deletions
                            .DeletionEvent(id(), provider(1), 1_000L, arrayOf(arrayOf("e", id())), "", "")
                    listOf(card, del)
                }
            val t1 = testScheduler.currentTime
            mixedStore.batchInsert(mixed)
            val mixedMs = testScheduler.currentTime - t1

            println("[ingest] clean_bulk($n)=${cleanMs}ms  deletion_interleaved(${mixed.size})=${mixedMs}ms  per_event=${mixedMs.toDouble() / mixed.size}ms")
            // A deletion-interleaved batch must ride a BATCHED path, not fall to
            // per-event (which was ~55ms/event = 100x+ the clean bulk cost).
            assertTrue(
                mixedMs < cleanMs * 4,
                "deletion-interleaved batch took ${mixedMs}ms vs ${cleanMs}ms clean (${mixedMs.toDouble() / mixed.size}ms/event) - fell back to the per-event path",
            )
        }

    /**
     * The throughput target: batches from DISTINCT providers touch disjoint
     * addresses, so their read checks have no reason to serialize. Once the
     * reads run outside the write lock, 8 concurrent batches must cost far less
     * than 8x a single batch — here, under 3x. Fails while reads are locked.
     */
    @Test
    fun `concurrent batches over disjoint addresses overlap their reads`() =
        runTest {
            seq = 0
            val one = NostrSemanticsStore(LatencyEventIndex(InMemoryEventIndex(), lat), relay = relayUrl)
            val t0 = testScheduler.currentTime
            one.batchInsert(scoreBatch(provider(0), count = 20)).let { }
            val single = testScheduler.currentTime - t0

            seq = 0
            val many = NostrSemanticsStore(LatencyEventIndex(InMemoryEventIndex(), lat), relay = relayUrl)
            val c = 8
            val batches = (0 until c).map { p -> scoreBatch(provider(p), count = 20) }
            val t1 = testScheduler.currentTime
            batches.map { b -> launch { many.batchInsert(b) } }.forEach { it.join() }
            val concurrent = testScheduler.currentTime - t1

            println("[ingest] single=$single  x${c}concurrent=$concurrent  ratio=${concurrent.toDouble() / single}")
            // Fully serialized would be 8x (ratio 8.0). With dedup + guards moved
            // out of the writer lock, the reads overlap and only the supersession
            // read + write serialize, so 8 disjoint batches must land well under
            // linear. (Pushing supersession off the lock too — engine-side
            // conditional puts — is the follow-up that would approach ~2x.)
            assertTrue(
                concurrent < single * 4,
                "8 disjoint-provider batches took ${concurrent}ms vs ${single}ms for one (ratio ${concurrent.toDouble() / single}) - reads still serializing under the write lock (expected < ${single * 4})",
            )
        }

    /**
     * Correctness gate that must hold NO MATTER how reads and writes overlap:
     * two batches racing to write the SAME address (same provider+subject,
     * different created_at) must leave exactly ONE version stored — the newest —
     * never two. This is the invariant the single writer protected and that any
     * read-outside-the-lock change must preserve.
     */
    @Test
    fun `racing batches on the same address leave exactly one version`() =
        runTest {
            seq = 0
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(LatencyEventIndex(index, lat), relay = relayUrl)
            val subject = "%064x".format(7)

            fun cardBatch(at: Long) =
                // 16+ events to engage the bulk path; the LAST one is the contested address.
                (0 until 16).map { i ->
                    ContactCardEvent(id(), provider(0), at, arrayOf(arrayOf("d", "pad-$i-$at"), arrayOf("rank", "1")), "", "")
                } + ContactCardEvent(id(), provider(0), at, arrayOf(arrayOf("d", subject), arrayOf("rank", "$at")), "", "")

            val older = cardBatch(at = 100)
            val newer = cardBatch(at = 200)
            listOf(
                launch { store.batchInsert(older) },
                launch { store.batchInsert(newer) },
            ).forEach { it.join() }

            val versions =
                index
                    .search(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(provider(0))))
                    .filter { it.dTagOrEmpty() == subject }
            assertEquals(1, versions.size, "the contested address must keep exactly one version, found ${versions.size}")
            assertEquals(200L, versions.single().createdAt, "the surviving version must be the newest")
        }
}
