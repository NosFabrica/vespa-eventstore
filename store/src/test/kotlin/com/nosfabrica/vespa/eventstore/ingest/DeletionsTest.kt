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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.client.RecencyStrategy
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The NIP-09 guard probe is ONE engine query per target style, whatever the
 * recency strategy — it is an existence check on the insert hot path, and a
 * time window can only add a round trip to it.
 *
 * The case that regressed: a BACKFILLED event (older than the speculative
 * strategy's first window). Its probe's `since` is the event's own
 * created_at, so the first window was tighter, came back empty, and the real
 * probe followed — two queries instead of one, per probe, per insert, under
 * the default SHARED_STRICT topology. The probe now opts out of planning.
 */
class DeletionsTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE)
    private val deletions = Deletions(index, relay = null, sweepPage = 100)
    private val alice = "a1".repeat(32)
    private val now = System.currentTimeMillis() / 1000
    private var seq = 0

    @AfterTest
    fun stop() {
        index.close()
        mock.stop()
    }

    private fun id() = (++seq).toString(16).padStart(64, '0')

    @Test
    fun `a backfilled event's guard probe is one query`() =
        runBlocking {
            val old = Event(id(), alice, now - 30 * 86_400L, 1, emptyArray(), "a month old", "")
            val before = mock.searchRequests.size
            assertFalse(deletions.isDeleted(old))
            val requests = mock.searchRequests.drop(before)
            val sent = requests.map { it.getValue("yql") }
            assertEquals(1, sent.size, "one probe, never a window first: $sent")
            assertTrue(sent.none { it.contains("created_at >= ${now - 3_600}") }, "no speculative window: $sent")
            // An existence question reads no document: a count, zero hits.
            assertTrue(sent.single().contains("all(output(count()))"), "the probe is a count: $sent")
            assertEquals("0", requests.single()["hits"], "and asks for no hit: ${requests.single()}")
        }

    @Test
    fun `the probe still finds the tombstone`() =
        runBlocking {
            val old = Event(id(), alice, now - 30 * 86_400L, 1, emptyArray(), "deleted later", "")
            val tombstone = DeletionRequestEvent(id(), alice, now - 29 * 86_400L, arrayOf(arrayOf("e", old.id)), "", "")
            index.put(tombstone.toDoc())
            assertTrue(deletions.isDeleted(old), "a same-owner kind 5 newer than the event blocks it")
        }

    /**
     * A replaceable or addressable event's guard is TWO probes, by id and by
     * address, and they are independent: they run together, so the insert path
     * under the writer lock waits for one round trip, not two. Proven by an index
     * that answers neither until both have arrived — a sequential guard never
     * sends the second, and times out.
     */
    @Test
    fun `the id and address probes of a guard run together`() =
        runBlocking {
            val bothAsked = CompletableDeferred<Unit>()
            var asked = 0
            val inner = InMemoryEventIndex()
            val rendezvous =
                object : EventIndex by inner {
                    override suspend fun count(query: EventQuery): Int {
                        if (query.kinds == listOf(DeletionRequestEvent.KIND)) {
                            synchronized(this) { if (++asked == 2) bothAsked.complete(Unit) }
                            bothAsked.await()
                        }
                        return inner.count(query)
                    }
                }
            val guard = Deletions(rendezvous, relay = null, sweepPage = 100)
            val profile = Event(id(), alice, now, 0, emptyArray(), "{}", "")
            withTimeout(10_000) { assertFalse(guard.isDeleted(profile)) }
            assertEquals(2, asked)
            val tombstone = DeletionRequestEvent(id(), alice, now + 1, arrayOf(arrayOf("a", "0:$alice:")), "", "")
            inner.put(tombstone.toDoc())
            asked = 0
            val again = Deletions(inner, relay = null, sweepPage = 100)
            assertTrue(again.isDeleted(profile), "an address tombstone still blocks")
        }

    /**
     * The id probe's TRUE is the verdict whatever the address probe does: a
     * failing address probe used to cancel the id probe's scope and fail the
     * insert with it. When the id probe says false, the address probe decides,
     * and its failure is the insert's.
     */
    @Test
    fun `a failed address probe does not undo the id probe's verdict`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val failingAddressProbe =
                object : EventIndex by inner {
                    override suspend fun search(query: EventQuery): List<EventDoc> {
                        check(!(query.kinds == listOf(DeletionRequestEvent.KIND) && "a" in query.tags)) { "address probe refused" }
                        // The id probe answers after the address probe has failed.
                        if (query.kinds == listOf(DeletionRequestEvent.KIND)) delay(50)
                        return inner.search(query)
                    }

                    override suspend fun count(query: EventQuery): Int {
                        check(!(query.kinds == listOf(DeletionRequestEvent.KIND) && "a" in query.tags)) { "address probe refused" }
                        if (query.kinds == listOf(DeletionRequestEvent.KIND)) delay(50)
                        return inner.count(query)
                    }
                }
            val profile = Event(id(), alice, now, 0, emptyArray(), "{}", "")
            inner.put(DeletionRequestEvent(id(), alice, now + 1, arrayOf(arrayOf("e", profile.id)), "", "").toDoc())
            val guard = Deletions(failingAddressProbe, relay = null, sweepPage = 100)

            assertTrue(guard.isDeleted(profile), "the id tombstone blocks it")

            val other = Event(id(), alice, now, 0, emptyArray(), "{\"v\":2}", "")
            assertFailsWith<IllegalStateException> { guard.isDeleted(other) }
            Unit
        }

    /**
     * [Deletions.sweepWalk] hands each batch to its remover inside one hold,
     * and what the remover leaves stays stored — the expiry sweep's plain
     * pass leaves the trust kinds.
     */
    @Test
    fun `a sweep walk removes what its remover removes, a batch per hold`() =
        runBlocking {
            val memory = InMemoryEventIndex()
            val walker = Deletions(memory, relay = null, sweepPage = 100)
            val notes = (1..7).map { Event(id(), alice, 1_000L + it, 1, emptyArray(), "n$it", "") }
            val cards = (1..3).map { Event(id(), alice, 2_000L + it, 30382, arrayOf(arrayOf("d", "%064x".format(it))), "", "") }
            memory.putAll((notes + cards).map { it.toDoc() })
            val cardIds = cards.mapTo(HashSet()) { it.id }
            var holds = 0

            walker.sweepWalk(EventQuery(), page = 3, remove = { ids -> ids.filterNot { it in cardIds }.also { memory.removeAll(it) } }) { batch ->
                holds++
                batch()
            }

            assertEquals(cardIds, memory.search(EventQuery()).mapTo(HashSet()) { it.id }, "only what the remover left")
            assertEquals(4, holds, "ten ids, three per hold")
        }

    /**
     * An a-tag deletion removes its victims by what an ADDRESS fixes (kind, author, d) and what a
     * version walk carries (id, created_at), so it reads them as stubs, never as documents: a
     * deleted kind 3 is 50-500 KB of tags, and this runs under the writer lock. Only the kinds a
     * decorator reacts to by reading the removed document (a 30382 card, a 10040) are read whole,
     * as the version reads do ([BulkRecordInsert.WHOLE_DOC_KINDS]). The victims are the same either
     * way: the deleted addresses' versions up to the deletion, and nothing beside them.
     */
    @Test
    fun `an a-tag deletion removes its victims without reading their documents`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val read = mutableListOf<EventQuery>()
            val removed = mutableListOf<EventDoc>()
            val recording =
                object : EventIndex by inner {
                    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query).also { read += query }

                    override suspend fun removeDocs(docs: List<EventDoc>) {
                        removed += docs
                        inner.removeDocs(docs)
                    }
                }
            val bulky = Array(400) { arrayOf("p", "%064x".format(it)) }
            val follows = Event(id(), alice, 1_000L, 3, bulky, "", "")
            val article = Event(id(), alice, 1_000L, 30023, arrayOf(arrayOf("d", "post"), arrayOf("title", "t")), "a long article", "")
            val other = Event(id(), alice, 1_000L, 30023, arrayOf(arrayOf("d", "other")), "kept", "")
            val newer = Event(id(), alice, 3_000L, 30023, arrayOf(arrayOf("d", "post2")), "after the deletion", "")
            val card = Event(id(), alice, 1_000L, 30382, arrayOf(arrayOf("d", "b".repeat(64)), arrayOf("rank", "40")), "", "")
            inner.putAll(listOf(follows, article, other, newer, card).map { it.toDoc() })
            val deletion =
                DeletionRequestEvent(
                    id(),
                    alice,
                    2_000L,
                    arrayOf(arrayOf("a", "3:$alice:"), arrayOf("a", "30023:$alice:post"), arrayOf("a", "30023:$alice:post2"), arrayOf("a", "30382:$alice:${"b".repeat(64)}")),
                    "",
                    "",
                )

            Deletions(recording, relay = null, sweepPage = 100).applyDeletion(deletion)

            assertEquals(setOf(follows.id, article.id, card.id), removed.mapTo(HashSet()) { it.id }, "the deleted addresses' versions up to the deletion, and no other")
            assertEquals(setOf(other.id, newer.id), inner.search(EventQuery()).mapTo(HashSet()) { it.id })
            assertEquals(listOf(listOf(30382)), read.map { it.kinds }, "only the card is read as a document: $read")
            assertEquals(card.tags.map { it.toList() }, removed.single { it.id == card.id }.tags, "and handed over whole, for the projection to react to")
            val stub = removed.single { it.id == article.id }
            assertEquals(listOf(30023, alice, alice, 1_000L, "post"), listOf(stub.kind, stub.pubkey, stub.owner, stub.createdAt, stub.dTagOrEmpty()), "a stub carries what removeDocs reads")
        }
}
