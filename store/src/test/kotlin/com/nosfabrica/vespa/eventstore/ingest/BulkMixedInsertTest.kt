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
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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

    /**
     * A deployment whose query profile caps `maxHits` (`VESPA_UNBOUNDED_HITS`):
     * a search with NO limit comes back holding at most [cap] hits, newest
     * first, and nothing says it was cut. A limit'd search is served whole.
     */
    private class HitsCappedIndex(
        private val inner: InMemoryEventIndex,
        private val cap: Int,
    ) : EventIndex by inner {
        val queries = CopyOnWriteArrayList<EventQuery>()

        override suspend fun search(query: EventQuery): List<EventDoc> {
            queries += query
            val all = inner.search(query)
            return if (query.limit == null && query.ids.isEmpty()) all.take(cap) else all
        }

        override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
    }

    private fun vanish(
        author: String,
        at: Long,
    ) = RequestToVanishEvent(id(), author, at, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", "")

    /** The same [prelude] + [batch] through batchInsert and through insert, one by one: outcomes and stored ids must agree. */
    private fun assertMixedMatchesSequential(
        prelude: List<Event>,
        batch: List<Event>,
        writers: WriterTopology,
        sweepPage: Int = 10_000,
        addressKeyed: Boolean = false,
    ): List<IEventStore.InsertOutcome> =
        runBlocking {
            assertTrue(batch.size >= 16 && batch.any { it is DeletionRequestEvent || it is RequestToVanishEvent }, "the batch must take the bulk mixed path")
            val bulkIndex = InMemoryEventIndex(supersedesViaPut = addressKeyed)
            val bulkStore = NostrSemanticsStore(bulkIndex, relay = relayUrl, writers = writers, sweepPage = sweepPage)
            prelude.forEach { bulkStore.insert(it) }
            val bulk = bulkStore.batchInsert(batch)

            val seqIndex = InMemoryEventIndex(supersedesViaPut = addressKeyed)
            val seqStore = NostrSemanticsStore(seqIndex, relay = relayUrl, writers = writers, sweepPage = sweepPage)
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

    /**
     * A vanish in a mixed batch erases the owner's WHOLE history up to it, the
     * per-event path's paged sweep, not the part one unlimited preload query
     * happened to return. That query was the batch's one read with no bound —
     * a prolific owner's every event materialized in the snapshot under both
     * locks, then removed in one call — and on a deployment that caps
     * unlimited hits it came back CUT, so the replay erased what it had loaded
     * and reported the vanish Accepted over the rest.
     */
    @Test
    fun `a vanish in a mixed batch erases the owner's history past a hits cap`() =
        runBlocking {
            val index = HitsCappedIndex(InMemoryEventIndex(), cap = 10)
            val store = NostrSemanticsStore(index, relay = relayUrl, sweepPage = 4)
            val history = (1..25).map { note(alice, at = 100L + it) }
            history.forEach { store.insert(it) }
            val bobs = (1..15).map { note(bob) }
            index.queries.clear()

            val outcomes = store.batchInsert(bobs + vanish(alice, at = 1_000))

            assertTrue(outcomes.all { it == IEventStore.InsertOutcome.Accepted }, "every event is admitted, got $outcomes")
            val survivors = history.filter { index.get(it.id) != null }
            assertEquals(emptyList(), survivors.map { it.createdAt }, "the vanish must erase all of alice's history, not the first hits-cap of it")
            assertTrue(bobs.all { index.get(it.id) != null }, "nobody else's events are touched")
            val unbounded = index.queries.filter { it.owners.isNotEmpty() && it.limit == null }
            assertEquals(emptyList(), unbounded, "no read of an owner's history may go out without a page bound")
        }

    /**
     * The mixed path's vanish horizon is the per-event one (STORE-D07): the owner's history is
     * erased strictly before the vanish's created_at, both what was stored before the batch and
     * what the batch put ahead of the vanish, and an event at that second offered after the
     * vanish is blocked.
     */
    @Test
    fun `a vanish in a mixed batch spares the owner's events at its own second`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val storedBefore = note(alice, at = 999)
            val storedAt = note(alice, at = 1_000)
            store.insert(storedBefore)
            store.insert(storedAt)
            val aheadAt = note(alice, at = 1_000)
            val behindAt = note(alice, at = 1_000)
            val batch = listOf(note(bob), aheadAt, vanish(alice, at = 1_000), behindAt)
            val outcomes = store.batchInsert(batch)
            assertEquals(IEventStore.InsertOutcome.Accepted, outcomes[1])
            assertTrue(outcomes[3] is IEventStore.InsertOutcome.Rejected, "an event at the vanish's second, after it, is blocked: ${outcomes[3]}")
            assertEquals(null, index.get(storedBefore.id), "history before the vanish is erased")
            assertTrue(index.get(storedAt.id) != null, "a stored event at the vanish's second survives")
            assertTrue(index.get(aheadAt.id) != null, "an event at that second put ahead of the vanish survives")
            assertEquals(null, index.get(behindAt.id))
            store.close()
        }

    /**
     * Parity with the per-event path around a vanish, with a sweep page far
     * below the history: events before it in the batch are admitted and then
     * erased, older ones after it are vanished, newer ones stay, a stored
     * profile the batch supersedes is erased by the vanish rather than by the
     * supersession, a gift wrap is its recipient's to vanish, and the vanish
     * survives its own erase.
     */
    @Test
    fun `a vanish in a mixed batch matches the per-event path`() {
        val stored =
            (1..25).map { note(alice, at = 100L + it) } +
                listOf(
                    Event(id(), alice, 50, 0, emptyArray(), "{}", ""),
                    GiftWrapEvent(id(), stranger, 60, arrayOf(arrayOf("p", alice)), "sealed", ""),
                    note(bob, at = 70),
                )
        val batch =
            (1..12).map { note(bob) } +
                listOf(
                    note(alice, at = 90),
                    vanish(alice, at = 1_000),
                    note(alice, at = 500),
                    note(alice, at = 2_000),
                    Event(id(), alice, 1_500, 0, emptyArray(), "{\"name\":\"new\"}", ""),
                    GiftWrapEvent(id(), stranger, 800, arrayOf(arrayOf("p", alice)), "late", ""),
                )
        for (writers in listOf(WriterTopology.SINGLE_WRITER, WriterTopology.SHARED_STRICT)) {
            assertMixedMatchesSequential(prelude = stored, batch = batch, writers = writers, sweepPage = 4)
        }
    }

    /**
     * The mixed path's write order: the batch's surviving RECORDS land before
     * the versions they supersede are removed, so a failed put leaves the old
     * version stored instead of neither. Its tombstones and vanish requests
     * still land last, after the erase they stand for (see [BulkMixedInsert.run]).
     */
    @Test
    fun `a failed put in a mixed batch never loses the version it was replacing`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val failing = AtomicBoolean(false)
            val index =
                object : EventIndex by inner {
                    override suspend fun put(doc: EventDoc) {
                        check(!(failing.get() && doc.kind == LongFormContentEvent.KIND)) { "put refused" }
                        inner.put(doc)
                    }

                    override suspend fun putAll(docs: List<EventDoc>) = docs.forEach { put(it) }

                    override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
                }
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val v1 = article(alice, "post")
            store.insert(v1)
            val v2 = article(alice, "post")
            failing.set(true)
            assertFailsWith<IllegalStateException> {
                store.batchInsert((1..16).map { note(bob) } + listOf(v2, deletion(bob, ids = listOf("dd".repeat(32)))))
            }
            assertEquals(
                listOf(v1.id),
                inner.search(EventQuery(kinds = listOf(LongFormContentEvent.KIND), authors = listOf(alice))).map { it.id },
                "the replaced version must still be stored",
            )
        }

    /**
     * ADDRESS-KEYED, a mixed batch that supersedes a stored address keeps its
     * winner. The engine stores a replaceable at its address, so the winner's
     * put replaced the old version in place, and the sweep that followed —
     * removing the old version by its docid, which is that same address —
     * deleted the winner: the address ended EMPTY while the batch answered
     * Accepted. The reference keyed everything by id whatever its flag said,
     * so no unit test could see it; it keys by address now.
     */
    @Test
    fun `an address-keyed mixed batch keeps the winner of every address it supersedes`() {
        for (addressKeyed in listOf(false, true)) {
            val oldPost = article(alice, "post", at = 100)
            val oldProfile = Event(id(), alice, 110, 0, emptyArray(), "{}", "")
            val newPost = article(alice, "post", at = 200)
            val newProfile = Event(id(), alice, 210, 0, emptyArray(), "{\"name\":\"new\"}", "")
            val batch = (1..14).map { note(bob) } + listOf(newPost, newProfile, deletion(bob, ids = listOf("dd".repeat(32))))
            val outcomes = assertMixedMatchesSequential(prelude = listOf(oldPost, oldProfile), batch = batch, writers = WriterTopology.SINGLE_WRITER, addressKeyed = addressKeyed)
            assertEquals(IEventStore.InsertOutcome.Accepted, outcomes[batch.indexOf(newPost)], "addressKeyed=$addressKeyed")
            runBlocking {
                val index = InMemoryEventIndex(supersedesViaPut = addressKeyed)
                val store = NostrSemanticsStore(index, relay = relayUrl, writers = WriterTopology.SINGLE_WRITER)
                listOf(oldPost, oldProfile).forEach { store.insert(it) }
                store.batchInsert(batch)
                assertEquals(
                    setOf(newPost.id, newProfile.id),
                    index.search(EventQuery(kinds = listOf(0, LongFormContentEvent.KIND), authors = listOf(alice))).map { it.id }.toSet(),
                    "addressKeyed=$addressKeyed: each superseded address holds its winner",
                )
            }
        }
    }

    /**
     * The preload reads a stored replaceable's VERSIONS as stubs, not as
     * documents: a kind 3 is 50-500 KB of tags, and with BULK_MIN at 2 every
     * batch carrying a kind 5 takes this path, which read each one whole under
     * both locks (the per-event path such a batch used to take reads stubs).
     * The outcome is the per-event path's.
     */
    @Test
    fun `the mixed preload reads stored replaceable versions without their documents`() {
        val followsOld = Event(id(), alice, 100, 3, Array(50) { arrayOf("p", it.toString(16).padStart(64, '0')) }, "", "")
        val followsNew = Event(id(), alice, 200, 3, arrayOf(arrayOf("p", bob)), "", "")
        val postOld = article(alice, "post", at = 100)
        val postNew = article(alice, "post", at = 200)
        val batch = (1..13).map { note(bob) } + listOf(followsNew, postNew, deletion(alice, addresses = listOf("3:$alice:")))
        assertMixedMatchesSequential(prelude = listOf(followsOld, postOld), batch = batch, writers = WriterTopology.SINGLE_WRITER)
        runBlocking {
            val index = RecordingIndex(InMemoryEventIndex())
            val store = NostrSemanticsStore(index, relay = relayUrl, writers = WriterTopology.SINGLE_WRITER)
            listOf(followsOld, postOld).forEach { store.insert(it) }
            index.queries.clear()
            store.batchInsert(batch)
            val documentReads = index.queries.filter { q -> q.ids.isEmpty() && q.kinds.any { it == 3 || it == LongFormContentEvent.KIND } }
            assertEquals(emptyList(), documentReads, "a stored version is read as a stub (id, created_at, d), never as a document")
        }
    }

    /**
     * A mixed batch whose kind 5 removes nothing and carries no vanish writes
     * its records and its tombstone in ONE putAll: the guards land after the
     * erase they stand for, and with no erase there is nothing to wait for.
     * With one, they still land apart, after it.
     */
    @Test
    fun `a mixed batch with nothing to erase writes records and guards together`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val puts = CopyOnWriteArrayList<List<String>>()
            val index =
                object : EventIndex by inner {
                    override suspend fun putAll(docs: List<EventDoc>) {
                        puts += docs.map { it.id }
                        inner.putAll(docs)
                    }

                    override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
                }
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val target = note(bob)
            store.insert(target)
            puts.clear()

            store.batchInsert((1..5).map { note(bob) } + deletion(bob, ids = listOf("dd".repeat(32))))
            assertEquals(1, puts.size, "one write for records and tombstone: $puts")

            puts.clear()
            store.batchInsert((1..5).map { note(bob) } + deletion(bob, ids = listOf(target.id)))
            assertEquals(2, puts.size, "a tombstone that erases lands after the erase: $puts")
            assertEquals(null, inner.get(target.id))
        }

    /** Lands every write, then throws for one carrying a kind in [kinds]: a timeout after the feed accepted it. */
    private class LandsThenThrows(
        val inner: InMemoryEventIndex,
        private val kinds: Set<Int>,
    ) : EventIndex by inner {
        var armed = true

        override suspend fun put(doc: EventDoc) {
            inner.put(doc)
            check(!(armed && doc.kind in kinds)) { "timeout after landing" }
        }

        override suspend fun putAll(docs: List<EventDoc>) {
            inner.putAll(docs)
            check(!(armed && docs.any { it.kind in kinds })) { "timeout after landing" }
        }

        override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
    }

    /**
     * The mixed path's form of a guard write that lands and then fails: the owners were noted
     * only after the write returned, so a tombstone stored by the together-write (records and
     * guards in one putAll) or a vanish stored after its erase flagged nobody, and under
     * SINGLE_WRITER the event it covers was admitted afterwards for good.
     */
    @Test
    fun `a mixed batch's guard that lands and then fails still flags its owner`() =
        runBlocking {
            for (kind in listOf(DeletionRequestEvent.KIND, RequestToVanishEvent.KIND)) {
                val index = LandsThenThrows(InMemoryEventIndex(), setOf(kind))
                val store = NostrSemanticsStore(index, relay = relayUrl, writers = WriterTopology.SINGLE_WRITER)
                store.insert(note(alice)) // loads the guard filters, with nobody in them
                val target = note(alice, at = 1_000)
                val guard =
                    if (kind == DeletionRequestEvent.KIND) {
                        // Names an event not stored yet: nothing to erase, so records and guard share one write.
                        deletion(alice, at = 2_000, ids = listOf(target.id))
                    } else {
                        RequestToVanishEvent(id(), alice, 2_000, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", "")
                    }
                assertFailsWith<IllegalStateException> { store.batchInsert(listOf(note(bob), guard)) }
                index.armed = false
                assertTrue(index.inner.get(guard.id) != null, "the guard landed (kind $kind)")

                val outcome = store.batchInsert(listOf(target)).single()
                assertTrue(outcome is IEventStore.InsertOutcome.Rejected, "the covered event is blocked (kind $kind), got $outcome")
                assertEquals(null, index.inner.get(target.id), "and not stored (kind $kind)")
                store.close()
            }
        }

    /**
     * The replay's guard object is the snapshot's, which caches nothing whatever the operator
     * set, so building it per batch must not read `GUARD_OWNERS_DISABLE`: it re-parsed the
     * variable for every mixed batch, and an unparseable value failed every one of them even
     * on a store built with an explicit setting, which never reads it.
     */
    @Test
    fun `a mixed batch's replay never reads the guard-cache switch`() =
        runBlocking {
            val reads = AtomicInteger()
            val saved = GuardOwners.environment
            try {
                val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl, guardOwnersDisabled = false)
                GuardOwners.environment = {
                    reads.incrementAndGet()
                    "not a boolean"
                }
                val outcomes = store.batchInsert((1..5).map { note(bob) } + deletion(bob, ids = listOf("dd".repeat(32))))
                assertTrue(outcomes.all { it == IEventStore.InsertOutcome.Accepted }, "$outcomes")
                assertEquals(0, reads.get(), "the environment is read once, by the store that asks for it")
                store.close()
            } finally {
                GuardOwners.environment = saved
            }
        }
}
