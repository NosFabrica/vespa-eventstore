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
import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.VersionCensus
import com.nosfabrica.vespa.eventstore.engine.async.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.ingest.Rejections
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip23LongContent.LongFormContentEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The bulk batchInsert path must be SEMANTICALLY IDENTICAL to feeding the
 * same events one by one through insert(): every test runs its batch through
 * both and compares the outcomes AND the final stored id sets. Batches are
 * sized past BULK_MIN so the fast path actually engages.
 */
class BulkRecordInsertTest {
    private val relayUrl = "wss://sot.test/".normalizeRelayUrl()
    private val alice = "a1".repeat(32)
    private val service = "c3".repeat(32)

    private var t = 1_000_000L
    private var seq = 0

    private fun next() = t++

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(
        at: Long = next(),
        content: String = "hello",
        eventId: String = id(),
    ) = Event(eventId, alice, at, 1, emptyArray(), content, "")

    private fun card(
        subject: String,
        at: Long = next(),
        rank: Int = 50,
        eventId: String = id(),
    ) = UserAssertionEvent(eventId, service, at, arrayOf(arrayOf("d", subject), arrayOf("rank", "$rank")), "", "")

    private fun deletion(
        targetId: String,
        at: Long = next(),
        author: String = alice,
    ) = DeletionRequestEvent(id(), author, at, arrayOf(arrayOf("e", targetId)), "", "")

    private fun article(
        d: String,
        at: Long = next(),
    ) = LongFormContentEvent(id(), alice, at, arrayOf(arrayOf("d", d)), "body", "")

    /** Padding so every batch crosses the BULK_MIN threshold. */
    private fun padding(n: Int) = (1..n).map { note(content = "pad $it") }

    /**
     * The equivalence harness: same [prelude] + [batch] into two fresh
     * stores — bulk vs one-by-one — then outcomes and stored ids must match.
     */
    private fun assertBulkMatchesSequential(
        prelude: List<Event> = emptyList(),
        batch: List<Event>,
        bulkSupersedesViaPut: Boolean = false,
    ) = runBlocking {
        val bulkIndex = InMemoryEventIndex(supersedesViaPut = bulkSupersedesViaPut)
        val bulkStore = NostrSemanticsStore(bulkIndex, relay = relayUrl)
        prelude.forEach { bulkStore.insert(it) }
        val bulkOutcomes = bulkStore.batchInsert(batch)

        val seqIndex = InMemoryEventIndex()
        val seqStore = NostrSemanticsStore(seqIndex, relay = relayUrl)
        prelude.forEach { seqStore.insert(it) }
        val seqOutcomes =
            batch.map { ev ->
                try {
                    seqStore.insert(ev)
                    IEventStore.InsertOutcome.Accepted
                } catch (e: Exception) {
                    IEventStore.InsertOutcome.Rejected(e.message ?: "insert failed")
                }
            }

        assertEquals(seqOutcomes.map { it::class.simpleName }, bulkOutcomes.map { it::class.simpleName }, "outcome kinds diverge")
        seqOutcomes.zip(bulkOutcomes).forEachIndexed { i, (s, b) ->
            if (s is IEventStore.InsertOutcome.Rejected && b is IEventStore.InsertOutcome.Rejected) {
                assertEquals(s.reason, b.reason, "rejection reason diverges at $i")
            }
        }
        val bulkIds = bulkIndex.search(EventQuery()).map { it.id }.toSet()
        val seqIds = seqIndex.search(EventQuery()).map { it.id }.toSet()
        assertEquals(seqIds, bulkIds, "stored documents diverge")
        assertTrue(batch.size >= 16, "test batch must engage the bulk path")
    }

    @Test
    fun `plain notes, duplicates in and across batches`() {
        val existing = note()
        val dupInBatch = note()
        assertBulkMatchesSequential(
            prelude = listOf(existing),
            batch = padding(14) + listOf(existing, dupInBatch, dupInBatch),
        )
    }

    /**
     * A second copy of an id in one batch takes the FIRST copy's verdict, as the per-event path
     * gives it: a copy of a blocked or `replaced:` event is not stored either, so answering it
     * `duplicate:` (OK true) told the client an event was stored that never was. A copy of an
     * accepted event is the duplicate it always was.
     */
    @Test
    fun `an in-batch copy takes the first copy's verdict`() {
        val covered = note()
        val newer = Event(id(), alice, next(), 0, emptyArray(), "{\"v\":2}", "")
        val older = Event(id(), alice, newer.createdAt - 100, 0, emptyArray(), "{\"v\":1}", "")
        val fresh = note()
        assertBulkMatchesSequential(
            prelude = listOf(deletion(covered.id, at = covered.createdAt + 10), newer),
            batch = padding(16) + listOf(covered, covered, older, older, fresh, fresh),
        )
    }

    @Test
    fun `addressable supersession resolves in run order`() {
        val subject = "9f".repeat(32)
        val older = card(subject, at = 100)
        val newer = card(subject, at = 200)
        val newest = card(subject, at = 300)
        // older-then-newer: both accepted, newest stored; a LATER older one rejected.
        assertBulkMatchesSequential(batch = padding(14) + listOf(older, newer) + listOf(card(subject, at = 50)))
        // newer-first: the older one in the same batch is rejected as replaced.
        assertBulkMatchesSequential(batch = padding(14) + listOf(newest, card(subject, at = 250)))
    }

    @Test
    fun `supersession against an already-stored version`() {
        val subject = "8e".repeat(32)
        val stored = card(subject, at = 500)
        assertBulkMatchesSequential(
            prelude = listOf(stored),
            batch = padding(14) + listOf(card(subject, at = 400), card(subject, at = 600)),
        )
    }

    // The putIfNewer bulk branch (address-keyed engine) must produce the SAME
    // outcomes as the read-then-supersede path — including the tricky cases where
    // a run supersedes in place and where the whole address loses to a stored version.
    @Test
    fun `putIfNewer bulk branch resolves in-run supersession like the read path`() {
        val subject = "7d".repeat(32)
        assertBulkMatchesSequential(batch = padding(14) + listOf(card(subject, at = 100), card(subject, at = 200)) + listOf(card(subject, at = 50)), bulkSupersedesViaPut = true)
        assertBulkMatchesSequential(batch = padding(14) + listOf(card(subject, at = 300), card(subject, at = 250)), bulkSupersedesViaPut = true)
    }

    @Test
    fun `putIfNewer bulk branch loses the whole address to a stored newer version`() {
        val subject = "6c".repeat(32)
        // stored@500 beats both batch versions -> both rejected, even the in-run winner.
        assertBulkMatchesSequential(
            prelude = listOf(card(subject, at = 500)),
            batch = padding(14) + listOf(card(subject, at = 400), card(subject, at = 450)),
            bulkSupersedesViaPut = true,
        )
        // stored@500 loses to a newer batch version -> newest stored.
        assertBulkMatchesSequential(
            prelude = listOf(card(subject, at = 500)),
            batch = padding(14) + listOf(card(subject, at = 400), card(subject, at = 600)),
            bulkSupersedesViaPut = true,
        )
    }

    @Test
    fun `tombstones and vanish requests block bulk inserts`() {
        val victim = note(at = 900)
        val tomb = deletion(victim.id, at = 1_000)
        val vanish = RequestToVanishEvent(id(), alice, 2_000, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", "")
        assertBulkMatchesSequential(
            prelude = listOf(tomb, vanish),
            // the tombstoned id, an old (vanished) note, and fresh (post-vanish) padding
            batch =
                (1..15).map { note(at = 3_000_000L + it) } +
                    listOf(Event(victim.id, alice, 900, 1, emptyArray(), "resurrect?", ""), note(at = 1_500)),
        )
    }

    @Test
    fun `a deletion inside the batch applies at its position`() {
        val early = note()
        val batch =
            padding(15) + listOf(early) +
                listOf(deletion(early.id, at = next())) +
                listOf(Event(early.id, alice, early.createdAt, 1, emptyArray(), early.content, "")) +
                padding(16)
        assertBulkMatchesSequential(batch = batch)
    }

    @Test
    fun `ephemeral and expired events keep their outcomes in bulk`() {
        val ephemeral = Event(id(), alice, next(), 20001, emptyArray(), "gone", "")
        val expired = note(at = next()).let { Event(it.id, alice, 100, 1, arrayOf(arrayOf("expiration", "200")), "old", "") }
        assertBulkMatchesSequential(batch = padding(15) + listOf(ephemeral, expired))
    }

    @Test
    fun `mixed batch of records and deletions of stored and absent targets matches sequential`() {
        // The outbox-stream shape: records interleaved with deletions of
        // already-stored notes (removals) and of ids we never had (tombstone only).
        val stored = (1..4).map { note(at = 900L + it) }
        val batch =
            padding(9) +
                listOf(deletion(stored[0].id, at = next()), deletion(stored[1].id, at = next())) +
                padding(5) +
                listOf(deletion("ab".repeat(32), at = next())) +
                padding(6)
        assertBulkMatchesSequential(prelude = stored, batch = batch)
    }

    /** Kind 30023 with NO d tag: one address per NIP-01 ("30023:pk:"), the shape the d-keyed version query cannot recall. */
    private fun dlessArticle(
        at: Long = next(),
        eventId: String = id(),
    ) = LongFormContentEvent(eventId, alice, at, emptyArray(), "body", "")

    /**
     * Empty-d addressables: a stored version with NO d tag carries no "d:" pair
     * in tag_index, so the bulk version read needs its broad (kind, author)
     * fallback — without it the batch accumulates a second live version and
     * accepts stale ones.
     */
    @Test
    fun `bulk supersession sees stored addressable versions that have no d tag`() {
        val current = dlessArticle(at = 1_000_000_000)
        assertBulkMatchesSequential(
            prelude = listOf(current),
            // A newer version must supersede the stored one; an older one must
            // be REJECTED as replaced — both require recalling the d-less doc.
            batch = padding(20) + dlessArticle(at = 1_000_000_100) + dlessArticle(at = 999_999_900),
        )
    }

    /**
     * The resurrection race: a kind 5 committed by a neighbouring writer BETWEEN
     * this batch's lock-free plan() and its locked commit() must still block the
     * covered event — the guard reads belong to commit, under the lock.
     */
    @Test
    fun `a deletion committed between plan and commit still blocks the batch`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val bulk = BulkRecordInsert(index, relayUrl, GuardOwners(index), nowSecs = { System.currentTimeMillis() / 1000 })

            val target = note(at = 1_000_000_000)
            val batch = padding(20) + target
            val plan = bulk.plan(batch)

            // A neighbouring batch wins the writer lock first and commits a
            // newer tombstone covering the target.
            store.insert(deletion(target.id, at = 1_000_000_500))

            val outcomes = bulk.commit(plan)
            val outcome = outcomes[batch.indexOf(target)]
            assertTrue(
                outcome is IEventStore.InsertOutcome.Rejected && outcome.reason == Rejections.DELETED,
                "a just-deleted event must not be resurrected by a racing batch, got $outcome",
            )
            assertEquals(null, index.get(target.id), "the covered event must not be stored")
        }

    /** As above for NIP-62: a vanish stored between plan and commit voids the early guard read. */
    @Test
    fun `a vanish committed between plan and commit still blocks the batch`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val bulk = BulkRecordInsert(index, relayUrl, GuardOwners(index), nowSecs = { System.currentTimeMillis() / 1000 })

            val target = note(at = 1_000_000_000)
            val batch = padding(20) + target
            val plan = bulk.plan(batch)

            store.insert(RequestToVanishEvent(id(), alice, 1_000_000_500, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))

            val outcome = bulk.commit(plan)[batch.indexOf(target)]
            assertTrue(
                outcome is IEventStore.InsertOutcome.Rejected && outcome.reason == Rejections.VANISHED,
                "a vanished author's event must not be stored by a racing batch, got $outcome",
            )
            assertEquals(null, index.get(target.id))
        }

    /** As above through the mixed bulk path: a whole batch carrying the tombstone commits between plan and commit. */
    @Test
    fun `a mixed batch committed between plan and commit still blocks the batch`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val bulk = BulkRecordInsert(index, relayUrl, GuardOwners(index), nowSecs = { System.currentTimeMillis() / 1000 })

            val target = note(at = 1_000_000_000)
            val batch = padding(20) + target
            val plan = bulk.plan(batch)

            store.batchInsert(padding(5) + deletion(target.id, at = 1_000_000_500))

            val outcome = bulk.commit(plan)[batch.indexOf(target)]
            assertTrue(
                outcome is IEventStore.InsertOutcome.Rejected && outcome.reason == Rejections.DELETED,
                "got $outcome",
            )
            assertEquals(null, index.get(target.id))
        }

    /**
     * Under [WriterTopology.SHARED] a rebuild of the guard filters is how a FOREIGN writer's
     * tombstone reaches this store. One that lands between a narrow batch's plan and its commit
     * changes what the early guard read should have queried (the owner was unflagged when it was
     * read, so nothing was), so the commit must read again: the rebuild counts as a guard write.
     */
    @Test
    fun `a guard rebuild between plan and commit voids the early guard read`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val guards = GuardOwners(index, WriterTopology.SHARED, refreshMillis = 0, override = null)
            val bulk = BulkRecordInsert(index, relayUrl, guards, nowSecs = { System.currentTimeMillis() / 1000 })
            bulk.commit(bulk.plan(padding(2))) // loads the filters: nobody flagged

            val target = note(at = 1_000_000_000)
            val batch = padding(20) + target
            val plan = bulk.plan(batch)
            // Another process stores a tombstone covering the target; this store's rebuild picks it up.
            index.put(deletion(target.id, at = 1_000_000_500).toDoc())
            guards.refresh()

            val outcome = bulk.commit(plan)[batch.indexOf(target)]
            guards.close()
            assertEquals(IEventStore.InsertOutcome.Rejected(Rejections.DELETED), outcome)
            assertEquals(null, index.get(target.id), "the rebuild flagged the owner, so the covered event must not be stored")
        }

    /**
     * A narrow batch reads its guards beside the dedup, so its locked commit asks the engine for
     * nothing before it writes when no guard was stored meanwhile — one round of reads, not two.
     */
    @Test
    fun `a narrow batch reads its guards once, before the lock`() =
        runBlocking {
            val index = CountingSearches(InMemoryEventIndex())
            NostrSemanticsStore(index, relay = relayUrl).insert(deletion("ee".repeat(32), at = 1))
            val bulk = BulkRecordInsert(index, relayUrl, GuardOwners(index), nowSecs = { System.currentTimeMillis() / 1000 })

            val batch = padding(20)
            val plan = bulk.plan(batch)
            assertTrue(index.searches > 0, "the plan reads the guards")
            index.searches = 0
            val outcomes = bulk.commit(plan)

            assertEquals(0, index.searches, "the commit must reuse the plan's guard read")
            assertTrue(outcomes.all { it is IEventStore.InsertOutcome.Accepted })
        }

    /**
     * A stream of narrow batches that are ALL duplicates (a relay syncing what it already holds)
     * must settle to the dedup's one existence read per batch. Under SHARED_STRICT, the default,
     * the early guard read is a tombstone and a vanish search whatever survives, and a batch with
     * no survivor never commits, so reading them early tripled the engine reads of such a stream.
     */
    @Test
    fun `an all-duplicate narrow stream settles to one engine read per batch`() =
        runBlocking {
            val index = CountingSearches(InMemoryEventIndex())
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val batch = padding(20)
            store.batchInsert(batch)
            repeat(5) { store.batchInsert(batch) }
            repeat(5) {
                index.searches = 0
                index.existence = 0
                val outcomes = store.batchInsert(batch)
                assertTrue(outcomes.all { it == IEventStore.InsertOutcome.Rejected(Rejections.DUPLICATE) }, "$outcomes")
                assertEquals(0, index.searches, "a duplicate batch reads no guards")
                assertEquals(1, index.existence, "the dedup is the batch's one engine read")
            }
            store.close()
        }

    /** The other half: once the stream turns fresh again, narrow batches go back to reading their guards beside the dedup. */
    @Test
    fun `a fresh stream after a duplicate one reads its guards early again`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val bulk = BulkRecordInsert(index, relayUrl, GuardOwners(index), nowSecs = { System.currentTimeMillis() / 1000 })
            assertTrue(bulk.plan(padding(20)).earlyGuards != null, "a fresh process reads early")
            val stored = padding(20)
            bulk.commit(bulk.plan(stored))
            repeat(10) { bulk.plan(stored) }
            assertEquals(null, bulk.plan(stored).earlyGuards, "a duplicate stream stops reading early")
            repeat(10) { bulk.commit(bulk.plan(padding(20))) }
            val fresh = bulk.plan(padding(20))
            assertTrue(fresh.earlyGuards != null, "a fresh stream reads early again")
            val outcomes = bulk.commit(fresh)
            assertTrue(outcomes.all { it is IEventStore.InsertOutcome.Accepted }, "$outcomes")
        }

    /** Counts [search] calls, which is how every guard query reaches the engine. */
    private class CountingSearches(
        private val inner: InMemoryEventIndex,
    ) : EventIndex by inner {
        var searches = 0
        var existence = 0

        override suspend fun search(query: EventQuery): List<EventDoc> {
            searches++
            return inner.search(query)
        }

        override suspend fun existingIds(ids: List<String>): Set<String> {
            existence++
            return inner.existingIds(ids)
        }
    }

    /**
     * Records every id removed and put, and runs [afterDedup] once, right after
     * the lock-free dedup read has answered — the window in which a neighbouring
     * writer's commit makes this batch's plan stale.
     */
    private class RacingIndex(
        private val inner: InMemoryEventIndex,
    ) : EventIndex by inner {
        var afterDedup: (suspend () -> Unit)? = null
        val removed = mutableListOf<String>()
        val put = mutableListOf<String>()

        override suspend fun existingIds(ids: List<String>): Set<String> {
            val answer = inner.existingIds(ids)
            afterDedup?.also { afterDedup = null }?.invoke()
            return answer
        }

        override suspend fun put(doc: EventDoc) {
            put += doc.id
            inner.put(doc)
        }

        override suspend fun putAll(docs: List<EventDoc>) = docs.forEach { put(it) }

        override suspend fun remove(id: String) {
            removed += id
            inner.remove(id)
        }

        override suspend fun removeAll(ids: List<String>) = ids.forEach { remove(it) }

        override suspend fun removeDocs(docs: List<EventDoc>) = docs.forEach { remove(it.id) }

        override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
    }

    /**
     * The raced replaceable duplicate: two batches carry the same kind-0, and the
     * other one commits it between this batch's lock-free dedup and its locked
     * commit. The version read in commit then finds the event ITSELF as the
     * address's best. It is a duplicate — what the per-event path, whose dup GET
     * runs under the lock, answers — not a winner: removing the stored copy and
     * re-putting it reported `Accepted`, left the address empty between the two
     * writes, and handed the projection a remove and a put for one unchanged event.
     */
    private fun racedReplaceableDuplicate(supersedesViaPut: Boolean) =
        runBlocking {
            val index = RacingIndex(InMemoryEventIndex(supersedesViaPut = supersedesViaPut))
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val profile = Event(id(), alice, next(), 0, emptyArray(), "{\"name\":\"alice\"}", "")
            index.afterDedup = { store.insert(profile) }
            val batch = padding(20) + profile

            val outcome = store.batchInsert(batch)[batch.indexOf(profile)]

            assertEquals(IEventStore.InsertOutcome.Rejected(Rejections.DUPLICATE), outcome, "a raced duplicate is a duplicate, got $outcome")
            assertEquals(listOf(profile.id), index.put.filter { it == profile.id }, "only the racing writer may put it")
            assertTrue(profile.id !in index.removed, "the stored copy must not be removed")
            assertEquals(profile.id, index.get(profile.id)?.id, "the event stays stored")
        }

    @Test
    fun `a replaceable stored between plan and commit is a duplicate, never removed and re-put`() = racedReplaceableDuplicate(supersedesViaPut = false)

    @Test
    fun `the putIfNewer branch reports a raced replaceable duplicate as a duplicate`() = racedReplaceableDuplicate(supersedesViaPut = true)

    /** Refuses every put of [kind] while [failing]: an engine 5xx, a cancelled feed, after the reads have answered. */
    private class PutFailingIndex(
        private val inner: InMemoryEventIndex,
        private val kind: Int,
    ) : EventIndex by inner {
        var failing = false

        override suspend fun put(doc: EventDoc) {
            check(!(failing && doc.kind == kind)) { "put refused" }
            inner.put(doc)
        }

        override suspend fun putAll(docs: List<EventDoc>) = docs.forEach { put(it) }

        override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
    }

    /**
     * A supersession writes its winner BEFORE it removes what the winner
     * replaces. The bulk path removed the stored version first and put the
     * batch after, so a put that failed (or a commit cancelled between the
     * two) left the address holding NEITHER version: a kind 0 or 3 gone until
     * its author happened to publish again, and the retry could not bring it
     * back — the old version is nobody's to re-offer. Both the bulk path and
     * the per-event one are held to it.
     */
    @Test
    fun `a failed put never loses the version it was replacing`() =
        runBlocking {
            for (bulk in listOf(true, false)) {
                val inner = InMemoryEventIndex()
                val index = PutFailingIndex(inner, kind = 0)
                val store = NostrSemanticsStore(index, relay = relayUrl)
                val v1 = Event(id(), alice, next(), 0, emptyArray(), "{\"name\":\"old\"}", "")
                store.insert(v1)
                val v2 = Event(id(), alice, next(), 0, emptyArray(), "{\"name\":\"new\"}", "")
                index.failing = true
                assertFailsWith<IllegalStateException> {
                    if (bulk) store.batchInsert(padding(20) + v2) else store.insert(v2)
                }
                assertEquals(
                    listOf(v1.id),
                    inner.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).map { it.id },
                    "the replaced version must still be stored (bulk=$bulk)",
                )
            }
        }

    /**
     * A wide batch's locked commit's three reads — the batch's tombstones, its owners'
     * vanishes and its addresses' stored versions — are independent, so they
     * run together: the writer lock is held for the slowest, not their sum.
     * Proven by an index that answers none of them until all three have
     * arrived; read back to back, the first never returns and the commit times
     * out. The batch still ends where the per-event path does.
     */
    @Test
    fun `a wide batch's commit reads its guards and versions together`() =
        runBlocking {
            // Past EARLY_GUARDS_MAX the guards are read under the lock, beside the version read.
            val arrived = readsMeeting(setOf("tombstones", "vanishes", "versions"), padding(BulkRecordInsert.EARLY_GUARDS_MAX))
            assertEquals(setOf("tombstones", "vanishes", "versions", "dedup"), arrived)
        }

    /** A narrow batch reads its guards before the lock, beside its dedup: neither waits for the other. */
    @Test
    fun `a narrow batch reads its guards beside its dedup`() =
        runBlocking {
            val arrived = readsMeeting(setOf("tombstones", "vanishes", "dedup"), padding(20))
            assertEquals(setOf("tombstones", "vanishes", "dedup", "versions"), arrived)
        }

    /**
     * Batch-inserts [pad] plus a replaceable whose older version is stored, with every engine read
     * named in [meeting] held until all of them have arrived: a read that waited for another would
     * never complete. Returns every read that ran.
     */
    private suspend fun readsMeeting(
        meeting: Set<String>,
        pad: List<Event>,
    ): Set<String> {
        val inner = InMemoryEventIndex()
        val arrived = HashSet<String>()
        val all = CompletableDeferred<Unit>()
        val index =
            object : EventIndex by inner {
                suspend fun arrive(read: String) {
                    synchronized(arrived) { if (arrived.add(read) && arrived.containsAll(meeting)) all.complete(Unit) }
                    if (read in meeting) all.await()
                }

                override suspend fun search(query: EventQuery): List<EventDoc> {
                    when {
                        query.kinds == listOf(DeletionRequestEvent.KIND) && query.tags.isNotEmpty() -> arrive("tombstones")
                        query.kinds == listOf(RequestToVanishEvent.KIND) -> arrive("vanishes")
                    }
                    return inner.search(query)
                }

                override suspend fun existingIds(ids: List<String>): Set<String> {
                    arrive("dedup")
                    return inner.existingIds(ids)
                }

                // A plain replaceable's versions are read off this grouping (winner and count) since they stopped being a document search.
                override suspend fun versionCensus(
                    kind: Int,
                    authors: List<String>,
                ): Map<String, VersionCensus> {
                    arrive("versions")
                    return inner.versionCensus(kind, authors)
                }

                override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
            }
        // SHARED_STRICT (the default): every owner is flagged, so both guard reads run.
        val store = NostrSemanticsStore(index, relay = relayUrl)
        val v1 = Event(id(), alice, next(), 0, emptyArray(), "{\"name\":\"old\"}", "")
        inner.put(v1.toDoc())
        val v2 = Event(id(), alice, next(), 0, emptyArray(), "{\"name\":\"new\"}", "")
        val outcomes = withTimeout(10_000) { store.batchInsert(pad + v2) }
        assertTrue(outcomes.all { it is IEventStore.InsertOutcome.Accepted }, "$outcomes")
        assertEquals(listOf(v2.id), inner.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).map { it.id })
        return synchronized(arrived) { arrived.toSet() }
    }

    /**
     * STAGE D READS NO DOCUMENT FOR A KIND NO REACTION NEEDS WHOLE. A plain
     * replaceable's versions come off one attribute-only grouping, an
     * addressable's off the id walk — the full-document search pulled a stored
     * kind 3 (50-500 KB of tags) under the writer lock to read three fields.
     * An author holding two versions (a sweep that failed) is still read in
     * full and swept clean; the trust kinds keep the whole read their removal
     * reaction needs.
     */
    @Test
    fun `the bulk version read fetches no document for a plain or addressable kind`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val searched = CopyOnWriteArrayList<EventQuery>()
            val index =
                object : EventIndex by inner {
                    override suspend fun search(query: EventQuery): List<EventDoc> {
                        searched += query
                        return inner.search(query)
                    }

                    override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
                }
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val bob = "b2".repeat(32)
            val follows = Event(id(), alice, next(), 3, arrayOf(arrayOf("p", bob)), "", "")
            val post = LongFormContentEvent(id(), alice, next(), arrayOf(arrayOf("d", "post")), "body", "")
            store.insert(follows)
            store.insert(post)
            // Drift: bob holds two kind 0s, as a failed sweep would leave them.
            val bobOld = Event(id(), bob, next(), 0, emptyArray(), "{}", "")
            val bobStale = Event(id(), bob, next(), 0, emptyArray(), "{}", "")
            inner.put(bobOld.toDoc())
            inner.put(bobStale.toDoc())
            searched.clear()

            val follows2 = Event(id(), alice, next(), 3, arrayOf(arrayOf("p", bob), arrayOf("p", alice)), "", "")
            val post2 = LongFormContentEvent(id(), alice, next(), arrayOf(arrayOf("d", "post")), "edited", "")
            val bobNew = Event(id(), bob, next(), 0, emptyArray(), "{\"name\":\"bob\"}", "")
            val outcomes = store.batchInsert(padding(20) + listOf(follows2, post2, bobNew))
            assertTrue(outcomes.all { it is IEventStore.InsertOutcome.Accepted }, "$outcomes")

            val documentReads = searched.filter { it.kinds == listOf(3) || it.kinds == listOf(LongFormContentEvent.KIND) }
            assertEquals(emptyList(), documentReads, "a kind 3 or 30023 version is never read as a document")
            assertEquals(listOf(listOf(bob)), searched.filter { it.kinds == listOf(0) }.map { it.authors }, "only the drifted author is read in full")
            assertEquals(listOf(follows2.id), inner.search(EventQuery(kinds = listOf(3), authors = listOf(alice))).map { it.id })
            assertEquals(listOf(post2.id), inner.search(EventQuery(kinds = listOf(LongFormContentEvent.KIND), authors = listOf(alice))).map { it.id })
            assertEquals(listOf(bobNew.id), inner.search(EventQuery(kinds = listOf(0), authors = listOf(bob))).map { it.id }, "both drifted versions swept")
        }

    /** The kinds stage D reads whole are exactly the ones the trust projection reacts to. */
    @Test
    fun `the whole-document kinds are the trust projection's`() {
        assertEquals(TrustProjection.TRUST_KINDS.toSet(), BulkRecordInsert.WHOLE_DOC_KINDS)
    }

    /** The bulk path still matches the per-event one with a drifted address and addressables in the batch. */
    @Test
    fun `stubbed version reads end where the per-event path does`() {
        val v1 = article(d = "x", at = 100)
        assertBulkMatchesSequential(
            prelude = listOf(v1, article(d = "y", at = 100), Event(id(), alice, 100, 0, emptyArray(), "{}", "")),
            batch = padding(14) + listOf(article(d = "x", at = 200), article(d = "x", at = 150), Event(id(), alice, 50, 0, emptyArray(), "{}", ""), Event(id(), alice, 300, 0, emptyArray(), "{}", "")),
        )
    }

    /**
     * ONE BOUND FOR A COMMIT'S READS. The version read, the tombstone guards,
     * the vanish guards and each grouping pair ran together, each with its own
     * `mapBounded(QUERY_FANOUT)`, so a commit put several times the fan-out in
     * flight — the ceiling VESPA_QUERY_FANOUT exists to hold (proton's summary
     * stage times out past it). Every guard is probed here (SHARED_STRICT
     * flags every owner) and twenty addressable authors give twenty version reads.
     */
    @Test
    fun `a commit's concurrent read stages share one query bound`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val inFlight = AtomicInteger()
            val peak = AtomicInteger()
            val counting = AtomicBoolean(false)

            suspend fun <T> read(body: suspend () -> T): T {
                if (!counting.get()) return body()
                peak.accumulateAndGet(inFlight.incrementAndGet(), ::maxOf)
                try {
                    delay(20)
                    return body()
                } finally {
                    inFlight.decrementAndGet()
                }
            }
            val index =
                object : EventIndex by inner {
                    override suspend fun search(query: EventQuery) = read { inner.search(query) }

                    override suspend fun countByAuthor(query: EventQuery) = read { inner.countByAuthor(query) }

                    override suspend fun newestVersions(
                        kind: Int,
                        authors: List<String>,
                    ) = read { inner.newestVersions(kind, authors) }

                    override suspend fun visitIds(
                        query: EventQuery,
                        withDTag: Boolean,
                        onPage: suspend (List<DocRef>) -> Boolean,
                    ) = read { inner.visitIds(query, withDTag, onPage) }

                    override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
                }
            val bulk = BulkRecordInsert(index, relayUrl, GuardOwners(index), nowSecs = { 2_000_000L })
            val authors = (1..20).map { it.toString(16).padStart(64, 'e') }
            val batch = authors.map { LongFormContentEvent(id(), it, next(), arrayOf(arrayOf("d", "post")), "body", "") } + authors.map { Event(id(), it, next(), 0, emptyArray(), "{}", "") }
            val plan = bulk.plan(batch)
            counting.set(true)

            bulk.commit(plan)

            assertTrue(peak.get() <= QUERY_FANOUT, "at most QUERY_FANOUT ($QUERY_FANOUT) engine reads in flight per commit, saw ${peak.get()}")
        }

    /**
     * A plain replaceable's stored versions are read off ONE grouping per author chunk (the
     * version census: winner and count), not a newest-version grouping beside a count grouping
     * — two queries, two snapshots a write could land between. An author holding a drifted
     * second version is still read whole and swept.
     */
    @Test
    fun `a replaceable's versions are read off one census grouping`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val census = AtomicInteger()
            val pairs = AtomicInteger()
            val index =
                object : EventIndex by inner {
                    override suspend fun versionCensus(
                        kind: Int,
                        authors: List<String>,
                    ) = inner.versionCensus(kind, authors).also { census.incrementAndGet() }

                    override suspend fun countByAuthor(query: EventQuery) = inner.countByAuthor(query).also { pairs.incrementAndGet() }

                    override suspend fun newestVersions(
                        kind: Int,
                        authors: List<String>,
                    ) = inner.newestVersions(kind, authors).also { pairs.incrementAndGet() }

                    override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
                }
            val store = NostrSemanticsStore(index, relay = relayUrl)
            val authors = (1..5).map { it.toString(16).padStart(64, 'd') }
            store.batchInsert(authors.map { Event(id(), it, 100, 0, emptyArray(), "{}", "") })
            // Drift: a second stored version beside the first author's.
            inner.put(Event(id(), authors[0], 90, 0, emptyArray(), "{}", "").toDoc())
            census.set(0)
            pairs.set(0)
            val newer = authors.map { Event(id(), it, 200, 0, emptyArray(), "{}", "") }

            assertTrue(store.batchInsert(newer).all { it == IEventStore.InsertOutcome.Accepted })

            assertEquals(1, census.get(), "one census grouping for the chunk")
            assertEquals(0, pairs.get(), "and no grouping pair")
            assertEquals(newer.map { it.id }.toSet(), inner.search(EventQuery(kinds = listOf(0), authors = authors)).map { it.id }.toSet(), "every version older than a winner swept, the drifted one included")
        }
}
