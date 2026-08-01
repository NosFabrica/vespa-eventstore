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
import com.vitorpamplona.quartz.eventstore.store.Rejections
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip23LongContent.LongTextNoteEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
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
    ) = ContactCardEvent(eventId, service, at, arrayOf(arrayOf("d", subject), arrayOf("rank", "$rank")), "", "")

    private fun deletion(
        targetId: String,
        at: Long = next(),
        author: String = alice,
    ) = DeletionEvent(id(), author, at, arrayOf(arrayOf("e", targetId)), "", "")

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
    ) = LongTextNoteEvent(eventId, alice, at, emptyArray(), "body", "")

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
            val bulk = BulkRecordInsert(index, relayUrl, GuardOwners(index))

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
}
