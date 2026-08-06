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
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A sweep (delete, NIP-40 expiry, NIP-62 vanish) walks its match set in
 * [NostrSemanticsStore.sweepPage] rounds. Two properties matter and they pull
 * against each other:
 *
 *  - it must delete EVERYTHING, however many pages that takes; and
 *  - it must never pull the whole match set into one list, which is what an
 *    unbounded read over a large corpus would do.
 *
 * The page is 4 here so a modest corpus spans several rounds.
 */
class SweepPagingTest {
    /**
     * Records every search, so both halves of "the read is bounded" are
     * checkable: [searchLimits] for how many rows a round may return, and
     * [queries] for how NARROW the match set was in the first place.
     */
    private class RecordingIndex(
        private val inner: EventIndex = InMemoryEventIndex(),
    ) : EventIndex by inner {
        val searchLimits = mutableListOf<Int?>()
        val queries = mutableListOf<EventQuery>()

        override suspend fun search(query: EventQuery): List<EventDoc> {
            searchLimits += query.limit
            queries += query
            return inner.search(query)
        }
    }

    private val index = RecordingIndex()
    private val store = NostrSemanticsStore(index, sweepPage = PAGE)

    private val alice = "a1".repeat(32)

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(at: Long) = Event(id(), alice, at, 1, emptyArray(), "n$seq", "")

    private fun article(
        at: Long,
        slug: String,
    ) = Event(id(), alice, at, 30023, arrayOf(arrayOf("d", slug)), "a$slug", "")

    private suspend fun stored() = store.query<Event>(listOf(Filter())).size

    @Test
    fun `a delete spanning many pages removes every match`() =
        runBlocking {
            val all = (1..CORPUS).map { note(at = 1_000L + it) }
            store.batchInsert(all)
            assertEquals(CORPUS, stored(), "seeded")

            index.searchLimits.clear()
            store.delete(Filter(kinds = listOf(1)))
            // Snapshot before asserting: stored() is itself an unbounded query.
            val sweepReads = index.searchLimits.toList()

            assertEquals(0, stored(), "every match deleted, across ${CORPUS / PAGE}+ pages")
            assertTrue(sweepReads.size > CORPUS / PAGE, "the sweep took multiple rounds: $sweepReads")
            assertTrue(
                sweepReads.all { it != null && it <= PAGE },
                "an unbounded read would materialize the whole match set: $sweepReads",
            )
        }

    @Test
    fun `expiry sweep also pages and clears the whole backlog`() =
        runBlocking {
            // Every event expires at t=500; the store's clock is well past it.
            val expiring =
                (1..CORPUS).map {
                    Event((++seq).toString(16).padStart(64, '0'), alice, 1_000L + it, 1, arrayOf(arrayOf("expiration", "500")), "e$it", "")
                }
            store.batchInsert(expiring)

            index.searchLimits.clear()
            store.deleteExpiredEvents()
            val sweepReads = index.searchLimits.toList()

            assertEquals(0, stored(), "the whole expired backlog is gone")
            assertTrue(sweepReads.all { it != null && it <= PAGE }, "expiry reads are paged too: $sweepReads")
        }

    /** An explicit limit still means "delete at most N", satisfied by one page. */
    @Test
    fun `a limited delete stops after its first page`() =
        runBlocking {
            store.batchInsert((1..CORPUS).map { note(at = 2_000L + it) })

            store.delete(Filter(kinds = listOf(1), limit = 2))

            assertEquals(CORPUS - 2, stored(), "only the first page's worth was removed")
        }

    /**
     * A sweep whose removes do not land must FAIL, not return: an acked remove
     * is visible to search (the EventIndex contract), so a repeated page means
     * the deletes are being dropped — reporting the vanish/delete as enforced
     * would leave the events stored and served with no diagnostic.
     */
    @Test
    fun `a sweep whose removes do not land fails loudly`() {
        val inner = InMemoryEventIndex()
        val broken =
            object : EventIndex by inner {
                override suspend fun removeAll(ids: List<String>) {} // acked, never applied

                override suspend fun removeDocs(docs: List<EventDoc>) {} // the sweep's actual path
            }
        val store = NostrSemanticsStore(broken, sweepPage = PAGE)
        runBlocking {
            store.batchInsert((1..CORPUS).map { note(at = 3_000L + it) })
            assertFailsWith<IllegalStateException> { store.delete(Filter(kinds = listOf(1))) }
        }
    }

    /**
     * NIP-09 by address: the read must be narrowed to the TARGET address, not
     * to (kind, author). Unnarrowed, deleting one address pulls back every
     * other address of that kind by the same author — full docs, one list,
     * under the writer lock — to keep at most one. [EventIndex.putIfNewer]
     * pushes the same `d` down on the write side.
     */
    @Test
    fun `an addressable deletion narrows its read to the target address`() =
        runBlocking {
            store.batchInsert((1..CORPUS).map { article(at = 4_000L + it, slug = "slug-$it") })

            index.queries.clear()
            store.insert(DeletionEvent(id(), alice, 5_000L, arrayOf(arrayOf("a", "30023:$alice:slug-7")), "", ""))
            val reads = index.queries.toList()

            assertEquals(
                CORPUS - 1,
                store.query<Event>(listOf(Filter(kinds = listOf(30023)))).size,
                "exactly the addressed article is gone",
            )
            assertTrue(reads.any { it.tags["d"] == listOf("slug-7") }, "the target d is pushed into the query: $reads")
            assertTrue(
                reads.none { it.kinds == listOf(30023) && it.tags.isEmpty() },
                "no unnarrowed read across the author's every address: $reads",
            )
        }

    /**
     * A replaceable target has ONE address whatever the a-tag's d part, and an
     * empty d carries no "d:" pair in tag_index to match on — so both stay
     * broad by (kind, author). Bounded anyway: one author, one address.
     */
    @Test
    fun `a replaceable deletion stays broad by kind and author`() =
        runBlocking {
            store.insert(Event(id(), alice, 4_000L, 10002, emptyArray(), "relays", ""))

            index.queries.clear()
            store.insert(DeletionEvent(id(), alice, 5_000L, arrayOf(arrayOf("a", "10002:$alice:")), "", ""))
            val reads = index.queries.toList()

            assertEquals(0, store.query<Event>(listOf(Filter(kinds = listOf(10002)))).size, "the relay list is gone")
            assertTrue(reads.any { it.kinds == listOf(10002) && it.tags.isEmpty() }, "no d to key on: $reads")
        }

    /**
     * The bulk path (a batch carrying a vanish) preloads the owner's history
     * into an in-memory snapshot for the replay. Unbounded, that is the ENTIRE
     * history in RAM under the writer lock — the one thing [PAGE]-sized sweep
     * rounds exist to prevent. It must be bounded by the vanish's own
     * created_at, which is all its sweep can reach anyway.
     */
    @Test
    fun `a bulk vanish preloads only the history its sweep can reach`() =
        runBlocking {
            store.batchInsert((1..CORPUS).map { note(at = 6_000L + it) })

            index.queries.clear()
            val vanish = RequestToVanishEvent(id(), alice, 6_010L, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", "")
            // At least BULK_MIN events, so the batch takes the preloading path.
            store.batchInsert(listOf<Event>(vanish) + (1..BULK).map { note(at = 7_000L + it) })
            val reads = index.queries.toList()

            val ownerReads = reads.filter { it.owners == listOf(alice) }
            assertTrue(ownerReads.isNotEmpty(), "the vanish preloads the owner's history: $reads")
            assertTrue(
                ownerReads.all { it.until == 6_010L },
                "bounded by the vanish's created_at, not the whole history: $ownerReads",
            )
            // And the semantics are untouched: everything at or before the
            // vanish is gone, everything after it survives.
            val left = store.query<Event>(listOf(Filter(kinds = listOf(1)))).map { it.createdAt }
            assertTrue(left.none { it <= 6_010L }, "the vanish still swept its window: $left")
            assertEquals(BULK + CORPUS - 10, left.size, "and nothing outside it")
        }

    private companion object {
        const val PAGE = 4
        const val CORPUS = 25

        /** Mirrors NostrSemanticsStore's private BULK_MIN — the batch size that takes the bulk path. */
        const val BULK = 16
    }
}
