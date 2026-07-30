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

import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A sweep (delete, NIP-40 expiry, NIP-62 vanish) walks its match set in
 * [NostrEventStore.sweepPage] rounds. Two properties matter and they pull
 * against each other:
 *
 *  - it must delete EVERYTHING, however many pages that takes; and
 *  - it must never pull the whole match set into one list, which is what an
 *    unbounded read over a large corpus would do.
 *
 * The page is 4 here so a modest corpus spans several rounds.
 */
class SweepPagingTest {
    /** Records the limit every search carried, so "the read is bounded" is checkable. */
    private class RecordingIndex(
        private val inner: EventIndex = InMemoryEventIndex(),
    ) : EventIndex by inner {
        val searchLimits = mutableListOf<Int?>()

        override suspend fun search(query: EventQuery): List<EventDoc> {
            searchLimits += query.limit
            return inner.search(query)
        }
    }

    private val index = RecordingIndex()
    private val store = NostrEventStore(index, sweepPage = PAGE)

    private val alice = "a1".repeat(32)

    private var seq = 0

    private fun note(at: Long) = Event((++seq).toString(16).padStart(64, '0'), alice, at, 1, emptyArray(), "n$seq", "")

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

    private companion object {
        const val PAGE = 4
        const val CORPUS = 25
    }
}
