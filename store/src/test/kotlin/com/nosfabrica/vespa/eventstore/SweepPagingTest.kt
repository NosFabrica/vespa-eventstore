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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip40Expiration.expiration
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
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

    private val realNow = System.currentTimeMillis() / 1000

    private fun expiringNote(
        at: Long,
        expiresAt: Long,
    ) = Event(id(), alice, at, 1, arrayOf(arrayOf("expiration", "$expiresAt")), "e$seq", "")

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
            // Admitted because it has not expired by the WALL clock (ingress
            // rejects an already-expired event, so a backlog seeded with an
            // expiration in the past would be stored as nothing at all — this
            // test used to, and passed with the sweep doing no work); the
            // sweeping store's clock is past it.
            val expiring = (1..CORPUS).map { expiringNote(at = 1_000L + it, expiresAt = realNow + 50_000) }
            store.batchInsert(expiring)
            assertEquals(CORPUS, stored(), "seeded")
            val late = NostrSemanticsStore(index, nowSecs = { realNow + 100_000 }, sweepPage = PAGE)

            index.searchLimits.clear()
            late.deleteExpiredEvents()
            val sweepReads = index.searchLimits.toList()

            assertEquals(0, stored(), "the whole expired backlog is gone")
            assertTrue(sweepReads.size > CORPUS / PAGE, "the sweep took multiple rounds: $sweepReads")
            assertTrue(sweepReads.all { it != null && it <= PAGE }, "expiry reads are paged too: $sweepReads")
        }

    /**
     * (a) Releasing the locks between pages must not change WHAT is swept: a
     * sweep over many pages leaves exactly the event documents AND the
     * reputation documents a one-page sweep leaves. Cards are in the mix
     * because each page's removes are a trust reaction of their own now,
     * rather than part of one long hold.
     */
    @Test
    fun `a paged expiry sweep removes exactly what a single-page sweep removes`() =
        runBlocking {
            fun corpus(): List<Event> {
                seq = 0
                return (1..CORPUS).flatMap { i ->
                    listOf(
                        expiringNote(at = 1_000L + i, expiresAt = realNow + 50_000),
                        expiringNote(at = 1_000L + i, expiresAt = realNow + 200_000), // not yet due
                        note(at = 1_000L + i), // never expires
                        UserAssertionEvent(
                            id(),
                            alice,
                            1_000L + i,
                            arrayOf(arrayOf("d", "%064x".format(i)), arrayOf("rank", "50"), arrayOf("expiration", "${if (i % 2 == 0) realNow + 50_000 else realNow + 200_000}")),
                            "",
                            "",
                        ),
                    )
                }
            }

            suspend fun sweptWith(page: Int): Triple<Set<String>, Map<String, *>, Int> {
                val inner = InMemoryEventIndex()
                val reputations = InMemoryReputationIndex()
                val recording = RecordingIndex(TrustProjection(inner, reputations))
                val seeding = NostrSemanticsStore(recording, sweepPage = page)
                seeding.batchInsert(corpus()).forEach { assertEquals(IEventStore.InsertOutcome.Accepted, it) }
                val late = NostrSemanticsStore(recording, nowSecs = { realNow + 100_000 }, sweepPage = page)
                recording.searchLimits.clear()
                late.deleteExpiredEvents()
                val rounds = recording.searchLimits.size
                return Triple(inner.search(EventQuery()).mapTo(HashSet()) { it.id }, reputations.docs.toMap(), rounds)
            }

            val (pagedDocs, pagedReputations, pagedRounds) = sweptWith(PAGE)
            val (wholeDocs, wholeReputations, wholeRounds) = sweptWith(10_000)

            val expected = corpus().filter { (it.expiration() ?: Long.MAX_VALUE) > realNow + 100_000 }.mapTo(HashSet()) { it.id }
            assertEquals(CORPUS * 4 - CORPUS - CORPUS / 2, expected.size, "fixture: a note and every other card are due")
            assertEquals(expected, pagedDocs, "the paged sweep removed exactly the due events")
            assertEquals(wholeDocs, pagedDocs, "and the same set as one page")
            assertTrue(wholeReputations.isNotEmpty(), "fixture: the cards projected reputation documents")
            assertEquals(wholeReputations, pagedReputations, "and left the same reputation documents")
            assertTrue(pagedRounds > (CORPUS + CORPUS / 2) / PAGE, "the paged sweep really took many pages: $pagedRounds")
            assertEquals(2, wholeRounds, "the reference sweep is one page plus the empty one that ends it")
        }

    /**
     * (b) THE POINT OF PAGING: a publish waiting for the writer lock gets it
     * between two pages of an expiry sweep, instead of after the last one. The
     * sweep is parked inside its first page's removes (both locks held), the
     * publish is started and queued on the lock, then the sweep is let go —
     * and the publish's note must already be stored when the second page's
     * removes run. Under one hold for the whole sweep it was stored after the
     * last page. Deterministic, not a latency budget: kotlinx `Mutex` hands
     * the lock to its first waiter on unlock, and the publish is started
     * UNDISPATCHED, so it is that waiter before the sweep can re-take it.
     */
    @Test
    fun `a publish takes the writer lock between two pages of an expiry sweep`() =
        runBlocking {
            val parking = ParkingSweepIndex()
            val store = NostrSemanticsStore(parking, nowSecs = { realNow + 100_000 }, sweepPage = PAGE)
            store.batchInsert((1..CORPUS).map { expiringNote(at = 1_000L + it, expiresAt = realNow + 50_000) })
            val publish = note(at = 9_000L)
            parking.watched = publish.id

            coroutineScope {
                val sweep = async { store.deleteExpiredEvents() }
                parking.parked.await()
                val published = async(start = CoroutineStart.UNDISPATCHED) { store.insert(publish) }
                assertFalse(published.isCompleted, "the publish is queued behind the sweep's first page")
                parking.release.complete(Unit)
                withTimeout(5_000) {
                    sweep.await()
                    published.await()
                }
            }

            val seen = parking.watchedStoredAtRemove.toList()
            assertTrue(seen.size >= 3, "the sweep spans several pages: $seen")
            assertEquals(false, seen.first(), "the publish was still queued during page one")
            assertTrue(seen.drop(1).all { it }, "the publish landed between page one and page two, not after the sweep: $seen")
            assertTrue(parking.inner.get(publish.id) != null, "and it is stored")
            assertEquals(1, parking.inner.search(EventQuery()).size, "while every expired event is gone")
        }

    /**
     * (c) The sweep's cutoff is taken ONCE: events that expire WHILE it runs
     * are not chased, so a steady trickle of short-lived events published
     * between pages cannot keep it going. Each page's removes publish one
     * event expiring a second after the store's clock and then move the
     * clock 100 s on, so by the next page that event is due. A cutoff re-read
     * per page sweeps every one of them, a page at a time; the fixed one ends
     * after the seeded backlog and leaves them for the next pass, which takes
     * them.
     */
    @Test
    fun `an expiry sweep terminates while new events expire during it`() =
        runBlocking {
            var clock = realNow + 100_000
            val inner = InMemoryEventIndex()
            val arrivals = mutableListOf<Event>()
            val inFlight = mutableListOf<Deferred<Unit>>()
            val publishers = this
            lateinit var store: NostrSemanticsStore
            var removes = 0
            var trickle = true
            val trickling =
                object : EventIndex by inner {
                    override suspend fun removeDocs(docs: List<EventDoc>) {
                        inner.removeDocs(docs)
                        removes++
                        if (trickle && arrivals.size < MAX_ARRIVALS) {
                            val e = expiringNote(at = 9_000L + arrivals.size, expiresAt = clock + 1)
                            arrivals += e
                            clock += 100
                            // Queued on the writer lock the sweep holds right now;
                            // it lands as soon as this page releases it.
                            inFlight += publishers.async(start = CoroutineStart.UNDISPATCHED) { store.insert(e) }
                        }
                    }
                }
            store = NostrSemanticsStore(trickling, nowSecs = { clock }, sweepPage = PAGE)
            val backlog = (1..CORPUS).map { expiringNote(at = 1_000L + it, expiresAt = realNow + 50_000) }
            store.batchInsert(backlog)

            withTimeout(5_000) { store.deleteExpiredEvents() }
            inFlight.awaitAll()

            val pages = (CORPUS + PAGE - 1) / PAGE
            assertEquals(pages, removes, "the sweep removed the seeded backlog's pages and stopped")
            assertTrue(backlog.none { inner.get(it.id) != null }, "the backlog is gone")
            assertEquals(pages, arrivals.size, "one arrival per page")
            assertTrue(arrivals.all { inner.get(it.id) != null }, "events that expired during the sweep are left for the next one")

            trickle = false
            store.deleteExpiredEvents()
            assertTrue(arrivals.none { inner.get(it.id) != null }, "and the next sweep reaps them")
        }

    /**
     * Parks the expiry sweep's FIRST removes until [release], with both writer
     * locks held, and records for every removes call whether [watched] was
     * already stored.
     */
    private class ParkingSweepIndex(
        val inner: InMemoryEventIndex = InMemoryEventIndex(),
    ) : EventIndex by inner {
        val parked = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        var watched: String? = null
        val watchedStoredAtRemove = mutableListOf<Boolean>()

        override suspend fun removeDocs(docs: List<EventDoc>) {
            val id = watched ?: return inner.removeDocs(docs)
            watchedStoredAtRemove += inner.get(id) != null
            if (!parked.isCompleted) {
                parked.complete(Unit)
                release.await()
            }
            inner.removeDocs(docs)
        }
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
            store.insert(DeletionRequestEvent(id(), alice, 5_000L, arrayOf(arrayOf("a", "30023:$alice:slug-7")), "", ""))
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
            store.insert(DeletionRequestEvent(id(), alice, 5_000L, arrayOf(arrayOf("a", "10002:$alice:")), "", ""))
            val reads = index.queries.toList()

            assertEquals(0, store.query<Event>(listOf(Filter(kinds = listOf(10002)))).size, "the relay list is gone")
            assertTrue(reads.any { it.kinds == listOf(10002) && it.tags.isEmpty() }, "no d to key on: $reads")
        }

    /**
     * The bulk path (a batch carrying a vanish) used to preload the owner's
     * history into the replay's in-memory snapshot: unbounded, the ENTIRE
     * history in RAM under the writer lock — the one thing [PAGE]-sized sweep
     * rounds exist to prevent — and later bounded by the vanish's created_at
     * but still one unlimited read. It now erases that history with the
     * per-event path's sweep, after the replay: every owner read is bounded
     * by the vanish's own created_at AND by the page.
     */
    @Test
    fun `a bulk vanish reads only the history its sweep can reach, a page at a time`() =
        runBlocking {
            store.batchInsert((1..CORPUS).map { note(at = 6_000L + it) })

            index.queries.clear()
            val vanish = RequestToVanishEvent(id(), alice, 6_010L, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", "")
            // At least BULK_MIN events, so the batch takes the preloading path.
            store.batchInsert(listOf<Event>(vanish) + (1..BULK).map { note(at = 7_000L + it) })
            val reads = index.queries.toList()

            val ownerReads = reads.filter { it.owners == listOf(alice) }
            assertTrue(ownerReads.isNotEmpty(), "the vanish sweeps the owner's history: $reads")
            assertTrue(
                ownerReads.all { it.until == 6_010L && it.limit == PAGE },
                "bounded by the vanish's created_at and by the sweep page, never the whole history: $ownerReads",
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

        /** A backstop for a regressed sweep that chases its arrivals: it then sweeps them all, and the count says so. */
        const val MAX_ARRIVALS = 50
    }
}
