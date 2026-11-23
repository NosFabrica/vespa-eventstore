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

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip40Expiration.expiration
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The NIP-40 expiry sweep ([NostrSemanticsStore.deleteExpiredEvents]) takes the
 * writer locks ONE PAGE AT A TIME, so a publish waits for one page of it rather
 * than the whole backlog. What that must not change — the set swept, the
 * reputation documents, termination — and what it must make possible — a
 * publish, a card, a trust drain between pages — are pinned here, with
 * `delete(filters)` pinned as the sweep that keeps one hold.
 *
 * Every store here reads ONE movable [clock]: admission rejects what is expired
 * by the store's clock, and the sweep's cutoff is read off the same clock, so a
 * test seeds at one instant and sweeps after moving it — never by giving two
 * stores two clocks.
 *
 * The concurrency tests are deterministic, not latency budgets: the sweep is
 * parked inside a page's removes (its locks held), the competing writer is
 * started UNDISPATCHED so it is queued on the lock before the test goes on,
 * and kotlinx `Mutex` hands the lock to its first waiter on unlock.
 */
class ExpirySweepTest {
    private val realNow = System.currentTimeMillis() / 1000

    /** Every store's clock. Seeded at [realNow]; a test moves it past [DUE] before sweeping. */
    private var clock = realNow

    private val alice = "a1".repeat(32)
    private val service = "5e".repeat(32)
    private val observer = "0b".repeat(32)

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(
        at: Long,
        kind: Int = 1,
    ) = Event(id(), alice, at, kind, emptyArray(), "n$seq", "")

    private fun expiringNote(
        at: Long,
        expiresAt: Long = realNow + DUE,
    ) = Event(id(), alice, at, 1, arrayOf(arrayOf("expiration", "$expiresAt")), "e$seq", "")

    private fun card(
        subject: Int,
        at: Long = 1_000L + subject,
        expiresAt: Long? = null,
        signer: String = service,
    ): Event {
        val tags = mutableListOf(arrayOf("d", "%064x".format(subject)), arrayOf("rank", "50"))
        if (expiresAt != null) tags += arrayOf("expiration", "$expiresAt")
        return UserAssertionEvent(id(), signer, at, tags.toTypedArray(), "", "")
    }

    private fun list10040() = TrustProviderListEvent(id(), observer, 5_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")

    /** The clock after the backlog fell due and before anything [LATER] did. */
    private fun sweepTime() {
        clock = realNow + SWEEP_AT
    }

    private fun store(
        index: EventIndex,
        page: Int = PAGE,
    ) = NostrSemanticsStore(index, nowSecs = { clock }, sweepPage = page, expirySweepPage = page)

    private fun projection(
        inner: EventIndex,
        reputations: InMemoryReputationIndex,
    ) = TrustProjection(inner, reputations, nowSecs = { clock })

    private suspend fun stored(index: EventIndex) = index.search(EventQuery()).mapTo(HashSet()) { it.id }

    @Test
    fun `the expiry sweep pages and clears the whole backlog`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val removes = mutableListOf<Int>()
            val counting =
                object : EventIndex by inner {
                    override suspend fun removeAll(ids: List<String>) {
                        removes += ids.size
                        inner.removeAll(ids)
                    }
                }
            val store = store(counting)
            store.batchInsert((1..CORPUS).map { expiringNote(at = 1_000L + it) }).forEach { assertEquals(IEventStore.InsertOutcome.Accepted, it) }
            assertEquals(CORPUS, stored(inner).size, "seeded")

            sweepTime()
            store.deleteExpiredEvents()

            assertEquals(emptySet(), stored(inner), "the whole expired backlog is gone")
            assertTrue(removes.size > 1 && removes.all { it <= PAGE }, "a page at a time: $removes")
        }

    /**
     * A hold removes `VESPA_EXPIRY_SWEEP_PAGE` ids, 1,000 by default — not the
     * 10,000 `sweepPage` the re-query sweeps keep, whose size was set by the
     * matching each of their rounds repeats. A store built without the seam.
     */
    @Test
    fun `the expiry sweep removes a thousand ids per hold by default`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val batches = mutableListOf<Int>()
            val counting =
                object : EventIndex by inner {
                    override suspend fun removeAll(ids: List<String>) {
                        batches += ids.size
                        inner.removeAll(ids)
                    }
                }
            val store = NostrSemanticsStore(counting, nowSecs = { clock })
            store.batchInsert((1..2_500).map { expiringNote(at = 1_000L + it) })
            sweepTime()

            store.deleteExpiredEvents()

            assertEquals(listOf(1_000, 1_000, 500), batches)
            assertEquals(emptySet(), stored(inner))
        }

    /**
     * Paging must not change WHAT is swept: a sweep over many pages leaves
     * exactly the event documents and the reputation documents a one-page
     * sweep leaves. Single-threaded on purpose — nothing writes between the
     * pages here, so this pins the paged walk's arithmetic (every page, no
     * page twice, the cards' reactions page by page) and nothing about
     * concurrency; the tests below do that. It passes on the single-hold sweep
     * too: it is a guard, not the change's proof.
     */
    @Test
    fun `a paged expiry sweep removes exactly what a single-page sweep removes`() =
        runBlocking {
            fun corpus(): List<Event> {
                seq = 0
                // The list FIRST: a card projects a cell only for a service some 10040 names.
                return listOf(list10040()) +
                    (1..CORPUS).flatMap { i ->
                        listOf(
                            expiringNote(at = 1_000L + i),
                            expiringNote(at = 1_000L + i, expiresAt = realNow + LATER), // not yet due
                            note(at = 1_000L + i), // never expires
                            card(subject = i, expiresAt = if (i % 2 == 0) realNow + DUE else realNow + LATER),
                        )
                    }
            }

            suspend fun sweptWith(page: Int): Pair<Set<String>, Map<String, ReputationDoc>> {
                clock = realNow
                val inner = InMemoryEventIndex()
                val reputations = InMemoryReputationIndex()
                val store = store(projection(inner, reputations), page)
                store.batchInsert(corpus()).forEach { assertEquals(IEventStore.InsertOutcome.Accepted, it) }
                sweepTime()
                store.deleteExpiredEvents()
                return stored(inner) to reputations.docs.toMap()
            }

            val (pagedDocs, pagedReputations) = sweptWith(PAGE)
            val (wholeDocs, wholeReputations) = sweptWith(10_000)

            val expected = corpus().filter { (it.expiration() ?: Long.MAX_VALUE) > realNow + SWEEP_AT }.mapTo(HashSet()) { it.id }
            assertEquals(1 + CORPUS * 4 - CORPUS - CORPUS / 2, expected.size, "fixture: a note and every other card are due")
            assertEquals(expected, pagedDocs, "the paged sweep removed exactly the due events")
            assertEquals(wholeDocs, pagedDocs, "and the same set as one page")
            assertTrue(wholeReputations.values.count { it.influenceScores.isNotEmpty() } == CORPUS - CORPUS / 2, "fixture: the surviving cards' cells: $wholeReputations")
            assertEquals(wholeReputations, pagedReputations, "and left the same reputation documents")
        }

    /**
     * THE POINT OF PAGING: a publish queued behind the sweep takes the writer
     * lock between page one and page two, instead of after the last page.
     */
    @Test
    fun `a publish takes the writer lock between two pages of an expiry sweep`() =
        runBlocking {
            val parking = ParkingSweepIndex(InMemoryEventIndex())
            val store = store(parking)
            store.batchInsert((1..CORPUS).map { expiringNote(at = 1_000L + it) })
            sweepTime()
            val publish = note(at = 9_000L)

            val seen = parking.racing(watched = publish.id, sweep = { store.deleteExpiredEvents() }) { store.insert(publish) }

            assertTrue(seen.size >= 3, "the sweep spans several pages: $seen")
            assertEquals(false, seen.first(), "the publish was still queued during page one")
            assertTrue(seen.drop(1).all { it }, "the publish landed between page one and page two, not after the sweep: $seen")
            assertEquals(setOf(publish.id), stored(parking.inner), "it is stored, and every expired event is gone")
        }

    /**
     * A CARD takes the trust gate before `writes`, and the sweep's trust pass
     * holds both per page — so a card publish waits on the GATE, and must
     * still get it (and then `writes`) between two pages rather than after
     * the sweep.
     */
    @Test
    fun `a card publish takes both locks between two pages of an expiry sweep`() =
        runBlocking {
            val reputations = InMemoryReputationIndex()
            val inner = InMemoryEventIndex()
            val parking = ParkingSweepIndex(projection(inner, reputations))
            val store = store(parking)
            val named = list10040()
            store.insert(named)
            // Expired CARDS, so the parked page is the trust pass's, holding the gate.
            store.batchInsert((1..CORPUS).map { card(subject = 100 + it, expiresAt = realNow + DUE) } + (1..CORPUS).map { expiringNote(at = 1_000L + it) })
            sweepTime()
            val publish = card(subject = 7, at = 9_000L)

            val seen = parking.racing(watched = publish.id, sweep = { store.deleteExpiredEvents() }) { store.insert(publish) }

            assertEquals(false, seen.first(), "the card was still queued during page one")
            assertTrue(seen.size >= 3 && seen.drop(1).all { it }, "the card landed between page one and page two: $seen")
            assertEquals(setOf(named.id, publish.id), stored(inner), "the card is stored, and every expired event is gone")
            assertTrue(reputations.docs["%064x".format(7)]?.influenceScores?.isNotEmpty() == true, "and its cell was applied: ${reputations.docs}")
        }

    /**
     * The sweep's cutoff is taken ONCE: events that fall due WHILE it runs are
     * not chased. Each page's removes start a publish of an event expiring a
     * second after the clock — its expiry check passes, and it queues on the
     * lock the page holds — then move the clock 100 s on, so by the next page
     * that event is due. Each is OLDER than the backlog, so the walk's cursor
     * (paged here like the engine's, [pagedWalk]) reaches it — and passes it,
     * since it was not due at the cutoff. The sweep ends after the seeded
     * backlog and leaves them for the next pass, which takes them. (A cutoff
     * re-read per page swept every one of them, a page at a time, on the
     * re-query loop this walk replaced.)
     */
    @Test
    fun `an expiry sweep terminates while new events fall due during it`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val arrivals = mutableListOf<Event>()
            val inFlight = mutableListOf<Deferred<Unit>>()
            val publishers: CoroutineScope = this
            lateinit var store: NostrSemanticsStore
            var removes = 0
            var trickle = true
            val trickling =
                object : EventIndex by inner {
                    override suspend fun removeAll(ids: List<String>) {
                        inner.removeAll(ids)
                        removes++
                        trickle(ids.size)
                    }

                    override suspend fun visitIds(
                        query: EventQuery,
                        withDTag: Boolean,
                        onPage: suspend (List<DocRef>) -> Boolean,
                    ) = pagedWalk(inner, query, onPage)

                    private fun trickle(size: Int) {
                        if (!trickle || size == 0 || arrivals.size >= MAX_ARRIVALS) return
                        // OLDER than the backlog, so the walk's cursor reaches it.
                        val e = expiringNote(at = 500L + arrivals.size, expiresAt = clock + 1)
                        arrivals += e
                        // Passes admission's expiry check NOW, then queues on the
                        // writer lock this page holds; it lands once the page ends.
                        inFlight += publishers.async(start = CoroutineStart.UNDISPATCHED) { store.insert(e) }
                        clock += 100
                    }
                }
            store = store(trickling)
            val backlog = (1..CORPUS).map { expiringNote(at = 1_000L + it) }
            store.batchInsert(backlog)
            sweepTime()

            withTimeout(5_000) { store.deleteExpiredEvents() }
            inFlight.awaitAll()

            val pages = (CORPUS + PAGE - 1) / PAGE
            assertEquals(pages, removes, "the sweep removed the seeded backlog's pages and stopped")
            assertTrue(backlog.none { inner.get(it.id) != null }, "the backlog is gone")
            assertEquals(pages, arrivals.size, "one arrival per page")
            assertTrue(arrivals.all { inner.get(it.id) != null }, "events that fell due during the sweep are left for the next one")
            assertTrue(arrivals.all { (it.expiration() ?: 0) <= clock }, "fixture: every arrival is due by now")

            trickle = false
            store.deleteExpiredEvents()
            assertTrue(arrivals.none { inner.get(it.id) != null }, "and the next sweep reaps them")
        }

    /**
     * A CANCELLED sweep stops at the next page. Nothing else in its loop is
     * a cancellation point here — the in-memory index never suspends and an
     * uncontended `Mutex.lock` returns without suspending — so without the
     * per-page check it ran every remaining page first.
     */
    @Test
    fun `a cancelled expiry sweep stops before its next page`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            var removes = 0
            lateinit var sweep: Deferred<Unit>
            val cancelling =
                object : EventIndex by inner {
                    override suspend fun removeAll(ids: List<String>) {
                        inner.removeAll(ids)
                        if (removes++ == 0) sweep.cancel()
                    }
                }
            val store = store(cancelling)
            store.batchInsert((1..CORPUS).map { expiringNote(at = 1_000L + it) })
            sweepTime()

            sweep = async(start = CoroutineStart.LAZY) { store.deleteExpiredEvents() }
            sweep.start()
            assertFailsWith<CancellationException> { sweep.await() }

            assertEquals(1, removes, "the page in hand finished; no page after it ran")
            assertEquals(CORPUS - PAGE, stored(inner).size, "the rest is left for the next pass")
        }

    /**
     * A page that THROWS mid-sweep fails the sweep and releases what it held:
     * afterwards a card — the one write that needs both locks — still goes
     * through, as does a plain note.
     */
    @Test
    fun `a sweep page that throws leaves both locks free`() =
        runBlocking {
            val reputations = InMemoryReputationIndex()
            val inner = InMemoryEventIndex()
            val projected = projection(inner, reputations)
            var removes = 0
            val failing =
                object : EventIndex by projected {
                    override suspend fun removeAll(ids: List<String>) {
                        check(removes++ == 0) { "engine refused the second page" }
                        projected.removeAll(ids)
                    }
                }
            val store = store(failing)
            store.batchInsert((1..CORPUS).map { expiringNote(at = 1_000L + it) })
            sweepTime()

            assertFailsWith<IllegalStateException> { store.deleteExpiredEvents() }

            val after = card(subject = 9, at = 9_000L)
            val plain = note(at = 9_001L)
            withTimeout(5_000) {
                store.insert(after)
                store.insert(plain)
            }
            assertTrue(inner.get(after.id) != null && inner.get(plain.id) != null, "both writes landed")
        }

    /**
     * `delete(filters)` keeps ONE hold for the whole sweep — a filter's match
     * set is not fixed, so a write between pages could add an event a later
     * page then deletes. A publish queued behind it lands after the LAST page.
     * Pinned so that nobody hands it the expiry sweep's per-page hold.
     */
    @Test
    fun `an admin delete keeps one hold across all its pages`() =
        runBlocking {
            val parking = ParkingSweepIndex(InMemoryEventIndex())
            val store = store(parking)
            store.batchInsert((1..CORPUS).map { note(at = 1_000L + it) })
            val publish = note(at = 9_000L, kind = 7)

            val seen = parking.racing(watched = publish.id, sweep = { store.delete(Filter(kinds = listOf(1))) }) { store.insert(publish) }

            assertTrue(seen.size >= 3, "the delete spans several pages: $seen")
            assertTrue(seen.none { it }, "no page of the delete ran after the publish landed: $seen")
            assertEquals(setOf(publish.id), stored(parking.inner), "the publish landed after it, and every kind 1 is gone")
        }

    /**
     * A TRUST DRAIN between two pages: the drain takes the gate the sweep
     * releases after a page, walks the service a fresh 10040 named — over
     * cards some of which the sweep has not reached yet — and the sweep then
     * finishes. Once drained, the reputation CELLS equal those of the same
     * corpus swept with nothing in between and drained after. Under one hold
     * for the whole sweep the drain could only run after its last page.
     */
    @Test
    fun `a trust drain between pages ends with the same reputations`() =
        runBlocking {
            suspend fun seed(store: NostrSemanticsStore) {
                seq = 0
                // Cards first, then the list naming their service: the list
                // queues a WALK of the stored cards, deferred to the drain.
                val cards = (1..CORPUS).map { card(subject = it, expiresAt = if (it % 3 == 0) null else realNow + DUE) }
                store.batchInsert(cards).forEach { assertEquals(IEventStore.InsertOutcome.Accepted, it) }
                store.insert(list10040())
            }

            suspend fun reference(): Map<String, ReputationDoc> {
                clock = realNow
                val reputations = InMemoryReputationIndex()
                val projection = projection(InMemoryEventIndex(), reputations)
                projection.backlog.drainInBackground {}
                val store = store(projection)
                seed(store)
                sweepTime()
                store.deleteExpiredEvents()
                projection.backlog.drain { store.withWriteLock(it) }
                return reputations.docs.toMap()
            }

            val expected = reference()

            clock = realNow
            val reputations = InMemoryReputationIndex()
            val projection = projection(InMemoryEventIndex(), reputations)
            projection.backlog.drainInBackground {}
            val parking = ParkingSweepIndex(projection)
            val store = store(parking)
            seed(store)
            sweepTime()
            // Observed by its EFFECT, not by completion: the drain chases the
            // queue, and every page's card removals queue more, so it ends with
            // the sweep. Subject 3's card never expires and has no cell until
            // the walk the 10040 queued gives it one.
            val walked = mutableListOf<Boolean>()
            parking.onRemove = { walked += reputations.docs["%064x".format(3)]?.influenceScores?.isNotEmpty() == true }

            coroutineScope {
                val sweep = async { store.deleteExpiredEvents() }
                parking.parked.await()
                val draining = async(start = CoroutineStart.UNDISPATCHED) { projection.backlog.drain { store.withWriteLock(it) } }
                assertFalse(draining.isCompleted, "the drain queues for the gate the sweep's page holds")
                parking.release.complete(Unit)
                withTimeout(5_000) {
                    sweep.await()
                    draining.await()
                }
            }
            projection.backlog.drain { store.withWriteLock(it) }

            assertTrue(walked.size >= 3 && walked.first() == false, "the sweep spans several pages, and page one ran before the walk: $walked")
            assertTrue(walked.drop(1).any { it }, "the walk ran between two pages, not after the sweep: $walked")
            assertTrue(expected.values.count { it.influenceScores.isNotEmpty() } == CORPUS / 3, "fixture: the walk projected the surviving cards: $expected")

            // Compared by their CELLS: a subject whose card the walk projected
            // and a later page removed keeps an emptied document — a husk, as
            // any projected card removed afterwards leaves (by a kind 5, or by
            // a later sweep), and read as no reputation at all.
            fun cells(docs: Map<String, ReputationDoc>) = docs.filterValues { it.influenceScores.isNotEmpty() || it.followerCounts.isNotEmpty() }
            assertEquals(cells(expected), cells(reputations.docs.toMap()), "the same reputations as with nothing in between")
        }

    /**
     * The sweep WALKS its match set once and reads full documents only for the
     * trust kinds. Two recorders: above the trust projection (what the store
     * asks) and below it (what the projection reads back to react). Neither
     * may see the due set re-queried page after page — the old loop matched
     * and sorted everything left on every round, as full documents — and the
     * only document reads below the projection are id lookups of its own kinds.
     */
    @Test
    fun `the expiry sweep walks once and reads full documents only for trust kinds`() =
        runBlocking {
            val below = Recording(InMemoryEventIndex())
            val above = Recording(projection(below, InMemoryReputationIndex()))
            val store = store(above)
            store.insert(list10040())
            store.batchInsert((1..CORPUS).map { expiringNote(at = 1_000L + it) } + (1..CORPUS).map { card(subject = it, expiresAt = realNow + DUE) })
            sweepTime()
            above.clear()
            below.clear()

            store.deleteExpiredEvents()
            val reads = above.searches + below.searches
            val walks = above.walks.toList()

            assertEquals(1, stored(below).size, "only the provider list is left")
            assertEquals(2, walks.size, "one walk per pass: $walks")
            assertEquals(TRUST_KINDS, walks.first().kinds, "the trust kinds first")
            assertTrue(walks.all { it.expiresBefore == realNow + SWEEP_AT + 1 }, "both on the one cutoff: $walks")
            assertTrue(reads.none { it.expiresBefore != null }, "no page re-queries the due set: $reads")
            val offenders = reads.filterNot { it.ids.isNotEmpty() && it.kinds == TRUST_KINDS }.map { "ids=${it.ids.size} kinds=${it.kinds} search=${it.search} tags=${it.tags} authors=${it.authors.size} limit=${it.limit}" }
            assertTrue(offenders.isEmpty(), "every document read is an id lookup of the trust kinds: $offenders")
        }

    /**
     * ONE trust-kind lookup per plain batch. The plain pass must know a batch
     * holds no card before it removes it under `writes` alone — and the trust
     * projection, asked to remove ids, reads its kinds back for the same ids
     * again. Two lookups of one question per batch (three per 1,000 ids, the
     * projection's own in chunks of 500); the projection now answers both at
     * once (`TrustProjection.removeAllButTrust`).
     */
    @Test
    fun `the plain pass looks each batch up for trust kinds once`() =
        runBlocking {
            val below = Recording(InMemoryEventIndex())
            val store = store(projection(below, InMemoryReputationIndex()))
            store.batchInsert((1..30).map { expiringNote(at = 1_000L + it) })
            sweepTime()
            below.clear()

            store.deleteExpiredEvents()
            // A lookup is any read of named ids: a search, or a walk of them.
            val reads = below.searches + below.walks.filter { it.ids.isNotEmpty() }

            assertEquals(emptySet(), stored(below), "every note swept")
            assertEquals(30 / PAGE + 1, reads.size, "one lookup per batch of $PAGE: ${reads.map { "ids=${it.ids.size} kinds=${it.kinds}" }}")
            assertTrue(reads.all { it.kinds == TRUST_KINDS && it.ids.isNotEmpty() }, "each a trust-kind id lookup")
        }

    /**
     * A plain-kind backlog is swept under `writes` ALONE: with the trust gate
     * held elsewhere (a drain, a reconcile), the sweep of notes still
     * finishes. A card in the backlog is the one thing that waits for it.
     */
    @Test
    fun `plain kinds are swept while the trust gate is held elsewhere`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val store = store(projection(inner, InMemoryReputationIndex()))
            store.batchInsert((1..CORPUS).map { expiringNote(at = 1_000L + it) })
            sweepTime()

            coroutineScope {
                val gateHeld = CompletableDeferred<Unit>()
                val release = CompletableDeferred<Unit>()
                val holder =
                    async {
                        store.withWriteLock {
                            gateHeld.complete(Unit)
                            release.await()
                        }
                    }
                gateHeld.await()
                withTimeout(5_000) { store.deleteExpiredEvents() }
                assertEquals(emptySet(), stored(inner), "every note swept while the gate was held")

                val dueCard = card(subject = 1, expiresAt = clock + 1)
                release.complete(Unit)
                holder.await()
                store.insert(dueCard)
                clock += 10
                val gateAgain = CompletableDeferred<Unit>()
                val releaseAgain = CompletableDeferred<Unit>()
                val holderAgain =
                    async {
                        store.withWriteLock {
                            gateAgain.complete(Unit)
                            releaseAgain.await()
                        }
                    }
                gateAgain.await()
                val sweep = async { store.deleteExpiredEvents() }
                repeat(10) { yield() }
                assertFalse(sweep.isCompleted, "a due card waits for the gate")
                releaseAgain.complete(Unit)
                holderAgain.await()
                withTimeout(5_000) { sweep.await() }
                assertEquals(emptySet(), stored(inner), "and is swept once it is free")
            }
        }

    /** An expiry sweep whose removes are acked and dropped FAILS rather than reporting done. */
    @Test
    fun `an expiry sweep whose removes do not land fails loudly`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val dropping =
                object : EventIndex by inner {
                    override suspend fun removeAll(ids: List<String>) {} // acked, never applied
                }
            val store = store(dropping)
            store.batchInsert((1..CORPUS).map { expiringNote(at = 1_000L + it) })
            sweepTime()
            val refused = assertFailsWith<IllegalStateException> { store.deleteExpiredEvents() }
            assertTrue("not landing" in refused.message.orEmpty(), "the sweep names the dropped removes: ${refused.message}")
        }

    /** Records the walks and the document reads that pass through it. */
    private class Recording(
        private val inner: EventIndex,
    ) : EventIndex by inner {
        val walks = mutableListOf<EventQuery>()
        val searches = mutableListOf<EventQuery>()

        fun clear() {
            walks.clear()
            searches.clear()
        }

        override suspend fun search(query: EventQuery): List<EventDoc> {
            searches += query
            return inner.search(query)
        }

        override suspend fun visitIds(
            query: EventQuery,
            withDTag: Boolean,
            onPage: suspend (List<DocRef>) -> Boolean,
        ) {
            walks += query
            inner.visitIds(query, withDTag, onPage)
        }
    }

    /**
     * A walk paged like the engine's cursor (`VespaEventIndex.visitIdsByCursor`):
     * newest first, [PAGE] at a time, each page read when the last one has
     * been handed over — so writes between pages are seen below the cursor.
     * The in-memory default hands the whole match set over as one page. Every
     * `created_at` here is distinct, so the cursor needs no tie handling.
     */
    private suspend fun pagedWalk(
        inner: EventIndex,
        query: EventQuery,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        var until = query.until
        while (true) {
            val page = inner.search(query.copy(until = until, limit = PAGE))
            if (page.isEmpty() || !onPage(page.map { DocRef(it.id, it.createdAt) })) return
            until = page.minOf { it.createdAt } - 1
        }
    }

    /**
     * Parks the FIRST page's removes until [release], with that page's locks
     * held; records, at every page's removes, whether [watched] was stored.
     */
    private class ParkingSweepIndex(
        val inner: EventIndex,
    ) : EventIndex by inner {
        val parked = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        private var watched: String? = null
        private val seen = mutableListOf<Boolean>()

        /** Runs at every page's removes, before the park. */
        var onRemove: (suspend () -> Unit)? = null

        /** The expiry sweep's removes. */
        override suspend fun removeAll(ids: List<String>) {
            atRemove()
            inner.removeAll(ids)
        }

        /** [Deletions.sweep]'s removes — `delete(filters)`. */
        override suspend fun removeDocs(docs: List<EventDoc>) {
            atRemove()
            inner.removeDocs(docs)
        }

        private suspend fun atRemove() {
            watched?.let { seen += inner.get(it) != null }
            onRemove?.invoke()
            if (!parked.isCompleted) {
                parked.complete(Unit)
                release.await()
            }
        }

        /**
         * Run [sweep], park it in its first page, queue [write] behind it, let
         * it go, and return — per page — whether [watched] was stored by then.
         */
        suspend fun racing(
            watched: String,
            sweep: suspend () -> Unit,
            write: suspend () -> Unit,
        ): List<Boolean> {
            this.watched = watched
            coroutineScope {
                val sweeping = async { sweep() }
                parked.await()
                val writing = async(start = CoroutineStart.UNDISPATCHED) { write() }
                assertFalse(writing.isCompleted, "the write is queued behind the sweep's first page")
                release.complete(Unit)
                withTimeout(5_000) {
                    sweeping.await()
                    writing.await()
                }
            }
            return seen.toList()
        }
    }

    private companion object {
        val TRUST_KINDS = TrustProjection.TRUST_KINDS
        const val PAGE = 4
        const val CORPUS = 25

        /** Seconds after [realNow]: the backlog's expiration, the sweep's clock, and what is not yet due at it. */
        const val DUE = 50L
        const val SWEEP_AT = 100L
        const val LATER = 200L

        /** A backstop for a regressed sweep that chases its arrivals: it then sweeps them all, and the count says so. */
        const val MAX_ARRIVALS = 50
    }
}
