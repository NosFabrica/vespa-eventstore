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
package com.nosfabrica.vespa.eventstore.trust

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The ledger's bookkeeping under a drain that runs BESIDE writes — the
 * deferred mode of production, where the drain holds only the trust gate and
 * the writes it interleaves with hold their own locks or none. The [ProjectionLedger.drain]
 * gate is injectable, so a test can put a write exactly where a live one would
 * land: after a subject's slice has been derived, before the round is over.
 */
class ProjectionLedgerTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private val service2 = "6e".repeat(32)
    private val subject = "ab".repeat(32)

    private fun cardFor(
        subj: String,
        rank: Int,
    ) = ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", subj), arrayOf("rank", rank.toString())), "", "")

    /** More dirty subjects than one gate slice holds, so a round is more than one slice. */
    private fun manySubjects(n: Int = TrustRecompute.GATE_SLICE + 100) = (1..n).map { it.toString(16).padStart(64, '0') }

    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun list10040(serviceKey: String = service) = TrustProviderListEvent(id(), observer, next(), arrayOf(arrayOf("30382:rank", serviceKey, "wss://scores.example.com/")), "", "")

    private fun card(rank: Int) = ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString())), "", "")

    private fun note() = MetadataEvent(id(), "cc".repeat(32), next(), emptyArray(), """{"name":"n"}""", "")

    /**
     * An index that fails every read naming one author, and serves the rest.
     * Stands in for what the cluster actually does: a read that fails for
     * reasons local to the documents it touched — a truncated sorted read, a
     * node a hair short of its target — while every other read is fine.
     */
    private class OneBadService(
        private val inner: InMemoryEventIndex,
        private val bad: String,
    ) : EventIndex by inner {
        var refusals = 0

        // The id listing is where both real failures landed: #121's truncated
        // sorted read and #122's `full: false`. Keyed on the author because
        // that is what makes a walk one service's walk.
        override suspend fun visitIds(
            query: EventQuery,
            withDTag: Boolean,
            onPage: suspend (List<DocRef>) -> Boolean,
        ) {
            if (bad in query.authors.orEmpty()) {
                refusals++
                throw IllegalStateException("vespa answered full: false (100% of the corpus, degraded: unspecified)")
            }
            inner.visitIds(query, withDTag, onPage)
        }
    }

    /**
     * ONE BAD UNIT MUST NOT STOP THE OTHERS. The failure that motivated this
     * is not hypothetical: a single service whose walk threw kept the other
     * queued services underived for a whole day, because the throw left the
     * drain loop. The healthy service must be walked and retired in the same
     * call that the sick one fails.
     */
    @Test
    fun `a service whose walk fails does not stop the other services from being walked`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val index = OneBadService(inner, bad = service)
            val reputations = InMemoryReputationIndex()
            val projection = TrustProjection(index, reputations)
            projection.backlog.drainInBackground { }

            // Two services named by the observer, each with a card to project.
            projection.put(cardFor(subject, 40).toDoc())
            projection.put(ContactCardEvent(id(), service2, next(), arrayOf(arrayOf("d", subject), arrayOf("followers", "70")), "", "").toDoc())
            // One provider per DIMENSION: naming two under `30382:rank` names
            // only the last. `rank` and `followers` are the two the projection
            // reads, so this is the smallest list that queues two walks.
            projection.put(
                TrustProviderListEvent(
                    id(),
                    observer,
                    next(),
                    arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/"), arrayOf("30382:followers", service2, "wss://counts.example.com/")),
                    "",
                    "",
                ).toDoc(),
            )

            assertFailsWith<ProjectionLedger.DrainIncomplete> { projection.backlog.drain { it() } }

            val doc = assertNotNull(reputations.get(subject), "the healthy service's walk wrote a cell")
            assertTrue(ServiceKey(service2) in doc.followerCounts, "the healthy service was walked even though the other one threw")
            assertTrue(ServiceKey(service) !in doc.influenceScores, "and the failed service wrote nothing")
            val marker = assertNotNull(reputations.get(ProjectionLedger.MARKER_KEY), "the failed service stays queued")
            assertTrue(ServiceKey(service) in marker.followerCounts, "the failed service is still named by the marker")
            assertTrue(ServiceKey(service2) !in marker.followerCounts, "the walked service was retired")
        }

    /**
     * AND IT MUST NOT SPIN ON IT. The ledger keeps failed work queued, so the
     * loop's next round would name it again — at full speed, forever, without
     * the poisoned set. One attempt per drain call, then out.
     */
    @Test
    fun `a failing service is attempted once per drain call, not spun on`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val index = OneBadService(inner, bad = service)
            val reputations = InMemoryReputationIndex()
            val projection = TrustProjection(index, reputations)
            projection.backlog.drainInBackground { }

            projection.put(cardFor(subject, 40).toDoc())
            projection.put(list10040().toDoc())

            assertFailsWith<ProjectionLedger.DrainIncomplete> { projection.backlog.drain { it() } }
            val afterFirst = index.refusals
            assertTrue(afterFirst in 1..4, "the walk was attempted, not retried in a loop (was $afterFirst)")

            // A second call is a fresh attempt: the work is still queued, and a
            // failure that has cleared must be retried, not written off.
            assertFailsWith<ProjectionLedger.DrainIncomplete> { projection.backlog.drain { it() } }
            assertTrue(index.refusals in (afterFirst + 1)..(afterFirst * 2 + 2), "the next call retries it exactly once more")
        }

    private class Deferred {
        val reputations = InMemoryReputationIndex()
        val projection = TrustProjection(InMemoryEventIndex(), reputations)

        init {
            projection.backlog.drainInBackground { }
        }

        suspend fun rankOf(subject: String) =
            reputations
                .get(subject)
                ?.influenceScores
                ?.values
                ?.singleOrNull()

        suspend fun marker() = reputations.get(ProjectionLedger.MARKER_KEY)
    }

    /**
     * THE MID-WALK CARD. A service walk (the one deferred reaction left) reads
     * a page of stored cards and writes their cells under the gate; a newer
     * card for one of those subjects landing right after that page applies
     * its own cell inline, and nothing the walk does afterwards may put the
     * page's older value back. The gate lambda here is the live writer.
     */
    @Test
    fun `a card written after its page was applied keeps its newer value through the walk`() =
        runBlocking {
            val d = Deferred()
            d.projection.put(card(40).toDoc()) // by a service nobody names yet: no cell
            d.projection.put(list10040().toDoc()) // names it: the walk is queued
            assertNull(d.rankOf(subject), "deferred: nothing projected yet")

            var landed = false
            d.projection.backlog.drain { body ->
                body()
                // Once, after the FIRST page: the walk just wrote rank 40 from
                // the cards it read; this card post-dates that read.
                if (!landed && d.rankOf(subject) == 40) {
                    landed = true
                    d.projection.put(card(90).toDoc())
                    assertEquals(90, d.rankOf(subject), "the card's cell is applied inline")
                    // The walk is still covered on disk until its round completes.
                    assertTrue(ServiceKey(service) in assertNotNull(d.marker()).followerCounts, "the marker still names the service being walked")
                }
            }
            assertTrue(landed, "the interleaved card was written mid-walk")
            assertEquals(90, d.rankOf(subject), "the newer card's value survived the walk")
            assertNull(d.marker(), "and then the ledger is clean")
        }

    /**
     * THE KIND-1 BESIDE THE DRAIN. A plain note's [ProjectionLedger.insuring] adds no
     * work; it must also not WRITE BACK the backlog it read, or a drain that took
     * that backlog out in between is undone in memory — after which the next card
     * for that subject finds its subject "already pending", persists no
     * write-ahead, and a crash before the drain loses it. Reproduced by
     * pinning the note's read before the drain and its write after.
     */
    @Test
    fun `a plain write interleaved with a drain does not resurrect drained backlog`() =
        runBlocking {
            val d = Deferred()
            d.projection.put(list10040().toDoc())
            d.projection.put(card(40).toDoc())

            // The note's guarded() runs to completion on its own; the
            // interleaving is created by draining INSIDE its block, between
            // the ledger read (before the block) and the write (after it).
            d.projection.backlog.insuring(ProjectionWork.NONE) {
                d.projection.backlog.drain { it() }
                assertEquals(40, d.rankOf(subject))
                assertNull(d.marker(), "the drain rewrote the marker clean")
                d.projection.put(note().toDoc()) // the plain write itself
                Outcome(Unit, ProjectionWork.NONE)
            }

            // A clean ledger: the next trust write that leaves work — a list
            // naming a fresh service — must persist its own write-ahead. Under
            // the lost-update bug `before` still named the drained entries, the
            // delta was empty and nothing was persisted.
            d.projection.put(list10040(serviceKey = service2).toDoc())
            val marker = d.marker()
            assertNotNull(marker, "the list's write-ahead was persisted")
            assertTrue(ServiceKey(service2) in marker.followerCounts)
            d.projection.backlog.drain { it() }
            assertNull(d.marker())
        }

    /**
     * A BULK write-ahead mid-round rewrites the marker as a whole document
     * (past DELTA_ADD_MAX the write-ahead is one put, not cell adds). The
     * round's own snapshot must survive that rewrite: a version of this ledger
     * that TOOK its snapshot out of memory computed the write-ahead without
     * it, so the marker stopped naming the subjects still being derived — a
     * crash before the round's final rewrite would have lost them for good.
     */
    @Test
    fun `a bulk write-ahead during a round keeps the marker covering the round's work`() =
        runBlocking {
            val d = Deferred()
            d.projection.put(card(40).toDoc())
            d.projection.put(list10040().toDoc()) // the walk is the round's work

            var landed = false
            d.projection.backlog.drain { body ->
                body()
                if (!landed && d.rankOf(subject) == 40) {
                    landed = true
                    // 100 cards for OTHER subjects: a delta past DELTA_ADD_MAX,
                    // persisted as one marker-doc put.
                    d.projection.putAll((1..100).map { i -> ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", i.toString(16).padStart(64, 'e')), arrayOf("rank", "1")), "", "").toDoc() })
                    val marker = assertNotNull(d.marker(), "the marker still stands")
                    assertTrue(ServiceKey(service) in marker.followerCounts, "the marker still names the service the round is walking")
                }
            }
            assertTrue(landed)
            assertNull(d.marker(), "everything drained in the end")
        }

    /**
     * INSURANCE THAT PRODUCED NO WORK must not leave the marker standing. A
     * batch of cards by a signer no 10040 maps insures every subject (one
     * marker put past DELTA_ADD_MAX) and then writes no cells; with nothing
     * pending no drain ever rewrote the marker, so the next boot inherited
     * every one of those subjects as drift to re-derive to nothing.
     */
    @Test
    fun `a batch that insures subjects but leaves no work clears its marker`() =
        runBlocking {
            val d = Deferred()
            // No 10040 at all: every card is by an unmapped signer.
            d.projection.putAll((1..100).map { i -> ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", i.toString(16).padStart(64, 'a')), arrayOf("rank", "1")), "", "").toDoc() })
            assertNull(d.marker(), "no work was left, so no marker stands")
            // The per-event path applies its cell inline (or, unmapped, nothing)
            // and leaves no work either; a list naming a fresh service is what
            // queues work, and its marker stands until the walk — then is gone.
            d.projection.put(card(40).toDoc())
            assertNull(d.marker(), "a single card leaves no work behind")
            d.projection.put(list10040().toDoc())
            assertNotNull(d.marker(), "the list's service walk is queued work, and the marker names it")
            d.projection.backlog.drain { it() }
            assertNull(d.marker())
            assertEquals(40, d.rankOf(subject))
        }

    /** A failed round leaves the snapshot pending in memory as well as on disk, so the retry re-derives it. */
    @Test
    fun `a failed drain puts its snapshot back`() =
        runBlocking {
            val d = Deferred()
            d.projection.put(list10040().toDoc())
            d.projection.put(card(40).toDoc())
            assertFailsWith<IllegalStateException> {
                d.projection.backlog.drain { error("engine down") }
            }
            assertNotNull(d.marker(), "the marker survived the failure")
            d.projection.backlog.drain { it() }
            assertEquals(40, d.rankOf(subject), "the retry derived the snapshot")
            assertNull(d.marker())
        }

    /** A previous process's marker is discovered by the first drain and healed with the provider map dropped. */
    @Test
    fun `an inherited marker is drained once and cleared`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val reputations: ReputationIndex = InMemoryReputationIndex()
            val first = TrustProjection(index, reputations)
            first.put(list10040().toDoc())
            first.put(card(40).toDoc())
            // The crashed process: its marker names the subject, its projection never ran.
            reputations.remove(subject)
            reputations.put(ReputationDoc(ProjectionLedger.MARKER_KEY, serviceCells(subject to 1), emptyMap()))

            val restarted = TrustProjection(index, reputations)
            restarted.backlog.drainInBackground { }
            restarted.backlog.drain { it() }
            assertEquals(serviceCells(service to 40), reputations.get(subject)?.influenceScores)
            assertNull(reputations.get(ProjectionLedger.MARKER_KEY))
        }

    /**
     * A crashed process's ledger, at the scale the real one runs at: more
     * subjects than one gate slice holds, so a round is several slices.
     * Returns the restarted projection and the subjects it must heal.
     */
    private suspend fun inheritedBacklog(reputations: ReputationIndex): Pair<TrustProjection, List<String>> {
        val index = InMemoryEventIndex()
        val subjects = manySubjects()
        val first = TrustProjection(index, reputations)
        first.put(list10040().toDoc())
        subjects.forEach { first.put(cardFor(it, 50).toDoc()) }
        // The process died with the projection unwritten and the marker naming all of it.
        subjects.forEach { reputations.remove(it) }
        reputations.put(ReputationDoc(ProjectionLedger.MARKER_KEY, subjects.associate { ServiceKey(it) to 1 }, emptyMap()))
        val restarted = TrustProjection(index, reputations)
        restarted.backlog.drainInBackground { }
        return restarted to subjects
    }

    /**
     * THE FROZEN BACKLOG. A round used to retire its whole snapshot in one
     * step, after the last slice: until then the marker stood at full size and
     * the backlog gauge never moved. On staging that meant 139,524 inherited
     * subjects showing as untouched for over an hour per pass —
     * indistinguishable, to anyone watching, from a drain doing nothing at all.
     * Each slice's ack is credited when it happens.
     */
    @Test
    fun `a round retires each slice as it lands, not once at the end`() =
        runBlocking {
            val reputations: ReputationIndex = InMemoryReputationIndex()
            val (store, subjects) = inheritedBacklog(reputations)

            // The marker's size at every point the drain handed the gate back.
            val sizes = mutableListOf<Int>()
            store.backlog.drain { body ->
                body()
                sizes += reputations.get(ProjectionLedger.MARKER_KEY)?.influenceScores?.size ?: 0
            }

            assertNull(reputations.get(ProjectionLedger.MARKER_KEY), "marker gone once the ledger is clean")
            assertEquals(0L, store.backlog.pendingSubjects())
            assertEquals(subjects.size, subjects.count { reputations.get(it)?.influenceScores == serviceCells(service to 50) }, "every subject healed")
            assertTrue(
                sizes.any { it in 1 until subjects.size },
                "the marker shrank WHILE the round ran; it only ever read ${sizes.distinct().sorted()}",
            )
        }

    /**
     * THE LOST ROUND. Retiring per round also meant a round that died partway
     * left nothing behind: the marker still named every subject, including the
     * ones already derived and acked, so the next round paid for all of them
     * again — and on a ledger that takes longer to drain than the process
     * stays up, it never finishes. The same property is what lets two
     * concurrent drains of one ledger (the background loop's and
     * [TrustReconciler]'s) skip each other's finished slices instead of both
     * deriving the whole snapshot.
     */
    @Test
    fun `slices retired before a failure are not re-derived by the next round`() =
        runBlocking {
            val reputations: ReputationIndex = InMemoryReputationIndex()
            val (store, subjects) = inheritedBacklog(reputations)

            // Die once the first slice has been derived, written and retired.
            var gateCalls = 0
            assertFailsWith<IllegalStateException> {
                store.backlog.drain { body ->
                    // Call 1 derives the first slice, call 2 retires it; the
                    // process dies reaching for the second slice's work.
                    if (++gateCalls > 2) error("the process dies mid-round")
                    body()
                }
            }

            val left = store.backlog.pendingSubjects()
            assertTrue(left in 1 until subjects.size.toLong(), "a slice was retired before the failure, not the whole round: $left")
            assertEquals(
                left.toInt(),
                assertNotNull(reputations.get(ProjectionLedger.MARKER_KEY)).influenceScores.size,
                "the marker names exactly the unhealed remainder — the crash-safety contract",
            )
            assertEquals(
                subjects.size - left.toInt(),
                subjects.count { reputations.get(it)?.influenceScores == serviceCells(service to 50) },
                "and the retired subjects are the ones already written",
            )

            store.backlog.drain { it() }
            assertNull(reputations.get(ProjectionLedger.MARKER_KEY), "the remainder healed")
            assertEquals(subjects.size, subjects.count { reputations.get(it)?.influenceScores == serviceCells(service to 50) })
        }
}
