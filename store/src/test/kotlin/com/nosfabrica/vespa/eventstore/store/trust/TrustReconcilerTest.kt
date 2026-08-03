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
package com.nosfabrica.vespa.eventstore.store.trust

import com.nosfabrica.vespa.eventstore.store.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.store.mapping.toDoc
import com.nosfabrica.vespa.eventstore.vespa.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.vespa.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.vespa.client.EventIndex
import com.nosfabrica.vespa.eventstore.vespa.client.ReputationIndex
import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc
import com.nosfabrica.vespa.eventstore.vespa.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.vespa.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The repair for a projection no write can reach: reconcile's drift detection and the rebuild hammer. */
class TrustReconcilerTest {
    private val observer = "0b".repeat(32)
    private val observer2 = "2b".repeat(32)
    private val service = "5e".repeat(32)
    private val service2 = "6e".repeat(32)
    private val subject = "ab".repeat(32)

    private val index = InMemoryEventIndex()
    private val reputations = InMemoryReputationIndex()
    private val projection = TrustProjection(index, reputations)
    private val reconciler = TrustReconciler(index, reputations, projection.recompute, projection.dirt)
    private val store = NostrSemanticsStore(projection, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    // The default names ONE service for BOTH dimensions — the single-provider norm.
    private fun list10040(
        author: String = observer,
        serviceKey: String = service,
        at: Long = next(),
        types: List<String> = listOf("30382:rank", "30382:followers"),
    ) = TrustProviderListEvent(id(), author, at, types.map { arrayOf(it, serviceKey, "wss://scores.example.com/") }.toTypedArray(), "", "")

    private fun card(
        signer: String = service,
        about: String = subject,
        rank: Int? = 87,
        followers: Int? = 120,
        at: Long = next(),
        eventId: String = id(),
    ): ContactCardEvent {
        val tags =
            buildList {
                add(arrayOf("d", about))
                rank?.let { add(arrayOf("rank", it.toString())) }
                followers?.let { add(arrayOf("followers", it.toString())) }
            }.toTypedArray()
        return ContactCardEvent(eventId, signer, at, tags, "", "")
    }

    @Test
    fun `rebuildAll re-derives everything from the event corpus`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            reputations.docs.clear()

            reconciler.rebuildAll()
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `an empty provider map is not cached, so a late corpus still projects`() =
        runBlocking {
            // The engine is cold, or the 10040s have not been mirrored yet. Either
            // way, asking early must not poison the map: it is read once per
            // reconcile pass, and a cached emptiness leaves a relay unrankable
            // until it restarts. Observed live — 271 kind-10040s queryable at full
            // coverage while ten minutes of retries all read the same cached
            // nothing, because only a 10040 WRITE invalidates, and dedup drops a
            // 10040 the store already holds before any write happens.
            val coldIndex = InMemoryEventIndex()
            val coldReputations = InMemoryReputationIndex()
            val cold = TrustProjection(coldIndex, coldReputations)
            val coldReconciler = TrustReconciler(coldIndex, coldReputations, cold.recompute, cold.dirt)
            val coldStore = NostrSemanticsStore(cold, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

            assertEquals(0, coldReconciler.reconcile().services, "nothing there yet")

            // Written through a DIFFERENT store over the same index, so nothing
            // invalidates the cold one's map — the dedup case, exactly.
            val other = NostrSemanticsStore(TrustProjection(coldIndex, InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            other.insert(list10040())
            other.insert(card())

            assertEquals(1, coldReconciler.reconcile().services, "the next pass must look again, not replay a cached empty map")
            coldStore.close()
            other.close()
        }

    @Test
    fun `reconcile re-derives a service whose scores were stored before its 10040`() =
        runBlocking {
            // The mirror's normal order: scores first, by a service nothing maps
            // yet, so putAll drops them. Then the 10040 arrives in a LATER run,
            // where it is a duplicate — no write, so no repair trigger fires.
            store.insert(card())
            assertNull(reputations.get(subject), "unmapped scores derive nothing")

            store.insert(list10040())
            reputations.docs.clear() // as if that run's derivation never happened
            assertNull(reputations.get(subject))

            val report = reconciler.reconcile()
            assertEquals(listOf(service), report.rebuilt, "the unprojected service is re-derived")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `reconcile leaves a healthy projection alone`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())

            val report = reconciler.reconcile()
            assertEquals(emptyList(), report.rebuilt, "nothing to fix")
            assertEquals(1, report.services, "but the service was examined")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `reconcile catches a service remapped to a different observer`() =
        runBlocking {
            // Cells exist, so "has a doc" would call this clean — but they belong
            // to the previous observer. Checking the CURRENT observer's cell is
            // what makes the difference.
            store.insert(list10040())
            store.insert(card())
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)

            reputations.docs.clear()
            reputations.updateCells(listOf(ReputationCells(subject, observer2, 87, 1.0)))

            val report = reconciler.reconcile()
            assertEquals(listOf(service), report.rebuilt)
            assertEquals(87, reputations.get(subject)?.influenceScores?.get(observer))
        }

    @Test
    fun `reconcile ignores a mapped service we hold no scores for`() =
        runBlocking {
            store.insert(list10040())
            val report = reconciler.reconcile()
            assertEquals(0, report.services, "nothing stored for it, nothing to project")
            assertTrue(report.isClean())
        }

    @Test
    fun `reconcile is a no-op with no provider lists at all`() =
        runBlocking {
            store.insert(card())
            val report = reconciler.reconcile()
            assertEquals(0, report.services)
            assertTrue(report.isClean())
        }

    /**
     * A crashed trust write leaves the persisted dirt marker. A RESTARTED
     * process's reconcile must repair exactly what it names — including drift
     * the sampling can never see (subjects mid-corpus while the newest cards
     * are healthy).
     */
    @Test
    fun `reconcile heals the dirty marker a crashed write left behind`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card()) // healthy, and the newest card the sample will hit
            val lost = "e1".repeat(32)
            index.put(card(about = lost, at = 100).toDoc()) // stored BEHIND the projection: the crash shape
            reputations.put(ReputationDoc(DirtLedger.MARKER_KEY, mapOf(lost to 1), emptyMap()))
            assertNull(reputations.get(lost))

            // A fresh projection + reconciler — the restart. The service samples
            // clean (its newest card is projected), so ONLY the marker heal can
            // repair the lost subject.
            val restarted = TrustProjection(index, reputations)
            val report = TrustReconciler(index, reputations, restarted.recompute, restarted.dirt).reconcile()
            assertEquals(mapOf(observer to 87), reputations.get(lost)?.influenceScores, "the marker-named subject is re-derived")
            assertNull(reputations.get(DirtLedger.MARKER_KEY), "marker cleared")
            assertTrue(report.isClean(), "nothing left for sampling to find")
        }

    /**
     * A provider shared by two observers where only ONE observer's cells exist —
     * the drift a single-winner map used to create. The per-observer check must
     * flag and repair it.
     */
    @Test
    fun `reconcile catches a shared provider missing one observer's cells`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service))
            store.insert(card())
            assertEquals(mapOf(observer to 87, observer2 to 87), reputations.get(subject)?.influenceScores)

            // Strip observer2's cell, as the old single-winner projection left it.
            reputations.put(ReputationDoc(subject, mapOf(observer to 87), mapOf(observer to 120.0)))

            val report = reconciler.reconcile()
            assertEquals(listOf(service), report.rebuilt, "one observer unprojected = the service is dirty")
            assertEquals(mapOf(observer to 87, observer2 to 87), reputations.get(subject)?.influenceScores)
        }

    /** A followers-only corpus IS projected (through the followers mapping) — no rebuild loop on every startup. */
    @Test
    fun `reconcile accepts a followers-only service as projected`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = null, followers = 42))
            assertEquals(mapOf(observer to 42.0), reputations.get(subject)?.followerCounts)

            val report = reconciler.reconcile()
            assertTrue(report.isClean(), "follower cells are projection too")
        }

    /**
     * A service mapped for a dimension its cards never assert derives nothing —
     * the sampling must not call that drift, or every startup re-walks the
     * service for a projection that can never exist.
     */
    @Test
    fun `reconcile does not rebuild a service whose cards never assert the mapped dimension`() =
        runBlocking {
            store.insert(list10040(types = listOf("30382:rank")))
            store.insert(card(rank = null, followers = 42)) // signer is rank-mapped only; the card asserts only followers
            assertNull(reputations.get(subject), "nothing attributable to derive")

            val report = reconciler.reconcile()
            assertTrue(report.isClean(), "no sampled card asserts the mapped dimension — nothing to rebuild")
        }

    /** The never-triggered mirror order, follower-provider edition: reconcile must repair through the followers map. */
    @Test
    fun `reconcile re-derives an unprojected follower service`() =
        runBlocking {
            store.insert(card(signer = service2, rank = null, followers = 42))
            store.insert(list10040(serviceKey = service2, types = listOf("30382:followers")))
            reputations.docs.clear() // as if that run's derivation never happened

            val report = reconciler.reconcile()
            assertEquals(listOf(service2), report.rebuilt, "the unprojected follower service is re-derived")
            assertEquals(mapOf(observer to 42.0), reputations.get(subject)?.followerCounts)
        }

    /**
     * A parent whose subject has no stored cards left (its last card's removal
     * crashed before the recompute) is unreachable from any card walk. The
     * hammer must still remove it — from the REPUTATION corpus.
     */
    @Test
    fun `rebuildAll removes an orphan parent no card walk can reach`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            val orphan = "e2".repeat(32)
            reputations.put(ReputationDoc(orphan, mapOf(observer to 50), emptyMap()))
            // The projection's own bookkeeping must survive the sweep untouched.
            val marker = ReputationDoc(DirtLedger.MARKER_KEY, emptyMap(), mapOf(service to 1.0))
            reputations.put(marker)

            reconciler.rebuildAll()
            assertNull(reputations.get(orphan), "no cards -> no parent")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores, "real subjects survive the sweep")
            assertEquals(marker, reputations.get(DirtLedger.MARKER_KEY), "the dirt marker is not a subject")
        }

    // ---- verify: the full derive-vs-stored audit ------------------------------

    @Test
    fun `verify calls a faithful projection clean`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            val audit = reconciler.verify()
            assertTrue(audit.isClean(), "stored doc matches the records")
            assertEquals(1, audit.subjectsChecked)
            assertEquals(1, audit.parentsChecked)
        }

    @Test
    fun `verify reports stale cells, missing docs and orphans, and repairs exactly them`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card()) // subject: the healthy baseline, then tampered stale
            val gone = "e3".repeat(32)
            store.insert(card(about = gone, rank = 60)) // will lose its doc entirely
            val orphan = "e4".repeat(32) // a doc with no records behind it

            reputations.put(ReputationDoc(subject, mapOf(observer to 1), mapOf(observer to 1.0))) // stale
            reputations.docs.remove(gone) // missing
            reputations.put(ReputationDoc(orphan, mapOf(observer to 99), emptyMap())) // orphan

            val audit = reconciler.verify()
            assertEquals(3, audit.driftCount)
            assertEquals(2, audit.subjectsChecked, "both scored subjects compared")
            val bySubject = audit.drift.associateBy { it.subject }
            assertEquals(
                87,
                bySubject
                    .getValue(subject)
                    .expected
                    ?.influenceScores
                    ?.get(observer),
                "stale: records say 87",
            )
            assertEquals(
                1,
                bySubject
                    .getValue(subject)
                    .actual
                    ?.influenceScores
                    ?.get(observer),
                "stale: doc says 1",
            )
            assertNull(bySubject.getValue(gone).actual, "missing: records score it, no doc")
            assertNull(bySubject.getValue(orphan).expected, "orphan: doc without records")

            val repaired = reconciler.verify(repair = true)
            assertEquals(3, repaired.driftCount, "the same drift, now repaired in place")
            assertTrue(reconciler.verify().isClean(), "repair converged")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
            assertEquals(mapOf(observer to 60), reputations.get(gone)?.influenceScores)
            assertNull(reputations.get(orphan))
        }

    /** A fully retracted subject legitimately has NO doc — the audit must not call that drift. */
    @Test
    fun `verify accepts a retracted subject with no parent doc`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, at = 100))
            store.insert(card(rank = null, followers = null, at = 200))
            assertNull(reputations.get(subject))
            val audit = reconciler.verify()
            assertTrue(audit.isClean(), "empty derivation == no doc")
            assertEquals(1, audit.subjectsChecked, "the retracted subject was still checked")
        }

    /** Deferred-mode queued work is LAG, not drift: verify drains it first and then finds nothing. */
    @Test
    fun `verify drains deferred work before judging`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val idx = InMemoryEventIndex()
            val proj = TrustProjection(idx, reps)
            proj.dirt.deferTo { }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            st.insert(card())
            assertNull(reps.get(subject), "still queued")

            val audit = TrustReconciler(idx, reps, proj.recompute, proj.dirt).verify()
            assertTrue(audit.isClean(), "the queue was settled, not reported")
            assertEquals(mapOf(observer to 87), reps.get(subject)?.influenceScores)
        }

    // ---- sweepOrphanScores: the scores nobody's 10040 can ever attribute ------

    @Test
    fun `sweeps the cards of a service no 10040 names, and keeps the mapped one's`() =
        runBlocking {
            store.insert(list10040()) // names `service`, both dimensions
            store.insert(card()) // mapped: projects, must survive
            store.insert(card(signer = service2, about = "c1".repeat(32)))
            store.insert(card(signer = service2, about = "c2".repeat(32)))

            val report = reconciler.sweepOrphanScores()
            assertEquals(listOf(service2), report.orphans)
            assertEquals(2, report.scoresSwept)
            assertEquals(2, report.servicesSeen, "both signers were examined")
            assertEquals(0, index.count(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service2))), "the orphan corpus is gone")
            assertEquals(1, index.count(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service))), "the mapped service is untouched")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores, "an orphan carried no cell, so none was lost")
            assertTrue(reconciler.verify().isClean(), "the sweep leaves the projection consistent")
            assertTrue(reconciler.sweepOrphanScores().isClean(), "second run finds nothing")
        }

    /** Named for ONE dimension is named: a followers-only provider is not an orphan. */
    @Test
    fun `a service named for a single dimension is not swept`() =
        runBlocking {
            store.insert(list10040()) // keeps the attribution map non-empty
            store.insert(list10040(author = observer2, serviceKey = service2, types = listOf("30382:followers")))
            store.insert(card(signer = service2, about = "c1".repeat(32)))

            val report = reconciler.sweepOrphanScores()
            assertTrue(report.isClean())
            assertEquals(1, index.count(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service2))))
        }

    /**
     * The footgun this refuses: "nobody named it" and "no 10040 is readable yet"
     * produce the same candidate list, and the second is the mirror that stored
     * its scores before its provider lists — sweeping there deletes the whole
     * score corpus.
     */
    @Test
    fun `an unreadable attribution map refuses the sweep instead of deleting everything`() =
        runBlocking {
            store.insert(card())
            store.insert(card(signer = service2, about = "c1".repeat(32)))

            val report = reconciler.sweepOrphanScores()
            assertTrue(report.refused, "no 10040 stored -> nothing swept")
            assertFalse(report.isClean(), "a refusal examined nothing; it is not an all-clear")
            assertEquals(0, report.scoresSwept)
            assertEquals(2, index.count(EventQuery(kinds = listOf(ContactCardEvent.KIND))), "every card survives")
        }

    @Test
    fun `a dry run reports what it would free and writes nothing`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            store.insert(card(signer = service2, about = "c1".repeat(32)))
            store.insert(card(signer = service2, about = "c2".repeat(32)))

            val report = reconciler.sweepOrphanScores(dryRun = true)
            assertTrue(report.dryRun)
            assertEquals(listOf(service2), report.orphans)
            assertEquals(2, report.scoresSwept, "the count it WOULD delete")
            assertEquals(3, index.count(EventQuery(kinds = listOf(ContactCardEvent.KIND))), "no writes")
        }

    /**
     * The race that must never delete a live provider's scores: a 10040 naming
     * the candidate lands after the snapshot. The re-check inside the writer
     * lock catches it at the page boundary.
     */
    @Test
    fun `a service claimed mid-sweep is dropped from the sweep`() =
        runBlocking {
            store.insert(list10040()) // the map is readable, so the sweep runs
            store.insert(card(signer = service2, about = "c1".repeat(32)))

            var claimed = false
            var inGate = false
            val guarded =
                object : EventIndex by index {
                    override suspend fun removeDocs(docs: List<EventDoc>) {
                        check(inGate) { "orphan deletion outside the writer lock" }
                        index.removeDocs(docs)
                    }
                }
            val racing =
                TrustReconciler(guarded, reputations, projection.recompute, projection.dirt, gate = { body ->
                    // A concurrent writer commits the 10040 just before the first
                    // page takes the lock — through the store, so the attribution
                    // cache is invalidated exactly as it would be live.
                    if (!claimed) {
                        claimed = true
                        store.insert(list10040(author = observer2, serviceKey = service2))
                    }
                    inGate = true
                    try {
                        body()
                    } finally {
                        inGate = false
                    }
                })

            val report = racing.sweepOrphanScores()
            assertEquals(listOf(service2), report.remapped)
            assertTrue(report.orphans.isEmpty(), "it was never swept")
            assertEquals(1, index.count(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service2))), "its cards stand")
        }

    /** Every mutating reconciler batch must run inside the gate (the store's writer lock). */
    @Test
    fun `reconcile and rebuildAll mutate only under the gate`() =
        runBlocking {
            store.insert(card()) // stored before its 10040 — reconcile will rebuild
            store.insert(list10040())
            reputations.docs.clear()

            var inGate = false
            var gated = 0
            val guarded =
                object : ReputationIndex by reputations {
                    override suspend fun putAll(reputations: List<ReputationDoc>) {
                        check(inGate) { "projection write outside the writer lock" }
                        this@TrustReconcilerTest.reputations.putAll(reputations)
                    }
                }
            val proj = TrustProjection(index, guarded)
            val gatedReconciler =
                TrustReconciler(index, guarded, proj.recompute, proj.dirt, gate = { body ->
                    inGate = true
                    gated++
                    try {
                        body()
                    } finally {
                        inGate = false
                    }
                })

            gatedReconciler.reconcile()
            gatedReconciler.rebuildAll()
            assertTrue(gated > 0, "the gate was exercised")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }
}
