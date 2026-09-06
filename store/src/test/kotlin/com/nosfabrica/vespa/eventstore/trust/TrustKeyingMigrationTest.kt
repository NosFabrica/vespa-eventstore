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

import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A store fed under the OBSERVER-keyed model — cells under the observer's
 * key, none under the service's — is brought onto the service-keyed one
 * exactly once: the named services walked into cells, the old cells swept,
 * a marker left, and nothing touched on the next boot.
 */
class TrustKeyingMigrationTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private val subject = "ab".repeat(32)
    private val subject2 = "cd".repeat(32)

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private val index = InMemoryEventIndex()
    private val reputations = InMemoryReputationIndex()
    private val projection = TrustProjection(index, reputations)
    private val reconciler = TrustReconciler(index, reputations, projection.recompute, projection.backlog)
    private val migration = TrustKeyingMigration(reputations, reconciler, projection.recompute)

    private fun list10040() = TrustProviderListEvent(id(), observer, 1_000L + seq, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/"), arrayOf("30382:followers", service, "wss://scores.example.com/")), "", "")

    private fun card(
        about: String,
        rank: Int,
    ) = ContactCardEvent(id(), service, 1_000L + seq, arrayOf(arrayOf("d", about), arrayOf("rank", rank.toString()), arrayOf("followers", "12")), "", "")

    /** The old shape, written straight to the index and the reputation store: events without their projection, cells under the observer. */
    private suspend fun seedObserverKeyedStore() {
        index.put(list10040().toDoc())
        index.put(card(subject, 87).toDoc())
        index.put(card(subject2, 40).toDoc())
        reputations.put(ReputationDoc(subject, serviceCells(observer to 87), serviceCells(observer to 12.0)))
        reputations.put(ReputationDoc(subject2, serviceCells(observer to 40), serviceCells(observer to 12.0)))
        // The projection's own bookkeeping must survive untouched.
        reputations.put(ReputationDoc(MaxRankBackfill.MARKER_KEY, serviceCells("done" to 1)))
    }

    @Test
    fun `an observer-keyed store is re-keyed by service, swept, and marked`() =
        runBlocking {
            seedObserverKeyedStore()

            val done = migration.run()
            assertFalse(done.refused)
            assertEquals(1, done.servicesProjected, "the one named service was walked into cells")
            assertEquals(2, done.keysRemoved, "the observer's key swept off each subject (both tensors at once)")
            assertEquals(ReputationDoc(subject, serviceCells(service to 87), serviceCells(service to 12.0)), reputations.get(subject))
            assertEquals(ReputationDoc(subject2, serviceCells(service to 40), serviceCells(service to 12.0)), reputations.get(subject2))
            assertNotNull(reputations.get(TrustKeyingMigration.MARKER_KEY), "the marker stands")
            assertNotNull(reputations.get(MaxRankBackfill.MARKER_KEY), "other markers are not subjects")
            assertTrue(reconciler.verify().isClean(), "what the migration wrote is exactly what a derive says")
        }

    @Test
    fun `the marker makes the next boot a no-op`() =
        runBlocking {
            seedObserverKeyedStore()
            migration.run()
            reputations.put(ReputationDoc(subject, serviceCells("ff".repeat(32) to 1))) // a stray cell after the marker: not this walk's business
            val again = migration.run()
            assertEquals(TrustKeyingMigration.Migration(0, 0, refused = false), again)
            assertEquals(serviceCells("ff".repeat(32) to 1), reputations.get(subject)?.influenceScores, "nothing swept under a standing marker")
        }

    @Test
    fun `a store with no reputation documents has nothing to migrate and is marked at once`() =
        runBlocking {
            val done = migration.run()
            assertEquals(TrustKeyingMigration.Migration(0, 0, refused = false), done)
            assertNotNull(reputations.get(TrustKeyingMigration.MARKER_KEY))
        }

    /** No readable 10040 beside real parents is an engine still serving its corpus, not a providerless relay: refuse, retry later. */
    @Test
    fun `no readable 10040 beside reputation documents refuses rather than sweeping everything`() =
        runBlocking {
            reputations.put(ReputationDoc(subject, serviceCells(observer to 87)))
            val done = migration.run()
            assertTrue(done.refused)
            assertEquals(serviceCells(observer to 87), reputations.get(subject)?.influenceScores, "nothing removed")
            assertNull(reputations.get(TrustKeyingMigration.MARKER_KEY), "no marker until a run finishes")
        }

    /** A service some list stopped naming is swept like an old observer key: no lens can reach it. */
    @Test
    fun `the sweep also drops cells of services no list names any more`() =
        runBlocking {
            seedObserverKeyedStore()
            val gone = "6e".repeat(32)
            reputations.put(ReputationDoc(subject, serviceCells(observer to 87, gone to 50), serviceCells(observer to 12.0)))
            val done = migration.run()
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)
            assertEquals(3, done.keysRemoved)
        }

    /**
     * THE SISYPHEAN MARKER. A boolean marker meant a crash anywhere threw the
     * whole attempt away, and step one is a corpus-scale reconcile: on a store
     * where the migration takes longer than the process stays up, it can never
     * finish, however fast each step is. `servicesProjected` is the witness —
     * it counts what the RECONCILE rebuilt, so a resumed run reporting 0 while
     * still sweeping is the reconcile being skipped rather than redone.
     */
    @Test
    fun `a marker left mid-migration resumes at the sweep instead of reconciling again`() =
        runBlocking {
            seedObserverKeyedStore()
            // What the previous process left after its reconcile landed: the
            // cells are keyed by service already, the old observer keys are not
            // yet swept, and the marker says so.
            reputations.put(ReputationDoc(subject, serviceCells(service to 87, observer to 87), serviceCells(service to 12.0)))
            reputations.put(ReputationDoc(subject2, serviceCells(service to 40, observer to 40), serviceCells(service to 12.0)))
            reputations.put(ReputationDoc(TrustKeyingMigration.MARKER_KEY, serviceCells("reconciled" to 1)))

            val done = migration.run()
            assertFalse(done.refused)
            assertEquals(0, done.servicesProjected, "the reconcile was skipped — the marker said it had landed")
            assertEquals(2, done.keysRemoved, "the sweep still ran: the observer's stale key is gone from both subjects")
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)
            assertEquals(serviceCells(service to 40), reputations.get(subject2)?.influenceScores)
            assertEquals(TrustKeyingProgress.Phase.Done, migration.progress.phase)
        }

    /** Every marker written before phases existed carries only `done`, and must still read as complete. */
    @Test
    fun `a legacy done-only marker still means finished`() =
        runBlocking {
            seedObserverKeyedStore()
            reputations.put(ReputationDoc(TrustKeyingMigration.MARKER_KEY, serviceCells("done" to 1)))
            val again = migration.run()
            assertEquals(TrustKeyingMigration.Migration(0, 0, refused = false), again)
            assertEquals(serviceCells(observer to 87), reputations.get(subject)?.influenceScores, "nothing swept under a standing marker")
            assertEquals(TrustKeyingProgress.Phase.Done, migration.progress.phase)
        }

    /** A finished run leaves the reconcile flag too, so a later restart never redoes it either. */
    @Test
    fun `a finished migration records both phases`() =
        runBlocking {
            seedObserverKeyedStore()
            migration.run()
            val marker = assertNotNull(reputations.get(TrustKeyingMigration.MARKER_KEY)).influenceScores
            assertTrue(ServiceKey("done") in marker, "finished")
            assertTrue(ServiceKey("reconciled") in marker, "and the expensive half is recorded on its own")
        }

    /** The question that started this: the walk reports where it is, not merely whether it broke. */
    @Test
    fun `progress reports a phase and a count rather than nothing at all`() =
        runBlocking {
            assertEquals(TrustKeyingProgress.Phase.NotStarted, migration.progress.phase)
            assertEquals("trust-keying notstarted", migration.progress.line())
            seedObserverKeyedStore()
            migration.run()
            assertEquals(TrustKeyingProgress.Phase.Done, migration.progress.phase)
            assertEquals(2L, migration.progress.keysRemoved.get(), "the sweep's work is counted, not just its completion")
        }
}
