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

import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * THE LONG PHASES MUST BE THE REPORTED ONES.
 *
 * The registry was wired to the phases AFTER the drain, and the drain is where
 * a reconcile spends nearly all of its time on an inherited ledger — so a page
 * watching a repair drew an empty panel for exactly as long as someone was
 * watching. The call that would have reported it was written and silently did
 * not apply, which no test noticed because no test asked whether anything
 * reported at all.
 */
class TrustProgressReportedTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private val index = InMemoryEventIndex()
    private val reputations = InMemoryReputationIndex()
    private val projection = TrustProjection(index, reputations)

    @BeforeTest fun clean() = TrustProgress.reset()

    @AfterTest fun tidy() = TrustProgress.reset()

    private fun list10040() = TrustProviderListEvent(id(), observer, 1_000L + seq, arrayOf(arrayOf("30382:rank", service, "wss://s.example/")), "", "")

    private fun card(about: String) = ContactCardEvent(id(), service, 1_000L + seq, arrayOf(arrayOf("d", about), arrayOf("rank", "80")), "", "")

    /** A drain that has work to do must appear in the registry WHILE it runs, with a denominator. */
    @Test
    fun `the drain reports itself, with a total`() =
        runBlocking {
            val subjects = (1..40).map { it.toString(16).padStart(64, '0') }
            index.put(list10040().toDoc())
            subjects.forEach { index.put(card(it).toDoc()) }
            subjects.forEach { reputations.remove(it) }
            reputations.put(
                ReputationDoc(
                    ProjectionLedger.MARKER_KEY,
                    subjects.associate {
                        com.nosfabrica.vespa.eventstore.engine.doc
                            .ServiceKey(it) to 1
                    },
                ),
            )

            val restarted = TrustProjection(index, reputations)
            var sawRunning = false
            restarted.backlog.drainInBackground { }
            restarted.backlog.drain(
                WriteGate { body ->
                    body()
                    // Mid-drain: the registry must already name it.
                    val step = TrustProgress.snapshot().firstOrNull { it.op == ProjectionLedger.DRAIN }
                    if (step != null && !step.finished && step.total > 0) sawRunning = true
                },
            )
            assertTrue(sawRunning, "the drain must report itself and a denominator WHILE it runs, not after")
            val done = TrustProgress.snapshot().firstOrNull { it.op == ProjectionLedger.DRAIN }
            assertTrue(done != null && done.finished, "and say when it is done")
        }

    /**
     * A DRAIN'S FRACTION IS ITS OWN ROUND'S — live ingest must not push it
     * below zero.
     *
     * The numerator was `work.size - pendingNow().size`: one round's snapshot
     * measured against the WHOLE ledger's depth. Those are different sets.
     * Every subject a live write queues DURING the drain joins `pendingNow()`
     * and never `work`, so under ingest the numerator drifts and then goes
     * NEGATIVE — and `TrustProgress.render` reads a non-positive `done` as "no
     * denominator known", printing "re-deriving queued subjects: -37, total
     * unknown". The fraction disappeared exactly when the store was busy
     * enough for someone to want it.
     *
     * SEVERAL SLICES, and a flood WIDER THAN THE ROUND: a page can only read
     * what an advance already wrote, so the reproduction needs one slice to
     * publish the bad number and a later one to observe it — and the flood has
     * to outweigh the round, since that is what takes the subtraction below
     * zero. Both are ordinary: `GATE_SLICE` is 500 and a mirror queues cards
     * far faster than a drain retires subjects.
     */
    @Test
    fun `work queued mid-drain never pushes the drain's fraction negative`() =
        runBlocking {
            // Past one GATE_SLICE, so the drain publishes a fraction and then
            // takes another turn where it can be read back.
            val subjects = (1..600).map { it.toString(16).padStart(64, '0') }
            index.put(list10040().toDoc())
            subjects.forEach { index.put(card(it).toDoc()) }
            subjects.forEach { reputations.remove(it) }
            reputations.put(
                ReputationDoc(
                    ProjectionLedger.MARKER_KEY,
                    subjects.associate {
                        com.nosfabrica.vespa.eventstore.engine.doc
                            .ServiceKey(it) to 1
                    },
                ),
            )

            val restarted = TrustProjection(index, reputations)
            restarted.backlog.drainInBackground { } // deferred: the flood stays queued
            val reported = ArrayList<Long>()
            var flooded = false
            restarted.backlog.drain(
                WriteGate { body ->
                    body()
                    // A burst of live writes lands mid-drain, wider than the
                    // round it interrupts — 600 in this round's work against
                    // 2,600 in the ledger.
                    if (!flooded) {
                        flooded = true
                        // LEFT BEHIND, not merely insured: `insuring` queues
                        // what the block reports as `workLeft`, which is what a
                        // card write actually leaves for the drain.
                        val burst = ProjectionWork((1..2_000).map { "f$it".padStart(64, '0') }.toSet(), emptySet())
                        restarted.backlog.insuring(burst) { Outcome(Unit, burst) }
                    }
                    TrustProgress.snapshot().firstOrNull { it.op == ProjectionLedger.DRAIN }?.let { reported += it.done }
                },
            )
            assertTrue(reported.isNotEmpty(), "the drain must report something while it runs, or this proves nothing")
            assertTrue(reported.all { it >= 0 }, "a fraction can never run backwards past zero, got ${reported.filter { it < 0 }}")
        }

    /** Nothing running reports nothing — the panel must not invent activity. */
    @Test
    fun `an idle store reports no steps`() {
        assertTrue(TrustProgress.snapshot().none { !it.finished }, "no repair, no rows")
    }

    /**
     * THE LONGEST OPERATION MUST CARRY A FRACTION. One service's walk is
     * O(its cards) — 279,594 for the largest on staging — and it reported no
     * denominator at all: a page could name what it was walking and say
     * nothing about how far through, which is the question actually being
     * asked. `projectServices` has always taken an `onCards`; the drain never
     * passed one, so the walk reports itself now instead.
     */
    @Test
    fun `a service walk reports cards done against that service's card count`() =
        runBlocking {
            index.put(list10040().toDoc())
            val subjects = (1..25).map { "d$it".padStart(64, '0') }
            subjects.forEach { index.put(card(it).toDoc()) }

            var sawFraction = false
            projection.recompute.projectServices(
                listOf(service),
                gate =
                    WriteGate { body ->
                        body()
                        val step = TrustProgress.snapshot().firstOrNull { it.op == TrustRecompute.WALK }
                        if (step != null && step.total >= subjects.size.toLong()) sawFraction = true
                    },
            )
            assertTrue(sawFraction, "the walk must report a denominator taken from the service's own card count")
            val done = TrustProgress.snapshot().firstOrNull { it.op == TrustRecompute.WALK }
            assertTrue(done != null && done.finished, "and finish")
            assertEquals(subjects.size.toLong(), done!!.done, "counting the cards it actually applied")
        }

    /**
     * A STEP THAT NEVER ADVANCES MUST READ AS STALLED. `elapsedSec` cannot say
     * it: `updatedMs` only moves on an advance, so a step that begins and never
     * advances has an elapsed of 0 and a `coerceAtLeast(1)` renders it as a
     * confident "1s" — forever. A walk blocked on a read looked exactly like
     * one restarting constantly, and I read it as the latter for an hour.
     */
    @Test
    fun `a step that never advances reports as stalled, not as one second`() {
        TrustProgress.begin("trust-test", "reading", 100)
        Thread.sleep(1100)
        val step = TrustProgress.snapshot().first { it.op == "trust-test" }
        assertEquals(1L, step.elapsedSec, "elapsed cannot tell a frozen step from a fast one")
        assertTrue(step.stalledForSec >= 1, "but stalledForSec grows while nothing advances: ${step.stalledForSec}")
        TrustProgress.advance("trust-test", 10)
        assertTrue(TrustProgress.snapshot().first { it.op == "trust-test" }.stalledForSec < 1, "and resets when it moves")
    }
}
