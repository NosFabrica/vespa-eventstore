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
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
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

    /** Nothing running reports nothing — the panel must not invent activity. */
    @Test
    fun `an idle store reports no steps`() {
        assertTrue(TrustProgress.snapshot().none { !it.finished }, "no repair, no rows")
    }
}
