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
package com.nosfabrica.vespa.eventstore.runtime

import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class WriteLocksTest {
    @BeforeTest
    @AfterTest
    fun clean() = IngestStats.reset()

    /**
     * The wait split is a process-wide map that never forgets a key, so its keys must come from
     * a closed set (docs/telemetry.md §4). A gate holder annotates its hold with free text
     * carrying this call's counts, as the trust derive does; keyed by that text, every distinct
     * count was a new row for good. Fifty holds with fifty different details, each with an
     * ingest card queued behind it, must stay one row.
     */
    @Test
    fun `the wait split stays one row however many ways a holder describes its work`() =
        runBlocking {
            val locks = WriteLocks()
            repeat(50) { i ->
                val holding = CompletableDeferred<Unit>()
                val release = CompletableDeferred<Unit>()
                val holder =
                    launch {
                        locks.underGate(WriteLocks.GATE) {
                            IngestStats.annotateHold("derive $i subject(s) in ${i / 10 + 1} chunk(s)", WriteLocks.TRUST_GATE)
                            holding.complete(Unit)
                            release.await()
                        }
                    }
                holding.await()
                // Undispatched: it samples the holder and parks on the gate before the hold ends.
                val waiter = launch(start = CoroutineStart.UNDISPATCHED) { locks.gated(trust = true, WriteLocks.INGEST) {} }
                release.complete(Unit)
                joinAll(holder, waiter)
            }
            val split = assertNotNull(IngestStats.blockedSplit()[WriteLocks.INGEST_TRUST.wait], "the card's gate wait was attributed")
            assertEquals(setOf(WriteLocks.GATE.hold), split.keys, "one closed key for fifty differently described holds")
        }
}
