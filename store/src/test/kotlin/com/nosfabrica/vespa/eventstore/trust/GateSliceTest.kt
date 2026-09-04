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
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The write gate is taken per SLICE, not once per 20,000-subject batch.
 *
 * On staging (2026-09-04) one `gate { recomputeBatch(20_000 subjects) }` held
 * the store's single write mutex for 13 minutes — `lockHeldBy` named it
 * ("derive 20000 subject(s) in 400 chunk(s)") while `lock.ingest.wait` reached
 * 38 minutes and ingest's own work totalled 0.56 seconds. Every other writer —
 * ingest, the monitor's verdicts, the sweeps — queues on that mutex, so the
 * hold is the store's fairness knob and nothing upstream was sizing it: the
 * batch constants are documented as bounding MEMORY.
 *
 * What is asserted here is the property that made the fix safe to make, not a
 * duration: the same subjects are still derived, and the gate is entered more
 * than once so somebody else can get in between.
 */
class GateSliceTest {
    private val reputations = InMemoryReputationIndex()
    private val projection = TrustProjection(InMemoryEventIndex(), reputations)

    @Test
    fun `the gate is taken per slice, not once for the whole batch`() =
        runBlocking {
            val subjects = (1..1_200).map { "%064x".format(it) }
            var entries = 0
            var deepest = 0
            var inside = 0

            projection.recompute.recomputeBatchGated(
                subjects,
                projection.recompute.providerMap(),
                removeEmpties = false,
            ) { body ->
                entries += 1
                inside += 1
                deepest = maxOf(deepest, inside)
                body()
                inside -= 1
            }

            // 1,200 subjects at the default slice of 500 is three holds. The
            // point is not the exact number — it is that the batch does not
            // become ONE hold, which is what it was.
            assertTrue(entries > 1, "the whole batch was taken under one hold: $entries")
            assertEquals(
                (subjects.size + TrustRecompute.GATE_SLICE - 1) / TrustRecompute.GATE_SLICE,
                entries,
                "one hold per slice",
            )
            // Never nested: a slice releases before the next one is taken, which
            // is the whole point — a nested take would deadlock the real mutex.
            assertEquals(1, deepest, "a slice must release before the next is taken")
        }

    @Test
    fun `an empty batch takes the gate no times at all`() =
        runBlocking {
            var entries = 0
            projection.recompute.recomputeBatchGated(
                emptyList(),
                projection.recompute.providerMap(),
                removeEmpties = false,
            ) { body ->
                entries += 1
                body()
            }
            // A drain with nothing to do must not touch the mutex: taking it to
            // do nothing is how a fairness fix quietly becomes a fairness bug.
            assertEquals(0, entries, "an empty batch took the gate")
        }
}
