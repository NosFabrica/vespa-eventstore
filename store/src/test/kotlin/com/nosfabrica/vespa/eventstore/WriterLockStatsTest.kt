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

import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * The writer lock's wait/hold accounting.
 *
 * This exists because the absence of it cost a night of wrong conclusions. The
 * status line timed every stage from AFTER the lock was held, so a writer
 * starved by another holder rendered as cheap stages beside a stalled
 * pipeline, and the only stage that looked expensive — `proj.fetch` — was
 * being read as ingest's critical path when it is a PROCESS-WIDE counter
 * booked by a background drain that merely shares the lock.
 *
 * So the property under test is not "the numbers exist". It is that a stall
 * caused by contention is ATTRIBUTABLE from the status line alone: the waiter
 * shows the wait, the holder shows the hold, and the two are separable.
 */
class WriterLockStatsTest {
    private val alice = "a".repeat(64)

    private fun card(id: String) = MetadataEvent(id, alice, 1, emptyArray(), """{"name":"n"}""", "")

    /**
     * Seconds booked to [stage] in [line].
     *
     * Takes the line rather than calling [IngestStats.statusLine] itself: that
     * call CONSUMES the delta for every stage at once, so asking it twice
     * reads the second stage as zero. One snapshot, parsed repeatedly.
     */
    private fun seconds(
        line: String,
        stage: String,
    ): Double =
        Regex("""\b${Regex.escape(stage)} ([0-9.]+)s""")
            .find(line)
            ?.groupValues
            ?.get(1)
            ?.toDouble() ?: 0.0

    @Test
    fun `a slow gate holder is charged to hold, and the blocked insert to wait`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            // Consume whatever earlier tests in this JVM booked: IngestStats is
            // process-wide by design, so only the DELTA is meaningful.
            IngestStats.statusLine()

            coroutineScope {
                val holding =
                    async {
                        store.withWriteLock {
                            // Long enough to dwarf the 50ms floor statusLine()
                            // applies, so neither figure is filtered away.
                            delay(400)
                        }
                    }
                // Let the holder actually take the lock before contending for
                // it; without this the insert can win the race and measure
                // nothing, which would make the test pass for the wrong reason.
                yield()
                delay(50)
                launch { store.insert(card("b".repeat(64))) }
                holding.await()
            }

            val line = IngestStats.statusLine()
            val gateHold = seconds(line, "lock.gate.hold")
            val ingestWait = seconds(line, "lock.ingest.wait")

            assertTrue(gateHold >= 0.3, "the gate's hold must be charged to the gate, got ${gateHold}s in `$line`")
            assertTrue(
                ingestWait >= 0.2,
                "the insert blocked behind it must be charged as ingest WAIT, not hidden inside a fast-looking stage; got ${ingestWait}s in `$line`",
            )
        }

    @Test
    fun `an uncontended insert books no meaningful wait`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            IngestStats.statusLine()

            store.insert(card("c".repeat(64)))

            // Below statusLine()'s own 50ms reporting floor, so an uncontended
            // writer never invents contention — the number has to stay
            // trustworthy when nothing is wrong, or nobody will believe it
            // when something is.
            assertTrue(
                seconds(IngestStats.statusLine(), "lock.ingest.wait") == 0.0,
                "an uncontended acquisition must not surface as wait time",
            )
        }
}
