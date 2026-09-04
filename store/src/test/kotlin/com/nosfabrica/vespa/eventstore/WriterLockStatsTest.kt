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
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlin.system.measureTimeMillis
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

    /**
     * A kind-0. NOT trust-relevant — [NostrSemanticsStore.touchesTrust] books
     * no reputation work for it, so it does not queue for the trust gate. The
     * old name of this helper was `card`, which read as a 30382 and is exactly
     * the confusion the split made load-bearing.
     */
    private fun metadata(id: String) = MetadataEvent(id, alice, 1, emptyArray(), """{"name":"n"}""", "")

    /** A real contact card (30382): trust-relevant, so it DOES queue for the trust gate. */
    private fun contactCard(id: String) = ContactCardEvent(id, alice, 1, arrayOf(arrayOf("d", "c".repeat(64)), arrayOf("rank", "50")), "", "")

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
                launch { store.insert(contactCard("b".repeat(64))) }
                holding.await()
            }

            val line = IngestStats.statusLine()
            val gateHold = seconds(line, "lock.gate.hold")
            // A CARD is trust-relevant, so it queues for the trust gate — and
            // that wait is booked to `lock.ingest.trust.wait`, its own stage.
            // It was `lock.ingest.wait` while one mutex served both, and
            // keeping it there would merge an insert's wait for the drain with
            // the drain's own (see NostrSemanticsStore.trustGate).
            val trustWait = seconds(line, "lock.ingest.trust.wait")

            assertTrue(gateHold >= 0.3, "the gate's hold must be charged to the gate, got ${gateHold}s in `$line`")
            assertTrue(
                trustWait >= 0.2,
                "the trust-relevant insert blocked behind it must be charged as its own trust WAIT, not hidden inside a fast-looking stage; got ${trustWait}s in `$line`",
            )
        }

    /**
     * THE BUG THE SPLIT FIRST SHIPPED WITH, pinned so it cannot return.
     *
     * `insert` took both locks and `batchInsert` took neither, so the mirror's
     * bulk card ingest — which writes reputation cells inline through
     * `TrustProjection.putAll` — ran with no exclusion against the drain at
     * all. The single-lock design could not have this bug; a split has to earn
     * its exclusion at EVERY write entry point, and there are several.
     */
    @Test
    fun `a batch carrying a card waits for the trust gate`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            IngestStats.statusLine()

            coroutineScope {
                val holding = async { store.withWriteLock { delay(400) } }
                yield()
                delay(50)
                // A batch of ordinary events with ONE card in it: the card is
                // what makes the whole batch trust-relevant.
                launch {
                    store.batchInsert(
                        listOf(metadata("e".repeat(64)), contactCard("f".repeat(64))),
                    )
                }
                holding.await()
            }

            val line = IngestStats.statusLine()
            assertTrue(
                seconds(line, "lock.ingest.trust.wait") >= 0.2,
                "a batch containing a card must queue for the trust gate; got `$line`",
            )
        }

    /** …and a batch with nothing trust-relevant in it must NOT wait. */
    @Test
    fun `a batch with no trust events is not blocked by a trust gate holder`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            IngestStats.statusLine()

            coroutineScope {
                val holding = async { store.withWriteLock { delay(400) } }
                yield()
                delay(50)
                val tookMs =
                    measureTimeMillis {
                        store.batchInsert(listOf(metadata("a1".repeat(32)), metadata("a2".repeat(32))))
                    }
                assertTrue(tookMs < 200, "a batch of kind-0s waited on the trust gate: ${tookMs}ms")
                holding.await()
            }
        }

    /**
     * THE POINT OF THE TRUST-GATE SPLIT, asserted rather than assumed.
     *
     * A kind-0 (or a kind-1, a reaction, a repost — anything
     * [NostrSemanticsStore.touchesTrust] books no work for) must not wait on a
     * trust holder. Measured on staging before the split: an ephemeral event,
     * which takes the lock and immediately returns without storing anything,
     * answered OK in 35-41 SECONDS while the drain re-derived reputation
     * documents. One mutex served both, so every client write paid for trust
     * maintenance it had nothing to do with.
     */
    @Test
    fun `a non-trust insert is not blocked by a trust gate holder`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            IngestStats.statusLine()

            coroutineScope {
                val holding = async { store.withWriteLock { delay(400) } }
                yield()
                delay(50)
                val tookMs =
                    measureTimeMillis {
                        store.insert(metadata("d".repeat(64)))
                    }
                // Generous: the assertion is "did not wait out the 400ms
                // holder", not a latency budget. The in-memory index makes the
                // insert itself sub-millisecond.
                assertTrue(
                    tookMs < 200,
                    "a kind-0 waited on the trust gate it has no work for: ${tookMs}ms",
                )
                holding.await()
            }

            val line = IngestStats.statusLine()
            assertTrue(
                seconds(line, "lock.ingest.trust.wait") < 0.1,
                "a non-trust insert must book no trust wait at all, got `$line`",
            )
        }

    @Test
    fun `an uncontended insert books no meaningful wait`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            IngestStats.statusLine()

            store.insert(metadata("c".repeat(64)))

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
