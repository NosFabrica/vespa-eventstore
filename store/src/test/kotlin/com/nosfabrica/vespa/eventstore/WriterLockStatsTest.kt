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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlin.system.measureTimeMillis
import kotlin.test.Test
import kotlin.test.assertEquals
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
@Suppress("DEPRECATION") // statusLine() is destructive BY DESIGN here: this suite is its only consumer in the JVM and asserts on the per-call delta, which is exactly what the deprecation warns two consumers not to share.
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
    private fun contactCard(
        id: String,
        about: String = "c".repeat(64),
    ) = ContactCardEvent(id, alice, 1, arrayOf(arrayOf("d", about), arrayOf("rank", "50")), "", "")

    /** A kind-1 with its own id — not replaceable, so a batch of them all survive. */
    private fun textNote(id: String) = Event(id, alice, 1, 1, emptyArray(), "hello", "")

    private fun hex(n: Int) = n.toString(16).padStart(64, '0')

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

    /**
     * THE LOCK ORDER, asserted from the plain writer's side.
     *
     * With writes-then-gate, a card took [writes] and then waited for the
     * trust gate WHILE HOLDING IT — so every kind-1 behind the card waited out
     * the drain slice the card was waiting for, and the split bought a plain
     * writer nothing whenever a card happened to be queued. Gate-then-writes
     * means the queued card holds nothing while it waits, and this metadata
     * insert must go straight through.
     */
    @Test
    fun `a plain insert is not blocked by a card queued behind the trust gate`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            IngestStats.statusLine()

            coroutineScope {
                val holding = async { store.withWriteLock { delay(400) } }
                yield()
                delay(50)
                // The card queues for the gate the holder has.
                val card = launch { store.insert(contactCard("b".repeat(64))) }
                yield()
                delay(50)
                val tookMs = measureTimeMillis { store.insert(metadata("d".repeat(64))) }
                assertTrue(tookMs < 200, "a kind-0 waited behind a card that was itself waiting for the drain: ${tookMs}ms")
                holding.await()
                card.join()
            }
        }

    /**
     * THE BATCH SPLIT. A pure-record batch's plain events commit under
     * [writes] alone while its cards wait for the gate — so with the gate held
     * for the whole test, the notes are readable long before the holder lets
     * go, and the cards only after.
     */
    @Test
    fun `a batch's plain events commit while its cards wait for the trust gate`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            IngestStats.statusLine()
            // Past BULK_MIN, so this is the bulk path and not the loop.
            val notes = (1..20).map { textNote(hex(it)) }
            val cards = listOf(contactCard(hex(100), "a".repeat(64)), contactCard(hex(101), "b".repeat(64)))
            val batch = notes.take(10) + cards.take(1) + notes.drop(10) + cards.drop(1)

            coroutineScope {
                val holding = async { store.withWriteLock { delay(400) } }
                yield()
                delay(50)
                val inserting = async { store.batchInsert(batch) }
                yield()
                delay(100)
                // The gate is still held: notes in, cards not.
                assertEquals(20, store.query<Event>(Filter(kinds = listOf(1))).size, "the notes committed while the cards waited")
                assertEquals(0, store.query<Event>(Filter(kinds = listOf(ContactCardEvent.KIND))).size, "the cards are still queued for the gate")
                holding.await()
                val outcomes = inserting.await()
                assertEquals(2, store.query<Event>(Filter(kinds = listOf(ContactCardEvent.KIND))).size, "the cards committed once the gate was free")
                assertTrue(outcomes.all { it == IEventStore.InsertOutcome.Accepted }, "every event was accepted: $outcomes")
            }
        }

    /** The split merges outcomes back BY POSITION: a rejection lands on the event that earned it, in either half. */
    @Test
    fun `a split batch reports outcomes in the caller's order`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            val dupNote = textNote(hex(7))
            val dupCard = contactCard(hex(200), "d".repeat(64))
            store.insert(dupNote)
            store.insert(dupCard)

            val batch =
                (1..6).map { textNote(hex(it)) } +
                    dupNote + // index 6: rejected as a duplicate, in the plain half
                    contactCard(hex(201), "e".repeat(64)) + // index 7: accepted, in the trust half
                    (8..20).map { textNote(hex(it)) } +
                    dupCard // index 21: rejected as a duplicate, in the trust half
            val outcomes = store.batchInsert(batch)

            assertEquals(batch.size, outcomes.size)
            val rejectedAt = outcomes.indices.filter { outcomes[it] != IEventStore.InsertOutcome.Accepted }
            assertEquals(listOf(6, 21), rejectedAt, "rejections sit at the positions of the events that earned them: $outcomes")
            assertEquals(20, store.query<Event>(Filter(kinds = listOf(1))).size, "the duplicate note was already stored; every other note is new")
            assertEquals(2, store.query<Event>(Filter(kinds = listOf(ContactCardEvent.KIND))).size)
        }

    /**
     * ONE HOLDER SLOT PER LOCK. With a single slot, a plain insert's short
     * `writes` hold overwrote the drain's seconds-long gate hold and then, on
     * release, erased it — so "who holds the gate right now" answered
     * "nobody" for most of every drain slice.
     */
    @Test
    fun `a plain insert's hold does not erase the gate holder`() =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex())
            coroutineScope {
                val holding = async { store.withWriteLock { delay(300) } }
                yield()
                delay(50)
                store.insert(metadata("9".repeat(64)))
                val now = IngestStats.heldNow()
                assertTrue(now != null && now.stage == "lock.gate.hold", "the gate holder is still reported after a plain insert came and went: $now")
                holding.await()
            }
            assertTrue(IngestStats.heldNow() == null, "released: nothing held")
        }
}
