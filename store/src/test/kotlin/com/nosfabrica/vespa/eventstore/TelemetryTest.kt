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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.MeteredEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.PortCall
import com.nosfabrica.vespa.eventstore.engine.metrics.currentActivity
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The telemetry stack over the in-memory reference: what it counts, what it
 * attributes, and the performance contract it makes executable.
 *
 * Deliberately NOT against Vespa. The whole argument for metering at the port
 * (docs/telemetry.md §3.1) is that the seam is engine-agnostic, so if these
 * need a real engine the placement is wrong. `TelemetryIT` covers the part that
 * genuinely cannot live here — Vespa's own timing block and rank profiles.
 */
class TelemetryTest {
    private val alice = "a".repeat(64)
    private val bob = "b".repeat(64)

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(
        author: String = alice,
        at: Long = 1_700_000_000L + seq,
        content: String = "hello world",
        eventId: String = id(),
    ) = Event(eventId, author, at, 1, emptyArray(), content, "")

    private class Rig {
        val ledger = CostLedger()
        val engine: EventIndex = InMemoryEventIndex()
        val index = MeteredEventIndex(ledger, engine)
        val store = NostrSemanticsStore(index, metrics = ledger)
    }

    // ------------------------------------------------------------ attribution

    @Test
    fun `port calls are attributed to the activity that caused them`() =
        runBlocking {
            val rig = Rig()
            rig.store.batchInsert(List(20) { note() })
            rig.store.query<Event>(Filter(kinds = listOf(1)))

            val snap = rig.ledger.snapshot()
            val writes = snap.ports.filter { it.activity == Activity.BatchInsert }
            val reads = snap.ports.filter { it.activity == Activity.Query }

            assertTrue(writes.isNotEmpty(), "a batch insert must book port calls under BatchInsert")
            assertTrue(reads.isNotEmpty(), "a REQ must book port calls under Query")
            assertTrue(
                reads.all { it.call == PortCall.Search },
                "a plain REQ only searches; got ${reads.map { it.call }}",
            )
            // The property the whole design turns on: the SAME port call
            // (search) lands in different cells depending on who asked. That is
            // what the dotted-name convention could never express.
            assertTrue(
                writes.any { it.call == PortCall.Search || it.call == PortCall.Exists },
                "the write path's own reads are booked to the writer, not to a reader",
            )
        }

    @Test
    fun `nothing is attributed to Other once entry points declare themselves`() =
        runBlocking {
            // EVERY public entry point, not a representative three. The first
            // version of this test exercised batchInsert/query/count, passed,
            // and missed that `rawQuery` — the path a relay actually serves
            // clients from — booked to Other, along with the negentropy
            // snapshot, the tag walk and the reindex. `Activity.Snapshot` was
            // declared and never used, which was the tell.
            val rig = Rig()
            rig.store.batchInsert(List(5) { note() })
            rig.store.insert(note())
            rig.store.transaction { insert(note()) }
            rig.store.query<Event>(Filter(kinds = listOf(1)))
            rig.store.rawQuery(listOf(Filter(kinds = listOf(1)))) { }
            rig.store.count(Filter(kinds = listOf(1)))
            rig.store.snapshotIdsForNegentropy(listOf(Filter(kinds = listOf(1))), null, null)
            rig.store.distinctTagValues(Filter(kinds = listOf(1)), "p")
            rig.store.deleteExpiredEvents()
            rig.store.refreshGuardOwners()
            rig.store.reindexFullTextSearch()

            val stray =
                rig.ledger
                    .snapshot()
                    .ports
                    .filter { it.activity == Activity.Other }
            assertTrue(stray.isEmpty(), "an entry point forgot withActivity: ${stray.map { "${it.call}(${it.calls})" }}")
        }

    @Test
    fun `every declared Activity is reachable from some entry point`() =
        runBlocking {
            // A declared-but-unused Activity means an entry point was missed —
            // that is precisely how the rawQuery/Snapshot gap announced itself.
            // Only the values a bare store can reach are checked here; the
            // background workers (Drain, Reconcile, Sweep, Backfill) belong to
            // VespaEventStore and are covered where they are wired.
            val rig = Rig()
            rig.store.insert(note())
            rig.store.batchInsert(List(20) { note() })
            rig.store.query<Event>(Filter(kinds = listOf(1)))
            rig.store.count(Filter(kinds = listOf(1)))
            rig.store.snapshotIdsForNegentropy(listOf(Filter(kinds = listOf(1))), null, null)
            rig.store.deleteExpiredEvents()
            rig.store.refreshGuardOwners()

            val seen =
                rig.ledger
                    .snapshot()
                    .ports
                    .map { it.activity }
                    .toSet()
            for (expected in listOf(Activity.Insert, Activity.BatchInsert, Activity.Query, Activity.Count, Activity.Snapshot)) {
                assertTrue(expected in seen, "$expected is declared but no entry point books port calls under it")
            }
        }

    @Test
    fun `re-declaring the ambient activity is a no-op, and an inner one still wins`() =
        runBlocking {
            // withActivity short-circuits when the activity is already ambient
            // (measured 933 -> 40 ns), so the semantics of that fast path have
            // to be identical to installing it again.
            withActivity(Activity.Query) {
                assertEquals(Activity.Query, currentActivity())
                withActivity(Activity.Query) {
                    assertEquals(Activity.Query, currentActivity(), "re-declaring the same activity must change nothing")
                }
                withActivity(Activity.Drain) {
                    assertEquals(Activity.Drain, currentActivity(), "an inner declaration wins for its own extent")
                }
                assertEquals(Activity.Query, currentActivity(), "and the outer one is restored after it")
            }
            assertEquals(Activity.Other, currentActivity(), "outside any declaration, work is Other")
        }

    // -------------------------------------------------------------- outcomes

    @Test
    fun `admission outcomes are tallied where the decision is made`() =
        runBlocking {
            val rig = Rig()
            val batch = List(10) { note() }
            rig.store.batchInsert(batch)
            // Offer the same events again: every one is a duplicate, which is
            // the state a mirroring relay spends most of its life in.
            rig.store.batchInsert(batch)

            val outcomes = assertNotNull(rig.ledger.snapshot().outcomes[Activity.BatchInsert])
            assertEquals(10L, outcomes[CostLedger.ADMITTED], "ten new events were stored")
            assertEquals(10L, outcomes[Rejections.DUPLICATE], "ten repeats were refused as duplicates")
        }

    @Test
    fun `the offered-versus-admitted ratio is recoverable`() =
        runBlocking {
            val rig = Rig()
            val batch = List(8) { note() }
            rig.store.batchInsert(batch)
            rig.store.batchInsert(batch)

            val snap = rig.ledger.snapshot()
            // "81% of what this node is offered is already stored" — the number
            // that tells an operator to narrow a sync, and the reason the
            // outcomes altitude exists at all.
            assertEquals(16L, snap.offered)
            assertEquals(8L, snap.admitted)
        }

    @Test
    fun `a rejection reason never becomes an unbounded key`() {
        // Reasons carry per-event detail ("duplicate: <id>"), so tallying the
        // raw message would key a counter by event id — the cardinality leak
        // §4 forbids. Everything must fold back onto the closed set.
        for (reason in Rejections.ALL) {
            assertEquals(reason, Rejections.reasonOf("$reason ${"f".repeat(64)}"))
        }
        assertEquals(Rejections.INSERT_FAILED, Rejections.reasonOf("something nobody predicted"))
        assertEquals(Rejections.INSERT_FAILED, Rejections.reasonOf(null))
    }

    // --------------------------------------------- the performance contract

    @Test
    fun `batchInsert amortizes round trips - the CLAUDE_md rule, executable`() =
        runBlocking {
            // "Never ingest in a loop over insert()" is a documented rule and a
            // benchmark memory. With round trips counted at the port it is a
            // gate: the same events, both ways, and the ratio has to hold.
            val n = 64

            val looped = Rig()
            repeat(n) { looped.store.insert(note()) }
            val loopedCalls = looped.ledger.snapshot().callsUnder(Activity.Insert)

            seq = 0
            val batched = Rig()
            batched.store.batchInsert(List(n) { note() })
            val batchedCalls = batched.ledger.snapshot().callsUnder(Activity.BatchInsert)

            assertTrue(
                batchedCalls < loopedCalls,
                "batching must cost fewer port calls than looping: batched $batchedCalls vs looped $loopedCalls",
            )
            val perEventLooped = loopedCalls.toDouble() / n
            val perEventBatched = batchedCalls.toDouble() / n
            assertTrue(
                perEventBatched <= perEventLooped / 2,
                "batching should at least halve calls per event: $perEventBatched vs $perEventLooped",
            )
        }

    @Test
    fun `callsPerDoc reads as the amortization number`() =
        runBlocking {
            val rig = Rig()
            rig.store.batchInsert(List(50) { note() })
            val put = assertNotNull(rig.ledger.snapshot().port(Activity.BatchInsert, PortCall.Put))
            assertEquals(50L, put.docs, "every document written is a denominator entry")
            assertTrue(put.callsPerDoc < 1.0, "a bulk put moves many documents per call, got ${put.callsPerDoc}")
        }

    // ------------------------------------------------------- transparency

    @Test
    fun `metering changes no answer`() =
        runBlocking {
            // A store that behaves differently because it is being measured is
            // worse than one that is not measured.
            val plain = InMemoryEventIndex()
            val bare = NostrSemanticsStore(plain)
            val rig = Rig()

            val events = List(30) { note(author = if (it % 2 == 0) alice else bob, content = "note $it") }
            seq = 0
            val bareOutcomes = bare.batchInsert(events)
            seq = 0
            val meteredOutcomes = rig.store.batchInsert(List(30) { note(author = if (it % 2 == 0) alice else bob, content = "note $it") })
            assertEquals(bareOutcomes.size, meteredOutcomes.size)

            val bareRead = bare.query<Event>(Filter(kinds = listOf(1))).map { it.id }
            val meteredRead = rig.store.query<Event>(Filter(kinds = listOf(1))).map { it.id }
            assertEquals(bareRead, meteredRead, "the metered store must serve the identical page")
        }

    @Test
    fun `the decorator forwards the port's must-delegate defaults`() =
        runBlocking {
            // The port's KDoc names several defaults a decorator MUST forward
            // or it silently downgrades the engine (the summary-free existence
            // probe, the server-side grouping, the streaming projections).
            // Counting them is fine; riding the default is not.
            val rig = Rig()
            rig.store.batchInsert(List(12) { note(author = if (it % 3 == 0) bob else alice) })

            val engineAuthors =
                rig.engine.scanAuthors(
                    com.nosfabrica.vespa.eventstore.engine.query
                        .EventQuery(),
                )
            val throughDecorator =
                rig.index.scanAuthors(
                    com.nosfabrica.vespa.eventstore.engine.query
                        .EventQuery(),
                )
            assertEquals(engineAuthors, throughDecorator)

            val ids = List(6) { (it + 1).toString(16).padStart(64, '0') }
            assertEquals(rig.engine.existingIds(ids), rig.index.existingIds(ids))
        }

    // ------------------------------------------------------------- gauges

    @Test
    fun `a gauge is pulled at snapshot time and a broken one cannot break the snapshot`() {
        val ledger = CostLedger()
        var reading = 7L
        ledger.gauge("queue.depth") { reading }
        ledger.gauge("explodes") { error("this supplier is broken") }

        assertEquals(7L, ledger.snapshot().gauges["queue.depth"])
        reading = 12L
        val snap = ledger.snapshot()
        assertEquals(12L, snap.gauges["queue.depth"], "a gauge reads live, it does not accumulate")
        assertTrue("explodes" !in snap.gauges, "a broken supplier is dropped, not propagated")
        assertEquals(1, snap.gauges.size, "observability that can crash its caller is worse than none")
    }

    // -------------------------------------------------------- slow queries

    @Test
    fun `the slow-query ring is off by default and bounded when on`() {
        assertTrue(
            CostLedger().let { l ->
                l.slowRead(Activity.Query, "search", 9_000_000_000, 0, 0, 1, 1, "nostr")
                l.snapshot().slowReads.isEmpty()
            },
            "retaining query strings must be an explicit choice",
        )

        val ledger = CostLedger(slowQueryThresholdNanos = 1_000_000, slowQueryRing = 4)
        repeat(20) { ledger.slowRead(Activity.Query, "search", 5_000_000, 0, 0, 1, 1, "term $it") }
        ledger.slowRead(Activity.Query, "search", 100, 0, 0, 1, 1, "fast one")
        val slow = ledger.snapshot().slowReads
        assertEquals(4, slow.size, "bounded by the ring, never by how many distinct queries exist")
        assertTrue(slow.none { it.detail == "fast one" }, "a read under the threshold is not captured")
    }
}
