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
package com.nosfabrica.vespa.eventstore.benchmark

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The telemetry facts that ONLY a real Vespa can settle.
 *
 * `TelemetryTest` covers everything engine-agnostic against the in-memory
 * reference — attribution, outcomes, the round-trip contract — precisely
 * because metering at the port should not need an engine. What is left here is
 * the part docs/telemetry.md §10.4 flagged as unverified and owed a test:
 *
 *  - **`presentation.timing` really returns the field names the DTO expects.**
 *    `VespaTiming` is a lenient `@Serializable`, so a renamed or absent field
 *    deserializes to 0.0 in complete silence, and every engine-time figure on
 *    an operator page becomes a confident zero. The mock cannot catch this:
 *    it returns whatever shape the test author imagined.
 *  - **the rank profile a query was priced by** reaches the ledger, and
 *    `docsMatched` really is the engine's `totalCount`.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class TelemetryIT {
    @Test
    fun `Vespa reports its own timing, and the ledger records what the engine did`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the telemetry IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"

                // ---------------------------------------------------------------
                // 1. The raw contract, before any of our own parsing can hide it.
                // ---------------------------------------------------------------
                VespaEventStore.open(queryUrl, configUrl = configUrl, autoDeploy = true).use { store ->
                    runBlocking {
                        store.batchInsert(corpus())
                        awaitVisible(store, CORPUS_SIZE)

                        val raw = rawTimingBlock(queryUrl)
                        assertTrue(
                            raw.contains("\"timing\""),
                            "Vespa returned no timing block for presentation.timing=true. Response was:\n$raw",
                        )
                        // The three names VespaTiming declares. If Vespa ever
                        // renames one, the DTO silently reads 0.0 and the whole
                        // engine-time column becomes a lie — so assert on the
                        // wire text, not on our own deserialization.
                        for (field in listOf("querytime", "summaryfetchtime", "searchtime")) {
                            assertTrue(
                                raw.contains("\"$field\""),
                                "Vespa's timing block no longer carries `$field`; VespaTiming needs updating. Got:\n$raw",
                            )
                        }

                        // ---------------------------------------------------------
                        // 2. The ledger, through the whole assembled stack.
                        // ---------------------------------------------------------
                        store.query<Event>(Filter(kinds = listOf(1), limit = 10))
                        store.query<Event>(Filter(kinds = listOf(0), search = "pamplona", limit = 10))
                        store.count(Filter(kinds = listOf(1)))

                        val snap = store.metrics()

                        // Engine altitude: a profile, real engine time, and the
                        // match-set size the client cannot compute itself.
                        assertTrue(snap.engine.isNotEmpty(), "no engine queries were booked at all")
                        val timed = snap.engine.filter { it.engineNanos > 0 }
                        assertTrue(
                            timed.isNotEmpty(),
                            "every engine query reported 0 ns — presentation.timing is not reaching the ledger. " +
                                "Profiles seen: ${snap.engine.map { "${it.profile}=${it.queries}q" }}",
                        )
                        assertTrue(
                            snap.engine.any { it.docsMatched > 0 },
                            "docsMatched never populated; totalCount is not reaching the ledger",
                        )
                        assertTrue(
                            snap.engine.sumOf { it.hitsServed } > 0,
                            "hitsServed never populated",
                        )

                        // Port altitude: the reads and writes are attributed to
                        // the activity that caused them, over a REAL engine.
                        val reads = snap.ports.filter { it.activity == Activity.Query }
                        assertTrue(reads.isNotEmpty(), "no port calls booked under Query")
                        assertTrue(
                            reads.any { (it.p99Nanos ?: 0L) > 0 },
                            "a search over a network must land in a latency bucket above zero",
                        )
                        // Distributions ride the read shapes only, and the
                        // snapshot must SAY so rather than reporting an
                        // unmeasured call as instant.
                        assertTrue(
                            writesOf(snap).all { it.latency == null },
                            "a write shape keeps no histogram, so its percentiles must be null rather than 0",
                        )
                        val writes = snap.ports.filter { it.activity == Activity.BatchInsert }
                        assertTrue(writes.isNotEmpty(), "no port calls booked under BatchInsert")

                        // Outcomes altitude: what the store admitted.
                        assertEquals(
                            CORPUS_SIZE.toLong(),
                            snap.outcomes[Activity.BatchInsert]?.get("admitted"),
                            "every event in a fresh corpus should be admitted",
                        )

                        // Gauges: pulled live from their owners.
                        assertTrue("feed.inflight" in snap.gauges, "the feed gauge was not registered")
                        assertTrue("trust.pending.subjects" in snap.gauges, "the trust backlog gauge was not registered")

                        // The write path's stage split still works alongside all
                        // of this, and its holds balanced out.
                        assertTrue(IngestStats.snapshot().isNotEmpty(), "the ingest stage split went missing")
                        assertTrue(
                            IngestStats.heldAll().isEmpty(),
                            "a lock is still held after every operation finished: ${IngestStats.heldAll().map { IngestStats.labelOf(it) }}",
                        )

                        report(snap)
                    }
                }

                // ---------------------------------------------------------------
                // 3. The two sections an operator opts into, which have their own
                //    producers and had NONE until a full run against a real relay
                //    found them empty. A second store on the same engine, because
                //    the slow-read ring is a constructor setting.
                // ---------------------------------------------------------------
                VespaEventStore.open(queryUrl, configUrl = configUrl, autoDeploy = false, slowQueryThresholdMillis = 1).use { store ->
                    runBlocking {
                        store.query<Event>(Filter(kinds = listOf(0), search = "pamplona observer:$observer", limit = 10))
                        store.query<Event>(Filter(kinds = listOf(1), search = "nostr observer:$observer", limit = 10))

                        val snap = store.metrics()

                        assertTrue(
                            snap.topObservers.any { it.key == observer },
                            "the lens that asked for the work is not in the sketch: ${snap.topObservers.map { it.key }}",
                        )
                        assertTrue(
                            snap.topTerms.map { it.key }.containsAll(listOf("pamplona", "nostr")),
                            "the terms that drove the reads are not in the sketch: ${snap.topTerms.map { it.key }}",
                        )
                        // A network round trip against a container is never under a
                        // millisecond, so the ring must have caught something. The
                        // ring is the one place a query string is retained, and
                        // only because this store was opened with a threshold.
                        assertTrue(snap.slowReads.isNotEmpty(), "nothing reached the slow-read ring at a 1ms threshold")
                        val slow = snap.slowReads.first()
                        assertTrue(slow.wallNanos > 0 && slow.detail.isNotEmpty(), "a captured slow read must carry its wall time and its query")
                        assertTrue(slow.profile.isNotEmpty(), "and the rank profile that priced it")

                        println("=== what drove the load ===")
                        snap.topObservers.forEach { println("  observer %-8s weight %5d ±%d".format(it.key.take(8), it.weight, it.error)) }
                        snap.topTerms.forEach { println("  term     %-8s weight %5d ±%d".format(it.key, it.weight, it.error)) }
                        println("=== slow reads (%d) ===".format(snap.slowReads.size))
                        snap.slowReads.take(5).forEach {
                            println(
                                "  %-8s %-16s wall %7.2f ms  engine %7.2f ms  matched %5d  hits %3d".format(
                                    it.activity,
                                    it.profile,
                                    it.wallNanos / 1e6,
                                    it.engineNanos / 1e6,
                                    it.docsMatched,
                                    it.hits,
                                ),
                            )
                        }
                        println()
                    }
                }
            }
    }

    /** The canonical observer of this repo's fixtures; a PUBLIC key, no secret ships with it. */
    private val observer = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

    private fun writesOf(snap: com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger.Snapshot) = snap.ports.filter { it.call == com.nosfabrica.vespa.eventstore.engine.metrics.PortCall.Put }

    /** "not measured" and "instant" must not render the same. */
    private fun ms(nanos: Long?): String = if (nanos == null) "—" else "%.2f ms".format(nanos / 1e6)

    /** Print what the engine actually said — the point of an IT is the evidence, not just the pass. */
    private fun report(snap: com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger.Snapshot) {
        println("\n=== engine, by rank profile ===")
        snap.engine.sortedByDescending { it.engineNanos }.forEach {
            println(
                "  %-22s %3d q  engine %7.2f ms  summary %7.2f ms  matched %6d  hits %4d  degraded %d".format(
                    it.profile,
                    it.queries,
                    it.engineNanos / 1e6,
                    it.summaryNanos / 1e6,
                    it.docsMatched,
                    it.hitsServed,
                    it.degraded,
                ),
            )
        }
        println("=== ports, by activity ===")
        snap.ports.sortedByDescending { it.nanos }.forEach {
            println(
                "  %-12s %-8s %4d calls  %8.2f ms  %5d docs  calls/doc %.3f  p50 %-9s p99 %-9s".format(
                    it.activity,
                    it.call,
                    it.calls,
                    it.nanos / 1e6,
                    it.docs,
                    it.callsPerDoc,
                    ms(it.p50Nanos),
                    ms(it.p99Nanos),
                ),
            )
        }
        println("=== outcomes ===  offered ${snap.offered}, admitted ${snap.admitted}")
        snap.outcomes.forEach { (a, row) -> println("  $a -> $row") }
        println("=== gauges ===   ${snap.gauges}")
        val blocked = IngestStats.blockedSplit()
        if (blocked.isNotEmpty()) {
            println("=== wait, by what was holding ===")
            blocked.forEach { (stage, split) ->
                split.entries.sortedByDescending { it.value }.forEach { (holder, ns) ->
                    println("  %-22s behind %-40s %8.2f ms".format(stage, holder, ns / 1e6))
                }
            }
        }
        println()
    }

    /** Ask Vespa directly, so the assertion is about VESPA's wire format rather than our decoding of it. */
    private fun rawTimingBlock(queryUrl: String): String {
        val body =
            """{"yql":"select * from event where true limit 5","hits":"5","ranking":"unranked","presentation.timing":"true"}"""
        val response =
            HttpClient.newHttpClient().send(
                HttpRequest
                    .newBuilder(URI.create("$queryUrl/search/"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        return response.body()
    }

    private suspend fun awaitVisible(
        store: VespaEventStore,
        expected: Int,
    ) {
        repeat(60) {
            if (store.count(Filter()) >= expected) return
            delay(500)
        }
    }

    private fun corpus(): List<Event> =
        buildList {
            // kind 0s carry searchable text, so a NIP-50 query has something to
            // rank and a ranked profile actually gets exercised.
            add(profile(1, """{"name":"vitor","about":"pamplona dev"}"""))
            add(profile(2, """{"name":"carol","about":"pamplona too"}"""))
            for (i in 3..CORPUS_SIZE) add(note(i))
        }

    private fun profile(
        n: Int,
        content: String,
    ) = Event.fromJson(
        """{"id":"${hex(n)}","pubkey":"${hex(100 + n)}","created_at":${AT + n},"kind":0,"tags":[],"content":"${content.replace("\"", "\\\"")}","sig":"${hex(n) + hex(n)}"}""",
    )

    private fun note(n: Int) =
        Event.fromJson(
            """{"id":"${hex(n)}","pubkey":"${hex(100 + n)}","created_at":${AT + n},"kind":1,"tags":[],"content":"note number $n","sig":"${hex(n) + hex(n)}"}""",
        )

    private fun hex(n: Int) = n.toString(16).padStart(64, '0')

    companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val CORPUS_SIZE = 40
        const val AT = 1_800_000_000L

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
