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
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
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
import java.io.File
import java.time.Duration
import kotlin.test.assertTrue

/**
 * THE TELEMETRY AGAINST A REAL CORPUS, under a real trust lens.
 *
 * `TelemetryIT` proves the wiring on forty synthetic events. This one answers
 * the question an operator actually asks — *where do my resources go* — using
 * events pulled from the live staging relay (`benchmark/capture_staging.py`),
 * fed into a local Vespa. The corpus carries what synthetic data cannot:
 *
 *  - **a working trust lens.** The observer's kind 10040 and its provider's
 *    kind-30382 score cards ride along, so `TrustProjection` builds real
 *    reputation tensors and a ranked search takes the gated profiles rather
 *    than falling back to plain text.
 *  - **real text.** Hundreds of distinct authors and hundreds of characters of
 *    content per event, which is what makes the write path's derivation cost
 *    (`SearchExtractors`, measured at 9,527 ns/event) real rather than nominal.
 *  - **real dirt.** Staging holds notes dated in the year 2100; captured as-is,
 *    because that raw shape is the point (CLAUDE.md).
 *
 * READ-ONLY AGAINST STAGING, and this test never touches it: the capture is a
 * separate offline step and this feeds a throwaway container. Staging deploys
 * on its own cadence and its schema may lag this repo, which is exactly why CI
 * stays hermetic.
 *
 * Needs `STAGING_CORPUS` pointing at the captured JSON array; skips cleanly
 * without it, so it never gates CI.
 */
@Tag("integration")
class StagingCorpusIT {
    @Test
    fun `where the resources go, on real events under a real lens`() {
        assumeTrue(dockerAvailable(), "Docker not available")
        val path = System.getenv("STAGING_CORPUS")
        assumeTrue(path != null && File(path).isFile, "set STAGING_CORPUS to a capture_staging.py export")

        val corpus = File(path!!).readText().let { parseArray(it) }
        println("corpus: ${corpus.size} events, ${corpus.map { it.pubKey }.toSet().size} authors")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"

                VespaEventStore
                    .open(
                        queryUrl,
                        configUrl = configUrl,
                        // Turn the slow-query ring ON, which is the only way to
                        // see it work: it is off unless an operator asks,
                        // because it is the one place a query string is kept.
                        slowQueryThresholdMillis = 250,
                    ).use { store ->
                        runBlocking {
                            // ---- ingest, in the shape a mirror uses ----
                            val t0 = System.nanoTime()
                            corpus.chunked(500).forEach { store.batchInsert(it) }
                            val ingestMs = (System.nanoTime() - t0) / 1_000_000
                            // The lens is only live once the projection settles.
                            store.awaitTrustProjection()
                            awaitVisible(store, corpus.size / 2)
                            println("ingested in $ingestMs ms")

                            // ---- the read mix a relay actually serves ----
                            for (term in TERMS) {
                                // Ranked, through the observer's web of trust —
                                // the most expensive thing this store serves.
                                store.query<Event>(Filter(kinds = listOf(1), search = "$term observer:$OBSERVER", limit = 50))
                                // The same term without the lens: pure text.
                                store.query<Event>(Filter(kinds = listOf(1), search = term, limit = 50))
                                // sort:recent — same filter, recency order.
                                store.query<Event>(Filter(kinds = listOf(1), search = "$term sort:recent observer:$OBSERVER", limit = 50))
                            }
                            // Plain NIP-01 paging, the mirror's shape.
                            repeat(5) { store.query<Event>(Filter(kinds = listOf(1), limit = 500)) }
                            // The raw path a relay serves clients from.
                            var served = 0
                            repeat(3) { store.rawQuery(listOf(Filter(kinds = listOf(1), limit = 200))) { served++ } }
                            // COUNT, and a sync snapshot walk.
                            store.count(Filter(kinds = listOf(1)))
                            store.count(Filter(kinds = listOf(0)))
                            store.snapshotIdsForNegentropy(listOf(Filter(kinds = listOf(1))), null, null)
                            // Re-offer the whole corpus: every event a duplicate,
                            // which is what a mirror spends its life doing.
                            corpus.chunked(500).forEach { store.batchInsert(it) }

                            val snap = store.metrics()
                            report(snap, corpus.size, served)

                            // ---- what this proves ----
                            assertTrue(snap.engine.any { it.engineNanos > 0 }, "no engine time recorded")
                            assertTrue(
                                snap.ports.none { it.activity == Activity.Other },
                                "unattributed work: ${snap.ports.filter { it.activity == Activity.Other }.map { it.call }}",
                            )
                            val dupes =
                                snap.outcomes[Activity.BatchInsert]
                                    ?.filterKeys { it != CostLedger.ADMITTED }
                                    ?.values
                                    ?.sum() ?: 0L
                            assertTrue(dupes > 0, "re-offering the corpus should have produced rejections")
                            assertTrue(IngestStats.allHeld().isEmpty(), "a lock leaked past the end of the run")
                        }
                    }
            }
    }

    private fun report(
        snap: CostLedger.Snapshot,
        corpusSize: Int,
        served: Int,
    ) {
        println("\n================ WHERE THE RESOURCES WENT ================")
        val engineTotal = snap.engine.sumOf { it.engineNanos }
        println("\n--- engine time by rank profile (total %.2f s) ---".format(engineTotal / 1e9))
        snap.engine.sortedByDescending { it.engineNanos }.forEach {
            println(
                "  %-22s %4d q  %7.2f s  %5.1f%%  match/q %8d  hits %5d  summary %6.0f ms  degraded %d".format(
                    it.profile,
                    it.queries,
                    it.engineNanos / 1e9,
                    if (engineTotal > 0) it.engineNanos * 100.0 / engineTotal else 0.0,
                    if (it.queries > 0) it.docsMatched / it.queries else 0,
                    it.hitsServed,
                    it.summaryNanos / 1e6,
                    it.degraded,
                ),
            )
        }

        println("\n--- port calls by activity (what actually reached Vespa) ---")
        snap.ports.sortedByDescending { it.nanos }.forEach {
            println(
                "  %-12s %-7s %5d calls %8.2f s  %7d docs  calls/doc %6.3f  p50 %-10s p99 %-10s".format(
                    it.activity,
                    it.call,
                    it.calls,
                    it.nanos / 1e9,
                    it.docs,
                    it.callsPerDoc,
                    ms(it.p50Nanos),
                    ms(it.p99Nanos),
                ),
            )
        }

        println("\n--- admission outcomes (offered ${snap.offered}, admitted ${snap.admitted}) ---")
        snap.outcomes.forEach { (a, row) ->
            val total = row.values.sum()
            println("  $a: " + row.entries.sortedByDescending { it.value }.joinToString(", ") { "${it.key}=${it.value}" } + "  (of $total)")
        }
        if (snap.offered > 0) {
            println("  already stored: %.1f%% of what this node was offered".format((snap.offered - snap.admitted) * 100.0 / snap.offered))
        }

        println("\n--- ingest stages (the write path's own split) ---")
        IngestStats.snapshot().entries.sortedByDescending { it.value.totalNanos }.take(12).forEach { (name, st) ->
            println(
                "  %-24s %7.2f s  %6d calls  mean %7.1f ms  max %7.1f ms".format(
                    name,
                    st.totalNanos / 1e9,
                    st.calls,
                    st.meanNanos / 1e6,
                    st.maxNanos / 1e6,
                ),
            )
        }

        val blocked = IngestStats.blockedSplit()
        if (blocked.isNotEmpty()) {
            println("\n--- lock wait, by what was holding (the causal edge) ---")
            blocked.forEach { (stage, split) ->
                val tot = split.values.sum()
                println("  $stage  %.2f s total".format(tot / 1e9))
                split.entries.sortedByDescending { it.value }.take(5).forEach { (holder, ns) ->
                    println("      %6.2f s (%4.1f%%) behind %s".format(ns / 1e9, ns * 100.0 / tot, holder))
                }
            }
        }

        println("\n--- gauges --- ${snap.gauges}")

        if (snap.slowReads.isNotEmpty()) {
            println("\n--- slow reads (threshold 250 ms) ---")
            snap.slowReads.take(8).forEach {
                println(
                    "  %-16s %7.2f s wall  %7.2f s engine  hits %4d  matched %8d  %s".format(
                        it.profile,
                        it.wallNanos / 1e9,
                        it.engineNanos / 1e9,
                        it.hits,
                        it.docsMatched,
                        it.detail,
                    ),
                )
            }
        }
        println("\n  (corpus $corpusSize events, rawQuery served $served)")
        println("==========================================================\n")
    }

    private fun ms(nanos: Long?): String = if (nanos == null) "—" else "%.1f ms".format(nanos / 1e6)

    /** Split a JSON array of events without pulling in a parser the module does not already have. */
    private fun parseArray(text: String): List<Event> {
        val out = ArrayList<Event>()
        var depth = 0
        var start = -1
        var inString = false
        var escaped = false
        for (i in text.indices) {
            val c = text[i]
            if (inString) {
                when {
                    escaped -> escaped = false
                    c == '\\' -> escaped = true
                    c == '"' -> inString = false
                }
                continue
            }
            when (c) {
                '"' -> {
                    inString = true
                }

                '{' -> {
                    if (depth == 0) start = i
                    depth++
                }

                '}' -> {
                    depth--
                    if (depth == 0 && start >= 0) {
                        runCatching { Event.fromJson(text.substring(start, i + 1)) }.getOrNull()?.let(out::add)
                        start = -1
                    }
                }
            }
        }
        return out
    }

    private suspend fun awaitVisible(
        store: VespaEventStore,
        atLeast: Int,
    ) {
        repeat(120) {
            if (store.count(Filter()) >= atLeast) return
            delay(500)
        }
    }

    companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** The canonical staging observer — a PUBLIC key, so the lens is simulated rather than authenticated. */
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

        val TERMS = listOf("bitcoin", "nostr", "lightning", "zap", "relay")

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
