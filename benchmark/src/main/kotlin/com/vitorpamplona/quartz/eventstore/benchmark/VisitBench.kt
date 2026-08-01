/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.system.measureNanoTime

/**
 * A/B of the full-corpus visit walk behind `snapshotIdsForNegentropy` against a
 * REAL Vespa: serial paged (the original), sliced paged, and sliced streamed
 * (JSON Lines) — the three transports of [VespaEventIndex.visitIds]. Feeds a
 * synthetic corpus once (idempotent: re-runs top up to BENCH_VISIT_DOCS, ids are
 * deterministic), then times a complete id walk per configuration, twice each,
 * and cross-checks that every configuration sees the same document count.
 *
 * Env: VESPA_URL (default http://localhost:8080), BENCH_VISIT_DOCS (default
 * 2_000_000), BENCH_VISIT_FEED_BATCH (default 2_000).
 */
object VisitBench {
    private data class Config(
        val label: String,
        val slices: Int,
        val concurrency: Int,
        val streaming: Boolean,
    )

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val target = System.getenv("BENCH_VISIT_DOCS")?.toIntOrNull() ?: 2_000_000
            val batch = System.getenv("BENCH_VISIT_FEED_BATCH")?.toIntOrNull() ?: 2_000

            println("visit A/B @ $url, corpus target $target docs")
            awaitServing(url)
            feedUpTo(url, target, batch)

            // The paged path is always ONE serial chain (slices measured 11x
            // slower there — roughly one small bucket per round trip), so the
            // paged configs vary only bucket concurrency. High total visitor
            // pressure (~64 sessions) wedged this node's document API
            // mid-response, indefinitely — hence the modest figures throughout
            // and the visit read deadline in VespaEventIndex.
            val configs =
                listOf(
                    Config("paged serial (original)", slices = 1, concurrency = 1, streaming = false),
                    Config("paged serial, conc=8", slices = 1, concurrency = 8, streaming = false),
                    Config("streamed 1 slice", slices = 1, concurrency = 1, streaming = true),
                    Config("streamed 4 slices", slices = 4, concurrency = 1, streaming = true),
                    Config("streamed 8 slices", slices = 8, concurrency = 1, streaming = true),
                    Config("streamed 16 slices", slices = 16, concurrency = 1, streaming = true),
                )

            println()
            println(String.format("%-28s %12s %12s %14s", "config", "run1", "run2", "docs/sec(best)"))
            val counts = HashMap<String, Long>()
            for (c in configs) {
                val index = VespaEventIndex(url, visitSlices = c.slices, visitConcurrency = c.concurrency, visitStreaming = c.streaming)
                try {
                    val times = LongArray(2)
                    var seen = 0L
                    repeat(2) { run ->
                        seen = 0L
                        times[run] =
                            measureNanoTime {
                                index.visitIds(EventQuery(kinds = listOf(1))) { page ->
                                    seen += page.size
                                    true
                                }
                            }
                    }
                    counts[c.label] = seen
                    val best = times.min()
                    println(
                        String.format(
                            "%-28s %10.2fs %10.2fs %,14d",
                            c.label,
                            times[0] / 1e9,
                            times[1] / 1e9,
                            (seen * 1e9 / best).toLong(),
                        ),
                    )
                } finally {
                    index.close()
                }
            }

            val distinct = counts.values.distinct()
            if (distinct.size == 1) {
                println("\nall configurations agree: ${distinct.single()} docs walked")
            } else {
                println("\nWARNING: configurations DISAGREE on the walked set: $counts")
            }
        }

    /** Deploy the bundled schema if needed and poll until the container serves queries. */
    private suspend fun awaitServing(url: String) {
        val store = VespaEventStore.open(url)
        try {
            repeat(90) {
                if (runCatching { store.count(Filter(kinds = listOf(0), limit = 1)) }.isSuccess) {
                    println("vespa serving.")
                    return
                }
                delay(2_000)
            }
            error("vespa at $url did not start serving within 180s")
        } finally {
            store.close()
        }
    }

    /**
     * Top the corpus up to [target] docs of kind 1 with deterministic ids, so
     * re-runs skip the feed. Docs are visit-shaped only (id, created_at,
     * pubkey): no signatures, fed straight through the index — the walk under
     * test reads ids and timestamps, not Nostr semantics.
     */
    private suspend fun feedUpTo(
        url: String,
        target: Int,
        batch: Int,
    ) {
        val index = VespaEventIndex(url)
        try {
            val have = index.count(EventQuery(kinds = listOf(1)))
            println("corpus has $have docs; target $target")
            if (have >= target) return
            var fed = have
            val started = System.nanoTime()
            while (fed < target) {
                val n = minOf(batch, target - fed)
                val docs =
                    List(n) { i ->
                        val seq = fed + i
                        EventDoc(
                            id = seq.toString(16).padStart(64, '0'),
                            pubkey = (seq % 512).toString(16).padStart(64, 'a'),
                            createdAt = 1_700_000_000L + seq,
                            kind = 1,
                            tags = emptyList(),
                            content = "visit bench doc $seq",
                            sig = "e".repeat(128),
                            owner = (seq % 512).toString(16).padStart(64, 'a'),
                        )
                    }
                index.putAll(docs)
                fed += n
                if (fed % 100_000 < batch) {
                    val rate = (fed - have) * 1e9 / (System.nanoTime() - started)
                    println("  fed $fed / $target (${String.format("%,.0f", rate)} docs/s)")
                }
            }
        } finally {
            index.close()
        }
    }
}
