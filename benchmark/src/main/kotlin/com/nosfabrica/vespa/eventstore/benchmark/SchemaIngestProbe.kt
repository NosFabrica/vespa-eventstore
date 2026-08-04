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

import com.nosfabrica.vespa.eventstore.vespa.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc
import kotlinx.coroutines.runBlocking
import kotlin.system.measureNanoTime

/**
 * Pure index-write probe (`BENCH_SCHEMA_INGEST=1`): feed N documents straight
 * through [VespaEventIndex] (`putAll`), bypassing all store logic (no dedup /
 * supersession / deletion probes), against whatever schema is currently deployed
 * at `VESPA_URL`. Reports feed events/sec.
 *
 * Run it once per deployed schema variant (full vs summary-only) to isolate how
 * much of Vespa's ingest cost is index/attribute build vs. the base document
 * write + transaction-log durability. The store logic and the query path are
 * deliberately out of the loop — this measures the engine's write path only.
 *
 * Env: VESPA_URL, BENCH_SI_SIZE (docs, default 60000), BENCH_SI_BATCH (500).
 */
object SchemaIngestProbe {
    fun main(args: Array<String>) {
        val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
        val size = System.getenv("BENCH_SI_SIZE")?.toIntOrNull() ?: 60_000
        val batch = System.getenv("BENCH_SI_BATCH")?.toIntOrNull() ?: 500
        // Warmup feed, discarded, so the measured rate is STEADY STATE not JVM warmup.
        // A freshly-deployed Vespa spends ~30s of its first feed JIT-compiling the
        // Jetty/jdisc/docproc/messagebus pipeline (profiled: compile-time +33s in feed
        // round 1, then ~0). Feeding this many docs first pushes those hot methods to
        // C2 so the timed feed measures the warm engine (~1.5x the cold rate here).
        val warmup = System.getenv("BENCH_SI_WARMUP")?.toIntOrNull() ?: (size / 2).coerceIn(10_000, 40_000)

        // id-disjoint bands: warmup (0x9) and the measured set (0x7) never collide, so
        // the timed feed is all fresh puts, not overwrites of the warmup docs.
        val docs = NostrCorpus.generate(NostrCorpus.Config(size = size, seed = 42, idBand = 0x7L)).map { EventDoc.fromEventJson(it.toJson()) }

        VespaEventIndex(url).use { idx ->
            if (warmup > 0) {
                val warmDocs = NostrCorpus.generate(NostrCorpus.Config(size = warmup, seed = 41, idBand = 0x9L)).map { EventDoc.fromEventJson(it.toJson()) }
                val wNanos = measureNanoTime { runBlocking { warmDocs.chunked(batch).forEach { idx.putAll(it) } } }
                println(String.format("warmup: fed %d docs (discarded) in %.1fs = %.0f events/sec", warmup, wNanos / 1e9, warmup / (wNanos / 1e9)))
            }
            val nanos = measureNanoTime { runBlocking { docs.chunked(batch).forEach { idx.putAll(it) } } }
            val secs = nanos / 1e9
            println(String.format("schema-ingest (warm): fed %d docs (batch %d) in %.1fs = %.0f events/sec", size, batch, secs, size / secs))
            println(idx.feedGauge())
        }
    }
}
