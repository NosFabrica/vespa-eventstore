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

import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.io.File

/**
 * Machine-readable run results (`BENCH_JSON=path`). Every suite row also lands
 * here as `(suite, name, metrics)`; at the end of the run the whole thing is
 * written as one JSON file. That turns the benchmark from a measurement tool
 * into a REGRESSION GATE: keep a baseline file per box, and
 * `benchmark/compare_results.py old.json new.json` diffs any two runs —
 * throughput down or p99 up beyond a threshold fails loudly instead of
 * scrolling past in a table.
 *
 * No env var -> fully inert (the suites print exactly as before).
 */
object BenchResults {
    private class Entry(
        val suite: String,
        val name: String,
        val metrics: Map<String, Double>,
    )

    private val entries = ArrayList<Entry>()
    private var suite = "main"

    /** Group subsequent [record]s under this suite label (backend or mode name). */
    @Synchronized
    fun section(name: String) {
        suite = name
    }

    @Synchronized
    fun record(
        name: String,
        vararg metrics: Pair<String, Double>,
    ) {
        entries += Entry(suite, name, metrics.toMap())
    }

    /** [record] plus the latency tail (p50/p95/p99 in µs) from [lat]. */
    fun record(
        name: String,
        lat: Latencies,
        vararg metrics: Pair<String, Double>,
    ) {
        val tail =
            listOfNotNull(
                lat.percentileNanos(0.50)?.let { "p50_us" to it / 1000.0 },
                lat.percentileNanos(0.95)?.let { "p95_us" to it / 1000.0 },
                lat.percentileNanos(0.99)?.let { "p99_us" to it / 1000.0 },
            )
        record(name, *(metrics.toList() + tail).toTypedArray())
    }

    /** Write the collected entries to `BENCH_JSON` (no-op when unset). */
    @Synchronized
    fun writeIfRequested(meta: Map<String, String>) {
        val path = System.getenv("BENCH_JSON") ?: return
        if (entries.isEmpty()) return
        val json =
            buildJsonObject {
                put(
                    "meta",
                    buildJsonObject {
                        put("timestamp_ms", System.currentTimeMillis())
                        meta.forEach { (k, v) -> put(k, v) }
                    },
                )
                put(
                    "results",
                    buildJsonArray {
                        entries.forEach { e ->
                            add(
                                buildJsonObject {
                                    put("suite", e.suite)
                                    put("name", e.name)
                                    e.metrics.forEach { (k, v) -> put(k, v) }
                                },
                            )
                        }
                    },
                )
            }
        File(path).writeText(json.toString())
        println("\nresults written to $path (${entries.size} rows)")
    }
}
