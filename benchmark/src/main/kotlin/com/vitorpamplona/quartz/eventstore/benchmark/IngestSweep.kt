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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicLong

/**
 * Bulk-ingest sweep (`BENCH_INGEST_SWEEP=1`): chunk size x parallel streams,
 * against a live Vespa, through the real store stack. Ingest on a single node
 * is throttle-floor-bound, not engine-bound — the feed client's dynamic
 * throttler idles at its `minInflight` floor under bursty batched writes — so
 * the open question is which knob actually raises throughput: bigger chunks
 * (fewer guard-read waves, longer put bursts), more concurrent `batchInsert`
 * streams (the multi-relay sync shape — plan/guard reads overlap outside the
 * store's writer lock), or both. This measures the grid and reports events/sec
 * plus per-CHUNK commit-latency tails.
 *
 * Every cell feeds genuinely fresh events (its own id band), so cell N+1 runs
 * against a slightly larger corpus than cell N — ingest cost grows only weakly
 * with corpus size, but read the grid as a comparison, not as absolutes.
 *
 * Tunables: BENCH_SWEEP_CHUNKS ("250,500,1000,2000"), BENCH_SWEEP_STREAMS
 * ("1,2,4"), BENCH_SWEEP_EVENTS (events per cell, default 4000).
 */
object IngestSweep {
    fun run(
        url: String,
        seed: Long,
    ) = runBlocking {
        val chunkSizes = (System.getenv("BENCH_SWEEP_CHUNKS") ?: "250,500,1000,2000").split(",").mapNotNull { it.trim().toIntOrNull() }
        val streamCounts = (System.getenv("BENCH_SWEEP_STREAMS") ?: "1,2,4").split(",").mapNotNull { it.trim().toIntOrNull() }
        val perCell = System.getenv("BENCH_SWEEP_EVENTS")?.toIntOrNull() ?: 4_000

        println("ingest sweep @ $url  chunks=$chunkSizes streams=$streamCounts events/cell=$perCell")
        println(String.format("%-8s %8s %14s %14s  %s", "chunk", "streams", "events/sec", "ms/chunk", "chunk latency tail"))

        VespaEventStore.open(url).use { store ->
            Warmup.warm(store) // steady-state, not JIT warmup — see Warmup
            var cell = 0
            for (chunk in chunkSizes) {
                for (streams in streamCounts) {
                    // Fresh id band per (cell, stream): every insert is genuinely new.
                    val feeds =
                        (0 until streams).map { s ->
                            NostrCorpus.generate(
                                NostrCorpus.Config(size = perCell / streams, seed = seed + 1000 + cell * 16 + s, idBand = 0xD00L + cell * 16 + s),
                            )
                        }
                    cell++
                    val written = AtomicLong()
                    val lat = Latencies()
                    val t0 = System.nanoTime()
                    withContext(Dispatchers.Default) {
                        feeds.forEach { feed ->
                            launch {
                                feed.chunked(chunk).forEach { c ->
                                    lat.timed { runCatching { store.batchInsert(c) } }
                                    written.addAndGet(c.size.toLong())
                                }
                            }
                        }
                    }
                    val secs = (System.nanoTime() - t0) / 1e9
                    val chunks = (written.get() + chunk - 1) / chunk
                    println(
                        String.format(
                            "%-8d %8d %14s %14.1f  %s",
                            chunk,
                            streams,
                            num(written.get() / secs),
                            secs * 1000 / chunks.coerceAtLeast(1),
                            lat.summary(),
                        ),
                    )
                    BenchResults.record("chunk$chunk x$streams", lat, "events_per_sec" to written.get() / secs)
                }
            }
        }
    }

    private fun num(v: Double) = if (v >= 1000) String.format("%,d", v.toLong()) else String.format("%.1f", v)
}
