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
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureNanoTime

/**
 * Concurrent-writer ingest A/B (`BENCH_CONCURRENT_INGEST=1`) — the write-side
 * mirror of the read concurrency suite, and the workload geode's
 * `publishThroughputConcurrent` targets. N publishers each `batchInsert` a
 * disjoint slice of one corpus at the same time; we report aggregate events/sec
 * as N rises.
 *
 * The point of interest: **SQLite has a single writer** — concurrent writers
 * serialize behind one lock, so its aggregate EPS should stay flat (or dip on
 * lock contention) as N grows. This store's Vespa feed client is the opposite:
 * many writes in flight over multiplexed HTTP/2, no global write lock, so it
 * should *scale* with the publisher count. This is the shape where the Vespa
 * store is expected to beat in-process SQLite outright.
 *
 * Each concurrency level gets its own id-disjoint corpus (distinct `idBand`), so
 * a shared real Vespa never sees a cross-level duplicate and every insert is a
 * real write. Tunables: BENCH_CI_SIZE (events/level, 60000), BENCH_CI_BATCH
 * (chunk, 500), BENCH_CI_CONC (1,2,4,8,16,32), BENCH_VESPA_URL (omit = SQLite only).
 */
object ConcurrentIngest {
    fun run(
        url: String?,
        seed: Long,
    ) = runBlocking {
        val size = System.getenv("BENCH_CI_SIZE")?.toIntOrNull() ?: 60_000
        val batch = System.getenv("BENCH_CI_BATCH")?.toIntOrNull() ?: 500
        val concs =
            (System.getenv("BENCH_CI_CONC") ?: "1,2,4,8,16,32")
                .split(",")
                .mapNotNull { it.trim().toIntOrNull() }

        println("concurrent-writer ingest: size=$size/level batch=$batch concurrencies=$concs")
        // Warm the JVM (and, for Vespa, the engine's jdisc JIT) before the sweep so the
        // conc=1 row isn't measured cold — otherwise warmup would masquerade as concurrency
        // scaling as the JIT settles across levels. See Warmup.
        Warmup.warmVia { Backends.sqliteMemory() }
        url?.let { u -> Warmup.warmVia { VespaEventStore.open(u) } }
        println(String.format("%-8s %6s %14s %11s %9s  %s", "engine", "writers", "events/sec", "scale@1", "rejects", "batchInsert latency"))

        for (engine in listOfNotNull("sqlite", url?.let { "vespa" })) {
            var base = 0.0
            for ((li, c) in concs.withIndex()) {
                // Distinct id space per (engine, level): a shared Vespa sees only fresh writes.
                val band = 0x40L + (if (engine == "sqlite") 0L else 0x20L) + li
                val corpus = NostrCorpus.generate(NostrCorpus.Config(size = size, seed = seed + li, idBand = band))
                val slices = partition(corpus, c)
                val store: IEventStore = if (engine == "sqlite") Backends.sqliteMemory() else VespaEventStore.open(url!!)

                val ok = AtomicInteger()
                val fail = AtomicInteger()
                val lat = Latencies()
                val latLock = Any()

                val nanos =
                    measureNanoTime {
                        coroutineScope {
                            slices.forEach { slice ->
                                launch(Dispatchers.IO) {
                                    slice.chunked(batch).forEach { chunk ->
                                        val t = System.nanoTime()
                                        runCatching { store.batchInsert(chunk) }
                                            .onSuccess { ok.addAndGet(chunk.size) }
                                            .onFailure { fail.addAndGet(chunk.size) }
                                        synchronized(latLock) { lat.record(System.nanoTime() - t) }
                                    }
                                }
                            }
                        }
                    }
                store.close()

                val eps = ok.get() / (nanos / 1e9)
                if (li == 0) base = eps
                println(String.format("%-8s %6d %14.0f %10.2fx %9d  %s", engine, c, eps, eps / base, fail.get(), lat.summary()))
                BenchResults.section("concurrent-ingest-$engine")
                BenchResults.record("ingest_eps@c$c", "value" to eps)
            }
        }
    }

    /** Split [list] into [parts] contiguous, disjoint slices (drops empty trailing slices). */
    private fun <T> partition(
        list: List<T>,
        parts: Int,
    ): List<List<T>> {
        val per = (list.size + parts - 1) / parts
        return (0 until parts)
            .map { list.subList((it * per).coerceAtMost(list.size), ((it + 1) * per).coerceAtMost(list.size)) }
            .filter { it.isNotEmpty() }
    }
}
