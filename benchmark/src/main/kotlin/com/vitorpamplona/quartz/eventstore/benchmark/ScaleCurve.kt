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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlin.system.measureNanoTime

/**
 * The throughput-vs-corpus-size CURVE (`BENCH_SCALE_CURVE=1`): one corpus,
 * generated once at the largest checkpoint with a PINNED author count (so
 * every smaller checkpoint is an exact prefix), delta-ingested into BOTH
 * stores checkpoint by checkpoint. At each checkpoint it measures the ingest
 * rate of the delta and a handful of query shapes on each engine — the
 * plottable series behind the 30k-vs-300k scale study, instead of two points.
 *
 * Emits a CSV block (`curve,size,engine,metric,value`) besides the table, and
 * records everything into BENCH_JSON.
 *
 * Tunables: BENCH_CURVE_SIZES ("25000,50000,100000,200000,400000"),
 * BENCH_VESPA_URL (omit for SQLite-only).
 */
object ScaleCurve {
    fun run(
        url: String?,
        seed: Long,
    ) = runBlocking {
        val sizes =
            (System.getenv("BENCH_CURVE_SIZES") ?: "25000,50000,100000,200000,400000")
                .split(",")
                .mapNotNull { it.trim().toIntOrNull() }
                .sorted()
        val max = sizes.last()
        println("scale curve: sizes=$sizes (corpus generated once at $max, pinned authors)")
        val corpus =
            NostrCorpus.generate(
                NostrCorpus.Config(size = max, authorCount = (max / 20).coerceIn(50, 50_000), seed = seed),
            )

        val sqlite = Backends.sqliteMemory()
        val vespa: IEventStore? = url?.let { VespaEventStore.open(it) }
        val engines = listOfNotNull("sqlite" to sqlite, vespa?.let { "vespa" to it })
        // Warm each engine (feed+delete junk) so the FIRST checkpoint's ingest is
        // steady state, not JIT warmup — otherwise the 25k point reads artificially
        // low (cold) while later checkpoints are already warm from delta-feeding.
        engines.forEach { (_, store) -> Warmup.warm(store) }
        val csv = StringBuilder("curve,size,engine,metric,value\n")

        fun emit(
            size: Int,
            engine: String,
            metric: String,
            value: Double,
        ) {
            csv.append("curve,$size,$engine,$metric,${"%.1f".format(value)}\n")
            BenchResults.section("curve-$engine")
            BenchResults.record("$metric@$size", "value" to value)
        }

        var done = 0
        for (size in sizes) {
            val delta = corpus.subList(done, size)
            done = size
            println("\n== checkpoint $size (${delta.size} new events) ==")
            for ((name, store) in engines) {
                val nanos = measureNanoTime { runBlocking { delta.chunked(1_000).forEach { store.batchInsert(it) } } }
                val rate = delta.size / (nanos / 1e9)
                println(String.format("%-8s ingest %12.0f events/sec", name, rate))
                emit(size, name, "ingest_eps", rate)
            }
            val w = BenchWorkload.from(corpus.subList(0, size))
            val shapes =
                listOf<Triple<String, Int, suspend (IEventStore, Int) -> Any?>>(
                    Triple("author-timeline", 150) { s, i -> s.query<Event>(Filter(authors = listOf(w.author(i)), limit = 50)) },
                    Triple("follow-feed(a300)", 40) { s, i -> s.query<Event>(Filter(authors = w.authorList(i, 300), kinds = listOf(1, 6, 7), limit = 500)) },
                    Triple("nip50-search", 30) { s, i -> s.query<Event>(Filter(kinds = listOf(1), search = w.term(i), limit = 50)) },
                    Triple("id-lookup(16)", 300) { s, i -> s.query<Event>(Filter(ids = w.idList(i, 16))) },
                    Triple("id-lookup(256)", 120) { s, i -> s.query<Event>(Filter(ids = w.idList(i, 256))) },
                )
            for ((name, store) in engines) {
                for ((shape, reps, op) in shapes) {
                    repeat((reps / 10).coerceAtLeast(3)) { runCatching { op(store, it) } }
                    val nanos = measureNanoTime { runBlocking { repeat(reps) { i -> runCatching { op(store, i) } } } }
                    val qps = reps / (nanos / 1e9)
                    println(String.format("%-8s %-18s %10.1f q/s", name, shape, qps))
                    emit(size, name, shape, qps)
                }
            }
        }
        sqlite.close()
        vespa?.close()
        println("\n--- CSV ---\n$csv")
    }
}
