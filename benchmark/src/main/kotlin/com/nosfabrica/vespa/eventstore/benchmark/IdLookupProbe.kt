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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlin.system.measureNanoTime

/**
 * id-lookup fan-out sweep (`BENCH_ID_LOOKUP=1`): a REQ carrying N ids at a time,
 * for N in [16, 64, 128, 256], on SQLite and real Vespa. The point of interest
 * is the Vespa fast-path threshold: at N ≤ `ID_GET_FANOUT` (32) the store does N
 * parallel document-API gets; above it, one `id in (…)` search. This shows how
 * a thread-fetch (dozens of ids) vs a big id-set (hundreds) behave on each side.
 *
 * Env: BENCH_ID_SIZE (corpus, 60000), BENCH_ID_REPS (300), BENCH_ID_NS
 * ("16,64,128,256"), BENCH_VESPA_URL (omit = SQLite only).
 */
object IdLookupProbe {
    fun run(
        url: String?,
        seed: Long,
    ) = runBlocking {
        val size = System.getenv("BENCH_ID_SIZE")?.toIntOrNull() ?: 60_000
        val reps = System.getenv("BENCH_ID_REPS")?.toIntOrNull() ?: 300
        val ns = (System.getenv("BENCH_ID_NS") ?: "16,64,128,256").split(",").mapNotNull { it.trim().toIntOrNull() }

        val corpus = NostrCorpus.generate(NostrCorpus.Config(size = size, seed = seed))
        val w = BenchWorkload.from(corpus)
        val sqlite = Backends.sqliteMemory()
        val vespa: IEventStore? = url?.let { VespaEventStore.open(it) }
        val engines = listOfNotNull("sqlite" to sqlite, vespa?.let { "vespa" to it })

        engines.forEach { (_, s) -> Warmup.warm(s) }
        engines.forEach { (name, s) ->
            corpus.chunked(1_000).forEach { s.batchInsert(it) }
            println("$name loaded ${corpus.size}")
        }

        println("\nid-lookup fan-out (real Vespa fast-path threshold ID_GET_FANOUT=32)")
        println(String.format("%4s %-8s %12s %14s %12s", "ids", "engine", "q/s", "events/query", "µs/query"))
        for (n in ns) {
            for ((name, s) in engines) {
                repeat((reps / 10).coerceAtLeast(3)) { i -> runCatching { s.query<Event>(Filter(ids = w.idList(i, n))) } }
                var events = 0L
                val nanos =
                    measureNanoTime {
                        runBlocking { repeat(reps) { i -> events += s.query<Event>(Filter(ids = w.idList(i, n))).size } }
                    }
                val secs = nanos / 1e9
                println(String.format("%4d %-8s %12.1f %14.1f %12.1f", n, name, reps / secs, events.toDouble() / reps, nanos / 1000.0 / reps))
            }
        }
        sqlite.close()
        vespa?.close()
    }
}
