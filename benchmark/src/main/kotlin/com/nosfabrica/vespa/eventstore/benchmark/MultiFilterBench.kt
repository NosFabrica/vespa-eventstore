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
import kotlinx.coroutines.runBlocking
import kotlin.system.measureNanoTime

/**
 * STORE-level multi-filter REQ latency over [VisitBench]'s corpus: the shapes a
 * real client sends as one REQ with several filters, timed through the full
 * [VespaEventStore] recall (per-filter engine queries, cross-filter dedup,
 * NIP-01 ordering). The per-filter fan-out is `VESPA_QUERY_FANOUT` — run this
 * bench under different values to A/B it (1 = serialized round trips, the
 * pre-fan-out behavior).
 *
 * Correctness gate: every multi-filter result must equal the union of its
 * filters queried INDIVIDUALLY, deduped by id, newest-first — the NIP-01
 * contract, computed from the same store so only the multi-filter path is
 * under test.
 *
 * Env: VESPA_URL (default http://localhost:8080), BENCH_MF_REPS (default 10),
 * VESPA_QUERY_FANOUT (the variable under test).
 */
object MultiFilterBench {
    private const val BASE = 1_700_000_000L

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val reps = System.getenv("BENCH_MF_REPS")?.toIntOrNull() ?: 10
            val store = VespaEventStore.open(url)
            var failures = 0
            try {
                val n = store.count(Filter(kinds = listOf(1)))
                check(n >= 1_400_000) { "corpus too small ($n) — run :benchmark:visitBench first" }
                val top = BASE + n - 1
                println("multi-filter store bench @ $url over $n docs, VESPA_QUERY_FANOUT=${System.getenv("VESPA_QUERY_FANOUT") ?: "4 (default)"}")

                fun author(k: Int) = k.toString(16).padStart(64, 'a')

                val shapes: List<Pair<String, List<Filter>>> =
                    listOf(
                        "12 author timelines (limit 100)" to
                            (0 until 12).map { Filter(kinds = listOf(1), authors = listOf(author(it * 7 + 1)), limit = 100) },
                        "6 mixed filters" to
                            listOf(
                                Filter(kinds = listOf(1), limit = 500, until = top),
                                Filter(kinds = listOf(1), since = top - 5_000, until = top - 2_000),
                                Filter(kinds = listOf(1), authors = (0 until 50).map { author(it) }, limit = 200),
                                Filter(kinds = listOf(1), authors = (50 until 100).map { author(it) }, limit = 200),
                                Filter(kinds = listOf(1), since = top - 100_000, until = top, limit = 300),
                                Filter(kinds = listOf(1), authors = listOf(author(200)), limit = 50),
                            ),
                        "4 heavy windows (10k full recall)" to
                            (0 until 4).map {
                                val since = BASE + (n / 5) * (it + 1)
                                Filter(kinds = listOf(1), since = since, until = since + 9_999)
                            },
                    )

                println()
                println(String.format("%-36s %12s %12s %14s", "shape", "multi p50", "Σ singles", "hits"))
                for ((label, filters) in shapes) {
                    // Correctness: the multi-filter result must equal the deduped,
                    // newest-first union of the individually-queried filters.
                    val multi = store.query<Event>(filters)
                    val union =
                        filters
                            .flatMap { store.query<Event>(it) }
                            .distinctBy { it.id }
                            .sortedWith(compareByDescending<Event> { it.createdAt }.thenBy { it.id })
                    if (multi.map { it.id } != union.map { it.id }) {
                        failures++
                        println("  !! $label: multi-filter result differs from the single-filter union (${multi.size} vs ${union.size})")
                    }

                    val multiP50 = medianNanos(reps) { store.query<Event>(filters).size }
                    // The serialized reference: each filter timed alone, summed.
                    val singlesSum = filters.sumOf { f -> medianNanos(reps) { store.query<Event>(f).size } }
                    println(
                        String.format(
                            "%-36s %10.2fms %10.2fms %,14d",
                            label,
                            multiP50 / 1e6,
                            singlesSum / 1e6,
                            multi.size.toLong(),
                        ),
                    )
                }

                println()
                if (failures == 0) {
                    println("all multi-filter results equal their single-filter unions")
                } else {
                    error("$failures correctness failure(s)")
                }
            } finally {
                store.close()
            }
        }

    private inline fun medianNanos(
        reps: Int,
        crossinline op: suspend () -> Int,
    ): Long =
        runBlocking {
            val lat = LongArray(reps) { measureNanoTime { op() } }
            lat.sort()
            lat[reps / 2]
        }
}
