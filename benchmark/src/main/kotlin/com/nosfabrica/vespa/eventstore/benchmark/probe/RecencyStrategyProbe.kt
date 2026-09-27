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
package com.nosfabrica.vespa.eventstore.benchmark.probe

import com.nosfabrica.vespa.eventstore.engine.client.RecencyStrategy
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.runBlocking

/**
 * A/B the four [RecencyStrategy]s — and the speculative one with and without
 * its window memory — on the REQ shapes that dominate a relay's
 * read cost, against an ALREADY-LOADED Vespa (read-only: it never deploys,
 * never feeds). Every shape runs through all four strategies through the real
 * client, and the probe FAILS a shape whose four pages are not identical —
 * same ids, same order — so a speed-up that changed an answer cannot report.
 *
 * Strategies are interleaved per rep (rotating the order) so cache warmth and
 * engine drift land on all four alike. Per strategy it reports the median and
 * p95 wall time, and — off a [CostLedger] per client — how many engine queries
 * one REQ cost and Vespa's own engine time for them.
 *
 * Env:
 *  - BENCH_VESPA_URL  (http://127.0.0.1:8080)
 *  - BENCH_NOW        the query instant, epoch seconds; default: the newest
 *                     kind-1 `created_at` not in the future — so a frozen
 *                     corpus is read as if it were live
 *  - BENCH_OBSERVER   hex pubkey; enables the gated shapes
 *  - BENCH_LENS_KEY   the key `user_q` carries (default BENCH_OBSERVER — right
 *                     for a schema that predates service-keyed trust; pass the
 *                     10040's service key on a current one)
 *  - BENCH_MIN_RANK   the gate floor (2.0, the store's DEFAULT_MIN_RANK)
 *  - BENCH_TAG        a busy `t` value (bitcoin)
 *  - BENCH_REPS       timed reps per shape (9)
 *  - BENCH_SHAPES     comma-separated substrings to select shapes (all)
 */
object RecencyStrategyProbe {
    private const val DAY = 86_400L

    private class Shape(
        val name: String,
        val query: EventQuery,
    )

    /** One client under test: a strategy, with or without the window memory, and its own cost ledger. */
    private class Variant(
        val label: String,
        val client: VespaEventIndex,
        val ledger: CostLedger,
    )

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val reps = System.getenv("BENCH_REPS")?.toIntOrNull() ?: 9
            val observer = System.getenv("BENCH_OBSERVER")?.lowercase()
            val lensKey = System.getenv("BENCH_LENS_KEY")?.lowercase() ?: observer
            val minRank = System.getenv("BENCH_MIN_RANK")?.toDoubleOrNull() ?: 2.0
            val tag = System.getenv("BENCH_TAG") ?: "bitcoin"
            val only =
                System
                    .getenv("BENCH_SHAPES")
                    ?.split(",")
                    ?.map { it.trim() }
                    ?.filter { it.isNotEmpty() }

            // Every strategy, plus speculative twice: WITHOUT the window memory
            // (every read cold — what the first read of any shape pays) and WITH
            // it (warm — every later read of a shape the client has seen).
            val variants =
                RecencyStrategy.entries.map { s ->
                    val ledger = CostLedger()
                    Variant(s.name.lowercase(), VespaEventIndex(url, recencyStrategy = s, recencyMemory = false, ledger = ledger), ledger)
                } +
                    CostLedger().let { ledger ->
                        Variant("speculative+memory", VespaEventIndex(url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = true, ledger = ledger), ledger)
                    }
            val reference = variants.first { it.label == "match_phase" }.client

            val wall = System.currentTimeMillis() / 1000
            val now =
                System.getenv("BENCH_NOW")?.toLongOrNull()
                    ?: reference.search(EventQuery(kinds = listOf(1), until = wall, limit = 1, nowSecs = wall)).first().createdAt
            println("query instant $now (wall clock $wall, ${(wall - now) / DAY}d behind)")

            // Real inputs, off the corpus itself: authors ranked by kind-1 volume.
            val byVolume =
                reference
                    .countByAuthor(EventQuery(kinds = listOf(1)))
                    .entries
                    .sortedByDescending { it.value }
            val follow300 = byVolume.subList(200, 500).map { it.key }
            val follow1000 = byVolume.subList(500, 1_500).map { it.key }
            val sparse50 = byVolume.filter { it.value in 5..59 }.take(50).map { it.key }
            println("authors ${byVolume.size}; follow300 ${follow300.size}, follow1000 ${follow1000.size}, sparse50 ${sparse50.size}")

            // What the store stamps on every REQ: the request clock and the expiry guard.
            fun req(q: EventQuery) = q.copy(nowSecs = now, notExpiredAt = now)

            fun gated(q: EventQuery) = q.copy(ranking = EventYql.RANK_RECENCY_GATED, observer = observer, rankKey = lensKey, minRank = minRank)

            val feeds = listOf(1, 6, 7)
            val plain =
                listOf(
                    Shape("global k1 lim50", EventQuery(kinds = listOf(1), limit = 50)),
                    Shape("global k1/6/7 lim100", EventQuery(kinds = feeds, limit = 100)),
                    Shape("global k1 lim500", EventQuery(kinds = listOf(1), limit = 500)),
                    Shape("follow300 lim500", EventQuery(kinds = feeds, authors = follow300, limit = 500)),
                    Shape("follow1000 lim100", EventQuery(kinds = feeds, authors = follow1000, limit = 100)),
                    Shape("sparse50 lim200", EventQuery(kinds = feeds, authors = sparse50, limit = 200)),
                    Shape("#t:$tag k1 lim50", EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf(tag)), limit = 50)),
                    Shape("deep k1 until-180d lim50", EventQuery(kinds = listOf(1), until = now - 180 * DAY, limit = 50)),
                    Shape("deep follow300 until-90d lim100", EventQuery(kinds = feeds, authors = follow300, until = now - 90 * DAY, limit = 100)),
                )
            val gatedShapes =
                if (observer == null) {
                    emptyList()
                } else {
                    listOf(
                        Shape("GATED global k1 lim50", gated(EventQuery(kinds = listOf(1), limit = 50))),
                        Shape("GATED follow300 lim500", gated(EventQuery(kinds = feeds, authors = follow300, limit = 500))),
                        Shape("GATED follow1000 lim100", gated(EventQuery(kinds = feeds, authors = follow1000, limit = 100))),
                        Shape("GATED sparse50 lim200", gated(EventQuery(kinds = feeds, authors = sparse50, limit = 200))),
                        Shape("GATED #p notif lim100", gated(EventQuery(kinds = listOf(1, 6, 7, 9735), tags = mapOf("p" to listOf(observer)), limit = 100))),
                        Shape("GATED deep k1 until-180d lim50", gated(EventQuery(kinds = listOf(1), until = now - 180 * DAY, limit = 50))),
                        Shape("GATED deep follow300 until-90d lim100", gated(EventQuery(kinds = feeds, authors = follow300, until = now - 90 * DAY, limit = 100))),
                    )
                }
            val shapes = (plain + gatedShapes).filter { s -> only == null || only.any { s.name.contains(it, ignoreCase = true) } }

            println()
            println("median ms / p95 ms / engine queries per REQ")
            println(String.format("%-38s %6s | %s | %s", "shape", "hits", variants.joinToString(" | ") { String.format("%-22s", it.label) }, "same page"))
            var mismatches = 0
            for (shape in shapes) {
                val q = req(shape.query)
                // Correctness first, off one untimed pass per variant.
                val pages = variants.associate { v -> v.label to v.client.search(q).map { it.id } }
                val ref = pages.getValue("full_scan")
                val same = pages.values.all { it == ref }
                if (!same) {
                    mismatches++
                    pages.forEach { (label, ids) -> System.err.println("  MISMATCH ${shape.name} $label: ${ids.size} ids, first diff at ${ids.indices.firstOrNull { it >= ref.size || ids[it] != ref[it] }}") }
                }
                // Warm every variant, then time them interleaved.
                repeat(2) { variants.forEach { it.client.search(q) } }
                variants.forEach { it.ledger.reset() }
                val nanos = variants.associate { it.label to ArrayList<Long>(reps) }
                repeat(reps) { rep ->
                    for (i in variants.indices) {
                        val v = variants[(i + rep) % variants.size]
                        val t0 = System.nanoTime()
                        v.client.search(q)
                        nanos.getValue(v.label).add(System.nanoTime() - t0)
                    }
                }
                val cells =
                    variants.map { v ->
                        val sorted = nanos.getValue(v.label).sorted()
                        val p50 = sorted[sorted.size / 2] / 1e6
                        val p95 = sorted[minOf(sorted.size - 1, (sorted.size * 95) / 100)] / 1e6
                        val queries =
                            v.ledger
                                .snapshot()
                                .engine
                                .sumOf { it.queries }
                                .toDouble() / reps
                        String.format("%7.1f %7.1f %4.1f", p50, p95, queries)
                    }
                println(String.format("%-38s %6d | %s | %s", shape.name, ref.size, cells.joinToString(" | "), if (same) "yes" else "NO"))
            }
            variants.forEach { it.client.close() }
            println()
            if (mismatches > 0) {
                System.err.println("$mismatches shape(s) served DIFFERENT pages across variants")
                kotlin.system.exitProcess(1)
            }
            println("every shape served the identical page under every variant")
        }
}
