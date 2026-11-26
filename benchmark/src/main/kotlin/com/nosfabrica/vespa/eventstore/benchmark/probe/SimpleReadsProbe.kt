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

import com.nosfabrica.vespa.eventstore.SchemaDeploy
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.client.RecencyStrategy
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking

/**
 * The simple reads a Nostr client sends most, one at a time, through the client a
 * relay runs (speculative recency with its window memory) on the REAL clock, so a
 * corpus whose newest note is days old is read the way a quiet or restored relay is.
 * Per shape: median and p95 wall time, engine queries per read off a [CostLedger],
 * and whether the page equals the [RecencyStrategy.FULL_SCAN] reference (same ids,
 * same order). Read-only against an already-loaded Vespa; exits 1 on any mismatch.
 *
 * `BENCH_VESPA_URL` (default `http://127.0.0.1:8080`), `BENCH_REPS` (default 40).
 * A REQ with no `limit` is asked at 500, the relay's `DEFAULT_LIMIT`.
 */
object SimpleReadsProbe {
    private const val DAY = 86_400L
    private const val DEFAULT_LIMIT = 500

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val reps = System.getenv("BENCH_REPS")?.toIntOrNull() ?: 40
            val ledger = CostLedger()
            val client = VespaEventIndex(url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = true, ledger = ledger)
            val reference = VespaEventIndex(url, recencyStrategy = RecencyStrategy.FULL_SCAN, recencyMemory = false)
            val wall = System.currentTimeMillis() / 1000
            val notes = listOf(1, 7, 30023)
            val newest = reference.search(EventQuery(kinds = notes, limit = 1, nowSecs = wall)).first().createdAt
            val byVolume = reference.countByAuthor(EventQuery(kinds = listOf(1))).entries.sortedByDescending { it.value }
            val busy = byVolume.first().key
            val quiet = byVolume.last { it.value >= 3 }.key
            val follow300 = byVolume.take(300).map { it.key }
            val profiled = reference.search(EventQuery(kinds = listOf(0), limit = 1, nowSecs = wall)).first().pubkey
            println("newest note $newest (${(wall - newest) / DAY}d behind the wall clock); ${byVolume.size} kind-1 authors")
            val shapes =
                listOf(
                    "newest 1, kinds 1/7/30023" to EventQuery(kinds = notes, limit = 1),
                    "newest 1, until = newest note" to EventQuery(kinds = notes, until = newest, limit = 1),
                    "newest 100, kinds 1/7/30023" to EventQuery(kinds = notes, limit = 100),
                    "newest 100, kind 1" to EventQuery(kinds = listOf(1), limit = 100),
                    "scroll back 1 day, 100" to EventQuery(kinds = notes, until = newest - DAY, limit = 100),
                    "scroll back 3 days, 100" to EventQuery(kinds = notes, until = newest - 3 * DAY, limit = 100),
                    "scroll back 20 days, 100 (empty)" to EventQuery(kinds = notes, until = newest - 20 * DAY, limit = 100),
                    "profile, kind 0, no limit" to EventQuery(kinds = listOf(0), authors = listOf(profiled), limit = DEFAULT_LIMIT),
                    "contacts, kind 3, limit 1 (none)" to EventQuery(kinds = listOf(3), authors = listOf(profiled), limit = 1),
                    "latest note, busy author, 1" to EventQuery(kinds = listOf(1), authors = listOf(busy), limit = 1),
                    "latest note, quiet author, 1" to EventQuery(kinds = listOf(1), authors = listOf(quiet), limit = 1),
                    "home feed, 300 follows, 50" to EventQuery(kinds = listOf(1), authors = follow300, limit = 50),
                    "home feed, 300 follows, 1" to EventQuery(kinds = listOf(1), authors = follow300, limit = 1),
                    "#p miss, no limit (idle test)" to EventQuery(kinds = listOf(1), tags = mapOf("p" to listOf("cd".repeat(32))), limit = DEFAULT_LIMIT),
                    "id miss" to EventQuery(ids = listOf("ab".repeat(32)), limit = 1),
                )
            println(String.format("%-36s %5s %8s %8s %8s  %s", "shape", "hits", "p50 ms", "p95 ms", "queries", "same page"))
            var mismatches = 0
            for ((name, shape) in shapes) {
                fun req() = shape.copy(nowSecs = System.currentTimeMillis() / 1000, notExpiredAt = System.currentTimeMillis() / 1000)
                val want = reference.search(req()).map { it.id }
                val got = client.search(req()).map { it.id }
                val same = got == want
                if (!same) mismatches++
                repeat(5) { client.search(req()) }
                ledger.reset()
                IngestStats.reset()
                val nanos =
                    (1..reps)
                        .map {
                            val t0 = System.nanoTime()
                            client.search(req())
                            System.nanoTime() - t0
                        }.sorted()
                val engine = ledger.snapshot().engine
                val queries = engine.sumOf { it.queries }.toDouble() / reps
                // Vespa's own split, per read: search (matching + ranking + sorting), summary fill, docs matched.
                val vespa = String.format("vespa %4.1f+%4.1f ms, matched %6.0f", engine.sumOf { it.engineNanos } / 1e6 / reps, engine.sumOf { it.summaryNanos } / 1e6 / reps, engine.sumOf { it.docsMatched }.toDouble() / reps)
                // Which recency outcomes the timed reads took, per read.
                val outcomes =
                    IngestStats
                        .snapshot()
                        .filter { (k, v) -> k.startsWith("recency.") && k != "recency.speculative" && v.calls > 0 }
                        .entries
                        .joinToString(" ") { (k, v) -> "${k.removePrefix("recency.").removePrefix("speculative.")}=${"%.1f".format(v.calls.toDouble() / reps)}" }
                println(String.format("%-36s %5d %8.1f %8.1f %8.1f  %-4s %s  %s", name, want.size, nanos[reps / 2] / 1e6, nanos[minOf(reps - 1, reps * 95 / 100)] / 1e6, queries, if (same) "yes" else "NO", vespa, outcomes))
            }
            // The same reads through the store FACADE, as a relay opens it (no deploy, the slow-read
            // ledger on): what the Nostr-semantics and trust layers add over the bare index.
            if (System.getenv("BENCH_FACADE") != null) {
                val store = VespaEventStore.open(url, schema = SchemaDeploy.NEVER, readOnly = true, slowQueryThresholdMillis = 1_000)
                println()
                println(String.format("%-36s %5s %8s %8s   (through VespaEventStore.query)", "shape", "hits", "p50 ms", "p95 ms"))
                for ((name, shape) in shapes) {
                    val f = Filter(ids = shape.ids.ifEmpty { null }, authors = shape.authors.ifEmpty { null }, kinds = shape.kinds.ifEmpty { null }, tags = shape.tags.ifEmpty { null }, since = shape.since, until = shape.until, limit = shape.limit)
                    val n = store.query<Event>(f).size
                    repeat(5) { store.query<Event>(f) }
                    val nanos =
                        (1..reps)
                            .map {
                                val t0 = System.nanoTime()
                                store.query<Event>(f)
                                System.nanoTime() - t0
                            }.sorted()
                    println(String.format("%-36s %5d %8.1f %8.1f", name, n, nanos[reps / 2] / 1e6, nanos[minOf(reps - 1, reps * 95 / 100)] / 1e6))
                }
                store.close()
            }
            client.close()
            reference.close()
            if (mismatches > 0) {
                System.err.println("$mismatches shape(s) served a page different from the full-scan reference")
                kotlin.system.exitProcess(1)
            }
        }
}
