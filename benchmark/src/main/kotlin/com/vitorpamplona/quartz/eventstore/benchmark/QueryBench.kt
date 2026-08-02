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

import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.system.measureNanoTime

/**
 * Latency AND correctness of the search/count paths over [VisitBench]'s
 * corpus, whose layout makes every expectation EXACT: doc `seq` has
 * `created_at = BASE + seq` (strictly unique, linear), `pubkey = seq % 512`,
 * kind 1. So a `[since, until]` window must return exactly `until - since + 1`
 * docs, newest-first ordering is checkable against the sequence itself,
 * count() must equal the window width, countDistinctAuthors must be exactly
 * 512, and countByKind must be {1: N}. A wrong size or order FAILS the run —
 * this is a correctness gate that happens to be timed.
 *
 * Env: VESPA_URL (default http://localhost:8080), BENCH_QUERY_REPS (default 20).
 */
object QueryBench {
    private const val BASE = 1_700_000_000L

    private var failures = 0

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val reps = System.getenv("BENCH_QUERY_REPS")?.toIntOrNull() ?: 20
            val index = VespaEventIndex(url)
            try {
                val n = index.count(EventQuery(kinds = listOf(1)))
                println("query/count bench @ $url over $n docs (reps=$reps per shape)")
                // 1.4M floor: the count(window=1000k) expectation anchors at
                // BASE + n/4 and needs the full window inside the corpus.
                check(n >= 1_400_000) { "corpus too small ($n docs) — run :benchmark:visitBench first to feed it" }
                // Every expectation derives from the PRISTINE visit band
                // (created_at = BASE + seq, contiguous). Foreign kind-1 docs —
                // corpusLoad's events, searchBench's band, mixed-load writes —
                // shift the derived top and poison hundreds of assertions with
                // cryptic order-wrong noise. Detect and name the fix instead.
                val newest = index.search(EventQuery(kinds = listOf(1), limit = 1)).single().createdAt
                check(newest == BASE + n - 1) {
                    "corpus polluted: newest kind-1 doc is at $newest, the pristine band would end at ${BASE + n - 1} " +
                        "(count includes foreign kind-1 docs). Restore the snapshot (docker stop vespa; docker rm vespa; " +
                        "docker run -d --name vespa -p 8080:8080 -p 19071:19071 vespa-2m-snapshot) or re-feed via :benchmark:visitBench " +
                        "on a clean engine — see benchmark/README.md's bench inventory."
                }
                val top = BASE + n - 1 // newest created_at in the corpus

                println()
                Header.print()

                // --- limit sweep, newest first (the relay's kind-scan REQ) ---
                for (limit in listOf(1, 10, 100, 1_000, 10_000, 100_000)) {
                    timed("kind=1 limit=$limit", reps.forSize(limit)) {
                        val docs = index.search(EventQuery(kinds = listOf(1), limit = limit))
                        expect(docs.size == limit) { "expected $limit hits, got ${docs.size}" }
                        // Newest first over a linear corpus = created_at counts straight down from the top.
                        expect(docs.first().createdAt == top && docs.last().createdAt == top - limit + 1) {
                            "order wrong: got ${docs.first().createdAt}..${docs.last().createdAt}, expected $top..${top - limit + 1}"
                        }
                        docs.size
                    }
                }

                // --- since/until windows, FULL recall (no limit) ---
                for (width in listOf(1_000, 10_000, 100_000)) {
                    // A window in the middle of the corpus, exact width.
                    val since = BASE + n / 2
                    val until = since + width - 1
                    timed("window=${width.k()} no limit", reps.forSize(width)) {
                        val docs = index.search(EventQuery(kinds = listOf(1), since = since, until = until))
                        expect(docs.size == width) { "expected $width hits, got ${docs.size}" }
                        docs.size
                    }
                }

                // --- windows + limit (the paging REQ a client actually sends) ---
                for (width in listOf(100_000, 1_000_000)) {
                    val since = BASE + n - width
                    timed("window=${width.k()} limit=500", reps) {
                        val docs = index.search(EventQuery(kinds = listOf(1), since = since, until = top, limit = 500))
                        expect(docs.size == 500) { "expected 500 hits, got ${docs.size}" }
                        expect(docs.first().createdAt == top) { "order wrong: top hit ${docs.first().createdAt} != $top" }
                        docs.size
                    }
                }

                // --- author timelines (pubkey = seq % 512 -> ~n/512 docs each) ---
                val author = 7L.author()
                timed("author limit=100", reps) {
                    val docs = index.search(EventQuery(kinds = listOf(1), authors = listOf(author), limit = 100))
                    expect(docs.size == 100) { "expected 100 hits, got ${docs.size}" }
                    expect(docs.all { it.pubkey == author }) { "hit from wrong author" }
                    docs.size
                }
                val fifty = (0L until 50L).map { it.author() }
                timed("authors=50 limit=500", reps) {
                    val docs = index.search(EventQuery(kinds = listOf(1), authors = fifty, limit = 500))
                    expect(docs.size == 500) { "expected 500 hits, got ${docs.size}" }
                    docs.size
                }

                // --- COUNT correctness + speed ---
                timed("count(all kind=1)", reps) {
                    index.count(EventQuery(kinds = listOf(1))).also {
                        expect(it == n) { "count(kind=1)=$it, expected $n" }
                    }
                }
                timed("count(no match)", reps) {
                    index.count(EventQuery(kinds = listOf(2))).also {
                        expect(it == 0) { "count(kind=2)=$it, expected 0" }
                    }
                }
                for (width in listOf(10_000, 1_000_000)) {
                    val since = BASE + n / 4
                    val until = since + width - 1
                    timed("count(window=${width.k()})", reps) {
                        index.count(EventQuery(kinds = listOf(1), since = since, until = until)).also {
                            expect(it == width) { "count(window $width)=$it, expected $width" }
                        }
                    }
                }
                // The generator's 'a'-padding makes some author strings COLLIDE
                // (hex(7) and hex(0xa7) both pad to "aa…a7"), so expectations
                // group by the stored string — a lucky trap that also proves the
                // engine counts the merged author exactly.
                val perString = (0 until 512).groupBy { it.toLong().author() }
                val expectedAuthorCount = perString.getValue(author).sumOf { countOfAuthor(it, n) }
                val expectedDistinct = perString.size
                timed("count(author)", reps) {
                    index.count(EventQuery(kinds = listOf(1), authors = listOf(author))).also {
                        expect(it == expectedAuthorCount) { "count(author)=$it, expected $expectedAuthorCount" }
                    }
                }
                timed("scanAuthors (exact)", 3) {
                    index.scanAuthors(EventQuery(kinds = listOf(1))).size.also {
                        expect(it == expectedDistinct) { "scanAuthors=$it distinct, expected $expectedDistinct" }
                    }
                }
                timed("countDistinctAuthors", reps) {
                    // Vespa's grouping count() over the group LIST is an
                    // ESTIMATE (sketch-based); measure and report its drift
                    // from the exact answer instead of failing on it.
                    index.countDistinctAuthors(EventQuery(kinds = listOf(1)))
                }
                val estimated = index.countDistinctAuthors(EventQuery(kinds = listOf(1)))
                if (estimated != expectedDistinct) {
                    println(
                        "  NOTE: countDistinctAuthors=$estimated vs $expectedDistinct exact " +
                            "(${"%.1f".format(100.0 * (estimated - expectedDistinct) / expectedDistinct)}% drift — Vespa's group count is an estimate)",
                    )
                }
                timed("countByKind", reps) {
                    val byKind = index.countByKind(EventQuery())
                    expect(byKind == mapOf(1 to n)) { "countByKind=$byKind, expected {1: $n}" }
                    byKind.values.sum()
                }

                // --- recency-planner A/B: same shapes, planner on vs off, ---
                // --- results must be byte-identical --------------------------
                println()
                println("recency planner A/B (planned vs unplanned, identical results asserted)")
                println(String.format("%-32s %12s %12s %9s", "shape", "planned p50", "baseline p50", "speedup"))
                val unplanned = VespaEventIndex(url, queryPlanning = false)
                try {
                    val shapes =
                        listOf(
                            "kind=1 limit=500 until=top" to EventQuery(kinds = listOf(1), limit = 500, until = top),
                            "kind=1 limit=10000 until=top" to EventQuery(kinds = listOf(1), limit = 10_000, until = top),
                            "no-kind limit=100 until=top" to EventQuery(limit = 100, until = top),
                            "kind=1 limit=500 (no until)" to EventQuery(kinds = listOf(1), limit = 500),
                        )
                    for ((label, q) in shapes) {
                        val a = index.search(q).map { it.id }
                        val b = unplanned.search(q).map { it.id }
                        expect(a == b) { "$label: planned and unplanned results DIFFER (${a.size} vs ${b.size} hits)" }
                        val planned = medianNanos(reps) { index.search(q).size }
                        val baseline = medianNanos(reps) { unplanned.search(q).size }
                        println(
                            String.format(
                                "%-32s %10.2fms %10.2fms %8.1fx",
                                label,
                                planned / 1e6,
                                baseline / 1e6,
                                baseline.toDouble() / planned,
                            ),
                        )
                    }
                } finally {
                    unplanned.close()
                }

                println()
                if (failures == 0) {
                    println("all shapes returned EXACTLY the expected sets/counts")
                } else {
                    error("$failures correctness failure(s) — see above")
                }
            } finally {
                index.close()
            }
        }

    /** Docs of author k in a corpus of n sequential docs (seq % 512 == k). */
    private fun countOfAuthor(
        k: Int,
        n: Int,
    ): Int = n / 512 + if (n % 512 > k) 1 else 0

    private fun Long.author(): String = (this % 512).toString(16).padStart(64, 'a')

    private fun Int.k(): String = if (this >= 1000) "${this / 1000}k" else toString()

    /** Fewer reps for shapes that move six figures of docs per call. */
    private fun Int.forSize(size: Int): Int = if (size >= 100_000) (this / 4).coerceAtLeast(3) else this

    private inline fun medianNanos(
        reps: Int,
        crossinline op: suspend () -> Int,
    ): Long =
        runBlocking {
            val lat = LongArray(reps) { measureNanoTime { op() } }
            lat.sort()
            lat[reps / 2]
        }

    private inline fun expect(
        ok: Boolean,
        msg: () -> String,
    ) {
        if (!ok) {
            failures++
            println("  !! ${msg()}")
        }
    }

    private inline fun timed(
        label: String,
        reps: Int,
        crossinline op: suspend () -> Int,
    ) = runBlocking {
        runCatching { op() } // warmup, and surface hard failures once
            .onFailure {
                failures++
                println(String.format("%-24s FAILED: %s", label, it.message?.take(120)))
                return@runBlocking
            }
        val lat = LongArray(reps)
        var checksum = 0L
        repeat(reps) { i ->
            lat[i] = measureNanoTime { checksum += op() }
        }
        lat.sort()
        println(
            String.format(
                "%-24s %8.2fms %8.2fms %8.2fms %12d",
                label,
                lat[reps / 2] / 1e6,
                lat[(reps * 9) / 10] / 1e6,
                lat[reps - 1] / 1e6,
                checksum,
            ),
        )
    }

    private object Header {
        fun print() = println(String.format("%-24s %10s %10s %10s %12s", "shape", "p50", "p90", "max", "Σresults"))
    }
}
