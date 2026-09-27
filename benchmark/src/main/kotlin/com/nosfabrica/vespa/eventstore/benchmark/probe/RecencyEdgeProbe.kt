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
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlin.random.Random

/**
 * The EDGES of the speculative recency strategy and its window memory, against
 * an already-loaded Vespa (read-only). Where RecencyStrategyProbe times the
 * dominant shapes one at a time, this drives the shapes a benchmark of
 * dominant shapes flatters:
 *
 *  - `follows`: a relay-shaped workload — thousands of DISTINCT follow lists,
 *    Zipf-popular, some gated, concurrent — so the memory meets more shapes
 *    than it holds and a realistic repeat rate instead of a perfect one;
 *  - `walk`: `until` pagination walked page by page, and reads landing at
 *    random depths out to a year (the memory's age buckets);
 *  - `band`: limits around the match-phase band edge and past it, to 5000;
 *  - `trend`: a hashtag's burst, read by a memory that learned it quiet (and
 *    the reverse) — what a STALE entry costs before its TTL;
 *  - `future`: a far-future `until` on a real engine.
 *
 * Variants: `match_phase` (shipped), `speculative` (memory off: every read
 * cold) and `speculative+memory`. Every section checks that the variants serve
 * IDENTICAL pages and exits non-zero if any does not.
 *
 * Env: BENCH_VESPA_URL, BENCH_NOW, BENCH_OBSERVER, BENCH_LENS_KEY, BENCH_MIN_RANK
 * (as RecencyStrategyProbe); BENCH_SECTIONS (all); BENCH_USERS (4000),
 * BENCH_REQS (6000), BENCH_CONC (8), BENCH_GATED_SHARE (0.3); BENCH_SEED (42).
 */
object RecencyEdgeProbe {
    private const val DAY = 86_400L
    private const val STAGE = "recency.speculative" // VespaEventIndex.SPECULATIVE_STAGE (internal)

    private class Variant(
        val label: String,
        val client: VespaEventIndex,
        val ledger: CostLedger,
    ) {
        fun queries(): Long = ledger.snapshot().engine.sumOf { it.queries }
    }

    private var mismatches = 0

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking<Unit> {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val observer = System.getenv("BENCH_OBSERVER")?.lowercase()
            val lensKey = System.getenv("BENCH_LENS_KEY")?.lowercase() ?: observer
            val minRank = System.getenv("BENCH_MIN_RANK")?.toDoubleOrNull() ?: 2.0
            val sections = (System.getenv("BENCH_SECTIONS") ?: "follows,walk,band,trend,future").split(",").map { it.trim() }.toSet()
            val seed = System.getenv("BENCH_SEED")?.toLongOrNull() ?: 42L

            fun variant(
                label: String,
                strategy: RecencyStrategy,
                memory: Boolean,
            ) = CostLedger().let { Variant(label, VespaEventIndex(url, recencyStrategy = strategy, recencyMemory = memory, ledger = it), it) }

            fun freshVariants() =
                listOf(
                    variant("match_phase", RecencyStrategy.MATCH_PHASE, false),
                    variant("speculative", RecencyStrategy.SPECULATIVE, false),
                    variant("speculative+memory", RecencyStrategy.SPECULATIVE, true),
                )

            val probeClient = variant("reference", RecencyStrategy.MATCH_PHASE, false).client
            val wall = System.currentTimeMillis() / 1000
            val now =
                System.getenv("BENCH_NOW")?.toLongOrNull()
                    ?: probeClient.search(EventQuery(kinds = listOf(1), until = wall, limit = 1, nowSecs = wall)).first().createdAt
            println("query instant $now (${(wall - now) / DAY}d behind the wall clock)")
            val byVolume =
                probeClient
                    .countByAuthor(EventQuery(kinds = listOf(1)))
                    .entries
                    .sortedByDescending { it.value }
                    .map { it.key }
            println("authors ${byVolume.size}")

            fun req(q: EventQuery) = q.copy(nowSecs = q.nowSecs ?: now, notExpiredAt = q.nowSecs ?: now)

            fun gated(q: EventQuery) = q.copy(ranking = EventYql.RANK_RECENCY_GATED, observer = observer, rankKey = lensKey, minRank = minRank)

            // Fresh clients per section, so one section's memory cannot flatter the next.
            suspend fun section(body: suspend (List<Variant>) -> Unit) {
                val variants = freshVariants()
                try {
                    body(variants)
                } finally {
                    variants.forEach { it.client.close() }
                }
            }
            if ("follows" in sections) section { follows(it, byVolume, seed, observer != null, ::req, ::gated) }
            if ("walk" in sections) section { walk(it, byVolume, now, seed, observer != null, ::req, ::gated) }
            if ("band" in sections) section { band(it, byVolume, observer != null, ::req, ::gated) }
            if ("trend" in sections) trend(probeClient, url, now, ::req)
            if ("future" in sections) section { future(it, now, observer != null, ::req, ::gated) }

            probeClient.close()
            println()
            if (mismatches > 0) {
                System.err.println("$mismatches check(s) served DIFFERENT pages across variants")
                kotlin.system.exitProcess(1)
            }
            println("every check served the identical page under every variant")
            // The feed clients' dispatcher threads would otherwise keep the JVM up.
            kotlin.system.exitProcess(0)
        }

    // ---------------------------------------------------------------- follows

    private suspend fun follows(
        variants: List<Variant>,
        byVolume: List<String>,
        seed: Long,
        withGate: Boolean,
        req: (EventQuery) -> EventQuery,
        gated: (EventQuery) -> EventQuery,
    ) {
        val users = System.getenv("BENCH_USERS")?.toIntOrNull() ?: 4_000
        val reqs = System.getenv("BENCH_REQS")?.toIntOrNull() ?: 6_000
        val conc = System.getenv("BENCH_CONC")?.toIntOrNull() ?: 8
        val gatedShare = if (withGate) (System.getenv("BENCH_GATED_SHARE")?.toDoubleOrNull() ?: 0.3) else 0.0
        val rnd = Random(seed)
        val pool = byVolume.take(20_000)
        // Each user: a follow list of 50..800 authors, a page size, maybe the lens.
        val feeds =
            (0 until users).map {
                val follows = (0 until rnd.nextInt(50, 801)).map { pool[rnd.nextInt(pool.size)] }.distinct()
                val q = EventQuery(kinds = listOf(1, 6, 7), authors = follows, limit = listOf(50, 100, 500)[rnd.nextInt(3)])
                req(if (rnd.nextDouble() < gatedShare) gated(q) else q)
            }
        // Zipf(1.1) over users: a few reconnect constantly, the long tail once.
        val weights = DoubleArray(users) { 1.0 / Math.pow((it + 1).toDouble(), 1.1) }
        val cumulative = weights.runningFold(0.0) { a, w -> a + w }.drop(1)
        val total = cumulative.last()
        val sequence =
            (0 until reqs).map {
                val x = rnd.nextDouble() * total
                cumulative.binarySearch(x).let { i -> if (i >= 0) i else -i - 1 }.coerceAtMost(users - 1)
            }
        val distinct = sequence.toSet().size
        println("\n== follows: $users users, $reqs REQs ($distinct distinct follow lists touched), concurrency $conc, ${"%.0f".format(gatedShare * 100)}% gated")

        // Same page, on a sample.
        for (i in sequence.shuffled(Random(seed)).distinct().take(120)) checkSame("follows user $i", variants, feeds[i])

        // Warm the engine's caches once, on the shipped path, so no variant pays them alone.
        runConcurrent(sequence.take(500), conc) { variants[0].client.search(feeds[it]) }

        println(String.format("%-20s %9s %8s %8s %8s %8s  %s", "variant", "wall s", "p50 ms", "p95 ms", "p99 ms", "q/REQ", "speculative outcomes"))
        for (v in variants) {
            val q0 = v.queries()
            val s0 = speculativeCalls()
            val t0 = System.nanoTime()
            val lat = runConcurrent(sequence, conc) { i -> timed { v.client.search(feeds[i]) } }
            val wallS = (System.nanoTime() - t0) / 1e9
            val sorted = lat.sorted()

            fun pct(p: Double) = sorted[minOf(sorted.size - 1, (sorted.size * p).toInt())] / 1e6
            val outcomes = speculativeCalls().minus(s0).filterKeys { it != "" && it != ".attempt" }
            println(
                String.format(
                    "%-20s %9.1f %8.1f %8.1f %8.1f %8.2f  %s",
                    v.label,
                    wallS,
                    pct(0.5),
                    pct(0.95),
                    pct(0.99),
                    (v.queries() - q0).toDouble() / sequence.size,
                    outcomes.entries.sortedByDescending { it.value }.joinToString(" ") { "${it.key.removePrefix(".")}=${it.value}" },
                ),
            )
        }
    }

    // ---------------------------------------------------------------- walk

    private suspend fun walk(
        variants: List<Variant>,
        byVolume: List<String>,
        now: Long,
        seed: Long,
        withGate: Boolean,
        req: (EventQuery) -> EventQuery,
        gated: (EventQuery) -> EventQuery,
    ) {
        val follow300 = byVolume.subList(200, 500)
        val walks =
            listOfNotNull(
                Triple("global k1 lim500 x100 pages", EventQuery(kinds = listOf(1), limit = 500), 100),
                Triple("follow300 lim100 x100 pages", EventQuery(kinds = listOf(1, 6, 7), authors = follow300, limit = 100), 100),
                if (withGate) Triple("GATED global k1 lim100 x100 pages", gated(EventQuery(kinds = listOf(1), limit = 100)), 100) else null,
                if (withGate) Triple("GATED follow300 lim100 x60 pages", gated(EventQuery(kinds = listOf(1, 6, 7), authors = follow300, limit = 100)), 60) else null,
            )
        println("\n== walk: `until` pagination, page by page (total ms / engine queries / depth reached)")
        println(String.format("%-36s | %s", "walk", variants.joinToString(" | ") { String.format("%-26s", it.label) }))
        for ((name, shape, pages) in walks) {
            val results =
                variants.map { v ->
                    val q0 = v.queries()
                    val ids = ArrayList<String>()
                    var until: Long? = null
                    var oldest = now
                    val t0 = System.nanoTime()
                    repeat(pages) {
                        val page = v.client.search(req(shape.copy(until = until)))
                        if (page.isEmpty()) return@repeat
                        page.forEach { ids += it.id }
                        oldest = page.last().createdAt
                        until = oldest - 1
                    }
                    Triple((System.nanoTime() - t0) / 1e6, v.queries() - q0, ids to oldest)
                }
            val ref = results[0].third.first
            val same = results.all { it.third.first == ref }
            if (!same) {
                mismatches++
                System.err.println("  MISMATCH walk $name")
            }
            val depthDays = (now - results[0].third.second) / DAY.toDouble()
            println(
                String.format(
                    "%-36s | %s | %s depth %.1fd",
                    name,
                    results.joinToString(" | ") { (ms, q, _) -> String.format("%9.0f ms %5d q     ", ms, q) },
                    if (same) "same" else "DIFF",
                    depthDays,
                ),
            )
        }

        // Reads landing at random depths (a link, a search result, a jump).
        val rnd = Random(seed)
        val depths = (0 until 200).map { rnd.nextLong(1, 366) }
        val scattered =
            listOfNotNull(
                "global k1 lim50" to EventQuery(kinds = listOf(1), limit = 50),
                "follow300 lim100" to EventQuery(kinds = listOf(1, 6, 7), authors = follow300, limit = 100),
                if (withGate) "GATED global k1 lim50" to gated(EventQuery(kinds = listOf(1), limit = 50)) else null,
            )
        println("\n== walk: 200 reads at random depths 1..365 days (total ms / engine queries)")
        for ((name, shape) in scattered) {
            val results =
                variants.map { v ->
                    val q0 = v.queries()
                    val t0 = System.nanoTime()
                    val pages = depths.map { d -> v.client.search(req(shape.copy(until = now - d * DAY))).map { it.id } }
                    Triple((System.nanoTime() - t0) / 1e6, v.queries() - q0, pages)
                }
            val same = results.all { it.third == results[0].third }
            if (!same) {
                mismatches++
                System.err.println("  MISMATCH scattered $name")
            }
            println(String.format("%-36s | %s | %s", name, results.joinToString(" | ") { (ms, q, _) -> String.format("%9.0f ms %5d q     ", ms, q) }, if (same) "same" else "DIFF"))
        }
    }

    // ---------------------------------------------------------------- band

    private suspend fun band(
        variants: List<Variant>,
        byVolume: List<String>,
        withGate: Boolean,
        req: (EventQuery) -> EventQuery,
        gated: (EventQuery) -> EventQuery,
    ) {
        val limits = listOf(1_000, EventYql.MATCH_PHASE_BAND - 64, EventYql.MATCH_PHASE_BAND - 1, EventYql.MATCH_PHASE_BAND, EventYql.MATCH_PHASE_BAND + 1, 3_000, 5_000)
        val shapes =
            listOfNotNull(
                "global k1" to EventQuery(kinds = listOf(1)),
                "follow1000 k1/6/7" to EventQuery(kinds = listOf(1, 6, 7), authors = byVolume.subList(500, 1_500)),
                if (withGate) "GATED global k1" to gated(EventQuery(kinds = listOf(1))) else null,
            )
        println("\n== band: limits around the match-phase band (${EventYql.MATCH_PHASE_BAND}) and past it (median of 3 ms / engine queries)")
        println(String.format("%-28s %6s | %s", "shape", "limit", variants.joinToString(" | ") { String.format("%-20s", it.label) }))
        for ((name, shape) in shapes) {
            for (limit in limits) {
                val q = req(shape.copy(limit = limit))
                checkSame("band $name $limit", variants, q)
                val cells =
                    variants.map { v ->
                        val q0 = v.queries()
                        val times = (0 until 3).map { timed { v.client.search(q) } }.sorted()
                        String.format("%8.1f ms %5.1f q", times[1] / 1e6, (v.queries() - q0) / 3.0)
                    }
                println(String.format("%-28s %6d | %s", name, limit, cells.joinToString(" | ")))
            }
        }
    }

    // ---------------------------------------------------------------- trend

    private suspend fun trend(
        probe: VespaEventIndex,
        url: String,
        now: Long,
        req: (EventQuery) -> EventQuery,
    ) {
        // Find a burst: the busiest DAY of a busy hashtag, against its prior week.
        val candidates = listOf("bitcoin", "nostr", "news", "grownostr", "zapathon", "art", "photography", "music", "asknostr", "plebchain", "memes", "introductions", "gm", "foodstr", "btc")

        data class Burst(
            val tag: String,
            val day: Long,
            val count: Int,
            val priorPerDay: Double,
        )
        var best: Burst? = null
        for (tag in candidates) {
            for (d in 1L..60L) {
                val end = now - (d - 1) * DAY
                val day = probe.count(EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf(tag)), since = end - DAY, until = end))
                if (day < 150) continue
                val prior = probe.count(EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf(tag)), since = end - 8 * DAY, until = end - DAY)) / 7.0
                val b = Burst(tag, end, day, prior)
                if (best == null || day / maxOf(prior, 1.0) > best.count / maxOf(best.priorPerDay, 1.0)) best = b
            }
        }
        val burst = best ?: return println("\n== trend: no hashtag burst found in the last 60 days; skipped")
        println("\n== trend: #${burst.tag} — ${burst.count} notes in the day ending ${burst.day}, against ${"%.0f".format(burst.priorPerDay)}/day the week before")
        val shape = EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf(burst.tag)), limit = 100)
        val quiet = burst.day - DAY // the start of the burst day: the quiet week behind it
        val peak = burst.day

        suspend fun cost(
            client: VespaEventIndex,
            ledger: CostLedger,
            at: Long,
        ): Triple<Double, Long, List<String>> {
            val q0 = ledger.snapshot().engine.sumOf { it.queries }
            val t0 = System.nanoTime()
            // `until` = the simulated clock, not just `nowSecs`: the corpus runs on
            // past `at`, and a live relay at `at` would hold none of it.
            val ids = client.search(req(shape.copy(nowSecs = at, until = at))).map { it.id }
            return Triple((System.nanoTime() - t0) / 1e6, ledger.snapshot().engine.sumOf { it.queries } - q0, ids)
        }

        fun client(
            strategy: RecencyStrategy,
            memory: Boolean,
        ) = CostLedger().let { VespaEventIndex(url, recencyStrategy = strategy, recencyMemory = memory, ledger = it) to it }

        for ((learnAt, readAt, label) in listOf(Triple(quiet, peak, "learned QUIET, read at PEAK"), Triple(peak, peak + 7 * DAY, "learned PEAK, read a week later"))) {
            val (shipped, shippedLedger) = client(RecencyStrategy.MATCH_PHASE, false)
            val (cold, coldLedger) = client(RecencyStrategy.SPECULATIVE, false)
            val (stale, staleLedger) = client(RecencyStrategy.SPECULATIVE, true)
            val learned = cost(stale, staleLedger, learnAt) // teach the memory the other regime
            val s = cost(shipped, shippedLedger, readAt)
            val c = cost(cold, coldLedger, readAt)
            val m = cost(stale, staleLedger, readAt)
            val same = s.third == c.third && c.third == m.third
            if (!same) {
                mismatches++
                System.err.println("  MISMATCH trend $label")
            }
            println(
                String.format(
                    "  %-34s learn %5.1f ms %d q | shipped %6.1f ms %d q | cold %6.1f ms %d q | STALE memory %6.1f ms %d q | %s",
                    label,
                    learned.first,
                    learned.second,
                    s.first,
                    s.second,
                    c.first,
                    c.second,
                    m.first,
                    m.second,
                    if (same) "same" else "DIFF",
                ),
            )
            listOf(shipped, cold, stale).forEach { it.close() }
        }
    }

    // ---------------------------------------------------------------- future

    private suspend fun future(
        variants: List<Variant>,
        now: Long,
        withGate: Boolean,
        req: (EventQuery) -> EventQuery,
        gated: (EventQuery) -> EventQuery,
    ) {
        val farFuture = now + 100L * 365 * DAY
        val shapes =
            listOfNotNull(
                "global k1 lim50, until +100y" to EventQuery(kinds = listOf(1), until = farFuture, limit = 50),
                if (withGate) "GATED global k1 lim50, until +100y" to gated(EventQuery(kinds = listOf(1), until = farFuture, limit = 50)) else null,
            )
        println("\n== future: a far-future `until` (median of 5 ms / engine queries)")
        for ((name, shape) in shapes) {
            val q = req(shape)
            checkSame("future $name", variants, q)
            val cells =
                variants.map { v ->
                    val q0 = v.queries()
                    val times = (0 until 5).map { timed { v.client.search(q) } }.sorted()
                    String.format("%s %7.1f ms %4.1f q", v.label, times[2] / 1e6, (v.queries() - q0) / 5.0)
                }
            println(String.format("  %-36s %s", name, cells.joinToString(" | ")))
        }
    }

    // ---------------------------------------------------------------- plumbing

    private suspend fun checkSame(
        what: String,
        variants: List<Variant>,
        q: EventQuery,
    ) {
        val pages = variants.map { it.client.search(q).map { d -> d.id } }
        if (pages.any { it != pages[0] }) {
            mismatches++
            System.err.println("  MISMATCH $what: ${variants.zip(pages).joinToString { (v, p) -> "${v.label}=${p.size}" }}")
        }
    }

    private suspend inline fun timed(block: () -> Unit): Long {
        val t0 = System.nanoTime()
        block()
        return System.nanoTime() - t0
    }

    private suspend fun <T> runConcurrent(
        items: List<Int>,
        conc: Int,
        body: suspend (Int) -> T,
    ): List<T> =
        withContext(Dispatchers.Default) {
            val gate = Semaphore(conc)
            items.map { i -> async { gate.withPermit { body(i) } } }.awaitAll()
        }

    /** Calls under each `recency.speculative*` stage, keyed by suffix. Process-global: compare deltas. */
    private fun speculativeCalls(): Map<String, Long> =
        IngestStats
            .snapshot()
            .filterKeys { it.startsWith(STAGE) }
            .mapKeys { it.key.removePrefix(STAGE) }
            .mapValues { it.value.calls }

    private fun Map<String, Long>.minus(before: Map<String, Long>): Map<String, Long> = mapValues { (k, v) -> v - (before[k] ?: 0L) }.filterValues { it != 0L }
}
