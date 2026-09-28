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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.io.File
import java.security.MessageDigest
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

/**
 * The recency strategies UNDER LOAD, against an already-loaded Vespa:
 *
 *  - `throughput` (read-only): closed-loop clients at rising concurrency
 *    issuing a relay-shaped REQ mix — follow feeds, global feeds, hashtag
 *    feeds, notifications, deep pages; a share gated — for a fixed time per
 *    (variant, concurrency). Reports REQs/s, latency percentiles, engine
 *    queries per REQ, errors, and the engine container's CPU (docker stats).
 *  - `writes`: the same reads while a writer lands events at fixed rates on a
 *    clock advancing from the corpus's newest note, so the newest window moves
 *    under the reads the way a live relay's does. Authors are real pubkeys from
 *    the follow pool, so follow and gated feeds see the new events.
 *
 * THE WRITES ARE TEMPORARY. They go in at the engine level (no store, no trust
 * projection), every id is appended to [BENCH_IDS_FILE] BEFORE it is written,
 * and the run ends by removing every one and checking the corpus's document
 * count is back to where it started. A run that dies midway leaves the file;
 * `BENCH_CLEANUP_ONLY=1` removes whatever it lists.
 *
 * Env: BENCH_VESPA_URL, BENCH_NOW, BENCH_OBSERVER, BENCH_LENS_KEY, BENCH_MIN_RANK;
 * BENCH_SECTIONS (throughput,writes); BENCH_PHASE_SECS (45); BENCH_CONCS
 * ("8,32,64"); BENCH_WRITE_RATES ("0,50,200" events/s); BENCH_READ_CONC (32);
 * BENCH_CONTAINER (vespa); BENCH_IDS_FILE (build/recency-load-ids.txt);
 * BENCH_SEED (42).
 */
object RecencyLoadProbe {
    private const val DAY = 86_400L
    private const val STAGE = "recency.speculative" // VespaEventIndex.SPECULATIVE_STAGE (internal)

    private class Variant(
        val label: String,
        val client: VespaEventIndex,
        val ledger: CostLedger,
    ) {
        fun queries(): Long = ledger.snapshot().engine.sumOf { it.queries }

        /** Vespa's own split of the time it spent: (matching, summary fetch), nanos, cumulative. */
        fun engineSplit(): Pair<Long, Long> = ledger.snapshot().engine.let { s -> s.sumOf { it.engineNanos } to s.sumOf { it.summaryNanos } }
    }

    /** A REQ template: the query, and whether its `until` is a depth below the clock. */
    private class Template(
        val query: EventQuery,
        val depthDays: Long? = null,
        /** The REQ class for the tail breakdown: follow / global / tag / notif / deep, `GATED ` prefixed when lensed. */
        val label: String = "",
    )

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking<Unit> {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val idsFile = File(System.getenv("BENCH_IDS_FILE") ?: "build/recency-load-ids.txt")
            val cleanupOnly = System.getenv("BENCH_CLEANUP_ONLY") == "1"
            val admin = VespaEventIndex(url, recencyStrategy = RecencyStrategy.MATCH_PHASE, recencyMemory = false)
            if (cleanupOnly) {
                cleanup(admin, idsFile, expected = null)
                admin.close()
                kotlin.system.exitProcess(0)
            }
            require(!idsFile.exists() || idsFile.readText().isBlank()) { "$idsFile lists writes from an earlier run — run with BENCH_CLEANUP_ONLY=1 first" }

            val observer = System.getenv("BENCH_OBSERVER")?.lowercase()
            val lensKey = System.getenv("BENCH_LENS_KEY")?.lowercase() ?: observer
            val minRank = System.getenv("BENCH_MIN_RANK")?.toDoubleOrNull() ?: 2.0
            val sections = (System.getenv("BENCH_SECTIONS") ?: "throughput,writes").split(",").map { it.trim() }.toSet()
            val phaseSecs = System.getenv("BENCH_PHASE_SECS")?.toLongOrNull() ?: 45L
            val concs = (System.getenv("BENCH_CONCS") ?: "8,32,64").split(",").map { it.trim().toInt() }
            val rates = (System.getenv("BENCH_WRITE_RATES") ?: "0,50,200").split(",").map { it.trim().toInt() }
            val readConc = System.getenv("BENCH_READ_CONC")?.toIntOrNull() ?: 32
            val container = System.getenv("BENCH_CONTAINER") ?: "vespa"
            val seed = System.getenv("BENCH_SEED")?.toLongOrNull() ?: 42L

            val wall = System.currentTimeMillis() / 1000
            val end =
                System.getenv("BENCH_NOW")?.toLongOrNull()
                    ?: admin.search(EventQuery(kinds = listOf(1), until = wall, limit = 1, nowSecs = wall)).first().createdAt
            val byVolume =
                admin
                    .countByAuthor(EventQuery(kinds = listOf(1)))
                    .entries
                    .sortedByDescending { it.value }
                    .map { it.key }
            val templates = workload(byVolume, observer, lensKey, minRank, seed)
            println("corpus newest note $end; ${byVolume.size} authors; ${templates.size} REQ templates")

            fun variants() =
                listOf(
                    Triple("match_phase", RecencyStrategy.MATCH_PHASE, false),
                    Triple("speculative", RecencyStrategy.SPECULATIVE, false),
                    Triple("speculative+memory", RecencyStrategy.SPECULATIVE, true),
                ).map { (label, s, memory) -> CostLedger().let { Variant(label, VespaEventIndex(url, recencyStrategy = s, recencyMemory = memory, ledger = it), it) } }

            if ("throughput" in sections) {
                val vs = variants()
                println("\n== throughput: closed-loop, ${phaseSecs}s per phase, clock pinned at the corpus's newest note")
                header()
                // Warm the engine's caches on the shipped path, so no variant pays them alone.
                phase(vs[0], templates, 32, 20, { end }, container, seed, quiet = true)
                for ((i, conc) in concs.withIndex()) {
                    val order = if (i % 2 == 0) vs else vs.reversed()
                    for (v in order) phase(v, templates, conc, phaseSecs, { end }, container, seed + conc)
                }
                vs.forEach { it.client.close() }
            }

            if ("writes" in sections) {
                val baseline = admin.count(EventQuery())
                println("\n== writes: $readConc readers while events land at $rates ev/s; ${phaseSecs}s per phase; baseline $baseline documents")
                val t0 = System.nanoTime()
                // The live clock: the corpus's newest second, advancing in real time.
                val clock = { end + (System.nanoTime() - t0) / 1_000_000_000L }
                val vs = variants()
                val writer = VespaEventIndex(url, recencyStrategy = RecencyStrategy.MATCH_PHASE, recencyMemory = false)
                val written = AtomicLong()
                val pool = byVolume.take(20_000)
                val notified = byVolume.take(2_000)
                try {
                    header(writes = true)
                    for (rate in rates) {
                        for (v in vs) {
                            val w0 = written.get()
                            val putNanos = ConcurrentLinkedQueue<Long>()
                            coroutineScopeWith { scope ->
                                val writing =
                                    scope.launch(Dispatchers.Default) {
                                        if (rate > 0) writeLoop(writer, rate, clock, pool, notified, idsFile, written, putNanos, Random(seed + rate))
                                    }
                                phase(v, templates, readConc, phaseSecs, clock, container, seed + rate, writeRate = rate)
                                writing.cancel()
                            }
                            val landed = written.get() - w0
                            val puts = putNanos.sorted()
                            if (rate > 0) {
                                println(
                                    String.format(
                                        "%-20s %6s   writer: %d events landed (%.0f ev/s achieved), batch put p50 %.1f ms p99 %.1f ms",
                                        "",
                                        "",
                                        landed,
                                        landed.toDouble() / phaseSecs,
                                        puts.getOrElse(puts.size / 2) { 0L } / 1e6,
                                        puts.getOrElse(minOf(puts.size - 1, (puts.size * 99) / 100)) { 0L } / 1e6,
                                    ),
                                )
                            }
                        }
                    }
                    // Same page, with the writes in place and the writer stopped.
                    val now = clock()
                    val rnd = Random(seed)
                    var mismatches = 0
                    repeat(60) {
                        val q = stamp(templates[rnd.nextInt(templates.size)], now)
                        val pages = vs.map { v -> v.client.search(q).map { d -> d.id } }
                        if (pages.any { it != pages[0] }) mismatches++
                    }
                    println(if (mismatches == 0) "  same page: 60/60 sampled REQs identical across variants, writes included" else "  MISMATCH: $mismatches of 60 sampled REQs differ across variants")
                } finally {
                    vs.forEach { it.client.close() }
                    writer.close()
                    cleanup(admin, idsFile, expected = baseline)
                }
            }
            admin.close()
            kotlin.system.exitProcess(0)
        }

    // ---------------------------------------------------------------- workload

    private fun workload(
        byVolume: List<String>,
        observer: String?,
        lensKey: String?,
        minRank: Double,
        seed: Long,
    ): List<Template> {
        val rnd = Random(seed)
        val pool = byVolume.take(20_000)
        val gatedShare = if (observer != null) 0.3 else 0.0

        fun maybeGated(q: EventQuery) = if (rnd.nextDouble() < gatedShare) q.copy(ranking = EventYql.RANK_RECENCY_GATED, observer = observer, rankKey = lensKey, minRank = minRank) else q

        fun labeled(
            cls: String,
            q: EventQuery,
            depthDays: Long? = null,
        ) = maybeGated(q).let { Template(it, depthDays, (if (it.ranking == EventYql.RANK_RECENCY_GATED) "GATED " else "") + cls + " lim${it.limit}") }

        // 2,000 users, Zipf-popular: each has one follow list, re-asked on every visit.
        val users = (0 until 2_000).map { (0 until rnd.nextInt(50, 801)).map { pool[rnd.nextInt(pool.size)] }.distinct() }
        val weights = DoubleArray(users.size) { 1.0 / Math.pow((it + 1).toDouble(), 1.1) }
        val cumulative = weights.runningFold(0.0) { a, w -> a + w }.drop(1)
        val tags = listOf("bitcoin", "nostr", "news", "art", "music", "photography", "grownostr", "asknostr", "plebchain", "memes")
        return (0 until 50_000).map {
            when (rnd.nextInt(100)) {
                in 0 until 50 -> {
                    val x = rnd.nextDouble() * cumulative.last()
                    val u = cumulative.binarySearch(x).let { i -> if (i >= 0) i else -i - 1 }.coerceAtMost(users.size - 1)
                    labeled("follow", EventQuery(kinds = listOf(1, 6, 7), authors = users[u], limit = listOf(50, 100, 500)[u % 3]))
                }

                in 50 until 65 -> {
                    labeled("global", EventQuery(kinds = if (rnd.nextBoolean()) listOf(1) else listOf(1, 6, 7), limit = if (rnd.nextBoolean()) 50 else 100))
                }

                in 65 until 75 -> {
                    labeled("tag", EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf(tags[rnd.nextInt(tags.size)])), limit = 50))
                }

                in 75 until 90 -> {
                    labeled("notif", EventQuery(kinds = listOf(1, 6, 7, 9735), tags = mapOf("p" to listOf(byVolume[rnd.nextInt(2_000)])), limit = 100))
                }

                else -> {
                    labeled("deep", EventQuery(kinds = listOf(1), limit = 50), depthDays = rnd.nextLong(1, 366))
                }
            }
        }
    }

    /** What the store stamps on a REQ at clock [now]: the request clock, the expiry guard, and a deep page's `until`. */
    private fun stamp(
        t: Template,
        now: Long,
    ) = t.query.copy(nowSecs = now, notExpiredAt = now, until = t.depthDays?.let { now - it * DAY })

    // ---------------------------------------------------------------- one phase

    private fun header(writes: Boolean = false) =
        println(
            String.format(
                "%-20s %6s %8s %8s %8s %8s %8s %6s %7s %7s %7s  %s",
                "variant",
                if (writes) "ev/s" else "conc",
                "REQ/s",
                "p50 ms",
                "p95 ms",
                "p99 ms",
                "q/REQ",
                "errors",
                "cpu %",
                "match",
                "summary",
                "speculative outcomes (per REQ)",
            ),
        )

    private suspend fun phase(
        v: Variant,
        templates: List<Template>,
        conc: Int,
        secs: Long,
        clock: () -> Long,
        container: String,
        seed: Long,
        quiet: Boolean = false,
        writeRate: Int? = null,
    ) {
        val latencies = ConcurrentLinkedQueue<Long>()
        val byClass = java.util.concurrent.ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>>()
        val errors = AtomicInteger()
        val next = AtomicInteger(Random(seed).nextInt(templates.size))
        val q0 = v.queries()
        val (m0, f0) = v.engineSplit()
        val s0 = speculativeCalls()
        val cpu = ConcurrentLinkedQueue<Double>()
        val deadline = System.nanoTime() + secs * 1_000_000_000L
        coroutineScopeWith { scope ->
            val sampler = scope.launch(Dispatchers.IO) { while (isActive) sampleCpu(container)?.let { cpu += it } }
            val workers =
                (0 until conc).map {
                    scope.launch(Dispatchers.Default) {
                        while (System.nanoTime() < deadline) {
                            val t = templates[Math.floorMod(next.getAndIncrement(), templates.size)]
                            val t0 = System.nanoTime()
                            try {
                                v.client.search(stamp(t, clock()))
                                val took = System.nanoTime() - t0
                                latencies += took
                                byClass.computeIfAbsent(t.label) { ConcurrentLinkedQueue() } += took
                            } catch (e: CancellationException) {
                                throw e
                            } catch (_: Exception) {
                                errors.incrementAndGet()
                            }
                        }
                    }
                }
            workers.forEach { it.join() }
            sampler.cancel()
        }
        if (quiet) return
        val sorted = latencies.sorted()
        val n = sorted.size

        fun pct(p: Double) = if (n == 0) 0.0 else sorted[minOf(n - 1, (n * p).toInt())] / 1e6
        val outcomes =
            speculativeCalls()
                .minus(s0)
                .filterKeys { it != "" && it != ".attempt" }
                .entries
                .sortedByDescending { it.value }
                .joinToString(" ") { String.format("%s=%.2f", it.key.removePrefix("."), it.value.toDouble() / maxOf(n, 1)) }
        val (m1, f1) = v.engineSplit()
        println(
            String.format(
                "%-20s %6d %8.1f %8.1f %8.1f %8.1f %8.2f %6d %7.0f %7.1f %7.1f  %s",
                v.label,
                writeRate ?: conc,
                n.toDouble() / secs,
                pct(0.5),
                pct(0.95),
                pct(0.99),
                (v.queries() - q0).toDouble() / maxOf(n, 1),
                errors.get(),
                if (cpu.isEmpty()) Double.NaN else cpu.average(),
                (m1 - m0) / 1e6 / maxOf(n, 1),
                (f1 - f0) / 1e6 / maxOf(n, 1),
                outcomes,
            ),
        )
        // WHERE THE TAIL IS: each REQ class's share of the phase and its own
        // percentiles, heaviest p99 first, and how much of the phase's
        // slowest 1% it accounts for.
        if (System.getenv("BENCH_BREAKDOWN") == "1" && n > 0) {
            val cut = sorted[minOf(n - 1, (n * 0.99).toInt())]
            println(String.format("    %-24s %6s %8s %8s %8s %9s", "class", "share", "p50 ms", "p95 ms", "p99 ms", "of slow 1%"))
            byClass.entries
                .map { (label, qs) -> label to qs.sorted() }
                .sortedByDescending { (_, xs) -> xs[minOf(xs.size - 1, (xs.size * 0.99).toInt())] }
                .forEach { (label, xs) ->
                    fun p(q: Double) = xs[minOf(xs.size - 1, (xs.size * q).toInt())] / 1e6
                    println(String.format("    %-24s %5.1f%% %8.1f %8.1f %8.1f %8.1f%%", label, 100.0 * xs.size / n, p(0.5), p(0.95), p(0.99), 100.0 * xs.count { it >= cut } / maxOf(1, sorted.count { it >= cut })))
                }
        }
    }

    /** The engine container's CPU (percent of one core, docker's convention), or null without docker. */
    private suspend fun sampleCpu(container: String): Double? =
        withContext(Dispatchers.IO) {
            runCatching {
                val p = ProcessBuilder("docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", container).redirectErrorStream(true).start()
                val out =
                    p.inputStream
                        .bufferedReader()
                        .readText()
                        .trim()
                p.waitFor()
                out.removeSuffix("%").toDouble()
            }.getOrNull()
        }

    // ---------------------------------------------------------------- writes

    /**
     * Land events at [rate]/s, in 5 batches a second, dated by [clock] — the
     * newest events in the corpus, as a live relay's are. Every id reaches
     * [idsFile] before its put, so no write can escape the cleanup.
     */
    private suspend fun writeLoop(
        writer: VespaEventIndex,
        rate: Int,
        clock: () -> Long,
        pool: List<String>,
        notified: List<String>,
        idsFile: File,
        written: AtomicLong,
        putNanos: ConcurrentLinkedQueue<Long>,
        rnd: Random,
    ) {
        val perBatch = maxOf(1, rate / 5)
        val sha = MessageDigest.getInstance("SHA-256")
        var seq = 0L
        // Unique per CALL, not just per rate: every phase at one rate starts a
        // fresh loop, and a repeated id sequence would OVERWRITE the last
        // phase's events instead of landing new ones.
        val nonce = System.nanoTime()
        idsFile.parentFile?.mkdirs()
        while (true) {
            val tick = System.nanoTime()
            val now = clock()
            val batch =
                (0 until perBatch).map {
                    val id = sha.digest("recency-load-probe:$nonce:$rate:${seq++}:${rnd.nextLong()}".toByteArray()).joinToString("") { b -> "%02x".format(b) }
                    val kind = listOf(1, 1, 1, 1, 1, 1, 1, 7, 7, 6)[rnd.nextInt(10)]
                    val tags =
                        buildList {
                            if (kind != 1 || rnd.nextInt(4) == 0) add(listOf("p", notified[rnd.nextInt(notified.size)]))
                            if (kind == 1 && rnd.nextInt(5) == 0) add(listOf("t", "bitcoin"))
                            if (kind != 1) add(listOf("e", id.reversed()))
                        }
                    EventDoc(id = id, pubkey = pool[rnd.nextInt(pool.size)], createdAt = now, kind = kind, tags = tags, content = "recency load probe", sig = "")
                }
            idsFile.appendText(batch.joinToString("\n", postfix = "\n") { it.id })
            val t0 = System.nanoTime()
            writer.putAll(batch)
            putNanos += System.nanoTime() - t0
            written.addAndGet(batch.size.toLong())
            val spent = (System.nanoTime() - tick) / 1_000_000
            delay(maxOf(0L, 200L - spent))
        }
    }

    /** Remove every id [idsFile] lists, then check the corpus is back to [expected] documents. */
    private suspend fun cleanup(
        admin: VespaEventIndex,
        idsFile: File,
        expected: Int?,
    ) {
        if (!idsFile.exists()) return println("cleanup: nothing to remove")
        val ids = idsFile.readLines().filter { it.isNotBlank() }.distinct()
        ids.chunked(1_000).forEach { admin.removeAll(it) }
        val stillThere = ids.chunked(500).sumOf { chunk -> admin.search(EventQuery(ids = chunk)).size }
        val count = admin.count(EventQuery())
        // The id check is the real one: the corpus also shrinks on its own
        // while a run is in flight — NIP-40 expiry is a garbage-collection
        // selection on the wall clock (docs/server-side-constraints.md §1).
        println(
            "cleanup: removed ${ids.size} probe events; $stillThere still found by id; corpus now $count documents" +
                (expected?.let { if (it == count) " (= baseline)" else " (baseline was $it; ${it - count} fewer — expiry GC removes expired events on the wall clock)" } ?: ""),
        )
        if (stillThere == 0) idsFile.delete()
    }

    // ---------------------------------------------------------------- plumbing

    private suspend fun coroutineScopeWith(body: suspend (kotlinx.coroutines.CoroutineScope) -> Unit) = kotlinx.coroutines.coroutineScope { body(this) }

    private fun speculativeCalls(): Map<String, Long> =
        IngestStats
            .snapshot()
            .filterKeys { it.startsWith(STAGE) }
            .mapKeys { it.key.removePrefix(STAGE) }
            .mapValues { it.value.calls }

    private fun Map<String, Long>.minus(before: Map<String, Long>): Map<String, Long> = mapValues { (k, v) -> v - (before[k] ?: 0L) }.filterValues { it != 0L }
}
