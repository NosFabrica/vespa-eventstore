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

import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

/**
 * ONE CLUSTER, TWO VERSIONS: the same corpus and read mix as `ShardScaleProbe` (relay-shaped
 * classes at concurrency 1 and 16), written only against APIs every version since the author-shard
 * work started has, so the identical file runs in a worktree of an older commit and in this one.
 * It is how the author-shard branch was held to "no cost at one cluster" (`docs/author-shards.md`).
 * Run each version against a FRESH Vespa, alternating, and compare the RESULT lines.
 *
 * Env: BENCH_VESPA_URL, BENCH_CONFIG_URL, BENCH_LABEL (printed), BENCH_CONTAINER (vespa-bench),
 * BENCH_EVENTS (400000), BENCH_PHASE_SECS (60).
 */
object OneClusterAbProbe {
    private const val AUTHORS = 40_000
    private const val SERVICES = 40
    private const val START = 1_700_000_000L
    private val WORDS = "bitcoin nostr lightning pizza coffee music art zap relay garden ocean bike chess climate photo film code linux rust kotlin".split(" ")

    private fun hex(seed: String): String = MessageDigest.getInstance("SHA-256").digest(seed.encodeToByteArray()).joinToString("") { "%02x".format(it) }

    private val authors = List(AUTHORS) { hex("author-$it") }
    private val services = List(SERVICES) { hex("service-$it") }

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val configUrl = System.getenv("BENCH_CONFIG_URL") ?: "http://127.0.0.1:19071"
            val container = System.getenv("BENCH_CONTAINER") ?: "vespa-bench"
            val events = System.getenv("BENCH_EVENTS")?.toInt() ?: 400_000
            val phaseSecs = System.getenv("BENCH_PHASE_SECS")?.toLong() ?: 60L

            val deployer = SchemaDeployer(configUrl)
            check(!deployer.isServing(url)) { "BENCH_VESPA_URL already serves an application: run against a fresh Vespa" }
            deployer.deployIfAbsent(url)
            println("deployed the bundled package")

            VespaEventIndex(url).use { index ->
                VespaReputationIndex(url).use { reputation -> load(index, reputation, events) }
                awaitCount(index, events)
                val idle = memory(container)
                warm(index, 20)
                val serial = mix(index, concurrency = 1, seconds = phaseSecs)
                val cpu = ConcurrentLinkedQueue<Double>()
                val sampler =
                    launch(Dispatchers.IO) {
                        while (true) {
                            cpuOf(container)?.let(cpu::add)
                            delay(5_000)
                        }
                    }
                val loaded = mix(index, concurrency = 16, seconds = phaseSecs)
                sampler.cancel()
                val busy = memory(container)
                println(
                    "RESULT " +
                        json(
                            "version" to (System.getenv("BENCH_LABEL") ?: "?"),
                            "events" to events,
                            "c1" to serial,
                            "c16" to loaded,
                            "cpuPctMeanC16" to (cpu.takeIf { it.isNotEmpty() }?.average() ?: -1.0),
                            "protonRssMbIdle" to idle,
                            "protonRssMbAfter" to busy,
                        ),
                )
            }
        }

    // ---- corpus --------------------------------------------------------------

    private suspend fun load(
        index: VespaEventIndex,
        reputation: VespaReputationIndex,
        events: Int,
    ) {
        val rnd = Random(42)
        val t0 = System.nanoTime()
        val noteIds = ArrayList<String>(events)
        (0 until events).chunked(2_000).forEach { chunk ->
            index.putAll(
                chunk.map { n ->
                    val author = authors[zipf(rnd)]
                    val id = hex("event-$n")
                    val created = START + n * 7L
                    val roll = rnd.nextInt(100)
                    when {
                        roll < 70 || noteIds.isEmpty() -> {
                            noteIds += id
                            val words = List(3 + rnd.nextInt(12)) { WORDS[rnd.nextInt(WORDS.size)] }
                            val tags =
                                buildList {
                                    if (rnd.nextInt(3) == 0) add(listOf("t", WORDS[rnd.nextInt(WORDS.size)]))
                                    if (rnd.nextInt(4) == 0) add(listOf("p", authors[zipf(rnd)]))
                                }
                            val text = words.joinToString(" ")
                            EventDoc(id, author, created, 1, tags, text, "e".repeat(128), search = SearchFields(text = text))
                        }

                        roll < 92 -> {
                            val target = noteIds[rnd.nextInt(noteIds.size)]
                            EventDoc(id, author, created, 7, listOf(listOf("e", target), listOf("p", authors[zipf(rnd)])), "+", "e".repeat(128))
                        }

                        else -> {
                            val target = noteIds[rnd.nextInt(noteIds.size)]
                            EventDoc(id, author, created, 6, listOf(listOf("e", target), listOf("p", authors[zipf(rnd)])), "", "e".repeat(128))
                        }
                    }
                },
            )
        }
        val fed = (System.nanoTime() - t0) / 1e9
        // Each service scores ~60% of authors: SERVICES x AUTHORS x 0.6 cells, as personalized keys would.
        val docs =
            authors.map { a ->
                val r = Random(a.hashCode())
                ReputationDoc(a, influenceScores = services.filter { r.nextInt(10) < 6 }.associate { ServiceKey(it) to r.nextInt(101) })
            }
        docs.chunked(2_000).forEach { reputation.putAll(it) }
        println("loaded $events events in %.0fs (%.0f/s) and ${docs.size} reputation docs (${docs.sumOf { it.influenceScores.size }} cells)".format(fed, events / fed))
    }

    /** Author index with a skew like a relay's: a few prolific authors, a long tail. */
    private fun zipf(rnd: Random): Int = (AUTHORS * Math.pow(rnd.nextDouble(), 3.0)).toInt().coerceIn(0, AUTHORS - 1)

    private suspend fun awaitCount(
        index: VespaEventIndex,
        expected: Int,
    ) {
        repeat(240) {
            if (index.count(EventQuery()) >= expected) return
            delay(1_000)
        }
        error("corpus never reached $expected")
    }

    // ---- the read mix ----------------------------------------------------------

    private val lens = services[0]

    /** One query per class, drawn from [rnd]; every class is a shape a relay serves. */
    private fun query(
        cls: String,
        rnd: Random,
    ): EventQuery =
        when (cls) {
            "global" -> EventQuery(kinds = listOf(1), limit = 50)
            "gatedGlobal" -> gated(EventQuery(kinds = listOf(1), limit = 50))
            "follow300" -> EventQuery(kinds = listOf(1, 6), authors = List(300) { authors[rnd.nextInt(AUTHORS)] }, limit = 50)
            "profile1" -> EventQuery(kinds = listOf(1), authors = listOf(authors[zipf(rnd)]), limit = 20)
            "hashtag" -> EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf(WORDS[rnd.nextInt(WORDS.size)])), limit = 50)
            "notifications" -> EventQuery(tags = mapOf("p" to listOf(authors[zipf(rnd)])), limit = 50)
            "search" -> gated(EventQuery(kinds = listOf(1), search = WORDS[rnd.nextInt(WORDS.size)], limit = 20, ranking = EventYql.RANK_SEARCH))
            "countGlobal" -> EventQuery(kinds = listOf(1))
            else -> error(cls)
        }

    private fun gated(q: EventQuery): EventQuery =
        q.copy(
            ranking = q.ranking ?: EventYql.RANK_RECENCY_GATED,
            minRank = 2.0,
            observer = lens,
            rankKey = lens,
            followersKey = lens,
        )

    private val CLASSES = listOf("global", "gatedGlobal", "follow300", "profile1", "hashtag", "notifications", "search", "countGlobal")

    private suspend fun run(
        index: VespaEventIndex,
        q: EventQuery,
        cls: String,
    ) {
        if (cls.startsWith("count")) index.count(q) else index.search(q)
    }

    private suspend fun warm(
        index: VespaEventIndex,
        rounds: Int,
    ) {
        val rnd = Random(7)
        repeat(rounds) { CLASSES.forEach { run(index, query(it, rnd), it) } }
    }

    /** Closed loop: [concurrency] clients, each cycling the classes, for [seconds]. Per class p50/p95 ms and completions; total q/s. */
    private suspend fun mix(
        index: VespaEventIndex,
        concurrency: Int,
        seconds: Long,
    ): Map<String, Any> {
        val lat = ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>>()
        val errors = AtomicLong()
        val deadline = System.nanoTime() + seconds * 1_000_000_000
        withContext(Dispatchers.IO) {
            List(concurrency) { c ->
                launch {
                    val rnd = Random(1_000 + c)
                    var i = c
                    while (System.nanoTime() < deadline) {
                        val cls = CLASSES[i++ % CLASSES.size]
                        val q = query(cls, rnd)
                        val t = System.nanoTime()
                        runCatching { run(index, q, cls) }.onFailure { errors.incrementAndGet() }.onSuccess { lat.getOrPut(cls) { ConcurrentLinkedQueue() } += System.nanoTime() - t }
                    }
                }
            }.forEach { it.join() }
        }
        val total = lat.values.sumOf { it.size }
        val perClass =
            CLASSES.associateWith { cls ->
                val v = lat[cls]?.sorted().orEmpty()
                if (v.isEmpty()) {
                    mapOf("n" to 0)
                } else {
                    mapOf("n" to v.size, "p50" to v[v.size / 2] / 1e6, "p95" to v[(v.size * 95) / 100] / 1e6)
                }
            }
        return mapOf("qps" to total.toDouble() / seconds, "errors" to errors.get(), "classes" to perClass)
    }

    // ---- the host ----------------------------------------------------------------

    /** Resident memory of each search process (vespa-proton-bin), MB. */
    private fun memory(container: String): List<Long> =
        sh("docker", "exec", container, "sh", "-c", "ps -C vespa-proton-bin -o rss=")
            .lines()
            .mapNotNull { it.trim().toLongOrNull() }
            .map { it / 1024 }
            .sorted()

    private fun cpuOf(container: String): Double? = sh("docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", container).trim().removeSuffix("%").toDoubleOrNull()

    private fun sh(vararg cmd: String): String =
        runCatching {
            val p = ProcessBuilder(*cmd).redirectErrorStream(true).start()
            p.inputStream
                .bufferedReader()
                .readText()
                .also { p.waitFor() }
        }.getOrDefault("")

    private fun json(vararg pairs: Pair<String, Any?>): String = value(pairs.toMap())

    private fun value(v: Any?): String =
        when (v) {
            null -> "null"
            is String -> "\"$v\""
            is Number, is Boolean -> v.toString()
            is Map<*, *> -> v.entries.joinToString(",", "{", "}") { (k, x) -> "\"$k\":${value(x)}" }
            is List<*> -> v.joinToString(",", "[", "]") { value(it) }
            else -> "\"$v\""
        }
}
