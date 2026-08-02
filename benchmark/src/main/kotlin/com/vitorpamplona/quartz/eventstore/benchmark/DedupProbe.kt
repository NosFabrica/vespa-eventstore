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
import com.vitorpamplona.quartz.eventstore.vespa.mapBounded
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.random.Random
import kotlin.system.measureNanoTime

/**
 * The bulk-dedup existence check A/B (`./gradlew :benchmark:dedupProbe`): the
 * hottest query of a mirroring relay is "which of these [chunk] ids do I
 * already hold?" at ~99% hit rate (a relay that syncs the network re-sees the
 * network), and the current path answers it by fetching ~[chunk] FULL document
 * summaries to read one field off them. This probe measures, against a live
 * corpus the caller loaded (CorpusLoad):
 *
 *  1. per-variant latency, response bytes and Vespa's own querytime vs
 *     summaryfetchtime split (presentation.timing) for one 500-id chunk at 99%
 *     hit rate:
 *       prod        VespaEventIndex.search(EventQuery(ids)) — the production
 *                   baseline, full client decode included
 *       full        the same wire query raw (EventYql.build), full summary
 *       idonly      `select id` only, still the default summary class
 *       dedup-cls   `select id` + presentation.summary=dedup — the attribute-
 *                   only class event.sd now ships, served from memory
 *       grouping    `limit 0 | all(group(id) each(output(count())))` — matched
 *                   ids as group values, NO summary stage at all
 *       doc-gets    N parallel document-API gets (the sub-32-id fast path
 *                   scaled up — expected to lose at 500, and WRONG under
 *                   address-keying; measured to close the question)
 *  2. chunk-size and fan-out curves (aggregate ids/s) for the baseline and the
 *     winner;
 *  3. read starvation: p50/p99 of a small author-timeline REQ while each
 *     variant's dedup load runs flat out — the "ingest starves clients" cost.
 *
 * Env: VESPA_URL (http://localhost:8080), DEDUP_HIT_RATE (0.99), DEDUP_REPS
 * (60), DEDUP_CHUNKS ("100,250,500,1000,2000"), DEDUP_FANOUTS ("1,2,4,8,16"),
 * DEDUP_ID_POOL (ids sampled from the corpus walk, 400000).
 */
object DedupProbe {
    private val http = HttpClient.newBuilder().proxy(java.net.ProxySelector.of(null)).build()
    private val decoder = Json { ignoreUnknownKeys = true }

    @JvmStatic
    fun main(args: Array<String>) {
        val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
        val hitRate = System.getenv("DEDUP_HIT_RATE")?.toDoubleOrNull() ?: 0.99
        val reps = System.getenv("DEDUP_REPS")?.toIntOrNull() ?: 60
        val chunkSizes = (System.getenv("DEDUP_CHUNKS") ?: "100,250,500,1000,2000").split(",").mapNotNull { it.trim().toIntOrNull() }
        val fanouts = (System.getenv("DEDUP_FANOUTS") ?: "1,2,4,8,16").split(",").mapNotNull { it.trim().toIntOrNull() }
        val poolSize = System.getenv("DEDUP_ID_POOL")?.toIntOrNull() ?: 400_000

        val index = VespaEventIndex(url)
        runBlocking {
            // ---- the stored-id pool: sampled straight off the store, so every "hit" id is provably held ----
            val pool = ArrayList<String>(poolSize)
            index.visitIds(EventQuery()) { page ->
                page.forEach { if (pool.size < poolSize) pool.add(it.id) }
                pool.size < poolSize
            }
            println("id pool: ${pool.size} stored ids from $url (hit rate $hitRate)")
            require(pool.size >= 50_000) { "corpus too small — run CorpusLoad first" }
            val rnd = Random(7)
            var cursor = 0

            fun chunk(n: Int): List<String> {
                val hits = (n * hitRate).toInt()
                val out = ArrayList<String>(n)
                repeat(hits) {
                    out += pool[cursor]
                    cursor = (cursor + 1) % pool.size
                }
                // The ~1% novel band: valid 64-hex that provably isn't stored
                // (the corpus mints ids in band 0x1; 0xf never collides).
                repeat(n - hits) { out += "f%063x".format(rnd.nextLong().and(Long.MAX_VALUE)) }
                return out.shuffled(rnd)
            }

            val variants: List<Pair<String, suspend (List<String>) -> Answer>> =
                listOf(
                    "prod" to { ids -> Answer(index.search(EventQuery(ids = ids)).mapTo(HashSet()) { it.id }, -1, -1.0, -1.0) },
                    "prod-new" to { ids -> Answer(index.existingIds(ids), -1, -1.0, -1.0) },
                    "full" to { ids -> rawSearch(url, fullYql(ids), summaryClass = null) },
                    "idonly" to { ids -> rawSearch(url, idOnlyYql(ids), summaryClass = null) },
                    "dedup-cls" to { ids -> rawSearch(url, idOnlyYql(ids), summaryClass = "dedup") },
                    "grouping" to { ids -> rawGrouping(url, ids) },
                    "doc-gets" to { ids -> docGets(url, ids) },
                )

            // ---- 1. single-chunk anatomy at 500 ids ----
            println("\n== single 500-id chunk, ${(hitRate * 100).toInt()}% stored (reps=$reps, serial) ==")
            println(String.format("%-10s %10s %10s %10s %12s %12s %12s", "variant", "ms/chunk", "p95 ms", "hits", "bytes/chunk", "query ms", "summary ms"))
            val expect = HashMap<String, Long>()
            for ((name, run) in variants) {
                repeat(5) { run(chunk(500)) } // warm
                val lat = ArrayList<Double>(reps)
                var bytes = 0L
                var qms = 0.0
                var sms = 0.0
                var timed = 0
                var hits = 0L
                repeat(reps) {
                    val ids = chunk(500)
                    lateinit var a: Answer
                    val ns = measureNanoTime { a = runBlocking { run(ids) } }
                    lat += ns / 1e6
                    hits += a.present.size
                    if (a.bytes >= 0) bytes += a.bytes
                    if (a.queryMs >= 0) {
                        qms += a.queryMs
                        sms += a.summaryMs
                        timed++
                    }
                }
                lat.sort()
                expect[name] = hits
                println(
                    String.format(
                        "%-10s %10.2f %10.2f %10.1f %12s %12s %12s",
                        name,
                        lat.sum() / reps,
                        lat[(reps * 95) / 100 - 1],
                        hits.toDouble() / reps,
                        if (bytes > 0) "%,d".format(bytes / reps) else "-",
                        if (timed > 0) "%.2f".format(qms / timed) else "-",
                        if (timed > 0) "%.2f".format(sms / timed) else "-",
                    ),
                )
            }
            // Exactness: every variant must agree on hit count (same pool walk order per variant run).
            println("hit counts by variant: $expect  (must all be ~equal — exactness gate)")

            // ---- 2. throughput curves: chunk size × fan-out, aggregate ids/s ----
            for ((name, run) in variants.filter { it.first in setOf("full", "dedup-cls", "grouping") }) {
                println("\n== $name: aggregate throughput (ids/s), chunk-size × fan-out ==")
                println(String.format("%8s %s", "chunk", fanouts.joinToString("") { String.format("%12s", "f=$it") }))
                for (c in chunkSizes) {
                    val cells =
                        fanouts.map { f ->
                            val work = List(((60_000 / c).coerceAtLeast(f * 4)).coerceAtMost(400)) { chunk(c) }
                            val ns = measureNanoTime { runBlocking { work.mapBounded(f) { run(it) } } }
                            (work.size.toLong() * c * 1_000_000_000L / ns)
                        }
                    println(String.format("%8d %s", c, cells.joinToString("") { String.format("%,12d", it) }))
                }
            }

            // ---- 3. read starvation: REQ latency under full-tilt dedup load ----
            println("\n== author-timeline REQ latency (ms) while dedup runs flat out (fanout=4, chunk=500) ==")
            val someAuthor = index.search(EventQuery(kinds = listOf(1), limit = 1)).first().pubkey

            suspend fun reqLatency(): Pair<Double, Double> {
                val l = ArrayList<Double>(40)
                repeat(40) {
                    val ns = measureNanoTime { runBlocking { index.search(EventQuery(authors = listOf(someAuthor), limit = 50)) } }
                    l += ns / 1e6
                    delay(25)
                }
                l.sort()
                return l[l.size / 2] to l[(l.size * 99) / 100 - 1]
            }
            run {
                val (p50, p99) = reqLatency()
                println(String.format("%-10s p50 %8.1f  p99 %8.1f   (idle baseline)", "idle", p50, p99))
            }
            for ((name, run) in variants.filter { it.first in setOf("full", "dedup-cls", "grouping") }) {
                val load: Job =
                    launch {
                        while (isActive) {
                            List(4) { chunk(500) }.mapBounded(4) { run(it) }
                        }
                    }
                delay(500)
                val (p50, p99) = reqLatency()
                load.cancelAndJoin()
                println(String.format("%-10s p50 %8.1f  p99 %8.1f", name, p50, p99))
            }
        }
        index.close()
    }

    private class Answer(
        val present: Set<String>,
        val bytes: Int,
        val queryMs: Double,
        val summaryMs: Double,
    )

    private fun fullYql(ids: List<String>): String = "select ${EventYql.SUMMARY_FIELDS} from event where ${idIn(ids)} order by created_at desc"

    private fun idOnlyYql(ids: List<String>): String = "select id from event where ${idIn(ids)}"

    private fun idIn(ids: List<String>): String = "id in (${ids.joinToString(", ") { "\"$it\"" }})"

    private fun post(
        url: String,
        body: JsonObject,
    ): String {
        val req =
            HttpRequest
                .newBuilder(URI.create("$url/search/"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build()
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        require(resp.statusCode() < 400) { "search ${resp.statusCode()}: ${resp.body().take(300)}" }
        return resp.body()
    }

    /** Timing split off `presentation.timing`: engine querytime vs summaryfetchtime, in ms. */
    private fun rawSearch(
        url: String,
        yql: String,
        summaryClass: String?,
    ): Answer {
        val body =
            buildJsonObject {
                put("yql", yql)
                put("hits", Int.MAX_VALUE.toString())
                put("ranking", EventYql.RANK_UNRANKED)
                put("presentation.timing", "true")
                if (summaryClass != null) put("presentation.summary", summaryClass)
            }
        val text = post(url, body)
        val root = decoder.parseToJsonElement(text).jsonObject
        requireFullCoverage(root)
        val timing = root["timing"]?.jsonObject
        val present =
            root["root"]!!
                .jsonObject["children"]
                ?.jsonArray
                ?.mapNotNullTo(HashSet()) {
                    it.jsonObject["fields"]
                        ?.jsonObject
                        ?.get("id")
                        ?.jsonPrimitive
                        ?.content
                }.orEmpty()
        return Answer(
            present,
            text.length,
            (
                timing
                    ?.get("querytime")
                    ?.jsonPrimitive
                    ?.content
                    ?.toDoubleOrNull() ?: -1.0
            ) * 1000,
            (
                timing
                    ?.get("summaryfetchtime")
                    ?.jsonPrimitive
                    ?.content
                    ?.toDoubleOrNull() ?: -1.0
            ) * 1000,
        )
    }

    /** Matched ids as GROUP VALUES — the summary stage never runs (`limit 0`). */
    private fun rawGrouping(
        url: String,
        ids: List<String>,
    ): Answer {
        val body =
            buildJsonObject {
                put("yql", "select * from event where ${idIn(ids)} limit 0 | all(group(id) each(output(count())))")
                put("hits", "0")
                put("ranking", EventYql.RANK_UNRANKED)
                put("presentation.timing", "true")
                put("grouping.defaultMaxGroups", "-1")
                put("grouping.defaultMaxHits", "-1")
            }
        val text = post(url, body)
        val root = decoder.parseToJsonElement(text).jsonObject
        requireFullCoverage(root)
        val timing = root["timing"]?.jsonObject
        val present = HashSet<String>()

        fun collect(node: JsonElement) {
            when (node) {
                is JsonObject -> {
                    if ((node["id"] as? JsonPrimitive)?.content?.startsWith("group:string:") == true) {
                        node["value"]?.jsonPrimitive?.content?.let { present += it }
                    }
                    node["children"]?.let { collect(it) }
                }

                is JsonArray -> {
                    node.forEach { collect(it) }
                }

                else -> {}
            }
        }
        collect(root["root"]!!)
        return Answer(
            present,
            text.length,
            (
                timing
                    ?.get("querytime")
                    ?.jsonPrimitive
                    ?.content
                    ?.toDoubleOrNull() ?: -1.0
            ) * 1000,
            (
                timing
                    ?.get("summaryfetchtime")
                    ?.jsonPrimitive
                    ?.content
                    ?.toDoubleOrNull() ?: -1.0
            ) * 1000,
        )
    }

    /** The ≤32-id fast path scaled to a whole chunk: one document-API get per id, 32 in flight. */
    private suspend fun docGets(
        url: String,
        ids: List<String>,
    ): Answer {
        var bytes = 0
        val present =
            ids
                .mapBounded(32) { id ->
                    val req = HttpRequest.newBuilder(URI.create("$url/document/v1/event/event/docid/$id")).GET().build()
                    val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
                    bytes += resp.body().length
                    if (resp.statusCode() == 200) id else null
                }.filterNotNullTo(HashSet())
        return Answer(present, bytes, -1.0, -1.0)
    }

    private fun requireFullCoverage(root: JsonObject) {
        val cov =
            root["root"]!!
                .jsonObject["coverage"]
                ?.jsonObject
                ?.get("full")
                ?.jsonPrimitive
                ?.content
        require(cov != "false") { "degraded coverage on the dedup probe — result would be a wrong answer" }
    }
}
