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
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * Query-plan debugging: ingest a corpus into a REAL Vespa, then for each named
 * REQ shape POST the store's OWN assembled YQL to `/search/` with Vespa's query
 * analyzer turned on — `trace.explainLevel` (the optimized blueprint, with a
 * per-term hit estimate) plus `presentation.timing` (querytime vs
 * summaryfetchtime). Prints the assembled YQL, the timing split, coverage, and
 * the blueprint tree, so we can see HOW each filter actually executes and
 * whether a big author/tag list compiled to one dictionary-backed operator or a
 * fat disjunction.
 *
 * Env: VESPA_URL, TRACE_SIZE (corpus, default 40000), TRACE_LEVEL (default 5).
 */
object TraceProbe {
    private val PRETTY = Json { prettyPrint = true }
    private val http = HttpClient.newBuilder().proxy(java.net.ProxySelector.of(null)).build()

    @JvmStatic
    fun main(args: Array<String>) {
        val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
        val size = System.getenv("TRACE_SIZE")?.toInt() ?: 40_000
        val level = System.getenv("TRACE_LEVEL")?.toInt() ?: 5
        val now = System.currentTimeMillis() / 1000

        VespaEventStore.open(url = url, autoDeploy = true).use { store ->
            val corpus = NostrCorpus.generate(NostrCorpus.Config(size = size, seed = 42))
            if (System.getenv("TRACE_SKIP_INGEST") == "1") {
                println("skipping ingest (TRACE_SKIP_INGEST=1) — assuming $url already holds the corpus")
            } else {
                println("ingesting ${corpus.size} events into $url ...")
                runBlocking { corpus.chunked(1_000).forEach { store.batchInsert(it) } }
            }
            val w = BenchWorkload.from(corpus)

            val shapes =
                listOf(
                    "author-timeline" to EventQuery(authors = listOf(w.author(0)), limit = 50, notExpiredAt = now),
                    "tag-mentions(p)" to EventQuery(kinds = listOf(1), tags = mapOf("p" to listOf(w.author(0))), limit = 50, notExpiredAt = now),
                    "follow-feed(a300)" to EventQuery(authors = w.authorList(0, 300), kinds = listOf(1, 6, 7), limit = 500, notExpiredAt = now),
                    "tag-list(p100)" to EventQuery(kinds = listOf(1, 7), tags = mapOf("p" to w.pList(0, 100)), limit = 300, notExpiredAt = now),
                    "contact-sync(a100)" to EventQuery(authors = w.authorList(0, 100), kinds = listOf(0, 3, 10002), limit = 300, notExpiredAt = now),
                )

            for ((name, q) in shapes) trace(url, name, q, level)
        }
    }

    private fun trace(
        baseUrl: String,
        name: String,
        q: EventQuery,
        level: Int,
    ) {
        val vq =
            EventYql.build(q) ?: run {
                println("\n#### $name -> match-nothing (no query)")
                return
            }
        val body =
            buildJsonObject {
                put("yql", vq.yql)
                put("hits", (q.limit ?: 100).toString())
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
                put("trace.level", level.toString())
                put("trace.explainLevel", "1")
                put("trace.timestamps", "true")
                put("presentation.timing", "true")
            }.toString()

        val req =
            HttpRequest
                .newBuilder(URI.create("$baseUrl/search/"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build()
        // Warm the engine: the first shot pays JIT + first-touch. Report the warm run.
        var root = Json.parseToJsonElement(http.send(req, HttpResponse.BodyHandlers.ofString()).body()).jsonObject
        val warmTimings = ArrayList<String>()
        repeat(6) {
            root = Json.parseToJsonElement(http.send(req, HttpResponse.BodyHandlers.ofString()).body()).jsonObject
            (root["timing"] as? JsonObject)?.let { warmTimings += it.toString() }
        }

        println("\n" + "=".repeat(78))
        println("#### $name")
        println("YQL:     ${vq.yql.take(180)}${if (vq.yql.length > 180) " …(${vq.yql.length} chars)" else ""}")
        println("ranking: ${vq.ranking}   params: ${vq.params.keys}")

        println("timing (cold→warm x6):")
        warmTimings.forEach { println("  $it") }
        (root["timing"] as? JsonObject)?.let { println("timing (warm): " + it.toString()) }
        (root["root"] as? JsonObject)?.get("coverage")?.let { println("coverage:" + it.toString()) }
        (root["root"] as? JsonObject)
            ?.get("fields")
            ?.jsonObject
            ?.get("totalCount")
            ?.let { println("totalCount: ${it.jsonPrimitive.content}") }

        // Pull every "message" string from the trace tree; the optimized blueprint
        // (with per-term hit estimates) arrives as one of these under explainLevel.
        val msgs = collectMessages(root["trace"])
        val plan = msgs.filter { m -> KEYWORDS.any { m.contains(it, ignoreCase = true) } }
        println("---- query execution plan (blueprint / plan messages) ----")
        (plan.ifEmpty { msgs }).forEach { println(it.take(4000)) }
    }

    /** Walk the trace tree, collecting every `message` value as text (objects/arrays get pretty-printed). */
    private fun collectMessages(node: JsonElement?): List<String> {
        val out = ArrayList<String>()

        fun textOf(e: JsonElement): String = (e as? kotlinx.serialization.json.JsonPrimitive)?.content ?: PRETTY.encodeToString(JsonElement.serializer(), e)

        fun walk(n: JsonElement?) {
            when (n) {
                is JsonObject -> {
                    n["message"]?.let { out += textOf(it) }
                    n.values.forEach { walk(it) }
                }

                is kotlinx.serialization.json.JsonArray -> {
                    n.forEach { walk(it) }
                }

                else -> {}
            }
        }
        walk(node)
        return out
    }

    private val KEYWORDS = listOf("blueprint", "optimized", "estimate", "query execution", "docsMatched", "WeightedSet", "WEIGHTEDSET", "InTerm", "IN[", "AND(", "OR(")
}
