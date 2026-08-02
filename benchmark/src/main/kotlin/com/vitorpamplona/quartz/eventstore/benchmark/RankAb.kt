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

import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import kotlinx.serialization.json.Json
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
import java.nio.file.Files
import java.nio.file.Path

/**
 * A/B a ranking change against a fixed set of `query -> expected doc id`
 * cases, on a RUNNING Vespa (staging port-forward, the docker IT container,
 * a local stack). Every knob in the schema's profiles is a rank-profile
 * `query()` input, so a candidate config is just extra
 * `ranking.features.query(...)` params on the request — NO deploy, NO
 * reindex, NO re-feed. Once a config wins, its values become the new
 * defaults in vespa/app/schemas/event.sd.
 *
 * The YQL is built by [EventYql] itself — imported, never reimplemented — so
 * what this sends is byte-identical to what the library sends in production.
 * That no-drift property is the whole reason this lives in this repo: an
 * inert `{prefix:true}userInput(...)` annotation survived for months
 * upstream precisely because nothing measured what the emitted query
 * actually did.
 *
 * ADD A CASE EVERY TIME SOMEONE REPORTS A BAD SEARCH — that is the point of
 * the fixed case file. Five cases demonstrate a problem; they are not enough
 * to tune on.
 *
 *     ./gradlew :benchmark:rankAb --args="--vespa http://localhost:8080"
 *     ./gradlew :benchmark:rankAb --args="--configs baseline,near_off --profile text"
 */
object RankAb {
    /**
     * Candidate configs: query() overrides layered on top of what the library
     * sends. `{}` = live behavior. Nothing here is written anywhere.
     */
    val CONFIGS: Map<String, Map<String, Double>> =
        mapOf(
            // Whatever the cluster's deployed schema defaults to.
            "baseline" to mapOf(),
            // The event.sd defaults as of the near-tier work. Use against a
            // cluster that has NOT been upgraded yet to preview the shipped
            // change. KEEP IN SYNC with vespa/app/schemas/event.sd.
            "shipped" to
                mapOf(
                    "w_name_tier" to 130000.0,
                    "w_near_tier" to 23000.0,
                    "w_weak_tier" to 4000.0,
                    "w_near_tier_text" to 700.0,
                    "w_weak_tier_text" to 620.0,
                    "w_pop_near_tier" to 100000000.0,
                    "w_pop_weak_tier" to 30000000.0,
                    "text_score_cutoff" to 100.0,
                    "w_words" to 60.0,
                    "w_exactness" to 40.0,
                    "w_wot_pow" to 2.7,
                ),
            // Individual levers, to attribute any movement.
            "near_off" to mapOf("w_near_tier" to 0.0, "w_near_tier_text" to 0.0, "w_pop_near_tier" to 0.0),
            "weak_off" to mapOf("w_weak_tier" to 0.0, "w_weak_tier_text" to 0.0, "w_pop_weak_tier" to 0.0),
            "precision_off" to mapOf("w_words" to 0.0, "w_exactness" to 0.0),
            "cutoff_50" to mapOf("text_score_cutoff" to 50.0),
            // Isolate the trust multiplier — the "is it ranking or is it data" test.
            "no_trust" to mapOf("w_wot" to 0.0),
            // Flatten the trust curve to linear deltas: crossing a rung then
            // needs a trust ratio past the rung ratio itself (~5.7x, ~236x for
            // bio->name) — the pre-curve "trust (almost) never crosses a tier"
            // feel, for attributing a movement to the curve exponent.
            "trust_linear" to mapOf("w_wot_pow" to 1.0),
        )

    @JvmStatic
    fun main(args: Array<String>) {
        val opts =
            args
                .toList()
                .chunked(2)
                .filter { it.size == 2 }
                .associate { it[0] to it[1] }
        val vespa = (opts["--vespa"] ?: "http://localhost:8080").trimEnd('/')
        val casesPath = opts["--cases"] ?: "benchmark/rank_cases.json"
        val profile = opts["--profile"] ?: "search"
        val observer = opts["--observer"]
        val hits = (opts["--hits"] ?: "100").toInt()
        val names = opts["--configs"]?.split(",") ?: CONFIGS.keys.toList()
        names.firstOrNull { it !in CONFIGS }?.let {
            System.err.println("unknown config '$it'; known: ${CONFIGS.keys}")
            return
        }

        val cases =
            Json
                .parseToJsonElement(Files.readString(Path.of(casesPath)))
                .jsonArray
                .map { it.jsonObject }
                .filter { (it["expect"]?.jsonPrimitive?.content).orEmpty().isNotEmpty() }
        if (cases.isEmpty()) {
            System.err.println("no cases with a non-empty `expect` in $casesPath — nothing to measure")
            return
        }

        println("target=$vespa profile=$profile observer=${observer ?: "none (pure text)"} depth=$hits cases=${cases.size}")
        val results = LinkedHashMap<String, List<Int?>>()
        for (config in names) {
            results[config] =
                cases.map { case ->
                    val ranked = search(vespa, case.getValue("query").jsonPrimitive.content, profile, observer, hits, CONFIGS.getValue(config))
                    ranked.indexOfFirst { it.startsWith(case.getValue("expect").jsonPrimitive.content) }.takeIf { it >= 0 }?.plus(1)
                }
        }

        val base = results.getValue(names.first())
        val labels = cases.map { "${it.getValue("query").jsonPrimitive.content}|${it.getValue("expect").jsonPrimitive.content.take(10)}" }
        val width = labels.maxOf { it.length }
        for ((config, row) in results) {
            println("\n--- $config${if (config == names.first()) "   (baseline)" else ""} ---")
            row.forEachIndexed { i, pos ->
                val delta =
                    when {
                        config == names.first() || pos == base[i] -> ""
                        base[i] == null -> "   <- FOUND"
                        pos == null -> "   <- LOST"
                        else -> "   <- ${if (pos < base[i]!!) "better" else "worse"} by ${Math.abs(pos - base[i]!!)}"
                    }
                println("  ${labels[i].padEnd(width)}  ${pos?.let { "#$it" } ?: "MISS"}$delta")
            }
            val found = row.filterNotNull()
            val mrr = found.sumOf { 1.0 / it } / row.size
            println("  ${"".padEnd(width)}  found ${found.size}/${row.size}   MRR ${"%.3f".format(mrr)}   median #${found.sorted().getOrNull(found.size / 2) ?: "-"}")
        }
        println("\nhigher MRR is better; MISS -> a number is the win that matters.")
    }

    /** One query with knob overrides; returns ranked doc ids. Same YQL the library sends, always. */
    private fun search(
        base: String,
        text: String,
        profile: String,
        observer: String?,
        hits: Int,
        overrides: Map<String, Double>,
    ): List<String> {
        val vq = EventYql.build(EventQuery(search = text, observer = observer, ranking = profile, minRank = observer?.let { 2.0 })) ?: return emptyList()
        val body =
            buildJsonObject {
                put("yql", vq.yql)
                put("hits", hits.toString())
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
                overrides.forEach { (k, v) -> put("ranking.features.query($k)", v.toString()) }
            }.toString()
        val resp =
            HTTP.send(
                HttpRequest
                    .newBuilder(URI.create("$base/search/"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        check(resp.statusCode() < 400) { "vespa ${resp.statusCode()}: ${resp.body().take(300)}" }
        return Json
            .parseToJsonElement(resp.body())
            .jsonObject["root"]
            ?.jsonObject
            ?.get("children")
            ?.jsonArray
            .orEmpty()
            .mapNotNull { ((it as? JsonObject)?.get("fields") as? JsonObject)?.get("id")?.let { id -> (id as? JsonPrimitive)?.content } }
    }

    private val HTTP: HttpClient = HttpClient.newHttpClient()
}
