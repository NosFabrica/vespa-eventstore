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

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
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
 * defaults in engine/app/schemas/event.sd.
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
            // change. KEEP IN SYNC with engine/app/schemas/event.sd.
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
                    "w_words_pop" to 3000.0,
                    "w_exactness_pop" to 6000.0,
                    "w_perfect_pop" to 80000.0,
                    "w_order_floor" to 0.5,
                    "w_wot_pow" to 2.7,
                    "w_recency" to 0.1,
                    "recency_halflife" to 30.0,
                ),
            // Individual levers, to attribute any movement.
            "near_off" to mapOf("w_near_tier" to 0.0, "w_near_tier_text" to 0.0, "w_pop_near_tier" to 0.0),
            "weak_off" to mapOf("w_weak_tier" to 0.0, "w_weak_tier_text" to 0.0, "w_pop_weak_tier" to 0.0),
            // Both weight pairs: the text profiles read w_words/w_exactness,
            // `search` reads the _pop pair (different scale — see event.sd).
            "precision_off" to
                mapOf("w_words" to 0.0, "w_exactness" to 0.0, "w_words_pop" to 0.0, "w_exactness_pop" to 0.0, "w_perfect_pop" to 0.0),
            // The perfect-match rung alone — the lever the 2026-08-05 reports
            // turn. Sweep it against the odell crossing bound, whose ceiling
            // (98306) was MEASURED on a live Vespa, not derived on paper.
            "perfect_off" to mapOf("w_perfect_pop" to 0.0),
            // Word order off — what the default profile did before the audit.
            "order_off" to mapOf("w_order_floor" to 1.0),
            // ---- §12.2 the split rung + the perfect exponent ----
            // `pre_split` is the ranking as it shipped BEFORE them: the token
            // band was one rung whether a doc answered the query in one field
            // or spread it across a name and a bio, and the perfect-match rung
            // was linear. Read every candidate against this row.
            "pre_split" to mapOf("w_split_tier" to 130000.0, "w_split_tier_text" to 1100.0, "w_perfect_pow" to 1.0),
            "split_off" to mapOf("w_split_tier" to 130000.0, "w_split_tier_text" to 1100.0),
            "perfect_pow_off" to mapOf("w_perfect_pow" to 1.0),
            // Where the split rung could sit instead: half a rung down
            // (60000), or all the way onto the weak rung (4000, which the
            // synthetic shapes put four orders under a title match).
            "split_60k" to mapOf("w_split_tier" to 60000.0),
            "split_weak" to mapOf("w_split_tier" to 4000.0),
            // §12.3, shipped INERT: a band above the token rung for a field
            // that IS a multi-word query. Sweep these against the whole case
            // table before shipping a value — a rung above the token band
            // re-prices every crossing beneath it.
            "perfect_tier_130k" to mapOf("w_perfect_tier" to 130000.0, "w_perfect_tier_text" to 1100.0),
            "perfect_tier_400k" to mapOf("w_perfect_tier" to 400000.0, "w_perfect_tier_text" to 3400.0),
            "perfect_tier_735k" to mapOf("w_perfect_tier" to 735000.0, "w_perfect_tier_text" to 6200.0),
            "perfect_max" to mapOf("w_perfect_pop" to 98000.0),
            "cutoff_50" to mapOf("text_score_cutoff" to 50.0),
            // Isolate the trust multiplier — the "is it ranking or is it data" test.
            "no_trust" to mapOf("w_wot" to 0.0),
            // Flatten the trust curve to linear deltas: crossing a rung then
            // needs a trust ratio past the rung ratio itself (~5.7x, ~236x for
            // bio->name) — the pre-curve "trust (almost) never crosses a tier"
            // feel, for attributing a movement to the curve exponent.
            "trust_linear" to mapOf("w_wot_pow" to 1.0),
            // ---- RECENCY (docs/recency-ranking.md) ----
            // event.sd ships w_recency 0.1, so `recency_off` is a real
            // control now: it is what the ranking looked like before recency
            // existed, and the row every candidate is read against.
            //
            // READ THE AGE COLUMN, not just MRR. These configs are supposed to
            // move the median age of the top hits and NOT move the calibrated
            // positions — the ladder cases (odell, amethyst, the Vitor prefix
            // ladder) are exactly the ones that must hold still. A config that
            // buys age by losing a pinned position is a losing config, however
            // fresh the results look.
            "recency_off" to mapOf("w_recency" to 0.0),
            // The MEASURED band (2026-08-22, 4.6k-event staging slice, see
            // benchmark/README.md): within a match band the trust curve
            // SATURATES — a page of trust-100 authors scores within 0.001% of
            // itself — so a 1% tilt already re-sorts it. w 0.05..0.1 takes the
            // median top-10 body-band age from ~1000d to 0..30d while the
            // median trust stays 98..100; from 0.25 up the page is same-day
            // and trust starts slipping. The candidates to sweep on real
            // staging are therefore these three, NOT the 0.5..2 the paper
            // calibration suggested.
            "recency_h30_w005" to mapOf("w_recency" to 0.05, "recency_halflife" to 30.0),
            "recency_h30_w01" to mapOf("w_recency" to 0.1, "recency_halflife" to 30.0),
            "recency_h30_w025" to mapOf("w_recency" to 0.25, "recency_halflife" to 30.0),
            "recency_h90_w01" to mapOf("w_recency" to 0.1, "recency_halflife" to 90.0),
            "recency_h30_w05" to mapOf("w_recency" to 0.5, "recency_halflife" to 30.0),
            "recency_h30_w1" to mapOf("w_recency" to 1.0, "recency_halflife" to 30.0),
            "recency_h30_w2" to mapOf("w_recency" to 2.0, "recency_halflife" to 30.0),
            "recency_h7_w1" to mapOf("w_recency" to 1.0, "recency_halflife" to 7.0),
            "recency_h365_w1" to mapOf("w_recency" to 1.0, "recency_halflife" to 365.0),
            // The paper ceiling, to SEE the failure the ceiling describes:
            // 1 + w = 5.65 is the ladder's smallest rung ratio, so this is
            // where a fresh bio mention starts overtaking a real name match.
            // Never a candidate — a demonstration.
            "recency_ceiling" to mapOf("w_recency" to 4.65, "recency_halflife" to 30.0),
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
        // The instant recency is measured against. Pinning it makes a sweep
        // reproducible across days (and lets one be replayed at the moment a
        // bad ranking was reported); omitted, EventYql stamps the wall clock.
        val nowSecs = opts["--now"]?.toLong()
        // Restrict recall to these kinds. The default (none) is what the
        // ladder cases want — a profile search reaches kind 0 above
        // everything. A RECENCY sweep wants `--kinds 1`: a mixed page is
        // profiles and titled notes, whose ages barely differ, so the weight
        // reads as inert when it is merely measuring the wrong page (see
        // docs/recency-ranking.md §4.3).
        val kinds =
            opts["--kinds"]
                ?.split(",")
                ?.map {
                    it.trim().toIntOrNull() ?: run {
                        System.err.println("--kinds takes a comma-separated list of integers; got '$it'")
                        return
                    }
                }.orEmpty()
        val names = opts["--configs"]?.split(",") ?: CONFIGS.keys.toList()
        names.firstOrNull { it !in CONFIGS }?.let {
            System.err.println("unknown config '$it'; known: ${CONFIGS.keys}")
            return
        }

        // ---- --dump: ONE query, the whole page, per config ----------------
        // The case table answers "did the pinned doc move"; this answers "what
        // is on the page and why it is there" — kind, name, the band
        // (text_score) and `event.sd` §12.2's scattered/coverage flags — which
        // is the reading a ranking change is argued from. No expected doc, no
        // MRR: just the page, side by side, from the same request the library
        // sends.
        opts["--dump"]?.let { text ->
            for (config in names) {
                println("\n=== $config === $text")
                dumpPage(search(vespa, buildJsonObject { put("query", text) }, profile, observer, hits, CONFIGS.getValue(config), nowSecs, kinds))
            }
            return
        }

        val cases =
            Json
                .parseToJsonElement(Files.readString(Path.of(casesPath)))
                .jsonArray
                .map { it.jsonObject }
                // `skip` opts a row out of BOTH columns — the file's TEMPLATE
                // row is documentation, and once empty-`expect` rows started
                // being measured for age it was being queried live and folded
                // into the headline median.
                .filter { (it["query"]?.jsonPrimitive?.content).orEmpty().isNotBlank() && it["skip"]?.jsonPrimitive?.content != "true" }
        // A case with an empty `expect` names no winning document, so it scores
        // no position — but it still has a PAGE, and the age of that page is a
        // measurement. Those rows ride along age-only (they were skipped
        // outright before the recency work) and stay out of found/MRR, which
        // are claims about positions.
        val expects = cases.map { (it["expect"]?.jsonPrimitive?.content).orEmpty().takeIf(String::isNotEmpty) }
        if (cases.isEmpty()) {
            System.err.println("no cases with a query in $casesPath — nothing to measure")
            return
        }

        println(
            "target=$vespa profile=$profile observer=${observer ?: "none (pure text)"} depth=$hits cases=${cases.size} " +
                "kinds=${kinds.takeIf { it.isNotEmpty() }?.joinToString(",") ?: "all"} now=${nowSecs?.toString() ?: "wall clock"}",
        )
        val clock = nowSecs ?: (System.currentTimeMillis() / 1000)
        val results = LinkedHashMap<String, List<Int?>>()
        // Median age, in days, of each case's top-[AGE_DEPTH] hits — the thing
        // a recency config is actually FOR, and invisible in a position table.
        val ages = LinkedHashMap<String, List<Double?>>()
        for (config in names) {
            val ranked = cases.map { search(vespa, it, profile, observer, hits, CONFIGS.getValue(config), nowSecs, kinds) }
            results[config] =
                ranked.mapIndexed { i, hitsOf ->
                    expects[i]?.let { want -> hitsOf.indexOfFirst { it.id.startsWith(want) }.takeIf { it >= 0 }?.plus(1) }
                }
            ages[config] = ranked.map { medianAgeDays(it.take(AGE_DEPTH), clock) }
        }

        val base = results.getValue(names.first())
        val labels = cases.mapIndexed { i, case -> "${case.getValue("query").jsonPrimitive.content}|${expects[i]?.take(10) ?: "age-only"}" }
        val width = labels.maxOf { it.length }
        val baseAges = ages.getValue(names.first())
        for ((config, row) in results) {
            println("\n--- $config${if (config == names.first()) "   (baseline)" else ""} ---")
            row.forEachIndexed { i, pos ->
                val delta =
                    when {
                        expects[i] == null -> ""
                        config == names.first() || pos == base[i] -> ""
                        base[i] == null -> "   <- FOUND"
                        pos == null -> "   <- LOST"
                        else -> "   <- ${if (pos < base[i]!!) "better" else "worse"} by ${Math.abs(pos - base[i]!!)}"
                    }
                val age = ages.getValue(config)[i]
                val ageDelta = baseAges[i]?.let { b -> age?.let { a -> if (config == names.first()) "" else " (${"%+.0f".format(a - b)})" } } ?: ""
                val position = if (expects[i] == null) "-" else pos?.let { "#$it" } ?: "MISS"
                println("  ${labels[i].padEnd(width)}  ${position.padEnd(6)}${delta.padEnd(16)}  age ${age?.let { "%.0fd".format(it) } ?: "-"}$ageDelta")
            }
            val scored = row.filterIndexed { i, _ -> expects[i] != null }
            val found = scored.filterNotNull()
            val mrr = if (scored.isEmpty()) 0.0 else found.sumOf { 1.0 / it } / scored.size
            val ageRow = ages.getValue(config).filterNotNull().sorted()
            println(
                "  ${"".padEnd(width)}  found ${found.size}/${scored.size}   MRR ${"%.3f".format(mrr)}   " +
                    "median #${found.sorted().getOrNull(found.size / 2) ?: "-"}   " +
                    "median top-$AGE_DEPTH age ${ageRow.getOrNull(ageRow.size / 2)?.let { "%.0fd".format(it) } ?: "-"}",
            )
        }
        println("\nhigher MRR is better; MISS -> a number is the win that matters.")
        println("age = median created_at age of the top-$AGE_DEPTH hits: what a recency config buys, and what it must not")
        println("pay for in positions — a config that trades a pinned case for fresher results has lost.")
    }

    /**
     * What to call a hit on a dumped page. The schema's summary carries the
     * LOSSLESS NIP-01 fields, not the derived search columns (see event.sd), so
     * the readable name comes back out of the event itself: a `title` tag for
     * the kinds that have one, the profile name for a kind 0. Best-effort by
     * design — this labels a diagnostic page, it does not parse events.
     */
    private fun labelOf(
        kind: String?,
        tags: String?,
        content: String?,
    ): String? {
        val title =
            tags
                ?.let { runCatching { Json.parseToJsonElement(it).jsonArray }.getOrNull() }
                ?.map { it.jsonArray.map { v -> v.jsonPrimitive.content } }
                ?.firstOrNull { it.size > 1 && (it[0] == "title" || it[0] == "name") }
                ?.get(1)
        if (title != null) return title
        if (kind != "0") return null
        val profile = content?.let { runCatching { Json.parseToJsonElement(it).jsonObject }.getOrNull() }
        return (profile?.get("display_name") ?: profile?.get("name"))?.jsonPrimitive?.content
    }

    /** One page, labelled: what each hit is and which rung it arrived on. */
    private fun dumpPage(page: List<Hit>) {
        val top = page.firstOrNull()?.relevance ?: 0.0
        println("  %-4s %-30s %13s %6s %9s %5s %5s %7s".format("kind", "name/title", "relevance", "ratio", "text", "cov", "scat", "perfect"))
        page.forEach {
            println(
                "  %-4s %-30s %13.0f %6.3f %9.0f %5.2f %5.0f %7.3f".format(
                    it.kind,
                    it.label.take(30),
                    it.relevance,
                    if (top > 0) it.relevance / top else 0.0,
                    it.textScore,
                    it.coverage,
                    it.scattered,
                    it.perfect,
                ),
            )
        }
    }

    /** How deep the age column looks — a page of results, not the whole recall. */
    private const val AGE_DEPTH = 10

    /**
     * One ranked hit: the id a case matches on, the timestamp the age column
     * reads, and — for [dumpPage] — the WHY: what the doc is, which band it
     * landed in, and whether one field answered the query or several shared it
     * (`event.sd` §12.2).
     */
    private data class Hit(
        val id: String,
        val createdAt: Long,
        val kind: String = "?",
        val label: String = "",
        val relevance: Double = 0.0,
        val textScore: Double = 0.0,
        val scattered: Double = 0.0,
        val coverage: Double = 0.0,
        val perfect: Double = 0.0,
    )

    /**
     * Median age in days of [hits] at [clock]; null when nothing came back.
     *
     * DISTANCE, like the ranker's own event_age_days(): the corpus holds notes
     * stamped in the year 2100 — the exact dirty data this column exists to
     * watch — and a signed age reports them at MINUS 27 000 days, which drags
     * a median toward "fresh" precisely when a config has gone wrong.
     */
    private fun medianAgeDays(
        hits: List<Hit>,
        clock: Long,
    ): Double? =
        hits
            .map { Math.abs(clock - it.createdAt) / 86400.0 }
            .sorted()
            .let { if (it.isEmpty()) null else it[it.size / 2] }

    /** One query with knob overrides; returns the ranked hits. Same YQL the library sends, always. */
    private fun search(
        base: String,
        case: JsonObject,
        profile: String,
        observer: String?,
        hits: Int,
        overrides: Map<String, Double>,
        nowSecs: Long?,
        kinds: List<Int>,
    ): List<Hit> {
        val text = case.getValue("query").jsonPrimitive.content
        // Case text feeds EventQuery.search RAW — below the store's syntax
        // parser, so `-word`/quotes in a rank case would be loose words here,
        // not exclusions/phrases. Deliberate (this A/Bs engine profiles on
        // identical recall); route cases through FilterMapping if they ever
        // adopt the term syntax.
        val vq =
            EventYql.build(
                EventQuery(
                    search = text,
                    kinds = kinds,
                    observer = observer,
                    ranking = profile,
                    minRank = observer?.let { 2.0 },
                    nowSecs = nowSecs,
                ),
            ) ?: return emptyList()
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
            .mapNotNull { child ->
                val hit = child as? JsonObject ?: return@mapNotNull null
                val fields = hit["fields"] as? JsonObject ?: return@mapNotNull null
                val id = (fields["id"] as? JsonPrimitive)?.content ?: return@mapNotNull null

                fun str(k: String) = (fields[k] as? JsonPrimitive)?.content
                val mf = fields["matchfeatures"] as? JsonObject

                fun feature(k: String) = (mf?.get(k) as? JsonPrimitive)?.content?.toDoubleOrNull() ?: -1.0
                Hit(
                    id = id,
                    createdAt = str("created_at")?.toLongOrNull() ?: 0L,
                    kind = str("kind") ?: "?",
                    label = labelOf(str("kind"), str("tags"), str("content")) ?: id.take(8),
                    relevance = (hit["relevance"] as? JsonPrimitive)?.content?.toDoubleOrNull() ?: 0.0,
                    textScore = feature("text_score"),
                    scattered = feature("scattered_match"),
                    coverage = feature("naming_coverage"),
                    perfect = feature("perfect_match"),
                )
            }
    }

    private val HTTP: HttpClient = HttpClient.newHttpClient()
}
