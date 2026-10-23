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
package com.nosfabrica.vespa.eventstore.benchmark.bench

import com.nosfabrica.vespa.eventstore.benchmark.harness.Agreement
import com.nosfabrica.vespa.eventstore.benchmark.harness.Judgments
import com.nosfabrica.vespa.eventstore.benchmark.harness.PairedComparison
import com.nosfabrica.vespa.eventstore.benchmark.harness.RelevanceMetrics
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.engine.text.StemText
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
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
 * to tune on — the verdict block prints the minimum detectable effect to say
 * how far from enough a run is.
 *
 * Beyond positions, this is the search-QUALITY harness of
 * `docs/search-evaluation.md`: graded judgments ([Judgments]) turn each page
 * into nDCG@10 / success@k ([RelevanceMetrics]), every candidate is compared
 * to the first config per topic ([PairedComparison]), and `--pool` / `--merge`
 * are the judging loop. That doc is the protocol; this is its instrument.
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
                    "perfect_df_lo" to 2e-4,
                    "perfect_df_hi" to 6e-4,
                    "perfect_df_min_docs" to 100000.0,
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
            // The rung damped by how common the word is AS A NAME (event.sd,
            // query(perfect_df_lo)): whole below lo, gone above hi, log-linear
            // between. SHIPPED at (2e-4, 6e-4), but OFF on an index under
            // perfect_df_min_docs — which every evaluation corpus is — so on
            // one, `baseline` is the damping OFF. These configs lift that guard
            // (min_docs 0): pair them with --name-df, which makes the corpus's
            // name statistics production's. `name_df_off` is the control.
            // Ranges from the 2026-10-23 measurement on a kind-stratified random
            // sample of production: one-word navigational topics name 0..1.6e-4
            // of documents (jack 1.6e-4, primal 1.2e-4, odell 2.7e-5), bitcoin and
            // nostr ~6.5e-4, lightning / coffee / bitcoiners 1-3e-5.
            "name_df_off" to mapOf("perfect_df_lo" to 1.0, "perfect_df_hi" to 1.0),
            "name_df_2e4_6e4" to mapOf("perfect_df_lo" to 2e-4, "perfect_df_hi" to 6e-4, "perfect_df_min_docs" to 0.0),
            "name_df_2e4_2e3" to mapOf("perfect_df_lo" to 2e-4, "perfect_df_hi" to 2e-3, "perfect_df_min_docs" to 0.0),
            "name_df_1e4_1e3" to mapOf("perfect_df_lo" to 1e-4, "perfect_df_hi" to 1e-3, "perfect_df_min_docs" to 0.0),
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
        lensOverride = opts["--lens-key"]
        // `--name-df <file>`: how common each word is AS A NAME on production,
        // measured on a random sample (docs/search-evaluation.md §9). See [withNameDf].
        nameDf =
            opts["--name-df"]
                ?.let { path ->
                    Json
                        .parseToJsonElement(Files.readString(Path.of(path)))
                        .jsonObject
                        .filterKeys { !it.startsWith("_") }
                        .mapValues {
                            it.value.jsonPrimitive.content
                                .toDouble()
                        }
                }.orEmpty()
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
        // `--run name=path[,name=path]`: rankings computed ELSEWHERE — the
        // production relay's served pages, another cluster's `--write-runs`
        // output — scored as configs beside the live ones, against the same
        // judgments (docs/search-evaluation.md §8.4). Named in `--configs`
        // like any other; put one FIRST to make it the baseline.
        val runs =
            opts["--run"]
                ?.split(",")
                ?.associate { spec -> spec.substringBefore("=") to loadRun(Path.of(spec.substringAfter("="))) }
                .orEmpty()
        val names = opts["--configs"]?.split(",") ?: (CONFIGS.keys.toList() + runs.keys)
        names.firstOrNull { it !in CONFIGS && it !in runs }?.let {
            System.err.println("unknown config '$it'; known: ${CONFIGS.keys + runs.keys}")
            return
        }
        // The graded judgments (docs/search-evaluation.md). A missing file is
        // an empty set: every topic then reads unjudged, and the report says so.
        val judgmentsPath = Path.of(opts["--judgments"] ?: "benchmark/search_judgments.json")

        // ---- --merge: fold a graded --pool worksheet into the judgments ---
        // No Vespa involved — this is the assessor's half of the loop.
        opts["--merge"]?.let { sheet ->
            merge(Path.of(sheet), judgmentsPath, opts["--by"])
            return
        }

        // ---- --agreement: a blind re-grade against the stored grades ------
        // The audit an LLM-judged batch owes (§5.3). Run it BEFORE merging the
        // audit sheet: --merge overwrites, so afterwards the two assessors
        // agree by construction.
        opts["--agreement"]?.let { sheet ->
            agreement(Path.of(sheet), Judgments.load(judgmentsPath))
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
            for (config in names.filter { it in CONFIGS }) {
                println("\n=== $config === $text")
                dumpPage(search(vespa, text, profile, observer, hits, CONFIGS.getValue(config), nowSecs, kinds))
            }
            return
        }

        // `--split dev|test|all`: tune on dev, confirm ONCE on test — a weight
        // swept on the topics it is scored on is fitted to them (§6.4).
        val split = opts["--split"] ?: "all"
        val topics =
            Json
                .parseToJsonElement(Files.readString(Path.of(casesPath)))
                .jsonArray
                .map { it.jsonObject }
                // `skip` opts a row out of BOTH columns — the file's TEMPLATE
                // row is documentation, and once empty-`expect` rows started
                // being measured for age it was being queried live and folded
                // into the headline median.
                .filter { (it["query"]?.jsonPrimitive?.content).orEmpty().isNotBlank() && it["skip"]?.jsonPrimitive?.content != "true" }
                .map { topicOf(it, observer, kinds) }
                .filter { split == "all" || it.split == split }
        if (topics.isEmpty()) {
            System.err.println("no cases with a query in $casesPath (split=$split) — nothing to measure")
            return
        }
        // A case with an empty `expect` names no winning document, so it scores
        // no position — but it still has a PAGE, and the age of that page is a
        // measurement. Those rows ride along age-only (they were skipped
        // outright before the recency work) and stay out of found/MRR, which
        // are claims about positions.
        val expects = topics.map { it.expect }
        // An `expect` is a human's report of THE answer, so it is a grade-3
        // judgment without anyone transcribing it.
        val qrels =
            Judgments
                .load(judgmentsPath)
                .withImplicit(topics.filter { it.expect != null }.associate { it.key to (it.expect!! to 3) })

        println(
            "target=$vespa profile=$profile observer=${observer ?: "none (pure text)"} depth=$hits cases=${topics.size} " +
                "kinds=${kinds.takeIf { it.isNotEmpty() }?.joinToString(",") ?: "all"} now=${nowSecs?.toString() ?: "wall clock"} " +
                "split=$split judgments=$judgmentsPath (${qrels.topicCount} topics)",
        )
        val clock = nowSecs ?: (System.currentTimeMillis() / 1000)
        val pages = LinkedHashMap<String, List<List<Hit>>>()
        for (config in names) {
            val run = runs[config]
            pages[config] =
                if (run != null) {
                    topics.filter { it.key !in run }.takeIf { it.isNotEmpty() }?.let { missing ->
                        System.err.println("run '$config' has no page for ${missing.map { it.key }} — scored as EMPTY pages")
                    }
                    topics.map { run[it.key].orEmpty().take(hits) }
                } else {
                    topics.map { search(vespa, it.query, profile, it.observer, hits, CONFIGS.getValue(config), nowSecs, it.kinds) }
                }
        }
        opts["--write-runs"]?.let { dir -> writeRuns(Path.of(dir), topics, pages) }
        val results =
            pages.mapValues { (_, ranked) ->
                ranked.mapIndexed { i, hitsOf ->
                    expects[i]?.let { want -> hitsOf.indexOfFirst { it.id.startsWith(want) }.takeIf { it >= 0 }?.plus(1) }
                }
            }
        // Median age, in days, of each case's top-[AGE_DEPTH] hits — the thing
        // a recency config is actually FOR, and invisible in a position table.
        val ages = pages.mapValues { (_, ranked) -> ranked.map { medianAgeDays(it.take(AGE_DEPTH), clock) } }
        val quality = pages.mapValues { (_, ranked) -> ranked.mapIndexed { i, page -> scoreOf(topics[i], page, qrels) } }
        val basePages = pages.getValue(names.first())

        val base = results.getValue(names.first())
        val labels = topics.mapIndexed { i, t -> "${t.query}|${expects[i]?.take(10) ?: "age-only"}" }
        val width = labels.maxOf { it.length }
        val baseAges = ages.getValue(names.first())
        for ((config, row) in results) {
            println("\n--- $config${if (config == names.first()) "   (baseline)" else ""} ---")
            val q = quality.getValue(config)
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
                val graded = q[i].ndcg?.let { "nDCG %.2f".format(it) } ?: "nDCG  -  "
                val judged = q[i].judged?.let { "j@10 %.1f".format(it) } ?: "j@10  - "
                val rbo = if (config == names.first()) "" else "  rbo %.2f".format(RelevanceMetrics.rbo(basePages[i].map { it.id }, pages.getValue(config)[i].map { it.id }))
                println(
                    "  ${labels[i].padEnd(width)}  ${position.padEnd(6)}${delta.padEnd(16)}  age ${(age?.let { "%.0fd".format(it) } ?: "-").padEnd(6)}" +
                        "${ageDelta.padEnd(8)}  $graded  $judged$rbo",
                )
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
            println("  ${"".padEnd(width)}  ${qualityLine(q)}")
        }
        println("\nhigher MRR is better; MISS -> a number is the win that matters.")
        println("age = median created_at age of the top-$AGE_DEPTH hits: what a recency config buys, and what it must not")
        println("pay for in positions — a config that trades a pinned case for fresher results has lost.")
        if (names.size > 1) compareToBaseline(names, topics, quality)

        // ---- --pool: what the assessors have to judge next ----------------
        // The union of every config's top `--pool-depth` that no judgment
        // covers — TREC pooling, run incrementally: only what THIS change
        // surfaced is new work. Grade the rows, then `--merge` the file.
        opts["--pool"]?.let { out ->
            val depth = (opts["--pool-depth"] ?: "20").toInt()
            writePool(Path.of(out), topics, pages, qrels, depth)
        }
    }

    /**
     * One `rank_cases.json` row as an evaluation TOPIC: the query plus the
     * lens and kinds it is asked under. A row's own `observer` ("none" for
     * the anonymous page) and `kinds` override the command line, because a
     * topic like "the People tab under the canonical observer" is only that
     * topic under that lens. `id` names it in the judgments (default: the
     * query text); `intent` and `split` are §3's strata and §6.4's split.
     */
    private data class Topic(
        val key: String,
        val query: String,
        val expect: String?,
        val intent: String,
        // What an assessor grades against (TREC's narrative), carried onto every worksheet row.
        val need: String?,
        val split: String,
        val observer: String?,
        val kinds: List<Int>,
    )

    private fun topicOf(
        case: JsonObject,
        observer: String?,
        kinds: List<Int>,
    ): Topic {
        fun str(k: String) = (case[k] as? JsonPrimitive)?.content?.takeIf(String::isNotEmpty)
        val query = case.getValue("query").jsonPrimitive.content
        return Topic(
            key = str("id") ?: query,
            query = query,
            expect = str("expect"),
            intent = str("intent") ?: "unclassified",
            need = str("need"),
            split = str("split") ?: "dev",
            observer =
                when (val o = str("observer")) {
                    null -> observer
                    "none" -> null
                    else -> o
                },
            kinds = (case["kinds"] as? JsonArray)?.map { it.jsonPrimitive.content.toInt() } ?: kinds,
        )
    }

    /** One topic's quality on one config's page; each field null where it does not apply (§5). */
    private data class Score(
        val ndcg: Double?,
        val judged: Double?,
        val rr: Double?,
        val s1: Double?,
        val s3: Double?,
        val authorShare: Double?,
    )

    /**
     * A topic is GRADED once it has [GRADED_MIN] judgments: condensed nDCG on
     * fewer is mostly the condensing (an `expect`-only topic would score 1.0
     * whenever its one judged doc is anywhere on the page). It is KNOWN-ITEM
     * when some document is judged "the answer" (3) — then the page is also
     * read as "where is it", on the uncondensed list.
     */
    private fun scoreOf(
        topic: Topic,
        page: List<Hit>,
        qrels: Judgments,
    ): Score {
        val grades = page.map { qrels.gradeOf(topic.key, it.id) }
        val all = qrels.grades(topic.key)
        val knownItem = 3 in all
        return Score(
            ndcg = if (all.size >= GRADED_MIN) RelevanceMetrics.ndcg(grades, all) else null,
            judged = if (all.isEmpty()) null else RelevanceMetrics.judgedAt(grades),
            rr = if (knownItem) RelevanceMetrics.reciprocalRank(grades, 3) else null,
            s1 = if (knownItem) RelevanceMetrics.successAt(grades, 3, 1) else null,
            s3 = if (knownItem) RelevanceMetrics.successAt(grades, 3, 3) else null,
            authorShare = RelevanceMetrics.maxAuthorShare(page.map { it.pubkey }),
        )
    }

    private fun qualityLine(q: List<Score>): String {
        fun mean(v: List<Double?>) = v.filterNotNull().let { if (it.isEmpty()) "-" else "%.3f".format(it.average()) }
        val graded = q.mapNotNull { it.ndcg }
        val underJudged = q.count { it.ndcg != null && (it.judged ?: 1.0) < JUDGED_FLOOR }
        // judged@10 over the GRADED topics only: it is the gauge for their
        // condensed nDCG, and an expect-only topic is 1-in-10 judged by design.
        return "graded ${graded.size}: nDCG@10 ${mean(q.map { it.ndcg })}  judged@10 ${mean(q.filter { it.ndcg != null }.map { it.judged })}" +
            (if (underJudged > 0) " ($underJudged under ${"%.1f".format(JUDGED_FLOOR)} — run --pool)" else "") +
            "   known-item ${q.count { it.rr != null }}: S@1 ${mean(q.map { it.s1 })} S@3 ${mean(q.map { it.s3 })}" +
            "   top-author share ${mean(q.map { it.authorShare })}"
    }

    /**
     * The verdict block (§6): every candidate against the FIRST config, paired
     * per topic, on the two headline measures — nDCG@10 over graded topics and
     * RR over known-item ones. p is the randomization test's, Holm-adjusted
     * across every candidate in this run (a sweep is many tests); MDE is the
     * smallest mean delta this many topics could have detected, so a
     * non-significant row says how small "nothing" is. Then the RISK view a
     * mean hides: every topic that lost more than [LOSS_REPORT], by name —
     * each must be explained before a change ships — and the per-intent means.
     */
    private fun compareToBaseline(
        names: List<String>,
        topics: List<Topic>,
        quality: Map<String, List<Score>>,
    ) {
        val baseline = names.first()
        val candidates = names.drop(1)
        for ((metric, pick) in listOf<Pair<String, (Score) -> Double?>>("nDCG@10" to { it.ndcg }, "RR (known-item)" to { it.rr })) {
            val b = quality.getValue(baseline).map(pick)
            val rows =
                candidates.map { config ->
                    val c = quality.getValue(config).map(pick)
                    // Only topics the metric covers in BOTH configs are paired.
                    val idx = topics.indices.filter { b[it] != null && c[it] != null }
                    Triple(config, idx, PairedComparison.compare(idx.map { b[it]!! }, idx.map { c[it]!! }))
                }
            if (rows.all { it.third.n == 0 }) {
                println("\n=== $metric vs $baseline: no topic carries it — judge some (docs/search-evaluation.md §5) ===")
                continue
            }
            val adjusted = PairedComparison.holm(rows.map { it.third.p })
            println("\n=== $metric vs $baseline — paired per topic; randomization test, Holm-adjusted over ${rows.size} candidate(s) ===")
            println("  %-22s %4s %8s %19s %8s %9s %7s".format("config", "n", "Δ mean", "95% CI", "p(holm)", "W/T/L", "MDE"))
            rows.forEachIndexed { r, (config, idx, res) ->
                println(
                    "  %-22s %4d %+8.3f   [%+.3f, %+.3f] %8.3f %9s %7.3f".format(
                        config,
                        res.n,
                        res.meanDelta,
                        res.ciLow,
                        res.ciHigh,
                        adjusted[r],
                        "${res.wins}/${res.ties}/${res.losses}",
                        PairedComparison.minimumDetectable(res.sdDelta, res.n),
                    ),
                )
                val c = quality.getValue(config).map(pick)
                val losses = idx.map { it to c[it]!! - b[it]!! }.filter { it.second < -LOSS_REPORT }.sortedBy { it.second }
                if (losses.isNotEmpty()) {
                    println("      losses > $LOSS_REPORT: " + losses.joinToString("  ") { (i, d) -> "'${topics[i].key}' %+.2f".format(d) })
                }
                val byIntent =
                    idx.groupBy { topics[it].intent }.toSortedMap().entries.joinToString("  ") { (intent, ii) ->
                        "$intent %+.3f (n=${ii.size})".format(ii.map { c[it]!! - b[it]!! }.average())
                    }
                println("      by intent: $byIntent")
            }
        }
        println("\nread: a candidate ships when no pinned case is LOST, the nDCG CI's low end clears -$NON_INFERIORITY, every listed loss")
        println("is explained, and the guardrails (age, top-author share, latency) hold — docs/search-evaluation.md §6.")
    }

    /**
     * Writes the unjudged pool as a worksheet: one row per (topic, event),
     * `grade` left null for the assessor. BLIND, as TREC pools are: no rank,
     * no score, no config name, and rows sorted by event id — an assessor
     * (human or model) who can see that a document came first, or came from
     * the candidate, grades the ranking instead of the document (§5.2).
     */
    private fun writePool(
        out: Path,
        topics: List<Topic>,
        pages: Map<String, List<List<Hit>>>,
        qrels: Judgments,
        depth: Int,
    ) {
        val rows =
            buildJsonArray {
                topics.forEachIndexed { i, topic ->
                    val pool = sortedMapOf<String, Hit>()
                    for (ranked in pages.values) {
                        ranked[i].take(depth).filter { qrels.gradeOf(topic.key, it.id) == null }.forEach { pool.putIfAbsent(it.id, it) }
                    }
                    for ((id, hit) in pool) {
                        add(
                            buildJsonObject {
                                put("topic", topic.key)
                                put("query", topic.query)
                                put("intent", topic.intent)
                                put("need", topic.need ?: "(no `need` on this case — write one in rank_cases.json before grading)")
                                put("id", id)
                                put("kind", hit.kind)
                                put("label", hit.label)
                                put("author", hit.pubkey)
                                put("created_at", hit.createdAt)
                                put("content", hit.content.take(POOL_CONTENT_CHARS))
                                // The first tags, values cut short: a NIP-29 `h` (which group), a
                                // `title`, the `t` hashtags are often what a grade turns on, and the
                                // first round had to be corrected for lacking them. Capped, because a
                                // follow list's tags are its whole content.
                                put("tags", worksheetTags(hit.tags))
                                put("grade", JsonNull)
                                put("by", JsonNull)
                            },
                        )
                    }
                }
            }
        Files.writeString(out, PRETTY.encodeToString(JsonArray.serializer(), rows) + "\n")
        println("\npool: ${rows.size} unjudged hit(s) in the top $depth of ${pages.size} config(s) over ${topics.size} topic(s) -> $out")
        println("grade each row 0-3 per docs/search-evaluation.md §4, set `by`, then: rankAb --merge $out")
    }

    private fun worksheetTags(raw: String): JsonArray =
        JsonArray(
            runCatching { Json.parseToJsonElement(raw).jsonArray }
                .getOrDefault(JsonArray(emptyList()))
                .take(POOL_TAGS)
                .map { tag -> JsonArray(tag.jsonArray.take(3).map { JsonPrimitive(it.jsonPrimitive.content.take(POOL_TAG_CHARS)) }) },
        )

    /** Folds every graded row of a `--pool` worksheet into the judgments file. */
    private fun merge(
        sheet: Path,
        judgmentsPath: Path,
        defaultBy: String?,
    ) {
        val qrels = Judgments.load(judgmentsPath)
        val today =
            java.time.LocalDate
                .now()
                .toString()
        var added = 0
        var ungraded = 0
        for (row in Json.parseToJsonElement(Files.readString(sheet)).jsonArray.map { it.jsonObject }) {
            val grade = (row["grade"] as? JsonPrimitive)?.content?.toIntOrNull()
            if (grade == null) {
                ungraded++
                continue
            }
            val by =
                (row["by"] as? JsonPrimitive)?.content?.takeIf { it.isNotBlank() && it != "null" } ?: defaultBy
                    ?: error("row ${row["topic"]}/${row["id"]} has no `by` and no --by was given — provenance is required (§5.3)")
            qrels.put(row.getValue("topic").jsonPrimitive.content, row.getValue("id").jsonPrimitive.content, Judgments.Judgment(grade, by, today))
            added++
        }
        qrels.write(judgmentsPath)
        println("merged $added judgment(s) into $judgmentsPath ($ungraded row(s) left ungraded)")
    }

    /**
     * A RUN FILE: `{ "<topic>": [ <event>, … ] }` in served order — what a
     * relay's NIP-50 REQ returns, captured as-is, or what [writeRuns] wrote.
     * Plain Nostr events (a `sig` is ignored), so a capture needs no
     * conversion. Keys starting with `_` are documentation.
     */
    private fun loadRun(path: Path): Map<String, List<Hit>> =
        Json
            .parseToJsonElement(Files.readString(path))
            .jsonObject
            .filterKeys { !it.startsWith("_") }
            .mapValues { (_, page) ->
                page.jsonArray.map { e ->
                    val o = e.jsonObject

                    fun str(k: String) = (o[k] as? JsonPrimitive)?.content
                    val tags = o["tags"]?.toString() ?: "[]"
                    Hit(
                        id = str("id")!!,
                        createdAt = str("created_at")?.toLongOrNull() ?: 0L,
                        kind = str("kind") ?: "?",
                        label = labelOf(str("kind"), tags, str("content")) ?: str("id")!!.take(8),
                        pubkey = str("pubkey").orEmpty(),
                        content = str("content").orEmpty(),
                        tags = tags,
                    )
                }
            }

    /** One run file per config, so a ranking from THIS cluster can be scored against another's later (§8.4). */
    private fun writeRuns(
        dir: Path,
        topics: List<Topic>,
        pages: Map<String, List<List<Hit>>>,
    ) {
        Files.createDirectories(dir)
        for ((config, ranked) in pages) {
            val run =
                buildJsonObject {
                    put("_doc", "rankAb run '$config': served order per topic (docs/search-evaluation.md §8.4)")
                    topics.forEachIndexed { i, topic ->
                        put(
                            topic.key,
                            buildJsonArray {
                                ranked[i].forEach { h ->
                                    add(
                                        buildJsonObject {
                                            put("id", h.id)
                                            put("pubkey", h.pubkey)
                                            put("created_at", h.createdAt)
                                            put("kind", h.kind.toIntOrNull() ?: -1)
                                            put("tags", runCatching { Json.parseToJsonElement(h.tags) }.getOrDefault(JsonArray(emptyList())))
                                            put("content", h.content)
                                        },
                                    )
                                }
                            },
                        )
                    }
                }
            Files.writeString(dir.resolve("run_$config.json"), Json.encodeToString(JsonObject.serializer(), run) + "\n")
        }
        println("wrote ${pages.size} run file(s) to $dir")
    }

    /**
     * Grades in [sheet] (a --pool-shaped worksheet a second assessor filled)
     * against the stored ones, on the documents both graded.
     */
    private fun agreement(
        sheet: Path,
        qrels: Judgments,
    ) {
        val rows = Json.parseToJsonElement(Files.readString(sheet)).jsonArray.map { it.jsonObject }
        val pairs = ArrayList<Pair<Int, Int>>()
        val byTopic = LinkedHashMap<String, MutableList<Pair<Int, Int>>>()
        for (row in rows) {
            val second = (row["grade"] as? JsonPrimitive)?.content?.toIntOrNull() ?: continue
            val topic = row.getValue("topic").jsonPrimitive.content
            val first = qrels.gradeOf(topic, row.getValue("id").jsonPrimitive.content) ?: continue
            pairs += first to second
            byTopic.getOrPut(topic) { ArrayList() } += first to second
        }
        val r = Agreement.of(pairs)
        println("agreement over ${r.n} document(s) graded in both (stored = first assessor, $sheet = second)")
        println("  exact %.2f   within one %.2f   quadratic-weighted kappa %s".format(r.exact, r.withinOne, if (r.kappa.isNaN()) "-" else "%.3f".format(r.kappa)))
        println("  confusion (rows: stored grade 0..3, columns: second assessor 0..3)")
        r.matrix.forEachIndexed { g, row -> println("    $g  " + row.joinToString(" ") { "%4d".format(it) }) }
        val higher = pairs.count { it.second > it.first }
        val lower = pairs.count { it.second < it.first }
        println("  second assessor higher on $higher, lower on $lower — a lenient first assessor shows as 'lower' mass")
        val worst =
            byTopic
                .mapValues { (_, p) -> p.count { it.first != it.second } }
                .filterValues { it > 0 }
                .entries
                .sortedByDescending { it.value }
        if (worst.isNotEmpty()) println("  disagreements by topic: " + worst.joinToString("  ") { "'${it.key}' ${it.value}/${byTopic.getValue(it.key).size}" })
    }

    /** Judgments a topic needs before nDCG means anything (see [scoreOf]). */
    private const val GRADED_MIN = 5

    /** judged@10 below this and a condensed nDCG is flagged: judge the pool first (§5.2). */
    private const val JUDGED_FLOOR = 0.9

    /** Per-topic losses larger than this are listed by name in the verdict. */
    private const val LOSS_REPORT = 0.1

    /** The non-inferiority margin on mean nDCG@10 a change must clear (§6.2). */
    private const val NON_INFERIORITY = 0.01

    /** How much of each pooled event's content the worksheet carries — enough to grade, not the whole article. */
    private const val POOL_CONTENT_CHARS = 600

    /** How many of each pooled event's tags the worksheet shows, and how much of each value. */
    private const val POOL_TAGS = 16
    private const val POOL_TAG_CHARS = 80

    @OptIn(ExperimentalSerializationApi::class)
    private val PRETTY =
        Json {
            prettyPrint = true
            prettyPrintIndent = "  "
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
        // For the quality columns: the author-share guardrail and the --pool worksheet.
        val pubkey: String = "",
        val content: String = "",
        // The raw tag array, so --write-runs saves an event a run file can be re-read from.
        val tags: String = "[]",
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
        text: String,
        profile: String,
        observer: String?,
        hits: Int,
        overrides: Map<String, Double>,
        nowSecs: Long?,
        kinds: List<Int>,
    ): List<Hit> {
        // Case text goes through the relay's GRAMMAR first — Quartz's
        // SearchQuery, the same parse the store's FilterMapping.toEventQuery
        // starts from (that function is `internal` to :store, so its policies
        // are mirrored here, not called): `-word` / `-"phrase"` exclude,
        // `"quoted"` spans are phrases, `include:spam` drops the gate but keeps
        // the trust curve anchored at 0, `language:pt|es` adds the stemmed
        // body column, and an unknown `key:value` extension is IGNORED, as
        // NIP-50 asks. Until 2026-10-23 the text went in RAW, so a case like
        // `group:rspuyeetackckp92 testin` required the literal token
        // `group:rspuyeetackckp92`, matched nothing, and scored a page the
        // relay never serves — invisible until production's pages became a
        // baseline (docs/search-evaluation.md §8.5). Not mirrored: NIP-30
        // shortcode rewriting, `sort:` / `filter:` (the profile and floor are
        // this harness's own knobs).
        val parsed = SearchQuery.parse(text)
        val vq =
            EventYql.build(
                EventQuery(
                    search = parsed.terms.ifEmpty { null },
                    phrases = parsed.phrases,
                    notSearch = parsed.notTerms + parsed.notPhrases,
                    includeSpam = parsed.includeSpam,
                    stemLanguage =
                        parsed.language
                            ?.lowercase()
                            ?.substringBefore('-')
                            ?.substringBefore('_')
                            ?.takeIf { it in StemText.LANGUAGES },
                    kinds = kinds,
                    observer = observer,
                    rankKey = observer?.let { lensOf(base, it).first },
                    followersKey = observer?.let { lensOf(base, it).second },
                    ranking = profile,
                    minRank = observer?.let { if (parsed.includeSpam) 0.0 else 2.0 },
                    nowSecs = nowSecs,
                ),
            ) ?: return emptyList()
        val body =
            buildJsonObject {
                put("yql", withNameDf(base, vq.yql, parsed))
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
                    pubkey = str("pubkey").orEmpty(),
                    content = str("content").orEmpty(),
                    tags = str("tags") ?: "[]",
                )
            }
    }

    /**
     * The observer's lens RESOLVED to service keys — what `user_q` and
     * `followers_q` really carry since trust went service-keyed
     * (docs/service-keyed-trust.md). The store does this per query off its
     * provider map; this does it once per observer off the cluster's own
     * kind 10040 (`30382:rank` / `30382:followers`), so the page is the one
     * the relay serves. Before this, an `--observer` run sent the OBSERVER as
     * the key, matched no tensor cell, and the min_rank gate emptied every
     * page. `--lens-key` overrides both (a cluster fed without the 10040).
     */
    private fun lensOf(
        base: String,
        observer: String,
    ): Pair<String?, String?> =
        lensCache.getOrPut(observer) {
            lensOverride?.let { return@getOrPut it to it }
            val body =
                buildJsonObject {
                    put("yql", "select * from event where kind = 10040 and pubkey contains \"$observer\"")
                    put("hits", "1")
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
            val tags =
                runCatching {
                    Json
                        .parseToJsonElement(resp.body())
                        .jsonObject["root"]
                        ?.jsonObject
                        ?.get("children")
                        ?.jsonArray
                        ?.firstOrNull()
                        ?.jsonObject
                        ?.get("fields")
                        ?.jsonObject
                        ?.get("tags")
                        ?.jsonPrimitive
                        ?.content
                        ?.let { Json.parseToJsonElement(it).jsonArray.map { t -> t.jsonArray.map { v -> v.jsonPrimitive.content } } }
                }.getOrNull().orEmpty()

            fun service(type: String) = tags.firstOrNull { it.size > 1 && it[0] == "30382:$type" }?.get(1)
            val lens = service("rank") to service("followers")
            if (lens.first == null) System.err.println("observer $observer has no kind 10040 naming a 30382:rank service here — it ranks as trusting nobody")
            lens
        }

    private val lensCache = HashMap<String, Pair<String?, String?>>()

    /** `--name-df`: word -> share of production documents carrying it in a naming field. */
    private var nameDf: Map<String, Double> = emptyMap()

    /**
     * Makes this cluster's NAME-FIELD document frequency read as production's.
     * `event.sd`'s perfect_share() damps the perfect-match rung by how common
     * the query word is as a name (queryTermDocumentFrequency over name,
     * display_name and search_primary, over num_docs_indexed). Those are
     * statistics of the LOCAL index, and an evaluation corpus is drawn from
     * the topics' own pages, so its names are full of the very words it
     * queries: "bitcoin" names 7% of it, against a fraction of a percent of
     * production. So, for a one-word query with a measured [nameDf], the three
     * naming-field clauses are rewritten from `userInput` to `contains` with a
     * `documentFrequency` annotation, the per-query override Vespa's
     * statistics honour. The frequency is scaled to THIS cluster's document
     * count because the expression divides by the local num_docs_indexed, not
     * by the override's count. The label is kept: itemRawScore(mtch_exact)
     * reads it. Matching is unchanged for a single plain word, the only shape
     * rewritten; anything with punctuation is left alone.
     */
    private fun withNameDf(
        base: String,
        yql: String,
        parsed: SearchQuery,
    ): String {
        val word = parsed.terms.trim()
        val ratio = nameDf[word.lowercase()] ?: return yql
        if (parsed.phrases.isNotEmpty() || !word.all { it.isLetterOrDigit() }) return yql
        val docs = localDocs.getOrPut(base) { countDocs(base) }
        val frequency = Math.round(ratio * docs)
        var out = yql
        for (field in listOf("name", "display_name", "search_primary")) {
            val clause = "({defaultIndex:\"$field\",label:\"mtch_exact\"}userInput(@w0))"
            check(clause in out) { "--name-df: no `$field` exact clause to annotate in the YQL; EventYql changed shape" }
            out =
                out.replace(
                    clause,
                    "($field contains ({label:\"mtch_exact\",documentFrequency:{frequency:$frequency,count:$docs}}\"$word\"))",
                )
        }
        return out
    }

    private val localDocs = HashMap<String, Long>()

    private fun countDocs(base: String): Long {
        val body =
            buildJsonObject {
                put("yql", "select * from event where true")
                put("hits", "0")
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
        return Json
            .parseToJsonElement(resp.body())
            .jsonObject["root"]!!
            .jsonObject["fields"]!!
            .jsonObject["totalCount"]!!
            .jsonPrimitive
            .content
            .toLong()
    }

    /** `--lens-key`, when given: the service key to rank by, whatever the 10040 says. */
    private var lensOverride: String? = null

    private val HTTP: HttpClient = HttpClient.newHttpClient()
}
