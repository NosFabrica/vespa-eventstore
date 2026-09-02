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
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * WHERE A SLOW SEARCH SPENDS ITS TIME — per clause group and per rank phase,
 * against a live cluster holding a real corpus.
 *
 * The reported shape ("bitcoin"/"nostr" take 5-6s on a production relay) is one
 * query word whose match set is enormous, and [com.nosfabrica.vespa.eventstore.engine.query.FuzzyWordGroup]
 * answers one word with ~20 OR'd matchers across four families — exact tokens,
 * prefix/fuzzy over the near attributes, AND-of-trigram infix nets, and the
 * body's trigram PHRASE net. Latency that big is never uniform across them, and
 * nothing here could previously say which family paid for it: `rankAb` measures
 * ranking QUALITY, `searchBench` measures whole shapes, `traceProbe` dumps plans
 * for filter (non-search) shapes.
 *
 * So: take the store's OWN assembled YQL ([EventYql.build] — byte-identical to
 * production's, the same rule rankAb follows), then ABLATE one clause family at
 * a time and re-time it. The ablation is a mechanical rewrite of the emitted
 * WHERE — every OR-branch that names a given family's fields is dropped — so it
 * cannot drift from what the builder emits: it is applied TO what the builder
 * emitted, and every variant prints the YQL it actually sent.
 *
 * ABLATIONS ARE MEASUREMENTS, NOT PROPOSALS. Dropping a matcher drops the
 * recall it carries, which is why each row also prints `totalCount` — a variant
 * that is fast because it stopped matching is not a speedup, and the count is
 * how the reader tells the two apart.
 *
 * The rank-profile rows are the other half of the split: the same match set
 * scored by `search` (the trust default), by `text`, and by
 * `recency_gated_exact` — which gates and scores by `created_at` alone and so
 * prices matching WITHOUT text ranking. `search` minus `recency_gated_exact` is
 * what the text cascade costs over that match set.
 *
 *     VESPA_URL=http://localhost:8080 SEARCH_OBSERVER=<hex> \
 *       ./gradlew :benchmark:searchTrace --args="bitcoin nostr"
 *
 * Env: `VESPA_URL`, `SEARCH_OBSERVER` (the trust lens; without one the query
 * ranks on `text`), `SEARCH_KINDS` (default 1 — the dominant relay shape),
 * `SEARCH_LIMIT` (50), `SEARCH_REPS` (5, medians reported), `SEARCH_YQL` (1 to
 * print every variant's YQL), `TRACE_LEVEL` (0 = off; >0 adds Vespa's
 * blueprint with per-term hit estimates to the FULL variant).
 */
object SearchTrace {
    private val json = Json { ignoreUnknownKeys = true }
    private val http = HttpClient.newBuilder().proxy(java.net.ProxySelector.of(null)).build()

    /**
     * One ablation: a label, and the field/operator names whose OR-branches it
     * removes. A branch is dropped when it mentions ANY of the markers, which is
     * exactly how the families differ — each matcher family names its own
     * columns ([com.nosfabrica.vespa.eventstore.engine.query.FuzzyWordGroup]'s
     * NEAR_FIELDS / PREFIX_ONLY_FIELDS / the *_gram lists) or its own operator.
     */
    private class Ablation(
        val label: String,
        val markers: List<String>,
    )

    /**
     * The columns only a PROFILE-shaped event fills (Quartz's
     * `IndexableFields.Profile`). A `kinds:[1]` REQ can match none of them, so
     * this row prices what the dominant relay shape spends on columns its own
     * filter has already excluded.
     */
    private val PROFILE_MARKERS =
        listOf(
            "\"name\"",
            "\"display_name\"",
            "\"about\"",
            "\"nip05\"",
            "\"lud16\"",
            "\"website\"",
            "name_gram",
            "display_name_gram",
            "about_gram",
            "name_near",
            "affil_tokens",
        )

    private val ABLATIONS =
        listOf(
            Ablation("-bodygram (search_text_gram phrase)", listOf("search_text_gram")),
            Ablation("-namegram (name/display/primary AND net)", listOf("name_gram", "display_name_gram", "search_primary_gram")),
            Ablation("-textgram (about/secondary AND net)", listOf("about_gram", "search_secondary_gram")),
            Ablation("-allgram", listOf("_gram")),
            Ablation("-fuzzy", listOf("fuzzy(")),
            Ablation("-prefix", listOf("prefix:true")),
            Ablation("-near (prefix+fuzzy)", listOf("prefix:true", "fuzzy(")),
            Ablation("-profilecols (kind-0 columns)", PROFILE_MARKERS),
            Ablation("exact index columns only", listOf("prefix:true", "fuzzy(", "_gram")),
        )

    @JvmStatic
    fun main(args: Array<String>) {
        val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
        val observer = System.getenv("SEARCH_OBSERVER")
        val kinds = (System.getenv("SEARCH_KINDS") ?: "1").split(",").mapNotNull { it.trim().toIntOrNull() }
        val limit = System.getenv("SEARCH_LIMIT")?.toIntOrNull() ?: 50
        val reps = System.getenv("SEARCH_REPS")?.toIntOrNull() ?: 5
        val showYql = System.getenv("SEARCH_YQL") == "1"
        val traceLevel = System.getenv("TRACE_LEVEL")?.toIntOrNull() ?: 0
        val terms = if (args.isNotEmpty()) args.toList() else listOf("bitcoin", "nostr")

        println("cluster $url, kinds=$kinds, limit=$limit, reps=$reps, observer=${observer ?: "(none)"}")
        for (term in terms) {
            val base = EventQuery(kinds = kinds, limit = limit, search = term, observer = observer, minRank = observer?.let { 0.0 })
            val built = EventYql.build(base) ?: error("query for '$term' provably matches nothing")
            println()
            println("=== \"$term\" — profile ${built.ranking} ===")
            if (showYql) println("  yql: ${built.yql}")

            val rows = ArrayList<Triple<String, Long, Long>>()
            rows += run(url, "FULL (as production)", built.yql, built.params, built.ranking, reps, traceLevel, showYql)

            // Rank-phase split: same match set, cheaper scoring.
            for (profile in listOf(EventYql.RANK_TEXT, EventYql.RANK_RECENCY_GATED_EXACT)) {
                val q = base.copy(ranking = profile)
                val v = EventYql.build(q) ?: continue
                rows += run(url, "profile=$profile", v.yql, v.params, v.ranking, reps, traceLevel = 0, showYql)
            }

            // Clause-family split: same profile, fewer matchers.
            for (ab in ABLATIONS) {
                val stripped = ablate(built.yql, ab.markers)
                if (stripped == null) {
                    println("  %-42s (no branch matched — nothing to strip)".format(ab.label))
                    continue
                }
                rows += run(url, ab.label, stripped, built.params, built.ranking, reps, traceLevel = 0, showYql)
            }

            val full = rows.first()
            println("  %-42s %8s %10s  %s".format("variant", "p50 ms", "matches", "vs FULL"))
            for ((label, ms, hits) in rows) {
                val delta = if (label == full.first) "—" else "%+.0f%%".format((ms - full.second) * 100.0 / full.second)
                println("  %-42s %8d %10d  %s".format(label, ms, hits, delta))
            }
        }
    }

    private fun run(
        url: String,
        label: String,
        yql: String,
        params: Map<String, String>,
        ranking: String,
        reps: Int,
        traceLevel: Int,
        showYql: Boolean,
    ): Triple<String, Long, Long> {
        if (showYql) println("  [$label] $yql")
        val times = ArrayList<Long>()
        var matches = 0L
        // One untimed pass: the first run of a shape pays dictionary and
        // posting-list page-ins the repeated ones do not.
        repeat(reps + 1) { i ->
            val t0 = System.nanoTime()
            val body = post(url, yql, params, ranking, if (i == 0) traceLevel else 0)
            val ms = (System.nanoTime() - t0) / 1_000_000
            val root = body["root"]?.jsonObject
            matches = root
                ?.get("fields")
                ?.jsonObject
                ?.get("totalCount")
                ?.jsonPrimitive
                ?.content
                ?.toLongOrNull() ?: 0L
            if (i == 0) {
                body["trace"]?.let { if (traceLevel > 0) println("  trace: $it") }
            } else {
                times += ms
            }
        }
        times.sort()
        return Triple(label, times[times.size / 2], matches)
    }

    /**
     * One search POST, returning the whole response object. NOT private:
     * `MatchThreadPageIT` sends the store's own YQL with one parameter changed,
     * and a second copy of this would be a second thing to keep honest.
     */
    internal fun post(
        url: String,
        yql: String,
        params: Map<String, String>,
        ranking: String,
        traceLevel: Int = 0,
    ): JsonObject {
        val form = StringBuilder()

        fun add(
            k: String,
            v: String,
        ) {
            if (form.isNotEmpty()) form.append('&')
            form.append(enc(k)).append('=').append(enc(v))
        }
        add("yql", yql)
        params.forEach { (k, v) -> add(k, v) }
        add("ranking.profile", ranking)
        add("presentation.timing", "true")
        add("timeout", "120s")
        if (traceLevel > 0) {
            add("trace.level", traceLevel.toString())
            add("trace.explainLevel", "1")
        }
        val req =
            HttpRequest
                .newBuilder(URI.create("${url.trimEnd('/')}/search/"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form.toString()))
                .build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofString())
        check(res.statusCode() == 200) { "vespa ${res.statusCode()}: ${res.body().take(600)}" }
        return json.parseToJsonElement(res.body()).jsonObject
    }

    private fun enc(s: String) = java.net.URLEncoder.encode(s, Charsets.UTF_8)

    /**
     * The WHERE with every top-level OR-branch mentioning [markers] removed —
     * null when nothing matched, so the caller can say "nothing to strip"
     * rather than re-time an identical query.
     *
     * Branch-wise, not clause-wise: the word group is `(b1 or b2 or …)` and a
     * matcher family occupies whole branches, so splitting an OR list at
     * DEPTH ZERO of its own parentheses (and outside quotes) removes exactly
     * the family and leaves every other matcher intact. A group left empty by
     * the strip collapses to `false`, which is what "this word can no longer
     * match here" means.
     */
    internal fun ablate(
        yql: String,
        markers: List<String>,
    ): String? {
        val at = yql.indexOf(WHERE)
        check(at >= 0) { "not a filter query: $yql" }
        val from = at + WHERE.length
        // The tail the builder appends after the WHERE, if any. Both are
        // depth-0 and quote-free by construction, so a plain search finds them.
        val to =
            listOf(" order by ", " limit ")
                .mapNotNull { t -> yql.indexOf(t, from).takeIf { it >= 0 } }
                .minOrNull() ?: yql.length
        val out = rewrite(yql.substring(from, to), markers)
        if (out == yql.substring(from, to)) return null
        return yql.substring(0, from) + out + yql.substring(to)
    }

    private const val WHERE = " where "

    private fun rewrite(
        text: String,
        markers: List<String>,
    ): String {
        val branches = splitOr(text)
        if (branches.size > 1) {
            val kept = branches.filterNot { b -> markers.any { b.contains(it) } }.map { rewrite(it, markers) }
            if (kept.isEmpty()) return "false"
            return kept.joinToString(" or ")
        }
        // An AND list is the filter's own conjunction (kinds, window, the word
        // group). NOTHING is dropped here — an AND branch is a REQUIREMENT, and
        // removing one would widen the query rather than narrow a matcher
        // family — so this only recurses, to reach the word group nested inside.
        val ands = splitAnd(text)
        if (ands.size > 1) return ands.joinToString(" and ") { rewrite(it, markers) }
        val t = text.trim()
        if (t.startsWith("(") && t.endsWith(")") && balanced(t.substring(1, t.length - 1))) {
            return "(" + rewrite(t.substring(1, t.length - 1), markers) + ")"
        }
        return text
    }

    private fun splitOr(text: String) = splitTop(text, " or ")

    private fun splitAnd(text: String) = splitTop(text, " and ")

    /** [text] split on [sep] at paren depth 0, outside quotes. */
    private fun splitTop(
        text: String,
        sep: String,
    ): List<String> {
        val parts = ArrayList<String>()
        var depth = 0
        var quoted = false
        var start = 0
        var i = 0
        while (i < text.length) {
            val c = text[i]
            when {
                c == '\\' && quoted -> {
                    i++
                }

                c == '"' -> {
                    quoted = !quoted
                }

                quoted -> {}

                c == '(' || c == '[' -> {
                    depth++
                }

                c == ')' || c == ']' -> {
                    depth--
                }

                depth == 0 && text.startsWith(sep, i) -> {
                    parts += text.substring(start, i)
                    i += sep.length
                    start = i
                    continue
                }
            }
            i++
        }
        parts += text.substring(start)
        return parts
    }

    private fun balanced(s: String): Boolean {
        var depth = 0
        var quoted = false
        var i = 0
        while (i < s.length) {
            val c = s[i]
            when {
                c == '\\' && quoted -> {
                    i++
                }

                c == '"' -> {
                    quoted = !quoted
                }

                quoted -> {}

                c == '(' -> {
                    depth++
                }

                c == ')' -> {
                    if (depth-- == 0) return false
                }
            }
            i++
        }
        return depth == 0
    }
}
