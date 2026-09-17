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

import com.nosfabrica.vespa.eventstore.benchmark.bench.SearchTrace
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * DOES A CANDIDATE RANK PROFILE SERVE THE SAME PAGE? — the other half of
 * [SearchTrace], which times profiles but cannot say what a faster one costs
 * in answers.
 *
 * A two-phase rewrite (a cheap first phase that selects, the shipped
 * expression restored in the second) is only sound if the hits the cheap phase
 * ADMITS are the hits the shipped expression would have ranked highest. That
 * is an empirical question about one corpus, and this is how it is asked:
 * send the store's OWN query ([EventYql.build], the rule `rankAb` and
 * `searchTrace` already follow), once per profile, and diff the pages —
 * position by position on id, and on relevance to six decimals.
 *
 * Reported per term: the length of the identical PREFIX, how many of the
 * baseline's ids the candidate holds anywhere on the page, and the first
 * position that differs with both ids and both scores, so a difference can be
 * read rather than merely counted.
 *
 *     VESPA_URL=http://localhost:8080 SEARCH_OBSERVER=<hex> \
 *       ./gradlew :benchmark:rankPageDiff --args="text=cand_text_b bitcoin nostr"
 *
 * Arguments are `baseline=candidate` pairs and then terms; with no pair it
 * diffs `text` against `cand_text_b`. Env: `VESPA_URL`, `SEARCH_OBSERVER`,
 * `SEARCH_KINDS` (default 1), `SEARCH_LIMIT` (default 160 — the page the
 * relay's own web UI asks for), `PAGE_NOW` (the pinned query instant; without
 * one the wall clock moves `recency_mult` between arms and re-orders
 * near-ties — see benchmark/README.md).
 */
object RankPageDiff {
    private class Page(
        val ids: List<String>,
        val scores: List<Double>,
    )

    @JvmStatic
    fun main(args: Array<String>) {
        val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
        val observer = System.getenv("SEARCH_OBSERVER")
        val kinds = (System.getenv("SEARCH_KINDS") ?: "1").split(",").mapNotNull { it.trim().toIntOrNull() }
        val limit = System.getenv("SEARCH_LIMIT")?.toIntOrNull() ?: 160
        // Pinned, not read from the clock: recency_mult() is a factor of the
        // shipped first phase, so an unpinned instant moves both arms apart.
        val now = System.getenv("PAGE_NOW")?.toLongOrNull() ?: (System.currentTimeMillis() / 1000)

        val pairs = args.filter { it.contains('=') }.map { it.substringBefore('=') to it.substringAfter('=') }
        val terms = args.filterNot { it.contains('=') }.ifEmpty { listOf("bitcoin", "nostr") }
        val arms = pairs.ifEmpty { listOf(EventYql.RANK_TEXT to "cand_text_b") }

        println("cluster $url, kinds=$kinds, limit=$limit, now=$now, observer=${observer ?: "(none)"}")
        for ((baseline, candidate) in arms) {
            println()
            println("=== $baseline  vs  $candidate ===")
            println("  %-16s %7s %9s %9s  %s".format("term", "prefix", "same ids", "hits", "first difference"))
            for (term in terms) {
                val q =
                    EventQuery(
                        kinds = kinds,
                        limit = limit,
                        search = term,
                        observer = observer,
                        minRank = observer?.let { 0.0 },
                        nowSecs = now,
                    )
                val built = EventYql.build(q) ?: error("query for '$term' provably matches nothing")
                val a = page(url, built.yql, built.params, baseline)
                val b = page(url, built.yql, built.params, candidate)
                val prefix =
                    a.ids
                        .zip(b.ids)
                        .takeWhile { (x, y) -> x == y }
                        .size
                val shared = a.ids.count { it in b.ids.toSet() }
                val where =
                    if (prefix >= minOf(a.ids.size, b.ids.size)) {
                        if (a.ids.size == b.ids.size) "identical" else "identical prefix, ${a.ids.size} vs ${b.ids.size} hits"
                    } else {
                        "#${prefix + 1}: ${a.ids[prefix].take(8)}(%.4f)".format(a.scores[prefix]) +
                            " -> ${b.ids[prefix].take(8)}(%.4f)".format(b.scores[prefix])
                    }
                println("  %-16s %7d %9d %9d  %s".format(term, prefix, shared, a.ids.size, where))
            }
        }
    }

    private fun page(
        url: String,
        yql: String,
        params: Map<String, String>,
        profile: String,
    ): Page {
        val hits =
            SearchTrace
                .post(url, yql, params, profile)["root"]
                ?.jsonObject
                ?.get("children")
                ?.jsonArray
                .orEmpty()
        val ids =
            hits.map {
                it.jsonObject["fields"]
                    ?.jsonObject
                    ?.get("id")
                    ?.jsonPrimitive
                    ?.content ?: "?"
            }
        val scores =
            hits.map {
                it.jsonObject["relevance"]
                    ?.jsonPrimitive
                    ?.content
                    ?.toDoubleOrNull() ?: 0.0
            }
        return Page(ids, scores)
    }
}
