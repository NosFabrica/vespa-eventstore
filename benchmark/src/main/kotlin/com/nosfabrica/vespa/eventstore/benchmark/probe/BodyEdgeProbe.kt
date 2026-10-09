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
import com.nosfabrica.vespa.eventstore.engine.text.EdgeText
import com.nosfabrica.vespa.eventstore.engine.text.NearText
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * PROTOTYPE A/B (#161): the body's partial-word reach as the trigram PHRASE on
 * `search_text_gram` (shipped) against one fed EDGE N-GRAM term on
 * `search_text_edge` ([EdgeText]), on one cluster holding both columns.
 *
 * Two halves per term:
 *  - CLAUSE: each net alone, unranked, `hits=0` — what the matcher costs and
 *    what it recalls, plus the two set differences (`phrase and !edge` is the
 *    recall the edge field gives up: mid-word hits; `edge and !phrase` is what
 *    it adds). Engine `querytime`, so the HTTP round trip is not in it.
 *  - STORE: the store's own assembled query ([EventYql.build], as production
 *    sends it, ranked, `limit`) with [EventQuery.bodyEdgeMatching] off and on —
 *    what a REQ would feel.
 *
 * Medians of `PROBE_REPS` (default 7) after one untimed warm-up pass, so the
 * first-touch page-in of a posting list is not what gets compared.
 *
 *     VESPA_URL=http://localhost:8080 PROBE_KINDS=1 ./gradlew :benchmark:bodyEdgeProbe --args="tarantella bitcoin bitc"
 *
 * Every row also prints as `ROW key=value …` for collection across corpus sizes.
 */
object BodyEdgeProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
        val kinds = (System.getenv("PROBE_KINDS") ?: "1").split(",").mapNotNull { it.trim().toIntOrNull() }
        val reps = (System.getenv("PROBE_REPS")?.toIntOrNull() ?: 7).coerceAtLeast(1)
        val limit = System.getenv("PROBE_LIMIT")?.toIntOrNull() ?: 10
        val label = System.getenv("PROBE_LABEL") ?: "-"
        val terms = args.toList().ifEmpty { listOf("tarantella", "bitcoin", "bitc", "nostr", "freedom") }
        val kindClause = if (kinds.isEmpty()) "" else "kind in (${kinds.joinToString(",")}) and "
        val total = clause(url, "true", reps).second
        println("cluster $url, corpus=$total docs, kinds=$kinds, limit=$limit, reps=$reps, label=$label")
        println("%-14s %-34s %10s %10s".format("term", "variant", "engine ms", "matches"))
        for (term in terms) {
            // The compiler's own floor and refusal rule (FuzzyWordGroup.phraseGramClause):
            // at least two trigrams, all alphanumeric. Anything else is a clause it
            // never sends, so the comparison would be meaningless (and a quote
            // would break the YQL); such terms are skipped, not approximated.
            val folded = NearText.foldAccents(term)
            if (folded.length < 4 || !folded.all(Char::isLetterOrDigit)) {
                println("%-14s skipped: the compiler sends no body phrase for it".format(term))
                continue
            }
            val grams = (0..folded.length - 3).map { folded.substring(it, it + 3) }
            val phrase = "search_text_gram contains phrase(${grams.joinToString(", ") { "\"$it\"" }})"
            val edgeTerm = EdgeText.queryTerm(term)
            val rows = ArrayList<Triple<String, Double, Long>>()

            fun clauseRow(
                name: String,
                where: String,
            ) {
                val (ms, n) = clause(url, kindClause + where, reps)
                rows += Triple(name, ms, n)
            }
            clauseRow("clause: phrase", phrase)
            if (edgeTerm != null) {
                val edge = "search_text_edge contains \"$edgeTerm\""
                clauseRow("clause: edge", edge)
                clauseRow("clause: phrase and !edge (lost)", "$phrase and !($edge)")
                clauseRow("clause: edge and !phrase (new)", "$edge and !($phrase)")
            }
            for (edgeOn in listOf(false, true)) {
                val built = EventYql.build(EventQuery(kinds = kinds, limit = limit, search = term, bodyEdgeMatching = edgeOn)) ?: continue
                val (ms, n) = timed(reps) { SearchTrace.post(url, built.yql, built.params, built.ranking) }
                rows += Triple("store ${built.ranking}: ${if (edgeOn) "edge" else "phrase"}", ms, n)
            }
            for ((name, ms, n) in rows) {
                println("%-14s %-34s %10.1f %10d".format(term, name, ms, n))
                println("ROW label=$label corpus=$total term=$term variant=\"$name\" ms=%.1f matches=$n".format(ms))
            }
        }
    }

    /** One unranked, hits=0 count of [where]: (median engine ms, totalCount). */
    private fun clause(
        url: String,
        where: String,
        reps: Int,
    ): Pair<Double, Long> = timed(reps) { SearchTrace.post(url, "select * from event where $where", mapOf("hits" to "0"), EventYql.RANK_UNRANKED) }

    private fun timed(
        reps: Int,
        send: () -> JsonObject,
    ): Pair<Double, Long> {
        val times = ArrayList<Double>()
        var matches = 0L
        repeat(reps + 1) { i ->
            val body = send()
            matches = SearchTrace.totalCount(body)
            val seconds =
                body["timing"]
                    ?.jsonObject
                    ?.get("querytime")
                    ?.jsonPrimitive
                    ?.doubleOrNull ?: 0.0
            if (i > 0) times += seconds * 1000
        }
        times.sort()
        return times[times.size / 2] to matches
    }
}
