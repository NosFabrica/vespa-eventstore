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
import kotlinx.coroutines.runBlocking
import kotlin.system.measureNanoTime

/**
 * The ranking-change gate (`BENCH_RANK_QUALITY=1`): runs a fixed query set
 * against TWO rank profiles on the same live corpus and reports, per term
 * class, how much the served ORDER differs (the current single-phase profile
 * is ground truth — it IS the tuned relevance) and how much each profile
 * costs. This is to ranking what ParityCheck is to recall: no ranking change
 * ships without this being green.
 *
 * Metrics per term: overlap@10 and overlap@50 (fraction of baseline's top-K
 * present in the candidate's top-K) and Kendall's tau over the shared top-50
 * (order agreement of the ids both returned). Timing: median/tails per
 * profile over BENCH_RANK_REPS reps.
 *
 * Term classes cover the known failure modes of a rerank window: common terms
 * (huge match sets — where two-phase must WIN), rare terms (smaller than the
 * window — where it must not lose), multi-word (proximity does real work),
 * and short/typo terms (gram-band-only matching, the tuned profile's
 * subtlest ordering).
 *
 * Tunables: BENCH_RANK_BASELINE ("text"), BENCH_RANK_CANDIDATE ("text2"),
 * BENCH_RANK_RERANK (override ranking.rerankCount), BENCH_RANK_REPS (60).
 */
object RankQuality {
    private val CLASSES =
        mapOf(
            "common" to listOf("nostr", "bitcoin", "relay", "key", "node"),
            "rare" to listOf("mempool", "sovereign", "ostrich", "consensus"),
            "multi-word" to listOf("nostr bitcoin", "coffee freedom", "trust graph web", "lightning wallet key"),
            "short" to listOf("web", "zap", "key"),
            "typo" to listOf("nostrr", "bitcion", "relya", "lightnig"),
        )

    fun run(url: String) =
        runBlocking {
            val baseline = System.getenv("BENCH_RANK_BASELINE") ?: "text"
            val candidate = System.getenv("BENCH_RANK_CANDIDATE") ?: "text2"
            val rerank = System.getenv("BENCH_RANK_RERANK")?.toIntOrNull()
            val reps = System.getenv("BENCH_RANK_REPS")?.toIntOrNull() ?: 60

            println("rank quality: baseline=$baseline candidate=$candidate rerank=${rerank ?: "(profile default)"} reps=$reps")
            println(
                String.format(
                    "%-11s %-22s %7s %7s %7s   %-26s %-26s",
                    "class",
                    "term",
                    "ov@10",
                    "ov@50",
                    "tau@50",
                    "$baseline latency",
                    "$candidate latency",
                ),
            )

            VespaEventStore.open(url).use { store ->
                val index = store.events

                suspend fun ids(
                    term: String,
                    profile: String,
                ): List<String> =
                    index
                        .search(EventQuery(kinds = listOf(1), search = term, ranking = profile, limit = 100, rerankCount = rerank))
                        .map { it.id }

                var worstOv10 = 1.0
                for ((cls, terms) in CLASSES) {
                    for (term in terms) {
                        val a = ids(term, baseline)
                        val b = ids(term, candidate)
                        if (a.isEmpty() && b.isEmpty()) {
                            println(String.format("%-11s %-22s %7s", cls, term, "(no matches)"))
                            continue
                        }
                        val ov10 = overlap(a, b, 10)
                        val ov50 = overlap(a, b, 50)
                        val tau = kendallTau(a.take(50), b.take(50))
                        worstOv10 = minOf(worstOv10, ov10)

                        val latA = time(reps) { ids(term, baseline) }
                        val latB = time(reps) { ids(term, candidate) }
                        println(
                            String.format(
                                "%-11s %-22s %7.2f %7.2f %7.2f   %-26s %-26s",
                                cls,
                                term,
                                ov10,
                                ov50,
                                tau,
                                latA.summary(),
                                latB.summary(),
                            ),
                        )
                        BenchResults.section("rank-quality")
                        BenchResults.record(
                            "$cls:$term",
                            "overlap10" to ov10,
                            "overlap50" to ov50,
                            "tau50" to tau,
                            "baseline_p50_us" to (latA.percentileNanos(0.5) ?: 0L) / 1000.0,
                            "candidate_p50_us" to (latB.percentileNanos(0.5) ?: 0L) / 1000.0,
                        )
                    }
                }
                println("\nworst overlap@10 across all terms: %.2f".format(worstOv10))
            }
        }

    private inline fun time(
        reps: Int,
        crossinline op: suspend () -> Any?,
    ): Latencies {
        val lat = Latencies()
        runBlocking {
            repeat((reps / 10).coerceAtLeast(3)) { op() }
            repeat(reps) { lat.record(measureNanoTime { runBlocking { op() } }) }
        }
        return lat
    }

    /** |baseline top-K ∩ candidate top-K| / |baseline top-K| (1.0 = identical membership). */
    private fun overlap(
        a: List<String>,
        b: List<String>,
        k: Int,
    ): Double {
        val ta = a.take(k).toSet()
        if (ta.isEmpty()) return 1.0
        val tb = b.take(k).toSet()
        return ta.count { it in tb }.toDouble() / ta.size
    }

    /** Kendall's tau over the ids BOTH lists contain (1.0 = same order, -1.0 = reversed). */
    private fun kendallTau(
        a: List<String>,
        b: List<String>,
    ): Double {
        val shared = a.filter { it in b.toSet() }
        if (shared.size < 2) return 1.0
        val posB = b.withIndex().associate { (i, id) -> id to i }
        var concordant = 0
        var discordant = 0
        for (i in shared.indices) {
            for (j in i + 1 until shared.size) {
                val bi = posB.getValue(shared[i])
                val bj = posB.getValue(shared[j])
                if (bi < bj) concordant++ else discordant++
            }
        }
        val total = concordant + discordant
        return if (total == 0) 1.0 else (concordant - discordant).toDouble() / total
    }
}
