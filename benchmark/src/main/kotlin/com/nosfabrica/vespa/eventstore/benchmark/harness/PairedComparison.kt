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
package com.nosfabrica.vespa.eventstore.benchmark.harness

import kotlin.math.abs
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Is a candidate config's per-topic score really different from the
 * baseline's, or is it topic noise? The paired comparison every ranking change
 * reports (`docs/search-evaluation.md` §6): both configs ran the SAME topics,
 * so the unit of evidence is the per-topic delta, never two independent means.
 *
 * The test is Fisher's randomization (sign-flip permutation) test — Smucker,
 * Allan & Carterette (CIKM 2007) found it, the bootstrap and the t-test in
 * practical agreement at TREC's ~50 topics, and it assumes nothing about the
 * shape of a bounded, lumpy metric like nDCG. Below [EXACT_MAX] topics it
 * enumerates every sign assignment instead of sampling, which is where the
 * three tests start to disagree (their 2009 follow-up). Seeded, so a rerun on
 * the same scores prints the same p.
 */
object PairedComparison {
    /** Topics up to which the permutation distribution is enumerated (2^16 = 65 536 assignments). */
    const val EXACT_MAX = 16

    data class Result(
        val n: Int,
        val meanDelta: Double,
        /** 95% percentile-bootstrap interval of the mean delta. */
        val ciLow: Double,
        val ciHigh: Double,
        /** Two-sided randomization-test p of "the configs are exchangeable". */
        val p: Double,
        val wins: Int,
        val ties: Int,
        val losses: Int,
        /** Sample SD of the per-topic deltas — what [minimumDetectable] needs. */
        val sdDelta: Double,
    )

    /**
     * Compare paired per-topic scores: [baseline]`[i]` and [candidate]`[i]`
     * are the same topic. A |delta| under [tie] counts as a tie — a metric
     * moving in its 9th decimal is the same page.
     */
    fun compare(
        baseline: List<Double>,
        candidate: List<Double>,
        trials: Int = 10_000,
        seed: Long = 1,
        tie: Double = 1e-9,
    ): Result {
        require(baseline.size == candidate.size) { "paired scores must cover the same topics" }
        val d = candidate.indices.map { candidate[it] - baseline[it] }
        val n = d.size
        if (n == 0) return Result(0, 0.0, 0.0, 0.0, 1.0, 0, 0, 0, 0.0)
        val mean = d.average()
        val sd = if (n < 2) 0.0 else sqrt(d.sumOf { (it - mean) * (it - mean) } / (n - 1))
        val rnd = Random(seed)
        // Every comparison is on |sum|: the mean's 1/n is common to all of them.
        val observed = abs(d.sum()) - 1e-12
        val p =
            if (n <= EXACT_MAX) {
                var extreme = 0L
                val total = 1L shl n
                for (mask in 0 until total) {
                    var s = 0.0
                    for (i in 0 until n) s += if (mask shr i and 1L == 1L) -d[i] else d[i]
                    if (abs(s) >= observed) extreme++
                }
                extreme.toDouble() / total
            } else {
                var extreme = 0
                repeat(trials) {
                    var s = 0.0
                    for (v in d) s += if (rnd.nextBoolean()) -v else v
                    if (abs(s) >= observed) extreme++
                }
                (extreme + 1.0) / (trials + 1.0)
            }
        val boot =
            DoubleArray(trials) {
                var s = 0.0
                repeat(n) { s += d[rnd.nextInt(n)] }
                s / n
            }.sorted()
        return Result(
            n = n,
            meanDelta = mean,
            ciLow = boot[(0.025 * (trials - 1)).toInt()],
            ciHigh = boot[(0.975 * (trials - 1)).toInt()],
            p = p,
            wins = d.count { it > tie },
            ties = d.count { abs(it) <= tie },
            losses = d.count { it < -tie },
            sdDelta = sd,
        )
    }

    /**
     * Holm-Bonferroni step-down adjustment, in the input's order. A sweep of
     * twenty configs WILL turn up a p < 0.05 by chance (Carterette, TOIS
     * 2012); adjusting across every candidate compared against one baseline
     * in the same run keeps "significant" meaning what it says.
     */
    fun holm(p: List<Double>): List<Double> {
        val m = p.size
        val order = p.indices.sortedBy { p[it] }
        val adjusted = DoubleArray(m)
        var running = 0.0
        order.forEachIndexed { rank, i ->
            running = maxOf(running, minOf(1.0, (m - rank) * p[i]))
            adjusted[i] = running
        }
        return adjusted.toList()
    }

    /**
     * Smallest mean delta this many topics can detect at α = 0.05 two-sided
     * and 80% power, given the per-topic delta SD (the normal approximation of
     * the paired t-test: (z₀.₉₇₅ + z₀.₈) · σ / √n). Printed next to every
     * comparison so "not significant" is read as "smaller than X, if anything"
     * rather than "no effect" — and so the topic set is grown when X is larger
     * than the changes people actually make.
     */
    fun minimumDetectable(
        sdDelta: Double,
        n: Int,
    ): Double = if (n == 0) Double.NaN else (1.959964 + 0.841621) * sdDelta / sqrt(n.toDouble())
}
