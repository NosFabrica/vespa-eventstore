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

/**
 * How well two assessors' grades of the SAME documents agree — the audit an
 * LLM-judged batch owes before its grades are trusted
 * (`docs/search-evaluation.md` §5.3: a human re-grades ≥ 20% blind, and the
 * PR reports this).
 *
 * The headline is Cohen's QUADRATIC-weighted kappa: chance-corrected, and on
 * an ordinal scale a 3-vs-2 disagreement costs a ninth of a 3-vs-0. Raw
 * agreement and within-one agreement ride beside it because kappa alone hides
 * the skew a lenient judge produces (Upadhyay et al. found humans STRICTER
 * than an LLM), and the confusion matrix shows that skew's direction: mass
 * above its diagonal is the second assessor grading higher.
 */
object Agreement {
    data class Result(
        val n: Int,
        /** Fraction graded identically. */
        val exact: Double,
        /** Fraction within one grade. */
        val withinOne: Double,
        /** Quadratic-weighted kappa; NaN when chance agreement is total (one grade used throughout). */
        val kappa: Double,
        /** `matrix[a][b]` = documents the first assessor graded a and the second b. */
        val matrix: List<List<Int>>,
    )

    fun of(
        pairs: List<Pair<Int, Int>>,
        scale: IntRange = Judgments.SCALE,
    ): Result {
        val k = scale.last - scale.first + 1
        val m = Array(k) { IntArray(k) }
        for ((a, b) in pairs) m[a - scale.first][b - scale.first]++
        val n = pairs.size
        if (n == 0) return Result(0, Double.NaN, Double.NaN, Double.NaN, m.map { it.toList() })
        val rows = m.map { it.sum() }
        val cols = (0 until k).map { j -> m.sumOf { it[j] } }

        fun w(
            i: Int,
            j: Int,
        ) = (i - j).toDouble() * (i - j) / ((k - 1).toDouble() * (k - 1))
        var observed = 0.0
        var expected = 0.0
        for (i in 0 until k) {
            for (j in 0 until k) {
                observed += w(i, j) * m[i][j]
                expected += w(i, j) * rows[i] * cols[j] / n.toDouble()
            }
        }
        return Result(
            n = n,
            exact = pairs.count { it.first == it.second }.toDouble() / n,
            withinOne = pairs.count { Math.abs(it.first - it.second) <= 1 }.toDouble() / n,
            kappa = if (expected == 0.0) Double.NaN else 1 - observed / expected,
            matrix = m.map { it.toList() },
        )
    }
}
