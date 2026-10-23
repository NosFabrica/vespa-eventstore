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

import kotlin.math.ln
import kotlin.math.pow

/**
 * The per-topic measures of search QUALITY — what a ranking change is judged
 * on (`docs/search-evaluation.md` is the protocol; this is its arithmetic).
 * Pure functions over one ranked page, so they are unit-tested without a
 * Vespa and every number `rankAb` prints is reproducible by hand.
 *
 * A page is given as the GRADES of its hits in served order: `null` for a hit
 * no assessor has judged, `0..3` otherwise (the scale in the doc: 0 not
 * relevant, 1 marginal, 2 relevant, 3 the answer). Keeping "unjudged" distinct
 * from "judged irrelevant" is the whole point — a live corpus grows documents
 * nobody has graded, and the two readings of such a hit can disagree by more
 * than the change being measured.
 */
object RelevanceMetrics {
    /** Depth every headline measure is cut at: one page of a search UI. */
    const val DEPTH = 10

    /**
     * Exponential gain (Burges et al. 2005): a grade-3 "the answer" is worth
     * 7 grade-1 mentions, not 3. That ratio is what makes nDCG notice a
     * navigational target slipping under a page of marginal hits — exactly the
     * shape of the 2026-08-05 reports, where a linear gain barely moves.
     */
    fun gain(grade: Int): Double = (1 shl grade) - 1.0

    /**
     * nDCG@[k] over the CONDENSED page (Sakai 2007, "nDCG'"): unjudged hits
     * are removed before the cut, so a judged document from rank 14 fills the
     * slot an unjudged one held. That makes the measure robust to the corpus
     * growing under a fixed set of judgments — but it can only be read beside
     * [judgedAt], because condensing a page that is mostly unjudged measures
     * the few judged hits it happened to keep. The ideal ranking is built from
     * EVERY judgment the topic has, retrieved or not ([allGrades]), so a
     * relevant document the engine stopped recalling still costs.
     *
     * Null when the topic has no judged-relevant document: its ideal DCG is
     * zero and nDCG is undefined, not 0 or 1.
     */
    fun ndcg(
        page: List<Int?>,
        allGrades: Collection<Int>,
        k: Int = DEPTH,
    ): Double? {
        val ideal = dcg(allGrades.sortedDescending(), k)
        if (ideal <= 0.0) return null
        return dcg(page.filterNotNull(), k) / ideal
    }

    private fun dcg(
        grades: List<Int>,
        k: Int,
    ): Double = grades.take(k).withIndex().sumOf { (i, g) -> gain(g) / log2(i + 2.0) }

    /**
     * 1/rank of the first hit graded at least [minGrade], 0 when none is on
     * the page. Over the UNCONDENSED page: for a known-item topic the question
     * is where the user finds the answer, and the unjudged hits above it are
     * in the way whatever they are. Averaged over topics this is MRR — useful
     * as the old `rankAb` column, but read [successAt] first: RR is not an
     * interval scale (Fuhr 2017), so a mean of it overweights the top ranks.
     */
    fun reciprocalRank(
        page: List<Int?>,
        minGrade: Int,
    ): Double = page.indexOfFirst { (it ?: -1) >= minGrade }.let { if (it < 0) 0.0 else 1.0 / (it + 1) }

    /** 1.0 when a hit graded at least [minGrade] is in the top [k], else 0.0. Uncondensed, like [reciprocalRank]. */
    fun successAt(
        page: List<Int?>,
        minGrade: Int,
        k: Int,
    ): Double = if (page.take(k).any { (it ?: -1) >= minGrade }) 1.0 else 0.0

    /**
     * Fraction of the top [k] that carries a judgment (judged@k). The trust
     * gauge for every condensed number: below ~0.9 the page has surfaced
     * documents nobody graded and the honest next step is `rankAb --pool`,
     * not a verdict. Null for an empty page — nothing to be unsure about.
     */
    fun judgedAt(
        page: List<Int?>,
        k: Int = DEPTH,
    ): Double? {
        val top = page.take(k)
        return if (top.isEmpty()) null else top.count { it != null }.toDouble() / top.size
    }

    /**
     * Share of the top [k] held by its most frequent author — the diversity
     * guardrail the 'Jack' report asked for (13 of 40 slots were one author's
     * identical pictures). Grades cannot see it: under the Cranfield
     * assumption each hit is judged on its own, so ten copies of a relevant
     * note are ten relevant hits. Null for an empty page.
     */
    fun maxAuthorShare(
        authors: List<String>,
        k: Int = DEPTH,
    ): Double? {
        val top = authors.take(k)
        if (top.isEmpty()) return null
        return top
            .groupingBy { it }
            .eachCount()
            .values
            .max()
            .toDouble() / top.size
    }

    /**
     * Rank-biased overlap, extrapolated (Webber, Moffat & Zobel 2010, eq. 32):
     * how alike two rankings are, top-weighted by persistence [p] (0.9 puts
     * ~86% of the weight on the first 10 ranks). Needs NO judgments — it says
     * how much a change moved the pages, which is how much judging it needs,
     * and it is what replaces overlap@k + Kendall's tau: it handles lists that
     * are not conjoint (a hit only one config returned) and of unequal length.
     * 1.0 identical, 0.0 disjoint.
     */
    fun rbo(
        a: List<String>,
        b: List<String>,
        p: Double = 0.9,
    ): Double {
        if (a.isEmpty() && b.isEmpty()) return 1.0
        if (a.isEmpty() || b.isEmpty()) return 0.0
        val (short, long) = if (a.size <= b.size) a to b else b to a
        val s = short.size
        val l = long.size
        val seenShort = HashSet<String>()
        val seenLong = HashSet<String>()
        var overlap = 0
        // X_d for d in 1..l: past depth s the short list stops growing and
        // only the long list's newcomers can still find a partner in it.
        val x = IntArray(l + 1)
        for (d in 1..l) {
            if (d <= s) {
                val sv = short[d - 1]
                if (sv in seenLong) overlap++
                seenShort += sv
            }
            // Checked AFTER the short side's newcomer joined seenShort, so a
            // pair equal at the same depth is counted once, here.
            val lv = long[d - 1]
            if (lv in seenShort) overlap++
            seenLong += lv
            x[d] = overlap
        }
        var sum = 0.0
        for (d in 1..l) sum += x[d].toDouble() / d * p.pow(d)
        for (d in s + 1..l) sum += x[s].toDouble() * (d - s) / (s.toDouble() * d) * p.pow(d)
        return (1 - p) / p * sum + ((x[l] - x[s]).toDouble() / l + x[s].toDouble() / s) * p.pow(l)
    }

    private fun log2(v: Double) = ln(v) / ln(2.0)
}
