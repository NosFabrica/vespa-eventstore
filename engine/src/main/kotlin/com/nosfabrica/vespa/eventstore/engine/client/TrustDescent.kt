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
package com.nosfabrica.vespa.eventstore.engine.client

import com.nosfabrica.vespa.eventstore.engine.WHITESPACE
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.utils.Hex
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow

/**
 * AN EARLY STOP THAT CANNOT CHANGE THE ANSWER — the trust descent.
 *
 * A relevance search under an observer scores `text × wot_mult(trust) ×
 * recency` (event.sd, the `search` profile), and the three factors are not
 * alike: text spans ×236 across its bands, recency ×1.1, and trust spans
 * ×250,000 across ranks 0..100 (`wot_mult = 1 + (rank − floor)^2.7`). So the
 * page is, overwhelmingly, the documents of the most trusted authors — and
 * matching every posting of a common word to find that page is what cost
 * `bitcoin` 4.4s and `nostr` 16s on the production relay (the relay's
 * docs/search-latency.md), at any limit.
 *
 * The reputation parent carries `max_rank`, the best rank ANY observer gives
 * an author, imported into every event as `author_max_rank`. A query carrying
 * `author_max_rank >= T` (an [EventQuery.trustFloor], a RUNG) makes Vespa
 * drive the AND with that range: it walks the trusted authors' documents and
 * checks the word, rather than the word's postings and the gate. Measured on
 * a 1.28M-note slice of staging under the observer's real lens: `the` 154ms
 * exact, 13ms at rank ≥ 90, 35ms at the rung the bound below proved.
 *
 * THE BOUND is what makes a rung an exact answer rather than a cut. Every
 * document a rung excludes is by an author whose rank under this observer is
 * at most `T − 1` (it cannot exceed their max), so it scores at most
 * [scoreCeiling] × `wot_mult(T − 1)` — and if the page's K-th hit scores at
 * least that, no excluded document could have displaced any hit on it: the
 * page is the exact page. The K-th score also says which T it proves, so the
 * descent is two queries: [FIRST_RUNG], whose page reveals the K-th score,
 * then the rung that score proves. A page whose K-th hit is by a poorly
 * ranked author proves nothing above the floor and descends to it, where the
 * clause excludes only authors the gate deletes anyway (`max_rank < floor`
 * implies `rank < floor`): the floor rung IS today's exact answer. T decides
 * how fast the page was found, never what is on it.
 *
 * THE CEILING must be an upper bound on what ANY document can earn in text,
 * or the proof is not one. It is assembled from the schema's own weights
 * (every `query(w_…)` input the `search` profile reads, mirrored below and
 * pinned against event.sd by TrustDescentTest) plus a bound on the open-ended
 * terms — the bm25 tails, whose idf is at most ln(N+1) and whose saturation
 * is at most k1+1. It is deliberately loose (~4× the token tier): a looser
 * ceiling only lowers the rung a page proves, and a rung too low costs
 * milliseconds where a rung too high would cost the answer.
 *
 * WHAT DESCENDS: a search on the trust profile ([EventYql.RANK_SEARCH]) with
 * a usable observer, a floor above zero (an `include:spam` read has no gate,
 * so no rung is exact for it) and a limit (a page to prove). Not the keyed
 * lookups, whose match set the query names; not the explicit sorts, whose
 * scores are not this shape; and nothing at all until the store says the
 * parent field is backfilled ([VespaEventIndex.trustDescent]), since a
 * document whose author has no `max_rank` yet reads 0 and would be excluded
 * from every rung above it.
 */
object TrustDescent {
    /**
     * Where the descent starts. High enough that the walk is a fraction of a
     * percent of the corpus (authors ranked ≥ 90 by anyone wrote 0.5% of the
     * measured slice's notes), low enough that the page it returns is usually
     * the top of the exact page already (`nostr`, `bitcoin`: all ten of the
     * top ten at this rung).
     */
    const val FIRST_RUNG = 90

    // ---- event.sd, the `search` profile's inputs, mirrored ----------------
    const val W_NAME_TIER = 130_000.0
    const val W_SPLIT_TIER = 23_000.0
    const val W_NEAR_TIER = 23_000.0
    const val W_WEAK_TIER = 4_000.0
    const val W_PERFECT_TIER = 0.0
    const val W_AFFIL_TIER_TEXT = 550.0
    const val GRAM_CAP = 80.0
    const val W_IDENTITY = 50.0
    const val W_SECONDARY = 0.4
    const val W_CONTENT = 0.15
    const val W_LOCATION = 0.3
    const val W_ABOUT = 0.1
    const val W_WORDS_POP = 3_000.0
    const val W_EXACTNESS_POP = 6_000.0
    const val W_PERFECT_POP = 80_000.0
    const val W_WOT = 1.0
    const val W_WOT_POW = 2.7
    const val W_RECENCY = 0.1

    /**
     * bm25's largest possible value per matched term: its idf is at most
     * `ln(N + 1)` for a corpus of N documents — 27.7 at N = 10^12, more than
     * any store this serves — and its term-frequency saturation is at most
     * `k1 + 1` = 2.2 (Vespa's default k1 = 1.2).
     */
    const val BM25_TERM_CEILING = 2.2 * 27.7

    /**
     * Matched query terms per field, at most: a word group carries the word,
     * an adjacent-pair concatenation and the joined variant, so `matchCount`
     * of one field is at most twice the number of things the user typed.
     */
    private const val TERMS_PER_WORD = 2

    /**
     * The most `text_score()` can be for a query of [words] words and
     * phrases, every band added as if one document could earn all of them
     * (the name band and the tiered token band are separate terms and are
     * both counted; the exclusive bands are counted too, which only loosens).
     */
    fun textCeiling(words: Int): Double {
        val n = max(1, words)
        val bm25 = BM25_TERM_CEILING * n * TERMS_PER_WORD
        val primaryText = 100.0 * n * TERMS_PER_WORD + bm25 + GRAM_CAP
        return 2 * W_NAME_TIER + W_SPLIT_TIER + W_PERFECT_TIER +
            2 * W_NEAR_TIER + W_WEAK_TIER +
            2 * primaryText +
            W_IDENTITY * 2 * bm25 +
            W_AFFIL_TIER_TEXT +
            W_SECONDARY * (bm25 + GRAM_CAP) + W_CONTENT * bm25 + W_LOCATION * bm25 +
            W_ABOUT * (bm25 + GRAM_CAP)
    }

    /** The most a document can score under the `search` profile at trust multiplier 1: both phases, times the recency ceiling. */
    fun scoreCeiling(words: Int): Double = (textCeiling(words) + W_WORDS_POP + W_EXACTNESS_POP + W_PERFECT_POP) * (1 + W_RECENCY)

    /** event.sd's `wot_of(r)` for a query floor of [floor]: 0 below it, the convex curve above. */
    fun wotMult(
        rank: Double,
        floor: Double,
    ): Double = if (rank < floor) 0.0 else 1.0 + W_WOT * min(max(0.0, rank - floor), 100.0).pow(W_WOT_POW)

    /** The most a document EXCLUDED by rung [rung] can score: an author ranked at most `rung − 1`, earning the ceiling. */
    fun bound(
        rung: Int,
        floor: Double,
        words: Int,
    ): Double = scoreCeiling(words) * wotMult((rung - 1).toDouble(), floor)

    /** The floor rung: the lowest rank the gate admits, as the integer rank the clause compares. */
    fun floorRung(floor: Double): Int = ceil(floor).toInt()

    /**
     * The highest rung a page whose K-th hit scores [kth] proves, in
     * `[floorRung, FIRST_RUNG]`. The floor rung is always proven — what it
     * excludes, the gate deletes — so this never returns less.
     */
    fun provenRung(
        kth: Double,
        floor: Double,
        words: Int,
    ): Int {
        val floorRung = floorRung(floor)
        val y = kth / scoreCeiling(words)
        if (y < 1.0) return floorRung
        // wot_mult(r) <= y  <=>  r <= floor + ((y − 1) / w_wot)^(1 / pow); the rung is one above that r.
        val r = floor + ((y - 1.0) / W_WOT).pow(1.0 / W_WOT_POW)
        return (floor(r).toInt() + 1).coerceIn(floorRung, FIRST_RUNG)
    }

    /** Whether [q] is a shape the descent serves — see the class doc's WHAT DESCENDS. */
    fun descends(q: EventQuery): Boolean =
        q.trustFloor == null &&
            q.ranking == null &&
            (q.limit ?: 0) > 0 &&
            (q.minRank ?: 0.0) > 0.0 &&
            q.observer?.lowercase()?.takeIf(Hex::isHex64) != null &&
            EventYql.profileOf(q) == EventYql.RANK_SEARCH &&
            q.ids.isEmpty() && q.idWeights.isEmpty() && q.authorWeights.isEmpty()

    /** How many things the user asked for — the words that can match, plus the quoted phrases — for the ceiling's per-term tails. */
    fun words(q: EventQuery): Int =
        q.search
            .orEmpty()
            .trim()
            .split(WHITESPACE)
            .count { w -> w.any(Char::isLetterOrDigit) } + q.phrases.size
}
