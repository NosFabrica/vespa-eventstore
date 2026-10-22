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
package com.nosfabrica.vespa.eventstore.search

import com.nosfabrica.vespa.eventstore.engine.LinkWeights
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlin.math.abs
import kotlin.math.pow
import kotlin.math.sign

/**
 * RERANK A SEARCH PAGE BY HOW MUCH THE OBSERVER'S NETWORK ENGAGED WITH EACH
 * HIT — a PROTOTYPE, opt-in per query with the NIP-50 token
 * `sort:links[:<strength>]`.
 *
 * The default `search` profile scores a hit by its text times its AUTHOR's
 * trust (`floored_text_score() * wot_mult() * recency_mult()` in event.sd).
 * That says nothing about whether anyone the observer trusts cared about the
 * note. This reads it: the events that LINK to each hit — `e:<id>`, and
 * `a:<kind:pubkey:d>` for an addressable one — written by authors the same
 * lens clears ([EventQuery.minRank], the read's own floor), scored PER PERSON
 * for the whole window in one engine call
 * ([com.nosfabrica.vespa.eventstore.engine.EventIndex.scoreLinks];
 * [com.nosfabrica.vespa.eventstore.engine.LinkWeights] has the three layers:
 * each repeat of a kind worth half the one before, at most [LinkWeights
 * .personCap] per person, times that person's trust; the hit's own author left
 * out; a zap counted for its `P` zapper) — a hit's ENGAGEMENT — and turned
 * into a bounded multiplier on the engine's own score ([multiplier]). A like
 * from an author trusted 90 is worth 0.9; from one barely over the floor, 0.02.
 *
 * WHY A MULTIPLIER ON THE ENGINE'S SCORE, not a new score: the engine's number
 * already carries the text tiers, the author's trust and recency on one
 * scale, and the store has no business re-deriving any of them. Scaling it
 * keeps every one of those decisions and lets engagement move a hit only as
 * far as [strength] and [maxBoost] allow — at the default a hit needs ~3
 * people's worth of trust to go ×1.7 (three fully trusted likers, six at 50),
 * and nothing takes it past ×[maxBoost] (reached at 8).
 *
 * WHY BOUNDED: a search page's text relevance comes in TIERS — a primary-field
 * match (~130,000 text score) over a body match (~4,000), 30–50× apart in the
 * final score — while inside a tier neighbours are effectively tied (median
 * ratio 1.00; the best is 1–2× the tier's median, from author trust and
 * recency). Measured on 12 captured searches (benchmark/README.md, "Trusted
 * links per search hit"). An uncapped curve (×31 at ~960 trusted links) lifted
 * body matches over primary-field ones on 4 of 12 pages; at ×3 a hit can only
 * pass hits already within 3× of it — the weakest primary-field matches just
 * across the border, never the strong ones — while it still decides freely
 * among the near-ties inside its own tier, which is where "highlight the
 * posts people engaged with" belongs.
 *
 * WHY A WINDOW: the signal depends on the observer, so it cannot be stored per
 * event, and a rank profile cannot read another document's links — it is a
 * second query over the first one's head. The read is widened to [window]
 * hits (when it asked for fewer), the head is rescored and re-sorted, and the
 * page is cut back to what was asked. A hit outside the engine's top [window]
 * can never be lifted in: that is the price of one round trip instead of a
 * corpus-wide join. Measured cost of the extra query: 10–40 ms for a 20–100
 * hit window on a local Vespa, tens of ms at production scale
 * (benchmark/README.md, "Trusted links per search hit").
 *
 * NO LENS, NO SIGNAL: a read with no resolved trust service has no web of
 * trust to filter the links by, and an unfiltered count is exactly the
 * popularity contest the gate exists to keep out — so it is served as if the
 * token were absent (still cut back to its limit).
 *
 * PER-KIND WEIGHTS ([LinkWeights]) are where the policy lives, and the seam for
 * what comes next — zaps by amount, likes, reposts, 5-star ratings. The default
 * ([DEFAULT_WEIGHTS]) is the minimal one: every link +1, a NIP-56 report (kind
 * 1984) −1, so a note that drew only reports scores BELOW one that drew nothing.
 * A negative total shrinks the engine's score by the same curve a positive one
 * grows it ([multiplier]): demoted, never sign-flipped, never dropped.
 */
class LinkRerank(
    /** How many of the engine's top hits are rescored; a read asking for fewer is widened to this. */
    val window: Int = DEFAULT_WINDOW,
    /** The exponent on `1 + |engagement|` when the token names none (`sort:links`). */
    val strength: Double = DEFAULT_STRENGTH,
    /** The multiplier's ceiling, and its reciprocal the floor: how far engagement may move a hit's score either way. */
    val maxBoost: Double = DEFAULT_MAX_BOOST,
    /** What one link is worth by its kind — the policy; see the class KDoc. */
    val weights: LinkWeights = DEFAULT_WEIGHTS,
) {
    init {
        require(window > 0) { "window must be positive" }
        require(maxBoost >= 1.0 && maxBoost.isFinite()) { "maxBoost must be finite and at least 1" }
        require(strength.isFinite() && strength >= 0.0) { "strength must be finite and non-negative" }
    }

    /**
     * The strength [filters] asked for, or null when none asked for the rerank.
     * `sort:links` takes [strength]; `sort:links:<x>` takes x, so a sweep can
     * move it per query with no redeploy (the way `rankAb` moves rank features).
     * An unparseable or negative x is ignored, like any unknown extension value.
     */
    fun requested(filters: List<Filter>): Double? {
        for (f in filters) {
            val sort = SearchQuery.parse(f.search ?: continue).extensions["sort"] ?: continue
            if (sort == TOKEN) return strength
            if (sort.startsWith("$TOKEN:")) {
                return sort.removePrefix("$TOKEN:").toDoubleOrNull()?.takeIf { it.isFinite() && it >= 0.0 } ?: strength
            }
        }
        return null
    }

    /** [q] widened to the window when it asks for fewer; an unlimited read already holds it. */
    fun widened(q: EventQuery): EventQuery = if (q.limit != null && q.limit in 1 until window) q.copy(limit = window) else q

    /**
     * The query whose served set IS the trusted linkers: [lens]'s observer and
     * resolved service through the observer gate's profile, at [lens]'s own
     * floor. Null when [lens] resolved no trust service ("no lens, no signal").
     */
    fun linkersOf(lens: EventQuery): EventQuery? {
        if (lens.observer == null || lens.rankKey == null) return null
        return EventQuery(
            observer = lens.observer,
            rankKey = lens.rankKey,
            followersKey = lens.followersKey,
            minRank = lens.minRank ?: DEFAULT_MIN_RANK,
            ranking = EventYql.RANK_RECENCY_GATED,
            // An expired link is a link the store would no longer serve.
            notExpiredAt = lens.notExpiredAt,
            nowSecs = lens.nowSecs,
        )
    }

    companion object {
        /** The `sort:` value that asks for it. */
        const val TOKEN = "links"

        /** Rescored head: the measured page sizes of the cost study, 20–100. */
        const val DEFAULT_WINDOW = 100

        /** See the class KDoc: ×1.7 at ~3 fully trusted people, the ceiling at 8. */
        const val DEFAULT_STRENGTH = 0.5

        /** See the class KDoc, "WHY BOUNDED": inside a tier engagement decides, across one it nudges. */
        const val DEFAULT_MAX_BOOST = 3.0

        /** NIP-56: a report names the event it reports with an `e` tag. */
        const val REPORT_KIND = 1984

        /** Every link +1, a report −1 — the minimal weighting; see the class KDoc. */
        val DEFAULT_WEIGHTS = LinkWeights(byKind = mapOf(REPORT_KIND to -1.0))

        /**
         * `(1 + |e|) ^ (strength × sign(e))`, clamped to [1/maxBoost, maxBoost]:
         * 1 at no engagement, growing with diminishing returns up to the ceiling,
         * and the reciprocal for the same amount of negative engagement — so a
         * report outweighing the likes costs a hit what the likes would have
         * bought it, never a sign flip on its score.
         */
        fun multiplier(
            engagement: Double,
            strength: Double,
            maxBoost: Double = DEFAULT_MAX_BOOST,
        ): Double = (1.0 + abs(engagement)).pow(strength * sign(engagement)).coerceIn(1.0 / maxBoost, maxBoost)
    }
}
