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
 * lens clears ([EventQuery.minRank], the read's own floor), counted per kind in
 * ONE grouping for the whole window ([com.nosfabrica.vespa.eventstore.engine.EventIndex.countLinks]),
 * folded into an ENGAGEMENT number by [kindWeight], and turned into a
 * multiplier on the engine's own score ([multiplier]).
 *
 * WHY A MULTIPLIER ON THE ENGINE'S SCORE, not a new score: the engine's number
 * already carries the text tiers, the author's trust and recency on one
 * scale, and the store has no business re-deriving any of them. Scaling it
 * keeps every one of those decisions and lets engagement move a hit only as
 * far as [strength] allows — at the default a hit needs ~3 trusted links to
 * double, ~99 to go ×10.
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
 * PER-KIND WEIGHTS are the seam for what comes next — zaps by amount, likes,
 * reports as a NEGATIVE weight, reposts, 5-star ratings. Today every kind
 * weighs 1 and engagement is a plain link count; a negative total shrinks the
 * score by the same curve it grows by ([multiplier]).
 */
class LinkRerank(
    /** How many of the engine's top hits are rescored; a read asking for fewer is widened to this. */
    val window: Int = DEFAULT_WINDOW,
    /** The exponent on `1 + |engagement|` when the token names none (`sort:links`). */
    val strength: Double = DEFAULT_STRENGTH,
    /** The linking kinds counted; empty = every kind. */
    val kinds: List<Int> = emptyList(),
    /** One link's worth by its kind. 1 for all today; see the class KDoc. */
    val kindWeight: (Int) -> Double = { 1.0 },
) {
    init {
        require(window > 0) { "window must be positive" }
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
            kinds = kinds,
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

    /** The `tag_index` keys that point at a hit: its id, and its address when it has one. */
    fun keysOf(
        id: String,
        address: String?,
    ): List<String> = if (address == null) listOf("e:$id") else listOf("e:$id", "a:$address")

    /** One hit's engagement: every link under every one of its keys, weighed by kind. */
    fun engagement(byKind: Iterable<Map<Int, Int>>): Double = byKind.sumOf { counts -> counts.entries.sumOf { (kind, n) -> kindWeight(kind) * n } }

    companion object {
        /** The `sort:` value that asks for it. */
        const val TOKEN = "links"

        /** Rescored head: the measured page sizes of the cost study, 20–100. */
        const val DEFAULT_WINDOW = 100

        /** See the class KDoc: ×2 at ~3 links, ×10 at ~99. */
        const val DEFAULT_STRENGTH = 0.5

        /**
         * `(1 + |e|) ^ (strength × sign(e))`: 1 at no engagement, growing with
         * diminishing returns, and the reciprocal for the same amount of negative
         * engagement — so a report outweighing the likes costs a hit what the
         * likes would have bought it, never a sign flip on its score.
         */
        fun multiplier(
            engagement: Double,
            strength: Double,
        ): Double = (1.0 + abs(engagement)).pow(strength * sign(engagement))
    }
}
