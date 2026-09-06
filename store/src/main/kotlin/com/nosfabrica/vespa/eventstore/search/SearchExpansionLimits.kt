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
import com.nosfabrica.vespa.eventstore.engine.async.QUERY_FANOUT

/**
 * How much of a subscription's feed the expansion may be, and how much index
 * work it may cost: a hit that nominates thousands of subjects — a 2,000-member
 * Trusted List is a normal one — must not turn a five-hit search page into a
 * five-thousand-frame flood, and a page of five such lists must not turn one
 * read into ten thousand key lookups.
 *
 * Both caps bound what is LOOKED UP, not what is found, and they truncate in
 * the pointer's OWN ORDER — the first N members it names. That is the
 * deterministic reading, and it is the useful one: a publisher orders a Trusted
 * List's members by the score it computed for them, so the first N are the N it
 * ranks highest. The alternative — "the first N we happen to hold" — would make
 * the answer depend on what a mirror had caught up on, and would let a run of
 * members this store does not have cost a lookup each anyway. Members past the
 * cap are not looked up at all, which is the point.
 *
 * THAT ORDERING IS A PUBLISHER CONVENTION, AND IT WAS CHECKED rather than
 * hoped: across the eleven Trusted Lists on the staging relay, all 180 members
 * carry a score, every score is inside quartz's 0..100 `SCORE_RANGE` (so none
 * reads back as unscored), and EVERY list is sorted descending. A publisher
 * that does not sort degrades to a deterministic-but-arbitrary top N, not to a
 * wrong one.
 *
 * Both are TRUNCATIONS, not refusals: the pointer is unaffected by either, so a
 * client that wants the whole membership reads the member tags and asks for
 * them by `#p` / `#e` / `#a` recall, which is what that recall is for —
 * provided the read asked for the pointer's kind, since a read that did not
 * never sees it (see [SearchReferenceExpansion]).
 */
data class SearchExpansionLimits(
    /** Off entirely: reads answer exactly what they matched, and nothing is spliced. */
    val enabled: Boolean = true,
    /** Subjects one pointer may bring. A page of lists spends [maxPerRequest] before this bites. */
    val maxPerEvent: Int = 100,
    /** Subjects one read may bring, across every pointer on it. */
    val maxPerRequest: Int = 1_000,
    /**
     * ENGINE ROUND TRIPS one read's expansion may spend, whatever its page
     * holds — the bound that stops a wide page turning one REQ into hundreds of
     * queries.
     *
     * WHAT IT DROPS WHEN IT BITES, because it does not fail the read: lookups
     * are planned in PAGE order, which on any page that can be sorted is
     * relevance order, so the budget is spent on the best-ranked pointers first
     * and a page too wide loses the subjects of its WORST-ranked ones. That is
     * the only defensible cut — the alternative is dropping whichever family
     * the loop reached last — and it is pinned by test rather than left to the
     * shape of the code.
     *
     * The number is per read and not per lens. One batch per pointer is the
     * shape a ranked page of Trusted Lists has (a pointer's own relevance is a
     * query-level feature, so no two differently-ranked rows can share a
     * query), so this is the page width past which subjects start going
     * missing. The trips overlap ([QUERY_FANOUT] of them in flight), so it
     * bounds cluster work rather than wall-clock.
     */
    val maxLookups: Int = 64,
    /**
     * HOW HARD A DOUBTED MEMBER SINKS — the exponent on the confidence a
     * Trusted List expressed about each member, on quartz's 0..100 scale.
     *
     * It shapes BOTH halves of a member's placement, because both are
     * confidence-driven: how far up its own rung a member sits, and where
     * inside [subjectFloorSpan] it lands under its pointer. 1.0 is linear;
     * above 1 punishes doubt harder; below 1 softens it, and as it approaches
     * 0 every confidence weighs the same and the whole block sits with its
     * pointer again.
     *
     * There is no corpus to tune this against yet — the honest default is the
     * one that applies the publisher's number as given.
     */
    val confidenceGamma: Double = 1.0,
    /**
     * HOW FAR BELOW ITS POINTER A DOUBTED SUBJECT MAY FALL, as a fraction of
     * the pointer's own relevance — or NULL for no floor at all, which is the
     * placement that came before this: every subject on its absolute rung,
     * wherever its pointer landed.
     *
     * WHY A FLOOR. `event.sd` §13's rung answers "how good is this member" and
     * cannot answer "how good is it FOR THIS QUERY": the member matched none of
     * the words — the lookup that fetched it carries none — so the pointer is
     * the only row on the page that knows the query. The rung's ceiling
     * (4,000 x wot) cannot reach a title match's (130,000 x wot) from below
     * whatever the publisher or the reader think of the person: measured on
     * staging, a `Verified Human` list ranked #10 on its title while the member
     * it is 87% sure of, ranked 100 by that reader, sat at #40 under 27 mirror
     * pages from one rank-30 bot.
     *
     * WHY A SPAN AND NOT A SHARE. A plain `pointer x confidence` was tried and
     * rejected twice over: as a placement it ejected discounted members out of
     * their band into the gap below, and even as a floor it lands them at
     * arbitrary points across a BANDED ladder (a quarter of a title match is
     * 32,500 — above the near rung, where nobody calibrated it). The span is a
     * ratio of rungs instead: [DEFAULT_SUBJECT_FLOOR_SPAN] is
     * `w_near_tier / w_name_tier`, so a member lands within ONE RUNG of its
     * pointer however doubted, ordered inside that span by its confidence, and
     * a member the publisher is sure of ties its pointer exactly.
     *
     * The arithmetic is the SCHEMA's — only it knows where the bands are, and
     * only it sees a member's own trust. This is the number handed to it.
     */
    val subjectFloorSpan: Double? = DEFAULT_SUBJECT_FLOOR_SPAN,
) {
    init {
        require(confidenceGamma > 0.0) { "confidenceGamma must be positive: $confidenceGamma" }
        require(subjectFloorSpan == null || subjectFloorSpan in 0.0..1.0) {
            "subjectFloorSpan must be a 0..1 fraction of the pointer, or null for no floor: $subjectFloorSpan"
        }
    }

    companion object {
        /**
         * `w_near_tier / w_name_tier` — one rung of `event.sd`'s text ladder,
         * and the schema's own default for `query(w_subject_floor_span)`. Kept
         * here as a number the store can pass and a test can reason with; the
         * two must move together.
         */
        const val DEFAULT_SUBJECT_FLOOR_SPAN = 0.1769

        val Default = SearchExpansionLimits()

        /** The expansion switched off — what a caller passes to get plain recall. */
        val Off = SearchExpansionLimits(enabled = false)
    }
}
