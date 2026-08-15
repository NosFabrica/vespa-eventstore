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

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.engine.query.FuzzyWordGroup

/** Whether [this] recalls through one of the observer-gate profiles. */
internal fun EventQuery.usesGatedProfile(): Boolean = ranking == EventYql.RANK_RECENCY_GATED || ranking == EventYql.RANK_RECENCY_GATED_EXACT

/**
 * Compatibility nets for a serving schema older than this build. `deployIfAbsent`
 * never redeploys onto a serving cluster, so the schema may predate the
 * `recency` rank profile, the `recency_gated*` profiles, the near
 * (prefix/fuzzy) attribute fields, or the `dedup` document-summary. Each net
 * watches for the FIRST 400 naming its feature, flips a flag for the life of
 * this client, and reruns the query demoted to the pre-feature behavior —
 * instead of failing every REQ. A schema redeploy plus restart restores the
 * feature path.
 *
 * The demotions FAIL OPEN by design: a missing gated profile serves the feed
 * ungated (the pre-gate behavior), consistent with how every other missing
 * trust input degrades.
 */
internal class SchemaFallbacks {
    companion object {
        /**
         * What the document API answers when the schema lacks a fed field —
         * verbatim from Vespa 29.3: `Field 'name_near' is not defined in
         * document type 'event'`. The YQL parser's wording for the same
         * schema gap is different (`does not exist`), which is why the read
         * and write predicates cannot share one string.
         */
        const val NOT_IN_DOCUMENT_TYPE = "is not defined in document type"

        /**
         * The YQL parser's wording for a column the serving schema lacks —
         * verbatim from Vespa 8: `Field 'name_near' does not exist.` The READ
         * side's anchor, and deliberately not [NOT_IN_DOCUMENT_TYPE]: the
         * document API and the query parser word the same gap differently, which
         * is why the two directions cannot share one string.
         */
        fun missingFieldPhrase(field: String): String = "Field '$field' does not exist"
    }

    @Volatile var recencyProfileAvailable = true
        private set

    @Volatile var gatedProfileAvailable = true
        private set

    @Volatile var nearFieldsAvailable = true
        private set

    @Volatile var bodyGramAvailable = true
        private set

    @Volatile var dedupSummaryAvailable = true
        private set

    fun markDedupSummaryMissing() {
        dedupSummaryAvailable = false
    }

    fun markNearFieldsMissing() {
        nearFieldsAvailable = false
    }

    /**
     * Whether this failure is a schema predating the near columns REFUSING A
     * FEED. Not the read side's predicate: the YQL parser says `Field 'x' does
     * not exist` while the document API says `is not defined in document type`.
     * One flag serves both directions — it is the same schema, so whichever path
     * discovers the gap spares the other a failed round trip.
     *
     * The phrase is matched EXACTLY, not just the field name, unlike the read
     * side. A false positive here strips the near columns off every subsequent
     * write for the life of the process, so a data-shaped 400 merely mentioning
     * the field ("invalid value for field 'name_near'") would silently and
     * permanently degrade search. Measured against Vespa 29.3.
     */
    fun isMissingNearField(message: String?): Boolean =
        message != null &&
            message.contains("400") &&
            message.contains(NOT_IN_DOCUMENT_TYPE) &&
            FuzzyWordGroup.ALL_NEAR_FIELDS.any { message.contains(it) }

    /** Whether this 400 names the missing `dedup` summary class — proof the attempt used it. */
    fun isMissingDedupSummary(e: IllegalArgumentException): Boolean = e.message?.contains("400") == true && e.message?.contains(EventYql.SUMMARY_DEDUP) == true

    /** [q] rebuilt for a schema without the `recency` profile — a no-op while the profile serves. */
    fun demoteRecency(q: EventQuery): EventQuery = if (!recencyProfileAvailable && EventYql.usesRecencyProfile(q)) q.copy(ranking = EventYql.RANK_UNRANKED) else q

    /**
     * [q] rebuilt for a schema without the gated profiles — a no-op while they
     * serve. Both demotions drop the gate (fail open, the pre-gate behavior);
     * they differ in what preserves the query's ORDER.
     *
     * TERMLESS recall demotes to a RANKING-FREE query, NOT
     * [EventYql.RANK_UNRANKED]: the fallback must regain the recency profile
     * and the count-probe planner a plain query would have (both key on
     * `ranking == null`), or every legacy-schema feed query would run as a
     * bare unranked scan.
     *
     * A `sort:recent` SEARCH cannot take that demotion: ranking-free WITH terms
     * selects the relevance profiles, and the page would come back as the
     * top-`limit` by relevance — re-sorted by date client-side, so it reads
     * chronological while silently being the wrong `limit` events.
     * [EventYql.RANK_UNRANKED] keeps the recall and the `order by created_at
     * desc` the token asked for; the planner opt-out it implies costs nothing
     * here, since a term-bearing query is not a bare recency scan either way.
     */
    fun demoteGated(q: EventQuery): EventQuery =
        when {
            gatedProfileAvailable || !q.usesGatedProfile() -> q
            q.search.isNullOrBlank() && q.phrases.isEmpty() -> q.copy(ranking = null)
            else -> q.copy(ranking = EventYql.RANK_UNRANKED)
        }

    /** [q] rebuilt for a schema without the near attribute fields — a no-op while they serve. */
    fun demoteNear(q: EventQuery): EventQuery = if (!nearFieldsAvailable && q.nearMatching) q.copy(nearMatching = false) else q

    /** [q] rebuilt for a schema without `search_text_gram` — a no-op while it serves. */
    fun demoteBodyGram(q: EventQuery): EventQuery = if (!bodyGramAvailable && q.bodyGramMatching) q.copy(bodyGramMatching = false) else q

    /**
     * Both column demotions. They are SEPARATE schema generations — every schema
     * carrying the near columns predates `search_text_gram` — so a schema can
     * lack either, or both, and demoting them together would strip name/title
     * prefix reach from a schema that still has it.
     */
    private fun demoteSchema(q: EventQuery): EventQuery = demoteBodyGram(demoteNear(q))

    /**
     * Run [attempt] with the rank-profile nets: a 400 naming the `recency` or
     * `recency_gated*` profile flips the matching flag and reruns the demoted
     * query. Any other failure propagates untouched.
     */
    suspend fun <T> withProfileFallback(
        q: EventQuery,
        attempt: suspend (EventQuery) -> T,
    ): T =
        try {
            // demoteGated FIRST: it strips the ranking, which is what lets
            // demoteRecency (and the profile selection it guards) see the
            // fallback as the plain query it now is.
            attempt(demoteRecency(demoteGated(q)))
        } catch (e: IllegalArgumentException) {
            // The search path's status guard is a require(), hence
            // IllegalArgument. No flag re-read: a demoted attempt is unranked
            // and can never 400 naming the profile, so the message alone proves
            // the attempt used it, while re-reading the flag would race a
            // concurrent query's flip into a spurious failure. The two nets
            // can't cross-fire — a gated query never satisfies
            // usesRecencyProfile, and a plain recency 400 never says
            // "recency_gated".
            val is400 = e.message?.contains("400") == true
            when {
                is400 && q.usesGatedProfile() && e.message?.contains(EventYql.RANK_RECENCY_GATED) == true -> {
                    gatedProfileAvailable = false
                    attempt(demoteRecency(demoteGated(q)))
                }

                is400 && EventYql.usesRecencyProfile(q) && e.message?.contains(EventYql.RANK_RECENCY) == true -> {
                    recencyProfileAvailable = false
                    attempt(demoteRecency(q))
                }

                else -> {
                    throw e
                }
            }
        }

    /**
     * Run [attempt] with the near-fields net: a 400 naming one of
     * [FuzzyWordGroup.ALL_NEAR_FIELDS] flips [nearFieldsAvailable] and reruns
     * the demoted query. Only queries carrying a search term can hit this, but
     * the net wraps every path that routes a term.
     */
    suspend fun <T> withNearFallback(
        q: EventQuery,
        attempt: suspend (EventQuery) -> T,
    ): T {
        // TWO demotions are reachable, so the net retries twice: the near
        // columns and `search_text_gram` are independent schema generations and
        // a schema old enough lacks both. Each pass can only ever flip a flag
        // that was still set, and flipping one strips its clauses from the next
        // attempt, so the loop cannot spin — a given column can be named at most
        // once.
        repeat(2) {
            try {
                return attempt(demoteSchema(q))
            } catch (e: IllegalArgumentException) {
                if (!flipMissingColumn(q, e)) throw e
            }
        }
        return attempt(demoteSchema(q))
    }

    /**
     * Flip the availability flag for whichever compatibility column a 400 names,
     * answering whether this net should retry.
     *
     * Keyed on the QUERY's intent ([EventQuery.nearMatching] /
     * [EventQuery.bodyGramMatching]) and the message, never on the flags: the
     * message already proves the attempt ran WITH those clauses, since a demoted
     * attempt cannot 400 naming a field it never sent, while re-reading a flag
     * would race a concurrent query's flip into a spurious failure (the same
     * reasoning as withProfileFallback).
     *
     * ANCHORED on the parser's exact wording, not on the field name alone, and
     * that anchor is load-bearing. Vespa ECHOES THE QUERY in some 400s —
     * MEASURED on Vespa 8 (2026-08-15), a stray syntax error answers
     * `no viable alternative at input '(name_near contains ({prefix:true}"x"))
     * and'`, which contains both "400" and a near-field name while saying
     * nothing about a missing column. On a bare `contains` check that flips
     * [nearFieldsAvailable] for the LIFE OF THE CLIENT, so one malformed query
     * would silently end prefix and typo recall until the process restarts. The
     * write side's [isMissingNearField] has always required its own literal
     * phrase for exactly this reason; this is the read side's equivalent.
     */
    private fun flipMissingColumn(
        q: EventQuery,
        e: IllegalArgumentException,
    ): Boolean {
        val message = e.message
        if (q.search.isNullOrBlank() || message?.contains("400") != true) return false

        fun names(unknown: List<String>) = unknown.any { message.contains(missingFieldPhrase(it)) }
        if (q.nearMatching && names(FuzzyWordGroup.ALL_NEAR_FIELDS)) {
            nearFieldsAvailable = false
            return true
        }
        if (q.bodyGramMatching && names(FuzzyWordGroup.PHRASE_GRAM_FIELDS)) {
            bodyGramAvailable = false
            return true
        }
        return false
    }
}
