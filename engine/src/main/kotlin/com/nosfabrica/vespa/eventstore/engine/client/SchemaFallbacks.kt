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
    }

    @Volatile var recencyProfileAvailable = true
        private set

    @Volatile var gatedProfileAvailable = true
        private set

    @Volatile var nearFieldsAvailable = true
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
     * FEED. Deliberately not the same predicate as the read side's: the query
     * path fails with `Field 'x' does not exist` out of the YQL parser, while
     * the document API answers `Field 'x' is not defined in document type
     * 'event'` — and unlike a query, which merely loses recall, a rejected
     * document fails the whole insert.
     *
     * ONE flag for both directions, not two: it is the same schema, so
     * whichever path discovers the columns are missing spares the other its own
     * failed round trip.
     *
     * The phrase is matched EXACTLY, not just the field name, and that
     * asymmetry with the read side is deliberate. A false positive here is
     * worse than a failure: it would strip the near columns off every
     * subsequent write for the life of the process, so a data-shaped 400 that
     * merely mentions the field ("invalid value for field 'name_near'") would
     * silently and permanently degrade search instead of surfacing. Failing
     * loudly on an unrecognised 400 is diagnosable; quietly writing worse
     * documents is not. Measured against Vespa 29.3.
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
     * serve. Demotes to a RANKING-FREE query, NOT [EventYql.RANK_UNRANKED]:
     * the fallback must regain the recency profile and the count-probe planner
     * a plain query would have (both key on `ranking == null`), or every
     * legacy-schema feed query would run as a bare unranked scan.
     */
    fun demoteGated(q: EventQuery): EventQuery = if (!gatedProfileAvailable && q.usesGatedProfile()) q.copy(ranking = null) else q

    /** [q] rebuilt for a schema without the near attribute fields — a no-op while they serve. */
    fun demoteNear(q: EventQuery): EventQuery = if (!nearFieldsAvailable && q.nearMatching) q.copy(nearMatching = false) else q

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
            // IllegalArgument. No flag re-read here: a demoted attempt was
            // unranked and can never 400 naming the profile, so this match
            // already proves the attempt used it — and re-reading the flag
            // would race a concurrent query's flip into a spurious failure.
            // The two nets can't cross-fire: a gated query never satisfies
            // usesRecencyProfile, and a plain recency 400's message never
            // contains "recency_gated".
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
    ): T =
        try {
            attempt(demoteNear(q))
        } catch (e: IllegalArgumentException) {
            // The message check proves the attempt ran WITH the near clauses (a
            // demoted attempt cannot 400 naming a field it never sent), so no
            // flag re-read — same reasoning as withProfileFallback.
            val missingField = e.message?.contains("400") == true && FuzzyWordGroup.ALL_NEAR_FIELDS.any { e.message?.contains(it) == true }
            if (q.search.isNullOrBlank() || !q.nearMatching || !missingField) throw e
            nearFieldsAvailable = false
            attempt(demoteNear(q))
        }
}
