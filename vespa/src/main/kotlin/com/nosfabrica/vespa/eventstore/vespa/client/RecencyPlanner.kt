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
package com.nosfabrica.vespa.eventstore.vespa.client

import com.nosfabrica.vespa.eventstore.vespa.query.EventQuery
import com.nosfabrica.vespa.eventstore.vespa.query.EventYql

/**
 * A bare recency scan: limit'd, unranked, and with no selective dimension —
 * the REQ shape that makes the engine's match phase visit EVERY posting its
 * kinds have (measured ~100ms per million) just to keep the newest few.
 * Everything selective (ids, authors, tags, search) already prunes the match
 * phase and runs in single-digit milliseconds.
 */
internal fun EventQuery.isBareRecencyScan(): Boolean =
    (limit ?: 0) > 0 &&
        // An explicit rank profile is never a recency scan: trust-sorted
        // profiles aren't recency-ordered (windowing one drops higher-ranked
        // older hits), and the gated profile drops hits the count probe counted
        // (a window proven full of MATCHES isn't proven full of ABOVE-FLOOR
        // matches). This is also the opt-out: internal reads stamp
        // RANK_UNRANKED to skip the planner — see NostrSemanticsStore's sweep.
        ranking == null &&
        search == null &&
        // Phrases are search text (selective, relevance-ordered); notSearch is
        // NOT excluded: an exclusion-only query is still a recency scan, and
        // the count probes carry the same exclusion clause.
        phrases.isEmpty() &&
        ids.isEmpty() &&
        authors.isEmpty() &&
        owners.isEmpty() &&
        tags.isEmpty() &&
        tagsAll.isEmpty() &&
        expiresBefore == null

/**
 * Query planning for bare recency scans: find a `since` window PROVEN (by an
 * exact count probe, ~5ms) to hold at least `limit` matches, and run the query
 * inside it — same result set, ~10x less match work on a live corpus.
 *
 * Correctness is structural, not statistical: the window is anchored at the
 * query's newest end (`until`, else now), so every event outside it is
 * strictly older than every event inside — the top-`limit` of a full window IS
 * the top-`limit` of the unbounded query. A window is only used when its probe
 * says >= limit; if no ladder rung is provably full, the query runs unchanged.
 */
internal class RecencyPlanner(
    /** `VESPA_QUERY_PLANNER=0` turns the planner off. */
    val enabled: Boolean,
    private val fallbacks: SchemaFallbacks,
    private val exactCount: suspend (EventQuery) -> Int,
) {
    /**
     * Window [q] if it is a bare recency scan the match-phase `recency`
     * profile does not already cover: the profile owns the small limits
     * (probing there costs more than it saves — measured 0.6x), the planner
     * windows the limits past the profile's headroom gate, and takes
     * everything back when the serving schema lacks the profile.
     */
    suspend fun plan(q: EventQuery): EventQuery {
        if (!enabled || !q.isBareRecencyScan()) return q
        if (fallbacks.recencyProfileAvailable && EventYql.usesRecencyProfile(q)) return q
        return window(q)
    }

    /**
     * The count-probe ladder itself, shared by [plan] and the short-page rerun
     * in the recall path. Requires an unranked, limit'd [q].
     *
     * The window is proven >= limit at PROBE time; a deletion committing
     * between the probe and the windowed query can transiently shrink it below
     * the limit — one short page on a rare interleaving, which a paginating
     * client's next `until` request recovers. Accepted: reads never hold the
     * writer lock, so no probe can be atomic with its query.
     */
    suspend fun window(q: EventQuery): EventQuery {
        val anchor = q.until ?: (System.currentTimeMillis() / 1000)
        for (window in PROBE_WINDOWS) {
            val since = anchor - window
            // An existing `since` at least this tight makes the rung (and any
            // wider one) pointless — the query is already windowed.
            if (q.since != null && since <= q.since) return q
            // A failed probe just means "don't window" — planning is an
            // optimization; the real query still carries its own guarantees.
            // Cancellation is NOT a failed probe: swallowing it would enqueue
            // one more engine request on a job that is already dead.
            val matches =
                try {
                    exactCount(q.copy(since = since, limit = null))
                } catch (e: kotlin.coroutines.cancellation.CancellationException) {
                    throw e
                } catch (_: Exception) {
                    return q
                }
            if (matches >= q.limit!!) return q.copy(since = since)
        }
        return q
    }

    private companion object {
        /**
         * The probe ladder, in seconds before the query's anchor: an hour, a
         * day, a month. Geometric so a live corpus exits on the first rung
         * that fits its event rate, and a probe miss costs one more ~5ms
         * count, not a rescan.
         */
        val PROBE_WINDOWS = longArrayOf(3_600L, 86_400L, 2_592_000L)
    }
}
