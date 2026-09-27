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

/**
 * HOW a limit'd, newest-first read keeps the engine from visiting every posting
 * its filter matches just to return the newest few. All four serve the SAME
 * page — they differ only in how much of the match set the engine walks, and
 * how many round trips it takes to prove the page complete.
 *
 * `VESPA_RECENCY_STRATEGY` selects one (`full_scan`, `match_phase`,
 * `count_probe`, `speculative`); the default is [SPECULATIVE]. MEASURED on a
 * 44M-doc relay corpus (2026-09-26, benchmark/README.md "Recency strategies"):
 * 3.6-455x faster than [MATCH_PHASE] on every broad feed, gated or not — the
 * gated global feed 368 -> 8 ms, a gated deep page 3.6 s -> 8 ms — for 8 ms on
 * a quiet 50-author feed that was already cheap. `match_phase` restores the
 * behavior this store shipped with.
 */
enum class RecencyStrategy {
    /**
     * A — no planning at all: the whole match set is walked and sorted
     * (`unranked` with Vespa's sorting degrader off, or the full-scan gated
     * profile). One query, trivially exact, cost linear in the match set.
     */
    FULL_SCAN,

    /**
     * B — the shipped behavior: the schema's match-phase profiles (`recency`,
     * `recency_gated`) cut to the newest ~max-hits candidates per node, and
     * [RecencyPlanner]'s count probes window only BARE scans the profiles do
     * not cover (deep `until`, limits past the band).
     */
    MATCH_PHASE,

    /**
     * C — the count-probe ladder generalized to every windowable read
     * ([isWindowable]: selective filters and the trust gate included): an
     * exact count proves a `since` window holds `limit` matches, then the
     * query runs inside it. A gated read probes with a GATED count, so the
     * window is proven full of above-floor matches, not merely of matches.
     */
    COUNT_PROBE,

    /**
     * D — the windowed query IS the probe: run the read inside a small
     * `since` window; a full page proves itself (everything outside the window
     * is strictly older than everything in it), a short one widens the window
     * by the rate it just observed. Runs the read unwindowed, planned as
     * [MATCH_PHASE] plans it, once the rate projects past
     * [RecencyPlanner.NARROW_HORIZON], after [RecencyPlanner.MAX_ATTEMPTS], or
     * — for the whole read — on a schema without the `recency` profile.
     */
    SPECULATIVE,
    ;

    companion object {
        fun fromEnv(): RecencyStrategy =
            System.getenv("VESPA_RECENCY_STRATEGY")?.let { v -> entries.firstOrNull { it.name.equals(v.trim(), ignoreCase = true) } }
                ?: SPECULATIVE
    }
}

/**
 * The reads a time window can serve EXACTLY: newest-first by `created_at`, a
 * limit (inside the match-phase band for a plain read; any, for a gated one),
 * and nothing already naming its documents.
 *
 * Why exactness holds: a window `[anchor - w, ∞)` is anchored at the query's
 * newest end, so every document outside it is strictly older than every
 * document inside it. If the window holds `limit` results, they ARE the
 * top-`limit` of the unbounded read. That argument is per-document, so it
 * survives the trust gate too — a gated profile drops documents one at a time,
 * and "limit above-floor documents inside the window" is the same proof.
 *
 * Excluded: explicit [EventYql.RANK_UNRANKED] / [EventYql.RANK_RECENCY_GATED_EXACT]
 * (internal reads and reruns stamp those precisely to opt out of planning), ids
 * and weighted key sets (the caller named its documents), and the
 * complete/sampled/keepMatchPhase reads, whose response contracts the window
 * would change.
 */
internal fun EventQuery.isWindowable(): Boolean {
    val limit = limit ?: return false
    if (limit < 1) return false
    val plain = ranking == null && search == null && phrases.isEmpty()
    // PLAIN reads past the band are already windowed a band-sized page at a
    // time (VespaEventIndex.pagedRecency pages it, and every page is a
    // windowable read of its own). A GATED read past the band has no pager:
    // it demotes to the full-scan gated profile over the WHOLE match set —
    // measured 4.8-5.0 s for a gated kind-1 feed at limits 2,001-5,000
    // (2026-09-27, 44M-doc corpus), the relay's max_limit being 5,000. The
    // window argument does not care about the limit, and inside a window
    // the full-scan profile walks only the window, so gated reads window at
    // any limit.
    if (plain && limit > EventYql.MATCH_PHASE_BAND) return false
    return (plain || ranking == EventYql.RANK_RECENCY_GATED) &&
        !complete &&
        !sampled &&
        !keepMatchPhase &&
        ids.isEmpty() &&
        idWeights.isEmpty() &&
        authorWeights.isEmpty() &&
        expiresBefore == null
}

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
    planning: Boolean,
    private val strategy: RecencyStrategy,
    private val fallbacks: SchemaFallbacks,
    private val exactCount: suspend (EventQuery) -> Int,
) {
    /**
     * Whether count probes run at all. [RecencyStrategy.FULL_SCAN] is defined
     * by NOT planning. [RecencyStrategy.SPECULATIVE] windows its windowable
     * reads by running the query itself (VespaEventIndex.speculative), and
     * keeps the shipped bare-scan probes for the unwindowed read it falls back
     * to ([shipped]).
     */
    val enabled: Boolean = planning && strategy != RecencyStrategy.FULL_SCAN

    /**
     * [q] as [strategy] plans it before it reaches the engine: the count-probe
     * window for [RecencyStrategy.COUNT_PROBE], [q] untouched for a read
     * [RecencyStrategy.SPECULATIVE] will window itself, and otherwise the
     * shipped planning ([shipped]).
     *
     * The shipped rule: window [q] if it is a bare recency scan the match-phase
     * `recency` profile does not already cover: the profile owns the small limits
     * (probing there costs more than it saves — measured 0.6x), the planner
     * windows the limits past the profile's headroom gate, and takes
     * everything back when the serving schema lacks the profile.
     */
    suspend fun plan(q: EventQuery): EventQuery {
        if (!enabled) return q
        if (q.isWindowable()) {
            when (strategy) {
                // C: every windowable read probes — the gated ones through a
                // GATED count (EventIndex.count runs a gated query on the
                // full-scan twin with one hit, so its total is net of the
                // floor) — over the wider ladder, and the match-phase profile
                // no longer exempts the small limits.
                RecencyStrategy.COUNT_PROBE -> return window(q, WIDE_WINDOWS)

                // D: the read itself windows, later (VespaEventIndex.speculative)
                // — but only while the `recency` profile serves. Without it the
                // profile net demotes a plain read to RANK_UNRANKED AFTER this
                // plan, an explicit ranking isWindowable reads as an opt-out, so
                // deferring would hand the read past the demotion unplanned: the
                // full unranked sort the degrader cuts. On such a schema the
                // shipped probes window it here, before the demotion, as they
                // always did.
                RecencyStrategy.SPECULATIVE -> if (fallbacks.recencyProfileAvailable) return q

                RecencyStrategy.FULL_SCAN, RecencyStrategy.MATCH_PHASE -> Unit
            }
        }
        return shipped(q)
    }

    /**
     * The shipped planning ([RecencyStrategy.MATCH_PHASE]'s): window a BARE scan
     * the match-phase profile does not cover. Also the unwindowed fallback of
     * [RecencyStrategy.SPECULATIVE], so giving up on speculating never leaves a
     * read worse planned than it was before the strategy existed.
     */
    suspend fun shipped(q: EventQuery): EventQuery {
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
    suspend fun window(
        q: EventQuery,
        windows: LongArray = PROBE_WINDOWS,
    ): EventQuery {
        // The request's clock when it carries one (EventYql.nowOf's rule): a
        // plan and the ranking it feeds must not disagree about "now".
        val anchor = q.until ?: q.nowSecs ?: (System.currentTimeMillis() / 1000)
        for (window in windows) {
            val since = anchor - window
            // An existing `since` at least this tight makes the rung (and any
            // wider one) pointless — the query is already windowed.
            if (q.since != null && since <= q.since) return q
            // A failed probe just means "don't window": planning is an
            // optimization. Cancellation is NOT a failed probe — swallowing it
            // would enqueue one more engine request on a job already dead.
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

    companion object {
        /**
         * Seconds before the query's anchor: an hour, a day, a month. Geometric
         * so a live corpus exits on the first rung that fits its event rate, and
         * a miss costs one more ~5ms count rather than a rescan.
         */
        private val PROBE_WINDOWS = longArrayOf(3_600L, 86_400L, 2_592_000L)

        /**
         * [RecencyStrategy.COUNT_PROBE]'s ladder. It adds a WEEK because a
         * selective read (a follow list, a hashtag) usually needs more than a
         * day and far less than a month — and the month is not free: measured
         * on a 44M-doc relay corpus, a 300-author feed windowed to 7 days
         * matched in 23ms and windowed to 30 days in 145ms, no faster than
         * unwindowed. And a YEAR, so a sparse read still gets one bounded rung
         * before the unwindowed query.
         */
        val WIDE_WINDOWS = longArrayOf(3_600L, 86_400L, 604_800L, 2_592_000L, 31_536_000L)

        /** [RecencyStrategy.SPECULATIVE]'s first window: an hour, like the ladder's. */
        const val FIRST_WINDOW = 3_600L

        /** Past this the speculative loop stops windowing and runs the unwindowed query. */
        const val MAX_WINDOW = 31_536_000L

        /**
         * THE NARROW-READ RULE: once an attempt's observed rate says a full
         * page needs a window wider than this, [RecencyStrategy.SPECULATIVE]
         * stops guessing and runs the unwindowed read.
         *
         * A read that thin over the recent past has a SMALL match set, and a
         * small match set is exactly the read the engine already answers
         * cheaply without a window — so every further attempt is pure
         * overhead. Measured on a 44M-doc relay corpus (2026-09-26): a 30-day
         * window was where a 300-author feed stopped gaining anything over no
         * window at all (145ms either way). With the rule, a read with no rate
         * in its first window pays one small attempt over the shipped cost —
         * the gated 50-quiet-author feed: 14ms shipped, 16ms speculative.
         */
        const val NARROW_HORIZON = 2_592_000L

        /**
         * The most windowed attempts before the unwindowed read, whatever the
         * rate says — the bound on what speculating can cost a read it does not
         * help: two small queries.
         *
         * The rate is not stationary, which is why a bound is needed at all. A
         * read whose authors were quiet in kind 1 but busy reacting (measured
         * 2026-09-26: 17 matches in the last hour, 71 in the day, 115 in the
         * week) projects "one more window is enough" after every attempt, and
         * at three attempts it spent four queries (29ms) on a read that costs
         * 12ms unwindowed. At two it spends three (23ms), and every busy shape
         * in the same battery still resolves inside two.
         */
        const val MAX_ATTEMPTS = 2

        /**
         * The window a full page needs, projected from [got] of [limit] results
         * in [window] seconds — at a PESSIMISTIC rate: the count less one
         * standard deviation (`got - sqrt(got)`, Poisson), so [Long.MAX_VALUE]
         * for zero or one.
         *
         * The discount keeps a count too small to be a rate from projecting a
         * confident window: a single event in the first hour would otherwise
         * promise a page at `limit` hours, and a read that thin is exactly the
         * one [NARROW_HORIZON] exists to send unwindowed. A busy read barely
         * notices — 100 in the window projects 11% wider. It is not enough on
         * its own: a read whose recent rate is REAL but decaying still projects
         * short, which is what [MAX_ATTEMPTS] bounds.
         */
        fun projected(
            window: Long,
            got: Int,
            limit: Int,
        ): Long {
            val rate = got - kotlin.math.sqrt(got.toDouble())
            return if (rate <= 0.0) Long.MAX_VALUE else (window * limit / rate).toLong()
        }

        /**
         * The next speculative window after one that returned [got] of [limit]:
         * the observed rate says roughly `window * limit / got` holds a page, so
         * ask for FOUR times that — and never less than 8x, so an empty or
         * near-empty window cannot creep up one small step at a time.
         *
         * Overshooting is cheap exactly where this runs: a short window means a
         * low rate, and a low rate means a wide window still matches few
         * documents. Undershooting costs a whole round trip. Measured: at 2x a
         * gated notifications feed took three queries (24ms), at 4x two (14.5ms).
         */
        fun widen(
            window: Long,
            got: Int,
            limit: Int,
        ): Long = window * maxOf(8L, 4L * limit / maxOf(got, 1))
    }
}
