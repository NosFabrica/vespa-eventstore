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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.engine.text.EntityNames
import kotlinx.coroutines.CancellationException

/**
 * THE TOPICAL INTENT OF A SEARCH: whether it asks ABOUT a subject or FOR one
 * entity, decided before the read and sent as [EventQuery.topical] (event.sd
 * `topical_perfect_drop()`). docs/embedding-experiment.md §8.5 holds the
 * measurement and §10 this implementation.
 *
 * THE PROBE: one extra engine search for the query as ONE QUOTED PHRASE over
 * profiles and app handlers ([EntityNames.KINDS]), under the reader's own lens,
 * from authors at or over [PROBE_TRUST]. If one of the first [PROBE_HITS] is
 * NAMED the query ([EntityNames.names]) and is NOTABLE (an app handler, or a
 * profile with at least [PROBE_FOLLOWERS] followers under the lens), the query
 * is navigational (0.0, the ranking as it was). Otherwise it is topical (1.0),
 * and the perfect-match rung stops paying entities named the topic.
 *
 * Both rules were measured on production (§10.2). Notability, because trust
 * saturates: "Coffee", "art", "privacy", "freedom" and "nostr" are each the
 * exact name of a trust-75+ account with a few hundred followers, so a trust
 * floor alone read 18 of 89 topical queries as navigational. A follower floor
 * reads 3, while ODELL, Gigi, Primal and the apps keep thousands. The phrase,
 * because "named exactly" needs no fuzzy, prefix or trigram reach. The same
 * probe sent as words took 342 ms at p50 (p95 1.3 s, up to 4 s for `dell`),
 * against 77 ms as a phrase (p95 0.65 s), with the same decision on 198 of 199
 * queries.
 *
 * WHEN IT ASKS: only a read that ranks on the `search` profile under a resolved
 * lens, carries words, and could surface an entity named them. A profiles-only
 * read is navigational by construction, and a read limited to notes and
 * reference documents ([QUIET_KINDS]) has nothing the rung it gates moves
 * enough to pay a round trip for. Answers are cached per lens and text for
 * [TTL_MILLIS], so paging, live re-reads and a REQ's sibling filters pay once.
 *
 * FAILS OPEN: a probe that throws leaves the query as it was.
 */
internal class QueryIntent(
    private val index: EventIndex,
    private val nowMillis: () -> Long = System::currentTimeMillis,
) {
    private val cache =
        object : LinkedHashMap<String, Pair<Double, Long>>(256, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Pair<Double, Long>>) = size > CACHE_ENTRIES
        }

    /** [queries] with [EventQuery.topical] set where the probe applies; the rest unchanged. */
    suspend fun apply(queries: List<EventQuery>): List<EventQuery> = queries.map { q -> topicalOf(q)?.let { if (it > 0.0) q.copy(topical = it) else q } ?: q }

    /** 1.0 topical, 0.0 navigational, null when the probe does not apply or failed. */
    internal suspend fun topicalOf(q: EventQuery): Double? {
        val lens = q.rankKey ?: return null
        if (q.observer == null || EventYql.profileOf(q) != EventYql.RANK_SEARCH) return null
        if (q.kinds.isNotEmpty() && q.kinds.all { it in QUIET_KINDS }) return null
        val text = textOf(q) ?: return null
        val key = "$lens\u0000${text.lowercase()}"
        val now = nowMillis()
        synchronized(cache) { cache[key]?.takeIf { now - it.second < TTL_MILLIS }?.let { return it.first } }
        val topical =
            try {
                val probe =
                    EventQuery(
                        phrases = listOf(text),
                        kinds = EntityNames.KINDS,
                        observer = q.observer,
                        rankKey = q.rankKey,
                        followersKey = q.followersKey,
                        ranking = EventYql.RANK_SEARCH,
                        minRank = maxOf(q.minRank ?: 0.0, PROBE_TRUST),
                        limit = PROBE_HITS,
                        notExpiredAt = q.notExpiredAt,
                        nowSecs = q.nowSecs,
                    )
                if (index.searchRanked(probe).any { notable(it) && EntityNames.names(text, it.hit.kind, it.hit.content) }) 0.0 else 1.0
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                return null
            }
        synchronized(cache) { cache[key] = topical to now }
        return topical
    }

    /**
     * Whether a hit is an entity a reader navigates to: an app handler always; a
     * profile with [PROBE_FOLLOWERS] or more followers, or an unknown count (an
     * index that reports none), which errs toward the ranking as it was.
     */
    private fun notable(hit: Ranked<EventDoc>): Boolean = hit.hit.kind != 0 || (hit.followers ?: Double.MAX_VALUE) >= PROBE_FOLLOWERS

    companion object {
        /** The follower count a profile needs for its name to make a query navigational. */
        const val PROBE_FOLLOWERS = 1000.0

        /** The trust an entity's author needs for its name to make a query navigational: well over the default gate (2). */
        const val PROBE_TRUST = 30.0

        /** How many entities the probe reads. Measured: at 20, "nostr"'s exact-name accounts sat past the page; at 50 the phrase still costs ~0.1 s. */
        const val PROBE_HITS = 50

        /** How long a probe's answer is reused: entities and trust move on the scale of hours. */
        const val TTL_MILLIS = 10 * 60 * 1000L

        /** Distinct (lens, text) answers kept. */
        const val CACHE_ENTRIES = 10_000

        /**
         * Kinds whose pages the topical intent barely moves: profiles (a profile
         * tab IS navigational), notes (a note is rarely NAMED anything), and the
         * reference kinds whose rung the intent keeps (event.sd reference_kind()).
         */
        val QUIET_KINDS: Set<Int> = setOf(0, 1, 30023, 30818)

        /** The words and phrases as the engine matches them, or null for a search that has none. */
        fun textOf(q: EventQuery): String? = (listOfNotNull(q.search) + q.phrases).joinToString(" ").trim().takeIf { it.isNotEmpty() }
    }
}
