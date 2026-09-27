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

/**
 * WHERE THE LAST READ OF THIS SHAPE FOUND ITS PAGE, so the next one starts
 * there instead of at [RecencyPlanner.FIRST_WINDOW].
 *
 * [RecencyStrategy.SPECULATIVE] pays one round trip per window it has to
 * guess. Relay traffic repeats its guesses: every client of a global feed
 * asks the identical filter, a follow feed is re-asked on every reconnect and
 * every page, a notification subscription on every open. What the first read
 * learned — "a page of this shape fits in W seconds", or "this shape is too
 * thin to window at all" — answers the next one's guess in advance:
 *
 *  - a remembered WINDOW is the first attempt (scaled to the new limit), so a
 *    shape that needed a second window last time proves out in one query;
 *  - a remembered NARROW skips the attempts and runs the read unwindowed
 *    immediately — the shipped cost, exactly, with nothing speculated.
 *
 * Correctness never depends on it: a window only changes what the engine
 * WALKS, never which page is served (a full window proves itself; a short
 * one widens as before), so a stale entry costs a round trip, not an answer.
 * Entries expire after [ttlMillis] so a feed that changes character — a
 * hashtag that starts trending — is re-probed rather than remembered wrong,
 * and the table is an LRU of [capacity] shapes, so its memory is bounded
 * whatever the traffic.
 *
 * The KEY is everything that sets a shape's event RATE, and nothing that
 * doesn't: kinds, authors, tags, owners, exclusions and the gate's lens and
 * floor — but not `since`/`until` (windows are anchor-relative) or `limit`
 * (a remembered window scales linearly with it). One coarse term does carry
 * time: how far back the anchor sits, in powers of two of a day, because a
 * page ten months back does not fill at the rate the newest page does.
 */
internal class RecencyWindowMemory(
    private val capacity: Int = 4_096,
    private val ttlMillis: Long = 600_000L,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    /** What one shape's last speculative read learned. */
    sealed interface Recall {
        /** A page fit in [window] seconds at [limit]; start the next attempt there, scaled. */
        data class Window(
            val window: Long,
            val limit: Int,
        ) : Recall

        /** The shape fell back as too thin to window; run it unwindowed. */
        data object Narrow : Recall
    }

    private class Entry(
        val recall: Recall,
        val atMillis: Long,
    )

    private data class Key(
        val kinds: List<Int>,
        val authors: List<String>,
        val owners: List<String>,
        val tags: Map<String, List<String>>,
        val tagsAll: Map<String, List<String>>,
        val notSearch: List<String>,
        val gated: Boolean,
        val rankKey: String?,
        val minRank: Double?,
        val ageBucket: Int,
    )

    // Access-ordered: the eldest entry is the least recently USED shape.
    private val entries =
        object : LinkedHashMap<Key, Entry>(256, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Key, Entry>?): Boolean = size > capacity
        }

    /** What [q] (anchored at [anchor], with the request clock [now]) should start from, or null to start cold. */
    fun recall(
        q: EventQuery,
        anchor: Long,
        now: Long,
    ): Recall? {
        val key = keyOf(q, anchor, now)
        return synchronized(entries) {
            val entry = entries[key] ?: return null
            if (clock() - entry.atMillis > ttlMillis) {
                entries.remove(key)
                null
            } else {
                entry.recall
            }
        }
    }

    fun remember(
        q: EventQuery,
        anchor: Long,
        now: Long,
        recall: Recall,
    ) {
        val key = keyOf(q, anchor, now)
        synchronized(entries) { entries[key] = Entry(recall, clock()) }
    }

    fun forget(
        q: EventQuery,
        anchor: Long,
        now: Long,
    ) {
        val key = keyOf(q, anchor, now)
        synchronized(entries) { entries.remove(key) }
    }

    /** Shapes currently remembered (expired ones included until touched). */
    val size: Int get() = synchronized(entries) { entries.size }

    private fun keyOf(
        q: EventQuery,
        anchor: Long,
        now: Long,
    ) = Key(
        kinds = q.kinds.sorted(),
        authors = q.authors.map { it.lowercase() }.sorted(),
        owners = q.owners.map { it.lowercase() }.sorted(),
        tags = q.tags.mapValues { (_, v) -> v.sorted() },
        tagsAll = q.tagsAll.mapValues { (_, v) -> v.sorted() },
        notSearch = q.notSearch.sorted(),
        gated = q.usesGatedProfile(),
        rankKey = if (q.usesGatedProfile()) q.rankKey else null,
        minRank = if (q.usesGatedProfile()) q.minRank else null,
        ageBucket = ageBucket(now - anchor),
    )

    companion object {
        /**
         * How far back an anchor sits, coarsely: 0 within a day of the clock,
         * then one bucket per doubling (2 days, 4, 8, … ). Deep pagination
         * walks through the buckets and re-learns at each, which is the point:
         * the rate a page meets depends on how old it is.
         */
        fun ageBucket(ageSecs: Long): Int {
            val days = ageSecs / 86_400L
            return if (days <= 0) 0 else 64 - java.lang.Long.numberOfLeadingZeros(days)
        }
    }
}
