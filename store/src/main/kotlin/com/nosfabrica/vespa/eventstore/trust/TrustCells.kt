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
package com.nosfabrica.vespa.eventstore.trust

import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey

/**
 * AN AUTHOR'S RANK CELL UNDER ONE SERVICE, READ FOR THE LIVE GATE.
 *
 * The stored page gates in the engine, reading `author_influence_scores` for
 * the lens's service key. A live event is gated here instead, off the same
 * cell: [read] fetches the author's reputation parent, [cached] answers from
 * what the last reads left. A missing cell, parent or lens is 0 — the engine's
 * `sum(user_q * scores)` over nothing — so the two gates cannot disagree about
 * an author the lens does not rank.
 *
 * Bounded and SHORT-LIVED on purpose: a card write moves a cell and nothing
 * here listens for it, so an entry is trusted for [ttlSecs] and then re-read.
 * The cost of staleness is a live event judged on a rank up to that old; the
 * stored page, re-read on every REQ, is never stale.
 */
internal class TrustCells(
    private val reputations: ReputationIndex?,
    private val nowSecs: () -> Long,
    private val ttlSecs: Long = DEFAULT_TTL_SECS,
    private val maxEntries: Int = DEFAULT_MAX_ENTRIES,
) {
    private class Entry(
        val score: Double,
        val readAt: Long,
    )

    private val entries =
        object : LinkedHashMap<String, Entry>(1024, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Entry>?): Boolean = size > maxEntries
        }

    private fun key(
        author: String,
        service: String,
    ) = "$service:$author"

    /** The cell from a read no older than [ttlSecs], or null when it must be read. */
    fun cached(
        author: String,
        service: String?,
    ): Double? {
        if (service == null || reputations == null) return 0.0
        val entry = synchronized(entries) { entries[key(author, service)] } ?: return null
        return entry.score.takeIf { nowSecs() - entry.readAt < ttlSecs }
    }

    /** The cell, read now when [cached] has none. */
    suspend fun read(
        author: String,
        service: String?,
    ): Double {
        if (service == null || reputations == null) return 0.0
        cached(author, service)?.let { return it }
        val score =
            reputations
                .get(author)
                ?.influenceScores
                ?.get(ServiceKey(service))
                ?.toDouble() ?: 0.0
        synchronized(entries) { entries[key(author, service)] = Entry(score, nowSecs()) }
        return score
    }

    companion object {
        const val DEFAULT_TTL_SECS = 60L
        const val DEFAULT_MAX_ENTRIES = 100_000
    }
}
