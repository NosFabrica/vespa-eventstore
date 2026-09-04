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

import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.mapBounded

/**
 * WHAT `max_rank` IS STORED AS, per subject, as far as this process knows —
 * so the incremental cell path can raise it in the same update as a cell
 * that overtakes it without reading the document first, every time.
 *
 * The bound the trust descent proves pages with (TrustDescent) rests on one
 * invariant: a reputation document's `max_rank` is never below any cell it
 * holds. A whole-document write keeps it by construction
 * ([ReputationDoc.maxRank] is the max of the cells written). The cell path
 * adds ONE cell to a document it does not read, so it needs the stored value
 * from somewhere: the first time a subject is seen here it is read once, and
 * from then on this map is the stored value, moved in lockstep with every
 * write that moves it — raised by [raise] on the cell path, replaced by
 * [remember] when a recompute rewrites the whole document (which can LOWER
 * it, and a cache reading high would then skip a raise the store needed).
 * A stale-high entry is the one unsound state; a stale-low one is merely a
 * redundant assign, which is why a miss is read rather than assumed.
 *
 * Bounded: past [CAPACITY] entries it forgets everything, and the next cell
 * for each subject reads again. A production store has ~340k scored
 * subjects, so the bound is headroom, not a working limit.
 */
internal class MaxRankCache(
    private val reputations: ReputationIndex,
) {
    private val known = HashMap<String, Int>()

    /** The stored `max_rank` for each of [subjects], read where unknown. */
    suspend fun stored(subjects: Collection<String>): Map<String, Int> {
        val missing = subjects.filter { it !in known }.distinct()
        if (missing.isNotEmpty()) {
            val read = missing.mapBounded(QUERY_FANOUT) { s -> s to (reputations.get(s)?.maxRank ?: 0) }
            synchronized(known) {
                if (known.size + read.size > CAPACITY) known.clear()
                read.forEach { (s, m) -> known[s] = m }
            }
        }
        return synchronized(known) { subjects.associateWith { known[it] ?: 0 } }
    }

    /**
     * [updates] with [ReputationCells.maxRank] set on every cell of a subject
     * whose influence overtakes the stored value — and the cache moved to the
     * new value, since the write that carries it is the caller's next step.
     */
    suspend fun raise(updates: List<ReputationCells>): List<ReputationCells> {
        val current = stored(updates.map { it.subject })
        val raised = HashMap<String, Int>()
        for (u in updates) {
            val q = u.influence ?: continue
            if (q > (raised[u.subject] ?: current.getValue(u.subject))) raised[u.subject] = q
        }
        if (raised.isEmpty()) return updates
        synchronized(known) { raised.forEach { (s, m) -> known[s] = m } }
        return updates.map { u -> raised[u.subject]?.let { u.copy(maxRank = it) } ?: u }
    }

    /** A whole document was just written: its `max_rank` is now exactly [ReputationDoc.maxRank]. */
    fun remember(docs: Collection<ReputationDoc>) {
        synchronized(known) {
            if (known.size + docs.size > CAPACITY) known.clear()
            docs.forEach { known[it.pubkey] = it.maxRank }
        }
    }

    /** A document was removed: nothing is stored, so the next cell starts from 0. */
    fun forget(subjects: Collection<String>) {
        synchronized(known) { subjects.forEach { known.remove(it) } }
    }

    private companion object {
        const val CAPACITY = 2_000_000
    }
}

/**
 * ONE WALK OVER EVERY REPUTATION DOCUMENT, writing the `max_rank` its cells
 * imply — for a store fed before the field existed, whose documents read 0
 * there and would be excluded from every rung of the descent above the
 * floor. Runs once: it leaves a marker document behind and is a no-op while
 * the marker stands, and the store enables the descent only after it
 * returns (VespaEventStore.open). Every write since the field shipped keeps
 * the value itself ([MaxRankCache]), so the walk is the migration and not
 * the upkeep.
 */
internal class MaxRankBackfill(
    private val reputations: ReputationIndex,
) {
    /** Walk and write unless already done; returns how many documents were written (0 when the marker stood). */
    suspend fun run(onProgress: ((Int) -> Unit)? = null): Int {
        if (reputations.get(MARKER_KEY) != null) return 0
        var written = 0
        reputations.visitDocs { page ->
            val cells = page.filter { it.pubkey != MARKER_KEY && it.pubkey != DirtLedger.MARKER_KEY }.map { ReputationCells(it.pubkey, SELF, null, null, maxRank = it.maxRank) }
            if (cells.isNotEmpty()) reputations.updateCells(cells)
            written += cells.size
            onProgress?.invoke(written)
            true
        }
        reputations.put(ReputationDoc(MARKER_KEY, mapOf(DONE to 1)))
        return written
    }

    companion object {
        /** A key no author can have (not hex), the same way DirtLedger's marker is. */
        const val MARKER_KEY = "max-rank-backfilled"

        private const val DONE = "done"

        /** The observer key of an update that changes no cell — [ReputationCells] needs one; `max_rank` is all this write carries. */
        private const val SELF = "backfill"
    }
}
