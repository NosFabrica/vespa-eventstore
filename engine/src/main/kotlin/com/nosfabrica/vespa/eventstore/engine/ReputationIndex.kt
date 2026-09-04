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
package com.nosfabrica.vespa.eventstore.engine
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc

/**
 * The engine port for the reputation parent documents — the trust twin of
 * [EventIndex]. Same consistency contract: an acked [put] is visible to ranking.
 */
interface ReputationIndex : AutoCloseable {
    suspend fun get(pubkey: String): ReputationDoc?

    suspend fun put(reputation: ReputationDoc)

    /** Bulk [put]; implementations may pipeline (see [EventIndex.putAll]). */
    suspend fun putAll(reputations: List<ReputationDoc>) = reputations.forEach { put(it) }

    /**
     * Upsert single tensor cells on the subjects' parents, creating missing
     * parents — the insert path's ZERO-READ alternative to a full [put] (Vespa
     * tensor `add`). Callers must only send current-best values for their
     * (subject, observer); same-subject updates apply in list order. The
     * default is read-modify-write (the in-memory spec).
     */
    suspend fun updateCells(updates: List<ReputationCells>) =
        updates.forEach { u ->
            val cur = get(u.subject) ?: ReputationDoc(u.subject)
            put(
                cur.copy(
                    influenceScores = u.influence?.let { cur.influenceScores + (u.observer to it) } ?: cur.influenceScores,
                    followerCounts = u.followers?.let { cur.followerCounts + (u.observer to it) } ?: cur.followerCounts,
                ),
            )
        }

    suspend fun remove(pubkey: String)

    /** Bulk [remove]; implementations may pipeline (see [EventIndex.removeAll]). */
    suspend fun removeAll(pubkeys: List<String>) = pubkeys.forEach { remove(it) }

    /**
     * Raise each subject's stored `max_rank` to AT LEAST the given value —
     * never lower it — and only on documents that exist. The backfill's
     * write: it computes a bound from the cells it READ a page ago, and a
     * live cell raise may have moved the document past that bound since; an
     * unconditional assign would then put `max_rank` BELOW a cell it holds,
     * the one state the trust descent cannot serve correctly. Implementations
     * make the assign conditional on the stored value; the default reads.
     */
    suspend fun raiseMaxRank(floors: Map<String, Int>) =
        floors.forEach { (subject, floor) ->
            val stored = storedMaxRank(subject) ?: return@forEach
            if (stored < floor) updateCells(listOf(ReputationCells(subject, "", null, null, maxRank = floor)))
        }

    /**
     * The `max_rank` the document STORES — the scalar the trust descent cuts
     * on — as distinct from [ReputationDoc.maxRank], the maximum of its cells.
     * The two are meant to agree (that is the invariant MaxRankCache keeps),
     * and this exists for the two readers that must not assume they do: the
     * cache's first read of a subject, and the backfill's check of its own
     * marker. They came apart on staging (2026-09-04): a revert deployed a
     * schema without the field, which dropped every value, and the redeploy
     * brought the field back at 0 with the marker still standing — every rung
     * then matched nobody, and every ranked search answered empty. Null when
     * there is no document; 0 when there is one without the field.
     */
    suspend fun storedMaxRank(pubkey: String): Int? = get(pubkey)?.maxRank

    /**
     * Stream every stored reputation pubkey, paged — the orphan sweep's walk:
     * a parent whose subject has no cards left is only findable from the
     * REPUTATION corpus. [onPage] returns whether to continue; order is
     * engine-defined.
     */
    suspend fun visitPubkeys(onPage: suspend (List<String>) -> Boolean)

    /**
     * Every document, cells included, a page at a time; the walk the
     * `max_rank` backfill takes once over a store fed before the field
     * existed. Default: the pubkey walk plus a get per document, which is what
     * an in-memory index can afford and a real one overrides.
     */
    suspend fun visitDocs(onPage: suspend (List<ReputationDoc>) -> Boolean) = visitPubkeys { pubkeys -> onPage(pubkeys.mapNotNull { get(it) }) }
}
