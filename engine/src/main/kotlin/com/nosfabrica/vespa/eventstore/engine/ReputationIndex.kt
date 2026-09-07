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
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
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
            val cur = get(u.subject) ?: if (u.influence == null && u.followers == null) return@forEach else ReputationDoc(u.subject)
            put(
                cur.copy(
                    influenceScores = u.influence?.let { cur.influenceScores + (u.key to it) } ?: if (u.dropInfluence) cur.influenceScores - u.key else cur.influenceScores,
                    followerCounts = u.followers?.let { cur.followerCounts + (u.key to it) } ?: if (u.dropFollowers) cur.followerCounts - u.key else cur.followerCounts,
                ),
            )
        }

    /**
     * Drop single tensor cells from the subjects' parents — a retracted or
     * removed card's cells, keyed by the service that signed it. A missing
     * document or cell is a no-op, never an error. The default is
     * read-modify-write (the in-memory spec); the real client sends Vespa's
     * tensor `remove` in one update.
     */
    suspend fun removeCells(removals: List<CellRemoval>) =
        removals.forEach { r ->
            val cur = get(r.subject) ?: return@forEach
            put(
                cur.copy(
                    influenceScores = if (r.influence) cur.influenceScores - r.key else cur.influenceScores,
                    followerCounts = if (r.followers) cur.followerCounts - r.key else cur.followerCounts,
                ),
            )
        }

    suspend fun remove(pubkey: String)

    /** Bulk [remove]; implementations may pipeline (see [EventIndex.removeAll]). */
    suspend fun removeAll(pubkeys: List<String>) = pubkeys.forEach { remove(it) }

    /**
     * Stream every stored reputation pubkey, paged — the orphan sweep's walk:
     * a parent whose subject has no cards left is only findable from the
     * REPUTATION corpus. [onPage] returns whether to continue; order is
     * engine-defined.
     */
    suspend fun visitPubkeys(onPage: suspend (List<String>) -> Boolean)

    /**
     * Every document, cells included, a page at a time. Default: the pubkey
     * walk plus a get per document, which is what an in-memory index can
     * afford and a real one overrides.
     */
    suspend fun visitDocs(onPage: suspend (List<ReputationDoc>) -> Boolean) = visitPubkeys { pubkeys -> onPage(pubkeys.mapNotNull { get(it) }) }
}
