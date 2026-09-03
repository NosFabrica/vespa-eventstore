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
