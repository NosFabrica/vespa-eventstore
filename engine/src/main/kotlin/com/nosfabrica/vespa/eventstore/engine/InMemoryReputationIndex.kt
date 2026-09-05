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
import java.util.concurrent.ConcurrentHashMap

/**
 * The in-memory reference [ReputationIndex] — what projection tests assert
 * against.
 *
 * IN MAIN, NOT testFixtures, for the same reason [InMemoryEventIndex] is: a
 * CONSUMER needs it. The trust-projected stack — `NostrSemanticsStore` over
 * `TrustProjection` — is the only shape where the search expansion's gate can
 * resolve a delegation, so a relay testing its own protocol against that gate
 * has to be able to build one without a Vespa and without a test-fixtures
 * dependency JitPack does not publish.
 */
class InMemoryReputationIndex : ReputationIndex {
    val docs = ConcurrentHashMap<String, ReputationDoc>()

    /**
     * The stored `max_rank` per document, kept BESIDE the cells the way the
     * engine keeps it: a whole-document put sets it from the cells, a cell
     * update moves it only when it carries a value, and nothing else touches
     * it — so a test can put the two apart (write a 0 here) the way a schema
     * flip did on staging, and prove the readers that must not assume they
     * agree. See [ReputationIndex.storedMaxRank].
     */
    val storedMaxRanks = ConcurrentHashMap<String, Int>()

    override suspend fun get(pubkey: String): ReputationDoc? = docs[pubkey]

    override suspend fun storedMaxRank(pubkey: String): Int? = if (docs.containsKey(pubkey)) storedMaxRanks[pubkey] ?: 0 else null

    override suspend fun put(reputation: ReputationDoc) {
        docs[reputation.pubkey] = reputation
        storedMaxRanks[reputation.pubkey] = reputation.maxRank
    }

    override suspend fun updateCells(updates: List<ReputationCells>) {
        updates.forEach { u ->
            // A retraction alone never creates a document (the engine's update carries no create).
            val cur = docs[u.subject] ?: if (u.influence == null && u.followers == null) return@forEach else ReputationDoc(u.subject)
            docs[u.subject] =
                cur.copy(
                    influenceScores = u.influence?.let { cur.influenceScores + (u.key to it) } ?: if (u.dropInfluence) cur.influenceScores - u.key else cur.influenceScores,
                    followerCounts = u.followers?.let { cur.followerCounts + (u.key to it) } ?: if (u.dropFollowers) cur.followerCounts - u.key else cur.followerCounts,
                )
            u.maxRank?.let { storedMaxRanks[u.subject] = it }
            storedMaxRanks.putIfAbsent(u.subject, 0)
        }
    }

    override suspend fun removeCells(removals: List<CellRemoval>) {
        removals.forEach { r ->
            val cur = docs[r.subject] ?: return@forEach
            docs[r.subject] =
                cur.copy(
                    influenceScores = if (r.influence) cur.influenceScores - r.key else cur.influenceScores,
                    followerCounts = if (r.followers) cur.followerCounts - r.key else cur.followerCounts,
                )
        }
    }

    override suspend fun remove(pubkey: String) {
        docs.remove(pubkey)
        storedMaxRanks.remove(pubkey)
    }

    override suspend fun raiseMaxRank(floors: Map<String, Int>) {
        floors.forEach { (subject, floor) ->
            if (docs.containsKey(subject) && (storedMaxRanks[subject] ?: 0) < floor) storedMaxRanks[subject] = floor
        }
    }

    override suspend fun visitPubkeys(onPage: suspend (List<String>) -> Boolean) {
        // One snapshot page — plenty for the reference; the real client streams.
        onPage(docs.keys.toList())
    }

    override fun close() {}
}
