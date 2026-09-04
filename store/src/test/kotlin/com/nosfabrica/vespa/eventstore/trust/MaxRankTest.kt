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

import com.nosfabrica.vespa.eventstore.BackgroundFailures
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import kotlinx.coroutines.runBlocking
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * THE INVARIANT THE DESCENT RESTS ON: a reputation document's `max_rank` is
 * never below a cell it holds. The cell path cannot read the document it
 * adds to, so the cache stands in for the stored value — read once, then
 * moved with every write that moves it.
 */
class MaxRankTest {
    private val reputations = InMemoryReputationIndex()
    private val cache = MaxRankCache(reputations)
    private val s = "a1".repeat(32)
    private val o1 = "b1".repeat(32)
    private val o2 = "b2".repeat(32)

    @Test
    fun `a cell that overtakes the stored max carries the new max, one that does not carries nothing`() =
        runBlocking {
            reputations.put(ReputationDoc(s, mapOf(o1 to 40)))
            val lower = cache.raise(listOf(ReputationCells(s, o2, 30, null)))
            assertNull(lower.single().maxRank, "30 does not overtake a stored 40 — read once, no assign")
            val higher = cache.raise(listOf(ReputationCells(s, o2, 70, null)))
            assertEquals(70, higher.single().maxRank, "70 does: the cell carries the new max")
            val same = cache.raise(listOf(ReputationCells(s, o1, 70, null)))
            assertNull(same.single().maxRank, "…and the cache moved with it, so 70 again is not a raise")
            // Two cells for one subject in one batch: every cell of the subject carries the batch's max.
            val batch = cache.raise(listOf(ReputationCells(s, o1, 80, null), ReputationCells(s, o2, 75, null)))
            assertEquals(listOf(80, 80), batch.map { it.maxRank })
            // A followers-only cell moves nothing.
            assertNull(cache.raise(listOf(ReputationCells(s, o1, null, 12.0))).single().maxRank)
        }

    @Test
    fun `a whole-document write can LOWER the max, and the cache follows it rather than reading high`() =
        runBlocking {
            reputations.put(ReputationDoc(s, mapOf(o1 to 90)))
            cache.raise(listOf(ReputationCells(s, o2, 10, null))) // reads 90
            val rewritten = ReputationDoc(s, mapOf(o1 to 20))
            reputations.put(rewritten)
            cache.remember(listOf(rewritten))
            assertEquals(50, cache.raise(listOf(ReputationCells(s, o2, 50, null))).single().maxRank, "50 overtakes the rewritten 20 — a stale 90 would have skipped this assign")
            cache.forget(listOf(s))
            reputations.remove(s)
            assertEquals(5, cache.raise(listOf(ReputationCells(s, o2, 5, null))).single().maxRank, "a removed document starts from 0 again")
        }

    @Test
    fun `the backfill writes every document once and leaves its marker`() =
        runBlocking {
            reputations.put(ReputationDoc(s, mapOf(o1 to 40, o2 to 65)))
            reputations.put(ReputationDoc("c1".repeat(32), mapOf(o1 to 3)))
            reputations.put(ReputationDoc(DirtLedger.MARKER_KEY, mapOf(s to 1)))
            val backfill = MaxRankBackfill(reputations)
            assertEquals(2, backfill.run(), "two authors written; the ledger's marker is not an author")
            assertEquals(65, reputations.get(s)!!.maxRank)
            assertEquals(1, reputations.get(MaxRankBackfill.MARKER_KEY)!!.influenceScores.size, "the marker stands")
            assertEquals(0, backfill.run(), "…and the walk is a no-op while it does")
        }

    /**
     * THE SCHEMA FLIP: a revert deploys a schema without `max_rank`, dropping
     * every value; the redeploy brings the field back at 0 under a marker that
     * still stands. Staging, 2026-09-04 — every rung matched nobody and every
     * ranked search answered empty. The marker is checked against the data.
     */
    @Test
    fun `a marker the data contradicts is not trusted, and the walk runs again`() =
        runBlocking {
            reputations.put(ReputationDoc(s, mapOf(o1 to 40, o2 to 65)))
            reputations.put(ReputationDoc("c1".repeat(32), mapOf(o1 to 3)))
            val backfill = MaxRankBackfill(reputations)
            assertEquals(2, backfill.run())
            assertEquals(0, backfill.run(), "the marker stands and the data agrees with it")
            // The flip: the field is gone and back at 0, the cells and the marker untouched.
            reputations.storedMaxRanks.keys.forEach { reputations.storedMaxRanks[it] = 0 }
            assertEquals(0, reputations.storedMaxRank(s), "what a rung would cut on")
            assertEquals(2, backfill.run(), "the marker is contradicted by the first page, so the walk runs again")
            assertEquals(65, reputations.storedMaxRank(s), "…and the scalar is back")
            assertEquals(0, backfill.run(), "…under a marker the data now agrees with")
        }

    /** The cache's first read must be the STORED scalar: after the flip the cells still say 40 while the rung cuts on 0. */
    @Test
    fun `the cache reads what is stored, not what the cells imply`() =
        runBlocking {
            reputations.put(ReputationDoc(s, mapOf(o1 to 40)))
            reputations.storedMaxRanks[s] = 0 // the flip
            val raised = cache.raise(listOf(ReputationCells(s, o2, 30, null)))
            assertEquals(30, raised.single().maxRank, "30 overtakes a STORED 0 — a cache reading the cells' 40 would have skipped the assign")
        }

    @Test
    fun `a walk the engine refuses is counted and started again, and the marker waits for the one that finished`() =
        runBlocking {
            BackgroundFailures.reset()
            reputations.put(ReputationDoc(s, mapOf(o1 to 40)))
            reputations.put(ReputationDoc("c1".repeat(32), mapOf(o1 to 3)))
            var refusals = 2
            var walks = 0
            // The staging shape: the visit itself is what a restarting engine refuses.
            val flaky =
                object : ReputationIndex by reputations {
                    override suspend fun visitDocs(onPage: suspend (List<ReputationDoc>) -> Boolean) {
                        walks++
                        if (refusals-- > 0) throw IOException("Connection refused")
                        reputations.visitDocs(onPage)
                    }
                }
            assertEquals(2, MaxRankBackfill(flaky).runUntilDone(retryMillis = 1), "the third walk wrote both authors")
            assertEquals(3, walks, "two refused, one finished")
            assertEquals(40, reputations.get(s)!!.maxRank)
            assertEquals(1, reputations.get(MaxRankBackfill.MARKER_KEY)!!.influenceScores.size, "the marker is the finished walk's")
            assertEquals(0, BackgroundFailures.consecutiveFailures(BackgroundFailures.MAX_RANK_BACKFILL), "recovered")
            assertTrue(BackgroundFailures.statusLine().contains("${BackgroundFailures.MAX_RANK_BACKFILL} 2 fail"), "…and the two refusals stay on the record: ${BackgroundFailures.statusLine()}")
        }
}
