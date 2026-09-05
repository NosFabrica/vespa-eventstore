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
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay

/**
 * ONE WALK that brings a store fed under the OBSERVER-keyed model onto the
 * service-keyed one, and a marker so it runs once.
 *
 * Reputation cells used to be keyed by the observer whose 10040 named the
 * signing service; they are keyed by the service itself now, and a query
 * resolves the observer's list to that key. A store written before the
 * change therefore holds documents no lens can read: every cell sits under
 * an observer's key, and no card's service has a cell. Two steps repair it,
 * both idempotent:
 *
 *  1. [TrustReconciler.reconcile] — samples every named service and finds
 *     none projected (no subject carries the service's cell), so it walks
 *     each one's stored cards into cells ([TrustRecompute.projectServices]).
 *     This is the same repair a fresh mirror gets, and a relay that already
 *     reconciles at boot has done it before this runs; the reconcile is then
 *     a sample per service and nothing more.
 *  2. [sweepUnmappedCells] — every stored parent's cells whose key is not a
 *     service some 10040 names are removed (tensor `remove`, pipelined): the
 *     old observer keys, and services no list names any more. Without this
 *     the old cells would stay resident in a global, memory-held document
 *     type and keep `max_rank` high.
 *
 * The marker [MARKER_KEY] is written only by a run that finished, so a
 * crashed migration restarts from the top; and a run that can read NO
 * 10040 while reputation documents exist stops without writing it — that
 * state is indistinguishable from an engine still serving its corpus, and
 * sweeping under it would empty every parent. A store with no reputation
 * documents at all has nothing to migrate and writes the marker at once.
 */
class TrustKeyingMigration internal constructor(
    private val reputations: ReputationIndex,
    private val reconciler: TrustReconciler,
    private val recompute: TrustRecompute,
) {
    /** What one run did: services walked by the reconcile, cell KEYS removed by the sweep (one per unmapped key per parent, both tensors), or refused (no 10040 readable). */
    data class Migration(
        val servicesProjected: Int,
        val keysRemoved: Int,
        val refused: Boolean,
    )

    /** Run unless the marker stands. */
    suspend fun run(onProgress: ((parents: Int, keysRemoved: Int) -> Unit)? = null): Migration {
        if (reputations.get(MARKER_KEY) != null) return Migration(0, 0, refused = false)
        var any = false
        reputations.visitPubkeys { page ->
            any = page.any(Hex::isHex64)
            !any // one page decides; stop as soon as a real parent is seen
        }
        if (!any) {
            reputations.put(marker())
            return Migration(0, 0, refused = false)
        }
        val providers = recompute.providerMap()
        if (providers.isEmpty()) return Migration(0, 0, refused = true)
        val reconciled = reconciler.reconcile()
        val removed = sweepUnmappedCells(providers.services, onProgress)
        reputations.put(marker())
        return Migration(reconciled.rebuilt.size, removed, refused = false)
    }

    /**
     * Drop every cell whose key is not in [services], across every stored
     * parent — pipelined removes, a page at a time, no gate: a cell keyed by
     * an unnamed key is read by no lens and written by no live path, so
     * nothing can race this. The projection's own bookkeeping documents
     * (non-hex ids) are never touched.
     */
    suspend fun sweepUnmappedCells(
        services: Set<String>,
        onProgress: ((parents: Int, keysRemoved: Int) -> Unit)? = null,
    ): Int {
        var parents = 0
        var removed = 0
        reputations.visitDocs { page ->
            val removals = ArrayList<CellRemoval>()
            for (doc in page) {
                if (!Hex.isHex64(doc.pubkey)) continue
                parents++
                val keys = (doc.influenceScores.keys + doc.followerCounts.keys).filterNot { it in services }
                for (key in keys) {
                    removals += CellRemoval(doc.pubkey, key, influence = key in doc.influenceScores, followers = key in doc.followerCounts)
                }
            }
            if (removals.isNotEmpty()) {
                reputations.removeCells(removals)
                removed += removals.size
            }
            onProgress?.invoke(parents, removed)
            true
        }
        return removed
    }

    /**
     * [run] until it returns, recording every failure under
     * [BackgroundFailures.TRUST_KEYING] and retrying after [retryMillis] — a
     * refused run (no 10040 readable yet) retries the same way, since the
     * engine still serving its corpus is the usual reason.
     */
    suspend fun runUntilDone(
        retryMillis: Long,
        onProgress: ((parents: Int, keysRemoved: Int) -> Unit)? = null,
    ): Migration {
        while (true) {
            try {
                val done = run(onProgress)
                if (!done.refused) {
                    BackgroundFailures.succeeded(BackgroundFailures.TRUST_KEYING)
                    return done
                }
                BackgroundFailures.record(BackgroundFailures.TRUST_KEYING, IllegalStateException("no kind 10040 readable while reputation documents exist; retrying"))
            } catch (e: CancellationException) {
                throw e
            } catch (t: Throwable) {
                BackgroundFailures.record(BackgroundFailures.TRUST_KEYING, t)
            }
            delay(retryMillis)
        }
    }

    companion object {
        /** A key no author can have (not hex), like the other markers'. */
        const val MARKER_KEY = "reputation-keyed-by-service"

        private const val DONE = "done"

        private fun marker(): ReputationDoc = ReputationDoc(MARKER_KEY, mapOf(DONE to 1))
    }
}
