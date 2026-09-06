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
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.runtime.BackgroundFailures
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
 * PHASES ARE RECORDED, IN A SECOND DOCUMENT. Nothing used to be written
 * until a run finished, so every restart began again at step 1 — and step 1
 * is a corpus-scale reconcile. On a store where that takes longer than the
 * process stays up the migration cannot finish at all: it is not slow, it is
 * Sisyphean, and the marker's shape was the reason. [PROGRESS_KEY] now
 * carries [RECONCILED] as soon as the reconcile lands, so a restart resumes
 * at the sweep.
 *
 * [MARKER_KEY] still means one thing and only one thing: FINISHED. The
 * phases deliberately do not live in it. Every operator check of this
 * migration — every runbook, every status script — asks whether that
 * document exists, and a half-written marker answering 200 would tell all of
 * them the migration was done when it was not. The completion marker is
 * unchanged from before this commit, and the progress document is deleted
 * when it is written.
 *
 * The sweep itself restarts from the beginning of the parent walk, which is
 * safe because it is idempotent and reads far more than it writes: a parent
 * whose cells are already correct costs a read and no write. Resuming it
 * mid-walk would mean persisting Vespa's opaque visit continuation, which
 * this deliberately does not do — see [sweepUnmappedCells].
 *
 * A run that can read NO 10040 while reputation documents exist stops
 * without writing anything — that state is indistinguishable from an engine
 * still serving its corpus, and sweeping under it would empty every parent.
 * A store with no reputation documents at all has nothing to migrate and is
 * marked done at once.
 */
class TrustKeyingMigration internal constructor(
    private val reputations: ReputationIndex,
    private val reconciler: TrustReconciler,
    private val recompute: TrustRecompute,
) {
    /** Where this migration has got to, for gauges and [BackgroundFailures]-style status. */
    internal val progress = TrustKeyingProgress()

    /** What one run did: services walked by the reconcile, cell KEYS removed by the sweep (one per unmapped key per parent, both tensors), or refused (no 10040 readable). */
    data class Migration(
        val servicesProjected: Int,
        val keysRemoved: Int,
        val refused: Boolean,
    )

    /** Run from wherever the marker says the last process got to. */
    suspend fun run(onProgress: ((parents: Int, keysRemoved: Int) -> Unit)? = null): Migration {
        if (reputations.get(MARKER_KEY) != null) {
            progress.enter(TrustKeyingProgress.Phase.Done)
            return Migration(0, 0, refused = false)
        }
        // Phases live in their OWN document. The completion marker must keep
        // meaning exactly one thing — "finished" — because every operator check
        // of this migration is `does the document exist`, and a partial marker
        // answering 200 would tell all of them it was done.
        val stored = reputations.get(PROGRESS_KEY)
        progress.resumeFrom(stored.counter(KEYS_REMOVED), UNKNOWN_TOTAL)
        var any = false
        reputations.visitPubkeys { page ->
            any = page.any(Hex::isHex64)
            !any // one page decides; stop as soon as a real parent is seen
        }
        if (!any) {
            reputations.put(marker())
            progress.enter(TrustKeyingProgress.Phase.Done)
            return Migration(0, 0, refused = false)
        }
        val providers = recompute.providerMap()
        if (providers.isEmpty()) {
            progress.enter(TrustKeyingProgress.Phase.AwaitingProviders)
            return Migration(0, 0, refused = true)
        }
        // RESUMED, not repeated: the reconcile is the corpus-scale half, and a
        // marker that already carries [RECONCILED] means a previous process
        // finished it. Re-running it would be correct and ruinous.
        var servicesProjected = 0
        if (!stored.has(RECONCILED)) {
            progress.enter(TrustKeyingProgress.Phase.Reconciling)
            val reconciled =
                reconciler.reconcile { inspected, total, _, _ ->
                    progress.record(inspected.toLong(), total.toLong())
                    onProgress?.invoke(inspected, progress.keysRemoved.get().toInt())
                }
            servicesProjected = reconciled.rebuilt.size
            // NO TOTAL PERSISTED HERE. The number in `progress.total` right now
            // is the RECONCILE's denominator — named services, a dozen or so —
            // and this cell is read back as [PARENTS_TOTAL], the sweep's
            // denominator, which is every parent document in the corpus. A
            // resumed process fed the first as the second and reported
            // "sweeping: 2400000/12 (100%), eta 0m0s". The sweep learns no
            // total of its own (it is a walk, and counting the corpus first
            // would cost a second pass), so it reports none: `line()` prints
            // "total unknown", which is the honest answer and the one its own
            // KDoc asks for.
            reputations.put(progressDoc(reconciled = true))
        }
        progress.enter(TrustKeyingProgress.Phase.Sweeping)
        progress.resumeFrom(stored.counter(KEYS_REMOVED), UNKNOWN_TOTAL)
        val removed = sweepUnmappedCells(providers.services, onProgress)
        reputations.put(marker())
        reputations.remove(PROGRESS_KEY) // the phases were scaffolding; the marker is the answer
        progress.enter(TrustKeyingProgress.Phase.Done)
        return Migration(servicesProjected, removed, refused = false)
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
        var lastPersistedAt = 0
        reputations.visitDocs { page ->
            val removals = ArrayList<CellRemoval>()
            for (doc in page) {
                if (!Hex.isHex64(doc.pubkey)) continue
                parents++
                val keys = (doc.influenceScores.keys + doc.followerCounts.keys).filterNot { it.hex in services }
                for (key in keys) {
                    removals += CellRemoval(doc.pubkey, key, influence = key in doc.influenceScores, followers = key in doc.followerCounts)
                }
            }
            if (removals.isNotEmpty()) {
                reputations.removeCells(removals)
                removed += removals.size
            }
            progress.record(parents.toLong())
            progress.keysRemoved.set(removed.toLong())
            onProgress?.invoke(parents, removed)
            // Counters durable every so often, so a restart reports what the
            // last process achieved instead of starting the number at zero.
            // Every page would be a document write per page of a corpus walk.
            if (parents - lastPersistedAt >= PERSIST_EVERY_PARENTS) {
                lastPersistedAt = parents
                reputations.put(progressDoc(reconciled = true, keysRemoved = removed.toLong()))
            }
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

        /**
         * WHERE THE PHASES LIVE — never [MARKER_KEY]. Removed when the
         * migration completes: the marker is then the whole answer, and a
         * surviving progress document means an attempt that did not finish.
         */
        const val PROGRESS_KEY = "reputation-keying-progress"

        private const val DONE = "done"

        /** The reconcile — the corpus-scale half — landed; a resumed run starts at the sweep. */
        private const val RECONCILED = "reconciled"

        /** Cell keys removed so far, carried across restarts so the number never walks backwards. */
        private const val KEYS_REMOVED = "keys-removed"

        /**
         * A denominator this migration never legitimately had. The cell is
         * still named because a store mid-migration under the previous version
         * carries one — holding the RECONCILE's service count, which the sweep
         * then reported its parent walk against. Neither written nor read now;
         * the sweep reports "total unknown" instead of a number that is wrong.
         */
        private const val PARENTS_TOTAL = "parents-total"

        /** What [TrustKeyingProgress.resumeFrom] is handed for a walk whose size is not knowable up front. */
        private const val UNKNOWN_TOTAL = 0L

        /** Parents between durable counter writes — the walk is millions of documents; the marker is one. */
        private const val PERSIST_EVERY_PARENTS = 25_000

        private fun ReputationDoc?.has(flag: String): Boolean = this?.influenceScores?.containsKey(ServiceKey(flag)) == true

        private fun ReputationDoc?.counter(name: String): Long = (this?.influenceScores?.get(ServiceKey(name)) ?: 0).toLong()

        /**
         * The marker as cells: flags present or absent, counters as their
         * values. Like [ProjectionLedger]'s marker this borrows the reputation
         * tensors as a small key-value store — the ids are non-hex so no card
         * can collide, and no lens reads them.
         */
        private fun marker(): ReputationDoc = ReputationDoc(MARKER_KEY, mapOf(ServiceKey(DONE) to 1))

        private fun progressDoc(
            reconciled: Boolean = false,
            keysRemoved: Long = 0,
        ): ReputationDoc {
            val cells = LinkedHashMap<ServiceKey, Int>()
            if (reconciled) cells[ServiceKey(RECONCILED)] = 1
            if (keysRemoved > 0) cells[ServiceKey(KEYS_REMOVED)] = keysRemoved.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
            return ReputationDoc(PROGRESS_KEY, cells)
        }
    }
}
