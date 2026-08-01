/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store.trust

import com.vitorpamplona.quartz.eventstore.vespa.client.ReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * Crash safety for the trust projection: a WRITE-AHEAD dirty marker around
 * every trust-mutating op.
 *
 * The event write and the projection write are two separate acks, and dedup
 * guarantees a write trigger fires exactly once — so a crash (or a partial
 * engine failure) BETWEEN them used to be permanent drift: the events are
 * stored, the retry is rejected as duplicates, and nothing is left to fire the
 * recompute. [guarded] closes that window by persisting WHAT the op is about to
 * invalidate before touching anything, and clearing it only after the
 * projection caught up. A marker that survives is exactly the drift, named.
 *
 * The marker is one [ReputationDoc] under [MARKER_KEY] — a non-hex id no card
 * can collide with, since [subjectOf] admits only 64-hex subjects. Its
 * influence cells carry the dirty SUBJECTS (repaired by exact re-derivation,
 * which also removes a parent whose last card died with the crash — the orphan
 * no card walk can reach); its follower cells carry the dirty SERVICES
 * (repaired by re-walking each service's corpus, for ops whose subject set is
 * unknown or too large to persist).
 *
 * [heal] runs the repair. It is invoked at the head of every [guarded] op (a
 * no-op field check once the marker is known clean) and by
 * [TrustReconciler.reconcile] at startup — so drift self-repairs at the next
 * trust write OR the next reconcile, whichever comes first. Callers hold the
 * store's single writer lock (the projection's ops always do; the reconciler
 * takes it through its gate), which is what makes the plain fields here safe.
 *
 * Cost on the hot path: ZERO for ops that touch no trust data, and two small
 * reputation-doc writes per trust-mutating op — noise against the batch they
 * bracket.
 */
internal class DirtLedger(
    private val reputations: ReputationIndex,
    private val recompute: TrustRecompute,
) {
    /** What an interrupted op would leave stale: subjects to re-derive exactly, services to re-walk. */
    internal data class Dirt(
        val subjects: Set<String>,
        val services: Set<String>,
    ) {
        fun isEmpty() = subjects.isEmpty() && services.isEmpty()

        companion object {
            val NONE = Dirt(emptySet(), emptySet())
        }
    }

    /**
     * Unrepaired dirt: null until the persisted marker has been read once (a
     * previous PROCESS may have crashed), then whatever this process's failed
     * ops have accumulated. Guarded by the store writer lock like everything
     * else here.
     */
    private var pending: Dirt? = null

    /**
     * Run [block] with [dirt] persisted first, cleared after. On failure the
     * marker stays (and [pending] remembers it), so the drift the failed op left
     * is repaired by the next [heal] — the retry of a failed batch alone cannot
     * do it, because its events already landed and come back all-duplicates,
     * which never reaches the projection.
     */
    suspend fun <T> guarded(
        dirt: Dirt,
        block: suspend () -> T,
    ): T {
        heal()
        if (dirt.isEmpty()) return block()
        reputations.put(marker(dirt))
        return try {
            val result = block()
            reputations.remove(MARKER_KEY)
            pending = Dirt.NONE
            result
        } catch (t: Throwable) {
            pending = dirt
            throw t
        }
    }

    /**
     * Repair whatever a crashed process or failed op left behind, then clear
     * the marker. Idempotent; throws (leaving the marker intact for the next
     * attempt) if the repair itself fails. The provider-map cache is dropped
     * first — the interrupted op may have died after writing a 10040 but before
     * the invalidation that should have followed it.
     */
    suspend fun heal() {
        val dirt = pending ?: (reputations.get(MARKER_KEY)?.let { Dirt(it.influenceScores.keys, it.followerCounts.keys) } ?: Dirt.NONE)
        pending = dirt
        if (dirt.isEmpty()) return
        recompute.invalidateProviders()
        if (dirt.subjects.isNotEmpty()) recompute.recomputeBatch(dirt.subjects.toList(), recompute.providerMap(), removeEmpties = true)
        if (dirt.services.isNotEmpty()) recompute.recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = dirt.services.toList()))
        reputations.remove(MARKER_KEY)
        pending = Dirt.NONE
    }

    companion object {
        /**
         * The marker's document id. Deliberately NOT 64-hex: [subjectOf] filters
         * subjects to 64-hex, so no event can name this id and the marker can
         * never collide with (or be clobbered by) a real subject's parent doc.
         * It also never joins ranking — the reputation import matches event
         * author pubkeys, which are hex.
         */
        const val MARKER_KEY = "projection-dirty"

        /** The persisted form: subjects ride the influence cells, services the follower cells (values are ignored). */
        private fun marker(dirt: Dirt): ReputationDoc = ReputationDoc(MARKER_KEY, dirt.subjects.associateWith { 1 }, dirt.services.associateWith { 1.0 })
    }
}
