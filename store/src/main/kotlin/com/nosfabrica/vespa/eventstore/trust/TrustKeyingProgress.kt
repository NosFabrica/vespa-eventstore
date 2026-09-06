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

import java.util.concurrent.atomic.AtomicLong

/**
 * WHERE THE KEYING MIGRATION HAS GOT TO — the answer to "when will this
 * finish", which the migration could not give before.
 *
 * Every layer under it already took an `onProgress`, and nothing passed one:
 * [TrustKeyingMigration.run], [TrustReconciler.reconcile] and
 * [MaxRankBackfill.run] all reported, and the store called each of them with
 * the argument left out, so a walk that takes hours over a real corpus was
 * indistinguishable from one that had not started. This holds what they say.
 *
 * PROCESS-LOCAL AND LOSSY BY DESIGN: a restart resets it, and the durable
 * half of the answer is the marker document the migration writes as it
 * crosses each phase ([TrustKeyingMigration.MARKER_KEY]). This is the live
 * view; that is the one that survives.
 */
internal class TrustKeyingProgress {
    /** The steps of one migration, in the order [TrustKeyingMigration.run] takes them. */
    enum class Phase {
        /** No run has begun in this process. */
        NotStarted,

        /** Waiting for a readable kind 10040 — the refusal that retries. */
        AwaitingProviders,

        /** [TrustReconciler.reconcile] — walks each named service's cards into cells. */
        Reconciling,

        /** [TrustKeyingMigration.sweepUnmappedCells] — drops every cell no 10040 names. */
        Sweeping,

        /** The marker is written; nothing further runs in this store's lifetime. */
        Done,
    }

    @Volatile
    var phase: Phase = Phase.NotStarted
        private set

    /** Parents visited by the phase that is running, and the denominator when one is known (0 = unknown). */
    val visited = AtomicLong()
    val total = AtomicLong()

    /** Cell keys the sweep has removed, cumulative across resumed runs. */
    val keysRemoved = AtomicLong()

    /** When the current phase began, for the rate the ETA is computed from. */
    private val phaseStartedMs = AtomicLong(System.currentTimeMillis())

    fun enter(next: Phase) {
        if (next == phase) return
        phase = next
        visited.set(0)
        total.set(0)
        phaseStartedMs.set(System.currentTimeMillis())
    }

    /** Adopt counters a previous process persisted, so a resumed run reports totals and not just its own slice. */
    fun resumeFrom(
        keysAlreadyRemoved: Long,
        knownTotal: Long,
    ) {
        keysRemoved.set(keysAlreadyRemoved)
        if (knownTotal > 0) total.set(knownTotal)
    }

    fun record(
        visitedNow: Long,
        totalNow: Long = 0,
    ) {
        visited.set(visitedNow)
        if (totalNow > 0) total.set(totalNow)
    }

    /**
     * The operator's line. Reports a fraction and an ETA only where a
     * denominator is actually known — a walk with no total says so rather
     * than inventing a percentage, because the denominator is exactly what
     * was missing when this was asked for.
     */
    fun line(): String {
        val p = phase
        if (p == Phase.NotStarted || p == Phase.Done) return "trust-keying $p".lowercase()
        val seen = visited.get()
        val all = total.get()
        val elapsedMs = (System.currentTimeMillis() - phaseStartedMs.get()).coerceAtLeast(1)
        val removed = keysRemoved.get()
        val tail = if (removed > 0) ", $removed cell key(s) removed" else ""
        if (all <= 0 || seen <= 0) return "trust-keying ${p.name.lowercase()}: $seen visited, total unknown$tail"
        val pct = (seen * 100 / all).coerceAtMost(100)
        val rate = seen.toDouble() / (elapsedMs / 1000.0)
        val etaSec = if (rate > 0) ((all - seen) / rate).toLong() else -1
        val eta = if (etaSec < 0) "unknown" else "${etaSec / 60}m${etaSec % 60}s"
        return "trust-keying ${p.name.lowercase()}: $seen/$all ($pct%), ${"%.0f".format(rate)}/s, eta $eta$tail"
    }
}
