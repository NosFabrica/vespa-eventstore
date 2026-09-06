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
package com.nosfabrica.vespa.eventstore.runtime

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Failure accounting for the store's background workers — the trust-projection
 * drain and the guard-owner refresh.
 *
 * Both retry forever by design, and that is right: their state is safe under
 * failure (the backlog marker still names the work; the previous guard sets stay
 * in place), so a transient engine outage must not kill the worker. What it
 * costs is VISIBILITY. A worker failing every cycle is indistinguishable from
 * one with nothing to do, and a drain that never succeeds means ranking has
 * silently stopped tracking trust writes — the failure mode with no symptom.
 * This is the signal, read back through [VespaEventStore.backgroundStatus].
 *
 * [consecutive] is what separates a blip from a stuck worker: a cumulative
 * count alone cannot, since "failed 812 times over a month" and "has failed
 * 812 times in a row since Tuesday" are the same number.
 *
 * Process-wide and dependency-free, like `IngestStats` — a library has no
 * business picking a logging framework for its embedder.
 */
internal object BackgroundFailures {
    /** The trust-projection drain worker (`VespaEventStore.startDrainer`). */
    const val TRUST_DRAIN = "trust.drain"

    /** The guard-owner cache refresher (`GuardOwners.startRefresher`). */
    const val GUARD_REFRESH = "guards.refresh"

    /** The one-time `max_rank` walk the trust descent waits on (MaxRankBackfill). */
    const val MAX_RANK_BACKFILL = "trust.maxRankBackfill"

    /** The one-time re-keying of a store fed under the observer-keyed model (TrustKeyingMigration). */
    const val TRUST_KEYING = "trust.keying"

    /** Exception chain depth reported — a timeout wrapped three deep says more than the outermost class. */
    private const val CAUSE_DEPTH = 4

    /** Frames from this package name the walk underneath a timeout; the rest are engine plumbing. */
    private const val OUR_PACKAGE = "com.nosfabrica"

    /** Longest failure message kept — enough to name the cause, not to hold a stack trace. */
    private const val MAX_MESSAGE = 200

    private class Tally {
        val count = AtomicLong()
        val consecutive = AtomicLong()

        @Volatile var lastMessage: String = ""

        /** Where in OUR code it threw, which the message never says. */
        @Volatile var lastWhere: String = ""

        /** One stack per task: a worker retrying forever must not flood the log. */
        val printedStack =
            java.util.concurrent.atomic
                .AtomicBoolean(false)
    }

    private val tallies = ConcurrentHashMap<String, Tally>()

    /** [task]'s cycle failed with [error]. Never throws — accounting must not break the worker it reports on. */
    fun record(
        task: String,
        error: Throwable,
    ) {
        val tally = tallies.computeIfAbsent(task) { Tally() }
        tally.count.incrementAndGet()
        tally.consecutive.incrementAndGet()
        tally.lastMessage = (error.message ?: error.toString()).take(MAX_MESSAGE)
        tally.lastWhere = whereOf(error)
        // THE FIRST ONE GETS THE STACK, ON STDERR. A message alone is not a
        // diagnosis: this deployment spent an afternoon on `last: timeout`,
        // which named neither the query that timed out nor the walk it was in,
        // and every caller of this recorded the message and dropped the
        // exception. Once per task, so a worker retrying forever does not
        // flood the log with the same trace.
        if (tally.printedStack.compareAndSet(false, true)) {
            System.err.println("background $task: first failure — ${error::class.simpleName}: ${error.message?.take(MAX_MESSAGE)}")
            error.printStackTrace()
        }
    }

    /**
     * The first frames that belong to THIS codebase, plus the exception chain.
     * A timeout's own frames are all engine and HTTP plumbing; what an
     * operator needs is which of our walks was underneath it.
     */
    private fun whereOf(error: Throwable): String {
        val chain = generateSequence(error) { it.cause }.take(CAUSE_DEPTH).joinToString(" <- ") { it::class.simpleName ?: "?" }
        val ours =
            error.stackTrace
                .firstOrNull { it.className.startsWith(OUR_PACKAGE) }
                ?.let { "${it.className.substringAfterLast('.')}.${it.methodName}:${it.lineNumber}" }
        return if (ours == null) chain else "$chain at $ours"
    }

    /**
     * [task]'s cycle completed. Only the CONSECUTIVE run resets — the
     * cumulative count is kept, so a worker that recovered still says it had
     * trouble.
     */
    fun succeeded(task: String) {
        tallies[task]?.consecutive?.set(0)
    }

    /**
     * One status line for the workers that have ever failed; empty while the
     * process is clean, so a caller can splice it into a status display
     * unconditionally.
     */
    fun statusLine(): String {
        val parts =
            tallies.entries
                .filter { it.value.count.get() > 0 }
                .sortedByDescending { it.value.count.get() }
                .map { (task, tally) ->
                    val stuck = tally.consecutive.get()
                    val where = tally.lastWhere.takeIf { it.isNotBlank() }?.let { " | $it" } ?: ""
                    "$task ${tally.count.get()} fail" +
                        if (stuck > 0) " ($stuck consecutive, last: ${tally.lastMessage}$where)" else ""
                }
        return if (parts.isEmpty()) "" else "background " + parts.joinToString("; ")
    }

    /** How many times [task] has failed IN A ROW, 0 once it has recovered. */
    fun consecutiveFailures(task: String): Long = tallies[task]?.consecutive?.get() ?: 0

    /** Test seam: forget every tally, so one test's failures cannot leak into the next. */
    fun reset() = tallies.clear()
}
