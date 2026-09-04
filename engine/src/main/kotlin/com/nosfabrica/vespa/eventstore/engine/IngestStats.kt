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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Process-wide wall-time accounting for the ingest pipeline's named stages —
 * answers "the writer is busy 100%, but WHERE?". Stages self-register on first
 * use; [statusLine] renders per-stage seconds since its last call, [dump] the
 * cumulative split. Global on
 * purpose: store and projection are separate modules composed per process, and
 * threading a stats object through both APIs would couple them for what is
 * strictly observability.
 */
object IngestStats {
    private val stages = ConcurrentHashMap<String, AtomicLong>()
    private val lastSeen = ConcurrentHashMap<String, Long>()

    /**
     * How a stage's time was SPENT, not just how much: the same 24 minutes is
     * one pathological call or a hundred thousand ordinary ones, and only the
     * count tells them apart. Measured on staging 2026-09-04, where
     * `proj.fetch` held the store's write gate for 24 minutes and the total
     * alone could not say whether to look at the query or at the loop around
     * it.
     */
    class Stage(
        val totalNanos: Long,
        val calls: Long,
        val maxNanos: Long,
    ) {
        /** Mean call, in nanos — 0 when nothing has been booked (a stage can be added to without being timed). */
        val meanNanos: Long get() = if (calls > 0) totalNanos / calls else 0L
    }

    private val calls = ConcurrentHashMap<String, AtomicLong>()
    private val maxima = ConcurrentHashMap<String, AtomicLong>()

    /** Add [nanos] of wall time to [stage]. */
    fun add(
        stage: String,
        nanos: Long,
    ) {
        // computeIfAbsent, NOT getOrPut: the latter compiles to get-then-put, so
        // concurrent first-touches of a new stage each construct their own
        // AtomicLong and every increment but the last put's is dropped (measured
        // at 8 threads racing one fresh key: 123 of 200 trials lost one).
        stages.computeIfAbsent(stage) { AtomicLong() }.addAndGet(nanos)
    }

    /** Time [body], booking its wall time under [stage]. */
    suspend fun <T> timed(
        stage: String,
        body: suspend () -> T,
    ): T {
        val t0 = System.nanoTime()
        try {
            return body()
        } finally {
            val took = System.nanoTime() - t0
            add(stage, took)
            // Booked only by [timed]: `add` is also called with a duration
            // measured elsewhere (the lock stages book wait and hold from one
            // pair of timestamps), and counting those as calls here would
            // report a mean over two different populations.
            calls.computeIfAbsent(stage) { AtomicLong() }.incrementAndGet()
            maxima.computeIfAbsent(stage) { AtomicLong() }.accumulateAndGet(took, ::maxOf)
        }
    }

    /** One status-line snapshot: per-stage seconds SINCE THE LAST CALL, busiest first; empty when idle. */
    fun statusLine(): String {
        val parts =
            stages.entries
                .mapNotNull { (name, total) ->
                    val now = total.get()
                    val delta = now - (lastSeen.put(name, now) ?: 0L)
                    if (delta < 50_000_000) null else name to delta
                }.sortedByDescending { it.second }
        if (parts.isEmpty()) return ""
        return "stages " + parts.joinToString(" ") { (n, d) -> "$n %.1fs".format(d / 1e9) }
    }

    /** Every stage's cumulative seconds, no threshold — the full split for profiling. */
    fun dump(): String {
        if (stages.isEmpty()) return "stages (none)"
        return "stages " +
            stages.entries
                .map { (n, t) -> n to t.get() }
                .sortedByDescending { it.second }
                .joinToString(" ") { (n, ns) -> "$n %.2fs".format(ns / 1e9) }
    }

    /**
     * WHO HOLDS THE WRITE LOCK RIGHT NOW, and since when. The cumulative
     * stages say the gate was held for 24 minutes; they cannot say whether it
     * is held *at this instant*, by what, or for how long so far — which is
     * the only question worth asking while ingest is stalled. One field
     * because the store serialises every write behind ONE mutex, so there is
     * never more than one holder.
     */
    class Held(
        val stage: String,
        val sinceNanos: Long,
        val detail: String?,
    ) {
        fun heldForMillis(): Long = (System.nanoTime() - sinceNanos) / 1_000_000
    }

    @Volatile
    private var held: Held? = null

    /** Called by the lock helper once the mutex is actually acquired. */
    fun beginHold(
        stage: String,
        detail: String? = null,
    ) {
        held = Held(stage, System.nanoTime(), detail)
    }

    /**
     * Say what the holder is DOING, from inside the critical section — the
     * stage name alone is `lock.gate.hold`, which names the lock and not the
     * work. Keeps the original start time: this annotates a hold, it does not
     * restart one.
     */
    fun annotateHold(detail: String) {
        held?.let { held = Held(it.stage, it.sinceNanos, detail) }
    }

    /** Called on release. Tolerates a missing begin — an unmatched end is a no-op, never a wrong holder. */
    fun endHold() {
        held = null
    }

    /** The current holder, or null when nothing holds the write lock. */
    fun heldNow(): Held? = held

    /**
     * The structured read the formatted ones should have been. Cumulative and
     * repeatable like [dump] — never destructive like [statusLine] — so any
     * number of callers may sample it.
     *
     * Exists because the relay parses [dump]'s String today and says so in its
     * own comment ("the wrong shape, and it is the only one available"). A
     * stage that was added to but never [timed] reports `calls = 0`, which is
     * the honest answer rather than a mean over a denominator that does not
     * exist.
     */
    fun snapshot(): Map<String, Stage> =
        stages.entries.associate { (name, total) ->
            name to
                Stage(
                    totalNanos = total.get(),
                    calls = calls[name]?.get() ?: 0L,
                    maxNanos = maxima[name]?.get() ?: 0L,
                )
        }
}
