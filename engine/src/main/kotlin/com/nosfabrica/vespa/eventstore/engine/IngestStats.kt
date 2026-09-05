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
     * WHO HOLDS A WRITE LOCK RIGHT NOW, and since when. The cumulative
     * stages say the gate was held for 24 minutes; they cannot say whether it
     * is held *at this instant*, by what, or for how long so far — which is
     * the only question worth asking while ingest is stalled. ONE SLOT PER
     * LOCK, keyed by the hold stage: the store has two mutexes since the trust
     * gate split, and a single slot let a plain insert's short `writes` hold
     * overwrite — and then, on release, ERASE — the drain's seconds-long gate
     * hold, so [heldNow] answered "nothing" for most of every drain slice.
     */
    class Held(
        val stage: String,
        val sinceNanos: Long,
        val detail: String?,
        /**
         * WHICH MUTEX this hold is on, which is not the same as [stage].
         *
         * Several stage labels share one mutex: `lock.gate`,
         * `lock.ingest.trust`, `lock.sweep.trust` and `lock.reindex.trust` are
         * all the trust gate; `lock.ingest`, `lock.sweep` and `lock.reindex`
         * are all the write lock. Keying the HOLD by stage is right — a mutex
         * has one holder, so the labels never collide — but matching a WAITER
         * to its holder by label misses whenever the two took the same mutex
         * under different names, which is the common case.
         *
         * Found on a real corpus (docs/telemetry.md §15.1): 7.4 s of measured
         * gate wait attributed to nobody, because every waiter arrived under a
         * different label than the holder. Empty when a caller does not say.
         */
        val lock: String = "",
    ) {
        fun heldForMillis(): Long = (System.nanoTime() - sinceNanos) / 1_000_000
    }

    private val held = ConcurrentHashMap<String, Held>()

    /** Called by the lock helper once the mutex is actually acquired. */
    fun beginHold(
        stage: String,
        detail: String? = null,
        lock: String = "",
    ) {
        held[stage] = Held(stage, System.nanoTime(), detail, lock)
    }

    /**
     * Say what the holder is DOING, from inside the critical section — the
     * stage name alone is `lock.gate.hold`, which names the lock and not the
     * work. Keeps the original start time: this annotates a hold, it does not
     * restart one. Annotates every current hold: the caller is inside the
     * critical section of whichever lock(s) it holds, and the work it names
     * is what all of them are held for.
     */
    fun annotateHold(detail: String) {
        held.replaceAll { _, h -> Held(h.stage, h.sinceNanos, detail, h.lock) }
    }

    /** Called on release of the lock booked under [stage]. Tolerates a missing begin — an unmatched end is a no-op, never a wrong holder. */
    fun endHold(stage: String) {
        held.remove(stage)
    }

    /** The LONGEST current holder, or null when no write lock is held; [heldAll] lists every one. */
    fun heldNow(): Held? = held.values.minByOrNull { it.sinceNanos }

    /** Every lock held right now, longest first. */
    fun heldAll(): List<Held> = held.values.sortedBy { it.sinceNanos }

    /** The most specific label a hold can offer: what it is doing, else which lock it is. */
    fun labelOf(h: Held): String = h.detail ?: h.stage

    /**
     * WHO HOLDS THE MUTEX NAMED [lock] RIGHT NOW — the causal edge, for the
     * price of scanning a map that never has more than a couple of entries.
     *
     * A waiter can read this BEFORE it blocks, because the lock helper captures
     * its request timestamp before entering the mutex. That turns `lock.*.wait`
     * from a scalar ("ingest waited 41 s", which only prompts a question) into
     * an attribution ("1.77 s of it behind `derive 499 subject(s) in 10
     * chunk(s)`", which names a fix).
     *
     * KEYED BY THE MUTEX, not the stage label — see [Held.lock].
     *
     * FIRST-HOLDER ATTRIBUTION, and a reader should know it: over a long wait
     * the lock may change hands several times, and all of that wait is charged
     * to whoever held it when the waiter arrived. For the case this exists to
     * catch — one pathological holder stalling everyone — that is exactly
     * right; for a uniformly busy queue it over-attributes to the head.
     */
    fun holderOf(lock: String): Held? = held.values.filter { it.lock == lock }.minByOrNull { it.sinceNanos }

    private val blocked = ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>>()

    /** Book [nanos] of [waitStage] as time spent behind [holder]. */
    fun addBlocked(
        waitStage: String,
        holder: String,
        nanos: Long,
    ) {
        blocked
            .computeIfAbsent(waitStage) { ConcurrentHashMap() }
            .computeIfAbsent(holder) { AtomicLong() }
            .addAndGet(nanos)
    }

    /** Wait time per lock stage, split by what was holding when the waiter arrived. */
    fun blockedSplit(): Map<String, Map<String, Long>> = blocked.mapValues { (_, row) -> row.mapValues { it.value.get() } }

    /** Test seam: forget every stage, hold and attribution. */
    fun reset() {
        stages.clear()
        calls.clear()
        maxima.clear()
        lastSeen.clear()
        held.clear()
        blocked.clear()
    }

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
