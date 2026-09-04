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
 * cumulative split, [snapshot] the structured read. Global on
 * purpose: store and projection are separate modules composed per process, and
 * threading a stats object through both APIs would couple them for what is
 * strictly observability. (The newer per-store `CostLedger` in `metrics/` is
 * instance-scoped; this one predates it and is reached statically from both
 * modules.)
 */
object IngestStats {
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

    /**
     * ONE cell per stage, holding all three counters.
     *
     * Was three parallel `ConcurrentHashMap`s keyed by the same string. That
     * cost three `computeIfAbsent` lookups per [timed] call, and — the reason
     * it changed — made [snapshot] read the three maps at three different
     * instants, so a stage could report a `totalNanos` that did not correspond
     * to its `calls` and a `meanNanos` computed from a mismatched pair. One
     * cell is read atomically enough that the three always describe the same
     * stage, and the lookup happens once.
     */
    private class Cell {
        val total = AtomicLong()
        val calls = AtomicLong()
        val max = AtomicLong()
    }

    private val stages = ConcurrentHashMap<String, Cell>()
    private val lastSeen = ConcurrentHashMap<String, Long>()

    // computeIfAbsent, NOT getOrPut: the latter compiles to get-then-put, so
    // concurrent first-touches of a new stage each construct their own Cell and
    // every increment but the last put's is dropped (measured at 8 threads
    // racing one fresh key: 123 of 200 trials lost one).
    private fun cell(stage: String): Cell = stages.computeIfAbsent(stage) { Cell() }

    /** Add [nanos] of wall time to [stage]. */
    fun add(
        stage: String,
        nanos: Long,
    ) {
        cell(stage).total.addAndGet(nanos)
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
            val c = cell(stage)
            c.total.addAndGet(took)
            // Booked only by [timed]: `add` is also called with a duration
            // measured elsewhere (the lock stages book wait and hold from one
            // pair of timestamps), and counting those as calls here would
            // report a mean over two different populations.
            c.calls.incrementAndGet()
            c.max.accumulateAndGet(took, ::maxOf)
        }
    }

    /** One status-line snapshot: per-stage seconds SINCE THE LAST CALL, busiest first; empty when idle. */
    @Deprecated("Destructive: consumes the delta for every stage at once, so two callers corrupt each other. Diff two snapshot() reads instead.")
    fun statusLine(): String {
        val parts =
            stages.entries
                .mapNotNull { (name, c) ->
                    val now = c.total.get()
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
                .map { (n, c) -> n to c.total.get() }
                .sortedByDescending { it.second }
                .joinToString(" ") { (n, ns) -> "$n %.2fs".format(ns / 1e9) }
    }

    /**
     * WHO HOLDS A WRITE LOCK RIGHT NOW, and since when. The cumulative
     * stages say the gate was held for 24 minutes; they cannot say whether it
     * is held *at this instant*, by what, or for how long so far — which is
     * the only question worth asking while ingest is stalled.
     */
    class Held(
        val stage: String,
        val sinceNanos: Long,
    ) {
        /**
         * What the holder is DOING, set from inside the critical section. The
         * stage name alone is `lock.gate.hold`, which names the lock and not
         * the work.
         */
        @Volatile var detail: String? = null

        fun heldForMillis(): Long = (System.nanoTime() - sinceNanos) / 1_000_000

        /** The most specific label available: what it is doing, else which lock it is. */
        val label: String get() = detail ?: stage
    }

    /**
     * Every hold currently open, across the process.
     *
     * Was one `@Volatile` field holding one [Held], justified as "the store
     * serialises every write behind ONE mutex, so there is never more than one
     * holder". That was true when it was written and stopped being true when
     * the trust gate was split off the write lock: `lockedForWrite` now nests
     * two mutexes, so the inner `beginHold` overwrote the outer and the inner
     * release cleared it — reporting the outer lock as UNHELD while it was
     * still held.
     *
     * NOT a `ThreadLocal` stack, which is the obvious repair and is wrong here:
     * this store is coroutines end to end, and a `suspend` body may resume on a
     * different dispatcher thread than it started on, so a push and its pop can
     * land on two different stacks. The open set is keyed by identity and the
     * ambient [HoldContext] carries the current hold through suspension the way
     * the coroutine machinery already carries everything else.
     */
    private val live: MutableSet<Held> = ConcurrentHashMap.newKeySet()

    /** Carries the innermost hold across suspension points, so [annotateHold] finds it wherever it resumes. */
    private class HoldContext(
        val held: Held,
    ) : kotlin.coroutines.AbstractCoroutineContextElement(HoldContext) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<HoldContext>
    }

    /**
     * Hold [stage] for the extent of [body] — called by the lock helper once
     * the mutex is actually acquired.
     */
    suspend fun <T> holding(
        stage: String,
        body: suspend () -> T,
    ): T {
        val held = Held(stage, System.nanoTime())
        live.add(held)
        try {
            return kotlinx.coroutines.withContext(HoldContext(held)) { body() }
        } finally {
            live.remove(held)
        }
    }

    /**
     * Say what the current holder is doing. Keeps the original start time: this
     * annotates a hold, it does not restart one. A no-op outside a [holding]
     * block rather than an error — accounting must never break the work it
     * reports on.
     */
    suspend fun annotateHold(detail: String) {
        kotlin.coroutines.coroutineContext[HoldContext]
            ?.held
            ?.detail = detail
    }

    /**
     * The hold that has been open LONGEST, or null when nothing is held. With
     * two nesting mutexes there can be several; the oldest is the one worth
     * showing, because it is the one a stalled pipeline is waiting out.
     */
    fun heldNow(): Held? = live.minByOrNull { it.sinceNanos }

    /** Every hold currently open — what a status page shows. */
    fun allHeld(): List<Held> = live.sortedBy { it.sinceNanos }

    /**
     * WHO HOLDS [holdStage] RIGHT NOW — the causal edge, for the price of
     * scanning a set that never has more than a couple of entries.
     *
     * A waiter can read this BEFORE it blocks, because the lock helper captures
     * its request timestamp before entering the mutex. That turns `lock.*.wait`
     * from a scalar ("ingest waited 41 s", which only prompts a question) into
     * an attribution ("38 of those 41 s were behind `proj.fetch.derive`", which
     * names a fix). Matching on the exact hold stage makes it the holder of the
     * SAME lock rather than whatever happened to be running.
     *
     * FIRST-HOLDER ATTRIBUTION, and a reader should know it: over a long wait
     * the lock may change hands several times, and all of that wait is charged
     * to whoever held it when the waiter arrived. For the case this exists to
     * catch — one pathological holder stalling everyone — that is exactly
     * right; for a uniformly busy queue it over-attributes to the head.
     */
    fun holderOf(holdStage: String): Held? = live.filter { it.stage == holdStage }.minByOrNull { it.sinceNanos }

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

    private val blocked = ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicLong>>()

    /** Wait time per lock stage, split by what was holding when the waiter arrived. */
    fun blockedSplit(): Map<String, Map<String, Long>> = blocked.mapValues { (_, row) -> row.mapValues { it.value.get() } }

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
        stages.entries.associate { (name, c) ->
            name to Stage(totalNanos = c.total.get(), calls = c.calls.get(), maxNanos = c.max.get())
        }

    /** Test seam: forget every stage, hold and attribution. */
    fun reset() {
        stages.clear()
        lastSeen.clear()
        blocked.clear()
        live.clear()
    }
}
