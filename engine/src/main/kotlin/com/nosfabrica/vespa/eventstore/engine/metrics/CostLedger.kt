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
package com.nosfabrica.vespa.eventstore.engine.metrics

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

/**
 * WHERE THE STORE'S RESOURCES GO — the structured, non-destructive, cumulative
 * record. See docs/telemetry.md for the argument; this is its implementation.
 *
 * THREE ALTITUDES, because no single one can see everything:
 *  - **port** ([port]) — every call through `EventIndex`, booked by the
 *    [Activity] that caused it. Placed as a decorator for the same reason
 *    `TrustProjection` is: it sees every route to the engine with no
 *    instrumentation at any call site.
 *  - **outcomes** ([outcome]) — admission decisions, which happen ABOVE the
 *    port. A refused event never reaches `EventIndex`, so nothing below can
 *    count it.
 *  - **engine** ([engineQuery]) — rank profile, Vespa's own reported time,
 *    documents matched, coverage. Only the client that speaks to Vespa knows
 *    these; a port decorator sees an `EventQuery`, not a profile.
 *
 * Plus [gauge]s, which are instantaneous and have no cumulative form, and two
 * bounded structures for the dimensions the cardinality rule forbids as keys:
 * [HeavyHitters] and the slow-query ring.
 *
 * COUNTERS, NEVER RATIOS. Everything here is cumulative and monotonic, so any
 * number of readers may sample it and a window is the difference between two
 * snapshots. A pre-divided rate cannot be re-windowed — a ratio over a window
 * is the summed numerator over the summed denominator, and averaging
 * per-scrape ratios is wrong in a way that looks plausible.
 *
 * PER STORE, not process-wide. Unlike `IngestStats` (which predates this and
 * is shared across modules by static reference), a ledger belongs to the store
 * that owns it: two stores in one process keep separate books, and a test
 * cannot leak into the next one.
 *
 * COST: ~97 ns per instrumented call, ~100 KiB retained for a whole store, both
 * measured in docs/telemetry.md §§5–6, 11.5.
 */
class CostLedger(
    /**
     * Reads slower than this are captured in the slow-query ring; null (the
     * default) captures nothing.
     *
     * OFF BY DEFAULT ON PURPOSE. The ring is the one place this design retains
     * a query string, and a query string is user data — an operator choosing to
     * keep a log of what people searched for should have to say so rather than
     * discover it.
     */
    val slowQueryThresholdNanos: Long? = null,
    /** Capacity of the by-observer and by-term sketches; see [HeavyHitters]. */
    heavyHitterCapacity: Int = 64,
    /** How many slow reads to retain. Bounded by the ring, never by the term space. */
    slowQueryRing: Int = 256,
) {
    // ---------------------------------------------------------------- ports

    /**
     * One (activity × call) cell. Indexed by ordinal arithmetic rather than
     * hashed: the key space is closed and known at class-load, so the hot path
     * is an array index instead of a `ConcurrentHashMap.get`.
     */
    class PortSlot internal constructor() {
        /** Port invocations — the amortization metric ("round trips per event"). */
        internal val calls = LongAdder()

        /** Wall time inside the call. */
        internal val nanos = LongAdder()

        /** Documents in or out — written, returned, probed. */
        internal val docs = LongAdder()

        /**
         * Allocated on first use, so a store that never counts pays nothing:
         * `Latencies` is ~1.6 KiB and most of the 96 cells stay empty in any
         * real deployment.
         */
        @Volatile internal var latencies: Latencies? = null

        internal fun latenciesOrCreate(): Latencies {
            latencies?.let { return it }
            synchronized(this) {
                latencies?.let { return it }
                val fresh = Latencies()
                latencies = fresh
                return fresh
            }
        }
    }

    private val slots = Array(Activity.ALL.size * PortCall.ALL.size) { PortSlot() }

    private fun slot(
        activity: Activity,
        call: PortCall,
    ): PortSlot = slots[activity.ordinal * PortCall.ALL.size + call.ordinal]

    /**
     * Book one port call: [nanos] of wall time moving [docs] documents.
     *
     * [docs] is the denominator half of every ratio worth reading — round trips
     * per event, engine time per hit — and it must be booked on the SAME cell
     * as the cost or the quotient is meaningless.
     */
    fun port(
        activity: Activity,
        call: PortCall,
        nanos: Long,
        docs: Long = 1,
    ) {
        val s = slot(activity, call)
        s.calls.increment()
        s.nanos.add(nanos)
        if (docs != 0L) s.docs.add(docs)
        if (call == PortCall.Search || call == PortCall.Count) s.latenciesOrCreate().record(nanos)
    }

    // ------------------------------------------------------------- outcomes

    /** Admission outcomes per activity: the reason string (or [ADMITTED]) to its count. */
    private val outcomes = ConcurrentHashMap<Activity, ConcurrentHashMap<String, LongAdder>>()

    /**
     * Book one admission decision. [reason] is [ADMITTED] or one of Quartz's
     * `RejectionReason` values — a CLOSED set, which is what keeps this inside
     * the cardinality rule.
     *
     * This altitude exists because the port cannot see it: an event rejected
     * for being a duplicate never reaches `EventIndex`, so the decorator
     * observes the dedup probe and the absence of a write, which is not the
     * same thing and breaks the moment a batch path changes.
     */
    fun outcome(
        activity: Activity,
        reason: String,
        n: Long = 1,
    ) {
        outcomes
            .computeIfAbsent(activity) { ConcurrentHashMap() }
            .computeIfAbsent(reason) { LongAdder() }
            .add(n)
    }

    // --------------------------------------------------------------- engine

    /** Per-rank-profile engine cost. Vespa's own accounting, not the client's wall clock. */
    class EngineSlot internal constructor() {
        internal val queries = LongAdder()
        internal val engineNanos = LongAdder()
        internal val summaryNanos = LongAdder()
        internal val docsMatched = LongAdder()
        internal val hitsServed = LongAdder()
        internal val degraded = LongAdder()
        internal val rungs = LongAdder()
    }

    private val engine = ConcurrentHashMap<String, EngineSlot>()

    /**
     * Book one engine query. [engineNanos] and [summaryNanos] come from Vespa's
     * own `timing` block, which splits "the match phase is expensive" from "we
     * asked for 2,500 summaries" — a different fix in each case, and invisible
     * from the client's wall clock.
     */
    fun engineQuery(
        profile: String,
        engineNanos: Long,
        summaryNanos: Long,
        docsMatched: Long,
        hitsServed: Long,
        degraded: Boolean,
        rungs: Int = 0,
    ) {
        val s = engine.computeIfAbsent(profile) { EngineSlot() }
        s.queries.increment()
        s.engineNanos.add(engineNanos)
        s.summaryNanos.add(summaryNanos)
        s.docsMatched.add(docsMatched)
        s.hitsServed.add(hitsServed)
        if (degraded) s.degraded.increment()
        if (rungs > 0) s.rungs.add(rungs.toLong())
    }

    // --------------------------------------------------------------- gauges

    private val gauges = ConcurrentHashMap<String, () -> Long>()

    /**
     * Register an instantaneous reading — queue depth, in-flight, set size.
     *
     * PULLED, not accumulated: a gauge costs nothing until a snapshot asks, and
     * it must never be diffed between snapshots the way a counter is (a queue
     * depth is not a rate). The supplier must be cheap and non-blocking; the
     * owner of the value publishes it safely rather than letting this reach
     * into its state.
     */
    fun gauge(
        name: String,
        read: () -> Long,
    ) {
        gauges[name] = read
    }

    // -------------------------------------------------- bounded by-key views

    /** Top observers by engine cost. Keys are expected to arrive TRUNCATED — see [HeavyHitters]. */
    val byObserver = HeavyHitters(heavyHitterCapacity)

    /** Top search terms by engine cost. */
    val byTerm = HeavyHitters(heavyHitterCapacity)

    /** One captured slow read. */
    class SlowRead(
        val atMillis: Long,
        val activity: Activity,
        val profile: String,
        val wallNanos: Long,
        val engineNanos: Long,
        val summaryNanos: Long,
        val hits: Long,
        val docsMatched: Long,
        val detail: String,
    )

    private val slowRing = arrayOfNulls<SlowRead>(slowQueryRing)
    private val slowSeq = AtomicLong()

    /**
     * Capture a slow read, if [slowQueryThresholdNanos] is set and this beat
     * it. Overwrites the oldest — bounded by the ring, never by how many
     * distinct queries exist, which is what keeps a retained query string
     * inside the cardinality rule.
     */
    fun slowRead(
        activity: Activity,
        profile: String,
        wallNanos: Long,
        engineNanos: Long,
        summaryNanos: Long,
        hits: Long,
        docsMatched: Long,
        detail: String,
    ) {
        val threshold = slowQueryThresholdNanos ?: return
        if (wallNanos < threshold) return
        val at = (slowSeq.getAndIncrement() % slowRing.size).toInt()
        slowRing[at] =
            SlowRead(
                System.currentTimeMillis(),
                activity,
                profile,
                wallNanos,
                engineNanos,
                summaryNanos,
                hits,
                docsMatched,
                detail,
            )
    }

    // ------------------------------------------------------------- snapshot

    /** One (activity × call) cell, read back. */
    class PortStat(
        val activity: Activity,
        val call: PortCall,
        val calls: Long,
        val nanos: Long,
        val docs: Long,
        val p50Nanos: Long,
        val p99Nanos: Long,
    ) {
        /** Port invocations per document — the "never ingest in a loop over insert()" number. */
        val callsPerDoc: Double get() = if (docs > 0) calls.toDouble() / docs else 0.0

        val meanNanos: Long get() = if (calls > 0) nanos / calls else 0L
    }

    /** One rank profile, read back. */
    class EngineStat(
        val profile: String,
        val queries: Long,
        val engineNanos: Long,
        val summaryNanos: Long,
        val docsMatched: Long,
        val hitsServed: Long,
        val degraded: Long,
        val rungs: Long,
    )

    /**
     * The whole record, cumulative and repeatable. Safe to call from any number
     * of readers as often as they like — nothing here is consumed by being
     * read, unlike `IngestStats.statusLine()`.
     */
    class Snapshot(
        val ports: List<PortStat>,
        val outcomes: Map<Activity, Map<String, Long>>,
        val engine: List<EngineStat>,
        val gauges: Map<String, Long>,
        val topObservers: List<HeavyHitters.Hit>,
        val topTerms: List<HeavyHitters.Hit>,
        val slowReads: List<SlowRead>,
    ) {
        /** Events the store accepted — the denominator most write-side costs are read per. */
        val admitted: Long get() = outcomes.values.sumOf { it[ADMITTED] ?: 0L }

        /** Events offered, admitted or not. */
        val offered: Long get() = outcomes.values.sumOf { row -> row.values.sum() }

        fun port(
            activity: Activity,
            call: PortCall,
        ): PortStat? = ports.firstOrNull { it.activity == activity && it.call == call }

        /** Total port calls under [activity], whatever the call shape. */
        fun callsUnder(activity: Activity): Long = ports.filter { it.activity == activity }.sumOf { it.calls }
    }

    fun snapshot(): Snapshot {
        val ports =
            buildList {
                for (a in Activity.ALL) {
                    for (c in PortCall.ALL) {
                        val s = slot(a, c)
                        val calls = s.calls.sum()
                        if (calls == 0L) continue
                        val lat = s.latencies
                        add(
                            PortStat(
                                activity = a,
                                call = c,
                                calls = calls,
                                nanos = s.nanos.sum(),
                                docs = s.docs.sum(),
                                p50Nanos = lat?.percentile(0.50) ?: 0L,
                                p99Nanos = lat?.percentile(0.99) ?: 0L,
                            ),
                        )
                    }
                }
            }
        return Snapshot(
            ports = ports,
            outcomes = outcomes.mapValues { (_, row) -> row.mapValues { it.value.sum() } },
            engine =
                engine.map { (profile, s) ->
                    EngineStat(
                        profile = profile,
                        queries = s.queries.sum(),
                        engineNanos = s.engineNanos.sum(),
                        summaryNanos = s.summaryNanos.sum(),
                        docsMatched = s.docsMatched.sum(),
                        hitsServed = s.hitsServed.sum(),
                        degraded = s.degraded.sum(),
                        rungs = s.rungs.sum(),
                    )
                },
            // Gauges are PULLED here, and a broken supplier must not take the
            // whole snapshot down with it: observability that can crash its
            // caller is worse than none.
            gauges =
                gauges.entries
                    .mapNotNull { (name, read) ->
                        runCatching { name to read() }.getOrNull()
                    }.toMap(),
            topObservers = byObserver.top(TOP_N),
            topTerms = byTerm.top(TOP_N),
            slowReads = slowRing.filterNotNull().sortedByDescending { it.atMillis },
        )
    }

    /** Test seam: forget everything. */
    fun reset() {
        slots.forEach {
            it.calls.reset()
            it.nanos.reset()
            it.docs.reset()
            it.latencies?.reset()
        }
        outcomes.clear()
        engine.clear()
        byObserver.reset()
        byTerm.reset()
        for (i in slowRing.indices) slowRing[i] = null
        slowSeq.set(0)
    }

    companion object {
        /** The outcome key for an event the store stored. */
        const val ADMITTED = "admitted"

        /** How many heavy hitters a snapshot carries. */
        const val TOP_N = 20
    }
}
