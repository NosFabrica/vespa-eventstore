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

/**
 * TOP-K BY COST, in fixed memory — the answer to the question the cardinality
 * rule forbids as a counter key.
 *
 * "Which observer is burning my relay" is the resource question a multi-tenant
 * operator most wants, and keying a counter by pubkey or search term is exactly
 * the unbounded key space that turns a fixed budget into a leak. Weighted
 * Space-Saving (Metwally et al.) answers it anyway: with [capacity] entries,
 * anything holding more than `1/capacity` of total weight is GUARANTEED to be
 * present, and every entry reports the [Hit.error] it might be overstating by,
 * so a reader can tell a real heavy hitter from a lucky newcomer.
 *
 * WEIGHTED BY COST, not frequency. The question is who is expensive, not who is
 * chatty: one `nostr`-shaped search outweighs a thousand id lookups, and a
 * frequency sketch would rank them the other way round.
 *
 * WHY A PLAIN LOCK IS ENOUGH. Only a read carrying an observer or terms feeds a
 * sketch — id lookups, dedup probes and guard checks never touch it — so this
 * sits on the search path at hundreds of queries per second. Measured
 * (docs/telemetry.md §11.2) at 62.9 ns/add on realistic skewed traffic at
 * K=64, which is single-threaded throughput four orders of magnitude above
 * anything a relay will ask of it. Striping first would be solving a problem
 * nobody has.
 *
 * K IS NOT A FREE KNOB. Eviction scans for the minimum, so a miss is O(capacity)
 * — measured at 159.5 ns for K=64 but 601.3 ns for K=256 under churning
 * traffic, i.e. the cost is worst in the case that matters least. 64 tracks the
 * twenty-odd heavy observers a relay actually has.
 *
 * PRIVACY. A top-K by observer is a ranked list of who searched for what.
 * Callers are expected to hand this TRUNCATED keys, and the whole feature is
 * expected to be off unless an operator configured it — see
 * `CostLedger.slowQueryThresholdNanos` for the same argument applied to the
 * slow-query ring.
 */
class HeavyHitters(
    val capacity: Int = 64,
) {
    /** One tracked key: [weight] is the estimate, [error] the most it may be overstating by. */
    class Hit(
        val key: String,
        val weight: Long,
        val error: Long,
    )

    private class Slot(
        @JvmField var key: String,
        @JvmField var weight: Long,
        @JvmField var error: Long,
    )

    private val slots = arrayOfNulls<Slot>(capacity)
    private val index = HashMap<String, Slot>(capacity * 2)
    private var used = 0
    private val lock = Any()

    /** Charge [weight] of cost to [key]. */
    fun add(
        key: String,
        weight: Long,
    ) {
        if (weight <= 0) return
        synchronized(lock) {
            val hit = index[key]
            if (hit != null) {
                hit.weight += weight
                return
            }
            if (used < capacity) {
                val fresh = Slot(key, weight, 0L)
                slots[used++] = fresh
                index[key] = fresh
                return
            }
            // Evict the minimum. The newcomer inherits its weight as an error
            // bound, which is what makes the guarantee hold: anything genuinely
            // heavy can never be evicted below the running minimum.
            var min = slots[0]!!
            for (i in 1 until capacity) {
                val s = slots[i]!!
                if (s.weight < min.weight) min = s
            }
            index.remove(min.key)
            min.key = key
            min.error = min.weight
            min.weight += weight
            index[key] = min
        }
    }

    /** The [n] heaviest keys, heaviest first. */
    fun top(n: Int): List<Hit> =
        synchronized(lock) {
            slots
                .asSequence()
                .take(used)
                .filterNotNull()
                .map { Hit(it.key, it.weight, it.error) }
                .sortedByDescending { it.weight }
                .take(n)
                .toList()
        }

    /** Test seam. */
    fun reset() =
        synchronized(lock) {
            index.clear()
            for (i in 0 until capacity) slots[i] = null
            used = 0
        }
}
