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

/**
 * HOW MUCH OF THE TRUST VIEW IS ACTUALLY USABLE — the number nobody computed.
 *
 * `trust.queued.services` says the drain's QUEUE is empty, which is not the
 * same as every named service being projected: a service enters that queue
 * only the first time a kind-10040 names it. Reading the queue as coverage is
 * how a service with 279,594 cards and cells on ~1% of parents sat unwalked
 * while every signal read clean, and how an observer could not find their own
 * profile through their own lens.
 *
 * This is the fraction users experience: of the services some provider list
 * names, how many carry cells — and of the provider lists themselves, how many
 * resolve to one that does. [TrustReconciler.reconcile] already decides
 * exactly this per service to choose what to rebuild; it just threw the
 * verdicts away afterwards.
 *
 * A STANDING FACT, not a counter: it is whatever the last reconcile saw, and
 * stale between runs. [checkedAtMs] says how stale, because a coverage number
 * with no age is the kind of reassurance that hides a problem for days.
 */
internal object TrustCoverage {
    @Volatile
    var servicesNamed: Long = 0
        private set

    @Volatile
    var servicesProjected: Long = 0
        private set

    @Volatile
    var lensesTotal: Long = 0
        private set

    @Volatile
    var lensesResolvable: Long = 0
        private set

    @Volatile
    var checkedAtMs: Long = 0
        private set

    fun record(
        named: Long,
        projected: Long,
        lenses: Long,
        resolvable: Long,
    ) {
        servicesNamed = named
        servicesProjected = projected
        lensesTotal = lenses
        lensesResolvable = resolvable
        checkedAtMs = System.currentTimeMillis()
    }

    fun reset() = record(0, 0, 0, 0).also { checkedAtMs = 0 }

    /** Empty until a reconcile has run — an unmeasured coverage must not read as 0%. */
    fun line(): String =
        if (checkedAtMs == 0L) {
            ""
        } else {
            val ageSec = (System.currentTimeMillis() - checkedAtMs) / 1000
            val sPct = if (servicesNamed > 0) servicesProjected * 100 / servicesNamed else 0
            val lPct = if (lensesTotal > 0) lensesResolvable * 100 / lensesTotal else 0
            "trust-coverage services $servicesProjected/$servicesNamed ($sPct%), lenses $lensesResolvable/$lensesTotal ($lPct%), measured ${ageSec}s ago"
        }
}
