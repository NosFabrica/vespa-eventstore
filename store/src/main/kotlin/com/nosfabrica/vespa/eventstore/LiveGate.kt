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
package com.nosfabrica.vespa.eventstore

import com.nosfabrica.vespa.eventstore.trust.TrustCells
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/**
 * THE OBSERVER GATE FOR EVENTS THAT ARRIVE AFTER EOSE.
 *
 * A lensed subscription's stored page is gated in the engine: below-floor
 * authors never leave it. Live events never touch the engine — the relay
 * matches them against the subscription's filters in memory — so without this
 * a signed-in feed dropped an author from its history and then streamed that
 * same author's next note. One gate per subscription, built by
 * [NostrSemanticsStore.liveGate] from the SAME lens and floor each filter's
 * stored read resolves to, so a live event is delivered exactly when the
 * page would have served it.
 *
 * Two speeds, because live delivery runs on the ingest path and must not wait:
 * [admitsNow] answers from trust already in hand (a hit is a map lookup), and
 * says null when an author's trust has to be read first; [admits] reads it.
 * A caller delivers on `true`, drops on `false`, and on null defers the event
 * to [admits] off the ingest path — never delivers first and checks later.
 */
class LiveGate internal constructor(
    private val rules: List<Rule>,
    private val trust: TrustCells,
) {
    /**
     * One filter's gate: the service whose rank cell its lens reads ([service];
     * null is an unresolved lens, "trusts nobody") and its [floor], or no floor
     * when the filter is ungated — no observer, `include:spam`, `sort:text`.
     */
    internal class Rule(
        val filter: Filter,
        val service: String?,
        val floor: Double?,
    )

    /** True/false from trust already known; null when an author must be read first. */
    fun admitsNow(event: Event): Boolean? {
        var unknown = false
        for (rule in rules) {
            if (!rule.filter.match(event)) continue
            val floor = rule.floor ?: return true
            val score = trust.cached(event.pubKey, rule.service)
            if (score == null) {
                unknown = true
            } else if (score >= floor) {
                return true
            }
        }
        return if (unknown) null else false
    }

    /** The full answer, reading any trust not yet in hand. */
    suspend fun admits(event: Event): Boolean {
        for (rule in rules) {
            if (!rule.filter.match(event)) continue
            val floor = rule.floor ?: return true
            if (trust.read(event.pubKey, rule.service) >= floor) return true
        }
        return false
    }
}
