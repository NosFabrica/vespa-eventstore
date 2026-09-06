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

/**
 * IS RANKED SEARCH WORKING, AND IF NOT WHY — the trust view's health, as
 * numbers a page can render.
 *
 * Separate from the pulse on purpose. The pulse answers "where did the store's
 * time go" and is gated because it quotes what people searched for. Nothing
 * here is: coverage is counts, progress is phases, and the degraded-read
 * shapes carry clause kinds and never their values. Putting these behind that
 * gate is what made an incomplete projection invisible for days, so they are
 * deliberately publishable.
 */
class TrustHealth(
    /** Services some kind-10040 names, and how many carry cells on any subject. */
    val servicesNamed: Long,
    val servicesProjected: Long,
    /**
     * Provider lists, and how many resolve to a service that carries cells.
     * THE NUMBER USERS FEEL: a lens resolving to an unprojected service serves
     * an empty ranked page, and the gate failing closed is correct — the
     * projection is what is incomplete.
     */
    val lensesTotal: Long,
    val lensesResolvable: Long,
    /** When a reconcile last measured the above; 0 = never, which is not the same as 0%. */
    val measuredAtMs: Long,
    /** Repairs running or lately finished. */
    val steps: List<Step>,
) {
    class Step(
        val op: String,
        val phase: String,
        val done: Long,
        /** 0 where the walk has no denominator; a page must say so rather than draw 0%. */
        val total: Long,
        val elapsedSec: Long,
        val finished: Boolean,
    )

    /** Never measured reads differently from measured-at-zero, and a page must not conflate them. */
    val measured: Boolean get() = measuredAtMs > 0
}
