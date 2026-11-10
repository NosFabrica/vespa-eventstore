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

import kotlin.test.Test
import kotlin.test.assertEquals

class CostLedgerTest {
    /**
     * A probe resets the ledger between variants and reads what the next one costs. The
     * per-profile and per-caller engine tables book the same queries, so after a reset they
     * must agree, both empty; the gauges are registrations rather than counts, and survive.
     */
    @Test
    fun `reset forgets the per-caller engine table with the per-profile one, and keeps the gauges`() {
        val ledger = CostLedger()
        ledger.gauge("feed.inflight") { 7 }
        ledger.engineQuery(
            profile = "unranked",
            engineNanos = 1_000,
            summaryNanos = 100,
            docsMatched = 50,
            hitsServed = 5,
            degraded = false,
            activity = Activity.Drain,
            shape = "kinds,authors",
        )
        assertEquals(1, ledger.snapshot().engineByCaller.size, "booked to the caller")

        ledger.reset()
        val after = ledger.snapshot()
        assertEquals(emptyList(), after.engine.map { it.profile }, "the per-profile table is empty")
        assertEquals(emptyList(), after.engineByCaller.map { "${it.activity} ${it.profile} ${it.shape}" }, "and so is the per-caller one")
        assertEquals(mapOf("feed.inflight" to 7L), after.gauges, "a gauge is a registration, not a count: still read")

        // And the next query lands in both alike.
        ledger.engineQuery("unranked", 10, 1, 2, 1, false, activity = Activity.Drain, shape = "kinds,authors")
        val next = ledger.snapshot()
        assertEquals(next.engine.single().queries, next.engineByCaller.single().queries, "the two tables agree")
        assertEquals(1, next.engineByCaller.single().queries)
    }
}
