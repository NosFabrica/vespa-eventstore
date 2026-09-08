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

import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * "What did the engine do" and "who made it do that" are different questions,
 * and the ledger used to answer only the first. Attributing load then meant an
 * ablation — on 2026-09-06 the only way to find what was driving ~20M docs/s
 * on staging was to scale the mirror to zero and watch it fall.
 */
class EngineByCallerTest {
    private fun ledger() = CostLedger()

    @Test
    fun `the same query is booked to the profile AND to the caller`() {
        val l = ledger()
        l.engineQuery(
            profile = "unranked",
            engineNanos = 1_000,
            summaryNanos = 100,
            docsMatched = 50,
            hitsServed = 5,
            degraded = false,
            activity = Activity.Drain,
            shape = "kinds,authors",
        )
        val snap = l.snapshot()
        assertEquals(1, snap.engine.single().queries, "the per-profile table is unchanged")
        val caller = snap.engineByCaller.single()
        assertEquals(Activity.Drain, caller.activity)
        assertEquals("unranked", caller.profile)
        assertEquals("kinds,authors", caller.shape)
        assertEquals(50, caller.docsMatched, "the same numbers, not a second count")
    }

    /** Two callers on ONE profile is the case the per-profile table cannot show. */
    @Test
    fun `two callers on the same profile are told apart`() {
        val l = ledger()
        repeat(3) {
            l.engineQuery(
                profile = "unranked",
                engineNanos = 10_000,
                summaryNanos = 0,
                docsMatched = 1_000_000,
                hitsServed = 1,
                degraded = false,
                activity = Activity.Drain,
                shape = "kinds,authors",
            )
        }
        l.engineQuery(
            profile = "unranked",
            engineNanos = 5,
            summaryNanos = 0,
            docsMatched = 2,
            hitsServed = 2,
            degraded = false,
            activity = Activity.Query,
            shape = "ids",
        )
        val snap = l.snapshot()
        assertEquals(1, snap.engine.size, "one profile, as before")
        assertEquals(2, snap.engineByCaller.size, "two callers, told apart")
        val heaviest = snap.engineByCaller.first()
        assertEquals(Activity.Drain, heaviest.activity, "heaviest first — that is the answer an operator wants")
        assertEquals(3_000_000, heaviest.docsMatched)
    }

    /**
     * THE PRIVACY RULE. This table feeds the pulse page, which is admin-gated
     * because it quotes what people searched for. Attribution carries the
     * clause SHAPE and nothing else — the same rule DegradedReads follows.
     */
    @Test
    fun `attribution carries a shape, never a term or an observer`() {
        val l = ledger()
        l.engineQuery(
            profile = "search",
            engineNanos = 1,
            summaryNanos = 1,
            docsMatched = 1,
            hitsServed = 1,
            degraded = false,
            activity = Activity.Query,
            shape = "kinds,search,observer",
        )
        val row = l.snapshot().engineByCaller.single()
        val rendered = "${row.activity}|${row.profile}|${row.shape}"
        assertTrue("bitcoin" !in rendered && "npub" !in rendered, "no search terms or keys reach this table: $rendered")
        assertEquals("kinds,search,observer", row.shape, "the shape names clause KINDS, not their values")
    }

    /** A caller that does not declare itself must not create a phantom row. */
    @Test
    fun `an unattributed query still books the profile and adds no caller row`() {
        val l = ledger()
        l.engineQuery(profile = "unranked", engineNanos = 1, summaryNanos = 0, docsMatched = 1, hitsServed = 1, degraded = false)
        val snap = l.snapshot()
        assertEquals(1, snap.engine.single().queries)
        assertTrue(snap.engineByCaller.isEmpty(), "no activity, no attribution — not a row keyed on a guess")
    }
}
