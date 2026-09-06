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

import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * What separates a cluster that is settling from one whose engine is cutting
 * the match set — the distinction that twice cost hours, because the
 * per-profile degraded counter says a profile was cut without saying why.
 */
class DegradedReadsTest {
    @BeforeTest fun clean() = DegradedReads.reset()

    @AfterTest fun tidy() = DegradedReads.reset()

    @Test
    fun `tallies by profile, flags and shape, keeping the last coverage`() {
        DegradedReads.record("unranked", setOf("match-phase"), "kind,tags,complete", refused = true, coverage = 54, documents = 185_000_000)
        DegradedReads.record("unranked", setOf("match-phase"), "kind,tags,complete", refused = true, coverage = 60, documents = 205_000_000)
        DegradedReads.record("recency", setOf("non-ideal-state"), "kind,author", refused = false, coverage = 99, documents = 340_000_000)

        val rows = DegradedReads.snapshot()
        assertEquals(2, rows.size, "same profile+flags+shape is one row, not two")
        val worst = rows.first()
        assertEquals(2L, worst.count)
        assertEquals(60, worst.lastCoverage, "the LAST reading, so a recovering cluster shows recovery")
        assertTrue(worst.refused)
    }

    /**
     * A MATCH-PHASE CUT ON A RECENCY PROFILE IS ALLOWED AND SERVED SILENTLY.
     * That is the read most worth recording: nothing throws, nothing counts it
     * as an error, and ranked pages quietly get shorter.
     */
    @Test
    fun `records reads that were served, not only the refused ones`() {
        DegradedReads.record("recency", setOf("match-phase"), "search", refused = false, coverage = 61, documents = 200_000_000)
        val line = DegradedReads.line()
        assertContains(line, "recency/search")
        assertContains(line, "match-phase")
        assertContains(line, "served")
    }

    /** Vespa lists every flag including the false ones; only the set ones may key a row. */
    @Test
    fun `flags are joined in a stable order so one condition is one row`() {
        DegradedReads.record("unranked", setOf("timeout", "match-phase"), "kind", refused = true, coverage = 40, documents = 1)
        DegradedReads.record("unranked", setOf("match-phase", "timeout"), "kind", refused = true, coverage = 41, documents = 2)
        assertEquals(1, DegradedReads.snapshot().size, "flag order must not split a row")
        assertContains(DegradedReads.snapshot().first().flags, "match-phase+timeout")
    }

    /** Empty while every read is clean, so a status display can splice it in unconditionally. */
    @Test
    fun `says nothing when nothing is degraded`() {
        assertEquals("", DegradedReads.line())
    }

    /** The shape is clause KINDS; a search term must never reach a line read outside the gate. */
    @Test
    fun `the recorded shape carries no values`() {
        DegradedReads.record("text", setOf("match-phase"), "kind,search", refused = false, coverage = 70, documents = 9)
        val line = DegradedReads.line()
        assertContains(line, "kind,search")
        assertFalse(line.contains("bitcoin"), "shapes only, never terms")
    }
}
