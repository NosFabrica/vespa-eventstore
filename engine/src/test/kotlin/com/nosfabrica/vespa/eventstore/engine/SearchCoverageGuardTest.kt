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

import com.nosfabrica.vespa.eventstore.engine.client.SearchCoverage
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertFailsWith

/**
 * The coverage guard's TRUTH TABLE, asserted on the block directly.
 *
 * [VespaCoverageTest] proves the guard over the wire — that the shapes a real
 * engine emits survive the HTTP client and the decode. This one covers the
 * cells that wire test cannot reach: combinations Vespa itself never renders,
 * which exist in the guard as defense against a version or a proxy that does.
 * Deleting a defensive conjunct must fail a test, or it is not defense, it is
 * decoration.
 */
class SearchCoverageGuardTest {
    private fun flags(vararg set: String): JsonObject = buildJsonObject { set.forEach { put(it, JsonPrimitive(true)) } }

    /**
     * A named reason still wins a rounded-100 percentage. `undegraded` reads
     * BOTH halves — the percentage and the absence of a reason — so the shape
     * that carries a flag at 100% is refused, not waved through by the
     * percentage alone. (Real Vespa renders the block only when it considers
     * itself degraded, so this is belt-and-braces on the flag half.)
     */
    @Test
    fun `a named reason at a rounded 100 percent is still refused`() {
        assertFailsWith<IllegalArgumentException> {
            SearchCoverage(full = false, coverage = 100, degraded = flags("timeout")).requireComplete()
        }
    }

    /**
     * And the percentage half: a sub-100 response with NO reason attached is
     * refused. Vespa cannot emit this (a sub-100 percentage sets
     * `isDegradedByNonIdealState`, which renders the block), but the guard must
     * not depend on that — a proxy that trims the block, or a version that
     * changes when it renders, must not turn into silent acceptance.
     */
    @Test
    fun `a sub-100 percentage with no reason named is refused`() {
        assertFailsWith<IllegalArgumentException> {
            SearchCoverage(full = false, coverage = 60, degraded = null).requireComplete()
        }
    }

    /** The carve-out is an OPT-IN: the same match-phase block is refused on a path that did not ask for the cut. */
    @Test
    fun `the match-phase carve-out does not apply without the opt-in`() {
        val cut = SearchCoverage(full = false, coverage = 80, degraded = flags("match-phase"))

        cut.requireComplete(allowMatchPhase = true)
        assertFailsWith<IllegalArgumentException> { cut.requireComplete() }
    }

    /** A match-phase cut that arrives WITH another flag set is a partial answer again — the carve-out is for the cut alone. */
    @Test
    fun `match-phase alongside a timeout is refused even with the opt-in`() {
        assertFailsWith<IllegalArgumentException> {
            SearchCoverage(full = false, coverage = 80, degraded = flags("match-phase", "timeout")).requireComplete(allowMatchPhase = true)
        }
    }
}
