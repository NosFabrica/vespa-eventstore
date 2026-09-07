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
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * `sampled` only helps if it survives the trip to the response check. The
 * check itself reads a field on [com.nosfabrica.vespa.eventstore.engine.query.VespaQuery];
 * a builder that forgets to carry it fails NOWHERE — the read simply goes back
 * to refusing truncated pages, which is how the reconcile spent 85 minutes
 * retrying one query at 57% coverage.
 */
class SampledQueryCarriesThroughTest {
    private val author = "5e".repeat(32)

    @Test
    fun `a sampled search carries the flag into the built query`() {
        val q = assertNotNull(EventYql.build(EventQuery(kinds = listOf(30382), authors = listOf(author), limit = 3, sampled = true)))
        assertTrue(q.sampled, "the search builder dropped `sampled`")
    }

    @Test
    fun `a sampled id-time walk carries the flag into the built query`() {
        val q = assertNotNull(EventYql.buildIdTime(EventQuery(kinds = listOf(30382), authors = listOf(author), sampled = true), withDTag = false))
        assertTrue(q.sampled, "the id-time builder dropped `sampled`")
    }

    @Test
    fun `not asking for a sample is still the default, and still strict`() {
        val q = assertNotNull(EventYql.build(EventQuery(kinds = listOf(30382), authors = listOf(author), limit = 3)))
        assertEquals(false, q.sampled, "`sampled` must be opt-in: every other read still refuses a short answer")
    }

    /**
     * And the semantics it buys, on the guard directly: a match-phase cut is
     * the one a sample may accept — fewer cards, never wrong ones. Every other
     * reason to be short still refuses, sample or not, because those can mean
     * the engine answered from a partial view of the corpus.
     */
    @Test
    fun `a sample accepts a match-phase cut but nothing else`() {
        val matchPhase = SearchCoverage(full = false, coverage = 57, degraded = buildJsonObject { put("match-phase", JsonPrimitive(true)) })
        matchPhase.requireComplete(allowMatchPhase = true)

        val timedOut = SearchCoverage(full = false, coverage = 57, degraded = buildJsonObject { put("timeout", JsonPrimitive(true)) })
        assertFailsWith<IllegalArgumentException>("a timeout is not a sample's to forgive") {
            timedOut.requireComplete(allowMatchPhase = true)
        }
    }
}
