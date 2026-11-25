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
package com.nosfabrica.vespa.eventstore.engine.client

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ClusterTokenTest {
    private val token = VespaVisits.ClusterToken

    @Test
    fun `a token round-trips under the shard count that issued it`() {
        assertEquals(1 to "AAAA", token.parse(token.format(2, 1, "AAAA"), 2))
        assertEquals(1 to null, token.parse(token.format(2, 1, null), 2), "the hand-over to the next cluster's start")
    }

    /** A reindex saved mid-walk on two clusters and resumed on three: its index names another cluster's walk. */
    @Test
    fun `a token from another shard count, or from before sharding, is refused`() {
        val saved = token.format(2, 1, "AAAA")
        assertFailsWith<IllegalArgumentException> { token.parse(saved, 3) }
        assertFailsWith<IllegalArgumentException>("a one-cluster token") { token.parse("AAAAQAAA", 2) }
        assertFailsWith<IllegalArgumentException>("the first cut's index-only token") { token.parse("1:AAAA", 2) }
        assertFailsWith<IllegalArgumentException>("an index past the clusters") { token.parse("2/2:AAAA", 2) }
    }
}
