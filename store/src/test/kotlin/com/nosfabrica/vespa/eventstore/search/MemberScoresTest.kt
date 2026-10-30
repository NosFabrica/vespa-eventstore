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
package com.nosfabrica.vespa.eventstore.search

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * [memberScores] reads a member tag's index-3 score through Quartz's
 * `MemberTagFields.score` — the family's own rule, which this store used to
 * copy: 0..100 inclusive, and a number on any other scale DROPPED (the member
 * stands, unscored) rather than clamped into a confident 100.
 */
class MemberScoresTest {
    @Test
    fun `scores on the 0 to 100 scale are kept and every other number is dropped`() {
        val tags =
            arrayOf(
                arrayOf("p", "a", "", "0"),
                arrayOf("p", "b", "wss://r.example/", "100"),
                arrayOf("p", "c", "", "950"),
                arrayOf("p", "d", "", "-1"),
                arrayOf("p", "e", "", "0.5"),
                arrayOf("p", "f"),
                arrayOf("e", "g", "", "40"),
            )
        assertEquals(mapOf("a" to 0, "b" to 100), tags.memberScores("p") { it[1] })
    }
}
