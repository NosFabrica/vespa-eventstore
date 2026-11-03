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

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class UnionCountTest {
    private val a = "a1".repeat(32)
    private val b = "b2".repeat(32)

    @Test
    fun `filters share a match unless a constraint both carry cannot meet`() {
        assertFalse(EventQuery(kinds = listOf(1)).mayShareAMatchWith(EventQuery(kinds = listOf(7))))
        assertFalse(EventQuery(authors = listOf(a)).mayShareAMatchWith(EventQuery(authors = listOf(b))))
        assertFalse(EventQuery(ids = listOf(a)).mayShareAMatchWith(EventQuery(ids = listOf(b))))
        assertTrue(EventQuery(kinds = listOf(1, 7)).mayShareAMatchWith(EventQuery(kinds = listOf(7))))
        assertTrue(EventQuery(kinds = listOf(1)).mayShareAMatchWith(EventQuery(authors = listOf(a))), "a missing constraint is no constraint")
    }

    @Test
    fun `windows are inclusive at both ends`() {
        assertTrue(EventQuery(until = 10).mayShareAMatchWith(EventQuery(since = 10)))
        assertFalse(EventQuery(until = 9).mayShareAMatchWith(EventQuery(since = 10)))
        assertFalse(EventQuery(since = 10).mayShareAMatchWith(EventQuery(until = 9)))
    }

    @Test
    fun `groups join through a shared neighbour`() {
        val notes = EventQuery(kinds = listOf(1))
        val reactions = EventQuery(kinds = listOf(7))
        val either = EventQuery(kinds = listOf(1, 7))
        val zaps = EventQuery(kinds = listOf(9735))
        assertEquals(listOf(1, 1, 1), overlapGroups(listOf(notes, zaps, reactions)).map { it.size })
        assertEquals(listOf(3, 1), overlapGroups(listOf(notes, zaps, reactions, either)).map { it.size }.sortedDescending())
    }
}
