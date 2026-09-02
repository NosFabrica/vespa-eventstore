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
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventSelection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * [EventSelection] builder + [MockSelection] parser: the emitted grammar must
 * round-trip back to the query it came from (the visit-side drift alarm), and
 * anything a document selection can't express must be refused, never
 * approximated.
 */
class EventSelectionTest {
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)

    private fun roundTrips(q: EventQuery) {
        val selection = EventSelection.build(q)!!
        assertEquals(q, MockSelection.parse(selection), "selection: $selection")
    }

    @Test
    fun `emits the documented grammar`() {
        assertEquals("true", EventSelection.build(EventQuery()))
        assertEquals("(event.kind==30382)", EventSelection.build(EventQuery(kinds = listOf(30382))))
        assertEquals(
            "(event.kind==0 or event.kind==1) and (event.pubkey==\"$alice\" or event.pubkey==\"$bob\") and " +
                "event.created_at>=100 and event.created_at<=200 and event.expires_at>150",
            EventSelection.build(
                EventQuery(kinds = listOf(0, 1), authors = listOf(alice, bob), since = 100, until = 200, notExpiredAt = 150),
            ),
        )
        assertEquals("(event.owner==\"$bob\")", EventSelection.build(EventQuery(owners = listOf(bob))))
    }

    @Test
    fun `round-trips through the mock parser`() {
        roundTrips(EventQuery())
        roundTrips(EventQuery(kinds = listOf(30382)))
        roundTrips(EventQuery(kinds = listOf(0, 1, 30382), authors = listOf(alice, bob)))
        roundTrips(EventQuery(owners = listOf(alice), since = 5, until = 9, notExpiredAt = 7))
    }

    @Test
    fun `refuses what a selection cannot express`() {
        assertNull(EventSelection.build(EventQuery(ids = listOf("f".repeat(64)))))
        assertNull(EventSelection.build(EventQuery(tags = mapOf("d" to listOf("x")))))
        assertNull(EventSelection.build(EventQuery(tagsAll = mapOf("t" to listOf("a", "b")))))
        assertNull(EventSelection.build(EventQuery(search = "vitor")))
        // A selection sees no derived search fields, so a dropped exclusion
        // would stream exactly the docs the query subtracted — and a dropped
        // phrase would stream everything the requirement filtered out.
        assertNull(EventSelection.build(EventQuery(notSearch = listOf("vitor"))))
        assertNull(EventSelection.build(EventQuery(phrases = listOf("vitor pamplona"))))
        // The WEIGHTED key sets recall through a dotProduct a selection has no
        // form for. Dropping one would not narrow the walk — it would widen it
        // to the whole corpus while the caller believed it named a few hundred
        // keys.
        assertNull(EventSelection.build(EventQuery(idWeights = mapOf("f".repeat(64) to 80))))
        assertNull(EventSelection.build(EventQuery(authorWeights = mapOf("a1".repeat(32) to 80))))
        assertNull(EventSelection.build(EventQuery(limit = 10)))
        assertNull(EventSelection.build(EventQuery(expiresBefore = 100)))
        // Injection rule: a non-64-hex key never reaches the expression.
        assertNull(EventSelection.build(EventQuery(authors = listOf("not-hex"))))
        assertNull(EventSelection.build(EventQuery(owners = listOf("\" or true"))))
    }
}
