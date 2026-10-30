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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.hints.SubjectReferences
import com.vitorpamplona.quartz.utils.EventFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * [SearchReferences.of] is quartz's [SubjectReferences] reading behind this
 * store's decision of WHICH kinds to read at all.
 */
class SearchReferencesTest {
    private val signer = "b2".repeat(32)
    private val member = "d4".repeat(32)

    private fun event(
        kind: Int,
        vararg tags: Array<String>,
    ) = EventFactory.create<Event>("0".repeat(63) + "1", signer, 1_000L, kind, arrayOf(*tags), "", "")

    /**
     * Every kind this store expands is one quartz can read, so a pointer is
     * never routed here only to come back empty. The converse is NOT asserted
     * on purpose: a family a later quartz learns stays unexpanded until this
     * store names it (and decides whether it is gated).
     */
    @Test
    fun `every kind this store expands is one quartz reads`() {
        assertTrue(SubjectReferences.KINDS.containsAll(SearchReferences.KINDS), "${SearchReferences.KINDS - SubjectReferences.KINDS}")
        assertEquals(SearchReferences.KINDS - SearchReferences.DECLARATIONS, setOf(1985), "the label is the only ungated pointer")
    }

    @Test
    fun `a kind this store does not expand names nothing`() {
        // 30385: a NIP-73 external-id assertion, no nostr record on the other end.
        assertTrue(SearchReferences.of(event(30385, arrayOf("d", "isbn:9780000000000"))).isEmpty())
        // Kind 3, a follow list: the social graph, not a curation.
        assertTrue(SearchReferences.of(event(3, arrayOf("p", member))).isEmpty())
    }

    /**
     * A member tag's index-3 score is quartz's `MemberTagFields.score`: 0..100
     * inclusive, and a number on any other scale DROPPED (the member stands,
     * unscored) rather than clamped into a confident 100. Scores are keyed by
     * the canonical (lowercase) reference, the key a found subject is filed
     * under.
     */
    @Test
    fun `member scores on the 0 to 100 scale are kept and every other number is dropped`() {
        val keys = listOf("a", "b", "c", "d", "e", "f").map { it.repeat(64) }
        val refs =
            SearchReferences.of(
                event(
                    30392,
                    arrayOf("d", "roster"),
                    arrayOf("p", keys[0].uppercase(), "", "0"),
                    arrayOf("p", keys[1], "wss://r.example/", "100"),
                    arrayOf("p", keys[2], "", "950"),
                    arrayOf("p", keys[3], "", "-1"),
                    arrayOf("p", keys[4], "", "0.5"),
                    arrayOf("p", keys[5]),
                    arrayOf("e", member, "", "40"),
                ),
            )
        assertEquals(keys, refs.pubKeys, "every member stands, lowercased; the e tag is not a 30392 member")
        assertEquals(mapOf(keys[0] to 0, keys[1] to 100), refs.scores)
        assertEquals(0.0, refs.weightOf(keys[0]))
        assertEquals(null, refs.weightOf(keys[2]))
    }

    /** An assertion's `d` IS its address: only a pubkey as NIP-01 writes one (64 lowercase hex) names a subject. */
    @Test
    fun `an assertion's d names a subject only when it is exactly a pubkey`() {
        assertEquals(listOf(member), SearchReferences.of(event(30382, arrayOf("d", member))).pubKeys)
        assertTrue(SearchReferences.of(event(30382, arrayOf("d", member.uppercase()))).isEmpty())
        assertTrue(SearchReferences.of(event(30382, arrayOf("d", "$member:v2"))).isEmpty())
    }
}
