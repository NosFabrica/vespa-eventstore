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

import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [EventIndex.scoreLinks]' contract on the in-memory reference — the executable
 * spec the real client's three reads must reproduce (`TrustedLinksIT` checks it
 * does, gate and zapper trust included, on a real Vespa). Every expected number
 * is worked by hand from [LinkWeights]' three layers: a repeat worth half the
 * one before (n same-kind links = 2 − 2^(1−n)), a person clamped to ±5 weight
 * units, people summed, the hit's author left out.
 */
class ScoreLinksTest {
    private val author = "a1".repeat(32)
    private val bob = "b2".repeat(32)
    private val carol = "c3".repeat(32)
    private val dave = "d4".repeat(32)
    private val zapService = "e5".repeat(32)
    private val frank = "f6".repeat(32)
    private val note = "70".repeat(32)
    private val address = "30023:$author:my-article"

    private var seq = 0

    private fun doc(
        kind: Int,
        pubkey: String,
        vararg tags: List<String>,
    ) = EventDoc(
        id = (++seq).toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = 1_700_000_000L + seq,
        kind = kind,
        tags = tags.toList(),
        content = "",
        sig = "",
    )

    private val target = LinkTarget(note, author, address)

    private suspend fun score(
        vararg docs: EventDoc,
        weights: LinkWeights = LinkWeights(),
        linkers: EventQuery = EventQuery(),
    ): LinkScore? = InMemoryEventIndex().apply { putAll(docs.toList()) }.scoreLinks(listOf(target), linkers, weights)[note]

    private val e = listOf("e", note)

    @Test
    fun `repeats diminish per person and kind, kinds add up, and the author is no one`() =
        runBlocking {
            val s =
                score(
                    doc(7, bob, e),
                    doc(7, bob, e), // bob's second like: +0.5
                    doc(6, bob, e), // bob's repost: a new kind, a full 1
                    doc(1, carol, e, listOf("a", address)), // ONE reply naming both keys: two entries, so 1 + 0.5
                    doc(1, author, e),
                    doc(1, author, e),
                    doc(7, author, e), // the author's own thread: none of it counts
                )!!
            assertEquals(2.5 + 1.5, s.score, 1e-12)
            assertEquals(2, s.people)
            assertEquals(mapOf(7 to 2, 6 to 1, 1 to 2), s.byKind, "the links that counted — the author's are not among them")
        }

    @Test
    fun `one person stops counting at the cap, and reports pull below zero`() =
        runBlocking {
            // Six different kinds from dave: 6 units, clamped to 5.
            val capped = score(*listOf(1, 6, 7, 16, 1111, 10003).map { doc(it, dave, e) }.toTypedArray())!!
            assertEquals(5.0, capped.score, 1e-12)

            val reports = LinkWeights(byKind = mapOf(1984 to -1.0))
            // Bob reports three times (−1.75), carol likes once (+1).
            val reported = score(doc(1984, bob, e), doc(1984, bob, e), doc(1984, bob, e), doc(7, carol, e), weights = reports)!!
            assertEquals(-0.75, reported.score, 1e-12)
            assertEquals(-1.75, score(doc(1984, bob, e), doc(1984, bob, e), doc(1984, bob, e), weights = reports)!!.score, 1e-12, "only reports: negative")
        }

    @Test
    fun `a zap counts for the zapper its P tag names, once per receipt`() =
        runBlocking {
            val s =
                score(
                    // Three receipts for frank's zaps, all signed by the same LNURL server:
                    // one person, three repeats — 1.75. Grouping by signer would have made
                    // the service the person, and every zapper on it one.
                    doc(9735, zapService, e, listOf("P", frank)),
                    doc(9735, zapService, e, listOf("P", frank)),
                    doc(9735, zapService, e, listOf("P", frank)),
                    // A receipt naming no zapper: its own one-link "person" — 1.
                    doc(9735, zapService, e),
                    // The author zapping their own note: no one.
                    doc(9735, zapService, e, listOf("P", author)),
                    // One receipt naming the id AND the address is still one zap.
                    doc(9735, zapService, e, listOf("a", address), listOf("P", carol)),
                )!!
            assertEquals(1.75 + 1.0 + 1.0, s.score, 1e-12)
            assertEquals(3, s.people, "frank, carol, and the unnamed zapper")
            assertEquals(mapOf(9735 to 5), s.byKind)
        }

    @Test
    fun `the linkers filter narrows who counts, and nothing linking is absent`() =
        runBlocking {
            val docs = arrayOf(doc(7, bob, e), doc(7, carol, e), doc(1, carol, e))
            assertEquals(2.0, score(*docs, linkers = EventQuery(authors = listOf(carol)))!!.score, 1e-12)
            assertEquals(2.0, score(*docs, linkers = EventQuery(kinds = listOf(7)))!!.score, 1e-12)
            assertNull(score(*docs, linkers = EventQuery(limit = 0)), "limit 0 is the matches-nothing sentinel")
            assertNull(score(doc(7, bob, listOf("e", "ff".repeat(32)))), "a hit nothing links to is absent, not zero")
            assertNull(score(doc(7, author, e)), "only the author linked: no one did")
            // An allow-list: otherwise = 0 never asks the engine about the other kinds.
            assertEquals(listOf(1984), LinkWeights(byKind = mapOf(1984 to -1.0), otherwise = 0.0).narrow(EventQuery()).kinds)
            assertEquals(0, LinkWeights(byKind = mapOf(7 to 0.0), otherwise = 0.0).narrow(EventQuery()).limit)
        }

    @Test
    fun `the fold is the three layers and nothing else`() {
        val w = LinkWeights(byKind = mapOf(1984 to -1.0, 7 to 0.5))
        assertEquals(1.0, w.repeats(1))
        assertEquals(1.75, w.repeats(3), 1e-12)
        assertTrue(w.repeats(1_000) <= 2.0)
        // A like at 0.5 weight from a person trusted 0.8: 0.4.
        assertEquals(0.4, w.person(Linker(0.8, mapOf(7 to 1))), 1e-12)
        // COUNT is plain counting: no decay, no cap a count can reach.
        assertEquals(12.0, LinkWeights.COUNT.person(Linker(1.0, mapOf(7 to 12))), 1e-12)
    }
}
