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
import kotlin.test.assertTrue

/**
 * [EventIndex.scoreLinks]' contract on the in-memory reference — the executable
 * spec the real client's grouping must reproduce (`TrustedLinksIT` checks it
 * does, gate included, on a real Vespa).
 */
class ScoreLinksTest {
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)
    private val target = "70".repeat(32)
    private val other = "71".repeat(32)
    private val address = "30023:$alice:my-article"

    private var seq = 0

    private fun doc(
        kind: Int,
        vararg tags: List<String>,
        pubkey: String = bob,
    ) = EventDoc(
        id = (++seq).toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = 1_700_000_000L + seq,
        kind = kind,
        tags = tags.toList(),
        content = "",
        sig = "",
    )

    @Test
    fun `links are counted per key and per kind, across letters, absent keys omitted`() =
        runBlocking {
            val idx = InMemoryEventIndex()
            idx.putAll(
                listOf(
                    doc(7, listOf("e", target), listOf("p", alice)),
                    doc(7, listOf("e", target)),
                    doc(6, listOf("e", target)),
                    // Names the target AND the address: counted once under EACH key.
                    doc(1, listOf("e", target), listOf("a", address)),
                    doc(1111, listOf("A", address), listOf("a", address)),
                    // Links elsewhere: never reaches an answer about [target].
                    doc(7, listOf("e", other)),
                ),
            )
            val links = idx.scoreLinks(listOf("e:$target", "a:$address", "e:${"ff".repeat(32)}"), EventQuery(), LinkWeights.COUNT)
            // No reputation data in the reference: every linker is fully trusted, so a link is worth its weight.
            assertEquals(LinkScore(4.0, mapOf(7 to 2, 6 to 1, 1 to 1), mapOf(7 to 2.0, 6 to 1.0, 1 to 1.0)), links["e:$target"])
            assertEquals(LinkScore(2.0, mapOf(1 to 1, 1111 to 1), mapOf(1 to 1.0, 1111 to 1.0)), links["a:$address"])
            assertEquals(setOf("e:$target", "a:$address"), links.keys, "a key nothing links to is absent, not zero")
        }

    @Test
    fun `weights fold the kinds into a signed score, and a reports-only key scores below zero`() =
        runBlocking {
            val idx = InMemoryEventIndex()
            idx.putAll(
                listOf(
                    doc(7, listOf("e", target)),
                    doc(7, listOf("e", target)),
                    doc(1984, listOf("e", target, "spam")),
                    doc(1984, listOf("e", other, "spam")),
                    doc(1984, listOf("e", other, "nudity")),
                ),
            )
            val weights = LinkWeights(byKind = mapOf(1984 to -1.0, 7 to 0.5))
            val links = idx.scoreLinks(listOf("e:$target", "e:$other"), EventQuery(), weights)
            assertEquals(0.0, links["e:$target"]?.score, "two half-weight likes cancel one report")
            assertEquals(-2.0, links["e:$other"]?.score, "only reports: negative")

            // What the real engine hands back under a lens: each kind's SUMMED linker trust (0..1 per link).
            // Two likes from authors trusted 0.9 and 0.3, one report from an author trusted 0.4:
            assertEquals(0.5 * 1.2 - 0.4, weights.score(mapOf(7 to 2, 1984 to 1), mapOf(7 to 1.2, 1984 to 0.4)).score, 1e-12)

            // `otherwise = 0` makes byKind an allow-list — and the engine is never asked about the rest.
            val reportsOnly = LinkWeights(byKind = mapOf(1984 to -1.0), otherwise = 0.0)
            assertEquals(mapOf(1984 to 1), idx.scoreLinks(listOf("e:$target"), EventQuery(), reportsOnly)["e:$target"]?.byKind)
            assertEquals(listOf(1984), reportsOnly.narrow(EventQuery()).kinds)
            assertEquals(0, LinkWeights(byKind = mapOf(7 to 0.0), otherwise = 0.0).narrow(EventQuery()).limit, "nothing can score: matches nothing")
        }

    @Test
    fun `the linkers filter narrows who counts, and a duplicated tag counts per entry`() =
        runBlocking {
            val idx = InMemoryEventIndex()
            idx.putAll(
                listOf(
                    doc(7, listOf("e", target), pubkey = alice),
                    doc(7, listOf("e", target), pubkey = bob),
                    // The engine groups array ELEMENTS; the reference states the same answer.
                    doc(1, listOf("e", target), listOf("e", target), pubkey = alice),
                ),
            )
            val count = LinkWeights.COUNT
            assertEquals(mapOf(7 to 1, 1 to 2), idx.scoreLinks(listOf("e:$target"), EventQuery(authors = listOf(alice)), count)["e:$target"]?.byKind)
            assertEquals(mapOf(7 to 2), idx.scoreLinks(listOf("e:$target"), EventQuery(kinds = listOf(7)), count)["e:$target"]?.byKind)
            assertTrue(idx.scoreLinks(listOf("e:$target"), EventQuery(limit = 0), count).isEmpty(), "limit 0 is the matches-nothing sentinel")
            assertTrue(idx.scoreLinks(listOf("not-a-key"), EventQuery(), count).isEmpty())
        }
}
