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
package com.nosfabrica.vespa.eventstore.benchmark

import com.nosfabrica.vespa.eventstore.engine.NearText
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * What the parts+tokens column merge actually saves, measured on real
 * derivations rather than argued (issue #69 / docs/attribute-memory.md).
 *
 * The merge's CORRECTNESS is pinned in `NearTextTest` — the merged column is
 * exactly the union, so no prefix or fuzzy reach is lost. This is the other
 * half: how many multivalue elements the union removes, which is what the enum
 * store, the multivalue mapping and the posting lists are all proportional to.
 * The two document vectors it drops (4.8 B/doc each, on EVERY document) are
 * certain and are not measured here.
 *
 * Reported per shape class, not blended: the answer depends entirely on how
 * many names in a corpus are one token (where the two granularities derive the
 * IDENTICAL list, so the merge halves the elements) versus multi-word (where
 * they overlap only partly). Any single number would be a claim about the
 * corpus, not about the change.
 */
class NearMergeSizingTest {
    private class Tally(
        val label: String,
    ) {
        var docs = 0
        var pairElements = 0
        var mergedElements = 0

        fun add(
            parts: List<String>,
            tokens: List<String>,
        ) {
            docs++
            pairElements += parts.size + tokens.size
            mergedElements += NearText.mergeNear(parts, tokens).size
        }

        fun report(): String {
            val saved = pairElements - mergedElements
            val pct = if (pairElements == 0) 0.0 else 100.0 * saved / pairElements
            return "%-34s docs=%-7d pair=%-8d merged=%-8d saved=%-7d (%.1f%%)".format(
                label,
                docs,
                pairElements,
                mergedElements,
                saved,
                pct,
            )
        }
    }

    /**
     * Real Nostr display-name shapes. The synthetic corpus below mints
     * one-token names only (`VOCAB[i] + rnd`), which is the merge's best case,
     * so this mix is what keeps the measurement honest — it is deliberately
     * weighted AWAY from the single-token shape.
     */
    private val nameShapes =
        listOf(
            "odell",
            "walker",
            "jack",
            "gigi", // one token
            "Vitor Pamplona",
            "Jon Gordon",
            "Avi Burra",
            "Lyn Alden", // two words
            "VitorPamplona",
            "BitcoinMemeTreasury",
            "CoffeeLover", // camelCase compounds
            "Vitor-Pamplona",
            "citadel_dispatch",
            "ODELL⚡", // separators / decoration
            "José Silva",
            "中村太郎",
            "Ode Fan Club", // diacritics, CJK, three words
        )

    @Test
    fun `report the element saving of merging parts and tokens`() {
        val shapes = Tally("real name shapes (weighted mix)")
        for (s in nameShapes) shapes.add(NearText.parts(s), NearText.tokens(s))

        // Split on whether the two granularities DERIVE the same list, which is
        // what decides the saving — not on word count: a camelCase compound is
        // one whitespace token but several parts.
        val identical = Tally("  of which parts == tokens")
        val differing = Tally("  of which parts != tokens")
        for (s in nameShapes) {
            val parts = NearText.parts(s)
            val tokens = NearText.tokens(s)
            (if (parts == tokens) identical else differing).add(parts, tokens)
        }

        // The repo's own corpus, through the real per-kind extraction path —
        // whatever `name` and `search_primary` each kind actually contributes.
        val corpusNames = Tally("NostrCorpus name column (kind 0)")
        val corpusPrimary = Tally("NostrCorpus search_primary column")
        for (event in NostrCorpus.generate(NostrCorpus.Config(size = 20_000))) {
            val f = SearchExtractors.extract(event)
            val names = listOfNotNull(f.name, f.displayName)
            if (names.isNotEmpty()) {
                corpusNames.add(
                    NearText.merge(*names.map(NearText::parts).toTypedArray()),
                    NearText.merge(*names.map(NearText::tokens).toTypedArray()),
                )
            }
            f.primary?.let { corpusPrimary.add(NearText.parts(it), NearText.tokens(it)) }
        }

        println("\n--- near-column merge: multivalue elements before/after ---")
        for (t in listOf(shapes, identical, differing, corpusNames, corpusPrimary)) println(t.report())
        println("(document vectors dropped by the merge are not in these numbers: 2 x 4.8 B/doc, on every doc)\n")

        // The merge can never ADD elements — the one property a sizing claim
        // must not get wrong.
        for (t in listOf(shapes, corpusNames, corpusPrimary)) {
            assertTrue(t.mergedElements <= t.pairElements, t.report())
        }
        // Where the two granularities derive the identical list — the common
        // one-word profile name — the merged column is exactly half the pair.
        assertTrue(identical.docs > 0 && identical.mergedElements * 2 == identical.pairElements, identical.report())
    }
}
