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

import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The feed-side derivation the near tier matches against. Each case here is a
 * query shape the derived elements must make reachable — the doc side of the
 * regressions in EventYqlTest.
 */
class NearTextTest {
    @Test
    fun `fold strips diacritics, lowercases, and folds compatibility forms`() {
        assertEquals("jose", NearText.fold("José"))
        assertEquals("vitor", NearText.fold("VÍTOR"))
        assertEquals("uber", NearText.fold("Über"))
        // NFKD compatibility: full-width forms fold to ASCII.
        assertEquals("abc", NearText.fold("ＡＢＣ"))
        // CJK is untouched.
        assertEquals("中村", NearText.fold("中村"))
    }

    @Test
    fun `parts split camelCase so word starts inside compounds are reachable`() {
        assertEquals(listOf("bitcoin", "meme", "treasury"), NearText.parts("BitcoinMemeTreasury"))
        assertEquals(listOf("coffee", "lover"), NearText.parts("CoffeeLover"))
        // ALLCAPS -> Capitalized boundary: the acronym stays whole.
        assertEquals(listOf("http", "server"), NearText.parts("HTTPServer"))
        // Separator runs split too, and elements are folded.
        assertEquals(listOf("vitor", "pamplona"), NearText.parts("Vitor-Pamplona"))
        assertEquals(listOf("jose", "silva"), NearText.parts("José⚡Silva"))
    }

    @Test
    fun `tokens keep compounds whole so compound prefixes work`() {
        // "vitorp" must prefix-match a one-token compound name.
        assertTrue("vitorpamplona" in NearText.tokens("VitorPamplona"))
        // ...and a separator-decorated one, via the alnum-stripped variant.
        assertTrue("vitorpamplona" in NearText.tokens("Vitor-Pamplona"))
        assertTrue("vitor-pamplona" in NearText.tokens("Vitor-Pamplona"))
        // ...and a SPACED one, via the whole-name concatenation — the doc-side
        // mirror of the query builder's joined variant.
        assertTrue("vitorpamplona" in NearText.tokens("Vitor Pamplona"))
        assertTrue("vitor" in NearText.tokens("Vitor Pamplona"))
        assertTrue("pamplona" in NearText.tokens("Vitor Pamplona"))
    }

    @Test
    fun `cjk runs emit suffixes so given names inside unsegmented names are reachable`() {
        // "太郎" (given name) must be able to prefix-match "中村太郎".
        val parts = NearText.parts("中村太郎")
        assertTrue("中村太郎" in parts)
        assertTrue("太郎" in parts)
        assertTrue("郎" in parts)
        // Long CJK runs are prose, not names — no suffix explosion.
        val prose = "中".repeat(NearText.MAX_CJK_SUFFIX_RUN + 1)
        assertEquals(listOf(prose), NearText.parts(prose))
        // Mixed-script tokens don't suffix-expand.
        assertEquals(listOf("abc中"), NearText.parts("abc中"))
    }

    @Test
    fun `outputs are distinct, capped, and never carry empties`() {
        val decorated = NearText.tokens("⚡⚡ ⚡")
        assertTrue(decorated.none { it.isEmpty() }, "$decorated")
        val huge = NearText.parts((1..200).joinToString(" ") { "word$it" })
        assertTrue(huge.size <= NearText.MAX_ELEMENTS)
        val long = NearText.tokens("x".repeat(NearText.MAX_ELEMENT_LEN + 1))
        assertFalse(long.any { it.length > NearText.MAX_ELEMENT_LEN })
    }

    /**
     * The premise behind event.sd's secondary_match(): the near ATTRIBUTE is
     * a bounded projection of the field, so a long summary/description keeps
     * every word in the `search_secondary` INDEX and loses its tail here. The
     * rung therefore cannot be read off the attribute alone — RankRegressionIT
     * pins the engine-side half against a real Vespa.
     */
    @Test
    fun `tokens drop the tail of a long field, so the attribute is not a rung oracle`() {
        val secondary = (1..50).joinToString(" ") { "fill%02d".format(it) } + " ai quilombola"
        val tokens = NearText.tokens(secondary)
        assertEquals(NearText.MAX_ELEMENTS, tokens.size)
        assertFalse("quilombola" in tokens, "past the element cap: $tokens")
        assertFalse("ai" in tokens, "past the element cap: $tokens")
    }

    @Test
    fun `merge unions sources in order without duplicates`() {
        assertEquals(
            listOf("vitor", "pamplona", "vitorpamplona"),
            NearText.merge(listOf("vitor", "pamplona"), listOf("pamplona", "vitorpamplona")),
        )
    }

    // ------------------------------------------------------------------
    // The parts+tokens column merge (2026-08-11, issue #69). Two attributes
    // became one, and the whole case for that being free is: the merged column
    // holds EXACTLY the union of what the two held, so no element — hence no
    // prefix and no fuzzy reach — is lost. These pin it against the old
    // contract, which is what `NearText.merge(parts…)` / `merge(tokens…)`
    // spell out below: verbatim what the two columns used to be fed.
    // ------------------------------------------------------------------

    /** Every shape the derivation has a code path for, plus the ones that stress the caps. */
    private val nearCorpus =
        listOf(
            "odell", // single token — parts == tokens, the maximal-overlap case
            "Vitor Pamplona",
            "VitorPamplona",
            "Vitor-Pamplona",
            "BitcoinMemeTreasury",
            "HTTPServer",
            "José⚡Silva",
            "中村太郎",
            "ＡＢＣ Corp",
            "Ode Fan Club",
            "amethyst@vitorpamplona.com",
            "The Rise and Fall of the Lightning Network: a Long-Form Title With Many Words",
            // over both caps: 120 distinct words, each also yielding variants
            (1..120).joinToString(" ") { "word$it-x" },
        )

    @Test
    fun `merging parts and tokens into one column loses no element`() {
        for (source in nearCorpus) {
            val parts = NearText.merge(NearText.parts(source))
            val tokens = NearText.merge(NearText.tokens(source))
            val merged = NearText.mergeNear(parts, tokens)
            // The old pair's reach, exactly — nothing added, nothing dropped.
            assertEquals((parts + tokens).distinct(), merged, source)
            assertTrue(parts.all { it in merged }, "$source: lost a parts element")
            assertTrue(tokens.all { it in merged }, "$source: lost a tokens element")
            // The cap can never bite: two 48-capped lists cannot exceed 96.
            assertTrue(merged.size <= NearText.MAX_MERGED_ELEMENTS, "$source: ${merged.size}")
        }
    }

    @Test
    fun `the merged column is bounded by what the two columns cost, and usually far under`() {
        // The saving is the dedup: the per-document dictionary bound is
        // unchanged (48+48 across two columns -> 96 across one), and the
        // overlap between the granularities is pure profit on top of the two
        // document vectors the merge drops outright (docs/attribute-memory.md).
        for (source in nearCorpus) {
            val parts = NearText.merge(NearText.parts(source))
            val tokens = NearText.merge(NearText.tokens(source))
            assertTrue(NearText.mergeNear(parts, tokens).size <= parts.size + tokens.size, source)
        }
        // A one-token name is the common profile shape AND the extreme of the
        // overlap: both granularities derive the identical single element, so
        // one column holds half of what the pair did.
        val single = NearText.mergeNear(NearText.parts("odell"), NearText.tokens("odell"))
        assertEquals(listOf("odell"), single)
    }

    @Test
    fun `the near columns still carry both granularities after the merge`() {
        // The recall properties the two columns existed for, now asserted on
        // the one that replaced them: word starts inside a compound (parts)
        // and the whole compound plus its joined variant (tokens).
        val name = SearchFields(name = "BitcoinMemeTreasury", displayName = "Vitor Pamplona").nearFields().getValue("name_near")
        assertTrue("meme" in name, "$name") // parts granularity
        assertTrue("bitcoinmemetreasury" in name) // tokens granularity
        assertTrue("vitorpamplona" in name) // tokens' whole-name concatenation
        assertTrue("pamplona" in name) // parts of the display_name sibling

        val primary = SearchFields(primary = "The Bitcoin Standard").nearFields().getValue("search_primary_near")
        assertTrue("bitcoin" in primary, "$primary")
        assertTrue("thebitcoinstandard" in primary)
        // CJK suffix expansion rides the merged column too.
        assertTrue("太郎" in SearchFields(name = "中村太郎").nearFields().getValue("name_near"))
    }

    @Test
    fun `affil_tokens carries identity and affiliation segments, identity first`() {
        // The as-you-type continuity column (SearchFields.nearFields): the
        // segments the exact clauses tokenize nip05/lud16/website/about into
        // must be prefix-reachable, or every doc reached through those fields
        // vanishes while the final word is still being typed.
        val affil =
            SearchFields(
                name = "amethyst",
                nip05 = "amethyst@vitorpamplona.com",
                lud16 = "vitor@vitorpamplona.com",
                website = "https://amethyst.social",
                about = "Nostr Client for Android",
            ).nearFields()
                .getValue("affil_tokens")
        // Email/URL segments become elements — what "vitorpamp" must prefix.
        assertTrue("vitorpamplona" in affil, "$affil")
        assertTrue("vitor" in affil)
        assertTrue("amethyst" in affil)
        assertTrue("social" in affil)
        // about rides too ("github.com/vitorpamplona/amethyst"-style bios are
        // how several of the report's docs match at all)…
        assertTrue("android" in affil)
        // …but LAST, so the element cap trims bio prose, never identity.
        assertTrue(affil.indexOf("vitorpamplona") < affil.indexOf("android"))
        // Absent sources contribute nothing; all-absent emits no column.
        assertFalse("affil_tokens" in SearchFields(name = "plain").nearFields())
    }
}
