/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.vespa

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

    @Test
    fun `merge unions sources in order without duplicates`() {
        assertEquals(
            listOf("vitor", "pamplona", "vitorpamplona"),
            NearText.merge(listOf("vitor", "pamplona"), listOf("pamplona", "vitorpamplona")),
        )
    }
}
