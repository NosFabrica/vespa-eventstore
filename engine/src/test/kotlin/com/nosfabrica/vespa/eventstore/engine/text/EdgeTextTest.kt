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
package com.nosfabrica.vespa.eventstore.engine.text

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class EdgeTextTest {
    @Test
    fun `every word contributes each prefix from four letters up, folded and distinct`() {
        assertEquals(
            listOf("tara", "taran", "tarant", "tarante", "tarantel", "tarantell", "tarantella", "tarantellas", "laza", "lazar", "lazaro"),
            EdgeText.prefixes("Tarantellas! Lázaro, tarantella? sí"),
        )
    }

    @Test
    fun `words split where the tokenizer splits, so an element is always one token`() {
        // "seed-phrase" is two words to Vespa; a fed "seed-p" would tokenize into
        // "seed" + "p" and match nothing it means.
        assertEquals(listOf("seed", "phra", "phras", "phrase"), EdgeText.prefixes("seed-phrase"))
    }

    @Test
    fun `a Latin word glued to Japanese is still indexed, and camel case adds its parts`() {
        assertEquals(listOf("nost", "nostr"), EdgeText.prefixes("Nostrを使って"))
        val tags = EdgeText.prefixes("#AskNostr #70sMusic")
        assertTrue("nostr" in tags && "music" in tags && "askn" in tags, tags.toString())
        assertTrue(EdgeText.prefixes("asknostr").none { it == "nost" }, "a lowercase compound word has no boundary to split on")
    }

    @Test
    fun `a hashtag is indexed by every substring, so a word at its end still finds it`() {
        val tag = EdgeText.prefixes("gm #asknostr")
        assertTrue(listOf("nost", "nostr", "askn", "skno").all { it in tag }, tag.toString())
        assertTrue("photography" in EdgeText.prefixes("#astrophotography"))
        // Only the token right after the `#`: the next word is an ordinary word again.
        assertTrue(EdgeText.prefixes("#gm asknostr").none { it == "nostr" })
        // A blob past MAX_HASHTAG keeps its prefixes and nothing else.
        val blob = "#" + "ab12".repeat(11)
        assertTrue(EdgeText.prefixes(blob).all { blob.substring(1).startsWith(it) })
    }

    @Test
    fun `prefixes stop at the cap, and a longer query word is truncated to meet them`() {
        val word = "pneumonoultramicroscopicsilicovolcanoconiosis"
        val fed = EdgeText.prefixes(word)
        assertEquals(EdgeText.MAX_PREFIX - EdgeText.MIN_PREFIX + 1, fed.size)
        assertEquals(fed.last(), EdgeText.queryTerm(word))
    }

    @Test
    fun `unspaced scripts and short words are left to the other nets`() {
        assertTrue(EdgeText.prefixes("比特币是一种数字货币 ok").isEmpty())
        assertNull(EdgeText.queryTerm("比特币是一种"))
        assertNull(EdgeText.queryTerm("bit"))
        assertNull(EdgeText.queryTerm("seed-phrase"), "inner punctuation is two tokens: the phrase net's case")
        assertEquals("bitc", EdgeText.queryTerm("Bitc"))
        assertEquals("lazar", EdgeText.queryTerm("Lázar"))
    }
}
