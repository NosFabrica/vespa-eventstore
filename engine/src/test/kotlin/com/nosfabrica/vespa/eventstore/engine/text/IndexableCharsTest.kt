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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * What the engine indexes and how a query must be cleaned to meet it — every
 * expectation here was MEASURED on Vespa 8.763 (2026-10-06), and
 * SearchExactTextIT holds the same cases against a real engine.
 */
class IndexableCharsTest {
    @Test
    fun `letters, digits and emoji are indexable, other symbols are not`() {
        listOf("a", "日", "7", "⚡", "🔥", "©", "🇧").forEach { assertTrue(IndexableChars.hasIndexable(it), it) }
        listOf("₿", "$", "∞", "+", "^", "🏽", "①", "#", "—", "️").forEach { assertFalse(IndexableChars.hasIndexable(it), it) }
    }

    @Test
    fun `letters are found by code point, so mathematical styles are letters`() {
        assertTrue(IndexableChars.hasLetterOrDigit("𝐧𝐨𝐬𝐭𝐫"))
        assertFalse(IndexableChars.hasLetterOrDigit("🔥🔥"))
    }

    @Test
    fun `query text - syntax becomes a space, lone selectors and skin tones go, glued selectors stay`() {
        assertEquals("bitcoin ", IndexableChars.queryText("bitcoin*"))
        assertEquals("f ck", IndexableChars.queryText("f*ck"), "the document indexed f and ck")
        assertEquals("x no-no  ha-ha ", IndexableChars.queryText("x\"no-no “ha-ha”"))
        assertEquals("❤ love", IndexableChars.queryText("❤️ love"))
        assertEquals("❤❤", IndexableChars.queryText("❤️❤️"))
        assertEquals("❤", IndexableChars.queryText("❤︎"), "the text-style selector too")
        assertEquals("👍 nice", IndexableChars.queryText("👍🏽 nice"))
        // Glued to a digit or letter, the selector is part of the indexed word.
        assertEquals("1️⃣", IndexableChars.queryText("1️⃣"))
        assertEquals("⚡️zap", IndexableChars.queryText("⚡️zap"))
        val plain = "plain words"
        assertTrue(plain === IndexableChars.queryText(plain), "nothing to clean: the same string")
    }

    @Test
    fun `query text - the default grammar's operators become spaces`() {
        assertEquals("kind 1", IndexableChars.queryText("kind:1"), "a field selector")
        assertEquals("created_at  0", IndexableChars.queryText("created_at:>0"), "a range")
        assertEquals(" bar", IndexableChars.queryText("-bar"), "a NOT")
        assertEquals(" bar", IndexableChars.queryText("+bar"))
        assertEquals("foo  bar", IndexableChars.queryText("foo*-bar"), "a sign after a star made a space")
        assertEquals("#  bar", IndexableChars.queryText("#(-bar"), "a sign behind any non-word lead is still a NOT")
        assertEquals("bitcoin 50", IndexableChars.queryText("bitcoin!50"), "a term weight")
        assertEquals("a b c d ", IndexableChars.queryText("a;b<c[d]"), "split the word into OR'd terms")
        assertEquals(" bar kind 1 wow ", IndexableChars.queryText("－bar kind：1 wow！"), "the full-width forms Vespa reads as the ASCII ones")
        assertEquals("pico-8 e+1", IndexableChars.queryText("pico-8 e+1"), "a sign inside a word is not an operator")
    }

    @Test
    fun `query text - a lone surrogate separates, a pair is one character`() {
        assertEquals("a b", IndexableChars.queryText("a\uD83Db"), "no document holds one, and the wire would send it as ?")
        assertEquals(" zap ", IndexableChars.queryText("\uDD25zap\uD83D"))
        assertEquals("a\uD83D\uDD25b", IndexableChars.queryText("a\uD83D\uDD25b"))
        assertEquals("a b", IndexableChars.exclusionText("a\uD83Db"), "nor in an exclusion's phrase")
    }

    @Test
    fun `search words are capped in number, in tokens and in length, keeping the first`() {
        val many = (1..40).joinToString(" ") { "w$it" }
        assertEquals((1..IndexableChars.MAX_SEARCH_WORDS).map { "w$it" }, IndexableChars.searchWords(many))
        // Vacuous words cost nothing: they are dropped before the caps count.
        assertEquals(listOf("a", "b"), IndexableChars.searchWords("# a ₿ b"))
        // Four tokens a word: the budget holds eight of them, and the rest are dropped.
        assertEquals(List(8) { "a.b.c.d" }, IndexableChars.searchWords(List(12) { "a.b.c.d" }.joinToString(" ")))
        // The word that crosses the budget is cut at a token, never inside one.
        val dense = (1..40).joinToString(".") { "t$it" }
        assertEquals((1..IndexableChars.MAX_SEARCH_TOKENS).joinToString(".") { "t$it" } + ".", IndexableChars.searchWords(dense).single())
        assertEquals(listOf("one", "zap⚡"), IndexableChars.searchWords("one zap⚡ " + (1..40).joinToString(" ") { "x$it" }).take(2))
        val long = "z".repeat(10_000)
        assertEquals(IndexableChars.MAX_WORD_CHARS, IndexableChars.searchWords(long).single().length)
    }

    @Test
    fun `an exclusion drops a lone selector only when one token is left`() {
        assertEquals("❤", IndexableChars.exclusionText("❤️"), "-❤️ drops every heart")
        assertEquals("👍", IndexableChars.exclusionText("👍🏽"))
        assertEquals("❤️❤️", IndexableChars.exclusionText("❤️❤️"), "the document indexed the selectors between the hearts")
        assertEquals("f*ck", IndexableChars.exclusionText("f*ck"), "phrase grammar reads the star as a separator, as the document did")
    }

    @Test
    fun `tokens - letter and digit runs, each emoji its own`() {
        assertEquals(listOf("zap", "⚡", "sent"), IndexableChars.tokens("zap⚡ sent"))
        assertEquals(listOf("🔥", "🔥"), IndexableChars.tokens("🔥🔥"))
        assertEquals(listOf("👍", "nice"), IndexableChars.tokens("👍🏽 nice"), "a skin tone separates, as the engine drops it")
        assertEquals(listOf("❤", "love"), IndexableChars.tokens("❤️ love"))
    }
}
