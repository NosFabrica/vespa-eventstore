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

/**
 * WHICH CHARACTERS AN INDEX HOLDS — the rule every "can this word match
 * anything?" decision shares, so the query builder, the in-memory reference,
 * the store's grammar and the live gate cannot drift apart on it.
 *
 * Letters and digits, AND every OTHER_SYMBOL (So) code point: emoji and
 * pictographs (⚡ ☕ 🔥 ❤ © ° ♫ ✓, the regional indicators of a flag). MEASURED
 * on Vespa 8.763 (2026-10-06), feeding one kind-1 note per character class
 * through the store and querying the engine raw: every So code point is
 * indexed as a term of its own and found by `contains`, by the default
 * grammar and by phrase grammar alike. Nothing else that is not a letter or
 * digit is — math (∞ ≠ → +), currency ($ € ₿ ¥), modifiers (^ ¨, a lone skin
 * tone), number forms (Ⅻ ① ½ ²) and punctuation all tokenize to nothing, and
 * a query made only of them is an HTTP 400 ("only resulted in NullItem"), so
 * those must still be dropped before the engine sees them.
 *
 * The rule this replaced — letters and digits only — dated from an older
 * engine and was never measured: it made a search for "⚡" match nothing and
 * silently ignored an exclusion "-🔥" while the notes sat in the index.
 *
 * How Vespa reads emoji, for whoever matches against them: each code point is
 * its own word ("zap⚡" is zap AND ⚡); a ZWJ sequence is its people ANDed, a
 * flag its two letters; a skin tone (Sk) is dropped, so 👍🏽 is 👍; and the
 * emoji variation selector U+FE0F is a combining mark, so it GLUES to a letter
 * or digit beside it ("1️⃣" indexes "1️", "⚡️zap" indexes ⚡ and "️zap") and is
 * a word of its own only between symbols or spaces ("❤️ love") — which is why
 * queries drop it only there ([queryText]).
 */
object IndexableChars {
    /** U+FE0F, which asks for an emoji's colour rendering; standing alone it is indexed as a word of its own. */
    const val VARIATION_SELECTOR = '\uFE0F'

    fun isIndexable(codePoint: Int): Boolean = Character.isLetterOrDigit(codePoint) || Character.getType(codePoint) == Character.OTHER_SYMBOL.toInt()

    /** True when [s] holds a code point some index can match — the "is this word vacuous?" test. */
    fun hasIndexable(s: String): Boolean {
        var i = 0
        while (i < s.length) {
            val cp = s.codePointAt(i)
            if (isIndexable(cp)) return true
            i += Character.charCount(cp)
        }
        return false
    }

    /**
     * True when [s] holds a letter or digit — the only text the prefix, typo and
     * n-gram matchers can use. By CODE POINT: "𝐧𝐨𝐬𝐭𝐫" (mathematical bold, common
     * in Nostr display names) is all surrogate pairs, and NearText folds it to
     * "nostr" for exactly those matchers.
     */
    fun hasLetterOrDigit(s: String): Boolean = s.codePoints().anyMatch(Character::isLetterOrDigit)

    /**
     * Search text as the DEFAULT grammar should see it — typed words, never
     * syntax — split by the caller on whitespace. MEASURED on Vespa 8.763
     * (2026-10-06):
     *  - The 13 QUOTE characters its query tokenizer reads as `"`
     *    (Tokenizer.java: ASCII, curly and low pairs, guillemets, CJK and
     *    full-width forms) and the STAR, ASCII and full-width, become SPACES.
     *    A quote inside a word made Vespa parse the rest as a phrase — a 400
     *    when it repeats (PhraseRuns); a trailing star is prefix syntax, which
     *    an index field answers with an HTTP 400 for the whole query
     *    ("bitcoin*"). A space, not nothing: the document tokenized "f*ck" as
     *    f and ck, and "fck" finds nothing while "f ck" finds it.
     *  - A LONE variation selector (FE0F / FE0E) is dropped: between symbols or
     *    spaces it is indexed as a word of its own, so "❤️" would ask for the
     *    heart AND the selector and miss every plain ❤. One glued to a letter
     *    or digit stays — the document indexed "1️" and "️zap", and stripping
     *    it there loses the very notes the text was copied from.
     *  - Emoji skin-tone modifiers are dropped: the engine ignores them (👍🏽
     *    is 👍), and the in-memory reference matches by substring.
     * None of these is ever an indexed word on its own, so a search made only
     * of them stays "provably no match".
     */
    fun queryText(s: String): String = clean(s, syntax = true)

    private fun clean(
        s: String,
        syntax: Boolean,
    ): String {
        if (s.none { (syntax && it in WORD_SYNTAX) || it in SELECTORS || Character.isSurrogate(it) }) return s
        val out = StringBuilder(s.length)
        var i = 0
        var prev = -1
        while (i < s.length) {
            val cp = s.codePointAt(i)
            val n = Character.charCount(cp)
            val next = if (i + n < s.length) s.codePointAt(i + n) else -1
            when {
                syntax && cp < 0x10000 && cp.toChar() in WORD_SYNTAX -> out.append(' ')
                cp < 0x10000 && cp.toChar() in SELECTORS && isLoneSelector(prev, next) -> Unit
                cp in SKIN_TONES -> Unit
                else -> out.appendCodePoint(cp)
            }
            prev = cp
            i += n
        }
        return out.toString()
    }

    /**
     * An exclusion (`-word`, phrase grammar) as sent: lone selectors and skin
     * tones dropped only when what is left is ONE token — `-❤️` must drop the
     * plain ❤ too — and the text kept as typed otherwise. In a run, the
     * document indexed each selector BETWEEN the symbols ("❤️❤️" is ❤, FE0F,
     * ❤, FE0F), so the stripped phrase "❤❤" could never match the note the
     * user means to exclude. Quotes and stars stay: phrase grammar reads them
     * as the separators the document's tokenizer did ("-f*ck" drops f*ck).
     */
    fun exclusionText(word: String): String {
        val stripped = clean(word, syntax = false)
        return if (stripped != word && tokens(stripped).size == 1) stripped else word
    }

    /** Drop the selector unless a letter or digit (which it would glue to) stands on either side. */
    private fun isLoneSelector(
        prev: Int,
        next: Int,
    ): Boolean = (prev < 0 || !Character.isLetterOrDigit(prev)) && (next < 0 || !isWordChar(next))

    /** What Vespa's tokenizer keeps inside a word: letters, digits and combining marks. */
    private fun isWordChar(cp: Int): Boolean =
        Character.isLetterOrDigit(cp) ||
            Character.getType(cp).let { it == Character.NON_SPACING_MARK.toInt() || it == Character.COMBINING_SPACING_MARK.toInt() || it == Character.ENCLOSING_MARK.toInt() }

    private const val WORD_SYNTAX = "\"\u201C\u201D\u201E\u201F\u2039\u203A\u00AB\u00BB\u301D\u301E\u301F\uFF02*\uFF0A"

    private const val SELECTORS = "\uFE0E\uFE0F"

    private val SKIN_TONES = 0x1F3FB..0x1F3FF

    /**
     * [s] as index tokens, the reference's stand-in for the engine tokenizer:
     * maximal letter/digit runs, and each other-symbol code point a token of
     * its own. Everything else separates, the variation selector included (the
     * engine indexes it as a word, but queries strip it, so the reference
     * never needs to match it).
     */
    fun tokens(s: String): List<String> {
        val out = ArrayList<String>()
        val cur = StringBuilder()
        var i = 0
        while (i < s.length) {
            val cp = s.codePointAt(i)
            when {
                Character.isLetterOrDigit(cp) -> {
                    cur.appendCodePoint(cp)
                }

                Character.getType(cp) == Character.OTHER_SYMBOL.toInt() -> {
                    if (cur.isNotEmpty()) out += cur.toString().also { cur.clear() }
                    out += String(Character.toChars(cp))
                }

                cur.isNotEmpty() -> {
                    out += cur.toString()
                    cur.clear()
                }
            }
            i += Character.charCount(cp)
        }
        if (cur.isNotEmpty()) out += cur.toString()
        return out
    }
}
