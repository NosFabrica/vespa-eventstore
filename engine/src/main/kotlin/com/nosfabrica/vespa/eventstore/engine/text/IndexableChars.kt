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
 * emoji variation selector U+FE0F is indexed as a term of its own, which is why
 * queries strip it ([VARIATION_SELECTOR]).
 */
object IndexableChars {
    /** U+FE0F, which asks for an emoji's colour rendering and is indexed as a word of its own. */
    const val VARIATION_SELECTOR = '️'

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

    /** True when [s] holds a letter or digit — the only text the prefix, typo and n-gram matchers can use. */
    fun hasLetterOrDigit(s: String): Boolean = s.any(Char::isLetterOrDigit)

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
