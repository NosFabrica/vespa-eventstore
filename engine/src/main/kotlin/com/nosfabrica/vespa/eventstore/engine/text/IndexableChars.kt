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

    /**
     * The most words of one search the engine is asked for ([searchWords]); the FIRST ones
     * are kept. Each word is a match group of its own (ten exact clauses, the near terms, five
     * gram nets) and, past two words, an adjacent-pair group too, so the query grew with every
     * word without bound: 10,000 words built 38 MB of YQL and 40,007 parameters, and the relay
     * admits 262,144-character messages. A real NIP-50 search is a handful of words — a name,
     * a topic, a title: the 55 topics of `benchmark/rank_cases.json`, gathered from reported
     * searches, run from one word to four (counted 2026-11-25). 16 is four times the longest
     * of them, and small enough that the worst search the three caps allow still fits Vespa's
     * query-tree limit of 10,000 items (measured on the local Vespa, 2026-11-25: 32 words of
     * 256 random letters built 14,030 items and the engine refused the query with an HTTP 400).
     */
    const val MAX_SEARCH_WORDS = 16

    /**
     * The most Vespa TOKENS one search's words may hold together ([searchWords]); the word
     * that crosses it is cut at a token, and the words after it are dropped. Each exact clause
     * is `userInput()`, which turns a word into one item per token on each of ten fields, and
     * the pair and joined groups repeat every token, so a word like `a.b.c.d…` costs ~40 items
     * per character it holds: [MAX_SEARCH_WORDS] of them would pass the engine's 10,000-item
     * limit on their own. Counted the way [tokens] reads text, which errs HIGH (a combining mark
     * separates there and not in Vespa). A typed word is one token, a pasted URL five or six.
     */
    const val MAX_SEARCH_TOKENS = 32

    /**
     * The most characters of one search word the engine is asked for ([searchWords]): a longer
     * word is cut to its first [MAX_WORD_CHARS], at a code point. Every character of a word
     * costs a copy in each parameter that carries it and, up to the gram nets' own cap, a
     * trigram on each gram field, so one 100,000-character word built 17 MB of YQL. The longest
     * real words are what people paste: a URL, a bech32 `nevent`/`naddr` with relay hints, a
     * 64-character hex id. Past this, a word is data, not something typed to find a note.
     */
    const val MAX_WORD_CHARS = 256

    /**
     * A search's words as the engine is asked for them: [queryText]-cleaned, split on
     * whitespace, each cut to [MAX_WORD_CHARS], the vacuous ones (nothing [hasIndexable])
     * dropped, and of what is left the first [MAX_SEARCH_WORDS] holding at most
     * [MAX_SEARCH_TOKENS] tokens between them. The query compiler and the in-memory reference
     * read the same list, so the two cannot disagree on which words count.
     */
    fun searchWords(search: String): List<String> {
        val out = ArrayList<String>()
        var spent = 0
        for (raw in queryText(search).split(WHITESPACE)) {
            if (out.size == MAX_SEARCH_WORDS || spent == MAX_SEARCH_TOKENS) break
            val word = cutWord(raw)
            if (!hasIndexable(word)) continue
            val left = MAX_SEARCH_TOKENS - spent
            val end = tokenEnd(word, left)
            out += if (end < word.length) word.substring(0, end) else word
            spent += minOf(tokens(word).size, left)
        }
        return out
    }

    /** [word]'s first [MAX_WORD_CHARS] chars, one fewer where the cut would split a surrogate pair. */
    private fun cutWord(word: String): String {
        if (word.length <= MAX_WORD_CHARS) return word
        val end = if (Character.isHighSurrogate(word[MAX_WORD_CHARS - 1])) MAX_WORD_CHARS - 1 else MAX_WORD_CHARS
        return word.substring(0, end)
    }

    /** Where [word]'s token number [max] + 1 starts, by the [tokens] rule; its length when it holds no more than [max]. */
    private fun tokenEnd(
        word: String,
        max: Int,
    ): Int {
        var count = 0
        var inRun = false
        var i = 0
        while (i < word.length) {
            val cp = word.codePointAt(i)
            val letter = Character.isLetterOrDigit(cp)
            val starts = (letter && !inRun) || Character.getType(cp) == Character.OTHER_SYMBOL.toInt()
            if (starts && ++count > max) return i
            inRun = letter
            i += Character.charCount(cp)
        }
        return word.length
    }

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
     *  - A LONE surrogate becomes a space: it has no UTF-8 form, so the wire
     *    sent it as `?` ("a\uD83Db" asked for a and b while the reference
     *    looked for the literal), and no document holds one
     *    (`VespaText.isStorable`). A space is what both ends make of it.
     *  - The rest of the default grammar's OPERATORS become spaces too, since
     *    a word reaches `userInput()` whole and Vespa parses it. MEASURED on
     *    the local Vespa (2026-11-25, 31,654 notes; the grammar is `weakAnd`):
     *    a COLON selects a field or a range — `kind:1` matched every kind 1,
     *    `created_at:>0` every note, and `kind:` was an HTTP 400 (NullItem);
     *    a BANG weights the term (`bitcoin!` is weight 150, `bitcoin!50` 50);
     *    `( ) ; < > [ ]` cut the word into separate OR'd items (`zzqqxx;bitcoin`
     *    matched every bitcoin note, where `zzqqxx.bitcoin` ANDs its two); and
     *    a `-` or `+` that opens a term is a NOT or a MUST — `-bar` matched
     *    every note. A sign opens a term behind ANY run of non-word characters
     *    ("#-bar", "(-bar" are NOTs too), so one is a space unless the character
     *    SENT before it is a letter, digit or mark (an emoji opens a term,
     *    a selector dropped beside it does not count): "pico-8" and "e+1" are in-word, and
     *    the engine ANDs their halves. Vespa reads the full-width forms
     *    (`－ ： ！ （ …`) as the ASCII ones, so they go as well. A space and not
     *    nothing for the same reason as the star: the document tokenized
     *    "kind:1" as kind and 1, and each half is then a word that must land.
     *    Everything else in ASCII parsed as plain text inside one word.
     * None of these is ever an indexed word on its own, so a search made only
     * of them stays "provably no match".
     */
    fun queryText(s: String): String = clean(s, syntax = true)

    private fun clean(
        s: String,
        syntax: Boolean,
    ): String {
        if (s.none { (syntax && (it in WORD_SYNTAX || it in SIGNS)) || it in SELECTORS || Character.isSurrogate(it) }) return s
        val out = StringBuilder(s.length)
        var i = 0
        var prev = -1
        while (i < s.length) {
            val cp = s.codePointAt(i)
            val n = Character.charCount(cp)
            val next = if (i + n < s.length) s.codePointAt(i + n) else -1
            when {
                cp in SURROGATES -> out.append(' ')
                syntax && cp < 0x10000 && cp.toChar() in WORD_SYNTAX -> out.append(' ')
                syntax && cp < 0x10000 && cp.toChar() in SIGNS && opensTerm(out) -> out.append(' ')
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
     * A lone surrogate is a space even then: the wire would send it as `?`.
     */
    fun exclusionText(word: String): String {
        val typed = if (word.any(Character::isSurrogate)) spaceLoneSurrogates(word) else word
        val stripped = clean(typed, syntax = false)
        return if (stripped != typed && tokens(stripped).size == 1) stripped else typed
    }

    /** [s] with every unpaired surrogate a space, the one change [exclusionText] makes to text it otherwise keeps as typed. */
    private fun spaceLoneSurrogates(s: String): String {
        val out = StringBuilder(s.length)
        var i = 0
        while (i < s.length) {
            val cp = s.codePointAt(i)
            if (cp in SURROGATES) out.append(' ') else out.appendCodePoint(cp)
            i += Character.charCount(cp)
        }
        return out.toString()
    }

    /** True when a sign appended to [out] would open a term: nothing, or no letter, digit or mark, stands before it. */
    private fun opensTerm(out: StringBuilder): Boolean = out.isEmpty() || !isWordChar(out.codePointBefore(out.length))

    /** Drop the selector unless a letter or digit (which it would glue to) stands on either side. */
    private fun isLoneSelector(
        prev: Int,
        next: Int,
    ): Boolean = (prev < 0 || !Character.isLetterOrDigit(prev)) && (next < 0 || !isWordChar(next))

    /** What Vespa's tokenizer keeps inside a word: letters, digits and combining marks. */
    private fun isWordChar(cp: Int): Boolean =
        Character.isLetterOrDigit(cp) ||
            Character.getType(cp).let { it == Character.NON_SPACING_MARK.toInt() || it == Character.COMBINING_SPACING_MARK.toInt() || it == Character.ENCLOSING_MARK.toInt() }

    /** Quotes, stars, and the operators of the default grammar ([queryText]), with the full-width forms Vespa reads as them. */
    private const val WORD_SYNTAX =
        "\"\u201C\u201D\u201E\u201F\u2039\u203A\u00AB\u00BB\u301D\u301E\u301F\uFF02*\uFF0A" +
            ":!()[];<>\uFF1A\uFF01\uFF08\uFF09\uFF3B\uFF3D\uFF1B\uFF1C\uFF1E"

    /** NOT and MUST, ASCII and full-width: syntax only where they open a term ([queryText]). */
    private const val SIGNS = "-+\uFF0D\uFF0B"

    private const val SELECTORS = "\uFE0E\uFE0F"

    /** What [String.codePointAt] returns for a surrogate with no partner: a pair decodes above it. */
    private val SURROGATES = 0xD800..0xDFFF

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
