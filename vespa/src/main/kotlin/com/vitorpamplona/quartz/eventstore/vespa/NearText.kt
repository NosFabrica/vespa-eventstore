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
package com.vitorpamplona.quartz.eventstore.vespa

import java.text.Normalizer

/**
 * Feed-side derivation of the near-match attribute fields (name_parts /
 * name_tokens / search_primary_parts / search_primary_tokens /
 * search_secondary_tokens in event.sd) and the matching query-side fold.
 *
 * Computed HERE, in the feed, rather than by a schema-side indexing
 * expression, for three reasons:
 *
 *  1. NORMALIZATION SYMMETRY. Vespa's linguistic pipeline folds diacritics on
 *     `index` fields but string ATTRIBUTES match raw bytes (uncased only), so
 *     a schema-side split would leave "jose" unable to prefix-match "josé".
 *     Deriving in Kotlin lets doc side and query side share ONE fold
 *     ([fold]): NFKD, strip combining marks, lowercase.
 *  2. NULL SAFETY. The schema-side form (`input name . " " . input
 *     display_name | split …`) nulls the whole expression when either input
 *     is missing, which forced the feed to write empty-string siblings. Here
 *     the merge is plain Kotlin over whatever fields exist.
 *  3. EXPRESSIVENESS. The tokenizations below (alnum-stripped compound
 *     variants, whole-name concatenation, CJK run suffixes) don't fit the
 *     indexing language's regex `split` at all.
 *
 * The COST is the reindex story: schema-side synthetic fields rebuild from
 * Vespa's own document store via a native reindex; feed-computed fields need
 * a RE-FEED of searchable docs to populate on an existing corpus. This repo
 * owns ingest end-to-end, so a re-feed is available wherever the store runs:
 * NostrSemanticsStore.reindexFullTextSearch detects docs whose stored arrays
 * drift from this derivation and re-puts exactly those.
 *
 * Keep in lockstep with FuzzyWordGroup (which folds query words with the same
 * [fold]) and event.sd's field comments. All outputs are lowercase, folded,
 * distinct, and length-capped — attribute dictionaries index every element.
 */
object NearText {
    /** Elements longer than this are dropped (a "name" that long is data noise, not a name). */
    const val MAX_ELEMENT_LEN = 64

    /** Cap on emitted elements per source string — bounds adversarial names. */
    const val MAX_ELEMENTS = 48

    /** Longest CJK run that gets suffix expansion (runs are names; longer is prose). */
    const val MAX_CJK_SUFFIX_RUN = 8

    /**
     * The shared normalization: NFKD-decompose, drop combining marks, then
     * lowercase. "José" -> "jose", "VÍTOR" -> "vitor", full-width forms fold
     * to ASCII via NFKD's compatibility mapping. CJK is untouched (NFKD is
     * identity there, and case doesn't apply).
     */
    fun fold(s: String): String =
        Normalizer
            .normalize(s, Normalizer.Form.NFKD)
            .filterNot { Character.getType(it) == Character.NON_SPACING_MARK.toInt() }
            .lowercase()

    /**
     * The *_parts granularity: every word START inside the string becomes an
     * element — split at camelCase transitions ("BitcoinMemeTreasury" ->
     * [bitcoin, meme, treasury]), at any non-letter/digit run, and CJK runs
     * additionally emit their suffixes ("中村太郎" -> [中村太郎, 村太郎, 太郎, 郎])
     * so a given-name query can reach an unsegmented CJK full name — there
     * are no spaces to split on, and a prefix anchored at the run start
     * cannot see "太郎" otherwise.
     *
     * This is what makes "meme" find "BitcoinMemeTreasury" and "lover" find
     * "CoffeeLover". Elements are folded ([fold]) after splitting — the camel
     * rule needs the original case.
     */
    fun parts(s: String): List<String> = cap(splitCamelAndSeparators(s).flatMap { withCjkSuffixes(it) }.map(::fold))

    /**
     * The *_tokens granularity: whole whitespace-delimited tokens, kept
     * intact so a compound-name prefix works ("vitorp" -> "vitorpamplona" —
     * the parts split regresses that case). Each token also emits an
     * alnum-only variant when separators decorate it ("vitor-pamplona" ->
     * "vitorpamplona"), and multi-token strings emit the whole-name
     * concatenation ("Vitor Pamplona" -> "vitorpamplona") — the doc-side
     * mirror of the query builder's joined variant, so a single-word
     * compound query prefix-matches a spaced name without spending the typo
     * budget. CJK tokens ride the same suffix expansion as [parts].
     */
    fun tokens(s: String): List<String> {
        val raw = s.split(WHITESPACE).filter { it.isNotEmpty() }
        val out = ArrayList<String>()
        for (t in raw) {
            out += t
            alnumOnly(t)?.let { out += it }
            out += withCjkSuffixes(t).drop(1)
        }
        if (raw.size >= 2) {
            raw
                .joinToString("")
                .filter(Char::isLetterOrDigit)
                .takeIf { it.isNotEmpty() }
                ?.let { out += it }
        }
        return cap(out.map(::fold))
    }

    /**
     * Merge one derived field from several source strings (name +
     * display_name, or every secondary text), preserving order, dropping
     * duplicates and over-long elements.
     */
    fun merge(vararg lists: List<String>): List<String> = cap(lists.asList().flatten())

    /** Query-side: the folded form a near clause should carry. */
    fun foldWord(word: String): String = fold(word)

    private fun cap(elements: List<String>): List<String> =
        elements
            .filter { it.isNotEmpty() && it.length <= MAX_ELEMENT_LEN }
            .distinct()
            .take(MAX_ELEMENTS)

    /** "vitor-pamplona" -> "vitorpamplona"; null when stripping changes nothing or empties it. */
    private fun alnumOnly(s: String): String? =
        s
            .filter(Char::isLetterOrDigit)
            .takeIf { it.isNotEmpty() && it != s }

    /**
     * Split at any non-letter/digit run, at lower/digit->Upper transitions,
     * and before the last capital of an ALLCAPS->Capitalized boundary
     * ("HTTPServer" -> [HTTP, Server]) — the same three boundaries the
     * original schema-side regex drew, in code instead of lookarounds.
     */
    private fun splitCamelAndSeparators(s: String): List<String> {
        val out = ArrayList<String>()
        val cur = StringBuilder()
        for (i in s.indices) {
            val c = s[i]
            if (!c.isLetterOrDigit()) {
                if (cur.isNotEmpty()) out += cur.toString().also { cur.clear() }
                continue
            }
            if (cur.isNotEmpty()) {
                val prev = s[i - 1]
                val camelStart = c.isUpperCase() && (prev.isLowerCase() || prev.isDigit())
                val acronymEnd = c.isUpperCase() && prev.isUpperCase() && i + 1 < s.length && s[i + 1].isLowerCase()
                if (camelStart || acronymEnd) out += cur.toString().also { cur.clear() }
            }
            cur.append(c)
        }
        if (cur.isNotEmpty()) out += cur.toString()
        return out
    }

    /**
     * A part plus, when it is a short CJK run, its proper suffixes — CJK
     * names have no case or separators to split on, so suffixes are the only
     * way "太郎" (the given name) reaches "中村太郎". Non-CJK parts pass
     * through alone.
     */
    private fun withCjkSuffixes(part: String): List<String> {
        if (part.isEmpty() || part.length > MAX_CJK_SUFFIX_RUN || !part.all(::isCjk)) return listOf(part)
        return (0 until part.length).map { part.substring(it) }
    }

    private fun isCjk(c: Char): Boolean =
        when (Character.UnicodeScript.of(c.code)) {
            Character.UnicodeScript.HAN,
            Character.UnicodeScript.HIRAGANA,
            Character.UnicodeScript.KATAKANA,
            Character.UnicodeScript.HANGUL,
            -> true

            else -> false
        }
}
