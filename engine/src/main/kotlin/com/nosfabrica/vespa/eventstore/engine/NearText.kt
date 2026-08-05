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

import java.text.Normalizer

/**
 * Feed-side derivation of the near-match attribute fields (*_parts /
 * *_tokens in event.sd) and the matching query-side fold. Computed in the
 * feed, not a schema-side indexing expression: string ATTRIBUTES match raw
 * bytes, so doc and query side must share ONE fold ([fold]: NFKD, strip
 * combining marks, lowercase) or "jose" could never prefix-match "josé";
 * the schema-side merge also nulls out on missing inputs, and these
 * tokenizations don't fit the indexing language. The cost: populating an
 * existing corpus needs a RE-FEED, not a native reindex —
 * NostrSemanticsStore.reindexFullTextSearch re-puts exactly the drifted docs.
 *
 * Keep in lockstep with FuzzyWordGroup (same [fold] on query words) and
 * event.sd's field comments. All outputs are lowercase, folded, distinct,
 * and length-capped — attribute dictionaries index every element.
 */
object NearText {
    /** Elements longer than this are dropped (a "name" that long is data noise, not a name). */
    const val MAX_ELEMENT_LEN = 64

    /** Cap on emitted elements per source string — bounds adversarial names. */
    const val MAX_ELEMENTS = 48

    /** Longest CJK run that gets suffix expansion (runs are names; longer is prose). */
    const val MAX_CJK_SUFFIX_RUN = 8

    /**
     * The shared normalization: NFKD-decompose, drop combining marks,
     * lowercase. "José" -> "jose"; full-width forms fold to ASCII; CJK is
     * untouched.
     */
    fun fold(s: String): String =
        Normalizer
            .normalize(s, Normalizer.Form.NFKD)
            .filterNot { Character.getType(it) == Character.NON_SPACING_MARK.toInt() }
            .lowercase()

    /**
     * The *_parts granularity: every word START becomes an element — split at
     * camelCase transitions ("BitcoinMemeTreasury" -> [bitcoin, meme,
     * treasury]) and non-letter/digit runs; CJK runs also emit their suffixes
     * so a given-name query can reach an unsegmented CJK full name. Folded
     * ([fold]) AFTER splitting — the camel rule needs the original case.
     */
    fun parts(s: String): List<String> = cap(splitCamelAndSeparators(s).asSequence().flatMap { withCjkSuffixes(it) })

    /**
     * The *_tokens granularity: whole whitespace-delimited tokens kept intact
     * ("vitorp" -> "vitorpamplona"), plus an alnum-only variant when
     * separators decorate a token ("vitor-pamplona"), plus the whole-name
     * concatenation for multi-token strings — the doc-side mirror of the
     * query builder's joined variant. CJK tokens ride the same suffix
     * expansion as [parts].
     */
    fun tokens(s: String): List<String> {
        val raw = s.split(WHITESPACE).filter { it.isNotEmpty() }
        return cap(
            sequence {
                for (t in raw) {
                    yield(t)
                    alnumOnly(t)?.let { yield(it) }
                    yieldAll(withCjkSuffixes(t).drop(1))
                }
                // The whole-name concatenation trails the tokens, so on a long
                // field [cap] has already filled up and this is never reached —
                // which is the point of building it lazily: joining a
                // thousand-word description to then discard it was pure waste.
                if (raw.size >= 2) {
                    raw
                        .joinToString("")
                        .filter(Char::isLetterOrDigit)
                        .takeIf { it.isNotEmpty() }
                        ?.let { yield(it) }
                }
            },
        )
    }

    /** Merge one derived field from several source strings, preserving order, dropping duplicates and over-long elements. */
    fun merge(vararg lists: List<String>): List<String> = cap(lists.asSequence().flatMap { it })

    /** Query-side: the folded form a near clause should carry. */
    fun foldWord(word: String): String = fold(word)

    /**
     * Fold, drop what no dictionary should hold, de-duplicate, and STOP at
     * [MAX_ELEMENTS] — the output is bounded, so the work is too.
     *
     * Takes a Sequence, not a List, deliberately. These run on the feed path
     * for every doc, and the sources are not short: `search_secondary` carries
     * summaries, descriptions and rule text, and the expansions multiply it
     * (a token yields its alnum variant, a CJK run yields up to
     * [MAX_CJK_SUFFIX_RUN] suffixes). Materializing all of that to keep 48
     * elements meant O(field) allocation — and in [tokens], concatenating the
     * entire field — for a result that was decided by its first few dozen
     * words. Laziness makes the cost O(MAX_ELEMENTS) instead, with byte-for-
     * byte the same output: fold-then-filter-then-distinct in the same order,
     * cut at the same point.
     *
     * [fold] is idempotent (NFKD of decomposed text is itself; lowercase
     * likewise), so [merge] re-folding already-folded inputs is a no-op.
     */
    private fun cap(elements: Sequence<String>): List<String> {
        val out = LinkedHashSet<String>()
        for (raw in elements) {
            val e = fold(raw)
            if (e.isEmpty() || e.length > MAX_ELEMENT_LEN) continue
            out += e
            if (out.size == MAX_ELEMENTS) break
        }
        return out.toList()
    }

    /** "vitor-pamplona" -> "vitorpamplona"; null when stripping changes nothing or empties it. */
    private fun alnumOnly(s: String): String? =
        s
            .filter(Char::isLetterOrDigit)
            .takeIf { it.isNotEmpty() && it != s }

    /**
     * Split at non-letter/digit runs, lower/digit->Upper transitions, and
     * before the last capital of an ALLCAPS->Capitalized boundary
     * ("HTTPServer" -> [HTTP, Server]).
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
     * way "太郎" reaches "中村太郎". Non-CJK parts pass through alone.
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
