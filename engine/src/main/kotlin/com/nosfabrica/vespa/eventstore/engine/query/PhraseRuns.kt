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
package com.nosfabrica.vespa.eventstore.engine.query

import com.nosfabrica.vespa.eventstore.engine.text.IndexableChars
import java.text.Normalizer

/**
 * THE TWO PHRASE SHAPES VESPA REFUSES, and the query rewritten so it never
 * sends one — because Vespa refuses the whole QUERY for it (HTTP 400, "Illegal
 * query"), so one such phrase anywhere in a REQ fails the REQ, not the clause.
 *
 * The refusal is the container's `InputCheckingSearcher`, in the default
 * search chain ahead of the content nodes. It walks every phrase item and
 * rejects one repeating a term
 *  - more than [MAX_RUN] times IN A ROW ("More than 5 occurrences of term
 *    'eee' in a row detected in phrase"), or
 *  - more than [MAX_OCCURRENCES] times ANYWHERE in it ("Phrase contains more
 *    than 10 occurrences of term 'hah' in phrase"),
 * comparing terms after the index's normalization. Its source states no reason
 * beyond "heuristics … whether the query should be sent to the search
 * backend"; repeats of one term share one position list, which is what makes
 * a phrase over them expensive to align, and the shape is rare in real input.
 * Kept, deliberately, rather than removed from our chain in services.xml: it
 * is Vespa's cost guard (and its double-encoded-UTF-8 check) on a relay that
 * answers anyone.
 *
 * Users reach both, through phrases they never typed as such. MEASURED on
 * Vespa 8.763 against a real corpus (2026-10-06), for the in-a-row rule:
 *  - a word with a run of 8+ identical characters ("noooooooo", and a real
 *    kind-31890 title's "feeeeeeeeed"): its body gram net is a phrase of
 *    trigrams, and 8 o's is six "ooo" in a row (7 is five, and passes);
 *  - a quoted phrase repeating a word ("no no no no no no");
 *  - an exclusion that tokenizes into one ("-no-no-no-no-no-no": the
 *    negation is a phrase-grammar term, see EventYql).
 * The anywhere rule takes longer input to the same places: a 24-character
 * "hahaha…" is eleven "hah" grams, never two in a row; a quoted sentence says
 * "the" eleven times. A hyphen-joined POSITIVE word passes both — its exact
 * clause is not phrase grammar — UNLESS it carries one of Vespa's quote
 * characters, which turns the rest of it into a phrase ([literalWord]).
 *
 * Public for the store's live gate, which must judge a streamed event by the
 * same rewritten phrases the page was read with.
 */
object PhraseRuns {
    /** The longest run of one term Vespa accepts inside a phrase. */
    const val MAX_RUN = 5

    /** The most occurrences of one term Vespa accepts anywhere in a phrase. */
    const val MAX_OCCURRENCES = 10

    /** True when [terms], as one phrase, passes both of Vespa's checks. */
    fun fits(terms: List<String>): Boolean {
        val seen = HashMap<String, Int>()
        var run = 0
        for (i in terms.indices) {
            run = if (i > 0 && terms[i] == terms[i - 1]) run + 1 else 1
            if (run > MAX_RUN) return false
            if (seen.merge(terms[i], 1, Int::plus)!! > MAX_OCCURRENCES) return false
        }
        return true
    }

    /**
     * [text] as the phrases Vespa will accept — for a phrase-grammar
     * `userInput`, whose tokens Vespa derives itself. A text that already
     * fits comes back as ITSELF, alone, which is nearly every phrase anyone
     * types.
     *
     * Two rewrites, one per rule, both toward a SUPERSET of what was typed,
     * the only honest direction: Vespa cannot express the phrase, so the
     * choice is between an approximation and an error that fails the REQ.
     *  - A run past [MAX_RUN] is CUT to it. Every document reading six "no"s
     *    in a row reads five, so the cut phrase still demands everything typed
     *    but the excess repetitions.
     *  - A term reaching occurrence [MAX_OCCURRENCES] + 1 SPLITS the phrase
     *    there: the next piece starts at it. The caller requires every piece,
     *    which keeps each piece's words adjacent and in order and gives up
     *    only the adjacency ACROSS a split.
     * Neither introduces a word the user did not type. An exclusion built from
     * the pieces (drop a document holding all of them) excludes a little more
     * than the exact phrase would, for the same reason.
     *
     * Tokens approximate Vespa 8's query tokenizer, and the approximation may
     * only ever OVER-count (rewrite a phrase Vespa would have taken), never
     * under-count (send one it refuses): a word is a run of letters, decimal
     * digits AND combining marks — Vespa keeps marks inside a word, so
     * "नमस्ते" is one term, not "नमस" and "त" — and every other-symbol code
     * point is a word of its own, which is how Vespa reads an emoji ("🔥🔥🔥🔥🔥🔥"
     * is six). Terms compare through [fold], which merges at least what
     * Vespa's NFKC + lowercasing merges. The first piece keeps the text before
     * its first token and the last piece the text after the LAST token —
     * dropped ones included, so a cut at the very end stays cut.
     */
    fun pieces(text: String): List<String> {
        val tokens = TOKEN.findAll(text).toList()
        val folded = tokens.map { fold(it.value) }
        if (fits(folded)) return listOf(text)

        // Kept token indices per piece. A run past MAX_RUN drops the token; a
        // term at MAX_OCCURRENCES starts a new piece AT it.
        val groups = ArrayList<ArrayList<Int>>()
        var current = ArrayList<Int>()
        val seen = HashMap<String, Int>()
        var run = 0
        for (i in tokens.indices) {
            run = if (i > 0 && folded[i] == folded[i - 1]) run + 1 else 1
            if (run > MAX_RUN) continue
            if ((seen[folded[i]] ?: 0) >= MAX_OCCURRENCES) {
                groups += current
                current = ArrayList()
                seen.clear()
            }
            seen.merge(folded[i], 1, Int::plus)
            current += i
        }
        groups += current

        return groups.mapIndexed { g, kept ->
            val out = StringBuilder()
            val first = kept.first()
            out.append(text, if (g == 0) 0 else tokens[first].range.first, tokens[first].range.last + 1)
            for (k in 1 until kept.size) {
                // The separator immediately before the token, then the token:
                // a cut run takes ITS separators with it, so "no no no" never
                // shortens to "no no " or "nono".
                val j = kept[k]
                out.append(text, tokens[j - 1].range.last + 1, tokens[j].range.last + 1)
            }
            if (g == groups.lastIndex) out.append(text, tokens.last().range.last + 1, text.length)
            out.toString()
        }
    }

    /**
     * [word] as a plain word for Vespa's DEFAULT grammar — the characters that
     * grammar reads as syntax, which only ever turn a typed word into a query
     * the user did not write, removed:
     *  - its 13 QUOTE characters (Tokenizer.java): ASCII, the curly and low
     *    pairs, the guillemets, the CJK and full-width forms. Upstream lifts
     *    only an ASCII quote standing at a word boundary into a phrase, so
     *    `x"no-no-no-no-no-no` or an iOS keyboard's `“ha-ha-ha-ha-ha-ha”`
     *    reached the word clauses still quoted, and Vespa parsed the quoted
     *    part as a phrase — a 400 when it repeats ([pieces]).
     *  - its STAR, ASCII and full-width: a trailing one is prefix syntax, and
     *    an index field answers prefix with an HTTP 400 ("Prefix matching is
     *    not supported") for the whole query. MEASURED on Vespa 8.763: "bitcoin*"
     *    failed every REQ it was in. Nothing is lost — every word already
     *    gets prefix reach from its near-attribute clauses.
     *  - the emoji variation selector ([IndexableChars.VARIATION_SELECTOR]),
     *    which the engine indexes as a word of its own: left in, "❤️" asks for
     *    the heart AND the selector and misses every plain ❤.
     * None of these is ever an indexed character on its own, so a word made
     * only of them was vacuous before and stays so.
     */
    fun literalWord(word: String): String = if (word.none { it in WORD_SYNTAX }) word else word.filterNot { it in WORD_SYNTAX }

    /**
     * [word] for a phrase-grammar exclusion: the variation selector removed
     * when it is one word (`-❤️` must drop the plain ❤ too), kept inside a
     * multi-word phrase, where removing it would demand an adjacency the
     * document — which indexed the selector between the two — cannot have.
     */
    fun exclusion(word: String): String = if (word.any(Char::isWhitespace)) word else word.filterNot { it == IndexableChars.VARIATION_SELECTOR }

    /** NFKD, marks dropped, lowercased: compatibility forms, accents and case merge, as they do (and more) in Vespa. */
    private fun fold(token: String): String =
        Normalizer
            .normalize(token, Normalizer.Form.NFKD)
            .filterNot { Character.getType(it).let { t -> t == Character.NON_SPACING_MARK.toInt() || t == Character.COMBINING_SPACING_MARK.toInt() || t == Character.ENCLOSING_MARK.toInt() } }
            .lowercase()

    private val TOKEN = Regex("[\\p{L}\\p{Nd}\\p{M}]+|\\p{So}")

    private const val WORD_SYNTAX = "\"\u201C\u201D\u201E\u201F\u2039\u203A\u00AB\u00BB\u301D\u301E\u301F\uFF02*\uFF0A\uFE0F"
}
