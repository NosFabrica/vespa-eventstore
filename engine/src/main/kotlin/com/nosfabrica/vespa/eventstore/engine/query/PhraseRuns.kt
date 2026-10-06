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

import com.nosfabrica.vespa.eventstore.engine.text.NearText

/**
 * VESPA REFUSES A PHRASE THAT REPEATS ONE TERM MORE THAN [MAX_RUN] TIMES IN A
 * ROW — and refuses the whole QUERY for it: HTTP 400, "More than 5 occurrences
 * of term 'eee' in a row detected in phrase", so one such phrase anywhere in a
 * REQ fails the REQ rather than the clause.
 *
 * Users type these. MEASURED on Vespa 8 against a real corpus (2026-10-06),
 * three ways in, all through a phrase item:
 *  - a word with a run of 8+ identical characters ("noooooooo", and a real
 *    kind-31890 title's "feeeeeeeeed"): its body gram net is a phrase of
 *    trigrams, and 8 o's is six "ooo" in a row (7 is five, and passes);
 *  - a quoted phrase repeating a word ("no no no no no no");
 *  - an exclusion that tokenizes into one ("-no-no-no-no-no-no": the
 *    negation is a phrase-grammar term, see EventYql).
 * A hyphen-joined POSITIVE word passes — its exact clause is not phrase
 * grammar — and so does a run that is not of one term ("hahahahahaha").
 */
internal object PhraseRuns {
    /** The longest run of one term Vespa accepts inside a phrase. */
    const val MAX_RUN = 5

    /** True when [terms] repeat one term more than [MAX_RUN] times in a row. */
    fun exceeds(terms: List<String>): Boolean {
        var run = 0
        for (i in terms.indices) {
            run = if (i > 0 && terms[i] == terms[i - 1]) run + 1 else 1
            if (run > MAX_RUN) return true
        }
        return false
    }

    /**
     * [text] with every run of one token beyond [MAX_RUN] cut back to
     * [MAX_RUN] — for a phrase-grammar `userInput`, whose tokens Vespa derives
     * itself.
     *
     * A SUPERSET of what was typed, deliberately, and the only honest
     * direction: Vespa cannot express six "no"s in a row, so the choice is
     * between five and an error that fails the REQ. A required phrase cut to
     * five still demands everything the user typed but the excess repetitions
     * (every document reading six reads five); an exclusion cut to five
     * excludes a little more. Neither is a word the user did not type.
     *
     * Tokens are letter/digit runs compared after [NearText.foldAccents] — the
     * approximation of Vespa's tokenizer this needs: a run that tokenizer
     * splits differently is at worst left uncut, which is today's behavior.
     * Text with no over-long run is returned as ITSELF.
     */
    fun cap(text: String): String {
        val tokens = TOKEN.findAll(text).toList()
        if (!exceeds(tokens.map { NearText.foldAccents(it.value) })) return text
        val out = StringBuilder(text.length)
        var copied = 0
        var run = 0
        for (i in tokens.indices) {
            val same = i > 0 && NearText.foldAccents(tokens[i].value) == NearText.foldAccents(tokens[i - 1].value)
            run = if (same) run + 1 else 1
            if (run > MAX_RUN) {
                // Drop this token AND the separator before it, so "no no no"
                // shortens to "no no", never to "no no ".
                out.append(text, copied, tokens[i - 1].range.last + 1)
                copied = tokens[i].range.last + 1
            }
        }
        out.append(text, copied, text.length)
        return out.toString()
    }

    private val TOKEN = Regex("[\\p{L}\\p{N}]+")
}
