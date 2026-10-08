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
 * PROTOTYPE (#161): the body's word-start reach as EDGE N-GRAMS — every word of
 * a body indexed also as each of its prefixes, so a partial word is ONE exact
 * term lookup against `search_text_edge` instead of the trigram PHRASE against
 * `search_text_gram`.
 *
 * Why: the phrase costs what the corpus holds, not what the query finds. Its
 * candidates are every body holding all of the word's trigrams — 1.49M of
 * 445M on production for "tarantella", each position-checked to keep 1,408 —
 * so it grows with the corpus (2 ms at 26k docs, 2.4 s at 445M). A prefix term's
 * posting list holds exactly the bodies with a word starting with it, so its
 * cost grows with the ANSWER. Every one of the 69 documents only the phrase
 * found on production was a word-START match (`tarantellas`, `tarantellaa`,
 * `tarantelladans`): inflections and compounds the exact column misses because
 * stemming is off (event.sd).
 *
 * Why fed rather than a prefix query: `{prefix:true}` needs an ATTRIBUTE in
 * Vespa, and a body attribute models at ~74 GiB of RAM (event.sd,
 * `search_text_gram`). Prefixes fed as ordinary terms of an INDEX field live in
 * the disk index, and the query needs nothing but an exact term.
 *
 * What it gives up: mid-word reach ("rantell" no longer finds "tarantella"),
 * and scripts written without spaces (CJK, Thai, …), where a "word" is a whole
 * run of text — those keep the trigram phrase ([eligible] is false for them).
 *
 * Doc and query sides share ONE fold ([NearText.foldAccents], the same accent
 * fold the phrase uses) and ONE length rule, or "lazar" could never meet "Lázaro".
 */
object EdgeText {
    /** Shortest prefix indexed — the phrase net's floor too (two trigrams = 4 characters). */
    const val MIN_PREFIX = 4

    /**
     * Longest prefix indexed. A query word longer than this is truncated to it
     * ([queryTerm]), so a 30-letter word matches every body word sharing its
     * first [MAX_PREFIX] letters: looser, never lost.
     */
    const val MAX_PREFIX = 20

    /**
     * Every distinct folded prefix of every eligible word in [body], in first-seen
     * order. Words split at anything that is not a letter or digit — the same
     * boundary Vespa's tokenizer draws — so an element is always one token and
     * the index field's own tokenization keeps it whole.
     *
     * Two more boundaries, both measured on 149k real notes (2026-10-08), where
     * the phrase found documents this net did not:
     *  - a SCRIPT change: Japanese glues particles straight onto Latin words
     *    ("Nostrを使って", "BitCoinのウォレット"), and skipping the whole run for
     *    its kana lost the Latin word. The Latin run is indexed; the unspaced run
     *    is left to the phrase.
     *  - CAMEL CASE, as extra words beside the whole one ([NearText]'s own split):
     *    `#AskNostr`, `#70sMusic`, `#LiveMusic` reach "nostr" and "music". A
     *    lowercase compound (`#asknostr`, `astrophotography`) has no boundary to
     *    split on and stays the phrase's — the recall this net gives up.
     *
     * HASHTAGS get every substring, not just prefixes ([MAX_HASHTAG] bounds it):
     * the measured loss that mattered was the lowercase compound hashtag with
     * the word at its END (`#asknostr`, `#grownostr`, `#astrophotography`, 220
     * of 400 sampled "nostr" misses). A hashtag is one short token a note
     * carries a handful of, so its full infix set is a few dozen elements, where
     * doing the same for every body word is the index the phrase already is.
     */
    fun prefixes(body: String): List<String> {
        val out = LinkedHashSet<String>()
        for ((run, hashtag) in spacedRuns(body)) {
            addPrefixes(run, out)
            val parts = NearText.splitCamelAndSeparators(run)
            if (parts.size > 1) for (part in parts) addPrefixes(part, out)
            if (hashtag) addSuffixPrefixes(run, out)
        }
        return out.toList()
    }

    /**
     * Letter/digit runs of [body], broken where the script switches between
     * spaced and unspaced; unspaced runs are dropped. Each run says whether it
     * opens right after a `#` — a hashtag.
     */
    private fun spacedRuns(body: String): List<Pair<String, Boolean>> {
        val runs = ArrayList<Pair<String, Boolean>>()
        val cur = StringBuilder()
        var curUnspaced = false
        var curHashtag = false
        var prev = -1

        fun flush() {
            if (cur.isNotEmpty() && !curUnspaced) runs += cur.toString() to curHashtag
            cur.clear()
        }
        var i = 0
        while (i < body.length) {
            val cp = body.codePointAt(i)
            i += Character.charCount(cp)
            if (!Character.isLetterOrDigit(cp)) {
                flush()
                prev = cp
                continue
            }
            val u = unspaced(cp)
            if (cur.isNotEmpty() && u != curUnspaced) {
                flush()
                prev = -1
            }
            if (cur.isEmpty()) curHashtag = prev == '#'.code
            curUnspaced = u
            cur.appendCodePoint(cp)
            prev = cp
        }
        flush()
        return runs
    }

    /**
     * Longest hashtag whose every substring is indexed. Beyond it a "hashtag" is
     * a pasted blob (a hex key, a URL fragment), and only its prefixes go in.
     */
    const val MAX_HASHTAG = 40

    /** The prefixes of every proper suffix of [token] at least [MIN_PREFIX] long — with [addPrefixes], every substring. */
    private fun addSuffixPrefixes(
        token: String,
        out: MutableSet<String>,
    ) {
        val folded = NearText.foldAccents(token)
        val len = folded.codePointCount(0, folded.length)
        if (len > MAX_HASHTAG) return
        for (start in 1..len - MIN_PREFIX) addPrefixes(folded.substring(folded.offsetByCodePoints(0, start)), out)
    }

    /** The term a query word looks up in `search_text_edge`, or null when the word is not [eligible]. */
    fun queryTerm(word: String): String? {
        if (!eligible(word)) return null
        val folded = NearText.foldAccents(word)
        if (folded.codePointCount(0, folded.length) < MIN_PREFIX) return null
        return truncate(folded)
    }

    /** A single run of letters/digits, in a script that separates words with spaces. */
    fun eligible(word: String): Boolean = word.isNotEmpty() && word.codePoints().allMatch { Character.isLetterOrDigit(it) && !unspaced(it) }

    private fun addPrefixes(
        token: String,
        out: MutableSet<String>,
    ) {
        val folded = truncate(NearText.foldAccents(token))
        val len = folded.codePointCount(0, folded.length)
        for (k in MIN_PREFIX..len) out += folded.substring(0, folded.offsetByCodePoints(0, k))
    }

    private fun truncate(folded: String): String = if (folded.codePointCount(0, folded.length) <= MAX_PREFIX) folded else folded.substring(0, folded.offsetByCodePoints(0, MAX_PREFIX))

    /**
     * Scripts whose space-delimited token is not one word — written without spaces
     * (Han, kana, Thai, …) or with particles agglutinated onto it (Hangul). Those
     * keep the trigram phrase, which needs no word boundary.
     */
    private fun unspaced(cp: Int): Boolean =
        when (Character.UnicodeScript.of(cp)) {
            Character.UnicodeScript.HAN,
            Character.UnicodeScript.HIRAGANA,
            Character.UnicodeScript.KATAKANA,
            Character.UnicodeScript.HANGUL,
            Character.UnicodeScript.THAI,
            Character.UnicodeScript.LAO,
            Character.UnicodeScript.KHMER,
            Character.UnicodeScript.MYANMAR,
            -> true

            else -> false
        }
}
