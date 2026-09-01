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
package com.nosfabrica.vespa.eventstore.mapping

/**
 * A NIP-30 `:shortcode:` IS ONE TERM — the tokenization both ends of this
 * store agree on, so a badge stays findable as itself and stops being findable
 * as the word inside it.
 *
 * A bridged Mastodon account is called `DotardTed 🇺🇸 :verified:` and carries
 * `["emoji", "verified", "https://…png"]` beside it: the `:verified:` is a
 * badge GLYPH, and the event says so. The tokenizer disagrees — Vespa splits at
 * every non-alphanumeric, `:` included — so the picture indexed as the name
 * token "verified", which put those accounts on the 130 000 name rung for a
 * bare `verified` search: six of the top six hits on staging (2026-09-01), one
 * of them outranking a Trusted List titled exactly what was searched for.
 *
 * The fix is a TOKENIZATION, not a deletion. Deleting the run (the shape this
 * replaced) also deletes the account: an account whose whole name is a badge
 * becomes unfindable, and `:verified:` typed as a query — the one search that
 * really is asking for the badge — finds nothing. Instead both ends rewrite the
 * run to a single synthetic term, [termOf]:
 *
 *  - feed side, [SearchExtractors]: the run leaves the text it decorated (so
 *    `name` stays the name, at its own length and exactness) and the term joins
 *    the SECONDARY tier, the weak 4 000 rung — a badge is worth something and
 *    is not worth a name;
 *  - query side, [toEventQuery]: a word that is ENTIRELY a shortcode gets the
 *    same rewrite, so `:verified:` finds exactly the accounts carrying that
 *    badge, and `verified` finds the ones that say the word.
 *
 * DECLARED ONLY on the feed side, never guessed by pattern — see
 * [SearchExtractors]. The query side has no tags to consult, so it anchors on
 * the WHOLE word instead: `8:30:45` and `1:2:1` are single words that are not
 * a shortcode end to end, and the same clock a regex would have eaten survives
 * as itself. A `:verified:` nobody declared simply matches no document, which
 * is the truth about it.
 *
 * ONE ROUTE REMAINS from the word into the term, and it is bounded: the
 * AND-of-trigrams net over `search_secondary_gram` reaches "verified" INSIDE
 * `xemojiverified`. Under an observer — what a relay always sends — that hit is
 * gram-only, so it scores under `query(text_score_cutoff)` (ceiling
 * `w_secondary` x `gram_cap` = 32 against 100) and the `search` profile's
 * rank-score-drop-limit deletes it outright. Un-lensed, on the pure-text
 * profile, nothing deletes it and it survives as noise under every real match —
 * which is the safety net doing its job, not the 130 000 rung this fixed.
 * BadgeTermIT pins both halves against a real Vespa; the reference engines are
 * looser still (substring) and can pin neither.
 *
 * Derived data on both ends: a change here rolls out with
 * `reindexFullTextSearch` and needs no resync.
 */
internal object Shortcodes {
    /**
     * The synthetic term's prefix. Deliberately not a word: it must survive
     * tokenization as ONE token and must never be typed by accident.
     */
    const val PREFIX = "xemoji"

    /** A word that is one shortcode and nothing else — the only thing the query side rewrites. */
    private val WHOLE_WORD = Regex(":[A-Za-z0-9_-]+:")

    private val WHITESPACE = Regex("\\s+")

    /**
     * The term one declared shortcode indexes as; null when the code carries
     * no alphanumeric at all (`:_:` is a picture with no name, strippable but
     * not searchable).
     *
     * ALPHANUMERIC-FLATTENED, because Vespa's tokenizer splits at `_` and `-`
     * too: `xemoji_official_verified` would index as three tokens, one of them
     * the bare `verified` this exists to remove. `official_verified` and
     * `officialverified` therefore collide into one term — two spellings of one
     * badge, which is the harmless direction to collide in.
     */
    fun termOf(code: String): String? =
        code
            .filter(Char::isLetterOrDigit)
            .lowercase()
            .ifEmpty { null }
            ?.let { PREFIX + it }

    /** The literal run a declared code writes in the text: `:code:`. */
    fun runOf(code: String): String = ":$code:"

    /**
     * The query side of [termOf]: rewrite every whitespace-separated word that
     * is entirely a shortcode, leave everything else byte-identical. Applied to
     * the search terms AND to `-exclusions`, so `-:verified:` excludes badges
     * rather than the word.
     */
    fun rewriteQuery(terms: String?): String? {
        if (terms == null || !terms.contains(':')) return terms
        return terms
            .split(WHITESPACE)
            .filter { it.isNotEmpty() }
            .joinToString(" ") { word -> rewriteWord(word) }
            .takeIf { it.isNotEmpty() }
    }

    /** [rewriteQuery] for one already-split word. */
    fun rewriteWord(word: String): String = if (WHOLE_WORD.matches(word)) termOf(word.substring(1, word.length - 1)) ?: word else word
}
