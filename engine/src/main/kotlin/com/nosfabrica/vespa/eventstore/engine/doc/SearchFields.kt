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
package com.nosfabrica.vespa.eventstore.engine.doc
import com.nosfabrica.vespa.eventstore.engine.NearText
import com.nosfabrica.vespa.eventstore.engine.WHITESPACE

/**
 * The derived, kind-specific search surface of one event; all-null = invisible
 * to NIP-50 search. Two groups, LARGELY disjoint per kind — which lets the
 * schema's rank profiles compose them as a plain sum (event.sd): the kind-0
 * profile group ([name]..[website]: name/displayName primary, nip05/lud16
 * identity, about/website affiliation) and the generic tiers ([primary],
 * [secondary], [text], plus [location] from any kind's `location` tags).
 * Disjointness is not strict — e.g. kind 31990 fills the whole profile group —
 * and the schema's max()/sum keeps the overlap well-defined.
 */
data class SearchFields(
    val name: String? = null,
    val displayName: String? = null,
    val about: String? = null,
    val nip05: String? = null,
    val lud16: String? = null,
    val website: String? = null,
    val primary: String? = null,
    val secondary: String? = null,
    val text: String? = null,
    val location: String? = null,
) {
    /** Schema field name -> value, for the doc field map. Nulls are omitted. */
    fun fields(): Map<String, String> =
        buildMap {
            name?.let { put("name", it) }
            displayName?.let { put("display_name", it) }
            about?.let { put("about", it) }
            nip05?.let { put("nip05", it) }
            lud16?.let { put("lud16", it) }
            website?.let { put("website", it) }
            primary?.let { put("search_primary", it) }
            secondary?.let { put("search_secondary", it) }
            text?.let { put("search_text", it) }
            location?.let { put("search_location", it) }
        }

    /**
     * Naive recall check for the in-memory reference, following the word-group
     * YQL's AND shape: EVERY query word must substring-match SOME field (words
     * may land in different fields; ranking decides what floats). Per word,
     * substring is a deliberately LOOSER superset of the engine's matchers
     * (whole tokens, prefixes, typo budget, trigram nets — but not arbitrary
     * infix) — the reason NIP-50 search is excluded from strict parity.
     *
     * ONE ENGINE MATCH ESCAPES THE SUPERSET, so "looser" is a near-truth rather
     * than an invariant. `search_text_gram` is trigram-indexed WITHIN each token
     * but positioned continuously across them, so a trigram phrase can span a
     * word boundary when the query's grams partition exactly there: Vespa
     * matches "estin" against "test sting" (est | sti,tin), which is not a
     * substring of it and which this method answers false for. Verified on
     * Vespa 8 (2026-08-15). It stays a documented divergence rather than
     * something modelled here — reproducing it would mean reproducing gram
     * positions, and NIP-50 recall is already outside strict parity for the
     * same class of reason. SearchBodyGramIT is where the engine's real
     * behaviour is pinned. One
     * place looseness must NOT apply: a word with no letter/digit ("⚡") is
     * erased by engine tokenization and EventYql drops it too — requiring it
     * here would split reference and engine on whole-query recall.
     */
    fun matches(term: String): Boolean {
        val words =
            term
                .split(WHITESPACE)
                .filter { it.isNotEmpty() }
                .filter { w -> w.any(Char::isLetterOrDigit) }
        if (words.isEmpty()) return false
        val values = fields().values
        return words.all { word -> values.any { it.contains(word, ignoreCase = true) } }
    }

    /**
     * Exact-adjacency check for the in-memory reference — the engine's
     * phrase-grammar term, both polarities: a REQUIRED quoted phrase
     * ([EventQuery.phrases]) matches iff true, a `-word` exclusion
     * ([EventQuery.notSearch]) drops the doc iff true. True when [phrase]'s
     * folded tokens appear ADJACENTLY, in order, in some field. Unlike [matches]
     * this mirrors ONLY the engine's exact side — whole tokens, folded like the
     * index, no substring reach ("-ode" must NOT drop a doc containing "model").
     * Engine stemming and CJK segmentation still diverge, accepted for the same
     * reason NIP-50 recall is outside strict parity.
     */
    fun containsPhrase(phrase: String): Boolean {
        val wanted = tokensOf(NearText.fold(phrase))
        if (wanted.isEmpty()) return false
        return fields().values.any { value ->
            val have = tokensOf(NearText.fold(value))
            (0..have.size - wanted.size).any { at -> wanted.indices.all { have[at + it] == wanted[it] } }
        }
    }

    /** Maximal letter/digit runs — the reference's stand-in for the engine's tokenizer. */
    private fun tokensOf(s: String): List<String> {
        val out = ArrayList<String>()
        val cur = StringBuilder()
        for (c in s) {
            if (c.isLetterOrDigit()) {
                cur.append(c)
            } else if (cur.isNotEmpty()) {
                out += cur.toString()
                cur.clear()
            }
        }
        if (cur.isNotEmpty()) out += cur.toString()
        return out
    }

    /**
     * The near-tier attribute arrays (event.sd prefix/fuzzy targets), derived
     * via [NearText] — folded, split, merged across fields nothing downstream
     * tells apart. name+display_name share one column; search_primary gets the
     * generic-tier one; search_secondary is TOKENS-only ("bitco" -> #bitcoin;
     * parts-splitting prose would flood the dictionary). affil_tokens (nip05,
     * lud16, website, about) is [NearText.parts]-split so email/URL SEGMENTS
     * become elements, matching what the exact clauses tokenize those fields
     * into: any doc a finished word reaches must stay reachable mid-typing
     * (SearchPrefixLadderIT). Identity fields come FIRST so
     * [NearText.MAX_ELEMENTS] trims bio prose, never identity segments.
     *
     * Each near tier is ONE column rather than a parts+tokens pair: both
     * granularities are still derived and fed, [NearText.mergeNear] just merges
     * them into one attribute (see its KDoc for why that is free).
     */
    fun nearFields(): Map<String, List<String>> =
        buildMap {
            val names = listOfNotNull(name, displayName)
            if (names.isNotEmpty()) {
                put(
                    "name_near",
                    NearText.mergeNear(
                        NearText.merge(*names.map(NearText::parts).toTypedArray()),
                        NearText.merge(*names.map(NearText::tokens).toTypedArray()),
                    ),
                )
            }
            primary?.let {
                put("search_primary_near", NearText.mergeNear(NearText.parts(it), NearText.tokens(it)))
            }
            secondary?.let { put("search_secondary_tokens", NearText.tokens(it)) }
            val affil = listOfNotNull(nip05, lud16, website, about)
            if (affil.isNotEmpty()) {
                put("affil_tokens", NearText.merge(*affil.map(NearText::parts).toTypedArray()))
            }
        }

    /**
     * [nearFields] minus empty arrays — exactly what a put writes, so
     * "stored == this" is the feed-parity test the reindex uses to catch a
     * corpus fed before the near tier existed.
     */
    fun nearFieldsWritten(): Map<String, List<String>> = nearFields().filterValues { it.isNotEmpty() }

    /**
     * "" and absent are the same state: real Vespa omits empty-string fields
     * from summaries while the mock serves them — fold "" back to null so
     * decoded docs compare equal to what was fed either way.
     */
    fun normalized(): SearchFields =
        SearchFields(
            name = name?.takeIf { it.isNotEmpty() },
            displayName = displayName?.takeIf { it.isNotEmpty() },
            about = about?.takeIf { it.isNotEmpty() },
            nip05 = nip05?.takeIf { it.isNotEmpty() },
            lud16 = lud16?.takeIf { it.isNotEmpty() },
            website = website?.takeIf { it.isNotEmpty() },
            primary = primary?.takeIf { it.isNotEmpty() },
            secondary = secondary?.takeIf { it.isNotEmpty() },
            text = text?.takeIf { it.isNotEmpty() },
            location = location?.takeIf { it.isNotEmpty() },
        )

    companion object {
        val NONE = SearchFields()

        /**
         * Rebuild from a doc field map (the [fields] shape). "" normalizes to
         * null — "" and absent are the same state (see [normalized]) — keeping
         * round-trips lossless.
         */
        fun fromFields(get: (String) -> String?): SearchFields {
            fun at(field: String): String? = get(field)?.takeIf { it.isNotEmpty() }
            return SearchFields(
                name = at("name"),
                displayName = at("display_name"),
                about = at("about"),
                nip05 = at("nip05"),
                lud16 = at("lud16"),
                website = at("website"),
                primary = at("search_primary"),
                secondary = at("search_secondary"),
                text = at("search_text"),
                location = at("search_location"),
            )
        }
    }
}
