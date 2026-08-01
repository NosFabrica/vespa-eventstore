/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.vespa.doc
import com.vitorpamplona.quartz.eventstore.vespa.NearText
import com.vitorpamplona.quartz.eventstore.vespa.WHITESPACE

/**
 * The derived, kind-specific search surface of one event: what the store's
 * extractors decompose a searchable event into. All-null means the event is
 * invisible to NIP-50 search.
 *
 * There are two groups, LARGELY disjoint per kind, which is what lets the
 * schema's rank profiles compose them as a plain sum (see event.sd):
 *
 *  - the kind-0 profile group ([name]..[website]), with each field's role:
 *    name/displayName primary, nip05/lud16 identity (IDF), about/website
 *    affiliation;
 *  - the generic tiers for every other kind: [primary] (title/subject-like),
 *    [secondary] (summary/hashtag-like), [text] (the body), plus [location]
 *    (place names, filled systemically from any kind's `location` tags).
 *
 * The disjointness is not strict: a kind may also fill a profile ROLE column
 * when it carries that data — an app handler (kind 31990) fills the whole
 * profile group, and a repo/podcast/stream fills [website] for the
 * affiliation-domain treatment. The schema composes the groups with max()/sum,
 * so the overlap stays well-defined.
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
     * Naive recall check for the in-memory reference index, following the
     * word-group YQL's OR shape. ANY query word that substring-matches ANY field
     * recalls the doc; ranking, not recall, decides what floats. Every word
     * counts, exactly as the YQL builder emits every word.
     *
     * Substring is a deliberately LOOSER superset of the real engine's
     * matchers, not a model of them: Vespa matches exact whole tokens, word
     * prefixes (via the *_parts and *_tokens attributes), a length-gated typo
     * budget, and the AND-trigram nets — but NOT arbitrary infix ("dell" does
     * not recall "ODELL" there, while it does here), and conversely fuzzy can
     * recall what substring misses ("Odelll" -> ODELL). NIP-50 search is
     * excluded from strict parity for exactly this reason; this reference
     * answers "could a reasonable engine recall it", and ranking is Vespa's.
     */
    fun matches(term: String): Boolean {
        val values = fields().values
        return term
            .split(WHITESPACE)
            .filter { it.isNotEmpty() }
            .any { word -> values.any { it.contains(word, ignoreCase = true) } }
    }

    /**
     * The near-tier attribute arrays (event.sd: prefix/fuzzy match targets),
     * derived from the searchable text via [NearText] — folded, split at two
     * granularities, merged across the fields nothing downstream needs to
     * tell apart. name+display_name share one pair (a near hit is a near hit
     * whichever carried it); search_primary gets the generic-tier pair;
     * search_secondary gets a TOKENS-only column ("bitco" -> #bitcoin —
     * hashtags and summaries deserve prefix reach, but parts-splitting prose
     * would flood the dictionary for no query shape anyone types).
     */
    fun nearFields(): Map<String, List<String>> =
        buildMap {
            val names = listOfNotNull(name, displayName)
            if (names.isNotEmpty()) {
                put("name_parts", NearText.merge(*names.map(NearText::parts).toTypedArray()))
                put("name_tokens", NearText.merge(*names.map(NearText::tokens).toTypedArray()))
            }
            primary?.let {
                put("search_primary_parts", NearText.parts(it))
                put("search_primary_tokens", NearText.tokens(it))
            }
            secondary?.let { put("search_secondary_tokens", NearText.tokens(it)) }
        }

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
         * Rebuild from a doc field map (the [fields] shape). Empty strings
         * normalize back to null: [fields] feeds "" for an absent name/
         * display_name sibling (see there), and real Vespa omits empty-string
         * fields from summaries anyway — so "" and absent are the same state,
         * and folding them keeps round-trips lossless.
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
