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
     * counts, exactly as the YQL builder now emits every word. Fuzzy/gram recall
     * is deliberately not modeled.
     */
    fun matches(term: String): Boolean {
        val values = fields().values
        return term
            .split(WHITESPACE)
            .filter { it.isNotEmpty() }
            .any { word -> values.any { it.contains(word, ignoreCase = true) } }
    }

    companion object {
        val NONE = SearchFields()

        /** Rebuild from a doc field map (the [fields] shape). */
        fun fromFields(get: (String) -> String?): SearchFields =
            SearchFields(
                name = get("name"),
                displayName = get("display_name"),
                about = get("about"),
                nip05 = get("nip05"),
                lud16 = get("lud16"),
                website = get("website"),
                primary = get("search_primary"),
                secondary = get("search_secondary"),
                text = get("search_text"),
                location = get("search_location"),
            )
    }
}
