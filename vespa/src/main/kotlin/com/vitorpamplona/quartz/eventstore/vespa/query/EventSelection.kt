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
package com.vitorpamplona.quartz.eventstore.vespa.query

import com.vitorpamplona.quartz.utils.Hex

/**
 * Builds a Vespa *document selection* from an [EventQuery]. A selection is the
 * expression language the document-API visit scans with: a streaming read over
 * the whole corpus, no result cap, no ranking. Only attribute-shaped
 * constraints translate: kinds, authors, owners, since/until, and the
 * not-yet-expired guard (`expires_at` is always materialized, so a plain range
 * works).
 *
 * Returns null when [q] carries anything a selection can't (or shouldn't)
 * express: ids, tags, a search term, a limit, the expiry-sweep bound, or a
 * non-64-hex key. In that case the caller falls back to the ranked `/search/`
 * path, which handles all of those. Keys are validated to 64-hex BEFORE they
 * are quoted into the expression (same injection rule as [EventYql]).
 */
object EventSelection {
    fun build(q: EventQuery): String? {
        if (q.ids.isNotEmpty() || q.tags.isNotEmpty() || q.tagsAll.isNotEmpty()) return null
        if (!q.search.isNullOrBlank() || q.limit != null || q.expiresBefore != null) return null
        val clauses = ArrayList<String>()
        if (q.kinds.isNotEmpty()) clauses += q.kinds.joinToString(" or ", "(", ")") { "event.kind==$it" }
        // Translated, never dropped: this builder's contract is "null for what a
        // selection can't express", and a silently missing exclusion would stream
        // the excluded kinds into whatever walk asked to skip them.
        if (q.notKinds.isNotEmpty()) clauses += q.notKinds.joinToString(" and ", "(", ")") { "event.kind!=$it" }
        if (q.authors.isNotEmpty()) clauses += keyGroup("pubkey", q.authors) ?: return null
        if (q.owners.isNotEmpty()) clauses += keyGroup("owner", q.owners) ?: return null
        q.since?.let { clauses += "event.created_at>=$it" }
        q.until?.let { clauses += "event.created_at<=$it" }
        q.notExpiredAt?.let { clauses += "event.expires_at>$it" }
        return if (clauses.isEmpty()) "true" else clauses.joinToString(" and ")
    }

    /** `(event.field=="k1" or …)`; null if any entry fails 64-hex validation (fall back to search). */
    private fun keyGroup(
        field: String,
        keys: List<String>,
    ): String? {
        val hexes = keys.map { it.lowercase() }
        if (hexes.any { !Hex.isHex64(it) }) return null
        return hexes.distinct().joinToString(" or ", "(", ")") { "event.$field==\"$it\"" }
    }
}
