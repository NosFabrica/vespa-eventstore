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
package com.vitorpamplona.quartz.eventstore.store.mapping

import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import com.vitorpamplona.quartz.utils.Hex

/*
 * NIP-01/NIP-50 filter -> engine query translation. A pure mapping with no
 * store state: the Quartz REQ Filter (with the NIP-50 sort:/filter:/include:spam
 * extensions) becomes the Nostr-agnostic EventQuery the :vespa module builds YQL
 * from.
 */

/**
 * Maps a Quartz Filter to the engine's plain [EventQuery]. Returns null when the
 * filter can never match. Under NIP-01, a present-but-EMPTY list means "the
 * event's value must be in the list" — of nothing, so it matches nothing. An
 * absent (null) list means no constraint, which is EventQuery's empty default.
 *
 * NIP-50 extensions are relay hints, not text. Quartz's parser splits them off,
 * and unlike a naive key:value regex it keeps `scheme://…` tokens as terms. The
 * extensions this store honors:
 *
 *  - `sort:rank[:desc]` / `sort:rank:asc` / `sort:followers` / `sort:text`
 *    pick the rank profile: trust order, verified-follower-count order, or
 *    pure text — the first two within match tiers. With no terms this is a
 *    match-all in that order.
 *  - `filter:rank:gte:N` / `filter:rank:gt:N` raise the observer trust floor
 *    from [DEFAULT_MIN_RANK] to N without changing the order: whatever profile
 *    the query resolves to keeps its ranking, gated at N. The profile is NOT
 *    chosen here — plain shapes stay ranking-null so the store can pick the
 *    gated-recall profile once the out-of-band observer is known.
 *  - `include:spam` turns OFF the default trust floor. Every observer-lensed
 *    query is otherwise gated at [DEFAULT_MIN_RANK] — searches here, plain
 *    recall by the store's observer gate (NostrSemanticsStore) — and
 *    include:spam is the inverse of both, so it rides along as
 *    [EventQuery.includeSpam].
 *  - `observer:<64-hex>` names the pubkey whose web-of-trust ranks the hits.
 *    It is the query-side way to pick the ranking lens (the relay otherwise
 *    supplies it from the NIP-42 connection); a non-hex value is ignored. With
 *    no observer resolved, a search degrades to pure-text relevance.
 *
 * Unknown extensions are ignored. A query that is nothing but extensions becomes
 * unconstrained (null terms), not match-nothing.
 */
internal fun Filter.toEventQuery(): EventQuery? {
    if (ids?.isEmpty() == true || authors?.isEmpty() == true || kinds?.isEmpty() == true) return null
    if (tags?.values?.any { it.isEmpty() } == true || tagsAll?.values?.any { it.isEmpty() } == true) return null
    val parsed = SearchQuery.parse(search)
    val terms = parsed.terms.ifEmpty { null }
    val sort = parsed.extensions["sort"]?.let(::rankReputationOf)
    val floor = parsed.extensions["filter"]?.let(::rankFloorOf)
    val observer = parsed.extensions["observer"]?.lowercase()?.takeIf(Hex::isHex64)
    val ranked = terms != null || sort != null
    return EventQuery(
        ids = ids.orEmpty(),
        kinds = kinds.orEmpty(),
        authors = authors.orEmpty(),
        tags = tags.orEmpty(),
        tagsAll = tagsAll.orEmpty(),
        since = since,
        until = until,
        limit = limit,
        search = terms,
        observer = observer,
        // A floor is just a floor: the profile stays whatever the query's shape
        // selects (minRank carries N). The no-terms case resolves in the store,
        // where the out-of-band observer is known — see the observer gate in
        // NostrSemanticsStore.
        ranking = sort,
        minRank = floor ?: if (ranked && !parsed.includeSpam) DEFAULT_MIN_RANK else null,
        includeSpam = parsed.includeSpam,
    )
}

/**
 * The default observer trust floor for search: min_rank=2 on the 0..100 rank
 * scale. Hits whose author the observer's provider doesn't rank are
 * spam-filtered out unless the query says `include:spam`.
 */
const val DEFAULT_MIN_RANK = 2.0

/** `sort:` value -> rank profile; null (ignored) for values we don't recognize. */
private fun rankReputationOf(value: String): String? =
    when (value) {
        "rank", "rank:desc" -> EventYql.RANK_DESC
        "rank:asc" -> EventYql.RANK_ASC
        "followers" -> EventYql.RANK_FOLLOWERS
        "text" -> EventYql.RANK_TEXT
        else -> null
    }

/** `filter:` value (`rank:gte:N` / `rank:gt:N`) -> the min_rank floor; null when unrecognized. */
private fun rankFloorOf(value: String): Double? {
    val parts = value.split(':')
    if (parts.size != 3 || parts[0] != "rank") return null
    val n = parts[2].toDoubleOrNull() ?: return null
    return when (parts[1]) {
        "gte" -> n

        // Scores are integers (0..100): strictly-greater = the next rank up.
        "gt" -> n + 1.0

        else -> null
    }
}
