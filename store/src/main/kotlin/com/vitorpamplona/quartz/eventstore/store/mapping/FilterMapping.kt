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
 *    [EventQuery.includeSpam]. On RANKED queries the floor is LOWERED to
 *    [INCLUDE_SPAM_MIN_RANK] (0 = keep everything), never omitted: min_rank
 *    is also the anchor of the default profile's trust boost, so it must
 *    still be sent for the order to stay trust-lensed.
 *  - `observer:<64-hex>` names the pubkey whose web-of-trust ranks the hits.
 *    It is the query-side way to pick the ranking lens (the relay otherwise
 *    supplies it from the NIP-42 connection); a non-hex value is ignored. With
 *    no observer resolved, a search degrades to pure-text relevance.
 *
 * Two pieces of TERM-level syntax ride beside the extensions (which are all
 * `key:value` tokens Quartz splits off) — Google's minus and Google's quotes:
 *
 *  - A term starting with `-` is an EXCLUSION and becomes
 *    [EventQuery.notSearch] instead of a search word. Exclusion is
 *    exact-match only (see the field's KDoc); a `-` on a word with nothing
 *    the index can hold (`-⚡`) excludes nothing and is dropped. A query of
 *    ONLY exclusions has no ranking terms, so it is plain recall minus the
 *    words — newest first, and the store's observer gate applies to it
 *    exactly as to any plain filter. A lone `-` is not syntax and stays a
 *    (never-matching) term, as before.
 *  - A `"quoted span"` is an exact-phrase REQUIREMENT ([EventQuery.phrases]):
 *    adjacent tokens, in order, none of the loose words' typo/prefix reach —
 *    quoting a single word is the opt-out from fuzzy matching. Unlike
 *    exclusions, phrases are search text: a phrase-only query is a
 *    relevance-ordered search that gates through the search profiles.
 *    `-"quoted span"` combines the two into a phrase exclusion. An unclosed
 *    quote runs to the end of the text; empty quotes are dropped. Quoted
 *    spans are lifted BEFORE Quartz's extension pass (see [liftQuotedSpans]
 *    for why that order is load-bearing), so quotes also PROTECT
 *    extension-shaped tokens: `"sort:rank"` is the phrase [sort, rank], not
 *    a sort order.
 *
 * Unknown extensions are ignored. A query that is nothing but extensions becomes
 * unconstrained (null terms), not match-nothing.
 */
internal fun Filter.toEventQuery(): EventQuery? {
    if (ids?.isEmpty() == true || authors?.isEmpty() == true || kinds?.isEmpty() == true) return null
    if (tags?.values?.any { it.isEmpty() } == true || tagsAll?.values?.any { it.isEmpty() } == true) return null
    // Term-level syntax parses in two stages AROUND Quartz's extension pass
    // (quotes first — see liftQuotedSpans for why that order is load-bearing;
    // then extensions off the residual; then `-word` off what remains). At
    // the EventQuery seam a search word is always a requirement, never
    // syntax — the pure-negative case arrives engine- and store-side with
    // search=null and takes every plain-recall path (the observer gate, the
    // unranked profile) instead of masquerading as a ranked search, while
    // phrases arrive as the typed requirement they are.
    val quoted = liftQuotedSpans(search.orEmpty())
    val parsed = SearchQuery.parse(quoted.residual)
    val words = splitMinusWords(parsed.terms)
    val terms = words.terms
    // Exclusions no index can hold ("-⚡") are vacuous either way — dropped.
    val notSearch = (words.notWords + quoted.notPhrases).filter { w -> w.any(Char::isLetterOrDigit) }
    val sort = parsed.extensions["sort"]?.let(::rankReputationOf)
    val floor = parsed.extensions["filter"]?.let(::rankFloorOf)
    val observer = parsed.extensions["observer"]?.lowercase()?.takeIf(Hex::isHex64)
    val ranked = terms != null || quoted.phrases.isNotEmpty() || sort != null
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
        phrases = quoted.phrases,
        notSearch = notSearch,
        observer = observer,
        // A floor is just a floor: the profile stays whatever the query's shape
        // selects (minRank carries N). The no-terms case resolves in the store,
        // where the out-of-band observer is known — see the observer gate in
        // NostrSemanticsStore.
        ranking = sort,
        minRank =
            floor ?: when {
                !ranked -> null
                parsed.includeSpam -> INCLUDE_SPAM_MIN_RANK
                else -> DEFAULT_MIN_RANK
            },
        includeSpam = parsed.includeSpam,
    )
}

/**
 * The default observer trust floor for search: min_rank=2 on the 0..100 rank
 * scale. Hits whose author the observer's provider doesn't rank are
 * spam-filtered out unless the query says `include:spam`.
 */
const val DEFAULT_MIN_RANK = 2.0

/**
 * The floor a ranked `include:spam` query still SENDS: 0 on the 0..100 score
 * scale, so no hit is dropped — but the feature must not be omitted. The
 * default search profile's wot_mult() anchors its trust curve at
 * query(min_rank) (`1 + (user_score - min_rank)^w_wot_pow`, delta clamped to
 * the 0..100 scale), and the schema's fail-open default for the feature is
 * -1e9: omit the floor and the clamp flattens the boost to the same constant
 * for EVERY author — trust stops ordering the hits and the observer's
 * most-trusted match sorts as if unranked (reported as "many Vitors above
 * the real one" under include:spam). Anchored at 0, the curve keeps its
 * designed full span and only the gate is off.
 */
const val INCLUDE_SPAM_MIN_RANK = 0.0

/** The quoted spans lifted off the RAW search text, plus the residual for the extension and `-word` passes. */
internal class QuotedSpans(
    val phrases: List<String>,
    val notPhrases: List<String>,
    val residual: String,
)

/** The `-word` split of the extension-free terms: loose words joined back up, exclusions apart. */
internal class MinusWords(
    val terms: String?,
    val notWords: List<String>,
)

/**
 * Stage one of the term-syntax scan, over the RAW search string: lift every
 * `"…"` / `-"…"` span out before anything else runs. The order is
 * load-bearing: Quartz's extension pass is quote-BLIND and strips ANY
 * whitespace token shaped `key:value`, so a span ending in one
 * (`"pizza sort:rank" -spam`) would lose its closing quote with the stripped
 * token — and the then-unclosed quote would swallow the rest of the query
 * into the phrase, flipping the trailing `-spam` EXCLUSION into required
 * text. Lifting first also means quotes protect extension-shaped tokens
 * (`"sort:rank"` is the phrase [sort, rank], not a sort order).
 *
 * A quote opens a span only at a token boundary (start of text, after
 * whitespace, or as `-"` there); a mid-token quote (`foo"bar`) stays an
 * ordinary character. A span runs to the next quote, or to the end when
 * unclosed — the tolerant reading of a dangling quote. Empty spans are
 * dropped; a positive phrase keeps even index-invisible content ("⚡"),
 * because like a loose "⚡" word it is an unsatisfiable requirement the
 * ENGINE turns into provably-no-match — dropping it here would silently
 * flip that into match-all.
 */
internal fun liftQuotedSpans(text: String): QuotedSpans {
    val phrases = ArrayList<String>()
    val notPhrases = ArrayList<String>()
    val residual = StringBuilder()
    var i = 0
    var boundary = true
    while (i < text.length) {
        val c = text[i]
        val neg = c == '-' && i + 1 < text.length && text[i + 1] == '"'
        if (boundary && (c == '"' || neg)) {
            val start = i + if (neg) 2 else 1
            val close = text.indexOf('"', start)
            val end = if (close < 0) text.length else close
            val span = text.substring(start, end).trim()
            i = if (close < 0) text.length else close + 1
            if (span.isNotEmpty()) {
                if (neg) notPhrases += span else phrases += span
            }
            // The lifted span's place stays a token boundary for what follows.
            residual.append(' ')
        } else {
            residual.append(c)
            boundary = c.isWhitespace()
            i++
        }
    }
    return QuotedSpans(phrases, notPhrases, residual.toString())
}

/**
 * Stage two, over the extension-free terms Quartz hands back: the `-word`
 * token rule. A leading `-` on a 2+ character token flips it to an exclusion
 * (every leading dash stripped, so `--word` excludes `word`); a lone `-` is
 * not syntax and stays a (never-matching) loose term. Note an
 * extension-shaped token with a `-` prefix (`-sort:rank`) is NOT an
 * extension (Quartz keys are strictly `a`-`z`), so it lands here and
 * excludes the literal — the phrase [sort, rank] — rather than negating a
 * sort order; there is no `-extension` syntax.
 */
internal fun splitMinusWords(terms: String): MinusWords {
    val loose = ArrayList<String>()
    val excluded = ArrayList<String>()
    for (token in terms.split(WHITESPACE)) {
        if (token.isEmpty()) continue
        if (token.length > 1 && token[0] == '-') excluded += token.trimStart('-') else loose += token
    }
    return MinusWords(loose.joinToString(" ").ifEmpty { null }, excluded)
}

/** Term splitter for [splitMinusWords] (the engine's own WHITESPACE is module-internal). */
private val WHITESPACE = Regex("\\s+")

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
