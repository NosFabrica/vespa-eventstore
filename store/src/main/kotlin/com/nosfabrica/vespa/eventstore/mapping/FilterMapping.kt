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

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
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
 * Maps a Quartz Filter to the engine's plain [EventQuery]; null when the filter
 * can never match. Under NIP-01 a present-but-EMPTY list matches nothing; an
 * absent (null) list is no constraint.
 *
 * NIP-50 extensions are relay hints Quartz's parser splits off (keeping
 * `scheme://…` tokens as terms). Honored here:
 *
 *  - `sort:rank[:desc|:asc]` / `sort:followers` / `sort:text` pick the rank
 *    profile; with no terms this is a match-all in that order.
 *  - `filter:rank:gte:N` / `filter:rank:gt:N` raise the observer trust floor
 *    from [DEFAULT_MIN_RANK] to N without changing the order. The profile is
 *    NOT chosen here — plain shapes stay ranking-null so the store can pick
 *    the gated-recall profile once the out-of-band observer is known.
 *  - `include:spam` turns OFF the default trust floor (searches here, plain
 *    recall via the store's observer gate) and rides along as
 *    [EventQuery.includeSpam]. On RANKED queries the floor is LOWERED to
 *    [INCLUDE_SPAM_MIN_RANK], never omitted: min_rank also anchors the default
 *    profile's trust boost, so it must still be sent for the order to stay
 *    trust-lensed.
 *  - `observer:<64-hex>` names the pubkey whose web-of-trust ranks the hits
 *    (otherwise the relay supplies it from the NIP-42 connection); non-hex is
 *    ignored. With no observer, search degrades to pure-text relevance.
 *
 * Term-level syntax beside the extensions — Google's minus and quotes:
 *
 *  - A leading `-` makes an EXCLUSION ([EventQuery.notSearch]), exact-match
 *    only. A `-` on an index-invisible word (`-⚡`) excludes nothing and is
 *    dropped; an exclusions-only query is plain recall minus the words (the
 *    observer gate applies as to any plain filter); a lone `-` stays a
 *    never-matching term.
 *  - A `"quoted span"` is an exact-phrase REQUIREMENT ([EventQuery.phrases]):
 *    adjacent tokens in order, no typo/prefix reach — quoting one word opts
 *    out of fuzzy matching. Phrases are search text (a phrase-only query is a
 *    ranked search); `-"…"` is a phrase exclusion. Spans are lifted BEFORE
 *    Quartz's extension pass (see [liftQuotedSpans] for why that order is
 *    load-bearing), so quotes also protect extension-shaped tokens:
 *    `"sort:rank"` is the phrase [sort, rank], not a sort order.
 *
 * Unknown extensions are ignored. A query that is nothing but extensions becomes
 * unconstrained (null terms), not match-nothing.
 */
internal fun Filter.toEventQuery(): EventQuery? {
    if (ids?.isEmpty() == true || authors?.isEmpty() == true || kinds?.isEmpty() == true) return null
    if (tags?.values?.any { it.isEmpty() } == true || tagsAll?.values?.any { it.isEmpty() } == true) return null
    // Two stages AROUND Quartz's extension pass: quotes first (order is
    // load-bearing — see liftQuotedSpans), then extensions, then `-word`. At
    // the EventQuery seam a search word is always a requirement, never syntax:
    // the pure-negative case arrives with search=null and takes every
    // plain-recall path instead of masquerading as a ranked search.
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
        // selects. The no-terms case resolves in the store, where the
        // out-of-band observer is known (see NostrSemanticsStore's gate).
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
 * The floor a ranked `include:spam` query still SENDS: 0, so no hit is dropped
 * — but the feature must not be omitted. The default profile's wot_mult()
 * anchors its trust curve at query(min_rank), whose schema fail-open default is
 * -1e9: omit the floor and the clamp flattens the boost to the same constant
 * for EVERY author, so trust stops ordering the hits ("many Vitors above the
 * real one"). Anchored at 0, the curve keeps its full span; only the gate is off.
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
 * Stage one, over the RAW search string: lift every `"…"` / `-"…"` span before
 * anything else runs. The order is load-bearing: Quartz's extension pass is
 * quote-BLIND and strips any `key:value` token, so a span ending in one
 * (`"pizza sort:rank" -spam`) would lose its closing quote and the unclosed
 * quote would swallow the rest of the query, flipping a trailing exclusion
 * into required text. Lifting first also lets quotes protect extension-shaped
 * tokens (`"sort:rank"` is the phrase [sort, rank]).
 *
 * A quote opens a span only at a token boundary; mid-token quotes stay
 * ordinary characters. An unclosed span runs to the end. Empty spans are
 * dropped, but a positive phrase keeps index-invisible content ("⚡"): it is an
 * unsatisfiable requirement the ENGINE turns into provably-no-match — dropping
 * it here would silently flip that into match-all.
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
 * Stage two, over the extension-free terms: the `-word` rule. A leading `-` on
 * a 2+ character token flips it to an exclusion (all leading dashes stripped);
 * a lone `-` stays a never-matching loose term. `-sort:rank` is NOT an
 * extension (Quartz keys are strictly `a`-`z`), so it lands here and excludes
 * the literal — there is no `-extension` syntax.
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
