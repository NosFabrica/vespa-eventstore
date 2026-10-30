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
import com.nosfabrica.vespa.eventstore.engine.text.IndexableChars
import com.nosfabrica.vespa.eventstore.engine.text.StemText
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import com.vitorpamplona.quartz.utils.Hex

/*
 * NIP-01/NIP-50 filter -> engine query translation. A pure mapping with no
 * store state: the Quartz REQ Filter (with the NIP-50 sort:/filter:/include:spam
 * extensions) becomes the Nostr-agnostic EventQuery the :engine module builds YQL
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
 *  - `sort:recent` asks for NIP-01 order instead of relevance: the SAME loose
 *    match set a search always recalls, ordered `created_at desc, id asc` and
 *    gated by the observer's trust floor — the gated-recall profile
 *    ([EventYql.RANK_RECENCY_GATED]) a plain filter uses, pointed at a search.
 *    Match tiers are NOT consulted: a trigram-level match from a minute ago
 *    outranks a perfect one from yesterday, which is what "chronological" means.
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
 *  - `language:pt` / `language:es` add the stemmed body column of that language
 *    ([EventQuery.stemLanguage]): a hint that widens recall to other forms of
 *    each word, never a filter. Other languages are ignored.
 *  - `observer:<64-hex>` names the pubkey whose web-of-trust ranks the hits
 *    (otherwise the relay supplies it from the NIP-42 connection); non-hex is
 *    ignored. With no observer, search degrades to pure-text relevance.
 *
 * Term-level syntax beside the extensions — Google's minus and quotes:
 *
 *  - A leading `-` makes an EXCLUSION ([EventQuery.notSearch]), exact-match
 *    only; an index-invisible exclusion (`-₿`, `-#`) is dropped here — an
 *    emoji is indexed, so `-🔥` excludes — and an
 *    exclusions-only query is plain recall minus the words.
 *  - A `"quoted span"` is an exact-phrase REQUIREMENT ([EventQuery.phrases]):
 *    adjacent tokens in order, no typo/prefix reach; `-"…"` excludes the
 *    phrase. The lexing itself (quotes before extensions before minus) lives
 *    in Quartz's [SearchQuery] — see its KDoc for the load-bearing order.
 *
 * Unknown extensions are ignored. A query that is nothing but extensions becomes
 * unconstrained (null terms), not match-nothing.
 */
internal fun Filter.toEventQuery(): EventQuery? {
    if (ids?.isEmpty() == true || authors?.isEmpty() == true || kinds?.isEmpty() == true) return null
    if (tags?.values?.any { it.isEmpty() } == true || tagsAll?.values?.any { it.isEmpty() } == true) return null
    // Quartz's SearchQuery parses the full grammar (quotes before extensions
    // before minus — the order is load-bearing and documented there); this store
    // only applies its own policies to the result. At the EventQuery seam a
    // search word is always a requirement, never syntax, so a pure-negative
    // query arrives with search=null and takes the plain-recall paths instead of
    // masquerading as a ranked search.
    val intent = SearchIntent.of(search)
    val parsed = intent.parsed
    // A word that is entirely a NIP-30 `:shortcode:` is one term, matching the
    // term the feed side wrote for that badge ([Shortcodes]) — on requirements
    // and exclusions alike. Quoted phrases are left literal: quoting asks for
    // the text as typed.
    val terms = Shortcodes.rewriteQuery(parsed.terms.ifEmpty { null })
    // Exclusions no index can hold ("-₿") are vacuous either way — dropped.
    // An emoji is indexed ([IndexableChars]): "-🔥" is a real exclusion.
    val notSearch = (parsed.notTerms.map(Shortcodes::rewriteWord) + parsed.notPhrases).filter(IndexableChars::hasIndexable)
    val sort = intent.sort
    val floor = intent.floor
    val observer = intent.observer
    // NIP-50 `language:<ISO-639-1>`: a hint that adds the stemmed body column for
    // that language, where one exists. Never a filter (see EventQuery.stemLanguage).
    // A regional tag (`pt-BR`, `es_419`) names its language by the primary subtag.
    val stemLanguage =
        parsed.language
            ?.lowercase()
            ?.substringBefore('-')
            ?.substringBefore('_')
            ?.takeIf { it in StemText.LANGUAGES }
    val ranked = intent.ranked
    return EventQuery(
        // Lowercased ONCE here: the engine matches hex keys uncased, but the
        // reference expansion compares them as strings against stored keys
        // and the enrolment, and an upper-case author would have recalled its
        // events and then silently skipped its declarations.
        ids = ids.orEmpty().map { it.lowercase() },
        kinds = kinds.orEmpty(),
        authors = authors.orEmpty().map { it.lowercase() },
        tags = tags.orEmpty(),
        tagsAll = tagsAll.orEmpty(),
        since = since,
        until = until,
        limit = limit,
        search = terms,
        phrases = parsed.phrases,
        notSearch = notSearch,
        observer = observer,
        stemLanguage = stemLanguage,
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
        // A search for NOTES ranks fresher than any other shape (see
        // NOTE_RECENCY_WEIGHT); the engine sends it only on the default profile.
        recencyWeight = NOTE_RECENCY_WEIGHT.takeIf { ranked && kinds?.toSet() == NOTE_KINDS },
    )
}

/**
 * WHAT A FILTER'S SEARCH STRING ASKS OF THIS STORE beside its words — the ONE
 * reading of the extensions, which [toEventQuery] compiles from and the facade's
 * `ReadShape` reports to a consumer deciding policy before the read (a lens
 * requirement, a ranked-read throttle).
 *
 * One reading because two drift, and did: vespa-relay re-derived "ranked" as
 * `hasText || extension("sort") != null`, while [rankReputationOf] ignores a
 * `sort:` value it does not recognise — so `sort:bogus` was throttled there as a
 * ranked read and served here as plain recall. Anything that asks "what does
 * this extension mean" asks it here.
 */
internal class SearchIntent private constructor(
    val parsed: SearchQuery,
) {
    /** The rank profile a recognised `sort:` selects; null for none, or for a value this store ignores. */
    val sort: String? = parsed.extensions["sort"]?.let(::rankReputationOf)

    /** The `filter:rank:gte|gt:N` trust floor, or null when absent or malformed. */
    val floor: Double? = parsed.extensions["filter"]?.let(::rankFloorOf)

    /** The `observer:` lens, lowercased; null when absent or not 64-hex (an `npub` is ignored, not decoded). */
    val observer: String? = parsed.extensions["observer"]?.lowercase()?.takeIf(Hex::isHex64)

    /** `include:spam`: the default trust floor waived. */
    val includeSpam: Boolean get() = parsed.includeSpam

    /**
     * Whether the read is RANKED by relevance: it carries text (terms or a
     * phrase) or a recognised `sort:`. Exactly the condition that stamps the
     * default floor on a query ([toEventQuery]'s `minRank`), and that a
     * consumer's ranked-read throttle must agree with.
     */
    val ranked: Boolean get() = parsed.hasText || sort != null

    companion object {
        fun of(search: String?): SearchIntent = SearchIntent(SearchQuery.parse(search))
    }
}

/**
 * The kinds a search must ask for, and ONLY these, to rank at [NOTE_RECENCY_WEIGHT]:
 * kind 1, the one kind the weight was measured on. Widening it (comments, articles,
 * pictures) is a re-measurement, not an edit.
 */
val NOTE_KINDS: Set<Int> = setOf(1)

/**
 * The recency weight (`query(w_recency)`) of a search whose kinds are exactly
 * [NOTE_KINDS] — every other search keeps the 0.1 event.sd ships.
 *
 * MEASURED 2026-10-23 (benchmark/README.md, "Does the order make sense?"): 20 real
 * note searches, the live relay's top 300 each under the canonical observer, judged
 * blind by two readers over four rounds. Judged quality rose at every step from 0.1
 * to 30 (nDCG@10 0.52 -> 0.59) and the median top-10 note went from ~2 years old to
 * ~4 months; 100 and 300 made pages younger and were judged no better. The stale
 * pages were a NIP-14 `subject` artifact: old clients wrote subjects, a subject match
 * sits on the 130,000 rung, and current clients' hashtags land 30x lower.
 *
 * Why only notes: the same 30 on EVERY search slipped known profiles (`Primal` #3 ->
 * #5 in rankAb's pinned cases) and put San Jose meetup calendar events above every
 * "jose" profile; exempting replaceable kinds instead (per-kind, the split event.sd
 * rejected) sank profiles under fresh notes (`Primal` #36). A query that asks for
 * notes alone has no profile to sink.
 *
 * What it costs, deliberately: 1 + 30 = 31x is past event.sd's 4.5 "never cross a
 * rung" ceiling, so a same-day note can pass an older one ONE rung up (a fresh body
 * match over an old weak-tier match, a fresh weak match over an old near match). Never
 * two: the smallest two-rung span on the ladder is weak -> exact, 4,000 -> 130,100 =
 * 32.5x > 31. RankRegressionIT pins both sides of that bound at this weight.
 */
const val NOTE_RECENCY_WEIGHT = 30.0

/**
 * The default observer trust floor for search: min_rank=2 on the 0..100 rank
 * scale. Hits whose author the observer's provider doesn't rank are
 * spam-filtered out unless the query says `include:spam`.
 */
const val DEFAULT_MIN_RANK = 2.0

/**
 * The floor a ranked `include:spam` query still SENDS: 0, so no hit is dropped —
 * but the feature must not be OMITTED. The default profile's wot_mult() anchors
 * its trust curve at query(min_rank), whose schema fail-open default is -1e9, so
 * omitting it flattens the boost to the same constant for every author and trust
 * stops ordering the hits. Anchored at 0 the curve keeps its full span and only
 * the gate is off.
 */
const val INCLUDE_SPAM_MIN_RANK = 0.0

/** `sort:` value -> rank profile; null (ignored) for values we don't recognize. */
private fun rankReputationOf(value: String): String? =
    when (value) {
        "rank", "rank:desc" -> EventYql.RANK_DESC

        "rank:asc" -> EventYql.RANK_ASC

        "followers" -> EventYql.RANK_FOLLOWERS

        "text" -> EventYql.RANK_TEXT

        // Chronological: the observer-gated recency profile, the very one plain
        // recall rides. Its score IS created_at, so the store re-sorts the page
        // client-side into the exact NIP-01 order (see NostrSemanticsStore's
        // keepsEngineOrder). With no observer resolved nothing gates and the
        // profile is pure recency — a search ordered like a feed.
        "recent" -> EventYql.RANK_RECENCY_GATED

        else -> null
    }

/** `filter:` value (`rank:gte:N` / `rank:gt:N`) -> the min_rank floor; null when unrecognized. */
private fun rankFloorOf(value: String): Double? {
    val parts = value.split(':')
    if (parts.size != 3 || parts[0] != "rank") return null
    // Finite only: `NaN` and `Infinity` parse as doubles, and a NaN floor
    // reaches the profile's gate where every comparison is false.
    val n = parts[2].toDoubleOrNull()?.takeIf { it.isFinite() } ?: return null
    return when (parts[1]) {
        "gte" -> n

        // Scores are integers (0..100): strictly-greater = the next rank up.
        "gt" -> n + 1.0

        else -> null
    }
}
