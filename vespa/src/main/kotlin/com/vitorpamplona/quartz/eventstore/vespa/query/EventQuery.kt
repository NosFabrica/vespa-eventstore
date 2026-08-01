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

/**
 * A NIP-01 filter as plain values. The relay module maps a REQ's Filter into
 * this, which keeps this module Nostr-library-agnostic. Empty lists mean "no
 * constraint". A filter that arrived with a present-but-empty list matches
 * nothing, and the caller must handle that before building.
 */
data class EventQuery(
    /** 64-hex event ids. */
    val ids: List<String> = emptyList(),
    val kinds: List<Int> = emptyList(),
    /** Kinds to EXCLUDE (`kind not in (…)`). No NIP-01 filter uses this; the CLI status metrics do, to count "content" as everything but the plumbing kinds. */
    val notKinds: List<Int> = emptyList(),
    /** 64-hex pubkeys. */
    val authors: List<String> = emptyList(),
    /** 64-hex owner pubkeys (the semantic owner: gift-wrap recipient or author). */
    val owners: List<String> = emptyList(),
    /** Single-letter tag name -> values. OR within a name, AND across names. */
    val tags: Map<String, List<String>> = emptyMap(),
    /** Like [tags], but EVERY value must be present (Quartz's `tagsAll`). */
    val tagsAll: Map<String, List<String>> = emptyMap(),
    val since: Long? = null,
    val until: Long? = null,
    /** Match docs whose NIP-40 expiration is strictly before this — the expiry sweep. */
    val expiresBefore: Long? = null,
    /** Exclude docs already expired at this time (NIP-40: never serve expired events). */
    val notExpiredAt: Long? = null,
    /** Max hits to return. Null = every match; nothing else caps a query. */
    val limit: Int? = null,
    /** NIP-50 search term; null/blank = plain recall ordered by recency. */
    val search: String? = null,
    /**
     * RANKING context, never recall: the 64-hex pubkey whose web-of-trust
     * weighs search hits (the NIP-42-authenticated user, the NIP-50 `observer:`
     * search token, or the operator's default). Only emitted alongside a search
     * term, as the `user_q` ranking feature. When absent, a search falls back to
     * pure-text relevance ([EventYql.RANK_TEXT]) and no trust gate is applied.
     */
    val observer: String? = null,
    /**
     * Rank-profile override (the NIP-50 `sort:` extension): one of the
     * schema's profiles — [EventYql.RANK_DESC] / [EventYql.RANK_ASC] /
     * [EventYql.RANK_FILTERED] / [EventYql.RANK_FOLLOWERS] /
     * [EventYql.RANK_TEXT]. Null = the default ([EventYql.RANK_SEARCH] with a
     * term, unranked recency without). A non-null ranking with no term is a
     * trust-ordered match-all ("who does my observer rank highest").
     */
    val ranking: String? = null,
    /**
     * The per-observer trust floor, emitted as query(min_rank). Every trust
     * profile gates on it, and the default profile's wot_mult() zeroes anything
     * below it. Set from NIP-50 `filter:rank:…`, or from the spam-filter
     * default that `include:spam` switches off.
     */
    val minRank: Double? = null,
    /**
     * Overrides a two-phase rank profile's rerank window for this query
     * (`ranking.rerankCount`, PER CONTENT NODE). Null = the profile's own
     * setting; meaningless (and ignored by the engine) on single-phase
     * profiles. Exists for the ranking A/B harness — production queries
     * should trust the profile.
     */
    val rerankCount: Int? = null,
    /**
     * Emit the direct prefix/fuzzy terms against the schema's *_parts /
     * *_tokens attribute fields (the near tier). On by default; the client
     * flips it off ONLY as a compatibility demotion when the serving schema
     * predates those fields — any query referencing them is then HTTP 400 on
     * every search (see VespaEventIndex.nearSafe). Not a caller-facing knob.
     */
    val nearMatching: Boolean = true,
)

/** A ready-to-send Vespa query: the YQL, its query parameters, and the rank profile. */
data class VespaQuery(
    val yql: String,
    val params: Map<String, String>,
    val ranking: String,
)
