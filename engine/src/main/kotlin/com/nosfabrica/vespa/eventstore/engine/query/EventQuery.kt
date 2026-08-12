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
package com.nosfabrica.vespa.eventstore.engine.query

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
    /** Kinds to EXCLUDE (`kind not in (…)`). No NIP-01 filter uses this; the CLI status metrics do. */
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
     * Quoted-phrase requirements — the engine half of the store's
     * `"exact words"` syntax. Each entry must appear as ADJACENT tokens, in
     * order, matched exactly, with none of [search]'s prefix/fuzzy reach
     * (though NOT of the schema's stemming on prose fields). Phrases are
     * positive search text, so a phrase-only query is relevance-ordered. A
     * phrase no index can hold ("⚡") is unsatisfiable — provably no match, same
     * rule as [search] words, the opposite of [notSearch] (where it is vacuous).
     */
    val phrases: List<String> = emptyList(),
    /**
     * Words that must NOT match any search field — the engine half of the
     * store's `-word` syntax. Exclusion is deliberately EXACT: one negated
     * tokenized term per word, none of the positive side's prefix/fuzzy/trigram
     * reach, because a loose matcher can only over-exclude and a wrongly dropped
     * hit is invisible in a way a wrongly kept one is not. The prose fields
     * index STEMMED tokens, so "-runs" also drops "running" there (unavoidable
     * from the query; the in-memory reference does not model it). Docs without
     * search fields are never excluded.
     */
    val notSearch: List<String> = emptyList(),
    /**
     * The ranking lens: the 64-hex pubkey whose web-of-trust weighs and gates
     * hits (NIP-42 auth, the NIP-50 `observer:` token, or the operator's
     * default). Emitted as `user_q` on every trust-reading profile, never on
     * unranked/recency recall. When absent, a search falls back to pure text
     * ([EventYql.RANK_TEXT]) and no trust gate applies — trust features fail
     * open, not closed.
     */
    val observer: String? = null,
    /**
     * Rank-profile override (the NIP-50 `sort:` extension): one of the
     * schema's profiles (e.g. [EventYql.RANK_DESC], [EventYql.RANK_TEXT]).
     * Null = the default ([EventYql.RANK_SEARCH] with a term, unranked recency
     * without). A non-null ranking with no term is a trust-ordered match-all.
     */
    val ranking: String? = null,
    /**
     * NIP-50 `include:spam`: opt-out from caller-applied default trust gates.
     * The YQL builder ignores it ([minRank] is the actual gate); it exists so
     * a caller resolving an observer AFTER filter mapping can still honor it.
     */
    val includeSpam: Boolean = false,
    /**
     * The per-observer trust floor, emitted as query(min_rank). Every trust
     * profile gates on it; the default profile's wot_mult() zeroes anything
     * below it. `include:spam` LOWERS it to 0 rather than removing it, because
     * min_rank also ANCHORS wot_mult()'s trust curve — a trust-ranked query must
     * always send some floor. Null = the schema's fail-open default (-1e9):
     * gates no-op, but trust stops ordering the hits.
     */
    val minRank: Double? = null,
    /**
     * Overrides a two-phase profile's rerank window (`ranking.rerankCount`, PER
     * CONTENT NODE); ignored on single-phase profiles. For the ranking A/B
     * harness — production queries should trust the profile.
     */
    val rerankCount: Int? = null,
    /**
     * Emit the direct prefix/fuzzy terms against the *_parts / *_tokens
     * attribute fields (the near tier). Not a caller-facing knob: the client
     * flips it off ONLY as a compatibility demotion when the serving schema
     * predates those fields, where any reference is an HTTP 400 (see
     * SchemaFallbacks.withNearFallback).
     */
    val nearMatching: Boolean = true,
)

/** A ready-to-send Vespa query: the YQL, its query parameters, and the rank profile. */
data class VespaQuery(
    val yql: String,
    val params: Map<String, String>,
    val ranking: String,
)
