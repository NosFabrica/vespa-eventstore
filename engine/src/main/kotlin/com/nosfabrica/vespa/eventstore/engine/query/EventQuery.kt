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
    /** 64-hex pubkeys. */
    val authors: List<String> = emptyList(),
    /**
     * A KEYED RECALL THAT CARRIES A NUMBER PER KEY — the constraint [authors]
     * expresses, plus a 0..100 weight the RANKING reads back per document as
     * `rawScore(pubkey)`.
     *
     * It exists because a rank feature is a property of the QUERY and a
     * publisher's confidence is not: the search expansion knows how sure a
     * Trusted List is about each member it names, and one lookup serves many
     * members. Without this the only way to tell the engine a per-member number
     * was to GROUP members by that number and pay a round trip per group — which
     * is why `spliced_member` quantized confidence to quarters. A weight rides
     * with its key instead, so one query carries a whole list and every member
     * is scored by what its own publisher said about it, unrounded.
     *
     * REPLACES [authors] for that field rather than joining it: a query carries
     * one or the other, and the caller is expected to have intersected its own
     * author constraint into these keys already.
     *
     * MEASURED against a real Vespa (2026-08-31), because the operator choice
     * is not obvious from the docs: `dotProduct` on a single-value `fast-search`
     * string attribute recalls exactly the keys AND sets `rawScore` to the
     * matched key's weight, while `weightedSet` recalls the same rows and leaves
     * `rawScore` at 0 — which is why this is not that operator. A weight of ZERO
     * still recalls its document (recall and score are independent), so a
     * publisher's honest 0 survives without an offset.
     */
    val authorWeights: Map<String, Int> = emptyMap(),
    /** [authorWeights] for [ids] — `rawScore(id)`, the shape a Trusted List of EVENTS names. */
    val idWeights: Map<String, Int> = emptyMap(),
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
     * THE READER'S OWN FLOOR, APPLIED TO A SPLICED SUBJECT — emitted as
     * query(member_floor) on [EventYql.RANK_SPLICED_MEMBER] and read by nothing
     * else. Null (every ordinary query) leaves the profile's fail-open default,
     * where the gate cannot fire.
     *
     * It exists because [minRank] cannot do this job on that profile. minRank is
     * BOTH the gate and the anchor of the trust curve, and `spliced_member`
     * deliberately does not multiply by wot_mult() — a member is placed by what
     * its list said about it, not by the reader's own view of it (event.sd §13)
     * — so the only place minRank still reaches is `wot_of(member_trust())`
     * inside one branch, which `max(member_rung(), …)` then floors back up.
     * Suppression and placement had collapsed into one number, and removing the
     * multiply removed the suppression with it.
     *
     * So the gate travels separately, and only when the reader ASKED for one:
     * a `filter:rank:gte:N` is a constraint on the whole answer, and a spliced
     * row is part of the answer. The DEFAULT floor deliberately does not travel
     * — a member arrives because a provider the reader enrolled vouched for it,
     * which is the entire point of the delegation, and gating that on the
     * reader's own opinion of a stranger would leave the feature serving
     * nothing.
     */
    val memberFloor: Double? = null,
    /**
     * Overrides a two-phase profile's rerank window (`ranking.rerankCount`, PER
     * CONTENT NODE); ignored on single-phase profiles. For the ranking A/B
     * harness — production queries should trust the profile.
     */
    val rerankCount: Int? = null,
    /**
     * The query instant for RECENCY ranking, epoch seconds — emitted as
     * query(now_secs) on every profile that reads it. Null = the wall clock,
     * which is what production sends.
     *
     * It travels on the query rather than being read from Vespa's own `now`
     * so that a score is a pure function of the REQUEST: a rank assertion can
     * pin a position that would otherwise rot as the corpus ages, every
     * content node scores one query against one instant, and a bad ranking can
     * be replayed at the moment it was reported. Same reason the store already
     * stamps [notExpiredAt] instead of letting the engine decide "now".
     */
    val nowSecs: Long? = null,
    /**
     * Extra rank features layered onto the request
     * (`ranking.features.query(<name>)`), overriding the profile's own
     * defaults. HARNESS KNOB, like [rerankCount]: RankAb sweeps w_recency /
     * recency_halflife across a live cluster with no redeploy, and the rank
     * ITs pin both sides of a calibration without one either. Production
     * queries should trust the profile.
     *
     * Names are validated (`[a-z][a-z0-9_]*`) before they reach the request:
     * everything else this builder puts on the wire is escaped or
     * out-of-band, and a caller-shaped parameter NAME must not be the hole in
     * that.
     */
    val rankFeatures: Map<String, Double> = emptyMap(),
    /**
     * ONE RUNG OF THE TRUST DESCENT: keep only documents whose author some
     * observer ranks at least this (`author_max_rank >= trustFloor`, the
     * scalar the reputation parent carries). Set by VespaEventIndex's descent,
     * never by a caller: a rung is a way of finding the exact page faster,
     * and which rung a page stops on is decided by the bound in TrustDescent,
     * not by anything a filter can say. Null (every caller's query) is the
     * whole corpus.
     */
    val trustFloor: Int? = null,
    /**
     * Emit the direct prefix/fuzzy terms against the *_parts / *_tokens
     * attribute fields (the near tier). Not a caller-facing knob: the client
     * flips it off ONLY as a compatibility demotion when the serving schema
     * predates those fields, where any reference is an HTTP 400 (see
     * SchemaFallbacks.withNearFallback).
     */
    val nearMatching: Boolean = true,
    /**
     * Emit the trigram-PHRASE terms against [FuzzyWordGroup.PHRASE_GRAM_FIELDS]
     * (the body's partial-word reach). Not a caller-facing knob either: the
     * client flips it off ONLY as a compatibility demotion when the serving
     * schema predates `search_text_gram`, where any reference is an HTTP 400.
     * Separate from [nearMatching] because the two shipped at different times —
     * every schema carrying the near columns predates this one, so demoting them
     * together would strip name/title prefix reach from a schema that has it.
     */
    val bodyGramMatching: Boolean = true,
)

/** A ready-to-send Vespa query: the YQL, its query parameters, and the rank profile. */
data class VespaQuery(
    val yql: String,
    val params: Map<String, String>,
    val ranking: String,
)
