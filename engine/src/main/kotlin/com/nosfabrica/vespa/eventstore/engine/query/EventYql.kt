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
import com.nosfabrica.vespa.eventstore.engine.WHITESPACE
import com.vitorpamplona.quartz.nip01Core.tags.isIndexableTagName
import com.vitorpamplona.quartz.utils.Hex

/**
 * Builds YQL over the `event` schema from an [EventQuery]. Returns null when
 * the query provably matches nothing (no valid 64-hex id/author, a
 * non-single-letter tag name, limit 0), so the caller answers empty (EOSE)
 * without asking Vespa.
 *
 * Injection safety: ids and authors reach the YQL only after 64-hex
 * validation; every other caller-supplied string is escaped ([quote]) or
 * passed out-of-band as a query parameter; trigram literals are filtered to
 * alphanumeric characters only.
 */
object EventYql {
    /** Vespa's built-in no-scoring profile — filters without a search term. */
    const val RANK_UNRANKED = "unranked"

    /** The DEFAULT search profile in event.sd: text relevance combined with concave trust. */
    const val RANK_SEARCH = "search"

    /** Pure text relevance, no trust (`sort:text`). */
    const val RANK_TEXT = "text"

    /** Text order with the trust floor applied. No longer selected by the store's filter mapping (the floor rides the query's own profile); kept for direct API use. */
    const val RANK_FILTERED = "rank_filtered"

    /**
     * NIP-01 recency order with the trust floor: score IS created_at,
     * below-floor authors dropped — the always-on spam gate for feeds and the
     * no-terms `filter:rank:` match-all. MATCH-PHASE variant: the engine keeps
     * only the newest ~[MATCH_PHASE_MAX_HITS] candidates per node before
     * gating. [build] demotes shapes the cut can't serve exactly to
     * [RANK_RECENCY_GATED_EXACT]; a degraded-and-unproven page reruns on the
     * exact profile (VespaEventIndex.recallRoot). The count-probe planner
     * excludes both variants: its windows are proven against the UNGATED
     * match set, which the gate breaks.
     */
    const val RANK_RECENCY_GATED = "recency_gated"

    /** The full-scan variant of [RANK_RECENCY_GATED]: exact for every shape, but ranks every match — the fallback and the unlimited/deep shape, not the hot path. */
    const val RANK_RECENCY_GATED_EXACT = "recency_gated_exact"

    /** Trust-sorted within each match tier, descending (`sort:rank`). */
    const val RANK_DESC = "rank_desc"

    /** Ascending trust within each (still-descending) match tier (`sort:rank:asc`). */
    const val RANK_ASC = "rank_asc"

    /** Verified-follower-count order within match tiers (`sort:followers`). */
    const val RANK_FOLLOWERS = "sort_followers"

    /**
     * Match-phase profile for LIMIT'D unranked recall: keeps only the
     * ~[MATCH_PHASE_MAX_HITS] newest candidates during matching. Selected only
     * when the limit sits [MATCH_PHASE_HEADROOM]x or more under max-hits, so
     * the true top-`limit` always survives the cut; the client accepts a
     * match-phase-degraded response only for this profile and [RANK_RECENCY_GATED].
     */
    const val RANK_RECENCY = "recency"

    /** `max-hits` in event.sd's `recency` match-phase — keep in sync with the schema. */
    const val MATCH_PHASE_MAX_HITS = 20_000

    /** A limit may use [RANK_RECENCY] only with this safety factor under [MATCH_PHASE_MAX_HITS]. */
    const val MATCH_PHASE_HEADROOM = 10

    /**
     * The summary fields needed to reconstruct an event
     * ([com.nosfabrica.vespa.eventstore.engine.doc.EventDoc.fromSummary]).
     * Selecting these instead of `*` omits the BM25 index fields — ~35% fewer
     * bytes on a plain 200-hit note scan, far more on long-form. The omitted
     * fields are index/ranking inputs, never part of the served event.
     */
    const val SUMMARY_FIELDS = "id, pubkey, created_at, kind, tags, content, sig, owner"

    /**
     * The attribute-only document-summary in event.sd serving the existence
     * check ([buildExistence]) — keep in sync with the schema.
     */
    const val SUMMARY_DEDUP = "dedup"

    /**
     * Existence-only recall for the bulk-dedup preload: `select id` under
     * [SUMMARY_DEDUP], answered from the id ATTRIBUTE in memory — the disk
     * summary store never runs. No `order by` (membership is unordered), no
     * `limit` (an existence answer must be complete: a short page would be a
     * wrong write upstream). Null when no valid 64-hex id remains — the
     * constraint is unsatisfiable, so nothing exists (same contract as [build]).
     */
    fun buildExistence(ids: List<String>): VespaQuery? {
        val clause = hexIn("id", ids) ?: return null
        return VespaQuery(
            yql = "select id from event where $clause",
            params = mapOf("presentation.summary" to SUMMARY_DEDUP),
            ranking = RANK_UNRANKED,
        )
    }

    /**
     * True when [build] would auto-select [RANK_RECENCY] for [q]. The index
     * keys two behaviors on this: skipping the count-probe planner and
     * demoting to [RANK_UNRANKED] against a serving schema that predates the
     * profile. A deep-past `until` is EXCLUDED: the newest-first match-phase
     * cut lands above the wanted window and forces the full-scan rerun; those
     * shapes fall to the count-probe planner, whose windows anchor at `until`.
     */
    fun usesRecencyProfile(q: EventQuery): Boolean =
        q.ranking == null &&
            q.search.isNullOrBlank() &&
            q.phrases.isEmpty() &&
            (q.limit ?: 0) in 1..(MATCH_PHASE_MAX_HITS / MATCH_PHASE_HEADROOM) &&
            (q.until == null || q.until >= System.currentTimeMillis() / 1000 - RECENT_UNTIL_HORIZON)

    /** How far back an `until` may sit and still ride [RANK_RECENCY] — beyond it, pagination anchors take the planner path. */
    const val RECENT_UNTIL_HORIZON = 2_592_000L

    /**
     * True when [build] keeps a [RANK_RECENCY_GATED] query on the match-phase
     * variant instead of demoting it to [RANK_RECENCY_GATED_EXACT]. Same shape
     * gate as [usesRecencyProfile], but the headroom must also absorb the
     * gate's drops: a qualifying shape can still come back degraded-and-short
     * when too few of the newest candidates are trusted — that case reruns
     * exact (VespaEventIndex.recallRoot), paid only on heavily-spammed corpora
     * or near-empty trust graphs.
     */
    fun usesGatedMatchPhase(q: EventQuery): Boolean =
        q.ranking == RANK_RECENCY_GATED &&
            (q.limit ?: 0) in 1..(MATCH_PHASE_MAX_HITS / MATCH_PHASE_HEADROOM) &&
            (q.until == null || q.until >= System.currentTimeMillis() / 1000 - RECENT_UNTIL_HORIZON)

    fun build(q: EventQuery): VespaQuery? {
        val params = LinkedHashMap<String, String>()
        val clauses = filterClauses(q, params) ?: return null

        // Trust ranking needs an observer: without one, an unguarded min_rank
        // would gate every hit against a zero score and return nothing — so a
        // search with no observer defaults to pure text and emits neither
        // feature. An explicit sort:/filter: keeps its profile but loses trust.
        val observer = q.observer?.lowercase()?.takeIf(Hex::isHex64)
        val requested =
            q.ranking ?: when {
                // Limit'd unranked recall rides the match-phase profile.
                // (Keep in sync with [usesRecencyProfile].)
                usesRecencyProfile(q) -> RANK_RECENCY

                // Phrases are search text: a phrase-only query ranks like any
                // search. Only notSearch-free-and-text-free recall is plain.
                q.search.isNullOrBlank() && q.phrases.isEmpty() -> RANK_UNRANKED

                observer != null -> RANK_SEARCH

                else -> RANK_TEXT
            }
        // The match-phase cut is only sound for shapes [usesGatedMatchPhase]
        // admits — others would silently lose every hit older than the newest
        // ~max-hits candidates, so they demote to the full-scan variant.
        val ranking = if (requested == RANK_RECENCY_GATED && !usesGatedMatchPhase(q)) RANK_RECENCY_GATED_EXACT else requested
        if (ranking != RANK_UNRANKED && ranking != RANK_RECENCY && observer != null) {
            params["ranking.features.query(user_q)"] = "{$observer:1.0}"
            q.minRank?.let { params["ranking.features.query(min_rank)"] = it.toString() }
        }
        // Two-phase profiles only; the engine ignores it elsewhere.
        q.rerankCount?.let { params["ranking.rerankCount"] = it.toString() }

        val where = whereOf(clauses)
        // Plain recall orders newest first; anything ranked keeps Vespa's
        // score order. created_at ONLY — an engine-side id tiebreak (compound
        // sort on the id STRING attribute) paid UCA collation over the whole
        // match set (measured 0.22s -> 1.3s on 2M matches). The client
        // restores the exact `created_at desc, id asc` contract from the
        // RETURNED page instead — see VespaEventIndex.recallSummaries.
        val order = if (ranking == RANK_UNRANKED || ranking == RANK_RECENCY) " order by created_at desc" else ""
        val limit = q.limit?.let { if (it <= 0) return null else " limit $it" } ?: ""
        return VespaQuery(
            // Reconstruction fields only, not `*` — see [SUMMARY_FIELDS].
            yql = "select $SUMMARY_FIELDS from event where $where$order$limit",
            params = params,
            ranking = ranking,
        )
    }

    /**
     * An EXACT-count query: same filters, a grouping `count()`, NO `order by` —
     * attribute sorting trips Vespa's match-phase on a large corpus and caps
     * `totalCount` (10x+ undercount). Grouping over the unranked match set is exact.
     */
    fun buildCount(q: EventQuery): VespaQuery? = grouping(q, "all(output(count()))")

    /**
     * A DISTINCT-value count over [field] (an attribute): `count()` on the
     * group LIST — distinct values, not docs. Used by status/metrics callers.
     * Null when the filter provably matches nothing.
     */
    fun buildDistinctCount(
        q: EventQuery,
        field: String,
    ): VespaQuery? = grouping(q, "all(group($field) output(count()))")

    /**
     * DISTINCT authors of the match set, aggregated server-side — unlike
     * [buildDistinctCount] this returns the author VALUES. No `max()`: EVERY
     * distinct author comes back; [grouping] and the bundled query profile
     * disable the engine's group ceilings, since a truncated author set would
     * make the orphan-score sweep silently under-delete.
     */
    fun buildDistinctAuthors(q: EventQuery): VespaQuery? = grouping(q, "all(group(pubkey) each(output(count())))")

    /** Per-KIND histogram: grouped by kind, a `count()` per group. Used by status/metrics callers. Null when the filter provably matches nothing. */
    fun buildKindHistogram(q: EventQuery): VespaQuery? = grouping(q, "all(group(kind) each(output(count())))")

    /**
     * The shared shape of every aggregation query: the filter WHERE clause,
     * `limit 0`, the [pipeline] grouping, NO `order by` (attribute sorting
     * trips match-phase and caps totals), unranked. Null when the filter
     * provably matches nothing.
     *
     * The per-request group ceilings are disabled ([UNLIMITED_GROUPS]) so an
     * aggregation answers over the WHOLE match set — a `max()`-less pipeline
     * otherwise returns `grouping.defaultMaxGroups` groups (TEN). The third
     * ceiling, `grouping.globalMaxGroups`, must be disabled too but CANNOT be
     * sent per-request (Vespa 400s any request carrying it): it lives in the
     * bundled query profile (`engine/app/search/query-profiles/default.xml`),
     * and a deployment that replaces that profile must carry the field over.
     */
    private fun grouping(
        q: EventQuery,
        pipeline: String,
    ): VespaQuery? {
        // A present limit <= 0 is the "matches nothing" sentinel (as in [build]);
        // a positive limit is about hits, not the grouping, so it is ignored.
        if (q.limit != null && q.limit <= 0) return null
        val params = LinkedHashMap<String, String>()
        val clauses = filterClauses(q, params) ?: return null
        val where = whereOf(clauses)
        params["grouping.defaultMaxGroups"] = UNLIMITED_GROUPS
        params["grouping.defaultMaxHits"] = UNLIMITED_GROUPS
        return VespaQuery(
            yql = "select * from event where $where limit 0 | $pipeline",
            params = params,
            ranking = RANK_UNRANKED,
        )
    }

    /** Vespa's "disable this ceiling" sentinel for the `grouping.*Max*` settings. */
    const val UNLIMITED_GROUPS = "-1"

    /**
     * The one grouping ceiling Vespa only accepts from a query profile — NOT
     * in [grouping]'s params. Named here for the guard that keeps it out.
     */
    const val GLOBAL_MAX_GROUPS = "grouping.globalMaxGroups"

    /** The shared WHERE clauses (filters + optional search term + exclusions); null when the filter provably matches nothing. */
    private fun filterClauses(
        q: EventQuery,
        params: MutableMap<String, String>,
    ): List<String>? {
        val clauses = ArrayList<String>()

        if (q.ids.isNotEmpty()) clauses += hexIn("id", q.ids) ?: return null
        if (q.kinds.isNotEmpty()) clauses += "kind in (${q.kinds.joinToString(", ")})"
        if (q.notKinds.isNotEmpty()) clauses += "!(kind in (${q.notKinds.joinToString(", ")}))" // Vespa negates with !(...), not `not in`
        if (q.authors.isNotEmpty()) clauses += hexIn("pubkey", q.authors) ?: return null
        if (q.owners.isNotEmpty()) clauses += hexIn("owner", q.owners) ?: return null
        for ((name, values) in q.tags) {
            clauses += tagClause(name, values, "or") ?: return null
        }
        for ((name, values) in q.tagsAll) {
            clauses += tagClause(name, values, "and") ?: return null
        }
        q.since?.let { clauses += "created_at >= $it" }
        q.until?.let { clauses += "created_at <= $it" }
        q.expiresBefore?.let { clauses += "expires_at < $it" }
        q.notExpiredAt?.let { clauses += "expires_at > $it" }

        // Every word the caller typed goes into the query — never silently
        // dropped for speed. Exception: a word with NO letter or digit ("⚡")
        // is erased by tokenization on the doc side too, so no index holds it
        // and its empty requirement would fall to Vespa's null-term handling;
        // it is dropped, and a query that is ONLY such words is provably no
        // match. (SearchFields.matches applies the same filter.)
        val words =
            q.search
                ?.trim()
                .orEmpty()
                .split(WHITESPACE)
                .filter { it.isNotEmpty() }
        val matchable = words.filter { w -> w.any(Char::isLetterOrDigit) }
        if (words.isNotEmpty() && matchable.isEmpty()) return null
        if (matchable.isNotEmpty()) {
            clauses += FuzzyWordGroup.clause(matchable, params, nearFields = q.nearMatching)
            // Short queries lean harder on the trigram safety net.
            params["ranking.features.query(w_gram)"] = if (FuzzyWordGroup.leansOnGrams(matchable)) "8.0" else "2.0"
            // How many words the USER typed. The schema's perfect_match() rung
            // needs it because it cannot count them itself: fieldMatch's
            // queryCompleteness divides by every term in the tree, and
            // [FuzzyWordGroup] adds SYNTHETIC ones — a joined variant at 2+
            // words, adjacent-pair concatenations at 3+, each emitted twice.
            // Those can never match a doc that spells the name normally, so
            // they sat in the denominator and made a whole-field match read
            // 2/3 at two words and 1/4 at three (measured 2026-08-05). Counting
            // the real words client-side is the only place the truth exists.
            params["ranking.features.query(n_words)"] = matchable.size.toString()
        }

        // Quoted phrases ([EventQuery.phrases]): one REQUIRED phrase-grammar
        // term per entry against the `default` fieldset, text out-of-band. No
        // fuzzy word group — exact and adjacent is the point of quoting. The
        // phrase rides RAW (the tokenizer drops what indexing dropped), so
        // only an ALL-erased phrase needs the unsatisfiable-requirement rule.
        q.phrases.forEachIndexed { i, phrase ->
            if (phrase.none(Char::isLetterOrDigit)) return null
            params["p$i"] = phrase
            clauses += "({defaultIndex:\"default\",grammar:\"phrase\"}userInput(@p$i))"
        }

        // Exclusions ([EventQuery.notSearch]): one negated term per word,
        // out-of-band, deliberately NOT the fuzzy word group — exclusion must
        // never out-reach what the user literally typed. grammar:"phrase"
        // keeps a punctuated word ("e-cash") one adjacent unit. A tokenization-
        // erased word ("⚡") is vacuous here (no index holds it, so nothing
        // can be excluded by it) and is simply dropped — the mirror of the
        // positive-side rule.
        q.notSearch
            .filter { w -> w.any(Char::isLetterOrDigit) }
            .forEachIndexed { i, word ->
                params["n$i"] = word
                clauses += "!(({defaultIndex:\"default\",grammar:\"phrase\"}userInput(@n$i)))"
            }
        return clauses
    }

    /**
     * The WHERE text for [clauses]. A negation-only list gets an explicit
     * `true` companion: YQL's `!` is AND-NOT sugar and needs a positive side
     * to subtract from.
     */
    private fun whereOf(clauses: List<String>): String =
        when {
            clauses.isEmpty() -> "true"
            clauses.all { it.startsWith("!(") } -> (listOf("true") + clauses).joinToString(" and ")
            else -> clauses.joinToString(" and ")
        }

    /**
     * One tag constraint: values joined with [op] ("or" = NIP-01 tags, "and" =
     * tagsAll). Null when it can't match: tag_index only holds single-letter
     * names, and a present-but-empty value list matches nothing.
     *
     * The OR case compiles to `in`, not an OR-chain of `contains`: one
     * dictionary-backed iterator vs per-term iterators plus the OR merge — the
     * gap grows with the list, and relay tag lists run to hundreds of values.
     * Semantics on an array attribute are identical. AND has no `in` form.
     */
    private fun tagClause(
        name: String,
        values: List<String>,
        op: String,
    ): String? {
        if (!isIndexableTagName(name)) return null
        if (values.isEmpty()) return null
        if (op == "or" && values.size > 1) {
            return values.joinToString(", ", prefix = "tag_index in (", postfix = ")") { v -> quote("$name:$v") }
        }
        return values.joinToString(" $op ", prefix = "(", postfix = ")") { v -> "tag_index contains ${quote("$name:$v")}" }
    }

    /**
     * `field in (…)` over the valid 64-hex entries of [values] (normalized to
     * lowercase). Invalid entries can never match and are dropped — but if
     * nothing valid remains the constraint is unsatisfiable: null.
     */
    private fun hexIn(
        field: String,
        values: List<String>,
    ): String? {
        val hexes = values.map { it.lowercase() }.filter(Hex::isHex64).distinct()
        if (hexes.isEmpty()) return null
        return "$field in (${hexes.joinToString(", ") { "\"$it\"" }})"
    }

    /** YQL string literal with backslash/quote/control escaping — for caller-supplied text. */
    private fun quote(s: String): String =
        "\"" +
            s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") +
            "\""
}
