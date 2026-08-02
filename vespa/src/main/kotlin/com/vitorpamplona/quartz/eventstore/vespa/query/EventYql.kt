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
import com.vitorpamplona.quartz.eventstore.vespa.WHITESPACE
import com.vitorpamplona.quartz.eventstore.vespa.isSingleLetterTagName
import com.vitorpamplona.quartz.utils.Hex

/**
 * Builds YQL over the `event` schema from an [EventQuery]. Returns null when
 * the query provably matches nothing, so the caller can answer with an empty
 * result (EOSE) instead of asking Vespa. That happens for an id/author
 * constraint with no valid 64-hex entries, a non-single-letter tag name, or
 * limit 0.
 *
 * Injection safety: ids and authors only reach the YQL after 64-hex
 * validation. Every other caller-supplied string is either escaped ([quote])
 * or passed out-of-band as a query parameter (the search words). The one
 * exception is the trigram literals, which are filtered to alphanumeric
 * characters only.
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
     * NIP-01 recency order with the trust floor applied: score IS created_at,
     * below-floor authors are dropped. The store stamps this on plain (non-
     * search) recall when an observer resolves — the always-on spam gate for
     * feeds — and on the no-terms `filter:rank:` match-all.
     *
     * This is the MATCH-PHASE variant, sized for the dominant REQ shape
     * (small recent limits): the engine keeps only the newest ~
     * [MATCH_PHASE_MAX_HITS] candidates per node before gating, so a bare
     * gated feed query costs the same as ungated recency. [build] demotes
     * shapes the cut can't serve exactly (no limit, limit past the headroom,
     * deep-past `until`) to [RANK_RECENCY_GATED_EXACT], and the client reruns
     * a degraded-and-unproven page on the exact profile
     * (VespaEventIndex.recallRoot). The count-probe planner still excludes
     * both variants: its windows are proven against the UNGATED match set,
     * which the gate breaks.
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
     * ~[MATCH_PHASE_MAX_HITS] newest candidates during matching instead of
     * ranking every posting the filter matches — the engine-side rescue for
     * the bare recency scans the count-guarded planner could not window.
     * Selected only when the limit sits at [MATCH_PHASE_HEADROOM]x or more
     * under max-hits, so the true top-`limit` always survives the cut; the
     * response arrives match-phase-degraded, which the client accepts only
     * for this profile and [RANK_RECENCY_GATED].
     */
    const val RANK_RECENCY = "recency"

    /** `max-hits` in event.sd's `recency` match-phase — keep in sync with the schema. */
    const val MATCH_PHASE_MAX_HITS = 20_000

    /** A limit may use [RANK_RECENCY] only with this safety factor under [MATCH_PHASE_MAX_HITS]. */
    const val MATCH_PHASE_HEADROOM = 10

    /**
     * The summary fields a hit actually needs to reconstruct its event
     * ([com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc.fromSummary]). Selecting these instead of `*`
     * omits the BM25 index fields (search_text — a full COPY of content for notes
     * — name, about, the _gram views, expires_at, …) from the returned summary: on
     * a plain 200-hit note scan that is ~35% fewer bytes to transfer and parse,
     * and far more on long-form content where search_text dwarfs everything else.
     * The omitted fields are index/ranking inputs, never part of the served event.
     */
    const val SUMMARY_FIELDS = "id, pubkey, created_at, kind, tags, content, sig, owner"

    /**
     * The attribute-only document-summary in event.sd serving the existence
     * check ([buildExistence]) — keep in sync with the schema.
     */
    const val SUMMARY_DEDUP = "dedup"

    /**
     * Existence-only recall for the bulk-dedup preload: `select id` under the
     * [SUMMARY_DEDUP] summary class, so the engine answers set membership from
     * the id ATTRIBUTE in memory and the disk summary store never runs. At the
     * mirror workload's ~99% hit rate the [build] query returns ~ids.size FULL
     * documents that the caller reads one field off; this returns just that
     * field. No `order by` (membership is unordered — and the recency sort is
     * pure cost here), no `limit` (an existence answer must be complete: a
     * short page would be a wrong write upstream, not a small answer).
     *
     * Null when no valid 64-hex id remains — the constraint is unsatisfiable,
     * so nothing exists (same contract as [build]).
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
     * keys two behaviors on this: skipping the count-probe planner (match-phase
     * already owns these limits) and demoting to [RANK_UNRANKED] against a
     * serving schema that predates the profile.
     *
     * A deep-past `until` (until-based REQ pagination reaching old history) is
     * EXCLUDED: it is exactly the anchor a newest-first match-phase cut is
     * hostile to — the cut lands above the wanted window, the page comes back
     * short, and the client's exactness rerun pays the full scan the profile
     * was meant to avoid. Left out of the profile, those shapes fall to the
     * count-probe planner, whose windows anchor at `until` and work at any
     * depth.
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
     * variant instead of demoting it to [RANK_RECENCY_GATED_EXACT]. The same
     * shape gate as [usesRecencyProfile] (small limit with 10x headroom under
     * max-hits, `until` recent or absent), for the same reasons — with one
     * addition the ungated profile doesn't need: the headroom must also absorb
     * the gate's drops, so a shape that qualifies can still come back
     * degraded-and-short when fewer than `limit` of the ~[MATCH_PHASE_MAX_HITS]
     * newest candidates are trusted. That case reruns exact
     * (VespaEventIndex.recallRoot); it is paid only on heavily-spammed corpora
     * or near-empty trust graphs, not on the routine feed query.
     */
    fun usesGatedMatchPhase(q: EventQuery): Boolean =
        q.ranking == RANK_RECENCY_GATED &&
            (q.limit ?: 0) in 1..(MATCH_PHASE_MAX_HITS / MATCH_PHASE_HEADROOM) &&
            (q.until == null || q.until >= System.currentTimeMillis() / 1000 - RECENT_UNTIL_HORIZON)

    fun build(q: EventQuery): VespaQuery? {
        val params = LinkedHashMap<String, String>()
        val clauses = filterClauses(q, params) ?: return null

        // Trust ranking needs an observer: user_q weights the author's scores and
        // min_rank gates against them. With no observer both are meaningless — and
        // an unguarded min_rank would gate every hit against a zero score, i.e.
        // return nothing — so a search with no observer defaults to pure text and
        // emits neither feature. An explicit sort:/filter: still selects its
        // profile, but degrades to match-tier order (no trust) without an observer.
        val observer = q.observer?.lowercase()?.takeIf(Hex::isHex64)
        val requested =
            q.ranking ?: when {
                // Limit'd unranked recall rides the match-phase profile: same
                // `order by`, but the engine keeps only the newest candidates
                // during matching instead of ranking every posting. Gated to
                // limits with 10x headroom under the profile's max-hits so the
                // top-`limit` always survives. (Keep in sync with [usesRecencyProfile].)
                usesRecencyProfile(q) -> RANK_RECENCY

                // Phrases are search text: a phrase-only query ranks like any
                // search. Only notSearch-free-and-text-free recall is plain.
                q.search.isNullOrBlank() && q.phrases.isEmpty() -> RANK_UNRANKED

                observer != null -> RANK_SEARCH

                else -> RANK_TEXT
            }
        // Gated recall's match-phase cut is only sound for the shapes
        // [usesGatedMatchPhase] admits — an unlimited or deep-until query under
        // the cut would silently lose every hit older than the newest ~max-hits
        // candidates, so those demote to the full-scan variant.
        val ranking = if (requested == RANK_RECENCY_GATED && !usesGatedMatchPhase(q)) RANK_RECENCY_GATED_EXACT else requested
        if (ranking != RANK_UNRANKED && ranking != RANK_RECENCY && observer != null) {
            params["ranking.features.query(user_q)"] = "{$observer:1.0}"
            q.minRank?.let { params["ranking.features.query(min_rank)"] = it.toString() }
        }
        // Two-phase profiles only; the engine ignores it elsewhere.
        q.rerankCount?.let { params["ranking.rerankCount"] = it.toString() }

        val where = whereOf(clauses)
        // No text and no rank profile = plain relay REQ semantics: newest
        // first, no scoring. Anything ranked keeps Vespa's score order.
        // (RANK_RECENCY is unranked-with-match-phase: same order contract.)
        // The id tiebreak makes a limit's cut deterministic when it falls inside
        // a created_at tie — the same (created_at desc, id asc) order the
        // EventIndex contract promises and the client-side sorts apply.
        // created_at ONLY — no engine-side id tiebreak. Compound-sorting by the
        // id STRING attribute made every full-scan recall pay UCA collation
        // over the whole match set (measured 0.22s -> 1.3s on 2M matches). The
        // client restores the exact `created_at desc, id asc` contract from
        // the RETURNED page instead: it overfetches a small slack so the
        // boundary timestamp's tie group arrives complete, resolves the rare
        // overflow with a [t,t] window query, and sorts the page in memory —
        // see VespaEventIndex.recallSummaries.
        val order = if (ranking == RANK_UNRANKED || ranking == RANK_RECENCY) " order by created_at desc" else ""
        val limit = q.limit?.let { if (it <= 0) return null else " limit $it" } ?: ""
        return VespaQuery(
            // Only the reconstruction fields, not `*`: the returned summary skips the
            // BM25 index fields (see [SUMMARY_FIELDS]) that a served event never carries.
            yql = "select $SUMMARY_FIELDS from event where $where$order$limit",
            params = params,
            ranking = ranking,
        )
    }

    /**
     * An EXACT-count query: the same filters, a grouping `count()`, and NO
     * `order by`. Sorting by an attribute trips Vespa's match-phase on a large
     * corpus (it stops after a slice), which caps the reported `totalCount` — so
     * [build]'s recency `order by` would undercount by 10x+. Grouping count over
     * the full, unranked match set is exact.
     */
    fun buildCount(q: EventQuery): VespaQuery? = grouping(q, "all(output(count()))")

    /**
     * A DISTINCT-value count over [field] (an attribute): the same filters, a
     * grouping that outputs `count()` on the group LIST — i.e. the number of
     * distinct values, not the number of docs. No `order by` (same match-phase
     * reasoning as [buildCount]). Used by status/metrics callers to count the
     * distinct pubkeys with content. Null when the filter provably matches nothing.
     */
    fun buildDistinctCount(
        q: EventQuery,
        field: String,
    ): VespaQuery? = grouping(q, "all(group($field) output(count()))")

    /**
     * DISTINCT authors of the match set: the same filters, unranked, grouped by
     * `pubkey`, emitting each group's value. Server-side aggregation returns only
     * the distinct pubkeys, however large the match set — the point of not
     * reconstructing every doc. (`pubkey` is an attribute, so it is groupable;
     * `each(output(count()))` gives each group a payload so Vespa emits it.)
     * Unlike [buildDistinctCount] this returns the author VALUES, not just a count.
     *
     * No `max()`: EVERY distinct author comes back. [grouping] and the bundled
     * query profile between them disable the engine's group ceilings so this
     * stays complete however high the cardinality goes — a truncated author set
     * would make the orphan-score sweep silently under-delete.
     */
    fun buildDistinctAuthors(q: EventQuery): VespaQuery? = grouping(q, "all(group(pubkey) each(output(count())))")

    /**
     * A per-KIND histogram: the same filters, grouped by kind with a `count()`
     * on each group. No `order by` (same match-phase reasoning as [buildCount]).
     * Used by status/metrics callers to show the corpus shape (top kinds by
     * volume). Null when the filter provably matches nothing.
     */
    fun buildKindHistogram(q: EventQuery): VespaQuery? = grouping(q, "all(group(kind) each(output(count())))")

    /**
     * The shared shape of every aggregation query ([buildCount],
     * [buildDistinctCount], [buildDistinctAuthors], [buildKindHistogram]): the
     * same filter WHERE clause, `limit 0` (no hits, only the grouping), the
     * given [pipeline] grouping expression, and NO `order by` — sorting by an
     * attribute trips Vespa's match-phase on a large corpus and caps the reported
     * totals. Unranked. Null when the filter provably matches nothing.
     *
     * The per-request group ceilings are disabled ([UNLIMITED_GROUPS]) so an
     * aggregation answers over the WHOLE match set. These are not optional
     * politeness: a pipeline with no `max()` otherwise returns
     * `grouping.defaultMaxGroups` groups — TEN.
     *
     * The third ceiling, `grouping.globalMaxGroups`, must be disabled TOO —
     * while it is on, Vespa fails a `max()`-less pipeline outright ("Cannot
     * return unbounded number of groups") instead of truncating it. But it
     * CANNOT be sent from here: Vespa's `GroupingQueryParser.validate` rejects
     * any request carrying it with `grouping.globalMaxGroups must be specified
     * in a query profile`, a 400 on every aggregation. So it lives in the
     * bundled query profile (`vespa/app/search/query-profiles/default.xml`) —
     * the only place the engine accepts it — and a deployment that replaces
     * that profile must carry the field over.
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
     * The one grouping ceiling Vespa refuses to take from a request: it must come
     * from a query profile, so it is NOT in [grouping]'s params. Named here for
     * the guard that keeps it out.
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

        // Every word the caller typed goes into the query. A long search term is
        // slower — that is the caller's call to make, not ours to silently make
        // for them by dropping words and answering a question they didn't ask.
        //
        // The one exception is a word with NO letter or digit ("⚡", "//"):
        // tokenization erases it from every matcher's view — userInput emits no
        // term for it, NearText folds it away, the trigram filter drops it — so
        // no index can hold it and no clause can require it. Under the AND'd
        // word groups such a word is not harmless dead weight (as it was under
        // OR): its empty requirement would leave the whole conjunction to
        // Vespa's null-term handling. Dropping it mirrors exactly what indexing
        // did to the doc side; a query that is ONLY such words asked for
        // something no index holds — provably no match, like an all-invalid
        // hex filter. (SearchFields.matches applies the same filter — the
        // reference must not require what the engine cannot see.)
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
        }

        // Quoted phrases ([EventQuery.phrases]): one REQUIRED phrase-grammar
        // term per entry, against the `default` fieldset, the text out-of-band
        // like everything else. Exact and adjacent by construction — no fuzzy
        // word group, that is the point of quoting. The phrase rides RAW:
        // Vespa's tokenizer drops what indexing dropped ("new ⚡ york" is the
        // phrase [new, york], exactly what the doc side holds), so only the
        // ALL-erased phrase needs the words' unsatisfiable-requirement rule.
        q.phrases.forEachIndexed { i, phrase ->
            if (phrase.none(Char::isLetterOrDigit)) return null
            params["p$i"] = phrase
            clauses += "({defaultIndex:\"default\",grammar:\"phrase\"}userInput(@p$i))"
        }

        // Exclusions ([EventQuery.notSearch]): one negated term per word,
        // against the `default` fieldset (every search field at once), the
        // word out-of-band like the positive side. Deliberately NOT the fuzzy
        // word group — see the field's KDoc: exclusion must never out-reach
        // what the user literally typed. grammar:"phrase" keeps a punctuated
        // word ("e-cash") one adjacent unit instead of an anywhere-in-doc AND
        // of its tokens; for a single token the two are identical. A word
        // tokenization erased doc-side ("⚡") is the positive-side rule's
        // mirror image with the opposite outcome: there the requirement was
        // unsatisfiable (provably no match), here it is vacuous — no index
        // holds the word, so no doc can be excluded by it, and the clause is
        // simply dropped.
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
     * `true` companion: YQL's `!` is AND-NOT sugar, so a negation needs a
     * positive side to subtract from — match-all, spelled out. (Reached by a
     * query whose only constraints are exclusions: `notSearch`- or
     * `notKinds`-only.)
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
     * The OR case compiles to the `in` operator, not an OR-chain of `contains`:
     * `tag_index` is a fast-search attribute, and `in` resolves the whole value
     * list through one dictionary-backed iterator where an OR tree pays a
     * per-term iterator plus the OR merge — the difference grows with the list,
     * and relay tag lists run to hundreds of values (a `#p` notification REQ
     * carries the observer's whole follow list). Semantics are identical:
     * `in` on an array attribute matches any element, exactly like the OR of
     * `contains`. AND (tagsAll) has no `in` form; it stays a `contains` chain.
     */
    private fun tagClause(
        name: String,
        values: List<String>,
        op: String,
    ): String? {
        if (!isSingleLetterTagName(name)) return null
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
