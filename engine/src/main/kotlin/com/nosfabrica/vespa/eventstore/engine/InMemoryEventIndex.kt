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
package com.nosfabrica.vespa.eventstore.engine
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.utils.Hex

/**
 * The in-memory reference [EventIndex]: a map of docs plus a direct
 * interpretation of [EventQuery]'s matching semantics — the executable spec
 * the real Vespa client must agree with (same fields [EventYql] queries), run
 * by store/relay tests without a Vespa container. Search-term matching is a
 * naive case-insensitive substring over the derived search fields
 * (recall-equivalent; ranking is Vespa's job), and the observer gate
 * ([EventQuery.minRank]/[EventQuery.observer]) is ignored — gated queries
 * recall UNGATED here, so gate tests belong against real Vespa.
 */
class InMemoryEventIndex(
    // Test hook: exercise the bulk path's putIfNewer branch (the address-keyed
    // engine's supersession) against the reference, which still resolves it via
    // the read-based default — so outcomes must match the read-then-supersede path.
    override val supersedesViaPut: Boolean = false,
) : EventIndex {
    // Guarded by synchronized(docs): the store deliberately runs its lock-free
    // dedup reads beside locked writes, so the reference must survive a reader
    // scanning while a writer mutates — exactly as the real engine does.
    private val docs = LinkedHashMap<String, EventDoc>()

    override suspend fun get(id: String): EventDoc? = synchronized(docs) { docs[id] }

    override suspend fun put(doc: EventDoc) {
        synchronized(docs) { docs[doc.id] = doc }
    }

    override suspend fun remove(id: String) {
        synchronized(docs) { docs.remove(id) }
    }

    /**
     * THE MEMBER RUNGS, REPRODUCED — the one rank profile this reference does
     * implement, because it is the one whose arithmetic is the contract rather
     * than the engine's judgement.
     *
     * Everything else here reports a null score on purpose: a fabricated
     * relevance would let a test pass against a ranking this index cannot
     * actually perform. But `spliced_member` is not a judgement about text, it
     * is a placement rule stated in `event.sd` §13 — a rung, a confidence span,
     * and a floor at a share of the pointer that found it — so a reference
     * implementation can hold it exactly, and a consumer assembling this index
     * gets the same order it would get from Vespa for the same confidences and
     * the same pointer. The constants are the schema's own defaults.
     *
     * PER DOCUMENT, because the confidence is: a weighted recall carries each
     * member's own number with its key ([EventQuery.authorWeights]), which is
     * what `rawScore` reads back on the real engine and [Compiled.rawScoreOf]
     * reads back here.
     *
     * And EXACTLY reproducible now, where it used to be reproducible only on a
     * corpus with no reputation in it: placement no longer multiplies by the
     * member's own trust, so there is no term left that this index cannot see.
     */
    private fun memberScoreOf(
        query: EventQuery,
        compiled: Compiled,
        doc: EventDoc,
    ): Double? {
        if (query.ranking != EventYql.RANK_SPLICED_MEMBER) return null
        val gamma = query.rankFeatures["w_member_gamma"] ?: 1.0
        // Per DOCUMENT when the recall carried weights, per QUERY otherwise —
        // the schema's own switch, for the same reason: a publisher's honest 0
        // reads as rawScore 0, so the value cannot double as the flag.
        val conf =
            if ((query.rankFeatures[EventYql.F_DOC_CONF] ?: 0.0) > 0.0) {
                compiled.rawScoreOf(doc) / 100.0
            } else {
                query.rankFeatures[EventYql.F_MEMBER_CONF] ?: 1.0
            }
        val weighted = Math.pow(conf.coerceIn(0.0, 1.0), gamma)

        // The rung carries NO trust term, here or in the schema: `event.sd` §13
        // dropped it so both sides of the max() below are in the pointer's
        // units. This line used to be right by accident (no reputation in a
        // reference index, so wot_mult() was 1.0) and is now right by
        // construction — which is the only reason it is unchanged.
        val rung = MEMBER_TIER + MEMBER_SPAN * weighted
        // The pointer floor, absent (0) unless the caller sent one. Same max()
        // the profile takes, so this reference orders a spliced page the way
        // Vespa would for the same confidences and the same pointer.
        val span = query.rankFeatures[EventYql.F_SUBJECT_FLOOR_SPAN] ?: DEFAULT_FLOOR_SPAN
        val floor = (query.rankFeatures[EventYql.F_POINTER_REL] ?: 0.0) * (span + (1 - span) * weighted)
        return maxOf(rung, floor)
    }

    /** Both ranked paths score per DOCUMENT now — the weights ride with the keys, so the page is not one number. */
    private fun <R> rankedBy(
        query: EventQuery,
        hits: List<EventDoc>,
        project: (EventDoc) -> R,
    ): List<Ranked<R>> {
        val compiled = Compiled(query)
        return hits.map { Ranked(project(it), memberScoreOf(query, compiled, it)) }
    }

    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = rankedBy(query, search(query)) { it }

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = rankedBy(query, search(query)) { it.toRawEvent() }

    override suspend fun search(query: EventQuery): List<EventDoc> {
        // A present limit <= 0 is the "matches nothing" sentinel, as in
        // EventYql.build (which returns null for it, never a negative take).
        if ((query.limit ?: 1) <= 0) return emptyList()
        val c = Compiled(query)
        val hits = synchronized(docs) { docs.values.filter { c.matches(it) } }.sortedWith(EventDoc.NEWEST_FIRST)
        return query.limit?.let(hits::take) ?: hits
    }

    override suspend fun count(query: EventQuery): Int {
        // Sentinel as in EventYql.grouping: limit <= 0 matches nothing; a
        // positive limit is about hits, not the count, and is ignored.
        if ((query.limit ?: 1) <= 0) return 0
        val c = Compiled(query)
        return synchronized(docs) { docs.values.count { c.matches(it) } }
    }

    /** Same sentinel rule as [count]: a positive limit bounds HITS, never a grouping. */
    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> {
        if ((query.limit ?: 1) <= 0) return emptyMap()
        val c = Compiled(query)
        return synchronized(docs) { docs.values.filter { c.matches(it) } }.groupingBy { it.pubkey }.eachCount()
    }

    override fun close() {}

    fun size(): Int = synchronized(docs) { docs.size }

    /**
     * List constraints compiled to hash sets ONCE per scan: matching runs per
     * doc over the whole map, so membership must not be O(list) too (a
     * 300-author filter over 30k docs would burn ~9M string compares).
     * Semantics identical to the direct interpretation.
     */
    private class Compiled(
        private val q: EventQuery,
    ) {
        // Key constraints normalize exactly as EventYql.hexIn: lowercase, valid
        // 64-hex only. A constraint whose values are ALL invalid is
        // unsatisfiable (hexIn returns null), not unconstrained.
        private val ids = q.ids.normHex()
        private val kinds = q.kinds.toHashSet()
        private val authors = q.authors.normHex()
        private val owners = q.owners.normHex()

        // A WEIGHTED recall constrains exactly as its unweighted twin does —
        // `dotProduct(field, {key: weight})` recalls those keys and nothing
        // else, and a ZERO weight recalls its document like any other key
        // (measured against a real Vespa). The numbers matter only to
        // [rawScoreOf]; to matching they are a key set.
        private val idWeights = q.idWeights.normHexKeys()
        private val authorWeights = q.authorWeights.normHexKeys()
        private val unsatisfiable =
            (q.ids.isNotEmpty() && ids.isEmpty()) ||
                (q.authors.isNotEmpty() && authors.isEmpty()) ||
                (q.idWeights.isNotEmpty() && idWeights.isEmpty()) ||
                (q.authorWeights.isNotEmpty() && authorWeights.isEmpty()) ||
                (q.owners.isNotEmpty() && owners.isEmpty())

        private fun List<String>.normHex(): HashSet<String> = mapTo(HashSet()) { it.lowercase() }.apply { retainAll { Hex.isHex64(it) } }

        private fun Map<String, Int>.normHexKeys(): Map<String, Int> = entries.mapNotNull { (key, weight) -> key.lowercase().takeIf(Hex::isHex64)?.let { it to weight } }.toMap()

        /**
         * `rawScore(pubkey) + rawScore(id)` for one document — the weight its
         * own key carried, or 0 where this query weighted nothing it matched.
         * Only one of the two is ever non-zero on a real lookup: a subject is
         * named by an id or by a pubkey, never both.
         */
        fun rawScoreOf(d: EventDoc): Double = ((authorWeights[d.pubkey] ?: 0) + (idWeights[d.id] ?: 0)).toDouble()

        /** One set of `name:value` pairs per OR-tag constraint: the doc matches if any of ITS pairs is in the set. */
        private val tagAny: List<HashSet<String>> = q.tags.map { (name, values) -> values.mapTo(HashSet()) { "$name:$it" } }

        /** tagsAll keeps AND semantics: every listed pair must be on the doc. */
        private val tagAll: List<List<String>> = q.tagsAll.map { (name, values) -> values.map { "$name:$it" } }

        fun matches(d: EventDoc): Boolean {
            if (unsatisfiable) return false
            val pairs = if (tagAny.isEmpty() && tagAll.isEmpty()) emptyList() else d.tagIndex()
            return (ids.isEmpty() || d.id in ids) &&
                (idWeights.isEmpty() || d.id in idWeights) &&
                (kinds.isEmpty() || d.kind in kinds) &&
                (authors.isEmpty() || d.pubkey in authors) &&
                (authorWeights.isEmpty() || d.pubkey in authorWeights) &&
                (owners.isEmpty() || d.owner in owners) &&
                tagAny.all { set -> pairs.any { it in set } } &&
                tagAll.all { required -> required.all { it in pairs } } &&
                (q.since == null || d.createdAt >= q.since) &&
                (q.until == null || d.createdAt <= q.until) &&
                (q.expiresBefore == null || (d.expiresAt()?.let { it < q.expiresBefore } == true)) &&
                (q.notExpiredAt == null || (d.expiresAt() ?: EventDoc.NO_EXPIRATION) > q.notExpiredAt) &&
                (q.search.isNullOrBlank() || d.search.matches(q.search.trim())) &&
                // Phrases and exclusions share the exact-adjacency check, never
                // the loose substring positive words get — mirroring the engine,
                // where both are the same phrase-grammar term.
                q.phrases.all { d.search.containsPhrase(it) } &&
                q.notSearch.none { d.search.containsPhrase(it) }
        }
    }

    private companion object {
        /** `event.sd` §13 defaults, mirrored so this reference orders as Vespa would. */
        const val MEMBER_TIER = 550.0
        const val MEMBER_SPAN = 3450.0

        /** `event.sd`'s query(w_subject_floor_span) default — one rung below the pointer. */
        const val DEFAULT_FLOOR_SPAN = 0.1769
    }
}
