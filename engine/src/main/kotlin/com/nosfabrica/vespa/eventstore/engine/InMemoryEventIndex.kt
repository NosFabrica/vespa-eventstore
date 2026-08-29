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
     * is a placement rule stated in `event.sd` §13 — a rung plus a confidence
     * span — so a reference implementation can hold it exactly, and a consumer
     * assembling this index gets the same order it would get from Vespa for the
     * same confidences. The constants are the schema's own defaults.
     */
    private fun memberScoreOf(query: EventQuery): Double? {
        val text = query.ranking == EventYql.RANK_SPLICED_MEMBER_TEXT
        if (!text && query.ranking != EventYql.RANK_SPLICED_MEMBER) return null
        val conf = query.rankFeatures[EventYql.F_MEMBER_CONF] ?: 1.0
        val gamma = query.rankFeatures["w_member_gamma"] ?: 1.0
        val tier = if (text) MEMBER_TIER_TEXT else MEMBER_TIER
        val span = if (text) MEMBER_SPAN_TEXT else MEMBER_SPAN
        // No reputation here, so wot_mult() is 1.0 — which is also what the
        // trust-multiplied profile computes for an unlensed read.
        return tier + span * Math.pow(conf.coerceIn(0.0, 1.0), gamma)
    }

    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> {
        val member = memberScoreOf(query)
        return search(query).map { Ranked(it, member) }
    }

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> {
        val member = memberScoreOf(query)
        return rawSearch(query).map { Ranked(it, member) }
    }

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
        private val unsatisfiable =
            (q.ids.isNotEmpty() && ids.isEmpty()) ||
                (q.authors.isNotEmpty() && authors.isEmpty()) ||
                (q.owners.isNotEmpty() && owners.isEmpty())

        private fun List<String>.normHex(): HashSet<String> = mapTo(HashSet()) { it.lowercase() }.apply { retainAll { Hex.isHex64(it) } }

        /** One set of `name:value` pairs per OR-tag constraint: the doc matches if any of ITS pairs is in the set. */
        private val tagAny: List<HashSet<String>> = q.tags.map { (name, values) -> values.mapTo(HashSet()) { "$name:$it" } }

        /** tagsAll keeps AND semantics: every listed pair must be on the doc. */
        private val tagAll: List<List<String>> = q.tagsAll.map { (name, values) -> values.map { "$name:$it" } }

        fun matches(d: EventDoc): Boolean {
            if (unsatisfiable) return false
            val pairs = if (tagAny.isEmpty() && tagAll.isEmpty()) emptyList() else d.tagIndex()
            return (ids.isEmpty() || d.id in ids) &&
                (kinds.isEmpty() || d.kind in kinds) &&
                (authors.isEmpty() || d.pubkey in authors) &&
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
        const val MEMBER_TIER_TEXT = 550.0
        const val MEMBER_SPAN_TEXT = 60.0
        const val MEMBER_TIER = 550.0
        const val MEMBER_SPAN = 3450.0
    }
}
