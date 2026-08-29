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
package com.nosfabrica.vespa.eventstore.search

import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.trust.Enrolment
import com.vitorpamplona.quartz.nip01Core.core.Address
import com.vitorpamplona.quartz.nip01Core.core.Event

/**
 * WHERE A SPLICED SUBJECT GOES.
 *
 * [Anchored] is the original reading and the default: a subject sits where its
 * pointer sits, immediately behind it, in the order the pointer named it. It
 * needs no relevance number and claims no equivalence — the reason is the
 * position.
 *
 * [Weighted] is the one that uses what the publisher said. A Trusted List
 * member carries a 0..100 confidence that the list's NAME applies to it, so a
 * subject inherits its pointer's relevance DISCOUNTED by that confidence and
 * sorts into the page on the result: a member the publisher is sure about
 * competes near its list, one it doubts sinks to where its evidence puts it.
 *
 * ## Why the default is Anchored, and should stay there for now
 *
 * Measured on the staging relay: 131 of 180 Trusted List member scores are
 * exactly 50, and 92 of the flagship list's 98 members are. Whatever computes
 * them is emitting a default for almost everyone, so [Weighted] would sort 94%
 * of that list into one bucket — real machinery expressing no signal, and a
 * visible reshuffle of the feed to express it. The scheme is here so it is
 * ready when a publisher starts computing properly; switching it on before then
 * trades a working page for a worse one.
 *
 * ## What it needs, and what it does without
 *
 * [Weighted] needs the POINTER's relevance, which only exists on a ranked read
 * whose engine reports scores. An engine that does not rank (the in-memory
 * reference) reports null rather than a fabricated constant, and a pointer with
 * no score cannot discount anything — so that pointer's subjects fall back to
 * [Anchored]. The degradation is per pointer, not per page.
 */
sealed interface SplicePlacement {
    /** A subject sits where its pointer sits. */
    data object Anchored : SplicePlacement

    /**
     * `subject = pointer × confidence^gamma`, with an unscored reference
     * treated as full confidence — a label and an assertion express none, and
     * absent must not read as "unsure".
     *
     * [gamma] shapes how hard doubt bites: 1.0 is linear, above 1 punishes a
     * low score harder, below 1 softens it. It is a feel dial and there is no
     * corpus to tune it against yet, which is the other half of why the default
     * is [Anchored].
     */
    data class Weighted(
        val gamma: Double = 1.0,
    ) : SplicePlacement {
        init {
            require(gamma > 0.0) { "gamma must be positive: $gamma" }
        }

        /** The pointer's relevance, discounted — or null when there is nothing to discount. */
        fun scoreFor(
            pointer: Double?,
            confidence: Double?,
        ): Double? {
            if (pointer == null) return null
            val c = confidence ?: return pointer
            return pointer * Math.pow(c.coerceIn(0.0, 1.0), gamma)
        }
    }
}

/**
 * How much of a subscription's feed the expansion may be, and how much index
 * work it may cost: a hit that nominates thousands of subjects — a 2,000-member
 * Trusted List is a normal one — must not turn a five-hit search page into a
 * five-thousand-frame flood, and a page of five such lists must not turn one
 * read into ten thousand key lookups.
 *
 * Both caps bound what is LOOKED UP, not what is found, and they truncate in
 * the pointer's OWN ORDER — the first N members it names. That is the
 * deterministic reading, and it is the useful one: a publisher orders a Trusted
 * List's members by the score it computed for them, so the first N are the N it
 * ranks highest. The alternative — "the first N we happen to hold" — would make
 * the answer depend on what a mirror had caught up on, and would let a run of
 * members this store does not have cost a lookup each anyway. Members past the
 * cap are not looked up at all, which is the point.
 *
 * THAT ORDERING IS A PUBLISHER CONVENTION, AND IT WAS CHECKED rather than
 * hoped: across the eleven Trusted Lists on the staging relay, all 180 members
 * carry a score, every score is inside quartz's 0..100 `SCORE_RANGE` (so none
 * reads back as unscored), and EVERY list is sorted descending. A publisher
 * that does not sort degrades to a deterministic-but-arbitrary top N, not to a
 * wrong one.
 *
 * Both are TRUNCATIONS, not refusals: the pointer itself is served either way,
 * so a client that wants the whole membership reads the member tags and asks
 * for them by `#p` / `#e` / `#a` recall, which is what that recall is for.
 */
data class SearchExpansionLimits(
    /** Off entirely: reads answer exactly what they matched, and nothing is spliced. */
    val enabled: Boolean = true,
    /** Subjects one pointer may bring. A page of lists spends [maxPerRequest] before this bites. */
    val maxPerEvent: Int = 100,
    /** Subjects one read may bring, across every pointer on it. */
    val maxPerRequest: Int = 1_000,
    /** Where a subject lands relative to the hits — see [SplicePlacement]. */
    val placement: SplicePlacement = SplicePlacement.Anchored,
) {
    /** Whether this read needs the engine's per-hit relevance to place anything. */
    val needsScores: Boolean get() = enabled && placement is SplicePlacement.Weighted

    companion object {
        val Default = SearchExpansionLimits()

        /** The expansion switched off — what a caller passes to get plain recall. */
        val Off = SearchExpansionLimits(enabled = false)
    }
}

/**
 * WHAT A SEARCH FOUND, PLUS WHAT IT POINTS AT: the events a NIP-32 label, a
 * NIP-85 assertion or a Tapestry Trusted List nominates, spliced in behind the
 * hit that nominated them.
 *
 * A search matches a LABEL's value or a LIST's title, not the record it is
 * about — so a reader searching "podcaster" gets the list and none of the
 * people on it, which reads as the index being wrong. This puts the record
 * beside the reason, at the reason's own position.
 *
 * ## Only a searching read, and only the searching half of it
 *
 * Everything here is driven by the queries that carry TERMS. A plain NIP-01
 * recall is left alone entirely: a termless query already matches the very
 * predicate the admission rule below applies, so there is nothing an expansion
 * could add to one that the caller did not already ask for. That is also what
 * makes this safe to run unconditionally — a mirror's paging and a NIP-77
 * catch-up carry no terms, and neither does the provider-list read this gate
 * itself performs.
 *
 * ## The trust gate
 *
 * A LIST OR AN ASSERTION UNPACKS ONLY FOR THE READER WHO ENROLLED ITS SIGNER,
 * per kind — those two families are a trust service's computed output, and
 * NIP-85 says how a reader picks services. [Enrolment] carries that argument.
 * NIP-32 labels are deliberately ungated: a label is a description anyone may
 * publish, and gating it would make this relay's own corpus invisible to the
 * anonymous reads that are most of its traffic.
 *
 * ## The lens is not pooled
 *
 * A read may carry several queries with DIFFERENT lenses — one waiving the
 * trust floor with `include:spam`, another ranked through `observer:X`. Their
 * subjects are looked up SEPARATELY, under the lens of the query that found the
 * pointer. Pooling them would let one query's waiver recall subjects for
 * another's gated read, which is a hole in the gate rather than an
 * optimisation. It is the same reason [recallOrdered] refuses to interleave two
 * profiles' scores.
 *
 * ## Admission is the engine's own job
 *
 * A subject is only served if it satisfies everything the finding query asked
 * for EXCEPT the words — its kinds, authors, tags, time window and trust floor.
 * That is not re-implemented here: the lookup query IS the pointer's query with
 * the terms stripped and the subject keys intersected in, so the index applies
 * the same predicate it applied to the hits. A second matcher would be a second
 * answer to "does this belong in this read".
 */
internal class SearchReferenceExpansion(
    /** The queries of this read that carry terms — the only ones that expand. */
    private val searching: List<EventQuery>,
    /** Whose declarations this read may unpack, resolved once for the whole read. */
    private val enrolment: Enrolment,
    private val limits: SearchExpansionLimits,
) {
    /**
     * The lens a subject lookup runs under: everything about a query that
     * decides WHICH CORPUS it sees, and nothing about what it was looking for.
     *
     * Grouped so a page of fifty pointers found by one query costs one lookup,
     * not fifty. The terms are deliberately absent — they are what the subject
     * provably does NOT contain, which is the whole reason it needs fetching.
     */
    private data class Lens(
        val observer: String?,
        val includeSpam: Boolean,
        val minRank: Double?,
        val notExpiredAt: Long?,
        val nowSecs: Long?,
    )

    private fun lensOf(q: EventQuery) = Lens(q.observer, q.includeSpam, q.minRank, q.notExpiredAt, q.nowSecs)

    /** The distinct lenses of this read, and one representative query per lens for admission. */
    private val lenses: List<EventQuery> = searching.distinctBy(::lensOf)

    /** What one read may still spend, across every pointer on it. */
    private var budget = limits.maxPerRequest

    /**
     * Every id this expansion has already served, so a subject that is also a
     * hit — or is named by two pointers — goes out once. NIP-01 asks a relay not
     * to send one event twice on a subscription, and a feature whose whole job
     * is to ADD events is the one most likely to break that.
     */
    private val sent = HashSet<String>()

    /** The rows to serve, and what each one brings with it. */
    class Expanded<R>(
        /** Index-aligned with the input: false for a row this read already served. */
        val fresh: BooleanArray,
        /** Index-aligned with the input: what each row nominates, in the order it named them. */
        val subjects: List<List<R>>,
        /**
         * Index-aligned with [subjects]: the relevance each subject inherits
         * from its pointer, discounted by the confidence the pointer expressed.
         *
         * Null under [SplicePlacement.Anchored], which needs no number, and null
         * per subject where the pointer carried no engine score to discount.
         */
        val scores: List<List<Double?>>,
    )

    /**
     * One pass over [rows], reading each pointer's references and looking up
     * what they name.
     *
     * The pass stops materializing the moment the request budget is spent:
     * reading a row's pointers costs a tags parse and an `EventFactory`
     * dispatch, and a page of 500 lists exhausts the default 1,000-subject
     * budget on the first fifty of them — so a read-them-all-then-plan pass
     * would pay 450 parses for rows it had already decided to take nothing from.
     */
    suspend fun <R> expand(
        rows: List<Ranked<R>>,
        keys: SubjectKeys<R>,
        pointerOf: (R) -> Event?,
        recall: suspend (EventQuery) -> List<R>,
    ): Expanded<R> {
        val fresh = BooleanArray(rows.size) { i -> sent.add(keys.idOf(rows[i].hit)) }
        val nothing = Expanded(fresh, rows.map { emptyList<R>() }, rows.map { emptyList<Double?>() })
        if (!limits.enabled || budget <= 0 || lenses.isEmpty()) return nothing

        var any = false
        val planned = ArrayList<References>(rows.size)
        val lensOfRow = IntArray(rows.size) { NO_LENS }
        for ((i, row) in rows.withIndex()) {
            val pointer = if (budget > 0) pointerOf(row.hit)?.takeIf { it.kind in SearchReferences.KINDS } else null
            // WHICH QUERY FOUND IT, and so which lens its subjects are read
            // through. A read ORs its queries and answers with one union, so a
            // row cannot say which query fetched it — but it can say which would
            // ACCEPT it, and that is the same question with the words left out.
            // First match wins, deterministically; a pointer no lens accepts
            // came from the plain half of a mixed read and expands nothing.
            val lens = if (pointer == null) NO_LENS else lenses.indexOfFirst { it.accepts(pointer) }
            val refs =
                when {
                    pointer == null || lens < 0 -> References.NONE

                    // Resolved once for the whole read; a page of labels never
                    // consults it at all.
                    SearchReferences.isDeclaration(pointer.kind) && !enrolment.admits(pointer.kind, pointer.pubKey) -> References.NONE

                    else -> plan(SearchReferences.of(pointer), lenses[lens])
                }
            if (!refs.isEmpty()) {
                any = true
                lensOfRow[i] = lens
            }
            planned.add(refs)
        }
        if (!any) return nothing

        val found = lookUp(planned, lensOfRow, keys, recall)
        val admitted = ArrayList<List<R>>(rows.size)
        val scores = ArrayList<List<Double?>>(rows.size)
        planned.forEachIndexed { i, refs ->
            val taken = admit(refs, found[lensOfRow[i]], keys)
            admitted.add(taken.map { it.subject })
            val weighted = limits.placement as? SplicePlacement.Weighted
            scores.add(if (weighted == null) taken.map { null } else taken.map { weighted.scoreFor(rows[i].score, refs.weightOf(it.namedBy)) })
        }
        return Expanded(fresh, admitted, scores)
    }

    /** One admitted subject, and the reference key the pointer named it by — which is where its confidence is filed. */
    private class Taken<R>(
        val subject: R,
        val namedBy: String,
    )

    /**
     * What this row may bring, under both caps, in the order it named them.
     * Spends the request budget on what it PLANS rather than on what is later
     * found — see [SearchExpansionLimits] for why the caps are on the lookup.
     *
     * Pubkey subjects are dropped BEFORE the budget is charged when the finding
     * query cannot admit a kind 0: a 2,000-member list under a read that asks
     * for no profiles would otherwise spend the whole request budget on subjects
     * that can never be served, starving an `e` or `a` subject further down.
     */
    private fun plan(
        raw: References,
        under: EventQuery,
    ): References {
        val refs = if (under.admitsKind(PROFILE_KIND) || raw.pubKeys.isEmpty()) raw else References(raw.eventIds, emptyList(), raw.addresses)
        val room = minOf(limits.maxPerEvent, budget)
        if (room <= 0 || refs.isEmpty()) return References.NONE
        if (refs.size <= room) {
            budget -= refs.size
            return refs
        }
        // In practice only one of the three is ever non-empty on a Trusted List,
        // and a label names one or two things in total, so this order decides
        // nothing a real pointer would notice.
        val ids = refs.eventIds.take(room)
        val pubKeys = refs.pubKeys.take(room - ids.size)
        budget -= room
        return References(ids, pubKeys, refs.addresses.take(room - ids.size - pubKeys.size))
    }

    /** The subjects one lens holds, keyed the three ways a pointer names one. */
    private class Found<R>(
        val byId: Map<String, R>,
        val byKey: Map<String, R>,
        val byAddress: Map<String, R>,
    ) {
        companion object {
            fun <R> empty() = Found<R>(emptyMap(), emptyMap(), emptyMap())
        }
    }

    /**
     * The subjects, looked up ONCE PER LENS and never across one.
     *
     * Three shapes per lens — by id, by author for a profile, by (kind, author)
     * for an addressable — because those are the three ways a pointer names a
     * record and each is a different index constraint. Each result is keyed by
     * what it was ASKED for, which is why no second parse of the answer is
     * needed to file it.
     */
    private suspend fun <R> lookUp(
        planned: List<References>,
        lensOfRow: IntArray,
        keys: SubjectKeys<R>,
        recall: suspend (EventQuery) -> List<R>,
    ): Map<Int, Found<R>> {
        val out = HashMap<Int, Found<R>>()
        for (lens in lensOfRow.toSortedSet().filter { it != NO_LENS }) {
            val ids = LinkedHashSet<String>()
            val pubKeys = LinkedHashSet<String>()
            val addresses = LinkedHashSet<String>()
            planned.forEachIndexed { i, refs ->
                if (lensOfRow[i] != lens) return@forEachIndexed
                ids += refs.eventIds
                pubKeys += refs.pubKeys
                addresses += refs.addresses
            }
            val under = lenses[lens].forLookup()
            var spent = 0
            val byId = HashMap<String, R>()
            val byKey = HashMap<String, R>()
            val byAddress = HashMap<String, R>()

            for (chunk in ids.chunked(LOOKUP_CHUNK)) {
                if (spent++ >= MAX_LOOKUPS) break
                under.narrowIds(chunk)?.let { q -> recall(q).forEach { byId[keys.idOf(it)] = it } }
            }
            for (chunk in pubKeys.chunked(LOOKUP_CHUNK)) {
                if (spent++ >= MAX_LOOKUPS) break
                under.narrowProfiles(chunk)?.let { q -> recall(q).forEach { byKey[keys.authorOf(it)] = it } }
            }
            // An addressable subject is a (kind, author, d) triple and the index
            // has no compound key for it, so one query per (kind, author) —
            // grouped, so a list of one publisher's articles costs one lookup.
            for ((owner, addrs) in addresses.mapNotNull { Address.parse(it) }.groupBy { it.kind to it.pubKeyHex }) {
                if (spent++ >= MAX_LOOKUPS) break
                under.narrowAddresses(owner.first, owner.second, addrs.map { it.dTag })?.let { q ->
                    recall(q).forEach { r -> keys.addressOf(r)?.let { byAddress[it] = r } }
                }
            }
            out[lens] = Found(byId, byKey, byAddress)
        }
        return out
    }

    /**
     * This row's planned subjects, as far as the index actually holds them,
     * minus anything this read has already sent — a subject is very often ALSO
     * a hit of the same search, and one pointer's member is often another's.
     */
    private fun <R> admit(
        refs: References,
        found: Found<R>?,
        keys: SubjectKeys<R>,
    ): List<Taken<R>> {
        if (refs.isEmpty() || found == null) return emptyList()
        val out = ArrayList<Taken<R>>(refs.size)
        refs.eventIds.forEach { key -> found.byId[key]?.let { if (sent.add(keys.idOf(it))) out.add(Taken(it, key)) } }
        refs.pubKeys.forEach { key -> found.byKey[key]?.let { if (sent.add(keys.idOf(it))) out.add(Taken(it, key)) } }
        refs.addresses.forEach { key -> found.byAddress[key]?.let { if (sent.add(keys.idOf(it))) out.add(Taken(it, key)) } }
        return out
    }

    private companion object {
        const val NO_LENS = -1
        const val PROFILE_KIND = 0

        /** Keys per lookup query — a bound on one YQL `in` list, not on the answer. */
        const val LOOKUP_CHUNK = 500

        /** Round trips one read's expansion may cost, whatever its page holds. */
        const val MAX_LOOKUPS = 64
    }
}

/**
 * How to read a subject out of whichever row type the caller is serving.
 *
 * The store answers reads as [EventDoc]s and raw reads as `RawEvent`s, and the
 * expansion is identical over both — so the three keys a pointer can name a
 * record by are the only thing it needs a projection for.
 */
internal class SubjectKeys<R>(
    val idOf: (R) -> String,
    val authorOf: (R) -> String,
    /** `kind:pubkey:d`, or null for a row that is not addressable. */
    val addressOf: (R) -> String?,
)

/**
 * Whether this query accepts [event] on everything EXCEPT its words.
 *
 * Used only to attribute a pointer to the lens that found it, so it checks the
 * structural constraints and not the trust floor: the floor already decided
 * whether the pointer was served at all, and re-applying it here would need the
 * rank this row was scored with, which the read no longer carries.
 */
private fun EventQuery.accepts(event: Event): Boolean =
    (ids.isEmpty() || event.id in ids) &&
        admitsKind(event.kind) &&
        (authors.isEmpty() || event.pubKey in authors) &&
        (since == null || event.createdAt >= since!!) &&
        (until == null || event.createdAt <= until!!) &&
        tags.all { (name, values) -> event.tags.any { it.size > 1 && it[0] == name && it[1] in values } }

/** Whether a read asking for these kinds could serve one of [kind]. */
private fun EventQuery.admitsKind(kind: Int): Boolean = kinds.isEmpty() || kind in kinds

/**
 * This query with its TERMS stripped — what a subject lookup runs under.
 *
 * Everything that decides which corpus is visible survives (observer, floor,
 * spam waiver, expiry); everything about what was being looked FOR goes,
 * including the ranking profile the terms selected, since what remains is a
 * keyed recall. The `limit` goes too: a limit is the caller's budget for HITS,
 * and the expansion's own caps already bound the subjects.
 */
private fun EventQuery.forLookup(): EventQuery = copy(search = null, phrases = emptyList(), notSearch = emptyList(), ranking = null, limit = null)

/** The same lookup, narrowed to these ids — null when the read cannot serve them anyway. */
private fun EventQuery.narrowIds(chunk: List<String>): EventQuery? {
    val wanted = if (ids.isEmpty()) chunk else chunk.filter { it in ids }
    return if (wanted.isEmpty()) null else copy(ids = wanted)
}

/** Narrowed to these authors' profiles — null when the read admits no kind 0, or none of these authors. */
private fun EventQuery.narrowProfiles(chunk: List<String>): EventQuery? {
    if (!admitsKind(0)) return null
    val wanted = if (authors.isEmpty()) chunk else chunk.filter { it in authors }
    return if (wanted.isEmpty()) null else copy(kinds = listOf(0), authors = wanted, ids = emptyList())
}

/** Narrowed to one owner's events of one kind — the coarsest key an addressable has. */
private fun EventQuery.narrowAddresses(
    kind: Int,
    pubkey: String,
    dTags: List<String>,
): EventQuery? {
    if (!admitsKind(kind)) return null
    if (authors.isNotEmpty() && pubkey !in authors) return null
    // `d` is a tag the index answers on, so the filter goes to the engine rather
    // than being applied to the answer — a publisher with 10,000 articles must
    // not be read whole to find the three a list named.
    return copy(kinds = listOf(kind), authors = listOf(pubkey), ids = emptyList(), tags = tags + ("d" to dTags))
}
