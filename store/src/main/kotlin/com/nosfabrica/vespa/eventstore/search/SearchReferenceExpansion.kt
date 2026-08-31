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
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.nosfabrica.vespa.eventstore.mapping.INCLUDE_SPAM_MIN_RANK
import com.nosfabrica.vespa.eventstore.trust.Enrolment
import com.vitorpamplona.quartz.nip01Core.core.Address
import com.vitorpamplona.quartz.nip01Core.core.Event

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
    /**
     * HOW HARD A DOUBTED MEMBER SINKS — the exponent on the confidence a
     * Trusted List expressed about each member, on quartz's 0..100 scale.
     *
     * A subject inherits its pointer's relevance discounted by
     * `(confidence)^gamma` and sorts into the page on the result, so a member
     * the publisher is sure about competes near its list and one it doubts
     * falls to where its evidence puts it. 1.0 is linear; above 1 punishes
     * doubt harder; below 1 softens it, and as it approaches 0 every
     * confidence weighs the same and a subject sits directly behind its
     * pointer again.
     *
     * There is no corpus to tune this against yet — the honest default is the
     * one that applies the publisher's number as given.
     */
    val confidenceGamma: Double = 1.0,
    /**
     * HOW MUCH OF ITS POINTER A SUBJECT IS WORTH — the share of the pointer's
     * own relevance that FLOORS a scored subject, before confidence discounts
     * it. 0.0 switches the floor off and restores the placement that came
     * before it: every subject on its own absolute rung, wherever its pointer
     * landed.
     *
     * WHY A FLOOR AT ALL. `event.sd` §13 scores a member on the affiliation
     * rung (550..4,000) times its own trust, which is an ABSOLUTE answer to
     * "how good is this person" and no answer at all to "how good is this
     * person FOR THIS QUERY". The member matched none of the words — the
     * lookup that fetched it carries none — so the only thing on the page that
     * knows the query is the pointer. Measured on staging, searching `verified
     * human` under a reader whose own service signed the `Verified Human` list:
     * the list ranked #10 on its title, and the member it is 87% sure of, whom
     * that reader ranks 100, sat at #40 — under 27 Wikipedia mirror pages from
     * one rank-30 bot, each matching ONE of the two words in a title. The
     * member ceiling (4,000 x wot) simply cannot reach the token rung (130,000
     * x wot): break-even needs the title's author below rank ~29, so a perfect
     * member of a perfectly-matched list loses to almost any title hit.
     *
     * WHY A FLOOR AND NOT A PLACEMENT, which is the distinction the whole
     * design turns on. `pointer x confidence` AS THE SCORE was tried and it
     * broke: on a banded scale a discounted member leaves its band and lands in
     * the gap below, which is nowhere. As a floor it can only ever raise a
     * subject toward the reason that brought it in — `max()` with the engine's
     * own number, never instead of it — so that failure is unreachable, and a
     * member whose own rung already beats its share keeps the rung.
     *
     * The ceiling is the pointer itself: `share <= 1` and `confidence <= 1`, so
     * a subject can tie its pointer and never pass it, and the stable sort
     * resolves that tie pointer-first. That is the same invariant the lift
     * states from the other side.
     *
     * 1.0 — a subject is worth its pointer, discounted by the confidence its
     * pointer expressed — is the reading with no arbitrary constant in it. As
     * with [confidenceGamma], there is no corpus to tune it against yet.
     */
    val subjectFloorShare: Double = 1.0,
) {
    init {
        require(confidenceGamma > 0.0) { "confidenceGamma must be positive: $confidenceGamma" }
        require(subjectFloorShare in 0.0..1.0) { "subjectFloorShare must be a 0..1 share of the pointer: $subjectFloorShare" }
    }

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
 * ## A kind-restricted search still reaches its pointers
 *
 * A query that asks for specific kinds recalls no pointer of another kind —
 * and a client hunting people asks for `kinds:[0]`, not for the 30392 whose
 * title is where the searched word lives. [companions] is the other half of
 * this class's recall: the same queries re-aimed at the pointer kinds that
 * convert into the asked-for ones, run WITH the caller's queries so the
 * pointers arrive as rows of the same page, and [EventQuery.accepts]
 * attributes each one back to the query it was fetched for.
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
    /**
     * Whose declarations this read may unpack. A SUPPLIER, not a value: on a
     * relay holding no 10040s the provider pass is never cached (ProviderMap
     * refuses to cache emptiness, by design), so resolving it eagerly would
     * bill one small engine query to every observer-carrying search — paid
     * before recall, even by reads that never meet a declaration. [enrolment]
     * resolves it at most once, and only on the paths that consult the gate.
     */
    private val enrolmentSource: suspend () -> Enrolment,
    private val limits: SearchExpansionLimits,
) {
    /** The resolved gate, once [enrolment] has been asked — see [enrolmentSource]. */
    private var resolved: Enrolment? = null

    private suspend fun enrolment(): Enrolment = resolved ?: enrolmentSource().also { resolved = it }

    /**
     * THE DISTINCT LENSES OF THIS READ: each searching query with its TERMS
     * STRIPPED, deduplicated. A lens is both halves of what a lookup needs —
     * which corpus is visible (observer, floor, spam waiver, expiry) AND what
     * the query would admit (kinds, authors, ids, tags, time window).
     *
     * Grouped so a page of fifty pointers found by one query costs one lookup,
     * not fifty; two filters that differ only in their words are one lens, which
     * is the common REQ. Two that differ in KINDS are not, and collapsing them
     * would be a bug rather than a saving: the survivor's kinds would decide
     * both which pointers get attributed and which subjects get admitted, so a
     * REQ asking for labels in one filter and Trusted Lists in another would
     * expand whichever came first and silently half-answer the other.
     *
     * The terms are what the subject provably does NOT contain, which is the
     * whole reason it needs fetching — see [forLookup] for what else goes.
     */
    private val lenses: List<EventQuery> = searching.map { it.forLookup() }.distinct()

    /**
     * The member rank profile per lens, resolved from the query that FOUND the
     * pointer rather than from the stripped lookup.
     *
     * It has to be: [forLookup] removes the terms, and `profileOf` reads the
     * terms to decide which ladder a query ranks on — a stripped query looks
     * like a plain recall and resolves to `unranked`, which would leave every
     * member unscored and silently restore the pointer's own order. The ladder
     * belongs to the search that produced the page, not to the keyed read that
     * fetches its subjects.
     */
    private val memberProfiles: List<String?> =
        searching
            .associateBy({ it.forLookup() }, { EventYql.memberProfileOf(it) })
            .let { byLookup -> lenses.map(byLookup::get) }

    /**
     * THE ADDITIONAL RECALL A SEARCH NEEDS TO REACH ITS POINTERS: each
     * searching query re-aimed at the pointer kinds that CONVERT into what it
     * asked for — because a pointer its own recall cannot return, or cannot
     * return within the caller's `limit`, is a subject the read never learns
     * about.
     *
     * A client hunting people sends `kinds:[0]` — and the Trusted List whose
     * title carries the searched word is a 30392, so the recall never returns
     * it, this expansion never sees it, and the read answers with none of the
     * profiles the corpus actually vouches for. The companions close that
     * hole: a query whose kinds exclude a pointer family that could still
     * name one of its kinds ([SearchReferences.convertibleInto]) is re-run
     * against those kinds under the SAME lens, terms and window — so a
     * companion-fetched pointer earned its place on the page exactly the way
     * a hit does, and is served with it, since the pointer is what tells a
     * client what its subjects mean and what more there is to fetch.
     *
     * The DECLARATION kinds are fetched from their enrolled signers only,
     * plus the reader. An explicit `kinds:[30392]` is a NIP-01 ask and serves
     * strangers' lists as plain hits, gate or no gate — but a companion is
     * this store's own addition to the feed, so it only adds what the gate
     * would let unpack; anything wider would surface every stranger's
     * matching list to a reader who asked for people, as if they had asked
     * for it. Labels are ungated and their companion keeps only the query's
     * own author constraint. The same split means an ANONYMOUS read gets the
     * label companion alone, without a provider-list read to prove it.
     *
     * A query that named ids is left alone — it may serve nothing outside
     * them.
     *
     * AN UNRESTRICTED QUERY GETS THE DECLARATION COMPANION TOO, and that is a
     * correction. This used to skip a query with no kinds on the grounds that
     * it "already recalls every pointer" — true of what such a read ADMITS,
     * and never the whole question, because a pointer still has to win one of
     * the caller's `limit` slots to be on the page at all. A declaration is
     * signed by a NIP-85 service key, which nobody follows and every
     * reputation tensor therefore leaves unranked, so it competes for those
     * slots from the bottom of the trust curve. Measured on staging: searching
     * `"Verified Human"` under a reader whose own provider signed the Trusted
     * List of that name put the 30392 at rank 80, while the ordinary kind-30000
     * copies people had re-published from it ranked 1-5 — so a page of 40 held
     * five look-alikes, none of the reader's own list, and unpacked nothing,
     * while the same search narrowed to `kinds:[0]` came back with all
     * seventeen member profiles. The companion is what makes an enrolled
     * signer reachable at any page depth; the floor waiver below is the other
     * half of the same handicap.
     *
     * THE LABEL COMPANION STAYS KIND-RESTRICTED. It carries no author
     * constraint and no waiver — it is the caller's own query with the kinds
     * swapped — so on an unrestricted read it would re-fetch rows the ranking
     * had already placed below the page, on every searching read a relay
     * serves, to second-guess an order nothing says is wrong. A label has no
     * signer handicap to correct: it is ungated, and it earned its rank the
     * way the hits around it did.
     *
     * THE DECLARATION COMPANION WAIVES THE DEFAULT TRUST FLOOR. Its authors
     * are exactly the signers the reader enrolled, and the canonical NIP-85
     * provider is a service key nobody follows — unranked in every reputation
     * tensor, so the default `min_rank` would spam-filter out the very lists
     * this fetch exists to find. The enrolment is the stronger, explicit
     * signal, and it is already the whole author set of the query; the floor
     * adds nothing but that hole. The waiver mirrors `include:spam` exactly:
     * the DEFAULT floor drops to [INCLUDE_SPAM_MIN_RANK] (never absent —
     * min_rank anchors the trust curve), while a floor the caller raised
     * themselves survives, and the SUBJECT lookups keep the read's own floor
     * untouched — admission still applies everything the caller asked.
     * Only a real Vespa executes the gate (the in-memory reference recalls
     * ungated), so the recall side of this lives with the integration tests.
     */
    suspend fun companions(): List<EventQuery> {
        val out = LinkedHashSet<EventQuery>()
        for (q in searching) {
            if (q.ids.isNotEmpty()) continue
            val missing = SearchReferences.convertibleInto(q.kinds)
            val labels = if (q.kinds.isEmpty()) emptyList() else missing.filterNot(SearchReferences::isDeclaration)
            if (labels.isNotEmpty()) out.add(q.copy(kinds = labels))
            val declarations = missing.filter(SearchReferences::isDeclaration)
            if (declarations.isEmpty()) continue
            // The one companion path that needs the gate — an anonymous read
            // resolves to Enrolment.NONE without a query and adds nothing.
            val gate = enrolment()
            declarations
                // Kinds sharing one signer set fetch together: with one
                // Treasure Map the common shape is a single query for
                // all of them, not one per kind.
                .groupBy(gate::signersOf)
                .forEach { (signers, kinds) ->
                    val authors = if (q.authors.isEmpty()) signers.toList() else q.authors.filter { it in signers }
                    if (authors.isNotEmpty()) {
                        val floor = if (q.minRank == DEFAULT_MIN_RANK) INCLUDE_SPAM_MIN_RANK else q.minRank
                        out.add(q.copy(kinds = kinds, authors = authors, minRank = floor))
                    }
                }
        }
        return out.toList()
    }

    /** What one read may still spend, across every pointer on it. */
    private var budget = limits.maxPerRequest

    /**
     * Round trips already spent, across every lens. A per-lens counter would
     * make the cap "64 times however many filters the REQ carried", which is
     * not a bound on anything a client cannot choose.
     */
    private var spent = 0

    /**
     * Every id this expansion has already served, so a subject that is also a
     * hit — or is named by two pointers — goes out once. NIP-01 asks a relay not
     * to send one event twice on a subscription, and a feature whose whole job
     * is to ADD events is the one most likely to break that.
     */
    private val sent = HashSet<String>()

    /** The rows to serve, and what each one brings with it. */
    class Expanded<R>(
        /** Index-aligned with the input: false for a hit this read already served. */
        val fresh: BooleanArray,
        /** Index-aligned with the input: what each hit nominates, in the order it named them. */
        val subjects: List<List<R>>,
        /**
         * Index-aligned with [subjects]: the relevance THE ENGINE gave each
         * subject, under the member rank profile of the ladder its finding
         * query ranks on (`event.sd` §13).
         *
         * Null per subject whose reference expressed no confidence — a NIP-32
         * label, a NIP-85 assertion. Those were never fetched under the member
         * profile at all, and the caller places them at their pointer's own
         * score.
         */
        val scores: List<List<Double?>>,
        /**
         * Index-aligned with [subjects]: how sure its pointer was about it,
         * ALREADY QUANTIZED AND ALREADY RAISED TO [SearchExpansionLimits.confidenceGamma]
         * — the very number the engine scored the member with, not the raw tag
         * value, so the floor the caller applies and the rung the engine
         * applied cannot disagree about how confident a member is.
         *
         * Null exactly where [scores] is null-by-nature: a reference that
         * expressed no confidence. Those need no floor — they already ride
         * their pointer's own score.
         */
        val confidences: List<List<Double?>>,
    )

    /**
     * One pass over [hits], reading each pointer's references and looking up
     * what they name — each lookup scored by the engine on the rung its
     * finding query ranks on, so what comes back needs no arithmetic here.
     *
     * The pass stops materializing the moment the request budget is spent:
     * reading a row's pointers costs a tags parse and an `EventFactory`
     * dispatch, and a page of 500 lists exhausts the default 1,000-subject
     * budget on the first fifty of them — so a read-them-all-then-plan pass
     * would pay 450 parses for rows it had already decided to take nothing from.
     */
    suspend fun <R> expand(
        hits: List<R>,
        keys: SubjectKeys<R>,
        pointerOf: (R) -> Event?,
        recall: suspend (EventQuery) -> List<Ranked<R>>,
    ): Expanded<R> {
        val fresh = BooleanArray(hits.size) { i -> sent.add(keys.idOf(hits[i])) }

        // Built only when it is actually returned: it is three lists the size
        // of the page, and the ordinary outcome of a searching read is that the
        // expansion has something to add.
        fun nothing() = Expanded(fresh, hits.map { emptyList<R>() }, hits.map { emptyList<Double?>() }, hits.map { emptyList<Double?>() })
        if (!limits.enabled || budget <= 0 || lenses.isEmpty()) return nothing()

        var any = false
        val planned = ArrayList<References>(hits.size)
        val lensOfRow = IntArray(hits.size) { NO_LENS }
        for ((i, hit) in hits.withIndex()) {
            val pointer = if (budget > 0) pointerOf(hit)?.takeIf { it.kind in SearchReferences.KINDS } else null
            // WHICH QUERY FOUND IT, and so which lens its subjects are read
            // through. A read ORs its queries and answers with one union, so a
            // row cannot say which query fetched it — but it can say which would
            // ACCEPT it, and that is the same question with the words left out.
            // First match wins, deterministically — but ASKED-FOR BEATS
            // CONVERTED: a lens whose kinds name the pointer outright takes it
            // before any lens it merely converts into, whatever their order.
            // Without that pass split, a kind-restricted filter early in the
            // REQ would capture a pointer that only a LATER filter's terms
            // matched, and read its subjects through the wrong lens — the
            // cross-lens leak "the lens is not pooled" exists to forbid. A
            // pointer no lens accepts either way came from the plain half of a
            // mixed read and expands nothing.
            val lens =
                when {
                    pointer == null -> {
                        NO_LENS
                    }

                    else -> {
                        lenses.indexOfFirst { it.accepts(pointer, converted = false) }.let { asked ->
                            if (asked >= 0) asked else lenses.indexOfFirst { it.accepts(pointer, converted = true) }
                        }
                    }
                }
            val refs =
                when {
                    pointer == null || lens < 0 -> References.NONE

                    // Resolved lazily, at most once per read — a page of labels
                    // never consults it at all, and neither does one with no
                    // declarations on it.
                    SearchReferences.isDeclaration(pointer.kind) && !enrolment().admits(pointer.kind, pointer.pubKey) -> References.NONE

                    else -> plan(SearchReferences.of(pointer), lenses[lens])
                }
            if (!refs.isEmpty()) {
                any = true
                lensOfRow[i] = lens
            }
            planned.add(refs)
        }
        if (!any) return nothing()

        val found = lookUp(planned, lensOfRow, keys, recall)
        val admitted = ArrayList<List<R>>(hits.size)
        val scores = ArrayList<List<Double?>>(hits.size)
        val confidences = ArrayList<List<Double?>>(hits.size)
        planned.forEachIndexed { i, refs ->
            val taken = admit(refs, found[lensOfRow[i]], keys)
            admitted.add(taken.map { it.subject })
            // THE ENGINE'S NUMBER, NOT ONE COMPUTED HERE. A member was fetched
            // under a rank profile that scores it on the affiliation rung of
            // whichever ladder the finding query used, so what comes back is
            // already comparable to the hits. Deriving it from the pointer's
            // relevance is the thing that broke: see event.sd §13.
            scores.add(taken.map { it.score })
            // The confidence rides out beside it, unused by that rung and
            // needed by the FLOOR the caller applies over it — which is the one
            // question this rung cannot answer, "how good is this member for
            // THIS query" (see [SearchExpansionLimits.subjectFloorShare]).
            confidences.add(taken.map { it.confidence })
        }
        return Expanded(fresh, admitted, scores, confidences)
    }

    /** One admitted subject, the relevance the engine gave it under the member profile, and how sure its pointer was. */
    private class Taken<R>(
        val subject: R,
        val score: Double?,
        /** Quantized and gamma'd, exactly as the lookup scored it — null where the pointer expressed nothing. */
        val confidence: Double?,
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
        val refs = if (under.admitsKind(SearchReferences.PROFILE_KIND) || raw.pubKeys.isEmpty()) raw else raw.withoutPubKeys()
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
        // CONFIDENCE RIDES ALONG UNTRIMMED. It is a lookup keyed by reference,
        // so the entries for members that did not survive cost a map and are
        // never read — while dropping it would silently unweight the one case
        // the cap exists for, a list long enough to truncate.
        return References(ids, pubKeys, refs.addresses.take(room - ids.size - pubKeys.size), refs.confidence)
    }

    /** The subjects one lens holds, keyed the three ways a pointer names one. */
    private class Found<R>(
        val byId: Map<String, Ranked<R>>,
        val byKey: Map<String, Ranked<R>>,
        val byAddress: Map<String, Ranked<R>>,
    )

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
        recall: suspend (EventQuery) -> List<Ranked<R>>,
    ): Map<Int, Found<R>> {
        val out = HashMap<Int, Found<R>>()
        for (lens in lensOfRow.toSortedSet().filter { it != NO_LENS }) {
            val under = lenses[lens]
            // The member profile of THIS lens's ladder — see [memberProfiles].
            val profile = memberProfiles[lens]
            val byId = HashMap<String, Ranked<R>>()
            val byKey = HashMap<String, Ranked<R>>()
            val byAddress = HashMap<String, Ranked<R>>()

            // ONE LOOKUP PER CONFIDENCE BUCKET, because confidence is a
            // property of the (list, member) PAIR and a rank feature is a
            // property of the QUERY. Quantizing to [BUCKETS] steps is what
            // makes that affordable: a publisher scores a whole roster the same
            // way far more often than not (131 of 180 members on the staging
            // relay are exactly 50), so the distinct buckets on a real page are
            // few, and a member's placement inside a rung does not need finer
            // resolution than a twentieth of it.
            for ((bucket, refs) in bucketed(planned, lensOfRow, lens)) {
                val conf = if (bucket == null) under else under.withMember(profile, bucket, limits.confidenceGamma)
                for (chunk in refs.ids.chunked(LOOKUP_CHUNK)) {
                    if (spent++ >= MAX_LOOKUPS) break
                    conf.narrowIds(chunk)?.let { q -> recall(q).forEach { byId.putIfAbsent(keys.idOf(it.hit), it) } }
                }
                for (chunk in refs.pubKeys.chunked(LOOKUP_CHUNK)) {
                    if (spent++ >= MAX_LOOKUPS) break
                    conf.narrowProfiles(chunk)?.let { q -> recall(q).forEach { byKey.putIfAbsent(keys.authorOf(it.hit), it) } }
                }
                // An addressable subject is a (kind, author, d) triple and the
                // index has no compound key for it, so one query per (kind,
                // author) — grouped, so a list of one publisher's articles
                // costs one lookup.
                for ((owner, addrs) in refs.addresses.mapNotNull { Address.parse(it) }.groupBy { it.kind to it.pubKeyHex }) {
                    if (spent++ >= MAX_LOOKUPS) break
                    conf.narrowAddresses(owner.first, owner.second, addrs.map { it.dTag })?.let { q ->
                        recall(q).forEach { r -> keys.addressOf(r.hit)?.let { byAddress.putIfAbsent(it, r) } }
                    }
                }
            }
            out[lens] = Found(byId, byKey, byAddress)
        }
        return out
    }

    /** One bucket's worth of references, keyed the three ways a pointer names a record. */
    private class Shapes(
        val ids: LinkedHashSet<String> = LinkedHashSet(),
        val pubKeys: LinkedHashSet<String> = LinkedHashSet(),
        val addresses: LinkedHashSet<String> = LinkedHashSet(),
    )

    /**
     * This lens's references, grouped by quantized confidence.
     *
     * Ordered HIGHEST FIRST so that a member two lists disagree about is looked
     * up under the higher one — `putIfAbsent` above then keeps that first
     * answer. The generous reading is the right one for a disagreement between
     * two publishers the reader delegated: they both vouched, and the reader
     * asked for both.
     */
    private fun bucketed(
        planned: List<References>,
        lensOfRow: IntArray,
        lens: Int,
    ): List<Pair<Double?, Shapes>> {
        val out = HashMap<Double?, Shapes>()
        planned.forEachIndexed { i, refs ->
            if (lensOfRow[i] != lens) return@forEachIndexed
            refs.eventIds.forEach { out.getOrPut(bucketOf(refs.weightOf(it))) { Shapes() }.ids.add(it) }
            refs.pubKeys.forEach { out.getOrPut(bucketOf(refs.weightOf(it))) { Shapes() }.pubKeys.add(it) }
            refs.addresses.forEach { out.getOrPut(bucketOf(refs.weightOf(it))) { Shapes() }.addresses.add(it) }
        }
        // UNSCORED FIRST, THEN DESCENDING CONFIDENCE, and the order is
        // load-bearing: [lookUp] files each found subject with `putIfAbsent`, so
        // whichever bucket runs first wins a member that two pointers name. An
        // unscored reference is not a doubted one — it is a claim with no
        // confidence attached — so it must not lose its subject to a scored
        // duplicate; and between two publishers the reader delegated, both of
        // whom vouched, the generous reading is the right one.
        //
        // `compareBy(nullsFirst())` sorted ASCENDING, which handed every
        // contested member to the publisher that doubted it most.
        val (unscored, scored) = out.entries.partition { it.key == null }
        return (unscored + scored.sortedByDescending { it.key!! }).map { it.key to it.value }
    }

    /**
     * A 0..1 weight quantized to [BUCKETS] steps, or NULL where the pointer
     * expressed no confidence at all.
     *
     * Null is not zero and not one: it means the member rung does not apply.
     * A NIP-32 label has no confidence field in the NIP and a NIP-85
     * assertion's `d` IS its subject, so neither claim is probabilistic —
     * there is no doubt for a rung to express, and those subjects keep the
     * placement they have always had, their POINTER's own score, which puts
     * them directly behind it. Only a Trusted List member is scored, and only
     * a scored member goes on a rung.
     */
    private fun bucketOf(weight: Double?): Double? = weight?.let { Math.round(it * BUCKETS) / BUCKETS.toDouble() }

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

        // The confidence is read back through the SAME two steps the lookup
        // scored with — quantize to a bucket, then raise to gamma — rather than
        // off the raw tag: [bucketed] is what decided which rank feature the
        // engine saw, and a floor computed from a finer number than the rung
        // would put a member above where its own rung says it belongs.
        fun take(
            r: Ranked<R>,
            key: String,
        ) {
            if (!sent.add(keys.idOf(r.hit))) return
            val conf = bucketOf(refs.weightOf(key))?.let { Math.pow(it, limits.confidenceGamma) }
            out.add(Taken(r.hit, r.score, conf))
        }
        refs.eventIds.forEach { key -> found.byId[key]?.let { take(it, key) } }
        refs.pubKeys.forEach { key -> found.byKey[key]?.let { take(it, key) } }
        refs.addresses.forEach { key -> found.byAddress[key]?.let { take(it, key) } }
        return out
    }

    private companion object {
        const val NO_LENS = -1

        /** Keys per lookup query — a bound on one YQL `in` list, not on the answer. */
        const val LOOKUP_CHUNK = 500

        /** Round trips one read's expansion may cost, whatever its page holds. */
        const val MAX_LOOKUPS = 64

        /**
         * Confidence steps one page may distinguish — and, because a rank
         * feature is a property of the QUERY, the number of round trips a lens
         * can cost: one lookup per occupied bucket per shape.
         *
         * FOUR, not twenty. Twenty gave 5% resolution and let a single
         * well-scored list cost twenty round trips where the old design cost
         * one — a real regression on the hottest thing this feature does. And
         * the resolution bought nothing: measured against the Tapestry corpus,
         * every confidence from 0.10 to 1.00 places a member in the SAME
         * position relative to the page, because the member band spans x7.3
         * while a page spans x367. A page that cannot resolve two ends of the
         * range cannot resolve twentieths of it.
         *
         * Members that land in one bucket tie, and a stable sort then keeps
         * them in the order their list named them — which for a
         * descending-sorted list is the publisher's own ranking. The coarse
         * bucket degrades to the best fallback available rather than to
         * arbitrary order.
         */
        const val BUCKETS = 4
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
 *
 * Two modes on the KIND, and the caller runs them as two PASSES. With
 * [converted] false, the pointer must be of a kind this query asked for
 * outright — the attribution that has always held. With [converted] true, a
 * pointer whose kind merely CONVERTS into this query's kinds is accepted —
 * that is what attributes a companion-fetched pointer
 * ([SearchReferenceExpansion.companions]) to the query whose kinds it was
 * fetched for, and its subjects are then admitted under THAT query's kinds,
 * never under the companion's own. The converting pass may only run after the
 * asked-for pass found nothing: every kind-restricted lens converts from the
 * id-shaped families, so a single merged pass would let whichever lens came
 * first take a pointer another lens explicitly asked for.
 */
private fun EventQuery.accepts(
    event: Event,
    converted: Boolean,
): Boolean =
    (ids.isEmpty() || event.id in ids) &&
        (
            if (converted) {
                kinds.isNotEmpty() && SearchReferences.converts(event.kind, kinds)
            } else {
                admitsKind(event.kind)
            }
        ) &&
        (authors.isEmpty() || event.pubKey in authors) &&
        (since == null || event.createdAt >= since!!) &&
        (until == null || event.createdAt <= until!!) &&
        tags.all { (name, values) -> values.any { event.has(name, it) } } &&
        tagsAll.all { (name, values) -> values.all { event.has(name, it) } }

private fun Event.has(
    name: String,
    value: String,
): Boolean = tags.any { it.size > 1 && it[0] == name && it[1] == value }

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

/**
 * The same lookup, asked to SCORE what it finds as a member of a list this
 * confident.
 *
 * [profile] null means the finding query ranks on no ladder — a recency read, a
 * plain recall — so there is nothing for a synthesized score to be comparable
 * with and the lookup stays unranked. The splice then falls back to the
 * pointer's own order, which is the same degradation an unscored page gets.
 */
private fun EventQuery.withMember(
    profile: String?,
    confidence: Double,
    gamma: Double,
): EventQuery =
    if (profile == null) {
        this
    } else {
        copy(
            ranking = profile,
            rankFeatures = rankFeatures + mapOf(EventYql.F_MEMBER_CONF to confidence, "w_member_gamma" to gamma),
        )
    }

/** The same lookup, narrowed to these ids — null when the read cannot serve them anyway. */
private fun EventQuery.narrowIds(chunk: List<String>): EventQuery? {
    val wanted = if (ids.isEmpty()) chunk else chunk.filter { it in ids }
    return if (wanted.isEmpty()) null else copy(ids = wanted)
}

/** Narrowed to these authors' profiles — null when the read admits no kind 0, or none of these authors. */
private fun EventQuery.narrowProfiles(chunk: List<String>): EventQuery? {
    if (!admitsKind(0)) return null
    val wanted = if (authors.isEmpty()) chunk else chunk.filter { it in authors }
    // `ids` survives rather than being cleared: a read that named specific ids
    // may only serve those, and the engine ANDs the two constraints. Clearing
    // it would let a keyed read hand back a profile it never asked for, which
    // is the one thing "admission is the engine's own job" promises it cannot.
    return if (wanted.isEmpty()) null else copy(kinds = listOf(0), authors = wanted)
}

/** Narrowed to one owner's events of one kind — the coarsest key an addressable has. */
private fun EventQuery.narrowAddresses(
    kind: Int,
    pubkey: String,
    dTags: List<String>,
): EventQuery? {
    if (!admitsKind(kind)) return null
    if (authors.isNotEmpty() && pubkey !in authors) return null
    // A `d` the read ALREADY constrained is intersected, never replaced: `tags`
    // is a map, so `+` would drop the caller's own `#d` and serve coordinates it
    // had excluded. Same reason `ids` survives in [narrowProfiles].
    val wanted = tags["d"]?.let { asked -> dTags.filter { it in asked } } ?: dTags
    if (wanted.isEmpty()) return null
    // `d` is a tag the index answers on, so the filter goes to the engine rather
    // than being applied to the answer — a publisher with 10,000 articles must
    // not be read whole to find the three a list named.
    return copy(kinds = listOf(kind), authors = listOf(pubkey), tags = tags + ("d" to wanted))
}
