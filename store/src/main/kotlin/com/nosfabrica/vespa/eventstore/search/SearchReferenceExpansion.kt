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

import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.mapBounded
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
 * Both are TRUNCATIONS, not refusals: the pointer is unaffected by either, so a
 * client that wants the whole membership reads the member tags and asks for
 * them by `#p` / `#e` / `#a` recall, which is what that recall is for —
 * provided the read asked for the pointer's kind, since a read that did not
 * never sees it (see [SearchReferenceExpansion]).
 */
data class SearchExpansionLimits(
    /** Off entirely: reads answer exactly what they matched, and nothing is spliced. */
    val enabled: Boolean = true,
    /** Subjects one pointer may bring. A page of lists spends [maxPerRequest] before this bites. */
    val maxPerEvent: Int = 100,
    /** Subjects one read may bring, across every pointer on it. */
    val maxPerRequest: Int = 1_000,
    /**
     * ENGINE ROUND TRIPS one read's expansion may spend, whatever its page
     * holds — the bound that stops a wide page turning one REQ into hundreds of
     * queries.
     *
     * WHAT IT DROPS WHEN IT BITES, because it does not fail the read: lookups
     * are planned in PAGE order, which on any page that can be sorted is
     * relevance order, so the budget is spent on the best-ranked pointers first
     * and a page too wide loses the subjects of its WORST-ranked ones. That is
     * the only defensible cut — the alternative is dropping whichever family
     * the loop reached last — and it is pinned by test rather than left to the
     * shape of the code.
     *
     * The number is per read and not per lens. One batch per pointer is the
     * shape a ranked page of Trusted Lists has (a pointer's own relevance is a
     * query-level feature, so no two differently-ranked rows can share a
     * query), so this is the page width past which subjects start going
     * missing. The trips overlap ([QUERY_FANOUT] of them in flight), so it
     * bounds cluster work rather than wall-clock.
     */
    val maxLookups: Int = 64,
    /**
     * HOW HARD A DOUBTED MEMBER SINKS — the exponent on the confidence a
     * Trusted List expressed about each member, on quartz's 0..100 scale.
     *
     * It shapes BOTH halves of a member's placement, because both are
     * confidence-driven: how far up its own rung a member sits, and where
     * inside [subjectFloorSpan] it lands under its pointer. 1.0 is linear;
     * above 1 punishes doubt harder; below 1 softens it, and as it approaches
     * 0 every confidence weighs the same and the whole block sits with its
     * pointer again.
     *
     * There is no corpus to tune this against yet — the honest default is the
     * one that applies the publisher's number as given.
     */
    val confidenceGamma: Double = 1.0,
    /**
     * HOW FAR BELOW ITS POINTER A DOUBTED SUBJECT MAY FALL, as a fraction of
     * the pointer's own relevance — or NULL for no floor at all, which is the
     * placement that came before this: every subject on its absolute rung,
     * wherever its pointer landed.
     *
     * WHY A FLOOR. `event.sd` §13's rung answers "how good is this member" and
     * cannot answer "how good is it FOR THIS QUERY": the member matched none of
     * the words — the lookup that fetched it carries none — so the pointer is
     * the only row on the page that knows the query. The rung's ceiling
     * (4,000 x wot) cannot reach a title match's (130,000 x wot) from below
     * whatever the publisher or the reader think of the person: measured on
     * staging, a `Verified Human` list ranked #10 on its title while the member
     * it is 87% sure of, ranked 100 by that reader, sat at #40 under 27 mirror
     * pages from one rank-30 bot.
     *
     * WHY A SPAN AND NOT A SHARE. A plain `pointer x confidence` was tried and
     * rejected twice over: as a placement it ejected discounted members out of
     * their band into the gap below, and even as a floor it lands them at
     * arbitrary points across a BANDED ladder (a quarter of a title match is
     * 32,500 — above the near rung, where nobody calibrated it). The span is a
     * ratio of rungs instead: [DEFAULT_SUBJECT_FLOOR_SPAN] is
     * `w_near_tier / w_name_tier`, so a member lands within ONE RUNG of its
     * pointer however doubted, ordered inside that span by its confidence, and
     * a member the publisher is sure of ties its pointer exactly.
     *
     * The arithmetic is the SCHEMA's — only it knows where the bands are, and
     * only it sees a member's own trust. This is the number handed to it.
     */
    val subjectFloorSpan: Double? = DEFAULT_SUBJECT_FLOOR_SPAN,
) {
    init {
        require(confidenceGamma > 0.0) { "confidenceGamma must be positive: $confidenceGamma" }
        require(subjectFloorSpan == null || subjectFloorSpan in 0.0..1.0) {
            "subjectFloorSpan must be a 0..1 fraction of the pointer, or null for no floor: $subjectFloorSpan"
        }
    }

    companion object {
        /**
         * `w_near_tier / w_name_tier` — one rung of `event.sd`'s text ladder,
         * and the schema's own default for `query(w_subject_floor_span)`. Kept
         * here as a number the store can pass and a test can reason with; the
         * two must move together.
         */
        const val DEFAULT_SUBJECT_FLOOR_SPAN = 0.1769

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
 * A COMPANION IS RECALL, NOT AN ANSWER. The pointer it fetches earns the
 * subjects a place on the page; it does not earn itself one. A read that named
 * `kinds:[0]` gets the profiles the 30392 vouches for and not the 30392, which
 * is a kind it said it did not want — the store drops what the caller's kinds
 * exclude on the way out (`NostrSemanticsStore.servedKinds`). Where a filter
 * of the same read DID name the pointer kind, it stays: it is an ordinary
 * NIP-01 hit for that filter. And a read that named no kinds narrows nothing,
 * because it excluded nothing.
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
     * Whose declarations ONE OBSERVER may unpack. A SUPPLIER, not a value: on a
     * relay holding no 10040s the provider pass is never cached (ProviderMap
     * refuses to cache emptiness, by design), so resolving it eagerly would
     * bill one small engine query to every observer-carrying search — paid
     * before recall, even by reads that never meet a declaration.
     * [enrolmentOf] resolves it at most once PER OBSERVER, and only on the
     * paths that consult the gate.
     */
    private val enrolmentSource: suspend (String?) -> Enrolment,
    private val limits: SearchExpansionLimits,
) {
    /** The lens a pointer is read through: the first that ACCEPTS it outright, else the first that merely converts into it. */
    private fun pickLens(pointer: Event): Int {
        for (converted in ACCEPT_PASSES) {
            val i = lenses.indexOfFirst { it.accepts(pointer, converted) }
            if (i >= 0) return i
        }
        return NO_LENS
    }

    /**
     * MAY THIS DECLARATION UNPACK ON THIS READ — asked of EVERY lens that could
     * have found it, and answered yes only if all of them enrolled its signer.
     *
     * A read ORs its filters into ONE page, so there is no version of this
     * answer that gives filter A a page without filter B's subjects on it. What
     * the gate can guarantee is that nothing unpacks unless every point of view
     * on the read asked for it — so one observer's service key never places
     * rows a reader beside them did not enrol, whatever order the filters
     * arrived in.
     *
     * The alternative was to attribute the pointer to whichever lens accepts it
     * first and ask only that one. It is cheaper and it is order-dependent:
     * `accepts` deliberately ignores the TERMS (that is the whole point of a
     * lens), so both filters of a two-observer read accept a pointer only one
     * of them actually matched, and which of them gets asked is then a property
     * of the filter order rather than of the corpus. Being unanimous costs a
     * mixed read the unpacks only one side enrolled — conservative, and the
     * side to err on — while a SINGLE-lens read, which is every shape we
     * serve today, asks exactly one lens and one gate, as before.
     */
    private suspend fun unanimouslyAdmitted(pointer: Event): Boolean {
        var accepted = false
        for (lens in lenses) {
            if (!lens.accepts(pointer, converted = false) && !lens.accepts(pointer, converted = true)) continue
            accepted = true
            if (!enrolmentOf(lens.observer).admits(pointer.kind, pointer.pubKey)) return false
        }
        return accepted
    }

    /**
     * The resolved gate PER OBSERVER — never pooled across them.
     *
     * A read may carry several filters with different `observer:` tokens, and
     * an earlier version resolved ONE gate for their union: a declaration any
     * of them enrolled unpacked for all of them. That reads fine for a client
     * sending its own two points of view and is wrong for anything else — a
     * relay that multiplexes two people's filters into one store call would let
     * B's trust service place rows on A's page, and the whole point of the gate
     * is that A picked their services in A's 10040. One observer's service key
     * may never unpack for another, so the gate is keyed by the lens that found
     * the pointer, the same way [lookUp] keeps the lens unpooled.
     *
     * A HashMap and not a single field because a two-lens read asks twice; it
     * holds at most one entry per distinct observer on the read, which is one
     * or two in every shape we serve.
     */
    private val resolved = HashMap<String?, Enrolment>()

    private suspend fun enrolmentOf(observer: String?): Enrolment = resolved[observer] ?: enrolmentSource(observer).also { resolved[observer] = it }

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
            // FIRST WINS, because [lenses] is `distinct()` and distinct keeps the
            // first too. `associateBy` kept the LAST, so two queries sharing a
            // lens but ranking on different ladders — `search:"x"` beside
            // `search:"x sort:recent"`, identical once the terms and the ranking
            // are stripped — left lens i pointing at query i's lookup and query
            // j's rung. Today that pair also splits `EventYql.profileOf`, which
            // sends the page down the unscored branch where no member score is
            // read, so the mismatch is masked rather than harmless; it is one
            // profile rule away from placing a whole list on a ladder its
            // finding query never ranked on.
            .asReversed()
            .associate { it.forLookup() to EventYql.memberProfileOf(it) }
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
     * companion-fetched pointer is admitted here exactly the way a hit is,
     * and its subjects are served on the strength of it.
     *
     * The pointer ITSELF is served only if the caller's own kinds admit it.
     * The companion buys recall — without it neither this store nor the
     * client ever learns there was anything to unpack — but a REQ that asked
     * for `kinds:[0]` asked a NIP-01 question, and answering it with a 30392
     * hands back a kind the client has no parser for and did not budget a slot
     * for. `NostrSemanticsStore.servedKinds` is where that narrowing happens,
     * after the splice, so the pointer still places its subjects (they rise
     * with it, and sit under it in its own order) before it steps out of the
     * answer.
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
            val gate = enrolmentOf(q.observer)
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
        /**
         * The relevance the page gave each hit, index-aligned — what a subject
         * is FLOORED at a share of, and null-per-row on a page the engine did
         * not score. A pointer with no relevance sends no floor, and its
         * subjects are placed by their own rung exactly as before.
         */
        relevance: List<Double?>?,
        /**
         * THE POINTERS' TEXT BANDS, index-aligned with [hits] — how well each
         * answered the QUERY, with the signer's trust and the recency
         * multiplier that [relevance] carries left out. Null (or 0 per row)
         * wherever the serving profile reports no match-features, and then a
         * member is placed exactly as it was before this existed.
         *
         * It is a second list rather than a richer score because it answers a
         * second question: [relevance] is where the pointer BELONGS on the
         * page, this is how much the pointer's own words earned — and only the
         * second may be handed to a member, whose trust is its own.
         */
        textBand: List<Double?>?,
        keys: SubjectKeys<R>,
        pointerOf: (R) -> Event?,
        recall: suspend (EventQuery) -> List<Ranked<R>>,
    ): Expanded<R> {
        val fresh = BooleanArray(hits.size) { i -> sent.add(keys.idOf(hits[i])) }

        // Built only when it is actually returned: it is two lists the size of
        // the page, and the ordinary outcome of a searching read is that the
        // expansion has something to add.
        fun nothing() = Expanded(fresh, hits.map { emptyList<R>() }, hits.map { emptyList<Double?>() })
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
                        pickLens(pointer)
                    }
                }
            val refs =
                when {
                    pointer == null || lens < 0 -> References.NONE

                    // Resolved lazily, at most once per read — a page of labels
                    // never consults it at all, and neither does one with no
                    // declarations on it.
                    // The gate is PER OBSERVER and never pooled — see
                    // [unanimouslyAdmitted] and [resolved].
                    SearchReferences.isDeclaration(pointer.kind) && !unanimouslyAdmitted(pointer) -> References.NONE

                    else -> plan(SearchReferences.of(pointer), lenses[lens])
                }
            if (!refs.isEmpty()) {
                any = true
                lensOfRow[i] = lens
            }
            planned.add(refs)
        }
        if (!any) return nothing()

        val found =
            lookUp(planned, lensOfRow, { row -> relevance?.get(row) ?: 0.0 }, { row -> textBand?.get(row) ?: 0.0 }, keys, recall)
        val admitted = ArrayList<List<R>>(hits.size)
        val scores = ArrayList<List<Double?>>(hits.size)
        planned.forEachIndexed { i, refs ->
            val taken = admit(refs, found[lensOfRow[i]], keys)
            admitted.add(taken.map { it.subject })
            // THE ENGINE'S NUMBER, NOT ONE COMPUTED HERE. A member was fetched
            // under a rank profile that scores it on the affiliation rung of
            // whichever ladder the finding query used, so what comes back is
            // already comparable to the hits. Deriving it from the pointer's
            // relevance is the thing that broke: see event.sd §13.
            scores.add(taken.map { it.score })
        }
        return Expanded(fresh, admitted, scores)
    }

    /** One admitted subject and the relevance the engine gave it under the member profile. */
    private class Taken<R>(
        val subject: R,
        val score: Double?,
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
        pointerRel: (Int) -> Double,
        pointerText: (Int) -> Double,
        keys: SubjectKeys<R>,
        recall: suspend (EventQuery) -> List<Ranked<R>>,
    ): Map<Int, Found<R>> {
        val out = HashMap<Int, Found<R>>()
        // The lenses this page actually attributed a pointer to, in index
        // order. `lensOfRow.toSortedSet()` boxed one Integer per ROW into a
        // TreeSet — 500 of them on a full page — to arrive at a set that can
        // never hold more than `lenses.size` entries, which is one on the REQ
        // shape a relay serves all day.
        for (lens in lenses.indices.filter { lens -> lensOfRow.any { it == lens } }) {
            val under = lenses[lens]
            // The member profile of THIS lens's ladder — see [memberProfiles].
            val profile = memberProfiles[lens]
            val byId = HashMap<String, Ranked<R>>()
            val byKey = HashMap<String, Ranked<R>>()
            val byAddress = HashMap<String, Ranked<R>>()

            // ONE LOOKUP PER DISTINCT QUERY for the keyed shapes — which is one
            // per POINTER only where the pointers actually differ in what they
            // send. The two numbers a member's placement needs are a property
            // of the PAIR (how sure this list is about this person) and of the
            // POINTER (how well the list matched the query), and a query can
            // now carry both: the confidence rides with each key as a weight
            // the ranking reads back per document (`rawScore`, see
            // [EventQuery.authorWeights]), leaving the pointer's own relevance
            // as the one query-level number.
            //
            // That per-query number is the ONLY thing that can force two
            // pointers apart, so rows are BATCHED by it. Everything a floor
            // cannot reach shares one round trip however long the page:
            // unscored references (a label, an assertion — no weights, no
            // floor, the caller's own lens unchanged), and every scored member
            // on a read with the floor switched off. A page of fifty labels is
            // one lookup, as it was before the weights existed; only a page of
            // fifty *differently-ranked Trusted Lists* costs fifty, and that is
            // the case the floor is for.
            //
            // The weights themselves cost nothing to batch: a key carries its
            // own number, so members of different lists ride in one query at
            // their own confidences. That is what replaced one lookup per
            // CONFIDENCE BUCKET — buckets existed only because a rank feature
            // is per-query, and the staging `Verified Human` list's seventeen
            // members occupy four of them, so its page paid four lookups where
            // this pays one, with the confidence unrounded (a member scored 12
            // is no longer quantized to zero).
            //
            // A key CLAIMED by an earlier row is not asked for again, and
            // batches are emitted in the order their first row created them:
            // rows are walked in page order, which on any page that can be
            // sorted is relevance order, so a subject two lists name is fetched
            // under the better-ranked of them — the same pointer [admit] will
            // file it under, and one fewer round trip than asking twice.
            val idBatches = LinkedHashMap<Any, Batch>()
            val keyBatches = LinkedHashMap<Any, Batch>()
            val claimedIds = HashSet<String>()
            val claimedKeys = HashSet<String>()
            for (row in planned.indices) {
                if (lensOfRow[row] != lens) continue
                val refs = planned[row]
                val rel = pointerRel(row)
                val text = pointerText(row)
                for ((weighted, ids) in refs.eventIds.filterNot { it in claimedIds }.splitByScored(refs)) {
                    claimedIds += ids
                    idBatches.batch(weighted, rel, text).take(ids, refs)
                }
                for ((weighted, pubKeys) in refs.pubKeys.filterNot { it in claimedKeys }.splitByScored(refs)) {
                    claimedKeys += pubKeys
                    keyBatches.batch(weighted, rel, text).take(pubKeys, refs)
                }
            }
            // PLANNED FIRST, SENT SECOND. Every lookup this lens needs is
            // listed before any of them goes out, for two reasons that both
            // used to be broken by sending them inline:
            //
            //  - THE BUDGET NOW CUTS A DEFINED PLACE. [SearchExpansionLimits.maxLookups] is spent in
            //    plan order, and plan order is page order — which on any page
            //    that can be sorted is relevance order — so a page too wide for
            //    the budget loses the subjects of its LOWEST-ranked pointers and
            //    never a higher one's. Sending inline made that true only by
            //    accident of loop nesting, and untested either way.
            //  - THE ROUND TRIPS OVERLAP. One batch per pointer is the shape a
            //    ranked page of Trusted Lists actually has (the identity below
            //    is the pointer's own relevance, and no two rows share one), so
            //    this was up to [SearchExpansionLimits.maxLookups] SEQUENTIAL engine queries on a
            //    single REQ — on the read path whose latency is already the
            //    thing being optimized.
            //
            // Results are merged in PLAN order, not completion order, which is
            // what keeps `putIfAbsent` deciding exactly what it decided when
            // these ran one after another: a subject two pointers name stays
            // filed under the better-ranked one. [mapBounded] preserves list
            // order for the same reason it is used here rather than
            // [forEachBounded], whose results arrive as they finish.
            val plan = ArrayList<Pair<Target, EventQuery>>()
            for (b in idBatches.values) {
                val q = b.query(under, profile, limits)
                for (chunk in b.keys.keys.chunked(LOOKUP_CHUNK)) {
                    if (spent++ >= limits.maxLookups) break
                    val narrowed = if (b.weighted) q.narrowIdWeights(chunk, b.keys) else q.narrowIds(chunk)
                    narrowed?.let { plan += Target.ID to it }
                }
            }
            for (b in keyBatches.values) {
                val q = b.query(under, profile, limits)
                for (chunk in b.keys.keys.chunked(LOOKUP_CHUNK)) {
                    if (spent++ >= limits.maxLookups) break
                    val narrowed = if (b.weighted) q.narrowProfileWeights(chunk, b.keys) else q.narrowProfiles(chunk)
                    narrowed?.let { plan += Target.KEY to it }
                }
            }

            // THE ADDRESSABLE SHAPE KEEPS THE BUCKETS. A coordinate is
            // (kind, author, d) and no single attribute holds it, so there is
            // no key to hang a weight on — a weighted recall needs one. The
            // members of a 30394 are therefore still grouped by quantized
            // confidence, and still cost a lookup per bucket per owner. The
            // `tag_index` array (`d:<value>`, fast-search) could carry weights
            // inside one owner's group if this family ever earns the work; on
            // the staging corpus it has no instances at all.
            for ((bucket, addresses) in bucketed(planned, lensOfRow, lens)) {
                val conf = if (bucket == null) under else under.withMember(profile, bucket, limits.confidenceGamma)
                for ((owner, addrs) in addresses.mapNotNull { Address.parse(it) }.groupBy { it.kind to it.pubKeyHex }) {
                    if (spent++ >= limits.maxLookups) break
                    conf.narrowAddresses(owner.first, owner.second, addrs.map { it.dTag })?.let { plan += Target.ADDRESS to it }
                }
            }

            // The plan, concurrently, folded back in plan order.
            val answers = plan.mapBounded(QUERY_FANOUT) { (_, q) -> recall(q) }
            plan.forEachIndexed { i, (target, _) ->
                answers[i].forEach { r ->
                    when (target) {
                        Target.ID -> byId.putIfAbsent(keys.idOf(r.hit), r)
                        Target.KEY -> byKey.putIfAbsent(keys.authorOf(r.hit), r)
                        Target.ADDRESS -> keys.addressOf(r.hit)?.let { byAddress.putIfAbsent(it, r) }
                    }
                }
            }
            out[lens] = Found(byId, byKey, byAddress)
        }
        return out
    }

    /**
     * ONE ROUND TRIP'S WORTH OF KEYS: everything that can be asked for in a
     * single query, with each key's own confidence.
     *
     * Two rows share a batch when they would send the SAME query, which is the
     * only thing that has to force them apart — the per-key weights never do,
     * since each key carries its own. Unscored references send the caller's
     * lens untouched, so they all share one; scored ones differ only in
     * [pointerRel], and not even in that when the floor is off.
     */
    private class Batch(
        val weighted: Boolean,
        val pointerRel: Double,
        val pointerText: Double,
    ) {
        /** Key -> the 0..100 confidence its pointer gave it; 0 and unread on an unscored batch. */
        val keys = LinkedHashMap<String, Int>()

        fun take(
            batched: List<String>,
            refs: References,
        ) {
            batched.forEach { keys[it] = refs.confidence[it] ?: 0 }
        }

        fun query(
            under: EventQuery,
            profile: String?,
            limits: SearchExpansionLimits,
        ): EventQuery =
            if (weighted) {
                under.withWeightedMember(profile, limits.confidenceGamma, pointerRel, pointerText, limits.subjectFloorSpan)
            } else {
                under
            }
    }

    /**
     * The batch this row's half belongs in — created on first use, so the map's
     * insertion order is page order and the best-ranked pointer keeps a
     * contested key.
     *
     * The identity is what the QUERY would carry: nothing at all for the
     * unscored, and for the scored either the pointer's relevance or, with the
     * floor off, one shared batch — [withWeightedMember] does not send the
     * relevance then, so splitting on it would buy identical queries.
     */
    private fun MutableMap<Any, Batch>.batch(
        weighted: Boolean,
        rel: Double,
        text: Double,
    ): Batch {
        val floored = weighted && limits.subjectFloorSpan != null
        // BOTH numbers are the identity now: two pointers that happen to share
        // a relevance may still have earned it differently (one on a title
        // match under a trusted signer, one on a weaker match under a better
        // one), and their members are placed by the TEXT half.
        val identity: Any = if (floored) listOf(rel, text) else weighted
        return getOrPut(identity) { Batch(weighted, if (floored) rel else 0.0, if (floored) text else 0.0) }
    }

    /**
     * One row's keys split into the SCORED and the UNSCORED, in that order,
     * dropping whichever half is empty.
     *
     * The two cannot share a lookup: a scored member is ranked on the member
     * rung by the number its list gave it, while a reference that expressed no
     * confidence "is as sure as the pointer itself" and must come back with NO
     * member score at all, so the placement can hand it the pointer's own. That
     * is the same split the buckets drew with a null key, and it is one query
     * each in the overwhelmingly common case where a list scores everybody or
     * nobody.
     */
    private fun List<String>.splitByScored(refs: References): List<Pair<Boolean, List<String>>> {
        val (scored, unscored) = partition { refs.weightOf(it) != null }
        return listOfNotNull(
            (true to scored).takeIf { scored.isNotEmpty() },
            (false to unscored).takeIf { unscored.isNotEmpty() },
        )
    }

    /**
     * This lens's COORDINATE references, grouped by quantized confidence.
     *
     * Ordered HIGHEST FIRST so that a member two lists disagree about is looked
     * up under the higher one — `putIfAbsent` above then keeps that first
     * answer. The generous reading is the right one for a disagreement between
     * two publishers the reader delegated: they both vouched, and the reader
     * asked for both.
     *
     * COORDINATES ONLY, because they are the only shape left that buckets. This
     * used to collect all three and hand back a `Shapes` holding each, from when
     * the buckets were how EVERY member reached its rung; the keyed shapes moved
     * to weighted batches ([Batch]) and their two sets became write-only. They
     * were not free: a page is walked here for every scored searching read, so a
     * list of 1,000 members cost 1,000 boxed bucket keys, hash lookups and
     * `LinkedHashSet` inserts that nothing ever read — and the addressable
     * family it all fed has no instances at all on the staging corpus. Buckets
     * that only ids or pubkeys created are gone with them, which changes
     * nothing: they reached the loop below with no addresses and issued no
     * query.
     */
    private fun bucketed(
        planned: List<References>,
        lensOfRow: IntArray,
        lens: Int,
    ): List<Pair<Double?, LinkedHashSet<String>>> {
        val out = HashMap<Double?, LinkedHashSet<String>>()
        planned.forEachIndexed { i, refs ->
            if (lensOfRow[i] != lens || refs.addresses.isEmpty()) return@forEachIndexed
            refs.addresses.forEach { out.getOrPut(bucketOf(refs.weightOf(it))) { LinkedHashSet() }.add(it) }
        }
        if (out.isEmpty()) return emptyList()
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

        fun take(r: Ranked<R>) {
            if (sent.add(keys.idOf(r.hit))) out.add(Taken(r.hit, r.score))
        }
        refs.eventIds.forEach { key -> found.byId[key]?.let(::take) }
        refs.pubKeys.forEach { key -> found.byKey[key]?.let(::take) }
        refs.addresses.forEach { key -> found.byAddress[key]?.let(::take) }
        return out
    }

    /** Which of [Found]'s three maps a planned lookup fills. */
    private enum class Target { ID, KEY, ADDRESS }

    private companion object {
        const val NO_LENS = -1

        /** [pickLens]'s two passes: a lens that names the pointer's kind outright, then one that merely converts into it. */
        val ACCEPT_PASSES = listOf(false, true)

        /** Keys per lookup query — a bound on one YQL `in` list, not on the answer. */
        const val LOOKUP_CHUNK = 500

        /**
         * Confidence steps one page may distinguish, FOR THE ADDRESSABLE SHAPE
         * ALONE — and, because a rank feature is a property of the QUERY, the
         * round trips it can cost: one lookup per occupied bucket per owner.
         *
         * THE KEYED SHAPES NO LONGER COME THROUGH HERE. A weighted recall
         * carries each member's confidence on its own key
         * ([EventQuery.authorWeights]), so a whole list is ONE query at the
         * publisher's own resolution — which is what quantizing was buying its
         * way out of. A coordinate is (kind, author, d) and no single attribute
         * holds it, so 30394 members have no key to hang a weight on and keep
         * the buckets; `tag_index` (`d:<value>`, fast-search) could carry them
         * inside one owner's group if that family ever earns the work.
         *
         * FOUR, not twenty, for the reason it always was: twenty gave 5%
         * resolution and let one well-scored list cost twenty round trips.
         * Measured against the Tapestry corpus, every confidence from 0.10 to
         * 1.00 placed a member in the SAME position relative to the page,
         * because the member band spans x7.3 while a page spans x367 — a page
         * that cannot resolve two ends of the range cannot resolve twentieths
         * of it. The floor is what undoes that argument for the keyed shapes: a
         * member is placed against its POINTER now, and a x367 page has room
         * for the difference between 0.10 and 1.00.
         *
         * Members that land in one bucket tie, and a stable sort then keeps
         * them in the order their list named them — which for a
         * descending-sorted list is the publisher's own ranking.
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

/**
 * The same lookup, asked to score what it finds as a member whose confidence
 * rides WITH ITS KEY, under a pointer this relevant.
 *
 * The two numbers arrive by different routes because they are different kinds
 * of fact. The confidence is per (list, member) and travels as a weight on the
 * key — one query for a whole list, at the publisher's own resolution. The
 * pointer's relevance is per query because the lookup is per pointer, which is
 * exactly what the weights bought.
 *
 * [profile] null means the finding query ranks on no ladder — a recency read, a
 * plain recall — so there is nothing for a member score to be comparable with,
 * and the lookup stays unranked. The splice then falls back to the pointer's own
 * order, as it always has.
 */
private fun EventQuery.withWeightedMember(
    profile: String?,
    gamma: Double,
    pointerRelevance: Double,
    pointerText: Double,
    floorSpan: Double?,
): EventQuery =
    if (profile == null) {
        this
    } else {
        copy(
            ranking = profile,
            rankFeatures =
                rankFeatures +
                    mapOf(EventYql.F_DOC_CONF to 1.0, "w_member_gamma" to gamma) +
                    // No span, no floor: the pointer's relevance is simply not
                    // sent, and the profile's own default of 0 leaves a subject
                    // on its rung exactly as it was before the floor existed.
                    (
                        floorSpan?.let {
                            mapOf(
                                EventYql.F_POINTER_REL to pointerRelevance.coerceAtLeast(0.0),
                                EventYql.F_POINTER_TEXT to pointerText.coerceAtLeast(0.0),
                                EventYql.F_SUBJECT_FLOOR_SPAN to it,
                            )
                        } ?: emptyMap()
                    ),
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

/**
 * [narrowIds] with each id's confidence attached — the same recall, scored per
 * document. The caller's own `ids` constraint still intersects, exactly as it
 * does unweighted: admission is the engine's job either way.
 */
private fun EventQuery.narrowIdWeights(
    chunk: List<String>,
    weights: Map<String, Int>,
): EventQuery? {
    val wanted = (if (ids.isEmpty()) chunk else chunk.filter { it in ids }).weighedBy(weights)
    return if (wanted.isEmpty()) null else copy(ids = emptyList(), idWeights = wanted)
}

/** [narrowProfiles] with each member's confidence attached. */
private fun EventQuery.narrowProfileWeights(
    chunk: List<String>,
    weights: Map<String, Int>,
): EventQuery? {
    if (!admitsKind(0)) return null
    val wanted = (if (authors.isEmpty()) chunk else chunk.filter { it in authors }).weighedBy(weights)
    // `ids` survives for the reason [narrowProfiles] keeps it.
    return if (wanted.isEmpty()) null else copy(kinds = listOf(0), authors = emptyList(), authorWeights = wanted)
}

/**
 * These keys with the 0..100 score their pointer gave them — quartz's own
 * scale, unrounded, which is also the integer scale a weighted recall takes.
 *
 * [weights] is the BATCH's map rather than one row's, because a batch pools the
 * members of every pointer that sends the same query — so a key must carry the
 * confidence ITS OWN list expressed, not the confidence of whichever list is
 * being read at the time.
 */
private fun List<String>.weighedBy(weights: Map<String, Int>): Map<String, Int> = mapNotNull { key -> weights[key]?.let { key to it } }.toMap()

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
