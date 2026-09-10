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
import com.nosfabrica.vespa.eventstore.engine.async.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.async.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.trust.Enrolment
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/** Whether this query recalls through a rank profile (its trust gates apply engine-side). */
internal fun EventQuery.isRanked(): Boolean = search != null || phrases.isNotEmpty() || ranking != null

/**
 * Whether the ENGINE's hit order is the serving order. Ranked queries keep
 * relevance order (NIP-50) — except the observer gate's profile, whose
 * order is defined as NIP-01 recency: the engine's created_at score order
 * is re-sorted client-side (engine score ties are arbitrary). That covers
 * `sort:recent` SEARCHES too — the point of the token is that its hits
 * come back in the same `created_at desc, id asc` order a plain filter's do.
 */
internal fun EventQuery.keepsEngineOrder(): Boolean = isRanked() && ranking != EventYql.RANK_RECENCY_GATED && ranking != EventYql.RANK_RECENCY_GATED_EXACT

/**
 * FROM A READ'S QUERIES TO THE PAGE IT SERVES: recall them all, put the union
 * in ONE order, splice in what the hits point at, and narrow the result to
 * what the caller actually asked for.
 *
 * Every decision here is about the ANSWER rather than about Nostr storage
 * rules, which is why it is no longer inside `NostrSemanticsStore`: which
 * order a mixed REQ gets, whether a page may carry a kind the filters did not
 * name, where a Trusted List's members sit relative to the list. The store
 * supplies the queries, the filters and a [Projection] — how to read an id, a
 * kind and an author off ITS row type, and how to recall one — and this
 * answers with the page.
 *
 * ONE implementation for both read paths. `query` serves `EventDoc`s and
 * `rawQuery` serves Quartz `RawEvent`s straight to the wire, and they must
 * agree on all of the above; before this they were two copies of the same
 * five-step pipeline, differing only in the row type.
 */
internal class PageAssembly(
    /** How much of a page may be expansion, and how much lookup it may cost. */
    private val limits: SearchExpansionLimits,
    /** The per-author cap on a RANKED page, or null for none — see `NostrSemanticsStore`'s `maxHitsPerAuthor`. */
    private val maxHitsPerAuthor: Int?,
    /** Whose declarations one observer may unpack; resolved at most once per observer, and only on the paths that consult the gate. */
    private val enrolments: suspend (String?) -> Enrolment,
) {
    /**
     * How to read one row type, and how to recall it — everything the pipeline
     * needs that differs between the document path and the raw path.
     */
    class Projection<R>(
        /** NIP-01 serving order over this row type: `created_at desc, id asc`. */
        val newestFirst: Comparator<R>,
        /** How the expansion reads a subject key (id / author / address) out of a row. */
        val keys: SubjectKeys<R>,
        val idOf: (R) -> String,
        val kindOf: (R) -> Int,
        val authorOf: (R) -> String,
        /** This row AS a pointer (a Trusted List, an assertion, a label), or null when it is not one. */
        val pointerOf: (R) -> Event?,
        val search: suspend (EventQuery) -> List<R>,
        val searchRanked: suspend (EventQuery) -> List<Ranked<R>>,
    )

    /**
     * The whole pipeline: expand, recall in one order, splice, narrow, cap.
     *
     * The steps are ordered the way they are for two reasons the comments
     * below carry in full — the splice needs the finished order AND the scores
     * that produced it, and the narrowing runs BEFORE the diversity cap so a
     * row about to be dropped cannot spend an author's slots.
     */
    suspend fun <R> serve(
        queries: List<EventQuery>,
        filters: List<Filter>,
        projection: Projection<R>,
    ): List<R> {
        val expansion = expansionOf(queries)
        val recalled =
            recallOrdered(
                queries + companionsOf(expansion, queries),
                projection.newestFirst,
                projection.idOf,
                projection.search,
                projection.searchRanked,
                expansion != null,
            )
        return spliced(expansion, recalled, projection.keys, projection.pointerOf, projection.searchRanked)
            .asked(expansion, filters, projection.kindOf, projection.idOf, projection.authorOf)
            .diverse(queries.all { it.keepsEngineOrder() }, projection.authorOf)
    }

    /**
     * The reference expansion for this read, or null where nothing can ever
     * expand: the feature switched off, or no query carrying TERMS. Terms, not
     * "carries a search field" — every anonymous read on a lens-requiring
     * relay stamps `include:spam`, and a mirror's paging carries it too, so
     * gating on the field would put all of that traffic behind an expansion
     * none of it asked for.
     *
     * Created BEFORE the recall, because the expansion now shapes it: a
     * searching read also recalls the pointer kinds it would otherwise miss
     * ([SearchReferenceExpansion.companions]) — the ones a kind-restricted
     * query cannot return at all, and the enrolled signers' declarations an
     * unrestricted one ranks below its own page. Without that, the lists,
     * assertions and labels whose text matched are never returned, and neither
     * this store nor the client ever learns there was anything to unpack.
     *
     * The GATE READ STAYS LAZY. [ProviderMap] never caches an empty pass, so
     * on a relay holding no 10040s the delegations read is one small engine
     * query every time it is asked — which is why the enrolment goes in as a
     * supplier the expansion resolves at most once, and only on the paths
     * that consult the gate: building a declaration companion, or meeting a
     * declaration pointer in the page. An anonymous read resolves to
     * [Enrolment.NONE] without ever querying, as before, and a termless one
     * never reaches the gate at all. An UNRESTRICTED-kind read with an
     * observer now does resolve it — it builds a declaration companion like
     * any other searching read — which is one small query per such read on a
     * relay with no 10040s to cache, the price of the reader's own lists
     * reaching a page they were ranked off.
     */
    private fun expansionOf(queries: List<EventQuery>): SearchReferenceExpansion? {
        if (!limits.enabled) return null
        val searching = queries.filter { it.search != null || it.phrases.isNotEmpty() }
        if (searching.isEmpty()) return null
        // An ANONYMOUS read can unpack no declaration at all, and asking the
        // gate to say so would cost a provider-list query on a relay that holds
        // no Treasure Maps — where the answer is never cached, by design. Labels
        // are ungated, so the expansion still runs; it just runs with nothing
        // enrolled.
        //
        // PER OBSERVER, never pooled: the supplier takes the lens's own observer
        // and the expansion memoizes per key, so one filter's `observer:` can
        // never unpack a declaration only the filter beside it enrolled. The
        // resolution is the same one either way — `delegations()` caches the
        // parsed Maps, and `of()` is a pure fold over that cache — so a
        // two-lens read costs a second fold, not a second query.
        return SearchReferenceExpansion(searching, enrolments, limits)
    }

    /**
     * [SearchReferenceExpansion.companions] minus any query the caller already
     * sent: a REQ can legitimately carry the very filter a companion would
     * duplicate (`kinds:[1]` beside `kinds:[1985]`, same terms), and running
     * the identical query twice buys nothing the id-dedup doesn't already
     * guarantee.
     */
    private suspend fun companionsOf(
        expansion: SearchReferenceExpansion?,
        queries: List<EventQuery>,
    ): List<EventQuery> = expansion?.companions()?.filterNot { it in queries } ?: emptyList()

    /**
     * Recall every query concurrently (bounded), dedup across queries, and
     * order the result — ONE order over the union, not one order per filter.
     *
     * NIP-50 asks for relevance order and NIP-01 for recency, and a REQ can
     * carry both kinds of filter at once, so there are three cases:
     *
     *  - **No query ranked.** The union is sorted `created_at desc, id asc` —
     *    the NIP-01 order, applied across filters.
     *  - **Every query ranked, by the SAME profile.** The union is merged on the
     *    engine's relevance, ties broken by recency: the filters are several
     *    ways of asking one question, and their answers belong in one order.
     *    It costs the scores, which is why [EventIndex.searchRanked] exists.
     *  - **Mixed, or ranked by different profiles.** Each query's hits keep
     *    their own order and the runs are concatenated. A relevance score and a
     *    timestamp share no scale, and neither do two profiles' scores, so
     *    interleaving them would be inventing a comparison. The honest floor,
     *    not a good answer — a client that wants one order should ask one
     *    question.
     *
     * Dedup is by id and keeps the BEST copy: sorting before [distinctBy] means
     * an event that answered two filters survives at its higher score.
     */
    private suspend fun <R> recallOrdered(
        queries: List<EventQuery>,
        newestFirst: Comparator<R>,
        idOf: (R) -> String,
        searchOne: suspend (EventQuery) -> List<R>,
        searchRankedOne: suspend (EventQuery) -> List<Ranked<R>>,
        /**
         * Whether the CALLER needs the per-hit relevance kept — the splice does,
         * to place a subject by the confidence its pointer expressed.
         *
         * IT COSTS NOTHING ON A RANKED QUERY, which is the only kind it applies
         * to: `recallSummaries` already goes through `rankedHits` there, one
         * `recallRoot` call, and the ranked path differs only in keeping the
         * `relevance` Vespa already returned. What the single-query fast path
         * avoids is wrapping every hit of an ORDINARY REQ — and an ordinary REQ
         * is recency-ordered, so `keepsEngineOrder()` sends it down the
         * score-free branch regardless of this flag.
         */
        wantScores: Boolean = false,
    ): Page<R> {
        if (queries.isEmpty()) return Page(emptyList(), null)
        // One filter is the ordinary REQ and never needs a score: its engine
        // order IS the answer, and asking for scores would wrap every hit on
        // the hottest read a relay serves.
        if (queries.size == 1) {
            if (wantScores && queries[0].keepsEngineOrder()) return Page.of(searchRankedOne(queries[0]))
            val hits = searchOne(queries[0])
            return Page(if (queries[0].keepsEngineOrder()) hits else hits.sortedWith(newestFirst), null)
        }
        if (queries.none { it.keepsEngineOrder() }) {
            return Page(
                queries
                    .mapBounded(QUERY_FANOUT) { searchOne(it) }
                    .flatten()
                    .distinctBy(idOf)
                    .sortedWith(newestFirst),
                null,
            )
        }
        // [EventYql.profileOf], not `ranking`: the field is null for every
        // ordinary search and the profile is picked from the query's shape, so
        // two filters where only one carries `observer:` read as "same profile"
        // by the field while running on `search` and `text` — two scales,
        // interleaved.
        if (queries.all { it.keepsEngineOrder() } && queries.mapTo(HashSet()) { EventYql.profileOf(it) }.size == 1) {
            val scored = queries.mapBounded(QUERY_FANOUT) { searchRankedOne(it) }.flatten()
            // An engine that does not rank (the in-memory reference) reports a
            // null rather than a fabricated constant; its hits are already
            // newest-first, so recency is the merge that keeps them coherent.
            if (scored.any { it.score == null }) {
                return Page(
                    scored
                        .map { it.hit }
                        .distinctBy(idOf)
                        .sortedWith(newestFirst),
                    null,
                )
            }
            return Page.of(
                scored
                    .sortedWith(compareByDescending<Ranked<R>> { it.score }.thenBy(newestFirst) { it.hit })
                    .distinctBy { idOf(it.hit) },
            )
        }
        val results = queries.mapBounded(QUERY_FANOUT) { searchOne(it) }
        val ordered = queries.zip(results).flatMap { (q, hits) -> if (q.keepsEngineOrder()) hits else hits.sortedWith(newestFirst) }
        // Two scales already, which is why these runs are concatenated rather
        // than merged — so there is no coherent score to carry out of here.
        return Page(ordered.distinctBy(idOf), null)
    }

    /**
     * ONE ORDERED PAGE, AND THE RELEVANCE BEHIND IT — [scores] index-aligned
     * with [hits], or NULL for the pages that have none: a recency-ordered
     * recall, two ranking profiles concatenated, an engine that does not rank.
     *
     * A nullable parallel list rather than a `List<Ranked<R>>` because the
     * unscored page is the hot one. A plain NIP-01 recall, a mirror's paging and
     * a NIP-77 catch-up all land here, and wrapping every hit of those in a
     * score-carrying object — then unwrapping it again in [spliced] — would be
     * two copies of the page and an allocation per event to carry a null.
     */
    private class Page<R>(
        val hits: List<R>,
        val scores: List<Double?>?,
        /**
         * The TEXT band behind each score — `Ranked.textScore`, index-aligned
         * with [hits] and null wherever [scores] is. The splice places a
         * Trusted List's member by the band the LIST earned times what the list
         * says about that MEMBER, so it needs the pointer's text apart from the
         * signer's trust that [scores] multiplies in.
         */
        val texts: List<Double?>? = null,
    ) {
        companion object {
            fun <R> of(ranked: List<Ranked<R>>) = Page(ranked.map { it.hit }, ranked.map { it.score }, ranked.map { it.textScore })
        }
    }

    /**
     * THE ORDERED PAGE, PLUS WHAT IT POINTS AT — a label's subject behind the
     * label, a Trusted List's members behind the list.
     *
     * Runs after [recallOrdered] rather than inside it, but over the [Page] it
     * produced rather than over a bare list: a scored member is placed by the
     * relevance the ENGINE gave it on the member rung, and its pointer rises to
     * sit just above the best of them, so the placement needs both the finished
     * order AND the scores that produced it. Splicing inside the recall would
     * have to answer that question once per ordering case; here it is answered
     * once.
     *
     * Returns the page's hits UNTOUCHED — the same list, not a copy — whenever
     * [expansion] is null, which is every plain recall this store serves
     * (see [expansionOf] for what qualifies). There is no cheaper early-out
     * left to take here: since the conversion recall, EVERY searching read can
     * splice — a kind-restricted one reaches its pointers through the
     * companion queries — so "can serve no pointer kind" no longer exists as
     * a shape.
     */
    private suspend fun <R> spliced(
        expansion: SearchReferenceExpansion?,
        page: Page<R>,
        keys: SubjectKeys<R>,
        pointerOf: (R) -> Event?,
        recall: suspend (EventQuery) -> List<Ranked<R>>,
    ): List<R> {
        val hits = page.hits
        if (expansion == null || hits.isEmpty()) return hits
        val expanded = expansion.expand(hits, page.scores, page.texts, keys, pointerOf, recall)

        // THE POINTER'S OWN ORDER FIRST, always — the sort below is a stable
        // re-sort of it, so a tie between a subject and its own pointer resolves
        // the only way it can read: the reason above the result. It is also the
        // answer whenever there are no scores to sort by.
        val placed = ArrayList<Placed<R>>(hits.size)
        hits.forEachIndexed { i, hit ->
            val pointer = page.scores?.get(i)
            // THE POINTER RISES TO ITS BEST MEMBER — it does not hold them down.
            //
            // "A reason cannot rank below the thing it explains" is still the
            // invariant, and this is the direction that satisfies it without
            // deciding the page. The other direction — clamping each member to
            // its pointer's score — made the SIGNER's trust the ceiling for
            // everyone the list names, and a trust service is a key nobody
            // follows: on the staging relay a `Verified Human` list signed by a
            // service scored 26 pinned sixteen members scored 65..100 to one
            // number (550 x wot(26)), so they came back in the publisher's tag
            // order, three orders of magnitude below their own relevance,
            // beneath organic hits from authors trusted 7. Member trust and
            // publisher confidence — the two things `event.sd` §13 computes —
            // could not move a member at all.
            //
            // Lifting instead keeps the pill row's reading exactly: the raised
            // score is a MAX over the pointer's own members, so no subject can
            // pass it, and a tie between the pointer and its best member
            // resolves to the pointer because the stable sort sees it first.
            //
            // Only SCORED members lift, which is why a label is untouched by
            // this: it expresses no confidence, none of its subjects is fetched
            // under a member profile, `lifted` collapses to `pointer`, and the
            // placement is bit-identical to before.
            //
            // A LOOP, not filterNotNull().maxOrNull(): this runs for every hit
            // of every scored page, and the overwhelming majority of them carry
            // no subjects at all — a throwaway ArrayList per row to reduce an
            // empty list is the wrong price for a page of 500.
            val lifted =
                if (pointer == null) {
                    null
                } else {
                    var best: Double = pointer
                    for (score in expanded.scores[i]) {
                        if (score != null && score > best) best = score
                    }
                    best
                }
            if (expanded.fresh[i]) placed.add(Placed(hit, lifted))
            expanded.subjects[i].forEachIndexed { j, subject ->
                val own = expanded.scores[i][j]
                placed.add(
                    Placed(
                        subject,
                        when {
                            // NO RUNG, SO NO MOVE — it sits WITH its pointer.
                            // A reference that expressed no confidence (a
                            // NIP-32 label, a NIP-85 assertion) was never
                            // fetched under the member profile, so it takes the
                            // pointer's score and a stable sort puts it right
                            // behind it. That is the placement those two
                            // families have always had, and it is right:
                            // neither claim is probabilistic, so there is no
                            // doubt for a rung to express.
                            //
                            // The LIFTED score, not the raw one, because
                            // adjacency is the whole point of this branch. On a
                            // list mixing scored and unscored members the raw
                            // pointer score would strand the unscored ones
                            // where the block used to be — on the staging
                            // numbers, ~340x below the siblings they were named
                            // beside. For a label, which has no scored member
                            // to lift anything, `lifted` IS `pointer` and this
                            // is bit-identical to before.
                            own == null -> lifted

                            // An UNSCORED pointer on a scored member is still a
                            // null, exactly as it was under the ceiling: it is
                            // the signal that this page cannot be sorted at all
                            // and must keep the pointer's own order. Answering
                            // `own` here would let one row's missing score turn
                            // a fallback page into a sorted one.
                            pointer == null -> null

                            // THE ENGINE'S OWN NUMBER, unclamped — see the
                            // lift above for what used to happen here.
                            else -> own
                        },
                    ),
                )
            }
        }
        // ONE SCALE, THE ENGINE'S. Hits carry the relevance their rank profile
        // gave them; members carry the relevance a MEMBER profile gave them on
        // the same ladder (event.sd §13), so the two sort together without this
        // class doing arithmetic on either. Nothing to normalize, nothing to
        // shift: a number computed here could only ever be a guess about a
        // scale the engine owns, and the guess is what broke.
        //
        // A page missing any score cannot be sorted at all — the in-memory
        // reference reports null rather than fabricating a constant, and a
        // recency-ordered read has no relevance to give — so it keeps the
        // pointer's own order.
        if (placed.any { it.score == null }) return placed.map { it.row }
        return placed.sortedByDescending { it.score }.map { it.row }
    }

    /** A row and the relevance the ENGINE placed it by — its rank profile's for a hit, the member profile's for a subject. */
    private class Placed<R>(
        val row: R,
        val score: Double?,
    )

    /**
     * The page narrowed to what the caller's filters ADMIT — the same list,
     * not a copy, whenever nothing expanded or every row passes, which is
     * every plain recall and every search whose expansion added only rows the
     * REQ's filters match.
     *
     * A kind-restricted search recalls MORE than its own kinds on purpose: the
     * pointer families that convert into them are fetched as companion queries
     * ([SearchReferenceExpansion.companions]), because a label, assertion or
     * Trusted List is the only route to the subjects it names. That is a recall
     * device, and it stops at recall. A REQ that asked for `kinds:[0]` asked a
     * NIP-01 question with a NIP-01 answer, and a 30382 on that page is a kind
     * the client said it did not want — it has no parser for it, it did not
     * budget a slot for it, and on a relay it is a protocol violation rather
     * than a bonus. So the pointer does its job (it names subjects, and those
     * subjects ARE of an asked-for kind) and is then dropped from the answer.
     *
     * JUDGED ON KINDS, IDS AND AUTHORS — the three exact keys — each row
     * against ANY filter, since a REQ ORs its filters and answers with one
     * page. This used to be a kinds-only check that treated a filter with no
     * `kinds` as admitting every kind, which served a companion pointer to a
     * REQ like `[{kinds:[0], search:…}, {ids:[e1]}]` — the second filter has
     * no kinds but admits exactly one event, and the 30392 matched neither.
     * Not tags or the time window, deliberately: the engine matches tag values
     * uncased and a client-side matcher would not, so re-judging those here
     * could drop a hit the engine rightly served; the exact keys have one
     * answer on both sides. Everything else the expansion nominates was looked
     * up under the finding query with its terms stripped, so it passed the
     * same keys the hits did and passes here too; the pointer kinds are the
     * one constraint the companions deliberately step outside of.
     */
    private fun <R> List<R>.asked(
        expansion: SearchReferenceExpansion?,
        filters: List<Filter>,
        kindOf: (R) -> Int,
        idOf: (R) -> String,
        authorOf: (R) -> String,
    ): List<R> {
        if (expansion == null) return this
        val keys = filters.map { Triple(it.kinds?.toSet(), it.ids?.mapTo(HashSet()) { id -> id.lowercase() }, it.authors?.mapTo(HashSet()) { a -> a.lowercase() }) }

        fun admitted(row: R): Boolean =
            keys.any { (kinds, ids, authors) ->
                (kinds == null || kindOf(row) in kinds) && (ids == null || idOf(row) in ids) && (authors == null || authorOf(row) in authors)
            }
        // ONE PASS, and no copy unless something is actually dropped. `all`
        // then `filter` judged every surviving row TWICE — and `admitted` is a
        // scan of every filter's key sets per row — on a path that already runs
        // per searching read.
        // An index counted by hand, NOT `withIndex()`: that allocates an
        // IndexedValue per row, which is a worse trade than the second pass it
        // was replacing on the page that admits everything — the common one.
        var kept: ArrayList<R>? = null
        var seen = 0
        for (row in this) {
            if (admitted(row)) {
                kept?.add(row)
            } else if (kept == null) {
                kept = ArrayList<R>(size).also { it.addAll(subList(0, seen)) }
            }
            seen++
        }
        return kept ?: this
    }

    /**
     * The page with no author holding more than [maxHitsPerAuthor] rows —
     * the same list, not a copy, whenever the cap is off or nothing exceeds it,
     * which is every read on a default store.
     *
     * STABLE and FIRST-WINS: the rows an author keeps are the ones the ranking
     * put highest, and everything else stays exactly where it was. A spliced
     * member cannot be dropped by this in practice — a person has one profile,
     * so one row — which is the right asymmetry: the cap exists to stop one
     * author's BULK from taking a page, not to ration the people a list names.
     */
    private fun <R> List<R>.diverse(
        /**
         * Read off the QUERIES, not off whether the page came back with scores.
         * The two differ on an engine that does not rank — the in-memory
         * reference reports a null score per hit — and which pages an operator
         * capped must not depend on which engine answered them.
         */
        ranked: Boolean,
        authorOf: (R) -> String,
    ): List<R> {
        val cap = maxHitsPerAuthor ?: return this
        if (!ranked || size <= cap) return this
        val seen = HashMap<String, Int>()
        var dropped = false
        val kept = ArrayList<R>(size)
        for (hit in this) {
            val n = seen.merge(authorOf(hit), 1, Int::plus)!!
            if (n <= cap) kept.add(hit) else dropped = true
        }
        return if (dropped) kept else this
    }
}
