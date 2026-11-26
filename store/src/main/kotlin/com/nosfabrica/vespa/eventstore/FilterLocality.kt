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
package com.nosfabrica.vespa.eventstore

import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.toEventQuery
import com.nosfabrica.vespa.eventstore.search.LinkRerank
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent

/**
 * WHICH WRITES AND WHICH SEARCHES LEAVE A CACHED NEG-OPEN SNAPSHOT VALID — this store's answer
 * to quartz's `LiveEventStore`, which a relay passes through unchanged as quartz 2.28.0's
 * five-argument constructor's last two arguments:
 * `LiveEventStore(store, ingest, LiveEventStore.DEFAULT_MAX_HELD_BEFORE_EOSE,
 * FilterLocality::writeIsFilterLocal, FilterLocality::searchIsFilterLocal)`.
 *
 * Quartz keeps a NEG-OPEN's sealed snapshot across a write that its store rules say cannot
 * change the filter's set (the write matches no filter, supersedes or deletes nothing the filter
 * could hold). That reasoning is about the NIP-01 fields only; what this store answers ALSO
 * follows stored state a write feeds (the observer gate's lens and cells) and, for a search,
 * whatever the text engine reads. These two predicates are the store's statement of exactly
 * that, and they live here because only the store knows it: vespa-relay carried its own copy of
 * the write half, derived from [NostrSemanticsStore]'s `touchesTrust`, which is the drift
 * AGENTS.md "What goes where" exists to stop.
 *
 * The two are ONE contract and must be changed together: every search shape
 * [searchIsFilterLocal] accepts reads nothing stored but the events its NIP-01 fields select and
 * the trust gate's inputs, and [writeIsFilterLocal] is false for every write that can change
 * those inputs. A token that starts reading anything else (a follow list, a report, a NIP-05,
 * corpus statistics) is rejected here, or the writes that feed it are.
 *
 * What neither can see, and quartz's snapshot TTL (30 s in 2.28.0) is the only bound on: state
 * that moves WITHOUT a write through this relay — another process feeding the same index
 * ([NostrSemanticsStore]'s `writers`; the provider map is then rebuilt on an interval), a 10040's
 * deferred trust walk draining after the write's `OK` (`DirtLedger`), an expiring card swept
 * by the clock. Each was already true of a plain gated snapshot before search snapshots were
 * kept; nothing here widens it.
 */
object FilterLocality {
    /**
     * Quartz's `writeIsFilterLocal`: false for a write that can change what the trust gate lets
     * through, which drops every cached snapshot; true for everything else, whose effect on a
     * snapshot quartz's own store-rules scope then decides.
     *
     * - A card (30382) moves a cell on its subject's reputation, and a provider list (10040)
     *   decides whose cards count and which service is its owner's lens:
     *   [TrustProjection.TRUST_KINDS], the kinds the projection reacts to. Both the rank cells
     *   (the gate, `sort:rank`) and the follower cells (`sort:followers`) come from cards.
     * - A vanish (62) can remove either.
     * - A deletion (5) can remove either, unless it names only `a` addresses of OTHER kinds:
     *   `Deletions.applyDeletion` deletes an `e` target whatever its kind, so an id says
     *   nothing, while an address carries its kind. An address whose kind does not parse is
     *   treated as a trust one.
     *
     * Follow lists, reports, profiles, labels, other assertions and Trusted Lists are true: no
     * shape [searchIsFilterLocal] accepts reads them (the reference expansion, which reads labels
     * and lists, runs only on a read carrying terms; the `sort:links` rerank, which reads
     * reactions, zaps and reports, is rejected there).
     *
     * Dispatched on the KIND, never `is DeletionRequestEvent`: a relay may hand over an event its
     * quartz parsed as a base [Event]. Called once per accepted write before its `OK`: no I/O.
     */
    fun writeIsFilterLocal(event: Event): Boolean =
        when (event.kind) {
            in TrustProjection.TRUST_KINDS, RequestToVanishEvent.KIND -> false
            DeletionRequestEvent.KIND -> deletesNoTrustDocument(event)
            else -> true
        }

    /**
     * Quartz's `searchIsFilterLocal` (2.28.0), asked once per search filter of a NEG-OPEN the
     * cache does not hold, never per write: true when [filter]'s `search` only narrows the set its
     * other fields select — by facts in each event, or by trust state [writeIsFilterLocal]
     * guards — and never makes the set depend on other writes. Then a snapshot of it is
     * reasoned about from its NIP-01 fields, as a plain filter's is.
     *
     * Read off the query the store compiles ([toEventQuery], the same one the NEG-OPEN walk
     * runs through `toExpiryQuery`), so this and the walk cannot read the grammar differently:
     *
     * - No search (null or blank): true. Quartz does not ask, but the answer is total.
     * - TEXT — terms or a quoted phrase, whatever else rides along: false. The walk runs it as a
     *   ranked search (`index.search`), and the match set is not a per-document fact: the
     *   `search` profile DELETES hits below a score floor (`rank-score-drop-limit`,
     *   `text_score_cutoff`) built from bm25, whose IDF moves with every document written
     *   anywhere in the corpus (event.sd: "Corpus-dependent (bm25 scales with corpus
     *   statistics)"), and `perfect_share()` reads `num_docs_indexed`. A limit then cuts by that
     *   same score. A write the filter's fields do not match can move both. The REQ of the same
     *   filter also reads other events outright: the reference expansion, and the topical-intent
     *   probe (`search/QueryIntent`), which asks whether a trusted profile or app is NAMED the
     *   words and ranks the page differently when one is.
     * - `sort:links[:x]`: false. The rerank widens the recall and cuts it back by the links
     *   (reactions, zaps, reports) other authors wrote — unguarded writes. A guard more than a
     *   need today: with text the filter is already false above, and a TERMLESS one is a recency
     *   read neither the walk nor the REQ reranks (the rerank needs a relevance-ordered page;
     *   `ReadShapeTest` pins that). Should the rerank ever reach a recency page, this keeps the
     *   two from drifting into different answers by this predicate.
     * - A termless trust sort (`sort:rank`, `rank:asc`, `followers`, `text`) WITH a limit:
     *   false. Its first N are its most trusted, not its newest ([EventYql.walksInOrder]), so the
     *   walk serves a ranked search page whose cut falls inside ties (ranks are 0..100 integers;
     *   `sort:text` with no text scores everything alike) that the engine resolves by its own
     *   document order, not by anything in the events.
     * - Every other termless shape: true. Its walk is created_at-ordered (plain recall, the
     *   observer gate's `recency_gated`, `sort:recent`), so a limit is the newest N of a gated
     *   set and quartz's "a write that matches no filter changes no filter's top N" holds; or it
     *   has no limit, and an unlimited walk of a trust sort is exactly its gated set
     *   ([EventYql.walksGated]). That covers `include:spam` (the floor waived: the plain set),
     *   `observer:` and `filter:rank:` (gated on rank cells only), `-word` exclusions (exact
     *   per-document terms), `language:` (it adds a stemmed column to the TEXT clause only, so
     *   with no text it compiles to nothing), and every extension the store ignores — `domain:`,
     *   `nsfw:`, `sentiment:`, an unknown `sort:`, any other `key:value` — which compile to
     *   the query the filter makes without them (`FilterLocalityTest` holds that, so a token the
     *   store starts honouring fails there until it is classified here).
     * - A filter that can never match ([toEventQuery] null): true; its set is empty for good.
     */
    fun searchIsFilterLocal(filter: Filter): Boolean {
        if (filter.search.isNullOrBlank()) return true
        if (LINKS.requested(listOf(filter)) != null) return false
        val q = filter.toEventQuery() ?: return true
        if (q.search != null || q.phrases.isNotEmpty()) return false
        return q.limit == null || EventYql.walksInOrder(q)
    }

    /** Only to read the `sort:links` token exactly as the store's own rerank does. */
    private val LINKS = LinkRerank()

    private fun deletesNoTrustDocument(deletion: Event): Boolean =
        deletion.tags.none { it.size > 1 && it[0] == "e" } &&
            deletion.tags.none { it.size > 1 && it[0] == "a" && it[1].substringBefore(':').toIntOrNull().let { kind -> kind == null || kind in TrustProjection.TRUST_KINDS } }
}
