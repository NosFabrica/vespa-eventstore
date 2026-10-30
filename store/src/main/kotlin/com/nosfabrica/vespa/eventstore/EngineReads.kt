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

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.LinkScore
import com.nosfabrica.vespa.eventstore.engine.LinkTarget
import com.nosfabrica.vespa.eventstore.engine.LinkWeights
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery

/**
 * THE ENGINE, READ DIRECTLY — under the trust projection and under the meter,
 * for the two jobs that genuinely want that and nothing else.
 *
 *  - **What is actually stored**, as opposed to what a lens would serve: the
 *    orphan-score sweep's own checks count 30382s the trust view hides.
 *  - **What a rank profile does**, measured: the rank-quality harness names a
 *    profile per query and compares the orders, which is a question about the
 *    engine and not about the store's answer.
 *  - **Whether an id is already held**, before paying to verify it: an ingest
 *    that skips a duplicate wants membership in the corpus, which is a
 *    different question from whether the connecting user could see it.
 *
 * READ-ONLY, and that is the point of the type. This used to be the concrete
 * [VespaEventIndex] hanging off the front door, which handed every consumer
 * `put`, `putAll`, `removeDocs` and a settable `trustDescent` — a way to write
 * to the index behind every rule the store exists to enforce (deletion guards,
 * supersession, the trust projection's reactions), reachable by autocomplete.
 * Nothing needed that; the read shapes below did.
 *
 * TWO KINDS OF READ, metered differently on purpose:
 *
 *  - The HARNESS reads ([search], [searchRanked], [count], [scoreLinks]) are
 *    NOT METERED: they appear in no activity on `VespaEventStore.metrics()`,
 *    because the meter is a decorator one layer up, and a rank-quality run
 *    measuring a profile should not book itself as the store's traffic.
 *  - The PRODUCTION probes ([existingIds], [newestVersions]) a consumer's
 *    ingest runs before every batch go THROUGH the meter, booked to
 *    [Activity.BatchInsert] — they are the admission probes of the write that
 *    follows, made a step early. They were unmetered when this type only had
 *    [existingIds], so a mirror's dedup traffic, the hottest read it makes,
 *    showed on the pulse page as nothing at all.
 *
 * A consumer's production path that needs another engine read is a missing
 * member HERE, never a reason to reach for the concrete client: this type is
 * where the schema's field names stop.
 */
class EngineReads internal constructor(
    private val index: VespaEventIndex,
    /** The same engine under the meter (`MeteredEventIndex`), for the production probes. */
    private val metered: EventIndex,
) {
    /** Documents matching [query], with no lens applied. */
    suspend fun search(query: EventQuery): List<EventDoc> = index.search(query)

    /** [search], keeping the relevance the query's rank profile gave each hit. */
    suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = index.searchRanked(query)

    /** How many documents [query] matches — exact, and un-gated. */
    suspend fun count(query: EventQuery): Int = index.count(query)

    /**
     * Each of [targets]' links among the documents [linkers] serves, per person and
     * scored by [weights] (see `EventIndex.scoreLinks`) — what the `sort:links`
     * rerank weighed, read back by the harness that reports why a hit moved.
     */
    suspend fun scoreLinks(
        targets: List<LinkTarget>,
        linkers: EventQuery,
        weights: LinkWeights,
    ): Map<String, LinkScore> = index.scoreLinks(targets, linkers, weights)

    /**
     * Which of [ids] the engine already holds — a pure membership read, and
     * the cheapest one the index offers (an id-only summary, no documents).
     *
     * UN-LENSED ON PURPOSE, which is what makes it right for a dedup gate: an
     * event this store already holds must be skipped whether or not the trust
     * view would serve it, and a lensed answer would re-admit everything the
     * gate hides. It follows that a hit here is NOT a promise that a read will
     * return the event — only that writing it again would be wasted work.
     */
    suspend fun existingIds(ids: List<String>): Set<String> = withActivity(Activity.BatchInsert) { metered.existingIds(ids) }

    /**
     * The stored winner of each `(kind, author)` address among [authors] — its
     * id and `created_at` under NIP-01's rule (highest `created_at`, ties to
     * the lowest id), the version an arriving replaceable has to BEAT to be
     * stored. Authors with nothing stored are absent. See
     * `EventIndex.newestVersions` for the contract; [kind] must be a plain
     * replaceable (0, 3, 10000-19999), anything else is refused.
     *
     * What a pre-verification drop needs and no more: one attribute-only
     * grouping per 500 authors, no document read, the tiebreak applied by the
     * engine that also applies it on write — so a consumer holding this
     * cannot drift from the rule that decides what is stored. Un-lensed and
     * expiry-blind like [existingIds], for the same reason: "would writing
     * this be wasted work" is a question about the corpus. Read before the
     * writer lock, so it is a hint: a version stored between this read and the
     * write is still caught by the store's own supersession.
     */
    suspend fun newestVersions(
        kind: Int,
        authors: List<String>,
    ): Map<String, DocRef> = withActivity(Activity.BatchInsert) { metered.newestVersions(kind, authors) }
}
