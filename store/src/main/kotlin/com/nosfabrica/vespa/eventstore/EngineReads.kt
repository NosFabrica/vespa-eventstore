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
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventSelection

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
 *  - **The whole corpus, walked**: a mirror fed by `open(observers = …)`
 *    diffs id windows against it ([visitIds]) and bulk-dumps it
 *    ([visitDocsPage]) to reconcile the batches an observer missed — and a
 *    mirror is of what is STORED, not of what some lens would serve.
 *
 * READ-ONLY, and that is the point of the type. This used to be the concrete
 * [VespaEventIndex] hanging off the front door, which handed every consumer
 * `put`, `putAll`, `removeDocs` and a settable `trustDescent` — a way to write
 * to the index behind every rule the store exists to enforce (deletion guards,
 * supersession, the trust projection's reactions), reachable by autocomplete.
 * Nothing needed that; the read shapes below did.
 *
 * NOT METERED: reads made here appear in no activity on
 * `VespaEventStore.metrics()`, because the meter is a decorator one layer up.
 * That is right for a health count and wrong for anything that walks the
 * corpus, which will make the store look idle while Vespa is busy. Prefer the
 * `IEventStore` surface for real reads.
 */
class EngineReads internal constructor(
    private val index: VespaEventIndex,
) {
    /** Documents matching [query], with no lens applied. */
    suspend fun search(query: EventQuery): List<EventDoc> = index.search(query)

    /** [search], keeping the relevance the query's rank profile gave each hit. */
    suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = index.searchRanked(query)

    /** How many documents [query] matches — exact, and un-gated. */
    suspend fun count(query: EventQuery): Int = index.count(query)

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
    suspend fun existingIds(ids: List<String>): Set<String> = index.existingIds(ids)

    /**
     * Stream every match's (id, created_at[, d tag]) in engine-defined order,
     * a page at a time — the id-window diff a mirror reconciles with. [onPage]
     * returns whether to CONTINUE. The client pages a created_at cursor over
     * the attribute-only id summary — no document is read — so it holds one
     * page in memory however large the corpus. [withDTag] also projects the `d`
     * tag an addressable-corpus diff keys on. UN-METERED like everything here,
     * so a full walk makes the store look idle while Vespa is busy (see the
     * class KDoc).
     */
    suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean = false,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) = index.visitIds(query, withDTag, onPage)

    /**
     * One page of FULL documents from a resumable, exhaustive walk (the
     * engine's document-API visit) — the bulk dump. Pass the previous page's [DocsPage.continuation] as [resumeFrom]
     * (null starts the walk; a null continuation back means it is complete).
     * O(page) memory, and resumable across a restart because the continuation
     * is a plain string the caller may persist.
     *
     * The query must be one a document selection can express (kinds, authors,
     * since/until — [EventSelection]): anything else (ids, tags, a limit,
     * search) would fall back to the port's search-based default, which is
     * capped — a dump that silently ends early. That is refused here instead.
     */
    suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage {
        require(EventSelection.build(query) != null) { "visitDocsPage needs a selection-expressible query (kinds, authors, since/until): $query" }
        return index.visitDocsPage(query, resumeFrom, maxDocs)
    }
}
