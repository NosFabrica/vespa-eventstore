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
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
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
 *  - **Which version of a replaceable event is held**, for the same reason and
 *    the same caller: an offered event already beaten by a stored one is work
 *    not worth doing.
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
     * The newest held version per author matching [query], keyed by pubkey —
     * NIP-01 supersession asked in bulk, on the engine's attribute-only
     * projection rather than a document summary per author.
     *
     * Un-lensed like the rest of this type, and that is again what makes it
     * usable as a gate: supersession is a property of what is STORED, and an
     * event the lens hides still beats the one being offered.
     */
    suspend fun newestPerAuthor(query: EventQuery): Map<String, DocRef> = index.newestPerAuthor(query)
}
