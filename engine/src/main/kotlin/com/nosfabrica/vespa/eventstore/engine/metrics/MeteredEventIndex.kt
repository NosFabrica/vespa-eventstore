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
package com.nosfabrica.vespa.eventstore.engine.metrics

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.store.RawEvent

/**
 * COUNTS EVERY CALL THROUGH THE PORT, and changes nothing else.
 *
 * The placement argument is `TrustProjection`'s, which decorates this same seam
 * so that "every deletion style updates trust tensors with zero
 * deletion-specific code". Metering wants exactly that property: a read path
 * added next year is counted the day it is written, because it must come
 * through here to reach the engine. There is no `IngestStats.timed(...)` to
 * forget.
 *
 * TRANSPARENCY IS THE CONTRACT. Every member forwards to [inner], including
 * [supersedesViaPut] and every default the port's own KDoc says a decorator
 * MUST delegate ([existingIds]'s summary-free path, [visitTags]'s streaming
 * projection, [countByAuthor]'s server-side grouping, [scanAuthors]'s paged
 * completeness, [visitDocsPage]'s resumable walk). Riding a default instead of
 * forwarding would silently downgrade the engine — and a store that got slower
 * because it was being measured is worse than one that is not measured.
 *
 * WHERE TO PUT IT. Wrapping the raw engine counts what actually reached Vespa;
 * wrapping the projection counts what the store asked for. Both is best — the
 * difference is the trust projection's own traffic, which nothing else can
 * show:
 * ```
 * NostrSemanticsStore(Metered(STORE, TrustProjection(Metered(ENGINE, VespaEventIndex), reputations)))
 * ```
 *
 * COST: ~97 ns per call (docs/telemetry.md §5), against a call that crosses a
 * network.
 */
class MeteredEventIndex(
    private val ledger: CostLedger,
    private val inner: EventIndex,
) : EventIndex {
    override val supersedesViaPut: Boolean get() = inner.supersedesViaPut

    /** Time [body], booking it to [call] under whatever [Activity] is ambient. */
    private suspend inline fun <T> meter(
        call: PortCall,
        docs: Long,
        body: () -> T,
    ): T {
        val activity = currentActivity()
        val t0 = System.nanoTime()
        try {
            return body()
        } finally {
            ledger.port(activity, call, System.nanoTime() - t0, docs)
        }
    }

    /**
     * [meter] for a call whose document count is only known from the ANSWER.
     * Booked in the same cell as the cost, which is what makes the ratio sound
     * — a numerator and denominator from different cells do not divide.
     */
    private suspend inline fun <T> meterResult(
        call: PortCall,
        body: () -> T,
        size: (T) -> Long,
    ): T {
        val activity = currentActivity()
        val t0 = System.nanoTime()
        var result: T? = null
        try {
            val r = body()
            result = r
            return r
        } finally {
            val took = System.nanoTime() - t0
            ledger.port(activity, call, took, result?.let(size) ?: 0L)
        }
    }

    override suspend fun get(id: String): EventDoc? = meterResult(PortCall.Get, { inner.get(id) }, { if (it == null) 0L else 1L })

    override suspend fun put(doc: EventDoc) = meter(PortCall.Put, 1) { inner.put(doc) }

    override suspend fun putAll(docs: List<EventDoc>) = meter(PortCall.Put, docs.size.toLong()) { inner.putAll(docs) }

    override suspend fun remove(id: String) = meter(PortCall.Remove, 1) { inner.remove(id) }

    override suspend fun removeAll(ids: List<String>) = meter(PortCall.Remove, ids.size.toLong()) { inner.removeAll(ids) }

    override suspend fun removeDocs(docs: List<EventDoc>) = meter(PortCall.Remove, docs.size.toLong()) { inner.removeDocs(docs) }

    override suspend fun search(query: EventQuery): List<EventDoc> = meterResult(PortCall.Search, { inner.search(query) }, { it.size.toLong() })

    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = meterResult(PortCall.Search, { inner.rawSearch(query) }, { it.size.toLong() })

    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = meterResult(PortCall.Search, { inner.searchRanked(query) }, { it.size.toLong() })

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = meterResult(PortCall.Search, { inner.rawSearchRanked(query) }, { it.size.toLong() })

    override suspend fun existingIds(ids: List<String>): Set<String> = meter(PortCall.Exists, ids.size.toLong()) { inner.existingIds(ids) }

    override suspend fun count(query: EventQuery): Int = meter(PortCall.Count, 1) { inner.count(query) }

    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> = meterResult(PortCall.Group, { inner.countByAuthor(query) }, { it.size.toLong() })

    override suspend fun scanAuthors(query: EventQuery): Set<String> = meterResult(PortCall.Group, { inner.scanAuthors(query) }, { it.size.toLong() })

    /**
     * Booked as ONE call however many pages it walks, with the documents it
     * streamed as the denominator — the walk is the unit of work a caller
     * asked for, and counting pages would make `callsPerDoc` a property of the
     * engine's page size rather than of the caller's behaviour.
     */
    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        // Booked by hand rather than through [meter]: the document count is
        // only known once the walk ENDS, and it has to land on the same
        // `port()` call as the cost — two calls would count the walk twice and
        // make `callsPerDoc` nonsense.
        val activity = currentActivity()
        val t0 = System.nanoTime()
        var seen = 0L
        try {
            inner.visitIds(query, withDTag) { page ->
                seen += page.size
                onPage(page)
            }
        } finally {
            ledger.port(activity, PortCall.Visit, System.nanoTime() - t0, seen)
        }
    }

    override suspend fun visitTags(
        query: EventQuery,
        onPage: suspend (List<List<List<String>>>) -> Boolean,
    ) {
        val activity = currentActivity()
        val t0 = System.nanoTime()
        var seen = 0L
        try {
            inner.visitTags(query) { page ->
                seen += page.size
                onPage(page)
            }
        } finally {
            ledger.port(activity, PortCall.Visit, System.nanoTime() - t0, seen)
        }
    }

    override suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage = meterResult(PortCall.Visit, { inner.visitDocsPage(query, resumeFrom, maxDocs) }, { it.docs.size.toLong() })

    /**
     * Forwarded, NOT ridden. The port's default supersedes through `this`,
     * which would double-count (the search and removes it issues are already
     * metered by whatever they pass through); forwarding keeps [inner]'s own
     * strategy — the real client's engine-atomic conditional put, or a reacting
     * decorator's read-then-supersede — exactly as it was.
     */
    override suspend fun putIfNewer(doc: EventDoc): Boolean = meter(PortCall.Put, 1) { inner.putIfNewer(doc) }

    override fun close() = inner.close()
}
