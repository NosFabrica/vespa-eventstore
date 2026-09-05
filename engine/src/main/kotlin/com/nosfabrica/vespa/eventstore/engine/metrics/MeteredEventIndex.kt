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

    /**
     * [meterResult], plus WHO AND WHAT this call was for.
     *
     * The read shapes are the only ones that carry a lens and terms, and this
     * is the altitude that has both the query and its cost — which is what
     * makes the weight meaningful: the sketches answer "what is driving the
     * load", and load is time, not popularity. A cheap query that ran a
     * thousand times and one that took four seconds are different problems,
     * and weighting by calls would rank them the same way.
     *
     * Rounded up to a millisecond so a sub-millisecond call still registers as
     * one unit rather than vanishing from a list of what the store is spending
     * its time on.
     */
    private suspend inline fun <T> meterQuery(
        call: PortCall,
        query: EventQuery,
        body: () -> T,
        size: (T) -> Long,
    ): T {
        val activity = currentActivity()
        val t0 = System.nanoTime()
        var docs = 0L
        try {
            val out = body()
            docs = size(out)
            return out
        } finally {
            val took = System.nanoTime() - t0
            ledger.port(activity, call, took, docs)
            bookLoad(query, took)
        }
    }

    /**
     * Charge this call's time to the lens and the terms that asked for it.
     *
     * THE FORBIDDEN DIMENSION, BOUNDED. An observer pubkey and a search term
     * are both unbounded key spaces, which is exactly what a metric label may
     * never be; a weighted Space-Saving sketch keeps the heavy hitters in a
     * fixed number of slots and forgets the tail, so the memory is the sketch's
     * and not the traffic's. Both sketches are read out only where an operator
     * has asked for client-derived sections.
     *
     * The terms are the SANITIZED ones the query carries, not the client's raw
     * search string: the extensions (`observer:`, `sort:`, `include:spam`)
     * have already been taken out of it, so a lens does not also land in the
     * term list under its own name.
     *
     * THE LENS IS TRUNCATED TO A PREFIX, which [HeavyHitters] asks its callers
     * for and this did not do. A top-K by observer is a ranked list of who
     * searched the most; a prefix is enough for an operator who knows their own
     * lenses to recognise one, and short of a key that can be copied out of a
     * screenshot or a log and used. It does not make the list safe on its own —
     * that is what the client-detail switch and the admin gate are for — it
     * stops the list being MORE identifying than it has to be.
     */
    private fun bookLoad(
        query: EventQuery,
        nanos: Long,
    ) {
        val weight = maxOf(1L, nanos / 1_000_000)
        query.observer?.let { ledger.byObserver.add(it.take(OBSERVER_KEY_CHARS), weight) }
        val terms = query.search ?: return
        // Distinct, so a term repeated inside one query is not charged twice
        // for one call's worth of work.
        for (term in terms.split(' ', '\t', '\n').filter { it.isNotEmpty() }.distinct()) {
            ledger.byTerm.add(term, weight)
        }
    }

    private companion object {
        /**
         * How much of an observer's 64-hex pubkey the load sketch keeps.
         *
         * Long enough that an operator recognises their own lenses and that two
         * real pubkeys will not collide into one row; short enough that the row
         * is not a key somebody can lift out of the page and use. See
         * [HeavyHitters] and docs/telemetry.md §11.2.
         */
        const val OBSERVER_KEY_CHARS = 16
    }

    override suspend fun get(id: String): EventDoc? = meterResult(PortCall.Get, { inner.get(id) }, { if (it == null) 0L else 1L })

    override suspend fun put(doc: EventDoc) = meter(PortCall.Put, 1) { inner.put(doc) }

    override suspend fun putAll(docs: List<EventDoc>) = meter(PortCall.Put, docs.size.toLong()) { inner.putAll(docs) }

    override suspend fun remove(id: String) = meter(PortCall.Remove, 1) { inner.remove(id) }

    override suspend fun removeAll(ids: List<String>) = meter(PortCall.Remove, ids.size.toLong()) { inner.removeAll(ids) }

    override suspend fun removeDocs(docs: List<EventDoc>) = meter(PortCall.Remove, docs.size.toLong()) { inner.removeDocs(docs) }

    override suspend fun search(query: EventQuery): List<EventDoc> = meterQuery(PortCall.Search, query, { inner.search(query) }, { it.size.toLong() })

    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = meterQuery(PortCall.Search, query, { inner.rawSearch(query) }, { it.size.toLong() })

    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = meterQuery(PortCall.Search, query, { inner.searchRanked(query) }, { it.size.toLong() })

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = meterQuery(PortCall.Search, query, { inner.rawSearchRanked(query) }, { it.size.toLong() })

    override suspend fun existingIds(ids: List<String>): Set<String> = meter(PortCall.Exists, ids.size.toLong()) { inner.existingIds(ids) }

    override suspend fun count(query: EventQuery): Int = meterQuery(PortCall.Count, query, { inner.count(query) }, { 1L })

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
