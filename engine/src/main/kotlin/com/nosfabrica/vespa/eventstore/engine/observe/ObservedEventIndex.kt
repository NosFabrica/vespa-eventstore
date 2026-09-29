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
package com.nosfabrica.vespa.eventstore.engine.observe

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import java.util.concurrent.atomic.AtomicLong

/**
 * TELLS [observers] ABOUT EVERY ACKED WRITE THROUGH THE PORT, and changes
 * nothing else — the [IndexObserver] hook, as an [EventIndex] decorator.
 *
 * WHERE IT SITS: directly over the metered engine and BELOW `TrustProjection`
 * (`VespaEventStore.open` places it), for one reason — the projection drives
 * writes of its own against its inner index (it replays read-then-supersede
 * there for trust kinds), and the orphan-score sweep removes through the raw
 * index, not the projection. Above the projection this would see neither.
 *
 * REPORTS AFTER THE INNER CALL RETURNS, never before: an observer is told what
 * the engine ACKED, so a mirror never holds an event the index refused. A call
 * that throws reports nothing (see [IndexObserver] for the partial-bulk case).
 * put/putAll → [IndexObserver.onPut]; remove/removeAll/removeDocs →
 * [IndexObserver.onRemove] with the ids asked for.
 *
 * SUPERSESSION ([putIfNewer]) is the one member that is not a plain forward:
 *  - Over an index that supersedes by READING ([EventIndex.supersedesViaPut]
 *    false — the in-memory reference, and the Vespa client unless
 *    address-keyed), it RIDES the port's read-then-supersede default, so the
 *    replaced versions leave through this decorator's own [removeDocs] and the
 *    winner through its [put]: the mirror is told both. The port's own KDoc
 *    asks exactly this of a reacting decorator. The engine traffic is the same
 *    search + remove + put the inner default would have issued; what changes is
 *    only that the meter below books them as three calls instead of one.
 *  - Over an ENGINE-ATOMIC index (address-keyed Vespa: one conditional put, no
 *    read), it forwards and reports only the winner — the version it replaced
 *    is overwritten inside the engine and never named to the client, so there
 *    is nothing to report short of a read the atomic path exists to avoid. The
 *    consumer applies the NIP-01 supersession rule (highest `created_at`, ties
 *    to the lowest id) to what it is told, which reaches the same state.
 *
 * TRANSPARENT FOR READS, like `MeteredEventIndex`: every read member forwards
 * to [inner] — including the defaults the port says a decorator MUST forward —
 * because a decorator that inherits one does not fail to decorate it, it
 * answers with the slow default (`PortDecoratorsTest`).
 *
 * OBSERVER FAILURES never reach the write path: each observer is called on its
 * own, and a throw is counted ([observerFailures]) and dropped, so one broken
 * observer neither fails the write nor starves the others. Only a
 * [VirtualMachineError] (out of memory, stack overflow) is rethrown — the
 * process, not the observer, is what is broken then.
 *
 * COST with observers installed: one plain [Event] per written doc (the tag
 * lists copied into Quartz's array shape), built once per call and shared by
 * every observer. With none installed, [of] installs nothing at all.
 */
class ObservedEventIndex(
    private val inner: EventIndex,
    private val observers: List<IndexObserver>,
) : EventIndex {
    private val failures = AtomicLong()

    /** Observer calls that threw since this index was built — non-zero means a mirror has missed batches and owes a reconcile. */
    fun observerFailures(): Long = failures.get()

    override val supersedesViaPut: Boolean get() = inner.supersedesViaPut

    // ---- writes: forward, then tell --------------------------------------------

    override suspend fun put(doc: EventDoc) {
        inner.put(doc)
        reportPut(listOf(doc))
    }

    override suspend fun putAll(docs: List<EventDoc>) {
        inner.putAll(docs)
        reportPut(docs)
    }

    override suspend fun remove(id: String) {
        inner.remove(id)
        reportRemove(listOf(id))
    }

    override suspend fun removeAll(ids: List<String>) {
        inner.removeAll(ids)
        reportRemove(ids)
    }

    override suspend fun removeDocs(docs: List<EventDoc>) {
        inner.removeDocs(docs)
        reportRemove(docs.map { it.id })
    }

    /** See the class KDoc: rides the read-then-supersede default unless the engine supersedes atomically. */
    override suspend fun putIfNewer(doc: EventDoc): Boolean {
        // The default supersedes through `this` — search, removeDocs, put —
        // so the replaced versions and the winner are each reported once, by
        // the members above. Nothing to report here on that branch.
        if (!inner.supersedesViaPut) return super.putIfNewer(doc)
        val stored = inner.putIfNewer(doc)
        if (stored) reportPut(listOf(doc))
        return stored
    }

    private fun reportPut(docs: List<EventDoc>) {
        if (docs.isEmpty()) return
        val events = docs.map { it.toPlainEvent() }
        tell { it.onPut(events) }
    }

    private fun reportRemove(ids: List<String>) {
        if (ids.isEmpty()) return
        tell { it.onRemove(ids) }
    }

    /** Each observer on its own, so one that throws neither fails the write nor silences the rest. */
    private inline fun tell(call: (IndexObserver) -> Unit) {
        for (observer in observers) {
            try {
                call(observer)
            } catch (e: VirtualMachineError) {
                throw e
            } catch (t: Throwable) {
                failures.incrementAndGet()
            }
        }
    }

    // ---- reads: forwarded, never ridden ----------------------------------------

    override suspend fun get(id: String): EventDoc? = inner.get(id)

    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query)

    override suspend fun existingIds(ids: List<String>): Set<String> = inner.existingIds(ids)

    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = inner.rawSearch(query)

    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = inner.searchRanked(query)

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = inner.rawSearchRanked(query)

    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) = inner.visitIds(query, withDTag, onPage)

    override suspend fun distinctTagIndexValues(
        query: EventQuery,
        tagName: String,
    ): Set<String>? = inner.distinctTagIndexValues(query, tagName)

    override suspend fun visitTags(
        query: EventQuery,
        onPage: suspend (List<List<List<String>>>) -> Boolean,
    ) = inner.visitTags(query, onPage)

    override suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage = inner.visitDocsPage(query, resumeFrom, maxDocs)

    override suspend fun count(query: EventQuery): Int = inner.count(query)

    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> = inner.countByAuthor(query)

    override suspend fun scanAuthors(query: EventQuery): Set<String> = inner.scanAuthors(query)

    override fun close() = inner.close()

    companion object {
        /**
         * [inner] wrapped for [observers], or NULL when there is nobody to tell
         * — so a stack built without observers is byte-identical to one built
         * before this hook existed, rather than paying a forwarding layer and a
         * changed supersession path for nothing.
         */
        fun of(
            inner: EventIndex,
            observers: List<IndexObserver>,
        ): ObservedEventIndex? = if (observers.isEmpty()) null else ObservedEventIndex(inner, observers)

        /**
         * A PLAIN [Event] from the stored NIP-01 fields — no kind dispatch
         * (Quartz's `EventFactory`), no JSON round trip: a mirror wants the
         * seven fields, and this runs once per written document.
         */
        private fun EventDoc.toPlainEvent(): Event = Event(id, pubkey, createdAt, kind, Array(tags.size) { tags[it].toTypedArray() }, content, sig)
    }
}
