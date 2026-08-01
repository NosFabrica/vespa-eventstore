/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.vespa.client.DocRef
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import java.util.concurrent.atomic.AtomicLong

/**
 * An [EventIndex] decorator that counts every call the store makes to the engine.
 *
 * Against a real Vespa each of these is a network round-trip; against SQLite each
 * is a prepared-statement execution. Either way the COUNT is engine-independent —
 * it is a property of [NostrSemanticsStore]'s own logic — so it is the cleanest way
 * to see the store layer's I/O amplification (the code comments claim "3-5 index
 * round-trips per event"; this measures the real figure per corpus) and to prove
 * the bulk path collapses that fan-out. That makes it the right lens for
 * optimizing THIS framework, independent of which engine sits underneath.
 */
class CountingEventIndex(
    private val inner: EventIndex,
) : EventIndex {
    val gets = AtomicLong()
    val puts = AtomicLong() // documents written (putAll counts each doc)
    val putCalls = AtomicLong() // put/putAll invocations (round-trips, not docs)
    val removes = AtomicLong()
    val removeCalls = AtomicLong()
    val searches = AtomicLong()
    val counts = AtomicLong()

    /** All engine reads (get + search + count): the recall round-trips. */
    fun reads(): Long = gets.get() + searches.get() + counts.get()

    /** All engine writes (put + remove round-trips): the mutation round-trips. */
    fun writeCalls(): Long = putCalls.get() + removeCalls.get()

    fun total(): Long = reads() + writeCalls()

    fun reset() {
        listOf(gets, puts, putCalls, removes, removeCalls, searches, counts).forEach { it.set(0) }
    }

    override suspend fun get(id: String): EventDoc? {
        gets.incrementAndGet()
        return inner.get(id)
    }

    override suspend fun put(doc: EventDoc) {
        putCalls.incrementAndGet()
        puts.incrementAndGet()
        inner.put(doc)
    }

    override suspend fun putAll(docs: List<EventDoc>) {
        putCalls.incrementAndGet()
        puts.addAndGet(docs.size.toLong())
        inner.putAll(docs)
    }

    override suspend fun remove(id: String) {
        removeCalls.incrementAndGet()
        removes.incrementAndGet()
        inner.remove(id)
    }

    override suspend fun removeAll(ids: List<String>) {
        removeCalls.incrementAndGet()
        removes.addAndGet(ids.size.toLong())
        inner.removeAll(ids)
    }

    override suspend fun search(query: EventQuery): List<EventDoc> {
        searches.incrementAndGet()
        return inner.search(query)
    }

    override suspend fun count(query: EventQuery): Int {
        counts.incrementAndGet()
        return inner.count(query)
    }

    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        searches.incrementAndGet()
        inner.visitIds(query, withDTag, onPage)
    }

    // Delegate (not ride the default) so the decorator matches the interface contract.
    override suspend fun distinctAuthors(query: EventQuery): Set<String> {
        searches.incrementAndGet()
        return inner.distinctAuthors(query)
    }

    override fun close() = inner.close()
}
