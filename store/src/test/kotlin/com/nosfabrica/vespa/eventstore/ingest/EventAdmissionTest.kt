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
package com.nosfabrica.vespa.eventstore.ingest

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.RejectedException
import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * [EventAdmission]'s single-event path: a replaceable with no guard to probe
 * sends its dup GET and its supersession's version read TOGETHER, one round
 * trip under the writer lock instead of two, with the sequential outcomes.
 */
class EventAdmissionTest {
    private val relayUrl = "wss://sot.test/".normalizeRelayUrl()
    private val alice = "a1".repeat(32)
    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun profile(at: Long) = Event(id(), alice, at, 0, emptyArray(), "{\"at\":$at}", "")

    /** Answers neither the dup GET nor the version read until both have been asked, once [armed]. */
    private class Rendezvous(
        val inner: InMemoryEventIndex = InMemoryEventIndex(),
    ) : EventIndex by inner {
        val armed = AtomicBoolean(false)
        private val got = CompletableDeferred<Unit>()
        private val read = CompletableDeferred<Unit>()

        override suspend fun get(id: String): EventDoc? {
            if (armed.get()) {
                got.complete(Unit)
                read.await()
            }
            return inner.get(id)
        }

        override suspend fun visitIds(
            query: EventQuery,
            withDTag: Boolean,
            onPage: suspend (List<DocRef>) -> Boolean,
        ) {
            if (armed.get()) {
                read.complete(Unit)
                got.await()
            }
            inner.visitIds(query, withDTag, onPage)
        }

        override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
    }

    @Test
    fun `a replaceable's dup read and version read go out together`() =
        runBlocking {
            val index = Rendezvous()
            val store = NostrSemanticsStore(index, relay = relayUrl, writers = WriterTopology.SINGLE_WRITER)
            store.insert(profile(100))
            val newer = profile(200)
            index.armed.set(true)

            // Sequential reads would wait on each other forever.
            withTimeout(10_000) { store.insert(newer) }

            assertEquals(listOf(newer.id), index.inner.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).map { it.id })
        }

    @Test
    fun `the outcomes are the sequential path's`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, relay = relayUrl, writers = WriterTopology.SINGLE_WRITER)
            val stored = profile(100)
            store.insert(stored)

            val dup = assertFailsWith<RejectedException> { store.insert(stored) }
            assertTrue(dup.message!!.startsWith("duplicate"), "${dup.message}")
            val stale = assertFailsWith<RejectedException> { store.insert(profile(50)) }
            assertTrue(stale.message!!.startsWith("replaced"), "${stale.message}")
            assertEquals(listOf(stored.id), index.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).map { it.id })
        }
}
