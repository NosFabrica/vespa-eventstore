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

import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.MeteredEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.PortCall
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `authorsMissingOutbox()` answers Quartz's contract without Quartz's default,
 * which streams `query(Filter())` — on this store a List of the whole corpus.
 * Held to that default's own algorithm (written out below over the same
 * store) on every case the contract names: an outbox, none, one that expired,
 * a gift-wrap-only key (not an author), and a key that wrapped AND posted.
 */
class AuthorsMissingOutboxTest {
    private var now = 1_000L
    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun ev(
        author: Char,
        kind: Int,
        tags: Array<Array<String>> = emptyArray(),
    ) = Event(id(), author.toString().repeat(64), 900L + seq, kind, tags, "", "")

    @Test
    fun `the anti-join matches quartz's default, without reading a single event`() =
        runBlocking {
            val ledger = CostLedger()
            val store = NostrSemanticsStore(MeteredEventIndex(ledger, InMemoryEventIndex()), nowSecs = { now })
            store.batchInsert(
                listOf(
                    ev('a', 10002),
                    ev('a', 1),
                    ev('b', 1),
                    ev('c', 10002, arrayOf(arrayOf("expiration", "1500"))),
                    ev('c', 1),
                    ev('d', 1059),
                    ev('e', 1059),
                    ev('e', 7),
                ),
            )
            now = 2_000L // c's only outbox has expired

            val reference = quartzDefault(store)
            assertEquals(setOf("b", "c", "e").map { it.repeat(64) }.toSet(), reference, "the contract's answer")
            val portsBefore =
                ledger
                    .snapshot()
                    .ports
                    .filter { it.call == PortCall.Search }
                    .sumOf { it.calls }
            assertEquals(reference, store.authorsMissingOutbox().toSet())
            val searches =
                ledger
                    .snapshot()
                    .ports
                    .filter { it.call == PortCall.Search }
                    .sumOf { it.calls } - portsBefore
            assertEquals(0L, searches, "author scans and groupings only: no document read")
            assertTrue(ledger.snapshot().ports.any { it.call == PortCall.Group }, "the scans are the engine's")
        }

    /** IEventStore.authorsMissingOutbox()'s default body, verbatim in substance, over this store's reads. */
    private suspend fun quartzDefault(store: NostrSemanticsStore): Set<String> {
        val withOutbox = store.query<Event>(Filter(kinds = listOf(10002))).map { it.pubKey }.toSet()
        return store
            .query<Event>(Filter())
            .filter { it.kind != 1059 && it.pubKey !in withOutbox }
            .map { it.pubKey }
            .toSet()
    }
}
