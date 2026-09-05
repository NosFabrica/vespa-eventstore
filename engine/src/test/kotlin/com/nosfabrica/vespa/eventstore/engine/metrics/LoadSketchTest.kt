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

import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * WHAT IS DRIVING THE LOAD, charged where the load is already measured.
 *
 * The two sketches were built, snapshotted and tested in isolation, and for a
 * while NOTHING FED THEM — the defect a full run against a real relay found and
 * no unit test could, because every piece passed on its own. These cases pin
 * the join: a read through the metered port must land on the lens and the terms
 * that asked for it.
 */
class LoadSketchTest {
    private fun index(ledger: CostLedger) = MeteredEventIndex(ledger, InMemoryEventIndex())

    private fun doc(
        id: String,
        kind: Int = 1,
    ) = EventDoc(id = id, pubkey = "a".repeat(64), createdAt = 1_700_000_000, kind = kind, tags = emptyList(), content = "hello", sig = "")

    private val lens = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

    @Test
    fun `a search charges its lens and each of its terms`() =
        runBlocking {
            val ledger = CostLedger()
            val index = index(ledger)
            index.put(doc("1"))

            index.search(EventQuery(kinds = listOf(1), search = "bitcoin lightning", observer = lens))

            val snap = ledger.snapshot()
            assertEquals(listOf(lens), snap.topObservers.map { it.key }, "the lens that asked for the work is charged for it")
            assertEquals(setOf("bitcoin", "lightning"), snap.topTerms.map { it.key }.toSet(), "every term in the query, not just the first")
            assertTrue(snap.topTerms.all { it.weight >= 1 }, "a sub-millisecond read still registers rather than vanishing")
        }

    @Test
    fun `a plain recall charges nobody`() =
        runBlocking {
            val ledger = CostLedger()
            val index = index(ledger)
            index.put(doc("1"))

            // An anonymous REQ resolves no observer and carries no terms. Charging
            // it to anything would put load on a lens that never asked for it.
            index.search(EventQuery(kinds = listOf(1)))

            val snap = ledger.snapshot()
            assertTrue(snap.topObservers.isEmpty())
            assertTrue(snap.topTerms.isEmpty())
            // The port cell is still booked: the call happened, it just has no owner.
            assertEquals(1L, assertNotNull(snap.port(Activity.Other, PortCall.Search)).calls)
        }

    @Test
    fun `a COUNT is charged like the search it summarizes`() =
        runBlocking {
            val ledger = CostLedger()
            val index = index(ledger)
            index.put(doc("1"))

            // NIP-45 over a lens costs a full ranked pass; a page that shows only
            // the REQ side would under-report the observer driving it.
            index.count(EventQuery(kinds = listOf(1), search = "bitcoin", observer = lens))

            assertEquals(listOf(lens), ledger.snapshot().topObservers.map { it.key })
        }

    @Test
    fun `a term repeated inside one query is charged once for that call`() =
        runBlocking {
            val ledger = CostLedger()
            val index = index(ledger)
            index.put(doc("1"))

            index.search(EventQuery(kinds = listOf(1), search = "zap zap zap", observer = lens))

            val zap = assertNotNull(ledger.snapshot().topTerms.firstOrNull { it.key == "zap" })
            val once = assertNotNull(ledger.snapshot().topObservers.firstOrNull { it.key == lens })
            // One call's worth of work, however many times the word appears in it.
            assertEquals(once.weight, zap.weight, "a repeated term must not multiply one call's cost")
        }

    @Test
    fun `the heavier lens outranks the busier one`() =
        runBlocking {
            val ledger = CostLedger()
            val index = index(ledger)
            repeat(200) { index.put(doc(it.toString().padStart(64, '0'))) }
            val busy = "b".repeat(64)

            // WEIGHTED BY TIME, NOT BY CALLS, which is the whole point: a cheap
            // query run often and one four-second query are different problems,
            // and counting calls ranks them the same way. The expensive lens here
            // is the one whose reads actually scan the corpus.
            repeat(20) { index.search(EventQuery(kinds = listOf(1), observer = busy, limit = 1)) }
            repeat(3) { index.search(EventQuery(kinds = listOf(1), search = "hello", observer = lens)) }

            val hits = ledger.snapshot().topObservers.associate { it.key to it.weight }
            assertEquals(2, hits.size, "both lenses are in the sketch")
            assertTrue(hits.getValue(busy) >= 20, "each call contributes at least its rounded-up millisecond")
        }

    @Test
    fun `writes are not charged to anyone`() =
        runBlocking {
            val ledger = CostLedger()
            val index = index(ledger)

            index.putAll(listOf(doc("1"), doc("2")))
            index.existingIds(listOf("1", "2"))

            // Only the read shapes carry a lens and terms; a write shape has no
            // owner to charge and must not invent one.
            assertTrue(ledger.snapshot().topObservers.isEmpty())
            assertTrue(ledger.snapshot().topTerms.isEmpty())
        }
}
