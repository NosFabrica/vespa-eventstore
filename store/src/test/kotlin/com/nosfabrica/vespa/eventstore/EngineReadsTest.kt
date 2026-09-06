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

import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.MeteredEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The front door's read-only engine handle: the ONE way out of `VespaEventStore`
 * to the index underneath, and the two properties that make it safe to publish.
 *
 * It goes over the wire ([MockVespaEngine]) rather than against a stub, because
 * a facet that delegated `searchRanked` to `search` would satisfy any stub and
 * still lose the scores the rank-quality harness reads it for.
 */
class EngineReadsTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)
    private val ledger = CostLedger()

    /**
     * The search fields are given explicitly rather than derived: what a term
     * matches is `SearchExtractors`' business (and `SearchExtractorsTest`'s),
     * and this file is about the facet in front of the engine.
     */
    private fun doc(
        id: String,
        kind: Int = 1,
        text: String = "hello",
    ) = EventDoc(
        id = id,
        pubkey = "a".repeat(64),
        createdAt = 100,
        kind = kind,
        tags = emptyList(),
        content = "hello",
        sig = "",
        search = SearchFields(text = text),
    )

    @AfterTest
    fun stop() {
        index.close()
        mock.stop()
    }

    @Test
    fun `it reads the documents the engine really holds`() =
        runBlocking {
            val engine = EngineReads(index)
            index.putAll(listOf(doc("1".repeat(64)), doc("2".repeat(64)), doc("3".repeat(64), kind = 7)))

            val at = mock.searchRequests.size
            assertEquals(3, engine.count(EventQuery()), "count must be the engine's own answer")
            assertEquals(2, engine.count(EventQuery(kinds = listOf(1))), "count must honour the query")
            // A COUNT, not a fetch-and-size: every request it issued asked the
            // engine for zero hits. `count(q) == search(q).size` would pass the
            // assertions above and materialize the corpus to answer "how many".
            assertTrue(
                mock.searchRequests.drop(at).isNotEmpty() && mock.searchRequests.drop(at).all { it["hits"] == "0" },
                "count must ask for no documents: ${mock.searchRequests.drop(at).map { it["hits"] }}",
            )
            assertEquals(
                listOf("1".repeat(64), "2".repeat(64)),
                engine.search(EventQuery(kinds = listOf(1))).map { it.id }.sorted(),
                "search must return the matching documents",
            )
        }

    /**
     * `searchRanked` is not `search`: it comes back through the RANKED recall,
     * which is the only path that carries a per-hit relevance at all. Asserted
     * with the mock ranking on, because a facet that answered this by wrapping
     * `search`'s hits in null scores would return the same DOCUMENTS — and
     * silently give the rank-quality harness nothing to measure.
     *
     * The scores are the mock's own; `RankRegressionIT` is where the real rank
     * profiles' numbers are pinned.
     */
    @Test
    fun `searchRanked carries the engine's relevance, not just the hits`() =
        runBlocking {
            val engine = EngineReads(index)
            index.putAll(listOf(doc("1".repeat(64)), doc("2".repeat(64))))
            mock.relevanceOf = { if (it.id.startsWith("1")) 9.0 else 3.0 }

            val ranked = engine.searchRanked(EventQuery(kinds = listOf(1), search = "hello"))
            assertEquals(2, ranked.size)
            assertEquals(listOf(9.0, 3.0), ranked.map { it.score }, "the per-hit scores must survive the facet")
            assertEquals("1".repeat(64), ranked.first().hit.id, "and must order the page")
        }

    /**
     * THE DOCUMENTED COST OF USING IT: the meter is a decorator one layer up,
     * so a read taken here is invisible to `VespaEventStore.metrics()`. That is
     * right for a health count and wrong for anything that walks the corpus —
     * and it is a property of the wiring, so it is pinned rather than trusted.
     */
    @Test
    fun `reads taken here are not metered`() =
        runBlocking {
            val metered = MeteredEventIndex(ledger, index)
            val engine = EngineReads(index)
            index.putAll(listOf(doc("1".repeat(64))))

            withActivity(Activity.Query) { engine.search(EventQuery()) }
            assertTrue(ledger.snapshot().ports.isEmpty(), "a read through the raw handle must book no port call")

            withActivity(Activity.Query) { metered.search(EventQuery()) }
            assertTrue(ledger.snapshot().ports.isNotEmpty(), "the same read through the metered index must book one")
        }
}
