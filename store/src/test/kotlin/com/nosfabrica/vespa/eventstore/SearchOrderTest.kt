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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * NIP-50: "The result should be ordered by relevance to the search query …
 * instead of the usual created_at ordering." The engine returns hits in rank
 * order; the store must PRESERVE that order for searching/ranked queries and
 * apply recency only to plain NIP-01 filters.
 */
class SearchOrderTest {
    // Engine order: the OLDER event first — i.e. relevance disagrees with recency.
    private val older = doc(id = "1".repeat(64), createdAt = 100)
    private val newer = doc(id = "2".repeat(64), createdAt = 200)

    private fun doc(
        id: String,
        createdAt: Long,
    ) = EventDoc(id = id, pubkey = "a".repeat(64), createdAt = createdAt, kind = 1, tags = emptyList(), content = "hello", sig = "")

    /** An engine stub that answers every query with a FIXED hit order (searching queries may answer differently). */
    private class FixedOrderIndex(
        private val hits: List<EventDoc>,
        private val searchingHits: List<EventDoc> = hits,
    ) : EventIndex {
        override suspend fun get(id: String): EventDoc? = hits.find { it.id == id }

        override suspend fun search(query: EventQuery): List<EventDoc> = if (query.search != null) searchingHits else hits

        override suspend fun count(query: EventQuery): Int = hits.size

        override suspend fun put(doc: EventDoc) {}

        override suspend fun remove(id: String) {}

        override fun close() {}
    }

    private fun store() = NostrSemanticsStore(FixedOrderIndex(listOf(older, newer)))

    @Test
    fun `a search keeps the engine's relevance order`() =
        runBlocking {
            val ids = store().query<Event>(Filter(search = "hello")).map { it.id }
            assertEquals(listOf(older.id, newer.id), ids, "rank order survives the store")
        }

    @Test
    fun `a ranked match-all (sort extension) keeps the engine's order too`() =
        runBlocking {
            val ids = store().query<Event>(Filter(search = "sort:rank")).map { it.id }
            assertEquals(listOf(older.id, newer.id), ids)
        }

    /**
     * `sort:recent` is the one searching query that does NOT keep the engine's
     * order: its profile scores by created_at (ties arbitrary), and the store
     * restores the exact `created_at desc, id asc` order the token asked for —
     * the same order a plain NIP-01 filter gets.
     */
    @Test
    fun `a sort recent search is re-sorted to NIP-01 recency`() =
        runBlocking {
            val ids = store().query<Event>(Filter(search = "hello sort:recent")).map { it.id }
            assertEquals(listOf(newer.id, older.id), ids, "chronological, not relevance")
        }

    /**
     * A plain filter riding beside a searching one in the SAME REQ still gets
     * NIP-01 recency for ITS hits — the search's relevance order never leaks
     * onto the sibling's results.
     */
    @Test
    fun `a plain filter beside a search keeps recency order`() =
        runBlocking {
            val store = NostrSemanticsStore(FixedOrderIndex(hits = listOf(older, newer), searchingHits = emptyList()))
            val ids = store.query<Event>(listOf(Filter(search = "nomatch"), Filter(kinds = listOf(1)))).map { it.id }
            assertEquals(listOf(newer.id, older.id), ids, "the plain filter's hits stay newest-first")
        }

    @Test
    fun `a plain filter gets NIP-01 recency order`() =
        runBlocking {
            val ids = store().query<Event>(Filter(kinds = listOf(1))).map { it.id }
            assertEquals(listOf(newer.id, older.id), ids, "newest first without a search")
        }

    /** A phrase-only query is a SEARCH: relevance order, never re-sorted to recency. */
    @Test
    fun `a phrase-only search keeps the engine's relevance order`() =
        runBlocking {
            val ids = store().query<Event>(Filter(search = "\"hello there\"")).map { it.id }
            assertEquals(listOf(older.id, newer.id), ids, "rank order survives the store")
        }

    /** An exclusion-only query is PLAIN recall: newest first, like any NIP-01 filter. */
    @Test
    fun `an exclusion-only search gets NIP-01 recency order`() =
        runBlocking {
            val ids = store().query<Event>(Filter(search = "-nomatch")).map { it.id }
            assertEquals(listOf(newer.id, older.id), ids, "recall minus a word is still recall")
        }

    // ---- merging the filters of ONE REQ --------------------------------------
    //
    // A multi-filter REQ is one question asked several ways — a search page
    // looking for a topic sends `#t`, a NIP-32 `#l` and two NIP-22 comment
    // filters — and the answer wants ONE order. Concatenated runs meant the
    // trust scale started again at every seam, which reads as a ranking mistake
    // and is not one.

    private val mid = doc(id = "3".repeat(64), createdAt = 150)

    /** An engine that RANKS: each query's hits come back with the scores it names. */
    private class ScoringIndex(
        private val perQuery: (EventQuery) -> List<Ranked<EventDoc>>,
    ) : EventIndex {
        override suspend fun get(id: String): EventDoc? = null

        override suspend fun search(query: EventQuery): List<EventDoc> = perQuery(query).map { it.hit }

        override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = perQuery(query)

        override suspend fun count(query: EventQuery): Int = 0

        override suspend fun put(doc: EventDoc) {}

        override suspend fun remove(id: String) {}

        override fun close() {}
    }

    private fun twoTopicFilters(sort: String = "") =
        listOf(
            Filter(search = "hello$sort", tags = mapOf("t" to listOf("nostr"))),
            Filter(search = "hello$sort", tags = mapOf("l" to listOf("nostr"))),
        )

    @Test
    fun `two ranked filters merge on the engine's scores`() =
        runBlocking {
            // The SECOND filter holds the best hit. Concatenated, it landed
            // behind everything the first filter returned.
            val store =
                NostrSemanticsStore(
                    ScoringIndex { q ->
                        if (q.tags["t"] != null) listOf(Ranked(older, 1.0), Ranked(newer, 0.5)) else listOf(Ranked(mid, 9.0))
                    },
                )
            val ids = store.query<Event>(twoTopicFilters()).map { it.id }
            assertEquals(listOf(mid.id, older.id, newer.id), ids, "one order over the union, by score")
        }

    @Test
    fun `a score tie falls back to created_at desc, id asc`() =
        runBlocking {
            val sameTime = doc(id = "4".repeat(64), createdAt = 200)
            val store =
                NostrSemanticsStore(
                    ScoringIndex { q ->
                        if (q.tags["t"] != null) listOf(Ranked(older, 5.0), Ranked(sameTime, 5.0)) else listOf(Ranked(newer, 5.0))
                    },
                )
            val ids = store.query<Event>(twoTopicFilters()).map { it.id }
            // `newer` and `sameTime` share created_at 200, so the lower id wins.
            assertEquals(listOf(newer.id, sameTime.id, older.id), ids, "ties are the NIP-01 order")
        }

    @Test
    fun `an event answering two filters is served once, at its better score`() =
        runBlocking {
            val store =
                NostrSemanticsStore(
                    ScoringIndex { q ->
                        if (q.tags["t"] != null) listOf(Ranked(older, 0.1)) else listOf(Ranked(older, 9.0), Ranked(newer, 1.0))
                    },
                )
            val ids = store.query<Event>(twoTopicFilters()).map { it.id }
            assertEquals(listOf(older.id, newer.id), ids, "deduped at the higher score, not the first seen")
        }

    /**
     * An engine that does not rank says so with a null score, and the merge
     * falls to recency — the order its hits are already in. The in-memory
     * reference IS that engine, so every store test running against it gets a
     * coherent union rather than a ranking nobody computed.
     */
    @Test
    fun `an engine with no scores merges ranked filters by recency`() =
        runBlocking {
            val ids = store().query<Event>(twoTopicFilters()).map { it.id }
            assertEquals(listOf(newer.id, older.id), ids, "no score to merge on; recency is the honest order")
        }

    /**
     * Two rank profiles are two scales — `sort:rank` scores trust, `sort:text`
     * scores words, and neither number means anything to the other. The runs
     * stay whole rather than being interleaved on a comparison that does not
     * exist.
     */
    @Test
    fun `filters ranked by different profiles keep their runs`() =
        runBlocking {
            val store =
                NostrSemanticsStore(
                    ScoringIndex { q -> if (q.ranking == EventYql.RANK_TEXT) listOf(Ranked(newer, 0.2)) else listOf(Ranked(older, 99.0)) },
                )
            val ids = store.query<Event>(listOf(Filter(search = "hi sort:text"), Filter(search = "hi sort:rank"))).map { it.id }
            assertEquals(listOf(newer.id, older.id), ids, "filter order, each run intact")
        }
}
