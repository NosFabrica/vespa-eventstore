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
package com.nosfabrica.vespa.eventstore.store

import com.nosfabrica.vespa.eventstore.vespa.client.EventIndex
import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc
import com.nosfabrica.vespa.eventstore.vespa.query.EventQuery
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

        override suspend fun distinctAuthors(query: EventQuery): Set<String> = hits.mapTo(HashSet()) { it.pubkey }

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
}
