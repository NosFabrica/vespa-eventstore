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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.nosfabrica.vespa.eventstore.mapping.INCLUDE_SPAM_MIN_RANK
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * THE OBSERVER GATE: supplying an observer — an `observer:` search token or the
 * out-of-band [StoreQueryContext] lens — opts the whole request into that lens,
 * plain NIP-01 recall included. Non-search queries keep recency order but drop
 * authors below the trust floor (the recency_gated profile); `include:spam`
 * opts back out; reads with no lens are never gated.
 */
class ObserverGateTest {
    private val hex = "c".repeat(64)

    /** Records every [EventQuery] the store sends to the engine. */
    private class CapturingIndex(
        private val hits: List<EventDoc> = emptyList(),
    ) : EventIndex {
        val queries = mutableListOf<EventQuery>()

        override suspend fun get(id: String): EventDoc? = null

        override suspend fun search(query: EventQuery): List<EventDoc> {
            queries += query
            return hits
        }

        override suspend fun count(query: EventQuery): Int = 0

        override suspend fun put(doc: EventDoc) {}

        override suspend fun remove(id: String) {}

        override fun close() {}
    }

    private fun captured(
        filter: Filter,
        observer: String? = null,
    ): EventQuery =
        runBlocking {
            val index = CapturingIndex()
            val store = NostrSemanticsStore(index)
            if (observer != null) {
                withContext(StoreQueryContext(setOf(observer))) { store.query<Event>(filter) }
            } else {
                store.query<Event>(filter)
            }
            index.queries.single()
        }

    /**
     * One request, one instant. [NostrSemanticsStore] merges sibling filters'
     * hits by ENGINE SCORE, and a score now depends on when it was asked
     * (recency ranking reads query(now_secs) — docs/recency-ranking.md), so a
     * per-filter clock would put one merge on two scales. Same argument, and
     * the same number, as the expiry cutoff this has always stamped.
     *
     * The clock here ticks a whole day per call, so a second read of it could
     * not hide inside a rounding error.
     */
    @Test
    fun `every filter in one request is ranked and expired against the same instant`() {
        runBlocking {
            var tick = 0L
            val index = CapturingIndex()
            val store = NostrSemanticsStore(index, nowSecs = { 1_700_000_000L + 86_400L * tick++ })
            store.query<Event>(listOf(Filter(kinds = listOf(1), search = "vitor"), Filter(kinds = listOf(0), search = "vitor")))
            assertEquals(2, index.queries.size)
            assertEquals(
                listOf(1_700_000_000L to 1_700_000_000L, 1_700_000_000L to 1_700_000_000L),
                index.queries.map { it.nowSecs to it.notExpiredAt },
                "sibling filters must share one clock: ${index.queries.map { it.nowSecs }}",
            )
        }
    }

    @Test
    fun `a context observer gates plain recall at the default floor, in recency order`() {
        val q = captured(Filter(kinds = listOf(1)), observer = hex)
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking)
        assertEquals(DEFAULT_MIN_RANK, q.minRank)
        assertEquals(hex, q.observer)
    }

    @Test
    fun `an observer search token gates plain recall the same way`() {
        val q = captured(Filter(kinds = listOf(1), search = "observer:$hex"))
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking)
        assertEquals(DEFAULT_MIN_RANK, q.minRank)
        assertEquals(hex, q.observer)
    }

    @Test
    fun `include spam opts a plain query back out of the gate`() {
        val q = captured(Filter(kinds = listOf(1), search = "include:spam"), observer = hex)
        assertNull(q.ranking, "opted out: plain unranked recall")
        assertNull(q.minRank)
    }

    @Test
    fun `an exclusion-only search is plain recall to the gate`() {
        // "-word" leaves no positive terms, so the mapping hands the store
        // search=null — and the recall gate must own it like any plain filter.
        val q = captured(Filter(kinds = listOf(1), search = "-spamword"), observer = hex)
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking)
        assertEquals(DEFAULT_MIN_RANK, q.minRank)
        assertEquals(listOf("spamword"), q.notSearch)
        assertNull(q.search)
    }

    @Test
    fun `a phrase-only search gates through the search profile, not the recall gate`() {
        // Quoted phrases are search text — the opposite polarity from the
        // exclusion-only case above: the query stays on the search path
        // (ranking null = EventYql's default profile) with the default floor.
        val q = captured(Filter(kinds = listOf(1), search = "\"hello world\""), observer = hex)
        assertNull(q.ranking, "phrases rank like terms: the search profile owns the gate")
        assertEquals(DEFAULT_MIN_RANK, q.minRank)
        assertEquals(listOf("hello world"), q.phrases)
        assertNull(q.search)
    }

    /**
     * INCLUDING a lookup by id. Naming an event's id is not a reason to serve
     * it: if its author is outside the observer's scored network, it is not a
     * result for that observer's query. The gate costs these lookups the
     * document-API fast path (VespaEventIndex.isPureIdLookup requires an
     * unranked query), which is the accepted price.
     */
    @Test
    fun `an ids-only REQ is gated like any other plain recall`() {
        val q = captured(Filter(ids = listOf("d".repeat(64))), observer = hex)
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking)
        assertEquals(DEFAULT_MIN_RANK, q.minRank)
    }

    @Test
    fun `no observer resolved means no gate`() {
        val q = captured(Filter(kinds = listOf(1)))
        assertNull(q.ranking)
        assertNull(q.minRank)
        assertNull(q.observer)
    }

    @Test
    fun `an explicit floor rides the gate instead of the default`() {
        val q = captured(Filter(search = "filter:rank:gte:50"), observer = hex)
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking)
        assertEquals(50.0, q.minRank)
    }

    @Test
    fun `an explicit floor survives include spam, like on the search path`() {
        val q = captured(Filter(search = "filter:rank:gte:50 include:spam"), observer = hex)
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking, "include:spam only cancels the DEFAULT floor")
        assertEquals(50.0, q.minRank, "the user asked for this floor; it gates")
    }

    @Test
    fun `include spam on a search sends a ZERO floor, not an absent one`() {
        val q = captured(Filter(search = "vitor include:spam"), observer = hex)
        assertNull(q.ranking, "terms: the default search profile still owns the order")
        assertEquals(
            INCLUDE_SPAM_MIN_RANK,
            q.minRank,
            "the floor must ride the query at 0 — omitted, wot_mult's curve anchors at the " +
                "schema's -1e9 fail-open and the trust ORDER collapses to pure text",
        )
    }

    @Test
    fun `searches and sorts gate through their own profile, not the recall gate`() {
        val search = captured(Filter(search = "vitor"), observer = hex)
        assertNull(search.ranking, "terms: EventYql's default (the search profile) owns the gate")
        assertEquals(DEFAULT_MIN_RANK, search.minRank)

        val sorted = captured(Filter(search = "sort:rank"), observer = hex)
        assertEquals(EventYql.RANK_DESC, sorted.ranking, "an explicit sort is never overridden")
        assertEquals(DEFAULT_MIN_RANK, sorted.minRank)
    }

    /**
     * `sort:recent` points the RECALL gate's own profile at a search: same
     * lens, same floor, same NIP-01 order — the terms only narrow the match
     * set.
     */
    @Test
    fun `a sort recent search gates through the recall profile, terms and all`() {
        val q = captured(Filter(kinds = listOf(1), search = "vitor sort:recent"), observer = hex)
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking)
        assertEquals(DEFAULT_MIN_RANK, q.minRank)
        assertEquals(hex, q.observer)
        assertEquals("vitor", q.search, "the terms still recall — only the ORDER changed")
    }

    /**
     * With no lens the gate is inert, so a TERMLESS `sort:recent` is exactly a
     * plain filter and is handed back to the plain path — which keeps the
     * recency profile and the count-probe planner (both key on a ranking-free
     * query) instead of ranking every match to reach the same order.
     */
    @Test
    fun `a termless sort recent without an observer falls back to plain recall`() {
        val q = captured(Filter(kinds = listOf(1), search = "sort:recent"))
        assertNull(q.ranking, "nothing to gate and nothing to search: a plain NIP-01 filter")
        assertNull(q.minRank)
    }

    @Test
    fun `a sort recent SEARCH without an observer keeps the profile — it is the only thing ordering it`() {
        val q = captured(Filter(kinds = listOf(1), search = "vitor sort:recent"))
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking, "ranking-free would mean the relevance profiles")
        assertNull(q.observer, "no lens: the profile orders, nothing gates")
    }

    /** The gate's order contract, on the search path: newest first, client-side. */
    @Test
    fun `a sort recent search is re-sorted newest-first client-side`() =
        runBlocking {
            fun doc(
                id: String,
                createdAt: Long,
            ) = EventDoc(id = id, pubkey = "a".repeat(64), createdAt = createdAt, kind = 1, tags = emptyList(), content = "vitor", sig = "")
            val older = doc("1".repeat(64), 100)
            val newer = doc("2".repeat(64), 200)
            val store = NostrSemanticsStore(CapturingIndex(hits = listOf(older, newer)))
            val ids =
                withContext(StoreQueryContext(setOf(hex))) {
                    store.query<Event>(Filter(search = "vitor sort:recent")).map { it.id }
                }
            assertEquals(listOf(newer.id, older.id), ids, "a searching query, ordered like a feed")
        }

    /**
     * The gate's ORDER contract is NIP-01 recency, not engine score order: the
     * engine returns created_at via the rank score (ties arbitrary), and the
     * store re-sorts the page client-side like any plain query.
     */
    @Test
    fun `gated recall is re-sorted newest-first client-side`() =
        runBlocking {
            fun doc(
                id: String,
                createdAt: Long,
            ) = EventDoc(id = id, pubkey = "a".repeat(64), createdAt = createdAt, kind = 1, tags = emptyList(), content = "hi", sig = "")
            val older = doc("1".repeat(64), 100)
            val newer = doc("2".repeat(64), 200)
            val store = NostrSemanticsStore(CapturingIndex(hits = listOf(older, newer)))
            val ids =
                withContext(StoreQueryContext(setOf(hex))) {
                    store.query<Event>(Filter(kinds = listOf(1))).map { it.id }
                }
            assertEquals(listOf(newer.id, older.id), ids, "engine order is not trusted for the recency contract")
        }
}
