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

import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.toEventQuery
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The two predicates a relay hands quartz's `LiveEventStore`. The write half is vespa-relay's
 * `FilterLocalWritesTest` cases, moved here with the predicate; the search half is one row per
 * search shape, each with the reason the store's own compile gives.
 */
class FilterLocalityTest {
    private val pk = "a1".repeat(32)
    private val hex = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

    private fun event(
        kind: Int,
        vararg tags: Array<String>,
    ): Event = Event("0".repeat(64), pk, 1_000L, kind, arrayOf(*tags), "", "")

    @Test
    fun `writes the gate reads drop every snapshot`() {
        val gateWrites =
            mapOf(
                "a card" to event(UserAssertionEvent.KIND, arrayOf("d", "b".repeat(64)), arrayOf("rank", "90")),
                "a card that only counts followers" to event(UserAssertionEvent.KIND, arrayOf("d", "b".repeat(64)), arrayOf("followers", "12")),
                "a provider list" to event(TrustProviderListEvent.KIND, arrayOf("30382:rank", "c".repeat(64), "wss://scores.example.com/")),
                "a vanish" to event(RequestToVanishEvent.KIND, arrayOf("relay", "ALL_RELAYS")),
                "a deletion by id" to event(DeletionRequestEvent.KIND, arrayOf("e", "d".repeat(64))),
                "a deletion by id that says its kind" to event(DeletionRequestEvent.KIND, arrayOf("e", "d".repeat(64)), arrayOf("k", "1")),
                "a deletion of a card's address" to event(DeletionRequestEvent.KIND, arrayOf("a", "30382:$pk:${"b".repeat(64)}")),
                "a deletion of a provider list's address" to event(DeletionRequestEvent.KIND, arrayOf("a", "10040:$pk:")),
                "a deletion of an address it cannot parse" to event(DeletionRequestEvent.KIND, arrayOf("a", "nonsense")),
                "a deletion of an article and a card" to event(DeletionRequestEvent.KIND, arrayOf("a", "30023:$pk:post"), arrayOf("a", "30382:$pk:x")),
            )
        for ((what, e) in gateWrites) assertFalse(FilterLocality.writeIsFilterLocal(e), "$what can change the gate")
    }

    /** The kinds the projection reacts to are exactly the ones the write half refuses outright. */
    @Test
    fun `every kind the trust projection reacts to drops every snapshot`() {
        for (kind in TrustProjection.TRUST_KINDS) assertFalse(FilterLocality.writeIsFilterLocal(event(kind)), "kind $kind feeds the projection")
    }

    /**
     * Follow lists, reports, labels, other assertions and Trusted Lists feed the text search, the
     * reference expansion and the `sort:links` rerank, none of which a search snapshot is kept
     * across ([FilterLocality.searchIsFilterLocal] rejects all three).
     */
    @Test
    fun `writes the gate does not read keep the snapshots they do not match`() {
        val ordinary =
            mapOf(
                "a note" to event(1),
                "a reaction" to event(7, arrayOf("e", "d".repeat(64))),
                "a repost" to event(6, arrayOf("e", "d".repeat(64))),
                "a profile" to event(0),
                "a follow list" to event(3, arrayOf("p", "b".repeat(64))),
                "a report" to event(1984, arrayOf("p", "b".repeat(64), "spam")),
                "a label" to event(1985, arrayOf("L", "topic"), arrayOf("l", "podcaster", "topic"), arrayOf("p", "b".repeat(64))),
                "an article" to event(30023, arrayOf("d", "post")),
                "an event assertion" to event(30383, arrayOf("d", "d".repeat(64))),
                "a Trusted List" to event(30392, arrayOf("d", "list")),
                "a deletion of an article's address" to event(DeletionRequestEvent.KIND, arrayOf("a", "30023:$pk:post"), arrayOf("k", "30023")),
                "a deletion naming nothing" to event(DeletionRequestEvent.KIND),
            )
        for ((what, e) in ordinary) assertTrue(FilterLocality.writeIsFilterLocal(e), "$what leaves the gate alone")
    }

    private fun local(
        search: String?,
        limit: Int? = null,
    ) = FilterLocality.searchIsFilterLocal(Filter(kinds = listOf(1), search = search, limit = limit))

    @Test
    fun `no search is a plain filter`() {
        assertTrue(local(null))
        assertTrue(local(""))
        assertTrue(local("   ", limit = 10))
    }

    /** A waived floor, a lens, a raised floor and a chronological sort all walk created_at-ordered sets the gate alone narrows. */
    @Test
    fun `termless gates and waivers keep their snapshot, limit or not`() {
        for (search in listOf("include:spam", "observer:$hex", "observer:$hex include:spam", "filter:rank:gte:20", "observer:$hex filter:rank:gt:50", "sort:recent", "observer:$hex sort:recent", "-spam", "-\"a phrase\" observer:$hex", "language:pt")) {
            assertTrue(local(search), "$search, unlimited")
            assertTrue(local(search, limit = 500), "$search, limited: the newest N of a gated set")
        }
    }

    /** bm25 IDF and the score floor it feeds move with every write anywhere in the corpus. */
    @Test
    fun `any text drops the snapshot on every write`() {
        for (search in listOf("bitcoin", "\"exact phrase\"", "bitcoin include:spam", "bitcoin observer:$hex", "bitcoin sort:recent", "bitcoin sort:rank", "bitcoin language:pt", "⚡", "₿", ":verified:")) {
            assertFalse(local(search), "$search, unlimited")
            assertFalse(local(search, limit = 500), "$search, limited")
        }
    }

    /** A trust sort's first N are its most trusted, cut inside ties the engine resolves; with no cut, its set is the gated set. */
    @Test
    fun `a termless trust sort keeps its snapshot only without a limit`() {
        for (search in listOf("sort:rank", "sort:rank:desc", "sort:rank:asc", "sort:followers", "sort:text", "observer:$hex sort:rank", "include:spam sort:followers", "filter:rank:gte:10 sort:rank:asc")) {
            assertTrue(local(search), "$search, unlimited: the gated set")
            assertFalse(local(search, limit = 500), "$search, limited: the top N by trust")
        }
    }

    /** The rerank cuts a widened recall back by reactions, zaps and reports, which no write predicate guards. */
    @Test
    fun `the links rerank drops the snapshot on every write`() {
        for (search in listOf("sort:links", "sort:links:2", "observer:$hex sort:links", "include:spam sort:links:0.5")) {
            assertFalse(local(search), search)
            assertFalse(local(search, limit = 50), search)
        }
    }

    /**
     * These are true because the store IGNORES them — they compile to the query the filter makes
     * without them. A token the store starts honouring fails here, and must then be classified in
     * [FilterLocality.searchIsFilterLocal] before it can pass.
     */
    @Test
    fun `tokens the store ignores compile to nothing and keep the snapshot`() {
        for (token in listOf("domain:example.com", "nsfw:false", "nsfw:true", "sentiment:positive", "sort:bogus", "foo:bar", "language:klingon", "language:en")) {
            for (base in listOf("", "include:spam", "observer:$hex")) {
                val with = Filter(kinds = listOf(1), search = "$base $token".trim(), limit = 100)
                val without = Filter(kinds = listOf(1), search = base.ifEmpty { null }, limit = 100)
                assertEquals(without.toEventQuery(), with.toEventQuery(), "'$token' beside '$base' must be ignored")
                assertTrue(FilterLocality.searchIsFilterLocal(with), "'$token' beside '$base'")
            }
        }
    }

    /** `language:` reaches the query, but only the text clause reads it: a termless walk is the same request without it. */
    @Test
    fun `a language hint with no text compiles to the same walk`() {
        for (language in listOf("pt", "es")) {
            val with = Filter(kinds = listOf(1), search = "language:$language observer:$hex", limit = 100).toEventQuery()!!
            val without = Filter(kinds = listOf(1), search = "observer:$hex", limit = 100).toEventQuery()!!
            assertEquals(language, with.stemLanguage, "the hint is honoured, so it is not merely ignored")
            assertEquals(EventYql.buildIdTime(without), EventYql.buildIdTime(with))
        }
    }

    @Test
    fun `a filter that can never match keeps its empty snapshot`() {
        assertTrue(FilterLocality.searchIsFilterLocal(Filter(ids = emptyList(), search = "bitcoin")))
    }
}
