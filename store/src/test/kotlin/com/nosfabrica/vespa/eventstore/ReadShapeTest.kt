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

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.LinkScore
import com.nosfabrica.vespa.eventstore.engine.LinkTarget
import com.nosfabrica.vespa.eventstore.engine.LinkWeights
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.toEventQuery
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip25Reactions.ReactionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [ReadShape] must say what the store will DO with a filter, so it is held to
 * the compiled query rather than to a second reading of the grammar: for every
 * search string below, its observer and spam waiver are the query's, and
 * "ranked" is exactly the condition that stamps the default trust floor
 * (`minRank`) on a read with no explicit `filter:`.
 */
class ReadShapeTest {
    private val hex = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

    private val battery =
        listOf(
            null,
            "",
            "bitcoin",
            "\"exact phrase\"",
            "-spam",
            "-\"a phrase\"",
            // recognised sorts, with and without text
            "sort:rank",
            "sort:rank:asc",
            "sort:followers",
            "sort:text",
            "sort:recent",
            "nostr sort:recent",
            // unknown and malformed extensions: the store ignores them
            "sort:bogus",
            "sort:",
            "nostr sort:bogus",
            "filter:rank:gte:nope",
            "observer:npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq",
            "observer:${hex.take(63)}",
            "observer:${hex.uppercase()}",
            "observer:$hex",
            "observer:$hex bitcoin",
            "include:spam",
            "include:spam sort:rank",
            "include:spamx",
            "language:pt",
            "unknown:thing",
        )

    @Test
    fun `the shape agrees with the compiled query on every extension`() {
        for (search in battery) {
            val shape = ReadShape.of(Filter(kinds = listOf(1), search = search))
            val q = Filter(kinds = listOf(1), search = search).toEventQuery()!!
            assertEquals(q.observer, shape.observer, "observer for `$search`")
            assertEquals(q.includeSpam, shape.includeSpam, "include:spam for `$search`")
            assertEquals(q.minRank != null, shape.ranked, "ranked for `$search` (minRank=${q.minRank})")
        }
    }

    @Test
    fun `an unknown sort is not a ranked read, which is where the relay's copy disagreed`() {
        assertFalse(ReadShape.of("sort:bogus").ranked)
        assertNull(Filter(search = "sort:bogus").toEventQuery()!!.ranking, "the store serves it as plain recall")
        assertTrue(ReadShape.of("sort:recent").ranked)
        assertTrue(ReadShape.of("\"just a phrase\"").ranked, "a phrase is text")
        assertFalse(ReadShape.of("-only -exclusions").ranked, "exclusions alone are recall minus words")
    }

    @Test
    fun `a lens is declared by a usable observer or the spam waiver, never by an npub`() {
        assertEquals(hex, ReadShape.of("observer:${hex.uppercase()}").observer, "lowercased, as the store reads it")
        assertTrue(ReadShape.of("observer:$hex").declaresLens)
        assertTrue(ReadShape.of("include:spam").declaresLens)
        assertFalse(ReadShape.of("observer:npub1abc").declaresLens, "an npub is ignored by the store, so it declares nothing")
        assertFalse(ReadShape.of("bitcoin").declaresLens)
        assertFalse(ReadShape.of(null).declaresLens)
    }

    /** Two shapes `equals` tells apart print apart, or a log line reads two different verdicts as one. */
    @Test
    fun `toString names every field equals compares`() {
        val shapes = (battery + listOf("include:spam filter:rank:gte:50", "filter:rank:gte:50")).map(ReadShape::of).distinct()
        assertEquals(shapes.size, shapes.map { it.toString() }.distinct().size, "$shapes")
        assertTrue("floored=true" in ReadShape.of("include:spam filter:rank:gte:50").toString())
    }

    @Test
    fun `a filter that matches nothing still has a shape`() {
        assertEquals(ReadShape.of("observer:$hex sort:rank"), ReadShape.of(Filter(ids = emptyList(), search = "observer:$hex sort:rank")))
    }

    private val alice = "a".repeat(64)
    private val bob = "b".repeat(64)

    /**
     * What a NIP-77 snapshot of [search] sends the engine as [connection]: the parts that decide its
     * set, and every lens lookup (a kind-10040 read) it made on the way.
     *
     * Through the TRUST PROJECTION, with [alice] and [bob] each naming a different rank service, so
     * a lens read through either connection shows as the service it resolved to, and a lookup the
     * anonymous read does not make shows as one more line. A store without the projection resolves
     * no lens at all, and could not tell a read that depends on its caller's lens (or on that
     * lookup succeeding) from one that does not.
     */
    private fun snapshotQueries(
        search: String?,
        connection: String?,
    ): List<String> =
        runBlocking {
            val seen = mutableListOf<EventQuery>()
            val inner = InMemoryEventIndex()
            val index =
                object : EventIndex by inner {
                    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query).also { seen += query }

                    override suspend fun visitIds(
                        query: EventQuery,
                        withDTag: Boolean,
                        onPage: suspend (List<DocRef>) -> Boolean,
                    ) {
                        seen += query
                        inner.visitIds(query, withDTag, onPage)
                    }
                }
            val store = NostrSemanticsStore(TrustProjection(index, InMemoryReputationIndex()))
            store.batchInsert(
                listOf(alice to "5a", bob to "5b").mapIndexed { i, (who, service) ->
                    TrustProviderListEvent("${i + 1}".padStart(64, '0'), who, 1_000L, arrayOf(arrayOf("30382:rank", service.repeat(32), "wss://scores.example.com/")), "", "")
                },
            )
            seen.clear()
            val filters = listOf(Filter(kinds = listOf(1), search = search))
            if (connection == null) store.snapshotIdsForNegentropy(filters, null, null) else withContext(StoreQueryContext(setOf(connection))) { store.snapshotIdsForNegentropy(filters, null, null) }
            seen.map { q ->
                if (TrustProviderListEvent.KIND in q.kinds) {
                    "lens lookup"
                } else {
                    "rankKey=${q.rankKey} followersKey=${q.followersKey} ranking=${q.ranking} minRank=${q.minRank} gate=${EventYql.gateFloor(q)}"
                }
            }
        }

    @Test
    fun `a read that answers every connection alike compiles the same for every connection`() {
        val extra = listOf("include:spam filter:rank:gte:50", "include:spam bitcoin", "include:spam -word", "include:spam sort:recent", "observer:$hex include:spam sort:rank")
        var alike = 0
        for (search in battery + extra) {
            val shape = ReadShape.of(search)
            val asAlice = snapshotQueries(search, alice)
            val same = asAlice == snapshotQueries(search, bob) && asAlice == snapshotQueries(search, null)
            if (shape.answersEveryConnectionAlike) {
                alike++
                assertTrue(same, "`$search` claims to answer alike, but compiles per caller: $asAlice vs ${snapshotQueries(search, null)}")
            } else {
                assertFalse(same, "`$search` compiles alike for every connection, so it should say so")
            }
        }
        assertTrue(alike >= 4, "the battery exercises the claim")
        assertTrue(ReadShape.of("include:spam").answersEveryConnectionAlike)
        assertTrue(ReadShape.of("observer:$hex bitcoin").answersEveryConnectionAlike, "a named lens wins over the connection's")
        assertFalse(ReadShape.of("include:spam filter:rank:gte:50").answersEveryConnectionAlike, "an explicit floor gates through the connection's lens")
        assertFalse(ReadShape.of("include:spam bitcoin").answersEveryConnectionAlike, "a ranked search's top N is the caller's")
        assertFalse(ReadShape.of(null).answersEveryConnectionAlike)
    }

    /**
     * Every engine request a REQ of [search] (as events, then as raw rows), then its COUNT, makes as
     * [connection], in order: the compiled request
     * ([EventYql.build], what the Vespa client sends) of each recall, and of the linkers a
     * `sort:links` rerank scores by. The same fixture as [snapshotQueries], with a note corpus a
     * search finds and a reaction a rerank would count; and the recall REPORTS A SCORE, as the real
     * engine does, so the rerank is not skipped for want of one (the in-memory reference ranks
     * nothing, and a page with no scores is never rescored).
     */
    private fun reqRequests(
        search: String?,
        connection: String?,
    ): List<String> =
        runBlocking {
            val seen = mutableListOf<String>()
            val inner = InMemoryEventIndex()
            val index =
                object : EventIndex by inner {
                    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query).also { seen += "search ${render(query)}" }

                    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = inner.search(query).map { Ranked(it, 1.0) }.also { seen += "searchRanked ${render(query)}" }

                    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = inner.rawSearch(query).also { seen += "rawSearch ${render(query)}" }

                    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = inner.rawSearch(query).map { Ranked(it, 1.0) }.also { seen += "rawSearchRanked ${render(query)}" }

                    override suspend fun scoreLinks(
                        targets: List<LinkTarget>,
                        linkers: EventQuery,
                        weights: LinkWeights,
                    ): Map<String, LinkScore> = inner.scoreLinks(targets, linkers, weights).also { seen += "scoreLinks ${render(linkers)}" }

                    override suspend fun count(query: EventQuery): Int = inner.count(query).also { seen += "count ${render(query)}" }

                    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> = inner.countByAuthor(query).also { seen += "countByAuthor ${render(query)}" }
                }
            // A fixed clock: the expiry cutoff and the recency anchor are in every request.
            val store = NostrSemanticsStore(TrustProjection(index, InMemoryReputationIndex()), nowSecs = { 2_000L })
            val note = TextNoteEvent("e".repeat(64), "c".repeat(64), 1_000L, emptyArray(), "bitcoin note", "")
            store.batchInsert(
                listOf(alice to "5a", bob to "5b").mapIndexed { i, (who, service) ->
                    TrustProviderListEvent("${i + 1}".padStart(64, '0'), who, 1_000L, arrayOf(arrayOf("30382:rank", service.repeat(32), "wss://scores.example.com/")), "", "")
                } +
                    listOf(
                        note,
                        TextNoteEvent("f".repeat(64), "d".repeat(64), 1_001L, emptyArray(), "another bitcoin note", ""),
                        ReactionEvent("9".repeat(64), alice, 1_002L, arrayOf(arrayOf("e", note.id), arrayOf("p", note.pubKey)), "+", ""),
                    ),
            )
            seen.clear()
            val filters = listOf(Filter(kinds = listOf(1), search = search, limit = 10))
            val read: suspend () -> Unit = {
                store.query<Event>(filters)
                seen += "-- raw"
                store.rawQuery(filters) {}
                seen += "-- count"
                store.count(filters)
            }
            if (connection == null) read() else withContext(StoreQueryContext(setOf(connection))) { read() }
            seen
        }

    /** The compiled request, less the identity hash a value without its own `toString` prints (`AuthorFilter@1a2b`). */
    private fun render(q: EventQuery): String = if (TrustProviderListEvent.KIND in q.kinds) "lens lookup" else EventYql.build(q).toString().replace(Regex("@[0-9a-f]+\\b"), "")

    /**
     * THE CLAIM HOLDS FOR THE REQ AND ITS COUNT TOO, not only for the NEG-OPEN walk the relay shares a snapshot of:
     * a REQ of a filter that answers every connection alike sends the engine the same requests for
     * every connection. A REQ does more per caller than the walk does — the `sort:links` rerank reads
     * the caller's trusted linkers, the reference expansion unpacks the declarations the caller
     * enrolled, the topical-intent probe ranks the caller's notable names — and every one of those
     * needs a read that is ranked or reads a lens, which [ReadShape.answersEveryConnectionAlike]
     * already excludes: a termless `include:spam sort:links` is plain recall (the store does not
     * know `links` as a profile, the rerank needs a relevance-ordered page and a resolved lens, and
     * this read resolves none), while a named `observer:` supplies the lens every step reads. Each
     * shape the relay could share is listed, and each one it must not, so a step that starts reading
     * the caller under a shape still called alike fails here.
     */
    @Test
    fun `a read that answers every connection alike serves the same REQ and COUNT for every connection`() {
        val links =
            listOf(
                "include:spam sort:links",
                "include:spam sort:links:2",
                "sort:links",
                "bitcoin sort:links",
                "include:spam bitcoin sort:links",
                "observer:$hex bitcoin sort:links",
                // A named lens that resolves (alice's 10040 names a service), so the rerank runs through it.
                "observer:$alice bitcoin sort:links",
                "observer:$alice include:spam bitcoin sort:links",
                "observer:$alice include:spam sort:links",
                "observer:$hex sort:links",
                "include:spam bitcoin",
                "include:spam \"bitcoin note\"",
                "include:spam -bitcoin",
                "include:spam sort:recent",
                "include:spam filter:rank:gte:50",
            )
        var alike = 0
        for (search in battery + links) {
            val asAlice = reqRequests(search, alice)
            val asBob = reqRequests(search, bob)
            val anonymous = reqRequests(search, null)
            val same = asAlice == asBob && asAlice == anonymous
            if (ReadShape.of(search).answersEveryConnectionAlike) {
                alike++
                assertTrue(same, "`$search` claims to answer alike, but its REQ asks per caller: as alice ${asAlice - anonymous.toSet()}, as bob ${asBob - anonymous.toSet()}, anonymously ${anonymous - asAlice.toSet()}")
            } else {
                assertFalse(same, "`$search` serves its REQ alike for every connection, so it should say so")
            }
        }
        assertTrue(alike >= 6, "the battery exercises the claim")
        // The rerank does run where a caller's lens is read, so the battery is not vacuous on it.
        assertTrue(reqRequests("include:spam bitcoin sort:links", alice).any { it.startsWith("scoreLinks") }, "a ranked links read reranks through the caller's lens")
        assertTrue(reqRequests("observer:$alice bitcoin sort:links", bob).any { it.startsWith("scoreLinks") }, "and through a named one")
        for (termless in listOf("include:spam sort:links", "sort:links", "observer:$alice sort:links")) {
            assertTrue(reqRequests(termless, alice).none { it.startsWith("scoreLinks") }, "`$termless` is a recency read, which is never reranked")
        }
    }
}
