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
import com.nosfabrica.vespa.eventstore.engine.LinkScore
import com.nosfabrica.vespa.eventstore.engine.LinkTarget
import com.nosfabrica.vespa.eventstore.engine.LinkWeights
import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.client.PartialAnswer
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * One of the cluster's content nodes is offline, and every engine response
 * says so: below 100%, `non-ideal-state`. A client's REQ or NIP-50 search
 * installed [AcceptPartial] and is served from the documents the cluster
 * could still search; every other read refuses as it always has, because the
 * store cannot know whether its caller decides something from the answer.
 */
class AcceptPartialTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)
    private val store = NostrSemanticsStore(index)

    @AfterTest
    fun tearDown() {
        index.close()
        mock.stop()
    }

    private val note =
        EventDoc(
            id = "1".repeat(64),
            pubkey = "a".repeat(64),
            createdAt = 1_700_000_000,
            kind = 1,
            tags = emptyList(),
            content = "hello partial world",
            sig = "e".repeat(128),
            search = SearchFields(text = "hello partial world"),
        )

    @Test
    fun `a client read under AcceptPartial is served while a node is missing`() =
        runBlocking {
            mock.inner.put(note)
            mock.nonIdealStateCoverage = true

            withContext(AcceptPartial) {
                assertEquals(listOf(note.id), store.query<Event>(Filter(kinds = listOf(1), limit = 10)).map { it.id }, "a feed REQ")
                assertEquals(listOf(note.id), store.query<Event>(Filter(kinds = listOf(1), search = "hello")).map { it.id }, "a NIP-50 search")
                val raw = ArrayList<String>()
                store.rawQuery(listOf(Filter(kinds = listOf(1), limit = 10))) { raw += it.id }
                assertEquals(listOf(note.id), raw, "the raw path a relay serves REQs from")
            }
        }

    @Test
    fun `without AcceptPartial the same read refuses`() =
        runBlocking {
            mock.inner.put(note)
            mock.nonIdealStateCoverage = true

            val refused = runCatching { store.query<Event>(Filter(kinds = listOf(1), limit = 10)) }.exceptionOrNull()
            assertTrue(refused?.message.orEmpty().contains("PARTIAL"), "a consumer's own read must not act on part of the corpus: $refused")
        }

    /** A NIP-45 count over part of the corpus is a wrong number, not a short one: the marker does not reach it. */
    @Test
    fun `a count refuses even under AcceptPartial`() =
        runBlocking {
            mock.inner.put(note)
            mock.nonIdealStateCoverage = true

            assertTrue(
                withContext(AcceptPartial) { runCatching { store.count(Filter(kinds = listOf(1))) } }.isFailure,
                "the count must refuse",
            )
        }

    /**
     * A cluster with a node down, over the in-memory reference: a read that does not accept a
     * partial answer is refused, and one that does is answered without the documents on [hidden].
     * Every read is recorded, so a test can ask which ones carried the allowance.
     */
    private class Outage(
        private val inner: EventIndex,
    ) : EventIndex by inner {
        @Volatile var down = false
        val hidden: MutableSet<String> = Collections.synchronizedSet(HashSet())
        val asked: MutableList<Pair<String, EventQuery>> = Collections.synchronizedList(ArrayList())

        private fun check(
            call: String,
            q: EventQuery,
        ) {
            asked += call to q
            if (down && !q.acceptsPartial) throw PartialAnswer("vespa searched only 75% of the corpus (non-ideal-state)", "non-ideal-state")
        }

        private fun reachable(id: String) = !down || id !in hidden

        override suspend fun search(query: EventQuery): List<EventDoc> {
            check("search", query)
            return inner.search(query).filter { reachable(it.id) }
        }

        override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> {
            check("searchRanked", query)
            return inner.search(query).filter { reachable(it.id) }.map { Ranked(it, 1.0) }
        }

        override suspend fun rawSearch(query: EventQuery): List<RawEvent> {
            check("rawSearch", query)
            return inner.rawSearch(query).filter { reachable(it.id) }
        }

        override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> {
            check("rawSearchRanked", query)
            return inner.rawSearch(query).filter { reachable(it.id) }.map { Ranked(it, 1.0) }
        }

        override suspend fun count(query: EventQuery): Int {
            check("count", query)
            return inner.count(query)
        }

        override suspend fun scoreLinks(
            targets: List<LinkTarget>,
            linkers: EventQuery,
            weights: LinkWeights,
        ): Map<String, LinkScore> {
            check("scoreLinks", linkers)
            return inner.scoreLinks(targets, linkers, weights)
        }
    }

    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private var seq = 0

    private fun id() = (++seq).toString().padStart(64, '0')

    private fun list10040(owner: String) = TrustProviderListEvent(id(), owner, 1_000L + seq, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")

    private fun trusted(outage: Outage) = NostrSemanticsStore(TrustProjection(outage, InMemoryReputationIndex()))

    private fun isProviderRead(q: EventQuery) = q.kinds == listOf(TrustProviderListEvent.KIND)

    /**
     * A COLD provider map (a restart, or any 10040 write) used to refuse every lensed read for the
     * whole outage, because the lens is resolved strictly before the page is read. Under
     * [AcceptPartial] it resolves from the lists the cluster can still reach; an observer whose
     * list is not among them is refused rather than ranked as trusting nobody, and nothing read
     * partial outlives the outage.
     */
    @Test
    fun `a lensed read with a cold provider map is served while a node is missing`() =
        runBlocking {
            val outage = Outage(InMemoryEventIndex())
            val store = trusted(outage)
            val unlucky = "0c".repeat(32)
            store.insert(list10040(observer))
            val lost = list10040(unlucky).also { store.insert(it) }
            val note = TextNoteEvent(id(), "a1".repeat(32), 2_000L, emptyArray(), "hello", "").also { store.insert(it) }
            outage.hidden += lost.id
            outage.down = true

            val lensed = Filter(kinds = listOf(1), search = "observer:$observer")
            assertEquals(listOf(note.id), withContext(AcceptPartial) { store.query<Event>(listOf(lensed)) }.map { it.id })
            assertTrue(runCatching { store.query<Event>(listOf(lensed)) }.isFailure, "and a strict read is still refused")

            val refused = withContext(AcceptPartial) { runCatching { store.query<Event>(listOf(Filter(kinds = listOf(1), search = "observer:$unlucky"))) } }.exceptionOrNull()
            assertEquals(PartialAnswer.LENS, (refused as? PartialAnswer)?.degradation, "a lens on the missing node is refused, not emptied: $refused")

            outage.down = false
            assertEquals(listOf(note.id), store.query<Event>(listOf(Filter(kinds = listOf(1), search = "observer:$unlucky"))).map { it.id }, "the partial pass was never cached")
        }

    /** Every engine read a searching page makes carries the allowance: the expansion's companions and the `sort:links` rerank too. */
    @Test
    fun `every read a NIP-50 page makes accepts a partial answer`() =
        runBlocking {
            val outage = Outage(InMemoryEventIndex())
            val store = trusted(outage)
            store.insert(list10040(observer))
            val note = TextNoteEvent(id(), "a1".repeat(32), 2_000L, emptyArray(), "hello world", "").also { store.insert(it) }
            outage.down = true
            outage.asked.clear()

            val page = withContext(AcceptPartial) { store.query<Event>(listOf(Filter(kinds = listOf(1), search = "hello sort:links observer:$observer"))) }
            assertEquals(listOf(note.id), page.map { it.id })

            val reads = outage.asked.filterNot { isProviderRead(it.second) }
            assertTrue(reads.size > 1, "the expansion's companion reads ran too: ${reads.map { it.first }}")
            assertTrue(reads.any { it.first == "scoreLinks" }, "the rerank ran: ${reads.map { it.first }}")
            assertTrue(reads.all { it.second.acceptsPartial }, "every page read carries the allowance: ${reads.filterNot { it.second.acceptsPartial }.map { it.first }}")
        }

    @Test
    fun `a scored profile search is served while a node is missing`() =
        runBlocking {
            val outage = Outage(InMemoryEventIndex())
            val store = trusted(outage)
            val alice = "a1".repeat(32)
            store.insert(list10040(observer))
            store.insert(MetadataEvent(id(), alice, 2_000L, emptyArray(), """{"name":"alice"}""", ""))
            outage.down = true

            assertEquals(listOf(alice), withContext(AcceptPartial) { store.scoredProfileSearch("alice", observer, limit = 10) }.hits.map { it.pubkey })
            assertTrue(runCatching { store.scoredProfileSearch("alice", observer, limit = 10) }.isFailure)
        }
}
