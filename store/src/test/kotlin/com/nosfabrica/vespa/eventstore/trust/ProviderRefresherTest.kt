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
package com.nosfabrica.vespa.eventstore.trust

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.runtime.BackgroundFailures
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * TWO STORES OVER ONE INDEX, for the kind-10040 provider pass — what
 * vespa-relay runs: a serving relay and a sync router, each with its own
 * store and so its own [ProviderMap], feeding one Vespa.
 *
 * A pass is dropped by every 10040 written through ITS store and by nothing
 * else, so a list the other process stored reached this one's lens and
 * Trusted List gate only at its next local 10040 write or restart — on
 * staging, nine days, 24 of 47 recent observers resolving no lens (#145).
 * The first test here is that report, reproduced; it failed before the
 * refresher. `store` is the process under test ("the relay"), `other` the
 * second writer ("the sync"); every refresh is driven explicitly except in
 * the two tests that pin the timer itself.
 */
class ProviderRefresherTest {
    private val relayUrl = "wss://sot.test/".normalizeRelayUrl()
    private val reader = key("a1")
    private val curator = key("b2")
    private val subject = key("d4")
    private val bystander = key("e5")
    private val serviceA = key("5e")
    private val serviceB = key("6e")
    private val serviceNew = key("7e")

    private val shared = InMemoryEventIndex()
    private val reputations = InMemoryReputationIndex()

    /** Flip to make the process under test's 10040 reads fail, or read nothing — a refresh during an outage. */
    private var listReads: ListReads = ListReads.SERVE

    private enum class ListReads { SERVE, THROW, EMPTY }

    private val flaky =
        object : EventIndex by shared {
            override suspend fun search(query: EventQuery): List<EventDoc> {
                if (query.kinds == listOf(TrustProviderListEvent.KIND) && query.authors.isEmpty()) {
                    when (listReads) {
                        ListReads.THROW -> error("engine unavailable")
                        ListReads.EMPTY -> return emptyList()
                        ListReads.SERVE -> Unit
                    }
                }
                return shared.search(query)
            }
        }

    private val projection = TrustProjection(flaky, reputations)

    /** Refresher timers OFF by default: these tests refresh on demand, not on a clock. */
    private val store = NostrSemanticsStore(projection, relay = relayUrl, providerRefreshMillis = 0)
    private val other = NostrSemanticsStore(TrustProjection(shared, reputations), relay = relayUrl, providerRefreshMillis = 0)

    private val opened = mutableListOf(store, other)

    @AfterTest
    fun close() = opened.forEach { it.close() }

    private var t = 1_000_000L
    private var seq = 0

    private fun next() = t++

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun key(prefix: String) = prefix.repeat(32)

    private fun event(
        kind: Int,
        tags: Array<Array<String>>,
        content: String = "",
        author: String = curator,
    ): Event = EventFactory.create(id(), author, next(), kind, tags, content, "")

    private fun treasureMap(
        owner: String,
        vararg entries: Array<String>,
    ) = event(10040, arrayOf(*entries), author = owner)

    private fun rankedBy(
        owner: String,
        service: String,
    ) = treasureMap(owner, arrayOf("30382:rank", service, "wss://scores.example/"))

    private fun card(
        signer: String,
        about: String = subject,
        rank: Int = 87,
    ) = event(30382, arrayOf(arrayOf("d", about), arrayOf("rank", rank.toString())), author = signer)

    private val profile = event(0, emptyArray(), """{"name":"Ada Bramble"}""", author = subject)

    private val trustedList =
        event(30392, arrayOf(arrayOf("d", "roster"), arrayOf("title", "Podcaster Trust List"), arrayOf("p", subject, "", "80")))

    private suspend fun lensOf(observer: String) = projection.recompute.providerMap().lensOf(observer)

    /** A search for the list's title as [reader] on the process under test: its members splice only if the gate admits the list's signer. */
    private suspend fun podcasterPage(): List<String> = store.query<Event>(Filter(kinds = listOf(0, 30392), search = "podcaster include:spam observer:$reader")).map { it.id }

    /** A bystander's list, so the process under test has a non-empty pass to cache — an empty one is never cached. */
    private suspend fun warm() {
        store.insert(rankedBy(bystander, serviceA))
        assertEquals(serviceA, lensOf(bystander).rank, "warm: the pass is built and cached")
    }

    @Test
    fun `a 10040 another process stores reaches this process's lens at the next refresh`() =
        runBlocking {
            warm()
            store.insert(rankedBy(reader, serviceA))
            assertEquals(serviceA, lensOf(reader).rank)

            // The sync mirrors the reader's newer list, re-pointed, and a
            // first-ever list from someone else.
            other.insert(rankedBy(reader, serviceB))
            other.insert(rankedBy(curator, serviceB))
            assertEquals(serviceA, lensOf(reader).rank, "the premise: nothing here was invalidated, so the pass is stale")
            assertNull(lensOf(curator).rank, "the premise: an observer only the other process stored has no lens here")

            store.refreshTrustProviders()
            assertEquals(serviceB, lensOf(reader).rank, "the re-pointed list applies")
            assertEquals(serviceB, lensOf(curator).rank, "the new observer resolves")
        }

    @Test
    fun `a Trusted List delegation another process stored unpacks after a refresh`() =
        runBlocking {
            warm()
            store.insert(profile)
            store.insert(trustedList)
            // Stored through the process under test: rank only, no 30392 row.
            store.insert(rankedBy(reader, serviceA))
            assertEquals(listOf(trustedList.id), podcasterPage(), "no delegation, so only the list itself matches")

            // The sync mirrors the reader's newer list, which adds the bare
            // 30392 row for the curator and switches the rank service.
            other.insert(
                treasureMap(reader, arrayOf("30382:rank", serviceB, "wss://scores.example/"), arrayOf("30392", curator, "wss://lists.example/")),
            )
            assertEquals(listOf(trustedList.id), podcasterPage(), "the premise: the gate still reads the stale pass")

            store.refreshTrustProviders()
            assertEquals(listOf(trustedList.id, profile.id), podcasterPage(), "the member splices")
            assertEquals(serviceB, lensOf(reader).rank)
        }

    @Test
    fun `a refresh that fails keeps the previous pass`() =
        runBlocking {
            warm()
            store.insert(rankedBy(reader, serviceA))
            // Rebuilt after that write's invalidation: what is cached is what a
            // failed refresh must leave standing. (A COLD pass has nothing to
            // fall back on and fails the read, as it always has.)
            assertEquals(serviceA, lensOf(reader).rank)
            other.insert(rankedBy(reader, serviceB))

            listReads = ListReads.THROW
            assertFailsWith<IllegalStateException> { store.refreshTrustProviders() }
            assertEquals(serviceA, lensOf(reader).rank, "stale by an interval, never emptied")

            listReads = ListReads.SERVE
            store.refreshTrustProviders()
            assertEquals(serviceB, lensOf(reader).rank, "the next good refresh catches up")
        }

    @Test
    fun `a refresh that reads no lists keeps the previous pass`() =
        runBlocking {
            warm()
            store.insert(rankedBy(reader, serviceA))
            assertEquals(serviceA, lensOf(reader).rank)
            listReads = ListReads.EMPTY
            store.refreshTrustProviders()
            // A relay that held lists a moment ago and reads none now is one
            // whose engine is not serving them; every observer losing their
            // lens is the wrong answer to that.
            listReads = ListReads.SERVE
            assertEquals(serviceA, lensOf(reader).rank)
            assertEquals(serviceA, lensOf(bystander).rank)
        }

    /**
     * The other direction of the same hole: a card is applied as a cell only
     * if the process handed it names its signer, so the sync skipped every
     * card by a service first named on the relay's socket. A refresh that
     * finds such a service queues its walk, which projects what was skipped.
     */
    @Test
    fun `a service first named by another process is walked, projecting the cards skipped meanwhile`() =
        runBlocking {
            warm()
            other.insert(rankedBy(reader, serviceNew))
            store.insert(card(serviceNew))
            assertTrue(reputations.get(subject)?.influenceScores.isNullOrEmpty(), "the premise: the card was skipped here")

            store.refreshTrustProviders()
            assertEquals(serviceCells(serviceNew to 87), reputations.get(subject)?.influenceScores, "the walk projected it")
        }

    @Test
    fun `a service a local write already walks is not handed to the refresher again`() =
        runBlocking {
            warm()
            store.insert(rankedBy(reader, serviceNew))
            projection.recompute.providerMap() // the cold rebuild after that write's invalidation
            assertFalse(projection.recompute.hasDiscoveredServices(), "the write declared the walk; the refresher must not")
        }

    @Test
    fun `explain reports a stale pass as a stale pass, not as the reader's list`() =
        runBlocking {
            warm()
            other.insert(rankedBy(reader, serviceB))
            val explain = TrustExplain(flaky, reputations, projection.recompute)

            val stale = explain.explain(reader)
            assertTrue(stale.passDisagrees)
            assertContains(stale.summary, "cached provider pass")
            assertEquals(serviceB, stale.storedRankService)
            assertNull(stale.rankService)

            store.refreshTrustProviders()
            val fresh = explain.explain(reader)
            assertFalse(fresh.passDisagrees)
            assertEquals(serviceB, fresh.rankService)
        }

    @Test
    fun `under a shared topology the timer refreshes on its own`() =
        runBlocking {
            val timedProjection = TrustProjection(shared, reputations)
            val timed = NostrSemanticsStore(timedProjection, relay = relayUrl, writers = WriterTopology.SHARED_STRICT, providerRefreshMillis = 20)
            opened += timed
            timed.insert(rankedBy(bystander, serviceA))
            timedProjection.recompute.providerMap()
            other.insert(rankedBy(reader, serviceB))
            withTimeout(5_000) {
                while (timedProjection.recompute
                        .providerMap()
                        .lensOf(reader)
                        .rank != serviceB
                ) {
                    delay(10)
                }
            }
            assertEquals(0L, BackgroundFailures.consecutiveFailures(BackgroundFailures.PROVIDER_REFRESH))
        }

    @Test
    fun `a single writer never refreshes on a timer`() =
        runBlocking {
            val singleProjection = TrustProjection(shared, reputations)
            val single = NostrSemanticsStore(singleProjection, relay = relayUrl, writers = WriterTopology.SINGLE_WRITER, providerRefreshMillis = 20)
            opened += single
            single.insert(rankedBy(bystander, serviceA))
            singleProjection.recompute.providerMap()
            other.insert(rankedBy(reader, serviceB))
            delay(300)
            assertNull(
                singleProjection.recompute
                    .providerMap()
                    .lensOf(reader)
                    .rank,
                "SINGLE_WRITER asserts nobody else writes: the cache is exact as long as that holds",
            )
        }
}
