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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.observe.IndexObserver
import com.nosfabrica.vespa.eventstore.engine.observe.ObservedEventIndex
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.nosfabrica.vespa.eventstore.trust.TrustReconciler
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A MIRROR IS TOLD ABOUT EVERY REMOVAL STYLE — the claim `IndexObserver` makes,
 * driven through the whole store rather than the decorator alone.
 *
 * Assembled in `VespaEventStore.open`'s order, with the in-memory reference in
 * place of Vespa: the observed index directly over the engine and BELOW the
 * trust projection, and the reconciler handed the observed index too. That
 * placement is half of what is under test: the projection replays card
 * supersession against its inner index, and the orphan sweep removes through
 * the raw one, so both write BENEATH the projection — a hook above it would
 * pass the plain-note cases here and never hear of the sweep.
 *
 * (`open()` itself needs a live Vespa to assemble; what it does with an empty
 * list is `ObservedEventIndex.of`'s null, pinned in `ObservedEventIndexTest`.)
 */
class IndexObserversOverStoreTest {
    /** Everything the mirror was told: stored ids in put order, and every removed id. */
    private class Mirror : IndexObserver {
        val puts = ArrayList<String>()
        val removes = ArrayList<String>()

        override fun onPut(events: List<Event>) {
            events.forEach { puts += it.id }
        }

        override fun onRemove(ids: List<String>) {
            removes += ids
        }
    }

    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private val service2 = "5f".repeat(32)

    /** The store's sweep clock. Starts at the REAL time: admission's NIP-40 check is Quartz's `isExpired`, which reads the wall clock. */
    private var now = System.currentTimeMillis() / 1000
    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private val mirror = Mirror()
    private val observed = ObservedEventIndex(InMemoryEventIndex(), listOf(mirror))
    private val reputations = InMemoryReputationIndex()
    private val projection = TrustProjection(observed, reputations)
    private val reconciler = TrustReconciler(observed, reputations, projection.recompute, projection.backlog)
    private val store = NostrSemanticsStore(projection, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"), nowSecs = { now })

    private fun note(
        author: String = alice,
        at: Long = next(),
        tags: Array<Array<String>> = emptyArray(),
    ) = Event(id(), author, at, 1, tags, "hello", "")

    private fun card(
        signer: String = service,
        about: String = bob,
        at: Long = next(),
    ) = UserAssertionEvent(id(), signer, at, arrayOf(arrayOf("d", about), arrayOf("rank", "87")), "", "")

    @Test
    fun `a plain insert is reported`() =
        runBlocking {
            val hello = note()
            store.insert(hello)
            assertEquals(listOf(hello.id), mirror.puts)
            assertTrue(mirror.removes.isEmpty())
        }

    /** The per-event path: EventAdmission → putIfNewer, ridden through the observed index. */
    @Test
    fun `a replaceable supersession reports the winner and the replaced version`() =
        runBlocking {
            val v1 = MetadataEvent(id(), alice, next(), emptyArray(), """{"name":"a"}""", "")
            val v2 = MetadataEvent(id(), alice, next(), emptyArray(), """{"name":"b"}""", "")
            store.insert(v1)
            store.insert(v2)

            assertEquals(listOf(v1.id, v2.id), mirror.puts)
            assertEquals(listOf(v1.id), mirror.removes)
        }

    /** The bulk path supersedes by its own version-read stage (removeDocs + putAll) — the same report. */
    @Test
    fun `a bulk supersession reports the replaced version too`() =
        runBlocking {
            val v1 = MetadataEvent(id(), alice, next(), emptyArray(), """{"name":"a"}""", "")
            store.batchInsert(listOf(v1))
            val v2 = MetadataEvent(id(), alice, next(), emptyArray(), """{"name":"b"}""", "")
            store.batchInsert(listOf(v2))

            assertEquals(listOf(v1.id, v2.id), mirror.puts)
            assertEquals(listOf(v1.id), mirror.removes)
        }

    /**
     * A TRUST kind supersedes inside the projection (its own read-then-supersede,
     * against its INNER index) rather than through the port's default — which
     * is why the observed index must be that inner index.
     */
    @Test
    fun `a card supersession inside the trust projection is reported`() =
        runBlocking {
            val c1 = card()
            val c2 = card()
            store.insert(c1)
            store.insert(c2)

            assertEquals(listOf(c1.id, c2.id), mirror.puts)
            assertEquals(listOf(c1.id), mirror.removes)
        }

    @Test
    fun `a nip-09 deletion reports its targets`() =
        runBlocking {
            val target = note()
            val kept = note()
            store.insert(target)
            store.insert(kept)
            val deletion = DeletionRequestEvent(id(), alice, next(), arrayOf(arrayOf("e", target.id)), "", "")
            store.insert(deletion)

            assertEquals(listOf(target.id), mirror.removes)
            assertTrue(deletion.id in mirror.puts, "the deletion request itself is stored, and so reported")
        }

    @Test
    fun `a nip-62 vanish reports the author's history`() =
        runBlocking {
            val hers = listOf(note(at = 100), note(at = 150))
            val his = note(author = bob, at = 120)
            (hers + his).forEach { store.insert(it) }
            store.insert(RequestToVanishEvent(id(), alice, 200, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))

            assertEquals(hers.map { it.id }.toSet(), mirror.removes.toSet())
        }

    @Test
    fun `a nip-40 expiry sweep reports what it reaped`() =
        runBlocking {
            val expiring = note(tags = arrayOf(arrayOf("expiration", "${now + 50_000}")))
            val keeper = note()
            store.insert(expiring)
            store.insert(keeper)

            now += 100_000
            store.deleteExpiredEvents()
            assertEquals(listOf(expiring.id), mirror.removes)
        }

    @Test
    fun `delete by filter reports every id it purged`() =
        runBlocking {
            val hers = listOf(note(), note())
            val his = note(author = bob)
            (hers + his).forEach { store.insert(it) }

            store.delete(Filter(authors = listOf(alice)))
            assertEquals(hers.map { it.id }.toSet(), mirror.removes.toSet())
        }

    /** The one trust-side deletion, which removes through the raw index rather than the projection. */
    @Test
    fun `the orphan-score sweep reports the cards it drops`() =
        runBlocking {
            store.insert(
                TrustProviderListEvent(id(), observer, next(), arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""),
            )
            store.insert(card())
            val orphans = listOf(card(signer = service2, about = "c1".repeat(32)), card(signer = service2, about = "c2".repeat(32)))
            orphans.forEach { store.insert(it) }

            assertEquals(2, reconciler.sweepOrphanScores().scoresSwept)
            assertEquals(orphans.map { it.id }.toSet(), mirror.removes.toSet())
        }

    @Test
    fun `a throwing observer does not break the write`() =
        runBlocking {
            val broken =
                object : IndexObserver {
                    override fun onPut(events: List<Event>) = error("mirror down")

                    override fun onRemove(ids: List<String>) = error("mirror down")
                }
            val index = ObservedEventIndex(InMemoryEventIndex(), listOf(broken, mirror))
            val guarded = NostrSemanticsStore(TrustProjection(index, InMemoryReputationIndex()))
            val hello = note()

            guarded.insert(hello)
            guarded.delete(Filter(ids = listOf(hello.id)))

            assertEquals(listOf(hello.id), mirror.puts, "the observer after the broken one is still told")
            assertEquals(listOf(hello.id), mirror.removes)
            assertEquals(2L, index.observerFailures(), "and the broken one's two throws are counted, not raised")
            assertEquals(0, guarded.count(Filter(kinds = listOf(1))))
        }
}
