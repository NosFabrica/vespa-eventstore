/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store.trust

import com.vitorpamplona.quartz.eventstore.store.NostrSemanticsStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationCells
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The repair for a projection no write can reach: reconcile's drift detection and the rebuild hammer. */
class TrustReconcilerTest {
    private val observer = "0b".repeat(32)
    private val observer2 = "2b".repeat(32)
    private val service = "5e".repeat(32)
    private val subject = "ab".repeat(32)

    private val index = InMemoryEventIndex()
    private val reputations = InMemoryReputationIndex()
    private val projection = TrustProjection(index, reputations)
    private val reconciler = TrustReconciler(index, reputations, projection.recompute)
    private val store = NostrSemanticsStore(projection, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun list10040(
        author: String = observer,
        serviceKey: String = service,
        at: Long = next(),
    ) = TrustProviderListEvent(id(), author, at, arrayOf(arrayOf("30382:rank", serviceKey, "wss://scores.example.com/")), "", "")

    private fun card(
        signer: String = service,
        about: String = subject,
        rank: Int? = 87,
        followers: Int? = 120,
        at: Long = next(),
        eventId: String = id(),
    ): ContactCardEvent {
        val tags =
            buildList {
                add(arrayOf("d", about))
                rank?.let { add(arrayOf("rank", it.toString())) }
                followers?.let { add(arrayOf("followers", it.toString())) }
            }.toTypedArray()
        return ContactCardEvent(eventId, signer, at, tags, "", "")
    }

    @Test
    fun `rebuildAll re-derives everything from the event corpus`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            reputations.docs.clear()

            reconciler.rebuildAll()
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `an empty provider map is not cached, so a late corpus still projects`() =
        runBlocking {
            // The engine is cold, or the 10040s have not been mirrored yet. Either
            // way, asking early must not poison the map: it is read once per
            // reconcile pass, and a cached emptiness leaves a relay unrankable
            // until it restarts. Observed live — 271 kind-10040s queryable at full
            // coverage while ten minutes of retries all read the same cached
            // nothing, because only a 10040 WRITE invalidates, and dedup drops a
            // 10040 the store already holds before any write happens.
            val coldIndex = InMemoryEventIndex()
            val coldReputations = InMemoryReputationIndex()
            val cold = TrustProjection(coldIndex, coldReputations)
            val coldReconciler = TrustReconciler(coldIndex, coldReputations, cold.recompute)
            val coldStore = NostrSemanticsStore(cold, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

            assertEquals(0, coldReconciler.reconcile().services, "nothing there yet")

            // Written through a DIFFERENT store over the same index, so nothing
            // invalidates the cold one's map — the dedup case, exactly.
            val other = NostrSemanticsStore(TrustProjection(coldIndex, InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            other.insert(list10040())
            other.insert(card())

            assertEquals(1, coldReconciler.reconcile().services, "the next pass must look again, not replay a cached empty map")
            coldStore.close()
            other.close()
        }

    @Test
    fun `reconcile re-derives a service whose scores were stored before its 10040`() =
        runBlocking {
            // The mirror's normal order: scores first, by a service nothing maps
            // yet, so putAll drops them. Then the 10040 arrives in a LATER run,
            // where it is a duplicate — no write, so no repair trigger fires.
            store.insert(card())
            assertNull(reputations.get(subject), "unmapped scores derive nothing")

            store.insert(list10040())
            reputations.docs.clear() // as if that run's derivation never happened
            assertNull(reputations.get(subject))

            val report = reconciler.reconcile()
            assertEquals(listOf(service), report.rebuilt, "the unprojected service is re-derived")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `reconcile leaves a healthy projection alone`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())

            val report = reconciler.reconcile()
            assertEquals(emptyList(), report.rebuilt, "nothing to fix")
            assertEquals(1, report.services, "but the service was examined")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `reconcile catches a service remapped to a different observer`() =
        runBlocking {
            // Cells exist, so "has a doc" would call this clean — but they belong
            // to the previous observer. Checking the CURRENT observer's cell is
            // what makes the difference.
            store.insert(list10040())
            store.insert(card())
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)

            reputations.docs.clear()
            reputations.updateCells(listOf(ReputationCells(subject, observer2, 87, 1.0)))

            val report = reconciler.reconcile()
            assertEquals(listOf(service), report.rebuilt)
            assertEquals(87, reputations.get(subject)?.influenceScores?.get(observer))
        }

    @Test
    fun `reconcile ignores a mapped service we hold no scores for`() =
        runBlocking {
            store.insert(list10040())
            val report = reconciler.reconcile()
            assertEquals(0, report.services, "nothing stored for it, nothing to project")
            assertTrue(report.isClean())
        }

    @Test
    fun `reconcile is a no-op with no provider lists at all`() =
        runBlocking {
            store.insert(card())
            val report = reconciler.reconcile()
            assertEquals(0, report.services)
            assertTrue(report.isClean())
        }
}
