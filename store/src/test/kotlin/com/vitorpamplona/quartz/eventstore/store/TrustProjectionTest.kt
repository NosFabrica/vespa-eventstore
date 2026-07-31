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
package com.vitorpamplona.quartz.eventstore.store

import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationCells
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationDoc
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The projection is driven through the REAL store, so every path that
 * mutates trust data — inserts, supersession, kind-5, vanish — reaches the
 * tensors through the index decorator, with no deletion-specific code.
 */
class TrustProjectionTest {
    private val observer = "0b".repeat(32)
    private val observer2 = "2b".repeat(32)
    private val service = "5e".repeat(32)
    private val service2 = "6e".repeat(32)
    private val subject = "ab".repeat(32)

    private val reputations = InMemoryReputationIndex()
    private val projection = TrustProjection(InMemoryEventIndex(), reputations)
    private val store = NostrEventStore(projection, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

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
    fun `scores land keyed by the observer, not the service key`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            assertEquals(
                ReputationDoc(subject, mapOf(observer to 87), mapOf(observer to 120.0)),
                reputations.get(subject),
            )
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
            val index = InMemoryEventIndex()
            val cold = TrustProjection(index, InMemoryReputationIndex())
            val coldStore = NostrEventStore(cold, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

            assertEquals(0, cold.reconcile().services, "nothing there yet")

            // Written through a DIFFERENT store over the same index, so nothing
            // invalidates the cold one's map — the dedup case, exactly.
            val other = NostrEventStore(TrustProjection(index, InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            other.insert(list10040())
            other.insert(card())

            assertEquals(1, cold.reconcile().services, "the next pass must look again, not replay a cached empty map")
            coldStore.close()
            other.close()
        }

    @Test
    fun `a 30382 arriving before its 10040 is attributed when the list shows up`() =
        runBlocking {
            store.insert(card())
            assertNull(reputations.get(subject), "no provider mapping yet")
            store.insert(list10040())
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `supersession without a rank tag retracts the score`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, at = 100))
            store.insert(card(rank = null, followers = null, at = 200))
            assertNull(reputations.get(subject), "the newest version is the whole truth")
        }

    @Test
    fun `kind 5 deletion of the score erases the cell`() =
        runBlocking {
            store.insert(list10040())
            val scored = card()
            store.insert(scored)
            store.insert(DeletionEvent(id(), service, next(), arrayOf(arrayOf("e", scored.id)), "", ""))
            assertNull(reputations.get(subject))
        }

    @Test
    fun `a vanishing service key sweeps its cells`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(at = 100))
            store.insert(RequestToVanishEvent(id(), service, 200, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))
            assertNull(reputations.get(subject))
        }

    @Test
    fun `two observers on one subject hold independent cells`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service2))
            store.insert(card(signer = service, rank = 87))
            store.insert(card(signer = service2, rank = 15, followers = 3))
            assertEquals(mapOf(observer to 87, observer2 to 15), reputations.get(subject)?.influenceScores)
            assertEquals(mapOf(observer to 120.0, observer2 to 3.0), reputations.get(subject)?.followerCounts)
        }

    @Test
    fun `switching providers re-attributes stored scores`() =
        runBlocking {
            store.insert(list10040(serviceKey = service, at = 100))
            store.insert(card(signer = service, rank = 87))
            store.insert(card(signer = service2, rank = 42))
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)

            // The observer's NEW 10040 picks service2: the superseding insert
            // re-attributes — service's score detaches, service2's attaches.
            store.insert(list10040(serviceKey = service2, at = 200))
            assertEquals(mapOf(observer to 42), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `rebuildAll re-derives everything from the event corpus`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            reputations.docs.clear()

            projection.rebuildAll()
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    // ---- a 10040 carrying tags that are not service tags ---------------------

    @Test
    fun `a client tag in a provider list does not take down the provider map`() {
        // From a live relay: 68 of 270 stored 10040s carried ["client","nostria"].
        // serviceProviders() parses EVERY tag name as "<kind>:<type>", so a name
        // with no colon split to a size-1 list and threw
        // IndexOutOfBoundsException: Index: 1, Size: 1 — which killed
        // ProviderMap, and with it every observer's trust graph. It also failed
        // each bulk insert carrying such an event, costing all 1000 events in
        // the batch. One legal tag from one client, and nothing ranked.
        runBlocking {
            val withClientTag =
                TrustProviderListEvent(
                    id(),
                    observer,
                    next(),
                    arrayOf(
                        arrayOf("30382:rank", service, "wss://scores.example.com/"),
                        arrayOf("client", "nostria"),
                    ),
                    "",
                    "",
                )
            store.insert(withClientTag)
            store.insert(card())

            // The service tag beside it still maps, and the score still derives.
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }
    }

    // ---- reconcile: the repair for a projection no write can reach ----------

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

            val report = projection.reconcile()
            assertEquals(listOf(service), report.rebuilt, "the unprojected service is re-derived")
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `reconcile leaves a healthy projection alone`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())

            val report = projection.reconcile()
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

            val report = projection.reconcile()
            assertEquals(listOf(service), report.rebuilt)
            assertEquals(87, reputations.get(subject)?.influenceScores?.get(observer))
        }

    @Test
    fun `reconcile ignores a mapped service we hold no scores for`() =
        runBlocking {
            store.insert(list10040())
            val report = projection.reconcile()
            assertEquals(0, report.services, "nothing stored for it, nothing to project")
            assertTrue(report.isClean())
        }

    @Test
    fun `reconcile is a no-op with no provider lists at all`() =
        runBlocking {
            store.insert(card())
            val report = projection.reconcile()
            assertEquals(0, report.services)
            assertTrue(report.isClean())
        }

    /** The BULK path: one store batch of scores builds every subject's parent doc. */
    @Test
    fun `a bulk batch of scores projects one parent doc per subject`() =
        runBlocking {
            store.insert(list10040())
            val subjects = (1..40).map { it.toString(16).padStart(64, 'f') }
            val batch = subjects.map { s -> card(about = s, rank = 10, followers = null) }
            val outcomes = store.batchInsert(batch)
            assertEquals(40, outcomes.count { it is IEventStore.InsertOutcome.Accepted })
            subjects.forEach { s ->
                assertEquals(mapOf(observer to 10), reputations.docs.getValue(s).influenceScores, "subject ${'$'}s")
            }
        }

    /**
     * Two services (both mapped to the observer) scoring one subject in the
     * SAME bulk batch: the observer cell holds the last-applied card's value —
     * the zero-read [ReputationIndex.updateCells] path's documented
     * "latest-arriving mapped card wins", matching what [derive] does with a
     * LinkedHashMap (last write wins) on the sequential path.
     */
    @Test
    fun `two services scoring one subject in a bulk batch attribute to the observer`() =
        runBlocking {
            store.insert(list10040(serviceKey = service))
            store.insert(list10040(author = observer, serviceKey = service2))
            val outcomes = store.batchInsert(listOf(card(signer = service, rank = 30), card(signer = service2, rank = 71)))
            assertEquals(2, outcomes.count { it is IEventStore.InsertOutcome.Accepted })
            // Both cards attribute to the ONE observer cell; last applied wins.
            assertEquals(mapOf(observer to 71), reputations.get(subject)?.influenceScores)
        }

    /** A retraction (rank tag gone) inside a bulk batch supersedes and empties the cell. */
    @Test
    fun `a retraction in a bulk batch empties the subject cell`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, at = 100))
            // Newer version with no rank/followers, delivered through the bulk path.
            val outcomes = store.batchInsert(listOf(card(rank = null, followers = null, at = 200)))
            assertEquals(1, outcomes.count { it is IEventStore.InsertOutcome.Accepted })
            assertNull(reputations.get(subject), "the retraction is the newest version — no cell left")
        }

    /**
     * A PARTIAL retraction through the bulk path — rank dropped, followers kept.
     * The zero-read cell update can't express "clear the influence cell", so this
     * must fall back to the read-based recompute; otherwise the stale influence
     * cell survives (the bulk-vs-single divergence the audit caught).
     */
    @Test
    fun `a partial retraction in a bulk batch drops the stale dimension`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, followers = 120, at = 100))
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
            // Newer card keeps followers but drops rank — via the bulk path.
            store.batchInsert(listOf(card(rank = null, followers = 200, at = 200)))
            val doc = reputations.get(subject)
            assertEquals(emptyMap(), doc?.influenceScores, "the dropped rank must not linger")
            assertEquals(mapOf(observer to 200.0), doc?.followerCounts, "followers updated")
        }

    /**
     * The bulk fast path (zero-read cell updates) must land the SAME tensors as
     * one-by-one inserts (full re-derivation), across supersession, multi-service
     * attribution, and retraction in one batch. The parity net for the
     * insert-path optimization.
     */
    @Test
    fun `bulk projection equals sequential across supersession, multi-service and retraction`() =
        runBlocking {
            val subjectB = "cd".repeat(32)
            val events =
                listOf(
                    list10040(serviceKey = service, at = 10),
                    list10040(author = observer2, serviceKey = service2, at = 11),
                    card(signer = service, about = subject, rank = 20, at = 20),
                    card(signer = service, about = subject, rank = 55, at = 30), // supersedes -> 55
                    card(signer = service2, about = subject, rank = 9, followers = 4, at = 40), // observer2 cell
                    card(signer = service, about = subjectB, rank = 88, at = 50),
                    card(signer = service, about = subjectB, rank = null, followers = null, at = 60), // retracts subjectB
                )

            val sequentialReputations = InMemoryReputationIndex()
            val sequential = NostrEventStore(TrustProjection(InMemoryEventIndex(), sequentialReputations), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            events.forEach { sequential.insert(it) }

            val bulkReputations = InMemoryReputationIndex()
            val bulk = NostrEventStore(TrustProjection(InMemoryEventIndex(), bulkReputations), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            bulk.batchInsert(events)

            assertEquals(sequentialReputations.docs, bulkReputations.docs, "bulk cell-updates must match sequential re-derivation")
            // And the values are what we expect, not coincidentally-equal empties.
            assertEquals(mapOf(observer to 55, observer2 to 9), bulkReputations.docs.getValue(subject).influenceScores)
            assertNull(bulkReputations.docs[subjectB], "subjectB was retracted")
        }
}
