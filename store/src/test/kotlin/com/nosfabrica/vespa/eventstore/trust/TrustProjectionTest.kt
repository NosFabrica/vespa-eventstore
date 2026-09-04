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
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
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
    private val store = NostrSemanticsStore(projection, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    // The default names ONE service for BOTH dimensions — the single-provider
    // norm. Per-dimension attribution is exercised by the split-provider tests.
    private fun list10040(
        author: String = observer,
        serviceKey: String = service,
        at: Long = next(),
        types: List<String> = listOf("30382:rank", "30382:followers"),
    ) = TrustProviderListEvent(id(), author, at, types.map { arrayOf(it, serviceKey, "wss://scores.example.com/") }.toTypedArray(), "", "")

    private fun card(
        signer: String = service,
        about: String = subject,
        rank: Int? = 87,
        followers: Int? = 120,
        at: Long = next(),
        eventId: String = id(),
        expires: Long? = null,
    ): ContactCardEvent {
        val tags =
            buildList {
                add(arrayOf("d", about))
                rank?.let { add(arrayOf("rank", it.toString())) }
                followers?.let { add(arrayOf("followers", it.toString())) }
                expires?.let { add(arrayOf("expiration", it.toString())) }
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

    /**
     * Every read the projection WRITES from is a complete read: the card fetch
     * a derivation rests on and the 10040 read the attribution map rests on.
     * An engine that would answer either short refuses them instead
     * (VespaCoverageTest), so no parent is derived or removed from a fetch
     * that missed cards.
     */
    @Test
    fun `the card fetch and the provider-list read are complete reads`() =
        runBlocking {
            val seen = mutableListOf<EventQuery>()
            val inner = InMemoryEventIndex()
            val recording =
                object : EventIndex by inner {
                    override suspend fun search(query: EventQuery): List<EventDoc> {
                        seen += query
                        return inner.search(query)
                    }
                }
            val reps = InMemoryReputationIndex()
            val recorded = NostrSemanticsStore(TrustProjection(recording, reps), relay = RelayUrlNormalizer.normalize("ws://localhost:7778"))
            // Card first, so the list's arrival walks the service and FETCHES it.
            recorded.insert(card())
            recorded.insert(list10040())
            assertEquals(mapOf(observer to 87), reps.get(subject)?.influenceScores, "derived through the fetch")
            // The derivation's shape: cards BY SUBJECT (`d`), any signer, live at
            // the cutoff — not the store's own by-author dedup read of the card.
            val cardFetches = seen.filter { it.kinds == listOf(ContactCardEvent.KIND) && it.authors.isEmpty() && it.notExpiredAt != null }
            // Likewise the attribution map's read: EVERY live 10040, not the store's by-author dedup read of the one arriving.
            val listReads = seen.filter { it.kinds == listOf(TrustProviderListEvent.KIND) && it.authors.isEmpty() }
            assertTrue(cardFetches.isNotEmpty() && cardFetches.all { it.complete }, "every unlimited card fetch is complete: $cardFetches")
            assertTrue(listReads.isNotEmpty() && listReads.all { it.complete }, "every 10040 read is complete: $listReads")
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
    fun `off-scale provider ranks are clamped to the served 0 to 100 scale`() =
        runBlocking {
            store.insert(list10040())
            // Fast path: a negative rank must land as 0, not below the
            // include:spam floor (min_rank=0 is the "keep everything" opt-out —
            // a negative cell would silently drop the author from it).
            store.insert(card(rank = -5, followers = null, at = 100))
            assertEquals(mapOf(observer to 0), reputations.get(subject)?.influenceScores, "negative clamps to 0")
            // Bulk path, over-scale: capped at 100 so wot_mult() stays on the
            // 0..100 span its calibrated tier-crossing thresholds are derived
            // against. Both paths must clamp identically or the reconciler
            // reads drift.
            store.batchInsert(listOf(card(rank = 250, followers = null, at = 200)))
            assertEquals(mapOf(observer to 100), reputations.get(subject)?.influenceScores, "over-scale caps at 100")
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

    // ---- per-dimension provider attribution (NIP-85 typed entries) -----------

    private fun splitList10040(at: Long = next()) =
        TrustProviderListEvent(
            id(),
            observer,
            at,
            arrayOf(
                arrayOf("30382:rank", service, "wss://scores.example.com/"),
                arrayOf("30382:followers", service2, "wss://followers.example.com/"),
            ),
            "",
            "",
        )

    /**
     * The observer picks DIFFERENT services per dimension: rank from [service],
     * follower counts from [service2]. Each card's tag lands only through the
     * service the observer named for THAT dimension — the rank provider's
     * followers tag (and the follower provider's rank tag) must not land.
     */
    @Test
    fun `rank and followers attribute through their own services`() =
        runBlocking {
            store.insert(splitList10040())
            store.insert(card(signer = service, rank = 87, followers = 999)) // followers: not its dimension
            store.insert(card(signer = service2, rank = 50, followers = 300)) // rank: not its dimension
            assertEquals(
                ReputationDoc(subject, mapOf(observer to 87), mapOf(observer to 300.0)),
                reputations.get(subject),
            )
        }

    /** The same split through the bulk zero-read cell path: per-dimension partial updates merge into one doc. */
    @Test
    fun `split providers merge per dimension on the bulk path`() =
        runBlocking {
            store.insert(splitList10040())
            // A real bulk batch (>= the bulk threshold), so the cells land inline;
            // the rank-only fillers must ride the fast path too — a missing
            // followers tag is no retraction when the signer is rank-mapped only.
            val fillers = (1..15).map { card(signer = service, about = it.toString(16).padStart(64, 'b'), rank = 10, followers = null) }
            val outcomes = store.batchInsert(fillers + card(signer = service, rank = 87, followers = 999) + card(signer = service2, rank = 50, followers = 300))
            assertEquals(17, outcomes.count { it is IEventStore.InsertOutcome.Accepted })
            assertEquals(
                ReputationDoc(subject, mapOf(observer to 87), mapOf(observer to 300.0)),
                reputations.get(subject),
            )
            fillers.forEach { f ->
                val about = f.tags.first { it[0] == "d" }[1]
                assertEquals(ReputationDoc(about, mapOf(observer to 10), emptyMap()), reputations.get(about), "rank-only filler $about")
            }
        }

    /** BOTH dimensions from ONE service the observer named twice — the single-provider norm. */
    @Test
    fun `a service named for both dimensions scores both tensors`() =
        runBlocking {
            store.insert(list10040()) // the default names service under rank AND followers
            store.insert(card(rank = 87, followers = 120))
            assertEquals(
                ReputationDoc(subject, mapOf(observer to 87), mapOf(observer to 120.0)),
                reputations.get(subject),
            )
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

    @Test
    fun `a Trusted List delegation in a provider list attributes nothing`() {
        // A Treasure Map is an open tag set, and Tapestry's Trusted Lists hang
        // their own `3039x` delegations off the same kind-10040 with the same
        // two-segment shape a NIP-85 entry has. Nothing here may read one as a
        // trust service: a list publisher is not a rank or follower provider,
        // and crediting its key would rank every subject it names under an
        // observer who never delegated a score to it.
        //
        // Quartz bounds this upstream as of 029c40ebb4 — `ServiceProviderTag`
        // now refuses a kind outside 30382..30385 — but the guarantee this
        // store owes is the outcome, not the layer that produces it: before
        // that bound the entry parsed and was dropped by ProviderMap's `when`
        // over ProviderTypes instead. Pinned here so a change on either side
        // has to keep it true.
        runBlocking {
            val listPublisher = "7c".repeat(32)
            val withTrustedListEntry =
                TrustProviderListEvent(
                    id(),
                    observer,
                    next(),
                    arrayOf(
                        arrayOf("30382:rank", service, "wss://scores.example.com/"),
                        arrayOf("30382:followers", service, "wss://scores.example.com/"),
                        // The generic bare-kind form: all of this publisher's kind-30392 lists.
                        arrayOf("30392", listPublisher, "wss://lists.example.com/"),
                        // And the reserved named form, `3039x:<name>`.
                        arrayOf("30392:podcasters", listPublisher, "wss://lists.example.com/"),
                    ),
                    "",
                    "",
                )
            store.insert(withTrustedListEntry)
            store.insert(card())
            // A card the LIST publisher signed: it is not a delegated service,
            // so its scores must reach no tensor at all.
            store.insert(card(signer = listPublisher, rank = 99, followers = 999))

            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
            assertEquals(mapOf(observer to 120.0), reputations.get(subject)?.followerCounts)
        }
    }

    /**
     * The BULK path: one store batch of scores builds every subject's parent
     * doc. The observer maps the service for RANK only, so the rank-only cards
     * take the zero-read cell update — a missing followers tag is not a
     * retraction when nobody named the signer as a follower provider.
     */
    @Test
    fun `a bulk batch of scores projects one parent doc per subject`() =
        runBlocking {
            store.insert(list10040(types = listOf("30382:rank")))
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
            val sequential = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), sequentialReputations), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            events.forEach { sequential.insert(it) }

            val bulkReputations = InMemoryReputationIndex()
            val bulk = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), bulkReputations), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            bulk.batchInsert(events)

            assertEquals(sequentialReputations.docs, bulkReputations.docs, "bulk cell-updates must match sequential re-derivation")
            // And the values are what we expect, not coincidentally-equal empties.
            assertEquals(mapOf(observer to 55, observer2 to 9), bulkReputations.docs.getValue(subject).influenceScores)
            assertNull(bulkReputations.docs[subjectB], "subjectB was retracted")
        }

    // ---- a provider shared by several observers (the NIP-85 norm) ------------

    /**
     * Popular providers are named by MANY users' 10040s. Every one of those
     * observers must get the service's scores under their own key — a
     * single-winner map silently unranked everyone else trusting that provider.
     */
    @Test
    fun `a provider shared by two observers scores both`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service))
            store.insert(card())
            assertEquals(
                mapOf(observer to 87, observer2 to 87),
                reputations.get(subject)?.influenceScores,
                "both users trusting the provider see its score",
            )
        }

    /** The same fan-out on the BULK zero-read cell path. */
    @Test
    fun `a shared provider fans out on the bulk path too`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service))
            val subjects = (1..20).map { it.toString(16).padStart(64, 'f') }
            val outcomes = store.batchInsert(subjects.map { s -> card(about = s) })
            assertEquals(20, outcomes.count { it is IEventStore.InsertOutcome.Accepted })
            subjects.forEach { s ->
                assertEquals(mapOf(observer to 87, observer2 to 87), reputations.docs.getValue(s).influenceScores, "subject $s")
            }
        }

    /** One observer un-naming the shared provider drops only THEIR cells. */
    @Test
    fun `dropping a shared provider detaches only that observer`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service))
            store.insert(card())
            assertEquals(mapOf(observer to 87, observer2 to 87), reputations.get(subject)?.influenceScores)

            // observer2's NEW 10040 picks service2 instead: their cell moves off
            // the shared provider; observer's stays.
            store.insert(list10040(author = observer2, serviceKey = service2))
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    // ---- crash safety: the dirty marker ---------------------------------------

    /**
     * The event write and the projection write are separate acks. A failure
     * between them stores the events, and the retry comes back all-duplicates —
     * which never reaches the projection. The persisted dirt marker must repair
     * that at the NEXT trust write.
     */
    @Test
    fun `a projection failure after the event write heals at the next trust write`() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val reps = InMemoryReputationIndex()
            val failing = FailingCellsReputationIndex(reps)
            val proj = TrustProjection(inner, failing)
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())

            val subjects = (1..20).map { it.toString(16).padStart(64, 'e') }
            val batch = subjects.map { s -> card(about = s) }
            failing.failNext = true
            assertFailsWith<RuntimeException> { st.batchInsert(batch) }

            // Events landed, cells did not — the exact drift, named by the marker.
            assertEquals(20, inner.search(EventQuery(kinds = listOf(ContactCardEvent.KIND))).count { subjectOf(it) in subjects })
            subjects.forEach { assertNull(reps.get(it), "cells must be missing after the failure") }
            assertEquals(subjects.toSet(), reps.get(DirtLedger.MARKER_KEY)?.influenceScores?.keys, "the marker names the dirty subjects")

            // A retry is all duplicates and never reaches the projection; the
            // next NEW trust write heals first.
            st.batchInsert(batch)
            subjects.forEach { assertNull(reps.get(it), "duplicates alone cannot repair") }
            st.insert(card(about = "9a".repeat(32)))
            subjects.forEach { s -> assertEquals(mapOf(observer to 87), reps.get(s)?.influenceScores, "healed subject $s") }
            assertNull(reps.get(DirtLedger.MARKER_KEY), "marker cleared after the heal")
        }

    /** A crafted card cannot collide with the marker: subjects must be 64-hex. */
    @Test
    fun `a card whose d tag is the marker id projects nothing and breaks nothing`() =
        runBlocking {
            store.insert(list10040())
            store.insert(ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", DirtLedger.MARKER_KEY), arrayOf("rank", "87")), "", ""))
            assertNull(reputations.get(DirtLedger.MARKER_KEY))
            // And ordinary projection still works beside it.
            store.insert(card())
            assertEquals(mapOf(observer to 87), reputations.get(subject)?.influenceScores)
        }

    // ---- expiry (NIP-40) ------------------------------------------------------

    /** An expired card must stop scoring the moment its subject is re-derived, not only when swept. */
    @Test
    fun `an expired card contributes nothing to a re-derivation`() =
        runBlocking {
            var now = System.currentTimeMillis() / 1000
            val inner = InMemoryEventIndex()
            val reps = InMemoryReputationIndex()
            val st = NostrSemanticsStore(TrustProjection(inner, reps, nowSecs = { now }), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040(author = observer, serviceKey = service))
            st.insert(list10040(author = observer2, serviceKey = service2))
            st.insert(card(signer = service, rank = 87, expires = now + 500))
            assertEquals(mapOf(observer to 87), reps.get(subject)?.influenceScores)

            now += 1_000 // the card is now past its NIP-40 expiration
            st.insert(card(signer = service2, rank = 15, followers = 3))
            assertEquals(
                mapOf(observer2 to 15),
                reps.get(subject)?.influenceScores,
                "the expired card's cell must drop with the re-derive",
            )
        }

    // ---- deterministic derivation --------------------------------------------

    /** Two services of ONE observer: the NEWEST card wins the cell, regardless of arrival order. */
    @Test
    fun `derivation is deterministic across arrival orders`() =
        runBlocking {
            val twoServices =
                TrustProviderListEvent(
                    id(),
                    observer,
                    next(),
                    arrayOf(
                        arrayOf("30382:rank", service, "wss://scores.example.com/"),
                        arrayOf("30382:rank", service2, "wss://scores.example.com/"),
                    ),
                    "",
                    "",
                )
            val newer = card(signer = service, rank = 30, at = 2_000_000)
            val older = card(signer = service2, rank = 71, at = 1_500_000)

            for (order in listOf(listOf(older, newer), listOf(newer, older))) {
                val reps = InMemoryReputationIndex()
                val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), reps), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
                st.insert(twoServices)
                order.forEach { st.insert(it) }
                assertEquals(mapOf(observer to 30), reps.get(subject)?.influenceScores, "newest card wins for order $order")
            }
        }

    // ---- deferred (async) projection -----------------------------------------

    /**
     * Deferred mode: the expensive reactions leave the insert as PENDING work —
     * signalled, persisted, and invisible to ranking until a drain. The drain
     * then settles everything and clears the marker.
     */
    @Test
    fun `deferred mode queues reactions and drains them on demand`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(InMemoryEventIndex(), reps)
            var signals = 0
            proj.dirt.deferTo { signals++ }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

            st.insert(list10040())
            st.insert(card())
            assertNull(reps.get(subject), "reactions are queued, not applied")
            assertTrue(signals >= 2, "each trust write signals the drainer")
            assertNotNull(reps.get(DirtLedger.MARKER_KEY), "the queue is persisted — a crash loses nothing")

            proj.dirt.drain { it() }
            assertEquals(mapOf(observer to 87), reps.get(subject)?.influenceScores, "drained")
            assertNull(reps.get(DirtLedger.MARKER_KEY), "queue empty, marker gone")
        }

    /** The bulk zero-read cell path stays INLINE in deferred mode — mirror ingest keeps immediate ranking. */
    @Test
    fun `deferred mode keeps the bulk cell path inline`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(InMemoryEventIndex(), reps)
            proj.dirt.deferTo { }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

            st.insert(list10040())
            proj.dirt.drain { it() } // attribute the 10040 so the map is live
            val subjects = (1..20).map { it.toString(16).padStart(64, 'f') }
            st.batchInsert(subjects.map { s -> card(about = s) })
            subjects.forEach { s ->
                assertEquals(mapOf(observer to 87), reps.docs.getValue(s).influenceScores, "bulk cells land without a drain")
            }
        }

    /**
     * The parity net for deferral: the same sequence — shared observers,
     * supersession, a provider switch, a retraction, a kind-5 deletion, a bulk
     * batch — lands the SAME tensors whether settled inline per write or
     * queued and drained at arbitrary points. Deferral needs no event ordering
     * because every drain re-derives from the store's current state.
     */
    @Test
    fun `deferred drains converge to the sync tensors`() =
        runBlocking {
            val deleted = card(signer = service2, about = "cd".repeat(32), rank = 44, at = 500)
            val script: suspend (IEventStore) -> Unit = { st ->
                st.insert(list10040(author = observer, serviceKey = service, at = 10))
                st.insert(list10040(author = observer2, serviceKey = service, at = 11)) // shared provider
                st.insert(card(rank = 20, at = 100))
                st.insert(card(rank = 55, at = 200)) // supersedes
                st.batchInsert((1..20).map { card(about = it.toString(16).padStart(64, 'a'), rank = 10) })
                st.insert(list10040(author = observer2, serviceKey = service2, at = 12)) // observer2 switches provider
                st.insert(deleted)
                st.insert(DeletionEvent(id(), service2, 600, arrayOf(arrayOf("e", deleted.id)), "", ""))
                st.insert(card(about = "ef".repeat(32), rank = 30, at = 300))
                st.insert(card(about = "ef".repeat(32), rank = null, followers = null, at = 400)) // retraction
            }

            val syncReps = InMemoryReputationIndex()
            script(NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), syncReps), relay = RelayUrlNormalizer.normalize("ws://localhost:7777")))

            val defReps = InMemoryReputationIndex()
            val defProj = TrustProjection(InMemoryEventIndex(), defReps)
            defProj.dirt.deferTo { }
            script(NostrSemanticsStore(defProj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777")))
            defProj.dirt.drain { it() }

            assertEquals(syncReps.docs, defReps.docs, "any drain schedule must converge to the inline tensors")
            // And they are the values we expect, not coincidentally-equal emptiness.
            assertEquals(mapOf(observer to 55), syncReps.docs.getValue(subject).influenceScores)
        }

    /**
     * Back-to-back updates to the SAME score (one replaceable address) in one
     * session: the doc must follow the replaceable winner — the newest version —
     * under EVERY drain timing. Supersession itself is synchronous at the event
     * layer (remove-old + put-new inside one writer-lock hold, un-interleavable
     * by a drain), and each drain re-derives from stored state, which only ever
     * contains the winner.
     */
    @Test
    fun `back-to-back updates to one score converge to the newest version under any drain timing`() =
        runBlocking {
            // Drain after the first version? after the second? Every combination
            // must land the same final cell.
            for (schedule in listOf(
                listOf(false, false),
                listOf(true, false),
                listOf(false, true),
                listOf(true, true),
            )) {
                val reps = InMemoryReputationIndex()
                val proj = TrustProjection(InMemoryEventIndex(), reps)
                proj.dirt.deferTo { }
                val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
                st.insert(list10040())
                st.insert(card(rank = 50, at = 100))
                if (schedule[0]) proj.dirt.drain { it() }
                st.insert(card(rank = 80, at = 200))
                if (schedule[1]) proj.dirt.drain { it() }
                proj.dirt.drain { it() }
                assertEquals(mapOf(observer to 80), reps.get(subject)?.influenceScores, "newest version wins for drains at $schedule")
            }
        }

    /** A STALE version arriving after a drain is rejected at the event layer and cannot regress the cell. */
    @Test
    fun `a stale version arriving later does not regress the projected score`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(InMemoryEventIndex(), reps)
            proj.dirt.deferTo { }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            st.insert(card(rank = 80, at = 200))
            proj.dirt.drain { it() }
            assertEquals(mapOf(observer to 80), reps.get(subject)?.influenceScores)

            val outcome = st.batchInsert(listOf(card(rank = 50, at = 100))).single()
            assertTrue(outcome is IEventStore.InsertOutcome.Rejected, "the older version is replaced, not stored")
            proj.dirt.drain { it() }
            assertEquals(mapOf(observer to 80), reps.get(subject)?.influenceScores, "nothing left to derive the stale value from")
        }

    /**
     * A bulk-projected cell superseded by a later single insert: the cell shows
     * the OLD value only until the next drain (the documented bounded lag),
     * then follows the replaceable winner.
     */
    @Test
    fun `an inline bulk cell follows a later superseding single insert at the next drain`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(InMemoryEventIndex(), reps)
            proj.dirt.deferTo { }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            proj.dirt.drain { it() }
            // A real bulk batch (>= the bulk threshold), so v1's cell lands INLINE.
            val fillers = (1..15).map { card(about = it.toString(16).padStart(64, 'b'), rank = 10) }
            st.batchInsert(fillers + card(rank = 50, at = 100))
            assertEquals(mapOf(observer to 50), reps.get(subject)?.influenceScores, "bulk cell is immediate")

            st.insert(card(rank = 80, at = 200))
            assertEquals(mapOf(observer to 50), reps.get(subject)?.influenceScores, "stale only until the drain — the documented lag")
            proj.dirt.drain { it() }
            assertEquals(mapOf(observer to 80), reps.get(subject)?.influenceScores, "the drain re-derives the winner")
        }

    /** Create then delete in one session: the cells die with the event, under every drain timing. */
    @Test
    fun `create then delete converges to no cells under any drain timing`() =
        runBlocking {
            for (drainBetween in listOf(false, true)) {
                val reps = InMemoryReputationIndex()
                val proj = TrustProjection(InMemoryEventIndex(), reps)
                proj.dirt.deferTo { }
                val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
                st.insert(list10040())
                val scored = card()
                st.insert(scored)
                if (drainBetween) {
                    proj.dirt.drain { it() }
                    assertEquals(mapOf(observer to 87), reps.get(subject)?.influenceScores, "projected while the event lives")
                }
                st.insert(DeletionEvent(id(), service, next(), arrayOf(arrayOf("e", scored.id)), "", ""))
                proj.dirt.drain { it() }
                assertNull(reps.get(subject), "deleted event derives nothing (drainBetween=$drainBetween)")
            }
        }

    /**
     * The decorator must forward visitDocsPage to the inner client's
     * implementation — the interface default re-lists the whole corpus through
     * search() per page, the exact O(corpus squared) shape the visit-backed
     * reindex replaced.
     */
    @Test
    fun `visitDocsPage forwards to the inner index`() =
        runBlocking {
            var forwarded = false
            val inner =
                object : EventIndex by InMemoryEventIndex() {
                    override suspend fun visitDocsPage(
                        query: EventQuery,
                        resumeFrom: String?,
                        maxDocs: Int,
                    ): DocsPage {
                        forwarded = true
                        return DocsPage(emptyList(), null)
                    }
                }
            val decorated = TrustProjection(inner, InMemoryReputationIndex())
            decorated.visitDocsPage(EventQuery(), null, 8)
            assertTrue(forwarded, "the reindex walk must reach the engine's visit, not the search-per-page default")
        }

    /**
     * The write-ahead marker must be priced to the op: per-cell adds for the
     * small dirt of live traffic, ONE doc put for a bulk batch — per-subject
     * feed ops at batch size would rival the event writes they insure.
     */
    @Test
    fun `the marker write-ahead is one doc put for a bulk batch and cell adds for singles`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            assertEquals(1, counting.markerCellAdds, "a single's dirt is one pipelined cell add")
            assertEquals(0, counting.markerPuts)

            val subjects = (1..100).map { it.toString(16).padStart(64, 'c') }
            st.batchInsert(subjects.map { s -> card(about = s) })
            assertEquals(1, counting.markerCellAdds, "a bulk batch must not pay per-subject marker ops")
            assertEquals(1, counting.markerPuts, "its write-ahead is one marker-doc put")

            st.insert(card())
            assertEquals(2, counting.markerCellAdds, "back to a single cell add for a single insert")
            assertEquals(1, counting.markerPuts)
        }
}

/** Counts marker-doc traffic, separating pipelined cell adds from whole-doc puts. */
private class MarkerCountingReputationIndex(
    private val inner: InMemoryReputationIndex,
) : ReputationIndex by inner {
    var markerCellAdds = 0
    var markerPuts = 0

    override suspend fun updateCells(updates: List<ReputationCells>) {
        markerCellAdds += updates.count { it.subject == DirtLedger.MARKER_KEY }
        inner.updateCells(updates)
    }

    override suspend fun put(reputation: ReputationDoc) {
        if (reputation.pubkey == DirtLedger.MARKER_KEY) markerPuts++
        inner.put(reputation)
    }
}

/**
 * Fails ONE projection cell write on demand — the "engine hiccup after the
 * event write" the dirt marker exists for. The ledger's write-ahead marker
 * persist ALSO rides updateCells (and runs before the event write), so the
 * failure targets only updates that touch real subjects.
 */
private class FailingCellsReputationIndex(
    private val inner: InMemoryReputationIndex,
) : ReputationIndex by inner {
    var failNext = false

    override suspend fun updateCells(updates: List<ReputationCells>) {
        if (failNext && updates.any { it.subject != DirtLedger.MARKER_KEY }) {
            failNext = false
            throw RuntimeException("simulated projection failure")
        }
        inner.updateCells(updates)
    }
}
