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
import com.nosfabrica.vespa.eventstore.RejectedException
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
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

    private suspend fun lensOf(observer: String) = projection.recompute.providerMap().lensOf(observer)

    /** The cell path leaves an emptied parent standing until a derive drops it: "no cells" is null OR empty. */
    private suspend fun assertNoCells(
        reps: InMemoryReputationIndex,
        about: String,
        message: String = "no cells expected",
    ) {
        val doc = reps.get(about)
        assertTrue(doc == null || doc.isEmpty(), "$message, got $doc")
    }

    /**
     * The marker names no work: absent, or the empty document a clean commit
     * leaves (ProjectionLedger.insuring clears its cells, not the document).
     */
    private suspend fun assertMarkerNamesNothing(
        reps: ReputationIndex,
        message: String = "the marker names nothing",
    ) {
        val marker = reps.get(ProjectionLedger.MARKER_KEY)
        assertTrue(marker == null || marker.isEmpty(), "$message, got $marker")
    }

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
    ): UserAssertionEvent {
        val tags =
            buildList {
                add(arrayOf("d", about))
                rank?.let { add(arrayOf("rank", it.toString())) }
                followers?.let { add(arrayOf("followers", it.toString())) }
                expires?.let { add(arrayOf("expiration", it.toString())) }
            }.toTypedArray()
        return UserAssertionEvent(eventId, signer, at, tags, "", "")
    }

    @Test
    fun `scores land keyed by the service key, and the observer's lens resolves to it`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            assertEquals(
                ReputationDoc(subject, serviceCells(service to 87), serviceCells(service to 120.0)),
                reputations.get(subject),
            )
            assertEquals(TrustProviders.Lens(service, service), lensOf(observer), "the 10040 resolves to the service the query tensors carry")
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
            recorded.insert(card())
            recorded.insert(list10040())
            // The derive — the repair path a crash marker or a verify takes.
            val proj = TrustProjection(recording, reps)
            val derived = proj.recompute.deriveBatch(listOf(subject), proj.recompute.providerMap())
            assertEquals(serviceCells(service to 87), derived[subject]?.influenceScores, "derived through the fetch")
            // The derivation's shape: cards BY SUBJECT (`d`), any signer, live at
            // the cutoff — not the store's own by-author dedup read of the card.
            val cardFetches = seen.filter { it.kinds == listOf(UserAssertionEvent.KIND) && it.authors.isEmpty() && it.notExpiredAt != null }
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
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)
        }

    @Test
    fun `off-scale provider ranks are clamped to the served 0 to 100 scale`() =
        runBlocking {
            store.insert(list10040())
            // Fast path: a negative rank must land as 0, not below the
            // include:spam floor (min_rank=0 is the "keep everything" opt-out —
            // a negative cell would silently drop the author from it).
            store.insert(card(rank = -5, followers = null, at = 100))
            assertEquals(serviceCells(service to 0), reputations.get(subject)?.influenceScores, "negative clamps to 0")
            // Bulk path, over-scale: capped at 100 so wot_mult() stays on the
            // 0..100 span its calibrated tier-crossing thresholds are derived
            // against. Both paths must clamp identically or the reconciler
            // reads drift.
            store.batchInsert(listOf(card(rank = 250, followers = null, at = 200)))
            assertEquals(serviceCells(service to 100), reputations.get(subject)?.influenceScores, "over-scale caps at 100")
        }

    /**
     * A rank that is not an Int, one beyond its range included, is NO rank:
     * quartz's `RankTag.parseBounded` answers null for it, not 100, which is
     * what this store's own clamp did before it read through quartz's. Pinned
     * so a quartz that began clamping such a value would show here, not as a
     * provider's garbage landing on the top rung.
     */
    @Test
    fun `a rank beyond Int range is no rank, not the top of the scale`() =
        runBlocking {
            store.insert(list10040())
            val tags = arrayOf(arrayOf("d", subject), arrayOf("rank", "99999999999"), arrayOf("followers", "120"))
            store.insert(UserAssertionEvent(id(), service, next(), tags, "", ""))
            val doc = reputations.get(subject)
            assertTrue(doc?.influenceScores.isNullOrEmpty(), "no influence cell, got $doc")
            assertEquals(serviceCells(service to 120.0), doc?.followerCounts, "the card's other cell still lands")
        }

    /**
     * A card's subject is a pubkey as NIP-01 writes one, 64 LOWERCASE hex
     * (quartz's `UserAssertionEvent.isSubjectPubKey`). An upper-case `d` is
     * another address: it lands no cell, under either spelling.
     */
    @Test
    fun `a card whose d is upper-case hex is about nobody`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(about = subject.uppercase()))
            assertNull(reputations.get(subject.uppercase()), "no cell under the upper-case key")
            assertNull(reputations.get(subject), "nor under the subject it spells")
        }

    @Test
    fun `supersession without a rank tag retracts the score`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, at = 100))
            store.insert(card(rank = null, followers = null, at = 200))
            assertNoCells(reputations, subject, "the newest version is the whole truth")
        }

    @Test
    fun `kind 5 deletion of the score erases the cell`() =
        runBlocking {
            store.insert(list10040())
            val scored = card()
            store.insert(scored)
            store.insert(DeletionRequestEvent(id(), service, next(), arrayOf(arrayOf("e", scored.id)), "", ""))
            assertNoCells(reputations, subject)
        }

    @Test
    fun `a vanishing service key sweeps its cells`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(at = 100))
            store.insert(RequestToVanishEvent(id(), service, 200, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))
            assertNoCells(reputations, subject)
        }

    @Test
    fun `two observers on one subject hold independent cells`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service2))
            store.insert(card(signer = service, rank = 87))
            store.insert(card(signer = service2, rank = 15, followers = 3))
            assertEquals(serviceCells(service to 87, service2 to 15), reputations.get(subject)?.influenceScores)
            assertEquals(serviceCells(service to 120.0, service2 to 3.0), reputations.get(subject)?.followerCounts)
            assertEquals(service, lensOf(observer).rank)
            assertEquals(service2, lensOf(observer2).rank)
        }

    @Test
    fun `switching providers re-attributes stored scores`() =
        runBlocking {
            store.insert(list10040(serviceKey = service, at = 100))
            store.insert(card(signer = service, rank = 87))
            store.insert(card(signer = service2, rank = 42))
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores, "service2 is named by nobody: its card is dead storage")
            assertEquals(service, lensOf(observer).rank)

            // The observer's NEW 10040 picks service2: the LENS moves — no cell
            // is rewritten for it — and service2's stored card, named for the
            // first time, is projected by the walk the list queued.
            store.insert(list10040(serviceKey = service2, at = 200))
            assertEquals(service2, lensOf(observer).rank, "the swap is a query-time resolution")
            assertEquals(serviceCells(service to 87, service2 to 42), reputations.get(subject)?.influenceScores, "the newly named service's card became a cell")
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
            // Every mapped service's card lands whole; the LENS picks the
            // dimension: rank through service (87), followers through service2 (300).
            assertEquals(
                ReputationDoc(subject, serviceCells(service to 87, service2 to 50), serviceCells(service to 999.0, service2 to 300.0)),
                reputations.get(subject),
            )
            assertEquals(TrustProviders.Lens(rank = service, followers = service2), lensOf(observer))
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
                ReputationDoc(subject, serviceCells(service to 87, service2 to 50), serviceCells(service to 999.0, service2 to 300.0)),
                reputations.get(subject),
            )
            assertEquals(TrustProviders.Lens(rank = service, followers = service2), lensOf(observer))
            fillers.forEach { f ->
                val about = f.tags.first { it[0] == "d" }[1]
                assertEquals(ReputationDoc(about, serviceCells(service to 10), emptyMap()), reputations.get(about), "rank-only filler $about")
            }
        }

    /** BOTH dimensions from ONE service the observer named twice — the single-provider norm. */
    @Test
    fun `a service named for both dimensions scores both tensors`() =
        runBlocking {
            store.insert(list10040()) // the default names service under rank AND followers
            store.insert(card(rank = 87, followers = 120))
            assertEquals(
                ReputationDoc(subject, serviceCells(service to 87), serviceCells(service to 120.0)),
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
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)
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

            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)
            assertEquals(serviceCells(service to 120.0), reputations.get(subject)?.followerCounts)
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
                assertEquals(serviceCells(service to 10), reputations.docs.getValue(s).influenceScores, "subject ${'$'}s")
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
            // The observer's NEWEST 10040 replaced the one naming `service`, so
            // only service2 is mapped: its card is the one cell, and the lens.
            assertEquals(serviceCells(service2 to 71), reputations.get(subject)?.influenceScores)
            assertEquals(service2, lensOf(observer).rank)
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
            assertNoCells(reputations, subject, "the retraction is the newest version — no cell left")
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
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)
            // Newer card keeps followers but drops rank — via the bulk path.
            store.batchInsert(listOf(card(rank = null, followers = 200, at = 200)))
            val doc = reputations.get(subject)
            assertEquals(emptyMap(), doc?.influenceScores, "the dropped rank must not linger")
            assertEquals(serviceCells(service to 200.0), doc?.followerCounts, "followers updated")
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

            // An emptied parent may stand on one path and never exist on the
            // other (a retraction creates nothing); cells are what is compared.
            assertEquals(
                sequentialReputations.docs.filterValues { !it.isEmpty() },
                bulkReputations.docs.filterValues { !it.isEmpty() },
                "bulk cell-updates must match sequential",
            )
            // And the values are what we expect, not coincidentally-equal empties.
            assertEquals(serviceCells(service to 55, service2 to 9), bulkReputations.docs.getValue(subject).influenceScores)
            assertNoCells(bulkReputations, subjectB, "subjectB was retracted")
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
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores, "ONE cell however many users name the provider")
            assertEquals(service, lensOf(observer).rank, "both users trusting the provider resolve to its cell")
            assertEquals(service, lensOf(observer2).rank)
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
                assertEquals(serviceCells(service to 87), reputations.docs.getValue(s).influenceScores, "subject $s")
            }
        }

    /** One observer un-naming the shared provider drops only THEIR cells. */
    @Test
    fun `dropping a shared provider detaches only that observer`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service))
            store.insert(card())
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)

            // observer2's NEW 10040 picks service2 instead: their LENS moves off
            // the shared provider; the cell, and observer's lens, stay.
            store.insert(list10040(author = observer2, serviceKey = service2))
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)
            assertEquals(service, lensOf(observer).rank)
            assertEquals(service2, lensOf(observer2).rank)
        }

    // ---- crash safety: the dirty marker ---------------------------------------

    /**
     * The event write and the projection write are separate acks. A failure
     * between them stores the events, and the retry comes back all-duplicates —
     * which never reaches the projection. The persisted backlog marker must repair
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
            assertEquals(20, inner.search(EventQuery(kinds = listOf(UserAssertionEvent.KIND))).count { subjectOf(it) in subjects })
            subjects.forEach { assertNull(reps.get(it), "cells must be missing after the failure") }
            assertEquals(subjects.map(::ServiceKey).toSet(), reps.get(ProjectionLedger.MARKER_KEY)?.influenceScores?.keys, "the marker names the dirty subjects")

            // A retry is all duplicates and writes nothing — but it is a trust
            // commit (TrustProjection.commit), and a commit settles the ledger
            // like any trust write, so the retry itself heals. (Before commits,
            // a duplicate-only batch never reached the projection, and the heal
            // waited for the next NEW trust write.)
            st.batchInsert(batch)
            subjects.forEach { s -> assertEquals(serviceCells(service to 87), reps.get(s)?.influenceScores, "healed subject $s") }
            assertNull(reps.get(ProjectionLedger.MARKER_KEY), "marker cleared after the heal")
        }

    /** A crafted card cannot collide with the marker: subjects must be 64-hex. */
    @Test
    fun `a card whose d tag is the marker id projects nothing and breaks nothing`() =
        runBlocking {
            store.insert(list10040())
            store.insert(UserAssertionEvent(id(), service, next(), arrayOf(arrayOf("d", ProjectionLedger.MARKER_KEY), arrayOf("rank", "87")), "", ""))
            assertNull(reputations.get(ProjectionLedger.MARKER_KEY))
            // And ordinary projection still works beside it.
            store.insert(card())
            assertEquals(serviceCells(service to 87), reputations.get(subject)?.influenceScores)
        }

    // ---- expiry (NIP-40) ------------------------------------------------------

    /** An expired card must stop scoring the moment its subject is re-derived, not only when swept. */
    @Test
    fun `an expired card contributes nothing to a re-derivation`() =
        runBlocking {
            var now = System.currentTimeMillis() / 1000
            val inner = InMemoryEventIndex()
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(inner, reps, nowSecs = { now })
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040(author = observer, serviceKey = service))
            st.insert(list10040(author = observer2, serviceKey = service2))
            st.insert(card(signer = service, rank = 87, expires = now + 500))
            assertEquals(serviceCells(service to 87), reps.get(subject)?.influenceScores)

            now += 1_000 // the card is now past its NIP-40 expiration
            st.insert(card(signer = service2, rank = 15, followers = 3))
            proj.recompute.recomputeBatch(listOf(subject), proj.recompute.providerMap(), removeEmpties = true)
            assertEquals(
                serviceCells(service2 to 15),
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
                val projection = TrustProjection(InMemoryEventIndex(), reps)
                val st = NostrSemanticsStore(projection, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
                st.insert(twoServices)
                order.forEach { st.insert(it) }
                // Both cells stand; the observer's lens is the FIRST-listed
                // service (one slot per metric, as Amethyst models it), so the
                // arrival order of the cards cannot change what is served.
                assertEquals(serviceCells(service to 30, service2 to 71), reps.get(subject)?.influenceScores, "one cell per service for order $order")
                assertEquals(
                    service,
                    projection.recompute
                        .providerMap()
                        .lensOf(observer)
                        .rank,
                    "the first-listed provider is the lens",
                )
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
            proj.backlog.drainInBackground { signals++ }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

            // A card by a service nobody names yet: nothing to apply. Then the
            // list — its service walk is the expensive reaction, and it queues.
            st.insert(card())
            st.insert(list10040())
            assertNull(reps.get(subject), "the walk is queued, not run")
            assertTrue(signals >= 1, "the trust write signals the drainer")
            assertNotNull(reps.get(ProjectionLedger.MARKER_KEY), "the queue is persisted — a crash loses nothing")

            proj.backlog.drain { it() }
            assertEquals(serviceCells(service to 87), reps.get(subject)?.influenceScores, "drained")
            assertNull(reps.get(ProjectionLedger.MARKER_KEY), "queue empty, marker gone")
        }

    /** The bulk zero-read cell path stays INLINE in deferred mode — mirror ingest keeps immediate ranking. */
    @Test
    fun `deferred mode keeps the bulk cell path inline`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(InMemoryEventIndex(), reps)
            proj.backlog.drainInBackground { }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

            st.insert(list10040())
            proj.backlog.drain { it() } // attribute the 10040 so the map is live
            val subjects = (1..20).map { it.toString(16).padStart(64, 'f') }
            st.batchInsert(subjects.map { s -> card(about = s) })
            subjects.forEach { s ->
                assertEquals(serviceCells(service to 87), reps.docs.getValue(s).influenceScores, "bulk cells land without a drain")
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
                st.insert(DeletionRequestEvent(id(), service2, 600, arrayOf(arrayOf("e", deleted.id)), "", ""))
                st.insert(card(about = "ef".repeat(32), rank = 30, at = 300))
                st.insert(card(about = "ef".repeat(32), rank = null, followers = null, at = 400)) // retraction
            }

            val syncReps = InMemoryReputationIndex()
            script(NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), syncReps), relay = RelayUrlNormalizer.normalize("ws://localhost:7777")))

            val defReps = InMemoryReputationIndex()
            val defProj = TrustProjection(InMemoryEventIndex(), defReps)
            defProj.backlog.drainInBackground { }
            script(NostrSemanticsStore(defProj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777")))
            defProj.backlog.drain { it() }

            assertEquals(syncReps.docs.filterValues { !it.isEmpty() }, defReps.docs.filterValues { !it.isEmpty() }, "any drain schedule must converge to the inline tensors")
            // And they are the values we expect, not coincidentally-equal emptiness.
            assertEquals(serviceCells(service to 55), syncReps.docs.getValue(subject).influenceScores)
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
                proj.backlog.drainInBackground { }
                val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
                st.insert(list10040())
                st.insert(card(rank = 50, at = 100))
                if (schedule[0]) proj.backlog.drain { it() }
                st.insert(card(rank = 80, at = 200))
                if (schedule[1]) proj.backlog.drain { it() }
                proj.backlog.drain { it() }
                assertEquals(serviceCells(service to 80), reps.get(subject)?.influenceScores, "newest version wins for drains at $schedule")
            }
        }

    /** A STALE version arriving after a drain is rejected at the event layer and cannot regress the cell. */
    @Test
    fun `a stale version arriving later does not regress the projected score`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(InMemoryEventIndex(), reps)
            proj.backlog.drainInBackground { }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            st.insert(card(rank = 80, at = 200))
            proj.backlog.drain { it() }
            assertEquals(serviceCells(service to 80), reps.get(subject)?.influenceScores)

            val outcome = st.batchInsert(listOf(card(rank = 50, at = 100))).single()
            assertTrue(outcome is IEventStore.InsertOutcome.Rejected, "the older version is replaced, not stored")
            proj.backlog.drain { it() }
            assertEquals(serviceCells(service to 80), reps.get(subject)?.influenceScores, "nothing left to derive the stale value from")
        }

    /**
     * A bulk-projected cell superseded by a later single insert follows the
     * replaceable winner AT ONCE: the single path is the same cell write, so
     * deferral no longer costs ranking a lag on a card.
     */
    @Test
    fun `an inline bulk cell follows a later superseding single insert at once`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(InMemoryEventIndex(), reps)
            proj.backlog.drainInBackground { }
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            proj.backlog.drain { it() }
            // A real bulk batch (>= the bulk threshold), so v1's cell lands INLINE.
            val fillers = (1..15).map { card(about = it.toString(16).padStart(64, 'b'), rank = 10) }
            st.batchInsert(fillers + card(rank = 50, at = 100))
            assertEquals(serviceCells(service to 50), reps.get(subject)?.influenceScores, "bulk cell is immediate")

            st.insert(card(rank = 80, at = 200))
            assertEquals(serviceCells(service to 80), reps.get(subject)?.influenceScores, "the single insert's cell is immediate too")
            proj.backlog.drain { it() }
            assertEquals(serviceCells(service to 80), reps.get(subject)?.influenceScores, "and a drain changes nothing")
        }

    /** Create then delete in one session: the cells die with the event, under every drain timing. */
    @Test
    fun `create then delete converges to no cells under any drain timing`() =
        runBlocking {
            for (drainBetween in listOf(false, true)) {
                val reps = InMemoryReputationIndex()
                val proj = TrustProjection(InMemoryEventIndex(), reps)
                proj.backlog.drainInBackground { }
                val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
                st.insert(list10040())
                val scored = card()
                st.insert(scored)
                if (drainBetween) {
                    proj.backlog.drain { it() }
                    assertEquals(serviceCells(service to 87), reps.get(subject)?.influenceScores, "projected while the event lives")
                }
                st.insert(DeletionRequestEvent(id(), service, next(), arrayOf(arrayOf("e", scored.id)), "", ""))
                proj.backlog.drain { it() }
                assertNoCells(reps, subject, "deleted event derives nothing (drainBetween=$drainBetween)")
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
     * The write-ahead marker must be priced to the op: one cell-add call for a
     * bulk batch as for a single, which the engine sends as one update of the
     * marker document, and never a whole-doc put, which would not compose with
     * a peer writer's insurance.
     */
    @Test
    fun `the marker write-ahead is one cell-add call for a bulk batch and never a doc put`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            assertEquals(1, counting.markerCellAddCalls, "a single's backlog (here: the list's fresh service) is one cell-add call")

            val subjects = (1..100).map { it.toString(16).padStart(64, 'c') }
            val cellsBefore = counting.markerCellAdds
            st.batchInsert(subjects.map { s -> card(about = s) })
            assertEquals(2, counting.markerCellAddCalls, "a bulk batch must not pay per-subject marker calls")
            assertEquals(100, counting.markerCellAdds - cellsBefore, "its one call names every subject it insures")

            st.insert(card())
            assertEquals(3, counting.markerCellAddCalls)
            assertEquals(0, counting.markerPuts, "the marker is never rewritten whole")
        }

    /**
     * A CLEAN COMMIT'S BRACKET IS TWO MARKER CALLS: the write-ahead and its
     * clear. It used to read the marker back and remove the emptied document
     * too — four serial round trips under the trust gate, ~15% of the ingest
     * lock on a loaded cluster (issue #183). The husk it left is harmless,
     * and a drain that empties the ledger still removes it.
     */
    @Test
    fun `a clean commit writes the marker and clears it, and never reads it back`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            st.insert(card(about = "c0".repeat(32)))
            val calls = counting.markerCalls
            val reads = counting.markerReads

            val subjects = (1..100).map { it.toString(16).padStart(64, 'c') }
            st.batchInsert(subjects.map { s -> card(about = s) })
            assertEquals(calls + 2, counting.markerCalls, "one write-ahead and one clear for the batch")
            st.insert(card(about = subjects[0], rank = 3))
            assertEquals(calls + 4, counting.markerCalls, "and the same for a single card")
            assertEquals(reads, counting.markerReads, "no read-back on a commit's clean finish")

            assertMarkerNamesNothing(counting, "the marker names nothing once the commits are clean")
            subjects.drop(1).forEach { s -> assertEquals(87, counting.get(s)?.influenceScores?.get(ServiceKey(service))) }
        }

    /**
     * A CARD NO 10040 MAPS WRITES NO CELL, so it has nothing to insure: a
     * crash between its write and the flush leaves nothing to repair. Insuring
     * it anyway cost a write-ahead and a clear per commit for no cell at all,
     * and a mirror's by-kind card ingest is mostly such cards (issue #183).
     */
    @Test
    fun `cards by a signer no 10040 maps cost no marker round trip`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            st.insert(card(about = "c0".repeat(32)))
            val calls = counting.markerCalls
            val unmapped = "7f".repeat(32)

            val subjects = (1..100).map { it.toString(16).padStart(64, 'e') }
            val out = st.batchInsert(subjects.map { s -> card(signer = unmapped, about = s) })
            assertTrue(out.all { it is IEventStore.InsertOutcome.Accepted }, "$out")
            st.insert(card(signer = unmapped, about = subject))
            assertEquals(calls, counting.markerCalls, "no marker I/O for cards that write no cell")
            subjects.forEach { s -> assertNoCells(counting.inner, s, "an unmapped signer's card writes no cell") }

            // Named later, its stored cards are still reached: through the walk
            // the naming list queues, not through any insurance.
            st.insert(list10040(author = observer2, serviceKey = unmapped))
            subjects.forEach { s -> assertEquals(87, counting.get(s)?.influenceScores?.get(ServiceKey(unmapped))) }
        }

    /**
     * A MAP THAT CANNOT ANSWER INSURES EVERYTHING. A pass naming no service is
     * a relay with no 10040s or an engine not yet serving them, and only the
     * first could safely skip the insurance; a pass a 10040 write just dropped
     * is no answer at all.
     */
    @Test
    fun `cards are insured while the provider map names nothing or was just dropped`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val proj = TrustProjection(InMemoryEventIndex(), counting)
            val st = NostrSemanticsStore(proj, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            proj.recompute.providerMap()
            val subjects = (1..20).map { it.toString(16).padStart(64, 'e') }
            st.batchInsert(subjects.map { s -> card(about = s) })
            assertTrue(counting.markerKeysAdded.containsAll(subjects), "an empty pass insures every card")

            st.insert(list10040())
            proj.recompute.invalidateProviders()
            val later = (1..20).map { it.toString(16).padStart(64, 'f') }
            st.batchInsert(later.map { s -> card(signer = "7f".repeat(32), about = s) })
            assertTrue(counting.markerKeysAdded.containsAll(later), "a dropped pass insures every card")
        }

    /**
     * A 10040 IN THE SAME BATCH NAMES ITS SERVICES for the batch's cover: the
     * commit is about to store it, and that service's cards would then write
     * cells — so they are insured like any mapped signer's.
     */
    @Test
    fun `a batch carrying the 10040 that names its cards' signer insures those cards`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            st.insert(card(about = "c0".repeat(32)))
            val subjects = (1..50).map { it.toString(16).padStart(64, 'e') }
            val out = st.batchInsert(listOf(list10040(author = observer2, serviceKey = service2)) + subjects.map { s -> card(signer = service2, about = s) })
            assertTrue(out.all { it is IEventStore.InsertOutcome.Accepted }, "$out")
            assertTrue(counting.markerKeysAdded.containsAll(subjects), "the batch's own list named their signer")
            subjects.forEach { s -> assertEquals(87, counting.get(s)?.influenceScores?.get(ServiceKey(service2))) }
        }

    /**
     * The same, when the list arrives as a PLAIN [Event] of kind 10040 rather
     * than the typed class: a caller is free to hand the store either, and
     * the batch's cover must read its services all the same. Address-keyed,
     * with the list last, so the cards are admitted one call each before it —
     * nothing but the planned cover can insure them.
     */
    @Test
    fun `a batch carrying an untyped 10040 still insures the cards it names`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(supersedesViaPut = true), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            st.insert(card(about = "c0".repeat(32)))
            val subjects = (1..50).map { it.toString(16).padStart(64, 'e') }
            val typed = list10040(author = observer2, serviceKey = service2)
            val plain = Event(typed.id, typed.pubKey, typed.createdAt, typed.kind, typed.tags, typed.content, typed.sig)
            val out = st.batchInsert(subjects.map { s -> card(signer = service2, about = s) } + plain)
            assertTrue(out.all { it is IEventStore.InsertOutcome.Accepted }, "$out")
            assertTrue(counting.markerKeysAdded.containsAll(subjects), "the untyped list named their signer too")
            subjects.forEach { s -> assertEquals(87, counting.get(s)?.influenceScores?.get(ServiceKey(service2))) }
        }

    /**
     * ONE ASSISTANT RUN, ONE REACTION PER BATCH — on the address-keyed path,
     * which replays each address through the engine's conditional put, one
     * `putIfNewer` per card. The projection used to react per CALL, so every
     * card paid its own ledger bracket and its own cell write (139 cards/s on
     * a 20,000-card run; see TrustProjection.commit, which now makes the
     * commit the unit of reaction). The verdicts and the cells must be exactly
     * what the per-card path produced — including a same-address pair inside
     * one batch, in both orders.
     */
    @Test
    fun `an address-keyed bulk batch of cards writes its cells in one call, newest winning`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(supersedesViaPut = true), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            val subjects = (1..100).map { it.toString(16).padStart(64, 'c') }
            val adds = counting.markerCellAddCalls

            val run1 = st.batchInsert(subjects.mapIndexed { i, s -> card(about = s, rank = i % 50, at = 2_000_000) })
            assertTrue(run1.all { it is IEventStore.InsertOutcome.Accepted }, "run 1: $run1")
            assertEquals(1, counting.subjectCellCalls, "a batch of cards is one cell write, not one per card")
            assertEquals(adds + 1, counting.markerCellAddCalls, "and one marker write-ahead for the batch, not one per card")
            assertEquals(0, counting.markerPuts, "never a whole-marker rewrite")

            // The next run supersedes every card. Inside it, subject 0 carries an
            // older-then-newer pair and subject 1 a newer-then-older pair.
            val run2Cards =
                buildList {
                    subjects.forEachIndexed { i, s -> add(card(about = s, rank = 99 - i % 50, at = 2_000_300)) }
                    add(card(about = subjects[0], rank = 7, at = 2_000_400))
                    add(card(about = subjects[1], rank = 8, at = 2_000_200))
                }
            val run2 = st.batchInsert(run2Cards)
            assertTrue(run2.dropLast(1).all { it is IEventStore.InsertOutcome.Accepted }, "run 2: $run2")
            assertTrue(run2.last() is IEventStore.InsertOutcome.Rejected, "an older version of a stored address is REPLACED: ${run2.last()}")
            assertEquals(2, counting.subjectCellCalls, "the superseding run is one more cell write")
            assertEquals(adds + 2, counting.markerCellAddCalls)

            assertEquals(7, counting.get(subjects[0])?.influenceScores?.get(ServiceKey(service)), "the in-batch newer version lands")
            assertEquals(98, counting.get(subjects[1])?.influenceScores?.get(ServiceKey(service)), "the in-batch older version loses")
            subjects.drop(2).forEachIndexed { k, s ->
                val i = k + 2
                assertEquals(99 - i % 50, counting.get(s)?.influenceScores?.get(ServiceKey(service)), "subject $i carries run 2's rank")
            }
            // One version per address stored: run 1 is gone, run 2 holds.
            assertEquals(100, st.count(Filter(kinds = listOf(UserAssertionEvent.KIND))))
        }

    /**
     * The read-then-supersede commit writes its diff as a removeDocs and then a
     * putAll, inside ONE trust commit, so a superseded card is not retracted
     * only for its winner to write the same cell back (TrustProjection.commit
     * nets the two calls per address). What a winner no longer carries must
     * still leave — through the winner's own update, not a retraction — and a
     * card a deletion took, with no winner, is still retracted.
     */
    @Test
    fun `a superseding bulk batch rewrites each cell once and retracts nothing it refills`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            val subjects = (1..50).map { it.toString(16).padStart(64, 'd') }
            st.batchInsert(subjects.map { s -> card(about = s, rank = 10, followers = 100, at = 2_000_000) })
            val cellCalls = counting.subjectCellCalls
            val removeCalls = counting.subjectRemoveCalls

            // Run 2: every card superseded; subject 0's winner drops its followers tag.
            val run2 = st.batchInsert(subjects.mapIndexed { i, s -> card(about = s, rank = 20, followers = if (i == 0) null else 200, at = 2_000_300) })
            assertTrue(run2.all { it is IEventStore.InsertOutcome.Accepted }, "run 2: $run2")
            assertEquals(cellCalls + 1, counting.subjectCellCalls, "one cell write for the superseding batch")
            assertEquals(removeCalls, counting.subjectRemoveCalls, "no retraction of a cell its winner refills")
            assertEquals(ReputationDoc(subjects[0], serviceCells(service to 20), emptyMap()), counting.get(subjects[0]), "the dropped dimension still leaves")
            subjects.drop(1).forEach { s -> assertEquals(ReputationDoc(s, serviceCells(service to 20), serviceCells(service to 200.0)), counting.get(s)) }
            assertEquals(50, st.count(Filter(kinds = listOf(UserAssertionEvent.KIND))), "one version per address")

            // A kind 5 taking a card has no winner: its cell is retracted.
            val doomed = st.query<UserAssertionEvent>(Filter(kinds = listOf(UserAssertionEvent.KIND), tags = mapOf("d" to listOf(subjects[1])))).single()
            st.insert(DeletionRequestEvent(id(), service, next(), arrayOf(arrayOf("e", doomed.id)), "", ""))
            val left = counting.get(subjects[1])
            assertTrue(left == null || left.isEmpty(), "a deleted card's cell goes, got $left")
        }

    /**
     * The commit, not the call, is the unit of reaction — so a batch too small
     * for the bulk path (under BULK_MIN, admitted one event at a time through
     * putIfNewer) is still one cell write, which no batch-shaped port member
     * could have reached.
     */
    @Test
    fun `a small batch admitted event by event still writes its cells once`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            val before = counting.subjectCellCalls
            val subjects = (1..10).map { it.toString(16).padStart(64, 'f') }
            val out = st.batchInsert(subjects.mapIndexed { i, s -> card(about = s, rank = 40 + i) })
            assertTrue(out.all { it is IEventStore.InsertOutcome.Accepted }, "$out")
            assertEquals(before + 1, counting.subjectCellCalls, "ten admissions, one commit, one cell write")
            subjects.forEachIndexed { i, s -> assertEquals(40 + i, counting.get(s)?.influenceScores?.get(ServiceKey(service))) }
        }

    /**
     * Inside one commit only the LAST thing done to an address reaches the
     * cells: a card stored and then deleted by a later event of the same
     * (mixed) batch is one retraction, and the cell does not exist afterwards.
     */
    @Test
    fun `a card stored and deleted in one mixed batch leaves no cell`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            val kept = (1..20).map { it.toString(16).padStart(64, 'b') }
            val doomedCard = card(about = "c1".repeat(32), rank = 55)
            val batch =
                kept.map { s -> card(about = s, rank = 33) } +
                    doomedCard +
                    DeletionRequestEvent(id(), service, next(), arrayOf(arrayOf("e", doomedCard.id)), "", "")
            st.batchInsert(batch)
            val left = counting.get("c1".repeat(32))
            assertTrue(left == null || left.isEmpty(), "the deleted card leaves no cell, got $left")
            kept.forEach { s -> assertEquals(33, counting.get(s)?.influenceScores?.get(ServiceKey(service))) }
            assertMarkerNamesNothing(counting, "a clean commit leaves no marker cell")
        }

    /**
     * A REJECTION IS NOT A FAILED COMMIT. `insert` signals duplicate, replaced,
     * deleted and the rest by throwing [RejectedException]
     * from inside the commit; the ledger reads a throw as "the writes may have
     * died half way" and queues the whole cover. A relay re-offered a card it
     * already holds — the commonest card insert there is — would then leave
     * a queued re-derive and a marker cell behind, every time.
     */
    @Test
    fun `a rejected card insert leaves no ledger work and no marker`() =
        runBlocking {
            store.insert(list10040())
            projection.backlog.drain { it() }
            val first = card(rank = 70)
            store.insert(first)
            assertFailsWith<RejectedException> { store.insert(first) } // duplicate
            assertFailsWith<RejectedException> { store.insert(card(rank = 10, at = first.createdAt - 1)) } // replaced
            assertEquals(0L, projection.backlog.pendingSubjects(), "nothing to re-derive after a rejection")
            assertMarkerNamesNothing(reputations, "and no marker left naming it")
            assertEquals(70, reputations.get(subject)?.influenceScores?.get(ServiceKey(service)))
        }

    /**
     * A commit bigger than [TrustProjection.FLUSH_AT] flushes mid-way, and the
     * flush must not land between a supersession's removeDocs and the putAll
     * that refills it: settled there, every superseded cell was retracted and
     * then written straight back, with the subjects briefly cell-less to a
     * gated read. Removals now wait for the next removal call or the commit's
     * end, so the refill still nets them away.
     */
    @Test
    fun `a superseding batch past the flush bound still retracts nothing it refills`() =
        runBlocking {
            val counting = MarkerCountingReputationIndex(InMemoryReputationIndex())
            val st = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            st.insert(list10040())
            val n = TrustProjection.FLUSH_AT + 100
            // Zero-padded behind a fixed prefix: padding with a HEX digit makes
            // 0xa and 0xaa the same subject.
            val subjects = (1..n).map { "d0" + it.toString(16).padStart(62, '0') }
            st.batchInsert(subjects.map { s -> card(about = s, rank = 10, at = 2_000_000) })
            val removeCalls = counting.subjectRemoveCalls
            val run2 = st.batchInsert(subjects.map { s -> card(about = s, rank = 20, at = 2_000_300) })
            assertTrue(run2.all { it is IEventStore.InsertOutcome.Accepted }, "run 2: ${run2.groupingBy { it.toString().take(60) }.eachCount()}")
            assertEquals(removeCalls, counting.subjectRemoveCalls, "no retraction of cells the same commit refills")
            assertEquals(20, counting.get(subjects.last())?.influenceScores?.get(ServiceKey(service)))
            assertMarkerNamesNothing(counting)
        }

    /**
     * Mid-commit retirement shrinks the marker only when no other call of the
     * commit is between "told it is covered" and "recorded what it wrote" —
     * otherwise the cell it is about to write would lose its insurance.
     */
    @Test
    fun `a cover retires insurance only when no call is in flight`() =
        runBlocking {
            val reps = InMemoryReputationIndex()
            val proj = TrustProjection(InMemoryEventIndex(), reps)
            proj.backlog.drainInBackground { }
            val a = "a1".repeat(32)
            proj.backlog.insuring(ProjectionWork.NONE) { cover ->
                cover.extend(ProjectionWork(setOf(a), emptySet()), call = true)
                assertEquals(setOf(ServiceKey(a)), reps.get(ProjectionLedger.MARKER_KEY)?.influenceScores?.keys, "insured before the write")
                assertTrue(!cover.retire(setOf(a)), "refused while the call is in flight")
                assertEquals(setOf(ServiceKey(a)), reps.get(ProjectionLedger.MARKER_KEY)?.influenceScores?.keys, "and the insurance stands")
                cover.landed()
                assertTrue(cover.retire(setOf(a)), "allowed once it has landed")
                val left = reps.get(ProjectionLedger.MARKER_KEY)
                assertTrue(left == null || left.influenceScores.isEmpty(), "its marker cell is cleared, got $left")
                Outcome(Unit, ProjectionWork.NONE)
            }
        }
}

/** Counts marker-doc traffic, separating pipelined cell adds from whole-doc puts. */
private class MarkerCountingReputationIndex(
    val inner: InMemoryReputationIndex,
) : ReputationIndex by inner {
    var markerCellAdds = 0
    var markerCellAddCalls = 0
    var markerPuts = 0

    /** Every call that touches the marker document — each one a round trip on a real engine. */
    var markerCalls = 0

    /** Reads of the marker document. */
    var markerReads = 0

    /** Every key ever written to the marker — what some write-ahead insured. */
    val markerKeysAdded = HashSet<String>()

    override suspend fun get(pubkey: String): ReputationDoc? {
        if (pubkey == ProjectionLedger.MARKER_KEY) {
            markerReads++
            markerCalls++
        }
        return inner.get(pubkey)
    }

    override suspend fun remove(pubkey: String) {
        if (pubkey == ProjectionLedger.MARKER_KEY) markerCalls++
        inner.remove(pubkey)
    }

    /** updateCells calls that write a real subject's cells — the projection's own writes, not the ledger's. */
    var subjectCellCalls = 0

    override suspend fun updateCells(updates: List<ReputationCells>) {
        val adds = updates.count { it.subject == ProjectionLedger.MARKER_KEY }
        if (adds > 0) {
            markerCellAddCalls++
            markerCalls++
        }
        markerCellAdds += adds
        updates.forEach { if (it.subject == ProjectionLedger.MARKER_KEY) markerKeysAdded += it.key.hex }
        if (updates.any { it.subject != ProjectionLedger.MARKER_KEY }) subjectCellCalls++
        inner.updateCells(updates)
    }

    /** removeCells calls that retract a real subject's cells. */
    var subjectRemoveCalls = 0

    override suspend fun removeCells(removals: List<CellRemoval>) {
        if (removals.any { it.subject != ProjectionLedger.MARKER_KEY }) subjectRemoveCalls++
        if (removals.any { it.subject == ProjectionLedger.MARKER_KEY }) markerCalls++
        inner.removeCells(removals)
    }

    override suspend fun put(reputation: ReputationDoc) {
        if (reputation.pubkey == ProjectionLedger.MARKER_KEY) {
            markerPuts++
            markerCalls++
        }
        inner.put(reputation)
    }
}

/**
 * Fails ONE projection cell write on demand — the "engine hiccup after the
 * event write" the backlog marker exists for. The ledger's write-ahead marker
 * persist ALSO rides updateCells (and runs before the event write), so the
 * failure targets only updates that touch real subjects.
 */
private class FailingCellsReputationIndex(
    private val inner: InMemoryReputationIndex,
) : ReputationIndex by inner {
    var failNext = false

    override suspend fun updateCells(updates: List<ReputationCells>) {
        if (failNext && updates.any { it.subject != ProjectionLedger.MARKER_KEY }) {
            failNext = false
            throw RuntimeException("simulated projection failure")
        }
        inner.updateCells(updates)
    }
}
