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
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A NIP-77 SNAPSHOT IS THE SET ITS REQ SERVES, WHOLE.
 *
 * Two ways it was not, both on a relay that stamped its REQ `default_limit` on
 * a NEG-OPEN: a limit'd filter left the id walk for ONE bounded search page, so
 * a reconcile covered the newest `default_limit` events and reported that as
 * the set; and the `observer:` token's gate ran with no lens resolved — the
 * empty tensor, "trusts nobody" — so an observer-declared reconcile was EMPTY.
 * What reaches the engine is pinned here, and what comes back through [gated], a
 * model of the engine's floor over the fixture's ranks (the in-memory reference
 * cannot gate); the real engine's answer is FilterMatrixIT's snapshot column.
 */
class NegentropySnapshotTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private val authors = listOf(90, 50, 10, 1).mapIndexed { i, _ -> "a${i + 1}".repeat(32) }
    private val ranks = listOf(90, 50, 10, 1)

    private val walked = mutableListOf<EventQuery>()
    private val searched = mutableListOf<EventQuery>()
    private val inner = InMemoryEventIndex()

    /**
     * The engine's observer gate over the fixture: a gated query admits an author whose rank under
     * the query's resolved service clears [EventYql.gateFloor]; an unresolved lens admits nobody.
     */
    private fun gated(
        query: EventQuery,
        pubkey: String,
    ): Boolean {
        val floor = EventYql.gateFloor(query) ?: return true
        val rank = if (query.rankKey == service) ranks.getOrNull(authors.indexOf(pubkey)) ?: 0 else 0
        return rank >= floor
    }

    /** A degraded cluster on which the provider lists cannot be read. */
    @Volatile
    private var providerListsDown = false

    private fun failIfDown(query: EventQuery) {
        if (providerListsDown && TrustProviderListEvent.KIND in query.kinds) throw IllegalStateException("content node down")
    }

    private val recording =
        object : EventIndex by inner {
            override suspend fun search(query: EventQuery): List<EventDoc> {
                failIfDown(query)
                searched += query
                return inner.search(query).filter { gated(query, it.pubkey) }
            }

            override suspend fun rawSearch(query: EventQuery): List<RawEvent> = inner.rawSearch(query).filter { gated(query, it.pubKey) }

            override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = inner.searchRanked(query).filter { gated(query, it.hit.pubkey) }

            override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = inner.rawSearchRanked(query).filter { gated(query, it.hit.pubKey) }

            override suspend fun visitIds(
                query: EventQuery,
                withDTag: Boolean,
                onPage: suspend (List<DocRef>) -> Boolean,
            ) {
                failIfDown(query)
                walked += query
                inner.visitIds(query, withDTag) { page -> onPage(page.filter { ref -> inner.get(ref.id)?.let { gated(query, it.pubkey) } ?: false }) }
            }
        }

    /** The store's clock; a test moves it to let an event expire after it was stored. */
    private var clock = System.currentTimeMillis() / 1000

    private val trust = TrustProjection(recording, InMemoryReputationIndex())

    private val store = NostrSemanticsStore(trust, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"), nowSecs = { clock })

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    /** Four authors at ranks 90/50/10/1 under [service], three notes each, and the observer's 10040 naming it. */
    private fun seed() =
        runBlocking {
            store.batchInsert(
                authors.zip(ranks).map { (a, r) -> UserAssertionEvent(id(), service, 1_000L + r, arrayOf(arrayOf("d", a), arrayOf("rank", r.toString())), "", "") } +
                    authors.flatMap { a -> (0 until 3).map { n -> TextNoteEvent(id(), a, 1_700_000_000L + seq, emptyArray(), "note $n", "") } },
            )
            store.insert(TrustProviderListEvent(id(), observer, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
        }

    private fun snapshot(filter: Filter): Set<String> = runBlocking { store.snapshotIdsForNegentropy(listOf(filter), null, null).map { it.id }.toSet() }

    private fun notesBy(minRank: Int): Set<String> =
        runBlocking {
            store
                .query<TextNoteEvent>(Filter(kinds = listOf(1), search = "include:spam"))
                .filter { n -> (ranks[authors.indexOf(n.pubKey)]) >= minRank }
                .map { it.id }
                .toSet()
        }

    @Test
    fun `an observer-declared snapshot walks through the resolved lens`() {
        seed()
        walked.clear()
        snapshot(Filter(kinds = listOf(1), search = "observer:$observer filter:rank:gte:50"))
        val q = walked.single()
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking)
        assertEquals(service, q.rankKey, "the token resolves to the service its 10040 names, not the empty lens")
        assertEquals(50.0, q.minRank)
    }

    @Test
    fun `every lens mode reaches the walk as the set its REQ serves`() {
        seed()

        fun walkOf(search: String): EventQuery {
            walked.clear()
            snapshot(Filter(kinds = listOf(1), search = search))
            return walked.single()
        }
        walkOf("observer:$observer").let {
            assertEquals(EventYql.RANK_RECENCY_GATED, it.ranking)
            assertEquals(2.0, it.minRank, "the default floor")
        }
        walkOf("observer:$observer include:spam").let { assertNull(it.ranking, "a waiver lifts the default floor: ungated") }
        walkOf("observer:$observer include:spam filter:rank:gte:50").let { assertEquals(50.0, it.minRank, "an explicit floor survives the waiver") }
        walkOf("include:spam").let { assertNull(it.ranking, "no observer, no gate") }
    }

    @Test
    fun `a limit'd plain filter walks instead of taking one search page`() {
        seed()
        walked.clear()
        searched.clear()
        val newest = snapshot(Filter(kinds = listOf(1), limit = 5, search = "include:spam"))
        assertEquals(5, newest.size)
        assertEquals(1, walked.size, "the walk honours the limit as the newest N")
        assertTrue(searched.none { it.kinds == listOf(1) }, "no bounded search page: $searched")
    }

    /**
     * A trust sort reaches the walk AS the trust sort, lens resolved: the
     * engine gates it on EventYql.gateFloor (the floor its REQ gates on), walks
     * it whole when unlimited, and answers a limit'd one with the ranked search
     * its REQ runs — its first N are its most trusted, not its newest.
     * `sort:text` reads no lens, so its walk gates nothing.
     */
    @Test
    fun `a trust sort reaches the walk gated, and sort-text ungated`() {
        seed()
        listOf("sort:rank", "sort:rank:asc", "sort:followers").forEach { sort ->
            walked.clear()
            snapshot(Filter(kinds = listOf(1), search = "observer:$observer $sort filter:rank:gte:50"))
            val q = walked.single()
            assertEquals(service, q.rankKey, sort)
            assertEquals(50.0, EventYql.gateFloor(q), sort)
            assertTrue(EventYql.walksGated(q), "$sort: the walk gates")
            walked.clear()
            snapshot(Filter(kinds = listOf(1), limit = 3, search = "observer:$observer $sort"))
            assertTrue(!EventYql.walksInOrder(walked.single()), "$sort: a limit'd trust sort is the engine's ranked search, not the newest N")
        }
        walked.clear()
        snapshot(Filter(kinds = listOf(1), search = "observer:$observer sort:text"))
        assertTrue(!EventYql.walksGated(walked.single()), "sort:text reads no lens, so its set is ungated")
    }

    private fun storageSet(filters: List<Filter>): Set<Pair<Long, String>>? =
        runBlocking {
            store.sealedNegentropyStorage(filters, 1_000)?.let { storage ->
                (0 until storage.size()).map { i -> storage.getItem(i).let { it.timestamp to it.id.toHexString() } }.toSet()
            }
        }

    @Test
    fun `the sealed storage holds exactly the snapshot's set, through the same walks`() {
        seed()
        val filters = listOf(Filter(kinds = listOf(1)), Filter(authors = authors.take(2)), Filter(kinds = listOf(1), search = "observer:$observer filter:rank:gte:50"))
        walked.clear()
        val listed = runBlocking { store.snapshotIdsForNegentropy(filters, 1_000, null) }.map { it.createdAt to it.id }
        val listWalks = walked.toList()
        walked.clear()
        val sealed = storageSet(filters)
        assertEquals(listed.size, listed.toSet().size, "the list is deduplicated across filters")
        assertEquals(listed.toSet(), sealed)
        assertEquals(listWalks, walked.toList())
    }

    @Test
    fun `the sealed storage is null past the cap, as the list's sentinel says`() {
        seed()
        val all = Filter(kinds = listOf(1))
        val n = snapshot(all).size
        assertEquals(n, runBlocking { store.sealedNegentropyStorage(listOf(all), n)?.size() })
        assertNull(runBlocking { store.sealedNegentropyStorage(listOf(all), n - 1) })
    }

    @Test
    fun `a cap at Int MAX_VALUE walks the whole set`() {
        seed()
        val all = Filter(kinds = listOf(1))
        val n = snapshot(all).size
        assertEquals(n, runBlocking { store.snapshotIdsForNegentropy(listOf(all), Int.MAX_VALUE, null) }.size)
        assertEquals(n, runBlocking { store.sealedNegentropyStorage(listOf(all), Int.MAX_VALUE)?.size() })
    }

    // ---- the connection's own lens (NIP-42) ----------------------------------

    /** An observer whose 10040 was never stored: it resolves to no lens, and so trusts nobody. */
    private val lensless = "0c".repeat(32)

    private fun <T> asCaller(
        who: String?,
        block: suspend () -> T,
    ): T = runBlocking { if (who == null) block() else withContext(StoreQueryContext(setOf(who))) { block() } }

    private fun servedTo(
        who: String?,
        filter: Filter,
    ): Set<String> =
        asCaller(who) {
            val ids = HashSet<String>()
            store.rawQuery(listOf(filter)) { ids += it.id }
            check(store.query<Event>(filter).map { it.id }.toSet() == ids) { "query and rawQuery disagree" }
            ids
        }

    private fun snapshotOf(
        who: String?,
        filter: Filter,
    ): Set<String> = asCaller(who) { store.snapshotIdsForNegentropy(listOf(filter), null, null).map { it.id }.toSet() }

    private fun sealedOf(
        who: String?,
        filters: List<Filter>,
        maxEntries: Int = 1_000,
    ): Set<String>? =
        asCaller(who) {
            store.sealedNegentropyStorage(filters, maxEntries)?.let { storage -> (0 until storage.size()).map { storage.getItem(it).id.toHexString() }.toSet() }
        }

    @Test
    fun `an authenticated snapshot is the set the same connection's REQ serves`() {
        seed()
        val notes = Filter(kinds = listOf(1))
        val req = servedTo(observer, notes)
        assertEquals(notesBy(minRank = 2), req, "the REQ drops the author below the default floor")
        assertEquals(req, snapshotOf(observer, notes), "a NEG-OPEN of the same filter on the same connection")
        assertEquals(req, sealedOf(observer, listOf(notes)), "the sealed storage a NEG-OPEN is served from")
        // Two filters take the dedup path rather than the single-filter walk; the lens rides both.
        val two = listOf(notes, Filter(authors = authors.take(2)))
        assertEquals(asCaller(observer) { store.query<Event>(two).map { it.id }.toSet() }, sealedOf(observer, two))
    }

    @Test
    fun `a by-id read on an authenticated connection is gated, and its snapshot with it`() {
        seed()
        val low = notesBy(minRank = 0) - notesBy(minRank = 2)
        val byId = Filter(ids = low.toList())
        assertEquals(emptySet(), servedTo(observer, byId), "a plain ids REQ is read through the connection's lens")
        assertEquals(emptySet(), snapshotOf(observer, byId))
        assertEquals(low, servedTo(observer, byId.copy(search = "include:spam")))
        assertEquals(low, snapshotOf(observer, byId.copy(search = "include:spam")))
    }

    @Test
    fun `include spam opts an authenticated snapshot out of the connection's lens`() {
        seed()
        val all = Filter(kinds = listOf(1), search = "include:spam")
        assertEquals(notesBy(minRank = 0), servedTo(observer, all))
        assertEquals(servedTo(observer, all), snapshotOf(observer, all))
        // Under a lens that trusts nobody, too: the waiver is what a mirror reads with.
        assertEquals(notesBy(minRank = 0), snapshotOf(lensless, all))
        assertEquals(servedTo(lensless, all), snapshotOf(lensless, all))
    }

    @Test
    fun `an observer token outranks the connection's lens on a snapshot, as on a REQ`() {
        seed()
        val token = Filter(kinds = listOf(1), search = "observer:$observer filter:rank:gte:50")
        assertEquals(notesBy(minRank = 50), servedTo(lensless, token))
        assertEquals(servedTo(lensless, token), snapshotOf(lensless, token))
        assertEquals(emptySet(), snapshotOf(lensless, Filter(kinds = listOf(1))), "the connection's own lens, which trusts nobody")
    }

    @Test
    fun `an unauthenticated snapshot is ungated, through the same walk as before`() {
        seed()
        walked.clear()
        assertEquals(notesBy(minRank = 0), snapshotOf(null, Filter(kinds = listOf(1))))
        val q = walked.single()
        assertNull(q.ranking, "no lens, no gate")
        assertNull(q.observer)
        assertNull(q.minRank)
    }

    @Test
    fun `an authenticated snapshot still drops what has expired`() {
        seed()
        val trusted = authors[0]
        val expired = TextNoteEvent(id(), trusted, 1_700_000_000L, arrayOf(arrayOf("expiration", (clock + 10).toString())), "gone", "")
        runBlocking { store.insert(expired) }
        assertTrue(expired.id in snapshotOf(observer, Filter(kinds = listOf(1))), "stored, and admitted, until it expires")
        clock += 60
        val notes = Filter(kinds = listOf(1))
        assertTrue(expired.id !in servedTo(observer, notes))
        assertTrue(expired.id !in snapshotOf(observer, notes))
        assertEquals(servedTo(observer, notes), snapshotOf(observer, notes))
    }

    @Test
    fun `the cap counts the set the lens admits`() {
        seed()
        val notes = Filter(kinds = listOf(1))
        val n = servedTo(observer, notes).size
        assertTrue(n < notesBy(minRank = 0).size, "the fixture gates something")
        assertEquals(n, sealedOf(observer, listOf(notes), maxEntries = n)?.size)
        assertNull(sealedOf(observer, listOf(notes), maxEntries = n - 1), "one over the gated set is over budget")
        assertEquals(n, asCaller(observer) { store.snapshotIdsForNegentropy(listOf(notes), n - 1, null) }.size, "maxEntries + 1, the overflow sentinel")
    }

    private class Caller(
        override val connectionId: Long,
        override val authenticatedUsers: Set<String>,
    ) : RequestContext {
        override val policy: IRelayPolicy get() = error("not asked")
    }

    private val noIngest =
        object : EventIngest {
            override suspend fun submit(
                event: Event,
                skipVerify: Boolean,
                onComplete: (IEventStore.InsertOutcome) -> Unit,
            ) = error("no writes here")

            override fun hasBacklog(): Boolean = false

            override fun close() {}
        }

    /** Quartz's relay backend caches NEG-OPEN snapshots per caller; each caller must get its own lens. */
    @Test
    fun `a cached snapshot never answers one caller with another's lens`() {
        seed()
        val live = LiveEventStore(store, noIngest)
        val notes = listOf(Filter(kinds = listOf(1)))

        fun negOpen(caller: Caller): Set<String>? =
            runBlocking {
                live.sealedNegentropyStorage(caller, notes, 1_000)?.let { storage -> (0 until storage.size()).map { storage.getItem(it).id.toHexString() }.toSet() }
            }
        val trusting = Caller(1, setOf(observer))
        val anonymous = Caller(2, emptySet())
        val nobody = Caller(3, setOf(lensless))
        assertEquals(servedTo(observer, notes[0]), negOpen(trusting))
        assertEquals(notesBy(minRank = 0), negOpen(anonymous), "not the authenticated caller's cached set")
        assertEquals(emptySet(), negOpen(nobody), "a lens that trusts nobody")
        assertEquals(servedTo(observer, notes[0]), negOpen(trusting), "and back")
        assertEquals(notesBy(minRank = 0), negOpen(anonymous))
    }

    /**
     * Quartz serves a single empty filter from a store's always-current full-set index when it keeps
     * one (`liveNegentropySnapshot`), and that path reads no caller: this store must keep none, so
     * the whole-corpus NEG-OPEN a mirror sends by default is gated like any other.
     */
    @Test
    fun `a whole-corpus NEG-OPEN is not served past the caller's lens`() {
        seed()
        assertNull(runBlocking { store.liveNegentropySnapshot(1_000) })
        val live = LiveEventStore(store, noIngest)
        val whole =
            runBlocking {
                live.sealedNegentropyStorage(Caller(9, setOf(observer)), listOf(Filter()), 1_000)?.let { storage -> (0 until storage.size()).map { storage.getItem(it).id.toHexString() }.toSet() }
            }
        assertEquals(servedTo(observer, Filter()), whole)
        assertTrue(whole!!.containsAll(notesBy(minRank = 2)) && (notesBy(minRank = 0) - notesBy(minRank = 2)).none { it in whole })
    }

    /**
     * A READ THAT CANNOT USE A LENS NEVER WAITS ON ONE. `include:spam` with no text, `sort:` or
     * floor reads nothing a lens could change, and [ReadShape] calls it alike for every caller;
     * so whether the caller signed in must not decide whether it is served either. It did: the
     * lens was resolved for any query carrying an observer, so with the provider map cold and the
     * 10040 read failing, the signed-in NEG-OPEN, REQ, COUNT and live gate of this filter threw
     * while the anonymous ones answered the whole set.
     */
    @Test
    fun `a read that waives the floor needs no lens, so an unreadable provider map fails no caller`() {
        seed()
        val all = Filter(kinds = listOf(1), search = "include:spam")
        val everything = notesBy(minRank = 0)
        assertTrue(ReadShape.of(all).answersEveryConnectionAlike)
        trust.recompute.invalidateProviders()
        providerListsDown = true
        assertEquals(everything, snapshotOf(null, all))
        assertEquals(everything, snapshotOf(observer, all), "the signed-in NEG-OPEN")
        assertEquals(everything, sealedOf(observer, listOf(all)), "the sealed storage it is served from")
        assertEquals(everything, servedTo(observer, all), "the signed-in REQ")
        assertEquals(everything.size, asCaller(observer) { store.count(all) }, "the signed-in COUNT")
        assertNull(asCaller(observer) { store.liveGate(listOf(all)) }, "nothing to hold live events to")
        // Where the lens IS read, it is still required: a gated walk does not answer ungated.
        assertTrue(runCatching { snapshotOf(observer, Filter(kinds = listOf(1))) }.isFailure)
    }

    /**
     * The same rule, priced: on a relay holding no 10040s `ProviderMap` never caches its empty
     * pass, so every lens lookup there is a 10040 query. A read that reads no lens pays none; a
     * gated one pays one per read, the price `ProviderMap` documents for refusing to cache
     * emptiness (an engine not yet serving its lists reads empty too, and no write would ever
     * invalidate that).
     */
    @Test
    fun `a read that reads no lens never looks one up`() {
        runBlocking { store.batchInsert(authors.flatMap { a -> (0 until 3).map { n -> TextNoteEvent(id(), a, 1_700_000_000L + seq, emptyArray(), "note $n", "") } }) }

        fun lookups(block: () -> Unit): Int {
            searched.clear()
            block()
            return searched.count { TrustProviderListEvent.KIND in it.kinds }
        }
        val spam = Filter(kinds = listOf(1), search = "include:spam")
        assertEquals(0, lookups { repeat(5) { snapshotOf(observer, spam) } }, "NEG-OPEN")
        assertEquals(0, lookups { repeat(5) { servedTo(observer, spam) } }, "REQ")
        assertEquals(0, lookups { repeat(5) { asCaller(observer) { store.count(spam) } } }, "COUNT")
        assertEquals(0, lookups { repeat(5) { asCaller(observer) { store.liveGate(listOf(spam)) } } }, "live gate")
        assertEquals(5, lookups { repeat(5) { snapshotOf(observer, Filter(kinds = listOf(1))) } }, "a gated NEG-OPEN still resolves its lens, once per read")
    }

    /**
     * A reconcile is answered strictly. Every NEG-OPEN fails on a degraded cluster, since the walk
     * never accepts a partial answer; this fixture degrades only the provider lists, which isolates
     * the second rule: a NEG-OPEN that reads a lens never accepts a lens resolved from part of
     * them either. Where a REQ under AcceptPartial would rank from what the cluster can still
     * search, it fails (a NEG-ERR) rather than answer a partial or ungated set a peer would act on;
     * a snapshot that reads no lens — the anonymous one here — is untouched by the lists.
     */
    @Test
    fun `a snapshot that reads a lens fails rather than answer ungated when the lens cannot be read`() {
        seed()
        // Cold, as after a 10040 write or a restart: the next lensed read must build the pass.
        trust.recompute.invalidateProviders()
        providerListsDown = true
        val notes = Filter(kinds = listOf(1))
        val down = runCatching { snapshotOf(observer, notes) }
        assertTrue(down.isFailure, "no set at all, not an ungated one: got ${down.getOrNull()?.size} ids, searched=${searched.count { TrustProviderListEvent.KIND in it.kinds }}")
        assertEquals(notesBy(minRank = 0), snapshotOf(null, notes), "an anonymous walk reads no lens")
        providerListsDown = false
        assertEquals(servedTo(observer, notes), snapshotOf(observer, notes))
    }
}
