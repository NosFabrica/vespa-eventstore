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
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.LiveWords
import com.nosfabrica.vespa.eventstore.trust.TrustCells
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.FanoutExecutor
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.EmptyPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip25Reactions.ReactionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A LIVE EVENT IS DELIVERED EXACTLY WHEN THE STORED PAGE WOULD SERVE IT.
 *
 * The gate reads the same lens and floor each filter's stored read resolves
 * to, off the rank cells the projection maintains — pinned here per lens mode.
 * (That the engine's own gate agrees is FilterMatrixIT's and the relay's
 * contract IT's to show: the in-memory index cannot gate.)
 */
class LiveGateTest {
    private val observer = "0b".repeat(32)
    private val stranger = "0c".repeat(32)
    private val service = "5e".repeat(32)
    private val trusted = "a1".repeat(32)
    private val midway = "a2".repeat(32)
    private val spammer = "a3".repeat(32)
    private val unranked = "a4".repeat(32)

    private val store = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(
        author: String,
        text: String = "live",
    ) = TextNoteEvent(id(), author, 1_700_000_000L + seq, emptyArray(), text, "")

    init {
        runBlocking {
            store.batchInsert(
                listOf(trusted to 90, midway to 50, spammer to 1).map { (a, r) -> UserAssertionEvent(id(), service, 1_000L + r, arrayOf(arrayOf("d", a), arrayOf("rank", r.toString())), "", "") },
            )
            store.insert(TrustProviderListEvent(id(), observer, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
        }
    }

    private fun gate(
        search: String?,
        connection: String? = null,
    ): LiveGate? =
        runBlocking {
            val filters = listOf(Filter(kinds = listOf(1), search = search))
            if (connection == null) store.liveGate(filters) else withContext(StoreQueryContext(setOf(connection))) { store.liveGate(filters) }
        }

    private fun LiveGate.admitted(author: String): Boolean = runBlocking { admits(note(author)) }

    @Test
    fun `no lens and no text, no gate`() {
        assertNull(gate(null), "an anonymous plain feed")
        assertNull(gate("include:spam"), "a waiver")
        assertNull(gate("filter:rank:gte:50"), "a floor with no observer to read it through")
        assertNull(gate("observer:$observer include:spam"), "include:spam's floor of 0 admits every author: no reads to spend")
    }

    /**
     * A SEARCH HOLDS ITS LIVE EVENTS TO ITS TEXT. Quartz's Filter.match ignores
     * `search`, so without this a "pizza" subscription streamed every note.
     * The check is the engine's exact and prefix tiers: every term a prefix of
     * some word, phrases word for word, exclusions absent.
     */
    @Test
    fun `a search admits only what its text matches`() {
        val g = assertNotNull(gate("pizza"), "an anonymous search has no lens, but it has text")
        assertTrue(runBlocking { g.admits(note(spammer, "pizza night")) }, "no lens: any author, if the text matches")
        assertTrue(runBlocking { g.admits(note(spammer, "pizzas all round")) }, "a prefix is a match")
        assertFalse(runBlocking { g.admits(note(spammer, "hello")) }, "no match, no delivery")
        val phrase = assertNotNull(gate("\"pizza party\""))
        assertTrue(runBlocking { phrase.admits(note(trusted, "a pizza party tonight")) })
        assertFalse(runBlocking { phrase.admits(note(trusted, "party pizza")) }, "a phrase is in order")
        val excluding = assertNotNull(gate("pizza -bitcoin"))
        assertFalse(runBlocking { excluding.admits(note(trusted, "pizza for bitcoin")) }, "an excluded word vetoes")
    }

    /** AN EMOJI IS A WORD, live as in the index: required, and excluded. */
    @Test
    fun `live text treats an emoji as the word the engine indexed`() {
        val bolt = assertNotNull(gate("⚡"))
        assertTrue(runBlocking { bolt.admits(note(trusted, "zap⚡ sent")) })
        assertFalse(runBlocking { bolt.admits(note(trusted, "zap sent")) })
        val fire = assertNotNull(gate("pizza -🔥"))
        assertFalse(runBlocking { fire.admits(note(trusted, "pizza 🔥🔥")) })
        assertTrue(runBlocking { fire.admits(note(trusted, "pizza 🍕")) })
        // The same cleaning as the page: -❤️ drops a plain heart, a star is no word.
        val hearts = assertNotNull(gate("pizza -❤️"))
        assertFalse(runBlocking { hearts.admits(note(trusted, "pizza ❤ forever")) })
        val star = assertNotNull(gate("pizz*"))
        assertTrue(runBlocking { star.admits(note(trusted, "pizza night")) })
    }

    /**
     * A PHRASE VESPA WOULD REFUSE IS JUDGED AS THE ENGINE RAN IT (PhraseRuns):
     * the page excluded every note holding five "no"s, so the live stream
     * must too — the text as typed (six) would let those through.
     */
    @Test
    fun `live text follows the phrase rewrite the page was read with`() {
        val g = assertNotNull(gate("pizza -no-no-no-no-no-no"))
        assertFalse(runBlocking { g.admits(note(trusted, "pizza? no no no no no")) }, "the exclusion runs cut to five")
        assertTrue(runBlocking { g.admits(note(trusted, "pizza? no no no no")) })
        val said = (1..11).joinToString(" ") { "the w$it" }
        val split = assertNotNull(gate("\"$said\""))
        assertTrue(runBlocking { split.admits(note(trusted, said)) })
        assertFalse(runBlocking { split.admits(note(trusted, said.replace("w11", "w12"))) }, "every piece is required")
    }

    /**
     * AN UNGATED SEARCH RULE VOUCHES ONLY FOR WHAT ITS TEXT MATCHES. Before, it
     * matched by its NIP-01 part alone and admitted an event a gated sibling
     * filter was there to judge (the audit's case).
     */
    @Test
    fun `an ungated search filter cannot vouch for a sibling's event`() {
        val g =
            assertNotNull(
                runBlocking {
                    withContext(StoreQueryContext(setOf(observer))) {
                        store.liveGate(listOf(Filter(kinds = listOf(1), search = "pizza sort:text"), Filter(kinds = listOf(1))))
                    }
                },
            )
        assertFalse(runBlocking { g.admits(note(spammer, "hello")) }, "only the gated plain filter matches this, and it drops the author")
        assertTrue(runBlocking { g.admits(note(spammer, "pizza time")) }, "the sort:text search matches it, and reads no lens")
        assertTrue(runBlocking { g.admits(note(trusted, "hello")) }, "the plain filter admits a trusted author")
    }

    @Test
    fun `concurrent reads of one cold author are one reputation read`() {
        val counting = CountingReputations(InMemoryReputationIndex())
        val cold = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
        runBlocking {
            cold.batchInsert(listOf(UserAssertionEvent(id(), service, 1_000L, arrayOf(arrayOf("d", trusted), arrayOf("rank", "90")), "", "")))
            cold.insert(TrustProviderListEvent(id(), observer, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
            val g = assertNotNull(cold.liveGate(listOf(Filter(kinds = listOf(1), search = "observer:$observer"))))
            counting.gets.set(0)
            counting.gate = CompletableDeferred()
            val waiting = (1..50).map { async(Dispatchers.Default) { g.admits(note(trusted)) } }
            delay(200)
            counting.gate?.complete(Unit)
            assertTrue(waiting.awaitAll().all { it })
            assertEquals(1, counting.gets.get(), "fifty readers of one cold cell, one read")
        }
    }

    /**
     * The single-flight read's OWNER being cancelled (its subscription closed
     * mid-read) is not the waiters' failure: a waiter that is still active
     * must get the cell, not the owner's CancellationException — which the
     * relay rethrew as its own and silently dropped the event.
     */
    @Test
    fun `a waiter on a cold read outlives the cancelled reader it was waiting on`() {
        val counting = CountingReputations(InMemoryReputationIndex())
        val cold = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), counting), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
        runBlocking {
            cold.batchInsert(listOf(UserAssertionEvent(id(), service, 1_000L, arrayOf(arrayOf("d", trusted), arrayOf("rank", "90")), "", "")))
            cold.insert(TrustProviderListEvent(id(), observer, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
            val g = assertNotNull(cold.liveGate(listOf(Filter(kinds = listOf(1), search = "observer:$observer"))))
            counting.gets.set(0)
            counting.gate = CompletableDeferred()
            val owner = async(Dispatchers.Default) { g.admits(note(trusted)) }
            while (counting.gets.get() == 0) delay(5) // the owner's read is in flight
            val waiter = async(Dispatchers.Default) { g.admits(note(trusted)) }
            delay(100) // the waiter is parked on the owner's read
            owner.cancel()
            counting.gate?.complete(Unit)
            val answer = runCatching { waiter.await() }
            assertEquals(true, answer.getOrNull(), "the waiter's own answer, not ${answer.exceptionOrNull()}")
            assertTrue(owner.isCancelled)
        }
    }

    @Test
    fun `a store with no 10040s gates live events without a provider query each`() {
        // An empty pass is never cached, so the gate must not lean on the cache for it.
        val events = CountingProviderQueries(InMemoryEventIndex())
        val bare = NostrSemanticsStore(TrustProjection(events, InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
        val g = assertNotNull(runBlocking { bare.liveGate(listOf(Filter(kinds = listOf(1), search = "observer:$observer"))) })
        val before = events.providerQueries.get()
        repeat(50) { assertFalse(g.admitsNow(note(trusted)) ?: runBlocking { g.admits(note(trusted)) }, "no lists: trusts nobody") }
        assertTrue(events.providerQueries.get() - before <= 1, "at most one 10040 query for fifty live events, not one each")
    }

    @Test
    fun `after a 10040 write, a burst of deferred live events shares one rebuild`() {
        val events = CountingProviderQueries(InMemoryEventIndex())
        val busy = NostrSemanticsStore(TrustProjection(events, InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
        runBlocking {
            busy.batchInsert(listOf(UserAssertionEvent(id(), service, 1_000L, arrayOf(arrayOf("d", trusted), arrayOf("rank", "90")), "", "")))
            val g = assertNotNull(busy.liveGate(listOf(Filter(kinds = listOf(1), search = "observer:$observer"))))
            busy.insert(TrustProviderListEvent(id(), observer, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
            assertNull(g.admitsNow(note(trusted)), "the write made the pass stale: deferred")
            events.providerQueries.set(0)
            events.gate = CompletableDeferred()
            val waiting = (1..50).map { async(Dispatchers.Default) { g.admits(note(trusted)) } }
            delay(200)
            events.gate?.complete(Unit)
            assertTrue(waiting.awaitAll().all { it }, "the new list's lens admits rank 90")
            // At most one: the write path may already have rebuilt the pass by the time the burst asks.
            assertTrue(events.providerQueries.get() <= 1, "fifty deferred events, ${events.providerQueries.get()} provider queries")
            assertEquals(true, g.admitsNow(note(trusted)), "and then it is a lookup again")
        }
    }

    /** Counts the provider pass's 10040 queries, and can hold them open so callers pile up behind the first. */
    private class CountingProviderQueries(
        private val inner: InMemoryEventIndex,
    ) : EventIndex by inner {
        val providerQueries = AtomicInteger()

        @Volatile var gate: CompletableDeferred<Unit>? = null

        override suspend fun search(query: EventQuery): List<EventDoc> {
            if (query.kinds == listOf(TrustProviderListEvent.KIND)) {
                providerQueries.incrementAndGet()
                gate?.await()
            }
            return inner.search(query)
        }
    }

    /** Counts gets, and can hold them open so readers pile up behind the first. */
    private class CountingReputations(
        private val inner: InMemoryReputationIndex,
    ) : ReputationIndex by inner {
        val gets = AtomicInteger()

        @Volatile var gate: CompletableDeferred<Unit>? = null

        override suspend fun get(pubkey: String): ReputationDoc? {
            gets.incrementAndGet()
            gate?.await()
            return inner.get(pubkey)
        }
    }

    @Test
    fun `an observer gates at the default floor`() {
        val g = assertNotNull(gate("observer:$observer"))
        assertTrue(g.admitted(trusted))
        assertTrue(g.admitted(midway))
        assertFalse(g.admitted(spammer), "rank 1 is below the default floor of 2")
        assertFalse(g.admitted(unranked), "no card is rank 0")
    }

    @Test
    fun `the connection observer gates the same way`() {
        val g = assertNotNull(gate(null, connection = observer))
        assertTrue(g.admitted(trusted))
        assertFalse(g.admitted(spammer))
    }

    @Test
    fun `an explicit floor moves the cut, and survives include-spam`() {
        val g = assertNotNull(gate("observer:$observer filter:rank:gte:60"))
        assertTrue(g.admitted(trusted))
        assertFalse(g.admitted(midway), "50 < 60")
        val waived = assertNotNull(gate("observer:$observer include:spam filter:rank:gte:60"))
        assertFalse(waived.admitted(midway))
        assertNull(gate("observer:$observer include:spam"), "a waiver alone lifts the floor")
    }

    @Test
    fun `sort-text reads no lens, so it gates nothing`() {
        assertNull(gate("observer:$observer sort:text"))
    }

    @Test
    fun `an observer whose 10040 names nobody trusts nobody`() {
        val g = assertNotNull(gate("observer:$stranger"))
        assertFalse(g.admitted(trusted))
    }

    @Test
    fun `a lens published after the subscription opened applies to its next live event`() {
        // A long-lived REQ opened before its observer had a 10040: the gate trusted nobody, and kept on.
        val g = assertNotNull(gate("observer:$stranger"))
        assertFalse(g.admitted(trusted), "no 10040 yet: trusts nobody")
        runBlocking { store.insert(TrustProviderListEvent(id(), stranger, 2_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")) }
        val next = note(trusted)
        assertNull(g.admitsNow(next), "the 10040 dropped the provider pass: deferred, not refused")
        assertTrue(runBlocking { g.admits(next) }, "the lens now names the service that ranks this author 90")
        assertFalse(g.admitted(spammer), "and its floor still applies")
    }

    @Test
    fun `a lens moved to another provider follows the move`() {
        val other = "6f".repeat(32)
        val mover = "0d".repeat(32)
        runBlocking {
            store.insert(UserAssertionEvent(id(), other, 3_000L, arrayOf(arrayOf("d", trusted), arrayOf("rank", "1")), "", ""))
            store.insert(TrustProviderListEvent(id(), mover, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
        }
        val g = assertNotNull(gate("observer:$mover"))
        assertTrue(g.admitted(trusted), "90 under the first provider")
        runBlocking { store.insert(TrustProviderListEvent(id(), mover, 2_000L, arrayOf(arrayOf("30382:rank", other, "wss://other.example.com/")), "", "")) }
        assertFalse(g.admitted(trusted), "1 under the provider the newer 10040 names")
    }

    @Test
    fun `admitsNow answers from what is known, and defers what is not`() {
        val g = assertNotNull(gate("observer:$observer"))
        val fresh = note(trusted)
        assertNull(g.admitsNow(fresh), "a cold author must be read first")
        assertTrue(runBlocking { g.admits(fresh) })
        assertEquals(true, g.admitsNow(note(trusted)), "then it is a lookup")
        runBlocking { g.admits(note(spammer)) }
        assertEquals(false, g.admitsNow(note(spammer)))
    }

    @Test
    fun `an event no filter matches is not admitted`() {
        val g = assertNotNull(gate("observer:$observer"))
        val reaction = ReactionEvent(id(), trusted, 1_700_000_000L, emptyArray(), "+", "")
        assertFalse(runBlocking { g.admits(reaction) })
        assertEquals(false, g.admitsNow(reaction))
    }

    @Test
    fun `any one admitting filter admits`() {
        val g =
            assertNotNull(
                runBlocking {
                    store.liveGate(listOf(Filter(kinds = listOf(1), search = "observer:$observer filter:rank:gte:95"), Filter(kinds = listOf(1), authors = listOf(midway), search = "include:spam")))
                },
            )
        assertTrue(g.admitted(midway), "the waived filter names this author")
        assertFalse(g.admitted(trusted), "90 < 95, and the waived filter does not match")
    }

    /** Text and non-text, gated and ungated, kind-scoped and not: every shape a gate's rule takes. */
    private val subscriptions: List<List<Filter>> =
        listOf(
            listOf(Filter(kinds = listOf(1), search = "pizza")),
            listOf(Filter(kinds = listOf(1), search = "\"pizza party\"")),
            listOf(Filter(kinds = listOf(1), search = "pizza -bitcoin")),
            listOf(Filter(kinds = listOf(1), search = "observer:$observer pizza")),
            listOf(Filter(kinds = listOf(1), search = "observer:$observer")),
            listOf(Filter(kinds = listOf(1), search = "observer:$observer filter:rank:gte:95"), Filter(kinds = listOf(1), authors = listOf(midway), search = "include:spam")),
            listOf(Filter(kinds = listOf(1), search = "pizza sort:text"), Filter(kinds = listOf(1), search = "observer:$observer")),
            listOf(Filter(search = "pizza")),
            listOf(Filter(kinds = listOf(1), search = "⚡ -🔥")),
        )

    private fun liveEvents(): List<Event> =
        listOf(trusted, midway, spammer, unranked, stranger).flatMap { a ->
            listOf("pizza night", "a pizza party tonight", "party pizza", "pizza for bitcoin", "hello", "zap⚡ sent", "zap⚡ 🔥", "pizzas all round").map { note(a, it) } +
                ReactionEvent(id(), a, 1_700_000_000L, emptyArray(), "+", "") +
                MetadataEvent(id(), a, 1_000L + seq, emptyArray(), """{"name":"pizza $a"}""", "")
        }

    /**
     * The gate's words before they were shared: the production read
     * ([LiveWords.read], what a memo miss computes), run again on every call.
     */
    private val unshared: (Event) -> List<String> = LiveWords::read

    /**
     * A NOTE IS EXTRACTED AND TOKENIZED ONCE, HOWEVER MANY SEARCH FEEDS IT
     * REACHES. Each subscription has its own gate, so per-gate words made the
     * text work per event O(open NIP-50 live subscriptions); the store's
     * gates now share one memo. Counted on the production wiring
     * (`store.liveGate`), event by event across every gate, the way fan-out
     * runs: one extraction per event a text rule's NIP-01 part matched.
     */
    @Test
    fun `an event's words are read once however many gates it reaches`() {
        val gates = runBlocking { (1..4).flatMap { subscriptions }.map { assertNotNull(store.liveGate(it)) } }
        val events = liveEvents()
        val before = store.liveWords.computed
        for (e in events) gates.forEach { it.admitsNow(e) }
        // Every event here meets some text rule's NIP-01 part (the kind-less search takes the reactions and profiles).
        assertEquals(events.size.toLong(), store.liveWords.computed - before, "${gates.size} gates, ${events.size} events: once per event, not per gate")
        val plain = runBlocking { assertNotNull(store.liveGate(listOf(Filter(kinds = listOf(1), search = "observer:$observer")))) }
        val untouched = store.liveWords.computed
        liveEvents().forEach { plain.admitsNow(it) }
        assertEquals(untouched, store.liveWords.computed, "a gate with no text never extracts")
    }

    /**
     * THE WORDS ARE READ ONLY BY A TEXT RULE WHOSE NIP-01 PART MATCHED: a
     * reaction or a profile passing `kinds:[1]` search feeds is never
     * extracted, on either path — a gate that read its event's words up front
     * would pay that for every event the fan-out offered it.
     */
    @Test
    fun `an event no text rule's kinds admit is never read`() {
        val gates =
            runBlocking {
                listOf("pizza", "observer:$observer pizza", "\"pizza party\" -bitcoin").map { assertNotNull(store.liveGate(listOf(Filter(kinds = listOf(1), search = it)))) }
            }
        val passers = listOf(ReactionEvent(id(), trusted, 1_700_000_000L, emptyArray(), "pizza", ""), MetadataEvent(id(), trusted, 1_000L, emptyArray(), """{"name":"pizza"}""", ""))
        val before = store.liveWords.computed
        for (e in passers) {
            gates.forEach {
                assertEquals(false, it.admitsNow(e))
                assertFalse(runBlocking { it.admits(e) })
            }
        }
        assertEquals(before, store.liveWords.computed, "no kind-1 rule matched, so nothing was read")
    }

    /**
     * A COLD CHECK READS ITS EVENT'S WORDS ONCE, HOWEVER ITS RULES SUSPEND.
     * [LiveGate.admits] reads a lens and trust between two text rules; a check
     * that resumes on another thread, or after another check took this
     * thread's slot, must not read the words again for its second rule. Two
     * checks interleaved on one thread (each lens read yields to the other):
     * two reads, where a slot-only memo read four.
     */
    @Test
    fun `interleaved cold checks read each event's words once`() {
        val words = LiveWords()
        val yielding =
            object : LiveGate.Lenses {
                override fun now(observer: String) = LiveGate.Resolved.Unknown

                override suspend fun read(observer: String): String? {
                    yield()
                    return null
                }
            }
        val pizza = LiveGate.Text(listOf("pizza"), emptyList(), emptyList())
        val rules =
            listOf(
                LiveGate.Rule(Filter(kinds = listOf(1)), pizza, observer, 50.0),
                LiveGate.Rule(Filter(kinds = listOf(1)), pizza, observer, 60.0),
            )
        val gate = LiveGate(rules, TrustCells(null, { 0L }), yielding, words::of)
        val first = note(trusted, "pizza night")
        val second = note(midway, "pizza party")
        val admitted = runBlocking { listOf(async { gate.admits(first) }, async { gate.admits(second) }).awaitAll() }
        assertEquals(listOf(false, false), admitted, "no service, a trust of 0: both floors refuse")
        assertEquals(2L, words.computed, "one read per event, not one per text rule")
    }

    private object AcceptingIngest : EventIngest {
        override suspend fun submit(
            event: Event,
            skipVerify: Boolean,
            onComplete: (IEventStore.InsertOutcome) -> Unit,
        ) = onComplete(IEventStore.InsertOutcome.Accepted)

        override fun hasBacklog() = false

        override fun close() {}
    }

    private class Connection(
        override val connectionId: Long,
        override val liveExecutor: FanoutExecutor?,
    ) : RequestContext {
        override val policy: IRelayPolicy = EmptyPolicy
        override val authenticatedUsers: Set<HexKey> = emptySet()
    }

    /** One delivery thread, the way a relay's event loop runs its connections' fan-out. */
    private class Loop : FanoutExecutor {
        val thread = Executors.newSingleThreadExecutor()

        override fun execute(task: () -> Unit) = thread.execute { task() }

        fun drain() = thread.submit {}.get(10, TimeUnit.SECONDS)
    }

    /**
     * THROUGH QUARTZ'S REAL FAN-OUT, ONE READ PER DELIVERY THREAD. The memo
     * keys on the event's identity, which holds only if `LiveEventStore`
     * hands every subscription the same instance: here it does, for
     * subscriptions delivered inline (no executor) and on two executors,
     * each holding several searching subscriptions with their own gate.
     */
    @Test
    fun `quartz's fan-out reads a note's words once per delivery thread`() {
        val loops = List(2) { Loop() }
        try {
            val live = LiveEventStore(store, AcceptingIngest, maxHeldBeforeEose = 0)
            val connections = listOf(Connection(1, null)) + loops.mapIndexed { i, loop -> Connection(2L + i, loop) }
            val perConnection = 6
            // Synchronized, not a ConcurrentLinkedQueue: a gated answer may be null (a cold author), which that queue refuses.
            val answers = Collections.synchronizedList(ArrayList<Boolean?>())
            val opened =
                connections.flatMap { connection ->
                    (1..perConnection).map { k ->
                        val filters = listOf(Filter(kinds = listOf(1), search = if (k % 2 == 0) "pizza" else "observer:$observer pizza"))
                        val gate = assertNotNull(runBlocking { store.liveGate(filters) })
                        live.openSubscription(connection, filters) { event, _ -> answers.add(gate.admitsNow(event)) }
                    }
                }
            val before = store.liveWords.computed
            runBlocking { live.submit(note(trusted, "pizza night")) {} }
            loops.forEach { it.drain() }
            assertEquals(connections.size * perConnection, answers.size, "every subscription was offered the note")
            assertTrue(answers.none { it == false }, "no gate refused a matching note")
            assertEquals(connections.size.toLong(), store.liveWords.computed - before, "one read per delivery thread, not per subscription")
            opened.forEach { it.cancel() }
        } finally {
            loops.forEach { it.thread.shutdownNow() }
        }
    }

    /** A store seeded like [store], on a clock that never moves, so no cached trust expires while a test compares answers. */
    private fun frozenStore(): NostrSemanticsStore {
        val frozen = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), InMemoryReputationIndex()), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"), nowSecs = { 1_700_500_000L })
        runBlocking {
            frozen.batchInsert(
                listOf(trusted to 90, midway to 50, spammer to 1).map { (a, r) -> UserAssertionEvent(id(), service, 1_000L + r, arrayOf(arrayOf("d", a), arrayOf("rank", r.toString())), "", "") },
            )
            frozen.insert(TrustProviderListEvent(id(), observer, 1_000L, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", ""))
        }
        return frozen
    }

    /**
     * SHARING THE WORDS CHANGES NO ADMISSION, on one thread or several. Every
     * gate answers every event exactly as the same filters reading
     * [LiveWords.read] afresh on every call — admitsNow cold, admits,
     * admitsNow warm — walked EVENT-MAJOR (each event through every gate, as
     * fan-out runs), so the shared gates answer from memo hits. Then four
     * threads fan the events out through the shared memo at once, two in the
     * same order (so they meet on an event) and two offset by one, and every
     * answer of every pass is held to the warm oracle. The clock is frozen,
     * so a cached trust cannot expire between the oracle and the threads.
     */
    @Test
    fun `shared words admit exactly what per-gate words admitted, across threads`() {
        val frozen = frozenStore()
        val shared = runBlocking { subscriptions.map { assertNotNull(frozen.liveGate(it)) } }
        val oracle = runBlocking { subscriptions.map { assertNotNull(frozen.liveGateWith(it, unshared)) } }
        val events = liveEvents()

        fun now(gates: List<LiveGate>) = events.map { e -> gates.map { it.admitsNow(e) } }

        fun full(gates: List<LiveGate>) = runBlocking { events.map { e -> gates.map { it.admits(e) } } }
        assertEquals(now(oracle), now(shared), "admitsNow, cold")
        val admitted = full(oracle)
        assertEquals(admitted, full(shared), "admits")
        val warm = now(oracle)
        assertEquals(warm, now(shared), "admitsNow, warm")
        assertTrue(admitted.flatten().any { it } && admitted.flatten().any { !it }, "the battery admits some and refuses some")
        assertTrue(warm.flatten().any { it == null }.not(), "warm: every author's trust is in hand")

        val threads = 4
        val pool = Executors.newFixedThreadPool(threads)
        try {
            val start = CyclicBarrier(threads)
            val runs =
                (0 until threads).map { t ->
                    pool.submit(
                        Callable {
                            start.await()
                            val order = events.indices.map { (it + t / 2) % events.size }
                            val wrong = ArrayList<String>()
                            repeat(200) { pass ->
                                for (i in order) {
                                    shared.forEachIndexed { g, gate ->
                                        val got = gate.admitsNow(events[i])
                                        if (got != warm[i][g]) wrong += "pass $pass, event $i, gate $g: $got"
                                    }
                                }
                            }
                            wrong
                        },
                    )
                }
            for (run in runs) assertEquals(emptyList(), run.get(120, TimeUnit.SECONDS))
        } finally {
            pool.shutdownNow()
        }
    }
}
