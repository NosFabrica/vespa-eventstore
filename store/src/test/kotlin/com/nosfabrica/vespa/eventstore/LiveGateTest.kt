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

import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
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
}
