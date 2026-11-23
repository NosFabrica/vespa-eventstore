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
package com.nosfabrica.vespa.eventstore.engine

import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The [EventIndex.putIfNewer] contract on the in-memory reference: newest-wins
 * per NIP-01 address, addressable by BOTH the event id and the address, with the
 * lowest-id tie-break. This is the semantics the address-keyed conditional put
 * must reproduce on the real client.
 */
class PutIfNewerTest {
    private val alice = "a1".repeat(32)

    private fun doc(
        id: String,
        kind: Int,
        at: Long,
        d: String? = null,
    ) = EventDoc(
        id = id.repeat(64).take(64),
        pubkey = alice,
        createdAt = at,
        kind = kind,
        tags = if (d != null) listOf(listOf("d", d)) else emptyList(),
        content = "",
        sig = "",
        owner = alice,
    )

    @Test
    fun replaceableNewestWinsAddressableByIdAndAddress() =
        runBlocking {
            val idx = InMemoryEventIndex()
            val v1 = doc("b1", kind = 0, at = 100)
            val v2 = doc("b2", kind = 0, at = 200)

            assertTrue(idx.putIfNewer(v1))
            assertTrue(idx.putIfNewer(v2)) // newer -> supersedes v1

            // Addressable by ID: the winner resolves, the superseded id is gone.
            assertEquals(v2.id, idx.get(v2.id)?.id)
            assertNull(idx.get(v1.id), "superseded version must not resolve by id")
            // Addressable by ADDRESS: exactly one doc for (kind 0, alice).
            assertEquals(listOf(v2.id), idx.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).map { it.id })
        }

    @Test
    fun staleVersionRejected() =
        runBlocking {
            val idx = InMemoryEventIndex()
            val cur = doc("c2", kind = 0, at = 200)
            assertTrue(idx.putIfNewer(cur))
            val stale = doc("c1", kind = 0, at = 100)
            assertFalse(idx.putIfNewer(stale), "older version must be rejected")
            assertNull(idx.get(stale.id))
            assertEquals(cur.id, idx.get(cur.id)?.id)
        }

    @Test
    fun tieBreakLowestIdWins() =
        runBlocking {
            val idx = InMemoryEventIndex()
            val high = doc("ff", kind = 0, at = 500)
            val low = doc("00", kind = 0, at = 500) // same created_at, lower id
            assertTrue(idx.putIfNewer(high))
            assertTrue(idx.putIfNewer(low), "same-time lower id must win")
            assertEquals(low.id, idx.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).single().id)
            // The reverse order must reject the higher id.
            val idx2 = InMemoryEventIndex()
            assertTrue(idx2.putIfNewer(low))
            assertFalse(idx2.putIfNewer(high), "same-time higher id must lose")
        }

    @Test
    fun addressableDTagsAreIndependent() =
        runBlocking {
            val idx = InMemoryEventIndex()
            val draftA1 = doc("a1", kind = 31234, at = 100, d = "draftA")
            val draftA2 = doc("a2", kind = 31234, at = 200, d = "draftA")
            val draftB1 = doc("b1", kind = 31234, at = 100, d = "draftB")
            assertTrue(idx.putIfNewer(draftA1))
            assertTrue(idx.putIfNewer(draftB1))
            assertTrue(idx.putIfNewer(draftA2)) // supersedes only draftA
            val ids = idx.search(EventQuery(kinds = listOf(31234), authors = listOf(alice))).map { it.id }.toSet()
            assertEquals(setOf(draftA2.id, draftB1.id), ids)
        }

    @Test
    fun addressableEmptyDTagSupersedes() =
        runBlocking {
            // An addressable event with a MISSING d tag can't be found by a `d:` tag
            // recall — the broad (kind, author) path + address filter must still catch it.
            val idx = InMemoryEventIndex()
            val v1 = doc("e1", kind = 30000, at = 100)
            val v2 = doc("e2", kind = 30000, at = 200)
            assertTrue(idx.putIfNewer(v1))
            assertTrue(idx.putIfNewer(v2))
            assertEquals(listOf(v2.id), idx.search(EventQuery(kinds = listOf(30000), authors = listOf(alice))).map { it.id })
        }

    @Test
    fun regularEventsNeverSupersede() =
        runBlocking {
            val idx = InMemoryEventIndex()
            val n1 = doc("11", kind = 1, at = 100)
            val n2 = doc("22", kind = 1, at = 200)
            assertTrue(idx.putIfNewer(n1))
            assertTrue(idx.putIfNewer(n2))
            assertEquals(2, idx.search(EventQuery(kinds = listOf(1), authors = listOf(alice))).size, "regular events must both remain")
        }

    /**
     * The winner is durable BEFORE the versions it replaces are removed. The
     * default removed first, so a put that failed (an engine 5xx, a cancelled
     * coroutine) after the remove had landed left the address holding NEITHER
     * version — a profile or follow list lost for good, since nothing re-offers
     * the stale one. Put first, the same failure leaves the old version served.
     */
    @Test
    fun aFailedPutLeavesTheReplacedVersionStored() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val v1 = doc("d1", kind = 0, at = 100)
            assertTrue(inner.putIfNewer(v1))
            val failing =
                object : EventIndex by inner {
                    override suspend fun put(doc: EventDoc) = error("put refused")

                    override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
                }
            assertFailsWith<IllegalStateException> { failing.putIfNewer(doc("d2", kind = 0, at = 200)) }
            assertEquals(listOf(v1.id), inner.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).map { it.id }, "the old version must survive a failed put")
        }

    /** The other half: once the winner is stored, a failed sweep of the old version leaves an extra version, never none, and the next write repairs it. */
    @Test
    fun aFailedRemoveLeavesAStaleVersionTheNextWriteSweeps() =
        runBlocking {
            val inner = InMemoryEventIndex()
            val v1 = doc("e1", kind = 0, at = 100)
            assertTrue(inner.putIfNewer(v1))
            val failing =
                object : EventIndex by inner {
                    override suspend fun removeDocs(docs: List<EventDoc>) = error("remove refused")

                    override suspend fun putIfNewer(doc: EventDoc): Boolean = super<EventIndex>.putIfNewer(doc)
                }
            val v2 = doc("e2", kind = 0, at = 200)
            assertFailsWith<IllegalStateException> { failing.putIfNewer(v2) }
            val after = inner.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).map { it.id }.toSet()
            assertEquals(setOf(v1.id, v2.id), after, "the winner is stored, the stale version is merely not yet swept")
            val v3 = doc("e3", kind = 0, at = 300)
            assertTrue(inner.putIfNewer(v3))
            assertEquals(listOf(v3.id), inner.search(EventQuery(kinds = listOf(0), authors = listOf(alice))).map { it.id }, "drift repair sweeps every older version")
        }
}
