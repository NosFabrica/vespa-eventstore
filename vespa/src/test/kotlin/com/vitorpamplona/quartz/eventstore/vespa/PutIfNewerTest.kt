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
package com.vitorpamplona.quartz.eventstore.vespa

import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
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
}
