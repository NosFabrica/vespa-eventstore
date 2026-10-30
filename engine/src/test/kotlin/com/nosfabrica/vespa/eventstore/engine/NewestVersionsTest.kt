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
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The [EventIndex.newestVersions] contract on the in-memory reference — the
 * executable spec the engine's grouping must reproduce (`NewestVersionsIT`
 * holds the real Vespa to the same answers): the winner is the version
 * [EventIndex.putIfNewer] would keep, ties to the lowest id.
 */
class NewestVersionsTest {
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)
    private val carol = "c3".repeat(32)

    private fun doc(
        id: Char,
        author: String,
        kind: Int,
        at: Long,
    ) = EventDoc(id = id.toString().repeat(64), pubkey = author, createdAt = at, kind = kind, tags = emptyList(), content = "", sig = "", owner = author)

    @Test
    fun `the newest version wins and a tie goes to the lowest id`() =
        runBlocking {
            val index = InMemoryEventIndex()
            // Stored with put, not putIfNewer: drift (several versions held at
            // once) is exactly the state a probe must still answer right.
            index.putAll(
                listOf(
                    doc('1', alice, 3, 100),
                    doc('9', alice, 3, 200),
                    doc('5', alice, 3, 200),
                    doc('2', bob, 3, 50),
                    doc('3', bob, 0, 900),
                ),
            )
            assertEquals(
                mapOf(alice to DocRef("5".repeat(64), 200), bob to DocRef("2".repeat(64), 50)),
                index.newestVersions(3, listOf(alice, bob, carol)),
                "alice's tie at 200 goes to the lower id; carol has nothing to beat; bob's kind 0 is another address",
            )
        }

    @Test
    fun `no authors asks about nobody`() =
        runBlocking {
            val index = InMemoryEventIndex()
            index.put(doc('1', alice, 0, 100))
            assertTrue(index.newestVersions(0, emptyList()).isEmpty(), "an empty author list is not 'every author'")
        }

    @Test
    fun `an addressable or regular kind is refused, not guessed`() =
        runBlocking {
            val index = InMemoryEventIndex()
            for (kind in listOf(1, 30023, 20001)) {
                assertFailsWith<IllegalArgumentException>("kind $kind") { index.newestVersions(kind, listOf(alice)) }
            }
        }
}
