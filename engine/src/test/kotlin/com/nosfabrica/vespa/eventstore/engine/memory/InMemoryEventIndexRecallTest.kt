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
package com.nosfabrica.vespa.eventstore.engine.memory

import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/** A recall naming its authors or ids answers exactly what a full scan of the same docs would. */
class InMemoryEventIndexRecallTest {
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)

    private fun doc(
        id: Char,
        author: String,
        kind: Int = 1,
        at: Long = 100,
        tags: List<List<String>> = emptyList(),
    ) = EventDoc(id = id.toString().repeat(64), pubkey = author, createdAt = at, kind = kind, tags = tags, content = "", sig = "", owner = author)

    private suspend fun InMemoryEventIndex.idsOf(query: EventQuery) = search(query).map { it.id.first() }.toSet()

    @Test
    fun `an author recall follows every put and remove`() =
        runBlocking {
            val index = InMemoryEventIndex()
            index.putAll(listOf(doc('1', alice), doc('2', alice), doc('3', bob)))
            index.remove("2".repeat(64))
            assertEquals(setOf('1'), index.idsOf(EventQuery(authors = listOf(alice))))
            assertEquals(setOf('1', '3'), index.idsOf(EventQuery(authors = listOf(alice, bob))))

            index.remove("1".repeat(64))
            assertEquals(emptySet(), index.idsOf(EventQuery(authors = listOf(alice))))

            index.put(doc('1', alice))
            assertEquals(setOf('1'), index.idsOf(EventQuery(authors = listOf(alice.uppercase()))))
            assertEquals(2, index.count(EventQuery(authors = listOf(alice, bob))))
        }

    @Test
    fun `a doc put again under another author moves with it`() =
        runBlocking {
            val index = InMemoryEventIndex()
            index.put(doc('1', alice))
            index.put(doc('1', bob))
            assertEquals(emptySet(), index.idsOf(EventQuery(authors = listOf(alice))))
            assertEquals(setOf('1'), index.idsOf(EventQuery(authors = listOf(bob))))
        }

    @Test
    fun `the narrowed recall still applies every other constraint`() =
        runBlocking {
            val index = InMemoryEventIndex()
            index.putAll(
                listOf(
                    doc('1', alice, kind = 5, at = 200, tags = listOf(listOf("e", "x"))),
                    doc('2', alice, kind = 5, at = 50, tags = listOf(listOf("e", "x"))),
                    doc('3', alice, kind = 1, at = 200, tags = listOf(listOf("e", "x"))),
                    doc('4', alice, kind = 5, at = 200, tags = listOf(listOf("e", "y"))),
                ),
            )
            val deletion = EventQuery(kinds = listOf(5), authors = listOf(alice), tags = mapOf("e" to listOf("x")), since = 100)
            assertEquals(setOf('1'), index.idsOf(deletion))
            assertEquals(index.idsOf(deletion.copy(authors = emptyList())), index.idsOf(deletion))
            assertEquals(setOf('3'), index.idsOf(EventQuery(ids = listOf("3".repeat(64), "9".repeat(64)), authors = listOf(alice))))
        }
}
