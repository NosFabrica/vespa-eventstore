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

import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.runtime.ReadOnlyEventIndex
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * `open(readOnly = true)`: the store a process opens when another process is the writer. Its
 * reads serve, and every write refuses before it reaches the engine, so a reader cannot make the
 * writer's single-writer assertion false. Over the wire ([MockVespaEngine]), because the point is
 * what reaches the engine: [MockVespaEngine.inner] is the whole of what is stored.
 */
class VespaEventStoreReadOnlyTest {
    private val mock = MockVespaEngine()

    @AfterTest
    fun stopMock() = mock.stop()

    private val stored =
        EventDoc(
            id = "1".repeat(64),
            pubkey = "a".repeat(64),
            createdAt = 1_700_000_000,
            kind = 1,
            tags = emptyList(),
            content = "already here",
            sig = "e".repeat(128),
            search = SearchFields(text = "already here"),
        )

    private val note: Event = TextNoteEvent("2".repeat(64), "b".repeat(64), 1_700_000_001, emptyArray(), "a new note", "e".repeat(128))

    private fun reader() = VespaEventStore.open(url = mock.url, autoDeploy = false, readOnly = true)

    private fun storedIds() =
        runBlocking {
            mock.inner
                .search(EventQuery())
                .map { it.id }
                .toSet()
        }

    @Test
    fun `a reader serves reads and refuses every write before the engine sees it`() {
        runBlocking { mock.inner.put(stored) }
        reader().use { store ->
            runBlocking {
                assertEquals(listOf(stored.id), store.query<Event>(Filter(kinds = listOf(1))).map { it.id })
                assertEquals(1, store.count(Filter(kinds = listOf(1))))
                assertEquals(0, store.awaitTrustKeying().keysRemoved, "the keying migration is the writer's, so nothing ran here")
                assertTrue(store.backgroundStatus().startsWith("read-only"), "the status says why no migration runs: ${store.backgroundStatus()}")

                val writes: List<Pair<String, suspend () -> Unit>> =
                    listOf(
                        "insert" to { store.insert(note) },
                        "batchInsert" to { store.batchInsert(listOf(note)) },
                        "transaction" to { store.transaction { insert(note) } },
                        "delete" to { store.delete(Filter(ids = listOf(stored.id))) },
                        "deleteExpiredEvents" to { store.deleteExpiredEvents() },
                        "reindexFullTextSearch" to { store.reindexFullTextSearch() },
                        "reindexFullTextSearch(kinds)" to { store.store.reindexFullTextSearch(listOf(1)) },
                        "backfillBodyEdge" to { store.store.backfillBodyEdge() },
                        "reconcileTrust" to { store.reconcileTrust() },
                        "rebuildTrust" to { store.rebuildTrust() },
                        "awaitTrustProjection" to { store.awaitTrustProjection() },
                        "verifyTrust(repair = true)" to { store.verifyTrust(repair = true) },
                        "sweepOrphanScores" to { store.sweepOrphanScores() },
                    )
                for ((name, write) in writes) {
                    val refused = assertFailsWith<IllegalStateException>("$name must refuse") { write() }
                    assertTrue("read-only" in refused.message.orEmpty(), "$name refused for its own reason: ${refused.message}")
                }
                assertEquals(setOf(stored.id), storedIds(), "nothing reached the engine")
            }
        }
    }

    @Test
    fun `a reader is never the only writer`() {
        assertFailsWith<IllegalArgumentException> {
            VespaEventStore.open(url = mock.url, autoDeploy = false, readOnly = true, writers = WriterTopology.SINGLE_WRITER)
        }
    }

    @Test
    fun `the port backstop forwards reads and refuses writes`() =
        runBlocking {
            // For a write path the store's own checks miss: it must fail at the port, not write.
            val inner = InMemoryEventIndex().apply { put(stored) }
            val port = ReadOnlyEventIndex(inner)
            assertEquals(stored.id, port.get(stored.id)?.id)
            assertEquals(setOf(stored.id), port.existingIds(listOf(stored.id, "f".repeat(64))))
            assertFalse(port.supersedesViaPut)
            val other = stored.copy(id = "3".repeat(64))
            for ((name, write) in listOf<Pair<String, suspend () -> Unit>>(
                "put" to { port.put(other) },
                "putAll" to { port.putAll(listOf(other)) },
                "putIfNewer" to { port.putIfNewer(other) },
                "remove" to { port.remove(stored.id) },
                "removeAll" to { port.removeAll(listOf(stored.id)) },
                "removeDocs" to { port.removeDocs(listOf(stored)) },
            )) {
                assertFailsWith<IllegalStateException>(name) { write() }
            }
            assertEquals(stored.id, inner.get(stored.id)?.id)
            assertEquals(null, inner.get(other.id))
        }
}
