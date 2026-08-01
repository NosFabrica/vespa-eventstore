/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store.ingest

import com.vitorpamplona.quartz.eventstore.store.NostrSemanticsStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The Bloom-backed [GuardOwners] must keep the invariant its whole design rests
 * on: every author with a stored kind-5/62 is flagged (no false negative), so
 * the guard-skip never skips a needed NIP-09/62 probe — at any scale, past the
 * old 10k exact-set cap.
 */
class GuardOwnersTest {
    private val relay = "wss://sot.test/".normalizeRelayUrl()

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun pk(tag: String) = tag.repeat(32).take(64)

    @Test
    fun everyStoredDeleterAndVanisherIsFlagged() =
        runBlocking {
            val index = InMemoryEventIndex()
            val store = NostrSemanticsStore(index, relay = relay)

            // Two deleters (each deletes a real prior own note), one vanisher, one
            // pure-content author who never guards.
            val deleters = (0 until 5).map { pk("a$it") }
            val vanishers = (0 until 3).map { pk("b$it") }
            val plain = (0 until 5).map { pk("c$it") }

            deleters.forEach { author ->
                val noteId = id()
                store.insert(Event(noteId, author, 1_000, 1, emptyArray(), "hi", ""))
                store.insert(DeletionEvent(id(), author, 2_000, arrayOf(arrayOf("e", noteId)), "", ""))
            }
            vanishers.forEach { author ->
                store.insert(Event(id(), author, 1_000, 1, emptyArray(), "hi", ""))
                store.insert(RequestToVanishEvent(id(), author, 2_000, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))
            }
            plain.forEach { author -> store.insert(Event(id(), author, 1_000, 1, emptyArray(), "hi", "")) }

            // A FRESH GuardOwners over the same index — exercises the exhaustive
            // scanAuthors load, not the write-time noteGuardStored path.
            val guards = GuardOwners(index)

            (deleters + vanishers).forEach { author ->
                assertTrue(guards.mightHaveGuards(author), "guard author $author not flagged — false negative")
            }

            // filterFlagged over a mixed set returns ALL guard authors (may over-return
            // a plain author on a Bloom collision, never under-return a guard one).
            val flagged = guards.filterFlagged(deleters + vanishers + plain).toSet()
            assertTrue(flagged.containsAll(deleters + vanishers), "filterFlagged dropped a guard author")

            // Pure-content authors are skippable (no false positive at this tiny fill).
            plain.forEach { author -> assertFalse(guards.mightHaveGuards(author), "plain author $author wrongly flagged") }
        }

    @Test
    fun noteGuardStoredFlagsAfterLoad() =
        runBlocking {
            val index = InMemoryEventIndex()
            val guards = GuardOwners(index)
            val author = pk("aa")
            // Trigger the (empty) load, then record a guard as the write path would.
            assertFalse(guards.mightHaveGuards(author))
            guards.noteGuardStored(author)
            assertTrue(guards.mightHaveGuards(author), "noteGuardStored did not flag the author")
        }
}
