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
            // scanAuthors load, not the write-time note*Stored path.
            val guards = GuardOwners(index)

            deleters.forEach { author ->
                assertTrue(guards.mightBeDeleted(author), "deleter $author not flagged — false negative")
            }
            vanishers.forEach { author ->
                assertTrue(guards.mightHaveVanished(author), "vanisher $author not flagged — false negative")
            }
            // The blooms gate INDEPENDENTLY: a mere deleter must not force the
            // vanish probe, and vice versa (no false positive at this tiny fill).
            deleters.forEach { author -> assertFalse(guards.mightHaveVanished(author), "deleter $author wrongly vanish-flagged") }
            vanishers.forEach { author -> assertFalse(guards.mightBeDeleted(author), "vanisher $author wrongly delete-flagged") }

            // The filters over a mixed set return ALL their guard authors (may
            // over-return on a Bloom collision, never under-return).
            val all = deleters + vanishers + plain
            assertTrue(guards.filterFlaggedDeleters(all).toSet().containsAll(deleters), "filterFlaggedDeleters dropped a deleter")
            assertTrue(guards.filterFlaggedVanishers(all).toSet().containsAll(vanishers), "filterFlaggedVanishers dropped a vanisher")

            // Pure-content authors are skippable on both probes.
            plain.forEach { author ->
                assertFalse(guards.mightBeDeleted(author), "plain author $author wrongly flagged")
                assertFalse(guards.mightHaveVanished(author), "plain author $author wrongly flagged")
            }
        }

    @Test
    fun noteGuardStoredFlagsAfterLoad() =
        runBlocking {
            val index = InMemoryEventIndex()
            val guards = GuardOwners(index)
            val author = pk("aa")
            // Trigger the (empty) load, then record guards as the write path would.
            assertFalse(guards.mightBeDeleted(author))
            guards.noteDeletionStored(author)
            assertTrue(guards.mightBeDeleted(author), "noteDeletionStored did not flag the author")
            assertFalse(guards.mightHaveVanished(author), "a deletion must not vanish-flag the author")
            guards.noteVanishStored(author)
            assertTrue(guards.mightHaveVanished(author), "noteVanishStored did not flag the author")
        }
}
