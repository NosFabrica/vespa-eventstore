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
package com.nosfabrica.vespa.eventstore.mapping

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.RejectedException
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.text.StorableText
import com.nosfabrica.vespa.eventstore.ingest.Rejections
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Vespa refuses the whole document when any `type string` field carries a code
 * point outside (roughly) XML 1.0's character set ([StorableText]). Nostr has no
 * such rule, so these arrive from the network continuously. Derived search fields
 * are scrubbed ([VespaText.sanitize]); the event itself is stored and served
 * exactly (the document writer escapes it, `EventDoc.CONTENT_JSON`).
 *
 * The code points below are the ones observed in production, not invented.
 */
class VespaTextTest {
    private val relayUrl = "wss://sot.test/".normalizeRelayUrl()
    private val alice = "a1".repeat(32)
    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun profile(content: String) = Event(id(), alice, 1_000_000L, 0, emptyArray(), content, "")

    /** Kind 1: several from one author coexist, where kind-0s would replace each other. */
    private fun note(content: String) = Event(id(), alice, 1_000_000L, 1, emptyArray(), content, "")

    /**
     * Written as escapes deliberately. A literal control character in source is
     * invisible, and any formatter or copy-paste that drops it turns a test that
     * catches a real defect into one that asserts nothing at all.
     */
    private companion object {
        const val SOH = '\u0001'
        const val VT = '\u000B'
        const val SYN = '\u0016'
    }

    // ---- escaped in content, illegal once derived ---------------------------

    @Test
    fun `an escaped control character clears content and would poison the field derived from it`() {
        // The gap that checking only the verbatim fields misses. Inside `content`
        // this is six ordinary characters — \\, u, 0, 0, 1, 6 — and every one of
        // them is storable. U+0016 exists only after the JSON is parsed to build
        // `about`, which is the string that actually reaches the feed.
        val e = MetadataEvent(id(), alice, 1L, emptyArray(), """{"name":"alice","about":"line one\u0016line two"}""", "")

        assertNull(StorableText.firstUnstorableCodePoint(e.content), "content itself carries nothing unstorable")

        val about = SearchExtractors.extract(e).about!!
        assertNull(StorableText.firstUnstorableCodePoint(about), "the derived field must be scrubbed on the way out")
        assertEquals("line oneline two", about)
    }

    @Test
    fun `sanitize drops the unstorable code point and leaves everything else alone`() {
        assertEquals("line oneline two", VespaText.sanitize("line one${SYN}line two"))
        assertEquals("tab\there", VespaText.sanitize("tab\there"))
        assertEquals("emoji \uD83D\uDE00 ok", VespaText.sanitize("emoji \uD83D\uDE00 ok"))
    }

    @Test
    fun `sanitize drops a lone surrogate but keeps a paired one`() {
        assertEquals("ab", VespaText.sanitize("a\uD800b"))
        val emoji = "\uD83D\uDE00"
        assertSame(emoji, VespaText.sanitize(emoji))
    }

    @Test
    fun `clean text comes back as the very same instance`() {
        // The fast path — this runs on every derived field of every event.
        val s = "nothing to strip"
        assertSame(s, VespaText.sanitize(s))
    }

    @Test
    fun `a profile is scrubbed and stored, not rejected`() =
        runBlocking {
            val store = store()
            val e = MetadataEvent(id(), alice, 1L, emptyArray(), """{"name":"alice","about":"bio\u0016here"}""", "")
            assertEquals(listOf(IEventStore.InsertOutcome.Accepted), store.batchInsert(listOf(e)))
            store.close()
        }

    // ---- the store stores it, exactly --------------------------------------

    private fun store() = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)

    /** relay-tester's two NIP-01 checks: the escapes NIP-01 lists, and U+FDD1 with an astral char. */
    private val nip01Escapes = "linebreak\ndoublequote\"backslash\\carraigereturn\rtab\tbackspace\bformfeed\u000cend"
    private val nonCharacters = "\uFDD1\uDBFF\uDFF2"

    @Test
    fun `bulk insert stores an event the engine cannot hold raw, with the rest of the batch`() =
        runBlocking {
            val store = store()
            // The real one: a Bluesky bridge using 0xB as a line separator.
            val bad = profile("{\"name\":\"Pup\",\"about\":\"The Pocket Puppy $VT Advocate\"}")
            val good = (1..5).map { note("ok$it") }

            val outcomes = store.batchInsert(good + bad)

            assertEquals(6, outcomes.count { it is IEventStore.InsertOutcome.Accepted }, "$outcomes")
            assertEquals(bad.content, store.query<Event>(Filter(ids = listOf(bad.id))).single().content)
            store.close()
        }

    @Test
    fun `the per-event path stores NIP-01's escapes and noncharacters exactly`() =
        runBlocking {
            val store = store()
            for (content in listOf(nip01Escapes, nonCharacters, "bad${SYN}here", "tag${SOH}value")) {
                val e = Event(id(), alice, 1_000_000L, 1, arrayOf(arrayOf("t", content), arrayOf("t", "plain")), content, "")
                store.insert(e)
                val back = store.query<Event>(Filter(ids = listOf(e.id))).single()
                assertEquals(e.toJson(), back.toJson(), "stored and served exactly")
                // The unstorable tag value is not matchable; the event still is, by its other values.
                assertEquals(listOf(e.id), store.query<Event>(Filter(tags = mapOf("t" to listOf("plain")))).filter { it.id == e.id }.map { it.id })
            }
            store.close()
        }

    // ---- what the store still refuses --------------------------------------

    /** A kind-30023 article at [d], one version per [at]. */
    private fun article(
        d: String,
        at: Long,
    ) = Event(id(), alice, at, 30023, arrayOf(arrayOf("d", d)), "version $at", "")

    @Test
    fun `an addressable whose d tag the engine cannot store is refused, not stored twice`() =
        runBlocking {
            val store = store()
            // Its versions are found by #d, which tag_index cannot hold for this value: stored, a
            // newer version would never find the older one and both would be served.
            val bad = "a${SOH}b"
            val thrown = runCatching { store.insert(article(bad, 1_000L)) }.exceptionOrNull()
            assertTrue(thrown is RejectedException, "expected a semantic rejection, got $thrown")
            assertEquals(Rejections.UNSTORABLE_TEXT, thrown.message)

            // The bulk planner (16 or more) refuses it the same way and stores the rest.
            val batch = (1..16).map { note("ok$it") } + article(bad, 2_000L)
            val outcomes = store.batchInsert(batch)
            assertEquals(16, outcomes.count { it is IEventStore.InsertOutcome.Accepted }, "$outcomes")
            assertEquals(IEventStore.InsertOutcome.Rejected(Rejections.UNSTORABLE_TEXT), outcomes.last())
            assertEquals(emptyList(), store.query<Event>(Filter(kinds = listOf(30023))))
            store.close()
        }

    @Test
    fun `an addressable with a storable d still supersedes, and other tags may hold anything`() =
        runBlocking {
            val store = store()
            val old = Event(id(), alice, 1_000L, 30023, arrayOf(arrayOf("d", "slug"), arrayOf("t", "x${SOH}y")), "old", "")
            val new = Event(id(), alice, 2_000L, 30023, arrayOf(arrayOf("d", "slug"), arrayOf("t", "x${SOH}y")), "new", "")
            store.insert(old)
            store.insert(new)
            assertEquals(listOf(new.id), store.query<Event>(Filter(kinds = listOf(30023))).map { it.id })
            store.close()
        }

    /** An index over a schema predating `content_json`: it stores only content a string field takes as is. */
    private class OldSchemaIndex(
        inner: InMemoryEventIndex,
    ) : EventIndex by inner {
        override fun storesContent(content: String): Boolean = StorableText.isStorable(content)
    }

    @Test
    fun `on a schema without content_json, unstorable content is refused and the batch still lands`() =
        runBlocking {
            val store = NostrSemanticsStore(OldSchemaIndex(InMemoryEventIndex()), relay = relayUrl)
            val bad = note("backspace\bhere")
            val good = (1..16).map { note("ok$it") }
            val outcomes = store.batchInsert(good + bad)
            assertEquals(16, outcomes.count { it is IEventStore.InsertOutcome.Accepted }, "$outcomes")
            assertEquals(IEventStore.InsertOutcome.Rejected(Rejections.UNSTORABLE_TEXT), outcomes.last())
            val thrown = runCatching { store.insert(note("form${'\u000c'}feed")) }.exceptionOrNull()
            assertTrue(thrown is RejectedException, "the per-event path refuses it too, got $thrown")
            store.close()
        }

    @Test
    fun `the mixed bulk path asks the real index, not its snapshot, about content`() =
        runBlocking {
            // A kind 5 sends the batch through BulkMixedInsert, which replays admission over an
            // in-memory snapshot that holds anything: it must still refuse what the real index cannot.
            val store = NostrSemanticsStore(OldSchemaIndex(InMemoryEventIndex()), relay = relayUrl)
            // Typed, as ingest hands it over: the mixed path is chosen by `is DeletionRequestEvent`.
            val deletion = EventFactory.create<Event>(id(), alice, 1_000_001L, 5, arrayOf(arrayOf("e", "ff".repeat(32))), "", "")
            val bad = note("backspace\bhere")
            val outcomes = store.batchInsert((1..15).map { note("ok$it") } + deletion + bad)
            assertEquals(IEventStore.InsertOutcome.Rejected(Rejections.UNSTORABLE_TEXT), outcomes.last(), "$outcomes")
            assertEquals(16, outcomes.count { it is IEventStore.InsertOutcome.Accepted }, "$outcomes")
            store.close()
        }

    @Test
    fun `a gift wrap whose recipient the engine cannot store is refused`() =
        runBlocking {
            // A gift wrap's owner is its first p tag, fed raw into the owner attribute.
            val store = store()
            // Typed, as every ingest path hands it over: owner() reads the p tag of a GiftWrapEvent.
            val wrap = EventFactory.create<Event>(id(), alice, 1_000_000L, 1059, arrayOf(arrayOf("p", "ab${SOH}cd")), "sealed", "")
            val thrown = runCatching { store.insert(wrap) }.exceptionOrNull()
            assertTrue(thrown is RejectedException, "expected a semantic rejection, got $thrown")
            assertEquals(Rejections.UNSTORABLE_TEXT, thrown.message)
            store.close()
        }

    @Test
    fun `an unpaired surrogate is refused in content and in tags, a paired one is not`() =
        runBlocking {
            // Storable escaped, but no serializer can hand an unpaired half back as signed bytes.
            val store = store()
            for (e in listOf(note("lone\uD800here"), Event(id(), alice, 1_000_000L, 1, arrayOf(arrayOf("t", "x\uDC00")), "ok", ""))) {
                val thrown = runCatching { store.insert(e) }.exceptionOrNull()
                assertTrue(thrown is RejectedException, "expected a semantic rejection, got $thrown")
            }
            val paired = note("emoji \uD83D\uDE00 fine")
            store.insert(paired)
            assertEquals(paired.content, store.query<Event>(Filter(ids = listOf(paired.id))).single().content)
            store.close()
        }

    @Test
    fun `a clean event is unaffected`() =
        runBlocking {
            val store = store()
            val e = profile("""{"name":"alice","about":"hi\nthere"}""")
            assertEquals(listOf(IEventStore.InsertOutcome.Accepted), store.batchInsert(listOf(e)))
            store.close()
        }
}
