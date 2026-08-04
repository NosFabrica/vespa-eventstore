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
import com.nosfabrica.vespa.eventstore.Rejections
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Vespa rejects the whole document when any `type string` field carries a code
 * point outside XML 1.0's character set. Nostr has no such rule, so these arrive
 * from the network continuously; caught here they are one counted rejection,
 * uncaught they are a feed exception that costs every event batched beside them.
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

    // ---- the rule ----------------------------------------------------------

    @Test
    fun `the control characters seen in the wild are all rejected`() {
        // Observed in mirrored kind-0s, in name / display_name / about.
        listOf(0x1, 0x3, 0xB, 0x10, 0x14, 0x16, 0x1C).forEach { cp ->
            assertEquals(
                cp,
                VespaText.firstIllegalCodePoint("before${cp.toChar()}after"),
                "0x${cp.toString(16)} must be reported",
            )
        }
    }

    @Test
    fun `tab newline and carriage return are the three controls that survive`() {
        // XML 1.0 keeps exactly these below 0x20, and real bios are full of them.
        listOf('\t', '\n', '\r').forEach {
            assertNull(VespaText.firstIllegalCodePoint("a${it}b"), "$it must stay legal")
        }
        // Everything else under 0x20 goes, including the ones no one thinks about.
        (0x0..0x1F).filter { it != 0x9 && it != 0xA && it != 0xD }.forEach {
            assertNotNull(VespaText.firstIllegalCodePoint(it.toChar().toString()), "0x${it.toString(16)}")
        }
    }

    @Test
    fun `DEL and the C1 block are accepted, though XML 1_0 excludes them`() {
        // Checked against a live engine. Reasoning from the XML spec would reject
        // these and quietly drop every profile carrying mojibake'd Latin-1.
        listOf(0x7F, 0x80, 0x9F, 0xA0).forEach {
            assertNull(VespaText.firstIllegalCodePoint(it.toChar().toString()), "0x${it.toString(16)}")
        }
    }

    @Test
    fun `the noncharacter block is FDD0 to FDDF, not the full Unicode range`() {
        // Boundary verified against a live engine: FDCF and FDE0 both store.
        assertNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0xFDCF))))
        assertNotNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0xFDD0))))
        assertNotNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0xFDDF))))
        assertNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0xFDE0))))
        assertNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0xFDEF))))
    }

    @Test
    fun `the plane-end noncharacters are rejected everywhere`() {
        assertNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0xFFFD))))
        assertNotNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0xFFFE))))
        assertNotNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0xFFFF))))
        assertNotNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0x1FFFE))))
        assertNotNull(VespaText.firstIllegalCodePoint(String(Character.toChars(0x10FFFF))))
    }

    @Test
    fun `ordinary text passes, emoji and astral planes included`() {
        // The bios that trip this are otherwise normal, so a false positive here
        // would silently drop a large slice of the network.
        listOf(
            "The Pocket Puppy 🐾 Adult kink creator | Advocate | 18+",
            "45th President of the United States of America🇺🇸",
            "阿拉山的阿拉颂",
            "multi\nline\ttext\r\nwith whitespace",
            "😀🤖 surrogate pairs judged as one character",
            "",
        ).forEach { assertNull(VespaText.firstIllegalCodePoint(it), "must stay storable: $it") }
    }

    @Test
    fun `a surrogate pair is judged as the character it encodes`() {
        // Walking by char instead of code point would see two lone surrogates
        // and reject every emoji on the network.
        val emoji = "🐾"
        assertNull(VespaText.firstIllegalCodePoint(emoji))
        assertEquals(0x1F43E, emoji.codePointAt(0))
    }

    // ---- which fields ------------------------------------------------------

    @Test
    fun `content and tags are both checked`() {
        assertEquals("content", VespaText.firstIllegalField(profile("bad${SYN}here"))?.first)

        val taggy = Event(id(), alice, 1_000_000L, 0, arrayOf(arrayOf("about", "bad${SOH}here")), "{}", "")
        assertEquals("tags", VespaText.firstIllegalField(taggy)?.first)

        assertNull(VespaText.firstIllegalField(profile("""{"name":"clean"}""")))
    }

    // ---- escaped in content, illegal once derived ---------------------------

    @Test
    fun `an escaped control character clears content and would poison the field derived from it`() {
        // The gap that checking only the verbatim fields misses. Inside `content`
        // this is six ordinary characters — \\, u, 0, 0, 1, 6 — and every one of
        // them is storable. U+0016 exists only after the JSON is parsed to build
        // `about`, which is the string that actually reaches the feed.
        val e = MetadataEvent(id(), alice, 1L, emptyArray(), """{"name":"alice","about":"line one\u0016line two"}""", "")

        assertNull(VespaText.firstIllegalField(e), "content itself carries nothing illegal")

        val about = SearchExtractors.extract(e).about!!
        assertNull(VespaText.firstIllegalCodePoint(about), "the derived field must be scrubbed on the way out")
        assertEquals("line oneline two", about)
    }

    @Test
    fun `sanitize drops the unstorable code point and leaves everything else alone`() {
        assertEquals("line oneline two", VespaText.sanitize("line one${SYN}line two"))
        assertEquals("tab\there", VespaText.sanitize("tab\there"))
        assertEquals("emoji \uD83D\uDE00 ok", VespaText.sanitize("emoji \uD83D\uDE00 ok"))
    }

    @Test
    fun `a lone surrogate is illegal but a paired one is a character`() {
        // Unpaired halves have no UTF-8 encoding: past this gate they either
        // corrupt the signed content ('?' substitution) or throw mid-batch.
        assertEquals(0xD800, VespaText.firstIllegalCodePoint("a\uD800b"))
        assertEquals(0xDFFF, VespaText.firstIllegalCodePoint("\uDFFF"))
        assertEquals("ab", VespaText.sanitize("a\uD800b"))
        // A proper pair decodes to one astral character and stays legal.
        val emoji = "\uD83D\uDE00"
        assertNull(VespaText.firstIllegalCodePoint(emoji))
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

    // ---- the store rejects rather than throwing -----------------------------

    private fun store() = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)

    @Test
    fun `bulk insert rejects the bad event and stores the rest of the batch`() =
        runBlocking {
            val store = store()
            // The real one: a Bluesky bridge using 0xB as a line separator.
            val bad = profile("{\"name\":\"Pup\",\"about\":\"The Pocket Puppy $VT Advocate\"}")
            val good = (1..5).map { note("ok$it") }
            val batch = good + bad

            val outcomes = store.batchInsert(batch)

            assertEquals(
                5,
                outcomes.count { it is IEventStore.InsertOutcome.Accepted },
                "one unstorable event must not cost the batch",
            )
            val rejected = outcomes.filterIsInstance<IEventStore.InsertOutcome.Rejected>().single()
            assertEquals(Rejections.UNSTORABLE_TEXT, rejected.reason)
            store.close()
        }

    @Test
    fun `the per-event path rejects it the same way`() =
        runBlocking {
            val store = store()
            val e = profile("bad${SYN}here")
            val thrown = runCatching { store.insert(e) }.exceptionOrNull()
            assertTrue(thrown is RejectedException, "expected a semantic rejection, got $thrown")
            assertEquals(Rejections.UNSTORABLE_TEXT, thrown.message)
            store.close()
        }

    @Test
    fun `a clean event is unaffected by the check`() =
        runBlocking {
            val store = store()
            val e = profile("""{"name":"alice","about":"hi\nthere"}""")
            assertEquals(listOf(IEventStore.InsertOutcome.Accepted), store.batchInsert(listOf(e)))
            store.close()
        }
}
