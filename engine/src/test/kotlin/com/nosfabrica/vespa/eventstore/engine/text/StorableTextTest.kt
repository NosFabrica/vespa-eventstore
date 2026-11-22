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
package com.nosfabrica.vespa.eventstore.engine.text

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Vespa refuses the whole document when any `type string` field carries a code
 * point outside (roughly) XML 1.0's character set. Nostr has no such rule, so these
 * arrive from the network continuously. The code points below are the ones observed
 * in production, not invented.
 */
class StorableTextTest {
    // ---- the rule ----------------------------------------------------------

    @Test
    fun `the control characters seen in the wild are all rejected`() {
        // Observed in mirrored kind-0s, in name / display_name / about.
        listOf(0x1, 0x3, 0xB, 0x10, 0x14, 0x16, 0x1C).forEach { cp ->
            assertEquals(
                cp,
                StorableText.firstUnstorableCodePoint("before${cp.toChar()}after"),
                "0x${cp.toString(16)} must be reported",
            )
        }
    }

    @Test
    fun `tab newline and carriage return are the three controls that survive`() {
        // XML 1.0 keeps exactly these below 0x20, and real bios are full of them.
        listOf('\t', '\n', '\r').forEach {
            assertNull(StorableText.firstUnstorableCodePoint("a${it}b"), "$it must stay legal")
        }
        // Everything else under 0x20 goes, including the ones no one thinks about.
        (0x0..0x1F).filter { it != 0x9 && it != 0xA && it != 0xD }.forEach {
            assertNotNull(StorableText.firstUnstorableCodePoint(it.toChar().toString()), "0x${it.toString(16)}")
        }
    }

    @Test
    fun `DEL and the C1 block are accepted, though XML 1_0 excludes them`() {
        // Checked against a live engine. Reasoning from the XML spec would reject
        // these and quietly drop every profile carrying mojibake'd Latin-1.
        listOf(0x7F, 0x80, 0x9F, 0xA0).forEach {
            assertNull(StorableText.firstUnstorableCodePoint(it.toChar().toString()), "0x${it.toString(16)}")
        }
    }

    @Test
    fun `the noncharacter block is FDD0 to FDDF, not the full Unicode range`() {
        // Boundary verified against a live engine: FDCF and FDE0 both store.
        assertNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0xFDCF))))
        assertNotNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0xFDD0))))
        assertNotNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0xFDDF))))
        assertNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0xFDE0))))
        assertNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0xFDEF))))
    }

    @Test
    fun `the plane-end noncharacters are rejected everywhere`() {
        assertNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0xFFFD))))
        assertNotNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0xFFFE))))
        assertNotNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0xFFFF))))
        assertNotNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0x1FFFE))))
        assertNotNull(StorableText.firstUnstorableCodePoint(String(Character.toChars(0x10FFFF))))
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
        ).forEach { assertNull(StorableText.firstUnstorableCodePoint(it), "must stay storable: $it") }
    }

    @Test
    fun `a surrogate pair is judged as the character it encodes`() {
        // Walking by char instead of code point would see two lone surrogates
        // and reject every emoji on the network.
        val emoji = "🐾"
        assertNull(StorableText.firstUnstorableCodePoint(emoji))
        assertEquals(0x1F43E, emoji.codePointAt(0))
    }

    @Test
    fun `a lone surrogate is unstorable but a paired one is a character`() {
        // Unpaired halves have no UTF-8 encoding: raw, they either corrupt the
        // signed content ('?' substitution) or throw mid-batch.
        assertEquals(0xD800, StorableText.firstUnstorableCodePoint("a\uD800b"))
        assertEquals(0xDFFF, StorableText.firstUnstorableCodePoint("\uDFFF"))
        // A proper pair decodes to one astral character and stays storable.
        assertNull(StorableText.firstUnstorableCodePoint("\uD83D\uDE00"))
    }
}
