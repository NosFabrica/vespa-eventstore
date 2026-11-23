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

/**
 * Which text the engine will accept in a `type string` field. Vespa validates
 * strings against (roughly) XML 1.0 and rejects the whole document otherwise;
 * Nostr has no such rule, and events carry these code points routinely
 * (0x1, 0x3, 0xB, 0x10, … observed in profiles — not all junk: a Bluesky bridge
 * uses 0xB as a bio line separator), and NIP-01's own escapes `\b` and `\f`
 * decode to two of them.
 *
 * The ONE place the rule lives, below both its users: the document writer
 * (`EventDoc`, which stores such an event's verbatim fields escaped instead of
 * raw) and the query compiler (`EventYql`, which treats such a tag value as
 * unmatchable), and the store's derived-field scrub above them (`VespaText`).
 */
object StorableText {
    /**
     * The first code point of [s] Vespa will not store, or null. Walks by code
     * point, not char, so a surrogate pair is judged as the one character it
     * encodes rather than two halves that are illegal alone.
     *
     * THE HOTTEST LOOP ON THE WRITE PATH, which is why it has a fast path.
     * Every stored event runs it over its whole `content` and every tag value,
     * and the store again over each derived search field — and a staging slice
     * averages 5 880 content characters per event. MEASURED on 64.5M characters
     * of that corpus (`benchmark/README.md`): the plain codePointAt walk runs at
     * 1.96 ns/char, and reading a CHAR first — the overwhelming majority of
     * them — at 0.78, a 2.5x difference that is most of what an ingest spends
     * deriving a document.
     *
     * The fast band is `[0x20, 0xD800)` and it is storable BY CONSTRUCTION, not
     * by assumption: every rule [isStorable] can reject on lies outside it —
     * C0 below it, surrogates at its top edge, and both the U+FDD0 block and
     * the U+xFFFE/U+xFFFF pattern far above it. Anything else goes to
     * [isStorable], which stays the SINGLE place the rules live. An inlined
     * second copy of them (surrogate pairs decoded by hand) measured no better
     * than this once the JVMs were separated, and a second copy of a table
     * verified against a live engine code point by code point is the last
     * thing this file should carry.
     */
    fun firstUnstorableCodePoint(s: String): Int? {
        var i = 0
        while (i < s.length) {
            if (s[i] >= ' ' && s[i] < '\uD800') {
                i++
                continue
            }
            val cp = s.codePointAt(i)
            if (!isStorable(cp)) return cp
            i += Character.charCount(cp)
        }
        return null
    }

    /** Whether every code point of [s] is one Vespa stores. */
    fun isStorable(s: String): Boolean = firstUnstorableCodePoint(s) == null

    /**
     * Whether [s] holds an unpaired surrogate: a UTF-16 half with no UTF-8 encoding, so no
     * serializer can hand it back as the bytes that were signed (one that writes non-ASCII raw
     * turns it into '?'). The document writer could store it escaped, but it could not be
     * served exactly, so the store refuses such an event rather than serve it altered. Same
     * fast band as [firstUnstorableCodePoint]: below U+D800 a char is never a surrogate.
     */
    fun hasLoneSurrogate(s: String): Boolean {
        var i = 0
        while (i < s.length) {
            val c = s[i]
            if (c < '\uD800' || c > '\uDFFF') {
                i++
                continue
            }
            if (c > '\uDBFF' || i + 1 >= s.length || s[i + 1] !in '\uDC00'..'\uDFFF') return true
            i += 2
        }
        return false
    }

    /**
     * Mirrors Vespa's `com.yahoo.text.Text.isTextCharacter` (not on our
     * classpath), checked against a live engine code point by code point — the
     * XML 1.0 spec gets two cases wrong in the direction that drops storable
     * events: DEL and the C1 block (U+007F–U+009F) are ACCEPTED (mojibake'd
     * Latin-1 lands there constantly), and the refused noncharacter block is
     * U+FDD0–U+FDDF only, not U+FDD0–U+FDEF. If a future Vespa tightens this,
     * the symptom is the feed exception coming back; widen to match then.
     */
    fun isStorable(cp: Int): Boolean =
        when {
            // C0: only tab, newline and carriage return survive.
            cp < 0x20 -> cp == 0x9 || cp == 0xA || cp == 0xD

            // Lone surrogates: codePointAt hands an UNPAIRED half back as-is.
            // It has no UTF-8 encoding, so it can never reach the engine raw —
            // the encoder either mangles it to '?' or throws mid-batch. JSON
            // escapes like \ud800 produce exactly this shape from ordinary parsers.
            cp in 0xD800..0xDFFF -> false

            // Noncharacters. Verified boundary: 0xFDCF and 0xFDE0 both pass.
            cp in 0xFDD0..0xFDDF -> false

            // U+FFFE / U+FFFF in every plane, up to U+10FFFF.
            (cp and 0xFFFE) == 0xFFFE -> false

            else -> true
        }
}
