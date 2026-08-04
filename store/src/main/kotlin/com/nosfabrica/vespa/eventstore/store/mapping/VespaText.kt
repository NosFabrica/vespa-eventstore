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
package com.nosfabrica.vespa.eventstore.store.mapping

import com.vitorpamplona.quartz.nip01Core.core.Event

/**
 * Which text the engine will accept in a `type string` field. Vespa validates
 * strings against (roughly) XML 1.0 and rejects the whole document otherwise;
 * Nostr has no such rule, and profiles carry illegal code points routinely
 * (0x1, 0x3, 0xB, 0x10, … observed in the wild — not all junk: a Bluesky
 * bridge uses 0xB as a bio line separator). Checking here turns a feed-client
 * exception that costs the whole batch into an ordinary counted rejection.
 *
 * Scrub the DERIVED fields, reject on the VERBATIM ones: derived search fields
 * are a lossy projection already, so [sanitize] drops the code point and the
 * profile stays stored and searchable. `content`/`tags` are covered byte for
 * byte by the signature — altering them yields a stored event that fails its
 * own signature check, worse than not storing it — so those reject.
 *
 * Both halves are needed; neither implies the other (see [firstIllegalField]).
 */
internal object VespaText {
    /**
     * [s] with every unstorable code point dropped — for derived search fields
     * only (they are lossy projections already). Returns [s] itself when there
     * is nothing to strip: the common case by far, kept allocation-free since
     * this runs on every derived field of every event.
     */
    fun sanitize(s: String): String {
        if (firstIllegalCodePoint(s) == null) return s
        val out = StringBuilder(s.length)
        var i = 0
        while (i < s.length) {
            val cp = s.codePointAt(i)
            if (isStorable(cp)) out.appendCodePoint(cp)
            i += Character.charCount(cp)
        }
        return out.toString()
    }

    /**
     * The first code point of [s] Vespa will not store, or null. Walks by code
     * point, not char, so a surrogate pair is judged as the one character it
     * encodes rather than two halves that are illegal alone.
     */
    fun firstIllegalCodePoint(s: String): Int? {
        var i = 0
        while (i < s.length) {
            val cp = s.codePointAt(i)
            if (!isStorable(cp)) return cp
            i += Character.charCount(cp)
        }
        return null
    }

    /**
     * The first field of [event] the engine would reject, as `field to
     * codePoint`. Checks only the verbatim-stored fields. NOT sufficient on its
     * own: `content` holds JSON, so an escape like `\u0016` sits in it as six
     * storable characters and only becomes the illegal code point once parsed —
     * verbatim-clean does not imply derived-clean. Derived fields are scrubbed
     * by [sanitize] instead.
     */
    fun firstIllegalField(event: Event): Pair<String, Int>? {
        firstIllegalCodePoint(event.content)?.let { return "content" to it }
        event.tags.forEach { tag ->
            tag.forEach { value ->
                firstIllegalCodePoint(value)?.let { return "tags" to it }
            }
        }
        return null
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
    private fun isStorable(cp: Int): Boolean =
        when {
            // C0: only tab, newline and carriage return survive.
            cp < 0x20 -> cp == 0x9 || cp == 0xA || cp == 0xD

            // Lone surrogates: codePointAt hands an UNPAIRED half back as-is.
            // It has no UTF-8 encoding, so it can never reach the engine — the
            // encoder either mangles it to '?' (the stored content no longer
            // matches the signature) or throws mid-batch. JSON escapes like
            // \ud800 produce exactly this shape from ordinary parsers.
            cp in 0xD800..0xDFFF -> false

            // Noncharacters. Verified boundary: 0xFDCF and 0xFDE0 both pass.
            cp in 0xFDD0..0xFDDF -> false

            // U+FFFE / U+FFFF in every plane, up to U+10FFFF.
            (cp and 0xFFFE) == 0xFFFE -> false

            else -> true
        }
}
