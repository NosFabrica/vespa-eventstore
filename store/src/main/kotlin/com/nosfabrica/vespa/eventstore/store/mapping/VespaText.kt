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
 * Which text the engine will accept in a `type string` field.
 *
 * Vespa validates every string field against the XML 1.0 character set and
 * rejects the whole document if any field carries a code point outside it:
 *
 *     Status 400 … Could not parse field 'about' of type string:
 *     The string field value contains illegal code point 0xB
 *
 * Nostr has no such rule, so profiles carry these routinely — observed in the
 * wild: 0x1, 0x3, 0xB, 0x10, 0x14, 0x16, 0x1C, in `name`, `display_name` and
 * `about`. Some are junk, but not all: a Bluesky bridge uses 0xB (vertical tab)
 * as a line separator in ordinary bios.
 *
 * The check exists because the alternative is a thrown [Exception] from the feed
 * client mid-batch, which costs the whole batch and says nothing useful. Catching
 * it here turns an engine-level crash into an ordinary counted rejection.
 *
 * ## Scrub the derived fields, reject on the verbatim ones
 *
 * The derived search fields (`name`, `about`, …) are a lossy projection already,
 * so [sanitize] drops the offending code point and the profile is still stored
 * and still searchable — losing one control character out of a bio is a far
 * better outcome than losing the bio.
 *
 * `content` and `tags` get no such treatment: they are the event *verbatim*, and
 * the signature covers them byte for byte. Altering one to make the engine happy
 * yields a stored event that fails its own signature check, which is worse than
 * not storing it. Storing the verbatim event in a `raw` field would lift the
 * restriction entirely — until then, rejection is the honest outcome there.
 *
 * Both halves are needed. Neither implies the other: see [firstIllegalField] for
 * why clean `content` does not mean clean `about`.
 */
internal object VespaText {
    /**
     * [s] with every unstorable code point dropped.
     *
     * For the derived search fields only. They are a lossy projection already, so
     * losing one control character out of a bio is a far better outcome than
     * rejecting the profile — which is what happens if this text reaches the feed.
     *
     * Returns [s] itself when there is nothing to strip. That fast path is the
     * common case by a wide margin (this runs on every derived field of every
     * event) and keeps the whole thing allocation-free for clean text.
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
     * The first code point [s] carries that Vespa will not store, or null when
     * all of it is storable.
     *
     * Walks by code point, not by char, so a surrogate pair is judged as the one
     * character it encodes rather than as two halves that are illegal alone.
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
     * The first field of [event] the engine would reject, as `field to codePoint`.
     *
     * Only the fields stored verbatim are checked — the ones whose content the
     * event's author controls and whose bytes we cannot alter.
     *
     * This is NOT sufficient on its own, and the reason is worth stating because
     * it looks like it should be. `content` holds JSON for every kind we derive
     * search fields from, so an escape sequence sits in it as six ordinary,
     * perfectly storable characters — `\`, `u`, `0`, `0`, `1`, `6` — and only
     * becomes the illegal code point U+0016 once the JSON is parsed to extract
     * `about`. Verbatim-clean therefore does not imply derived-clean. The derived
     * fields are scrubbed by [sanitize] on the way out instead.
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
     * Mirrors Vespa's `com.yahoo.text.Text.isTextCharacter`. That class is not on
     * our classpath — the feed client ships neither `com.yahoo.text` nor the
     * document model — so the rule is restated here, and it was checked against a
     * live engine code point by code point rather than inferred from the spec.
     *
     * Two of those results are worth writing down, because reasoning from "Vespa
     * validates XML 1.0" gets them wrong in the direction that silently drops
     * storable events:
     *
     *  - DEL and the C1 block (U+007F–U+009F) are ACCEPTED, though XML 1.0
     *    excludes them. Mojibake'd Latin-1 lands here constantly, so rejecting it
     *    would cost real profiles.
     *  - The noncharacter block Vespa refuses is U+FDD0–U+FDDF, not the full
     *    Unicode U+FDD0–U+FDEF. U+FDE0 and up are accepted.
     *
     * If a future Vespa tightens this, the symptom is the feed exception coming
     * back; widen the check to match rather than guessing ahead of it.
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
