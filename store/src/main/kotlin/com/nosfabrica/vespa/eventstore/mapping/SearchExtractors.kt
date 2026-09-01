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

import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip30CustomEmoji.emojis
import com.vitorpamplona.quartz.nip50Search.IndexableFields
import com.vitorpamplona.quartz.nip50Search.SearchFieldExtractor

/**
 * The schema-facing face of Quartz's [SearchFieldExtractor]: the per-kind
 * decomposition (which accessor lands in which tier) lives upstream beside the
 * kinds themselves, and this wrapper applies only the decisions that belong to
 * THIS store — Vespa sanitization (see [VespaText]: one illegal code point
 * reaching the feed costs the whole batch) and the join policy for
 * multi-valued roles (hashtags fold into the secondary tier at this schema's
 * weights; tier values newline-join; locations space-join).
 *
 * Extractors are derived data: changes upstream or here roll out with
 * `reindexFullTextSearch`, no resync.
 */
object SearchExtractors {
    fun extract(event: Event): SearchFields {
        // The event's own NIP-30 declarations, resolved once and threaded
        // through every string it extracts — see [stripShortcodes].
        val emoji = Shortcodes(event)
        return when (val fields = SearchFieldExtractor.extract(event)) {
            IndexableFields.None -> {
                SearchFields.NONE
            }

            is IndexableFields.Profile -> {
                SearchFields(
                    name = emoji.clean(fields.name),
                    displayName = emoji.clean(fields.displayName),
                    about = emoji.clean(fields.about),
                    nip05 = emoji.clean(fields.nip05),
                    lud16 = emoji.clean(fields.lud16),
                    website = emoji.clean(fields.website),
                )
            }

            is IndexableFields.Tiered -> {
                SearchFields(
                    primary = emoji.joinLines(fields.primary),
                    secondary = emoji.joinLines(fields.secondary + listOfNotNull(emoji.joinSpaced(fields.hashtags))),
                    text = emoji.clean(fields.text),
                    location = emoji.joinSpaced(fields.locations),
                    website = emoji.joinLines(fields.websites),
                )
            }
        }
    }

    /**
     * THE `:shortcode:` RUNS AN EVENT DECLARED AS PICTURES (NIP-30), removed
     * from everything it indexes.
     *
     * A bridged Mastodon account is called `DotardTed 🇺🇸 :verified:` and carries
     * `["emoji", "verified", "https://…png"]` beside it: the `:verified:` is a
     * badge GLYPH, and the event says so. Indexed as text it is a name token
     * like any other, which put a profile on the 130 000 name rung for the word
     * "verified" — six of the top six hits for a bare `verified` search on
     * staging (2026-09-01) are shortcode names, and one of them opened this
     * whole investigation by outranking a Trusted List titled exactly what was
     * searched for.
     *
     * DECLARED ONLY, never guessed by pattern. `:[a-z0-9_]+:` is also what a
     * clock looks like: stripping it by regex turns "8:30:45" into "845" and a
     * ratio "1:2:1" into "1". The event's own `emoji` tags say which runs are
     * pictures, so there is nothing to infer and no false positive to trade
     * against — a shortcode nobody declared stays the word it is.
     *
     * Derived data, so a change here rolls out with `reindexFullTextSearch`
     * and needs no resync.
     */
    private class Shortcodes(
        event: Event,
    ) {
        private companion object {
            val DOUBLE_SPACE = Regex(" {2,}")
        }

        private val codes: List<String> = event.tags.emojis().map { ":${it.code}:" }

        /** Trim, drop empties, strip declared shortcodes, and strip what the engine will not store — the single funnel every derived string passes through. */
        fun clean(s: String?): String? =
            s
                ?.let { strip(it) }
                ?.let { VespaText.sanitize(it) }
                ?.trim()
                ?.ifEmpty { null }

        fun joinLines(parts: List<String>): String? =
            parts
                .mapNotNull { clean(it) }
                .joinToString("\n")
                .ifEmpty { null }

        fun joinSpaced(parts: List<String>): String? = clean(parts.joinToString(" "))

        /**
         * A SPACE, not nothing: `Em :official_verified:` must not become
         * `Emofficial_verified` when only half a pair is declared, and a name
         * that is nothing BUT shortcodes must collapse to empty rather than to
         * a run of punctuation. The trim in [clean] finishes the job.
         */
        private fun strip(s: String): String {
            if (codes.isEmpty()) return s
            var out = s
            var hit = false
            for (code in codes) {
                if (out.contains(code)) {
                    out = out.replace(code, " ")
                    hit = true
                }
            }
            // The doubled SPACES a removal leaves, and only those: a bio's own
            // newlines are its paragraphs and stay exactly as written. Nothing
            // downstream tokenizes differently for either, so this is about the
            // value being canonical rather than about matching.
            return if (hit) out.replace(DOUBLE_SPACE, " ") else out
        }
    }
}
