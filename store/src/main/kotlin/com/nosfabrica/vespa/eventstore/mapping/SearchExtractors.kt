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
 * weights; tier values newline-join; locations space-join) — plus the one
 * rewrite that is neither, [Emoji]: a NIP-30 badge is a picture, not a word,
 * and indexes as its own term rather than as the text it is spelled with.
 *
 * Extractors are derived data: changes upstream or here roll out with
 * `reindexFullTextSearch`, no resync.
 */
object SearchExtractors {
    fun extract(event: Event): SearchFields {
        // The event's own NIP-30 declarations, resolved once and threaded
        // through every string it extracts — see [Emoji].
        val emoji = Emoji(event)
        return when (val fields = SearchFieldExtractor.extract(event)) {
            IndexableFields.None -> {
                SearchFields.NONE
            }

            is IndexableFields.Profile -> {
                // Cleaned FIRST, then read: the badge terms are whatever the
                // rewrite met on the way through, so nothing may be built
                // before every field has passed [Emoji.clean].
                val name = emoji.clean(fields.name)
                val displayName = emoji.clean(fields.displayName)
                val about = emoji.clean(fields.about)
                val nip05 = emoji.clean(fields.nip05)
                val lud16 = emoji.clean(fields.lud16)
                val website = emoji.clean(fields.website)
                SearchFields(
                    name = name,
                    displayName = displayName,
                    about = about,
                    nip05 = nip05,
                    lud16 = lud16,
                    website = website,
                    secondary = emoji.badges(null),
                )
            }

            is IndexableFields.Tiered -> {
                val primary = emoji.joinLines(fields.primary)
                val secondary = emoji.joinLines(fields.secondary + listOfNotNull(emoji.joinSpaced(fields.hashtags)))
                val text = emoji.clean(fields.text)
                val location = emoji.joinSpaced(fields.locations)
                val website = emoji.joinLines(fields.websites)
                SearchFields(
                    primary = primary,
                    secondary = emoji.badges(secondary),
                    text = text,
                    location = location,
                    website = website,
                )
            }
        }
    }

    /**
     * THE `:shortcode:` RUNS AN EVENT DECLARED AS PICTURES (NIP-30), rewritten
     * as ONE term each wherever it indexes them — the feed half of
     * [Shortcodes], which holds the why and the term format.
     *
     * DECLARED ONLY, never guessed by pattern. `:[a-z0-9_]+:` is also what a
     * clock looks like: rewriting it by regex would turn "8:30:45" into one
     * term and a ratio "1:2:1" into another. The event's own `emoji` tags say
     * which runs are pictures, so there is nothing to infer and no false
     * positive to trade against — a shortcode nobody declared stays the words
     * it is. (The query side has no tags to consult and anchors on the whole
     * word instead; same clock, same outcome, different evidence.)
     *
     * WHERE THE TERM LANDS is the other half of the decision: NOT inline, in
     * the field the badge decorated, but in the SECONDARY tier — the weak
     * 4 000 rung — for every kind alike. Inline, `DotardTed :verified:` would
     * index as two name tokens, and every exactness signal the name rung is
     * won on (fieldLength, queryCompleteness, perfect_match) would read the
     * badge as half the name: a search for "dotardted" would score that
     * account BELOW a plain `DotardTed`, punishing it for wearing a badge.
     * Moved to the secondary tier, the name is the name, and the badge is a
     * weak signal of its own, which is what a badge is.
     */
    private class Emoji(
        event: Event,
    ) {
        private companion object {
            val DOUBLE_SPACE = Regex(" {2,}")
        }

        /** `:code:` -> the term it indexes as, for the codes THIS event declares; a code with no alphanumeric strips but indexes nothing. */
        private val declared: List<Pair<String, String?>> = event.tags.emojis().map { Shortcodes.runOf(it.code) to Shortcodes.termOf(it.code) }

        /** The terms actually met while cleaning — declaration is not use, and a badge the text never wears is not indexed. Ordered, deduped. */
        private val seen = LinkedHashSet<String>()

        /** Trim, drop empties, rewrite declared shortcodes, and strip what the engine will not store — the single funnel every derived string passes through. */
        fun clean(s: String?): String? =
            s
                ?.let { rewrite(it) }
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
         * [secondary] with this event's badge terms appended — the ONLY place
         * they enter the document. Call last: [seen] fills as fields are
         * cleaned.
         */
        fun badges(secondary: String?): String? =
            if (seen.isEmpty()) {
                secondary
            } else {
                (listOfNotNull(secondary) + seen.joinToString(" ")).joinToString("\n")
            }

        /**
         * A SPACE where the run was, not nothing: `Em :official_verified:` must
         * not become `Emofficial_verified` when only half a pair is declared,
         * and a name that is nothing BUT shortcodes must collapse to empty
         * rather than to a run of punctuation. The trim in [clean] finishes the
         * job; the term itself is carried out of band, by [seen].
         */
        private fun rewrite(s: String): String {
            if (declared.isEmpty()) return s
            var out = s
            var hit = false
            for ((run, term) in declared) {
                if (out.contains(run)) {
                    out = out.replace(run, " ")
                    hit = true
                    term?.let { seen += it }
                }
            }
            // The doubled SPACES a rewrite leaves, and only those: a bio's own
            // newlines are its paragraphs and stay exactly as written. Nothing
            // downstream tokenizes differently for either, so this is about the
            // value being canonical rather than about matching.
            return if (hit) out.replace(DOUBLE_SPACE, " ") else out
        }
    }
}
