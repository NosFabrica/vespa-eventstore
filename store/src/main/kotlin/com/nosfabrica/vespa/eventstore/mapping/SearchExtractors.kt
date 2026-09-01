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
    fun extract(event: Event): SearchFields =
        when (val fields = SearchFieldExtractor.extract(event)) {
            IndexableFields.None -> {
                // Not searchable: the NIP-30 declarations are never read, so
                // they are never looked up. Reactions and receipts are most of
                // what a relay ingests and none of what it searches.
                SearchFields.NONE
            }

            is IndexableFields.Profile -> {
                // The event's own NIP-30 declarations, resolved once and
                // threaded through every string it extracts — see [Emoji].
                val emoji = Emoji(event)
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
                val emoji = Emoji(event)
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
        /**
         * `:code:` -> the term it indexes as, for the codes THIS event declares;
         * a code with no alphanumeric strips but indexes nothing.
         *
         * Upstream's `emojis()`, deliberately, after measuring the obvious
         * alternative: a real corpus almost never declares an emoji (SEVEN of
         * 10 961 events in a staging slice, 0.06%, against 16.5 tags per event),
         * so an allocation-free `any {}` scan to decide whether to parse at all
         * looks like the cheap move — and is not one. Both forms measure
         * 130-165 ns/event (`extractBench --stage emojis|declared`, medians of
         * 15 rounds, inside each other's spread): the list `mapNotNull` builds
         * never escapes, so the JIT does not allocate it, and both shapes are
         * the same walk over the same tags. The idiom upstream owns wins the
         * tie.
         */
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
         *
         * ONE PASS, and only over a field that can contain a run. The obvious
         * shape — `replace(run, " ")` per declared code, then a `Regex` sweep to
         * collapse the doubled spaces it left — walks the field twice per code
         * and then hands the whole thing to a regex engine, which on a corpus
         * where every event wears three badges measured 77 us/event against a
         * 17 us baseline (benchmark/README.md: `extractBench --badges 3`).
         * Scanning colon to colon instead, appending as it goes and swallowing
         * the space where the previous character is already one, does the
         * removal and the collapse together: 20 us on the same corpus, and an
         * untouched field is returned as ITSELF, uncopied.
         *
         * The one behavioural difference is a narrowing: this collapses only the
         * spaces around a run it removed, where the regex also collapsed
         * unrelated double spaces elsewhere in a field that happened to carry a
         * badge. Nothing downstream tokenizes on either.
         */
        private fun rewrite(s: String): String {
            if (declared.isEmpty()) return s
            // No colon, no shortcode: one vectorized indexOf over the field
            // instead of a substring search per declared code. Most of what an
            // event declaring a badge indexes does not carry it.
            var at = s.indexOf(':')
            if (at < 0) return s

            var out: StringBuilder? = null
            var copied = 0
            while (at >= 0) {
                val run = runAt(s, at)
                if (run != null) {
                    val sb = out ?: StringBuilder(s.length).also { out = it }
                    sb.append(s, copied, at)
                    // The space the picture leaves, unless what it followed
                    // already ends in whitespace — and then the spaces on the
                    // other side of it, so `My :verified: Post` is two tokens
                    // and not three. This is the collapse, done in place.
                    // Whitespace, not ' ', on the left and ONLY ' ' on the
                    // right: a bio's newlines are its paragraphs, and a badge
                    // alone on a line must not weld them together.
                    if (sb.isNotEmpty() && !sb[sb.length - 1].isWhitespace()) sb.append(' ')
                    copied = at + run.length
                    while (copied < s.length && s[copied] == ' ') copied++
                    at = s.indexOf(':', copied)
                } else {
                    at = s.indexOf(':', at + 1)
                }
            }
            val sb = out ?: return s
            sb.append(s, copied, s.length)
            return sb.toString()
        }

        /**
         * The declared run starting at [at], or null. Indexed loop and
         * `startsWith`: the list is a handful of codes at most, and neither a
         * substring nor an iterator is allocated to compare them.
         */
        private fun runAt(
            s: String,
            at: Int,
        ): String? {
            for (i in declared.indices) {
                val (run, term) = declared[i]
                if (s.startsWith(run, at)) {
                    term?.let { seen += it }
                    return run
                }
            }
            return null
        }
    }
}
