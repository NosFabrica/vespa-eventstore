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
    fun extract(event: Event): SearchFields =
        when (val fields = SearchFieldExtractor.extract(event)) {
            IndexableFields.None -> {
                SearchFields.NONE
            }

            is IndexableFields.Profile -> {
                SearchFields(
                    name = clean(fields.name),
                    displayName = clean(fields.displayName),
                    about = clean(fields.about),
                    nip05 = clean(fields.nip05),
                    lud16 = clean(fields.lud16),
                    website = clean(fields.website),
                )
            }

            is IndexableFields.Tiered -> {
                SearchFields(
                    primary = joinLines(fields.primary),
                    secondary = joinLines(fields.secondary + listOfNotNull(joinSpaced(fields.hashtags))),
                    text = clean(fields.text),
                    location = joinSpaced(fields.locations),
                    website = joinLines(fields.websites),
                )
            }
        }

    /** Trim, drop empties, and strip what the engine will not store — the single funnel every derived string passes through. */
    private fun clean(s: String?): String? = s?.let { VespaText.sanitize(it) }?.trim()?.ifEmpty { null }

    private fun joinLines(parts: List<String>): String? =
        parts
            .mapNotNull { clean(it) }
            .joinToString("\n")
            .ifEmpty { null }

    private fun joinSpaced(parts: List<String>): String? = clean(parts.joinToString(" "))
}
