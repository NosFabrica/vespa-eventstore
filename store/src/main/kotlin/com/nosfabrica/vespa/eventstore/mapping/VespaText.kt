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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.text.StorableText
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable

/**
 * The store's scrub for DERIVED search fields, on the engine's storability rule
 * ([StorableText]). Derived fields are a lossy projection already, so [sanitize]
 * drops a code point the engine refuses and the profile stays searchable.
 *
 * The VERBATIM fields `content` and `tags` are covered byte for byte by the
 * signature, so the document writer stores an event carrying such a code point
 * escaped instead of raw (`EventDoc.CONTENT_JSON`) and serves it back exact.
 * Until then the store refused the whole event, and NIP-01's own `\b` and `\f`
 * escapes were enough to be refused. What it still refuses is [refuses].
 *
 * NOT implied by the verbatim fields being clean: `content` holds JSON, so an
 * escape like `\u0016` sits in it as six storable characters and only becomes the
 * refused code point once parsed into the derived field.
 */
internal object VespaText {
    /**
     * Whether the store must refuse [event] for text it cannot hold, settled with the other
     * no-I/O verdicts rather than surfacing as a feed exception that costs the batch:
     *
     *  - an ADDRESSABLE whose `d` holds a code point the engine refuses. Its versions are found
     *    by `#d` (supersession, NIP-09 `a` deletions, the bulk version reads), and `tag_index`
     *    cannot hold that value, so a newer version would never find and replace the older one;
     *    in address-keyed mode the address is the document id. Read the way the address is
     *    computed (`EventDoc.dTagOrEmpty`): the first `d` tag with a value.
     *  - content holding such a code point while [index] cannot store it
     *    ([EventIndex.storesContent]: a deployed schema predating `content_json`).
     *
     * Both cases cost nothing on the common path: the `d` scan runs for addressables only, and
     * a current schema answers [EventIndex.storesContent] without reading the content.
     */
    fun refuses(
        event: Event,
        index: EventIndex,
    ): Boolean {
        if (event.kind.isAddressable()) {
            val d = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1)
            if (d != null && !StorableText.isStorable(d)) return true
        }
        return !index.storesContent(event.content)
    }

    /**
     * [s] with every unstorable code point dropped — for derived search fields
     * only (they are lossy projections already). Returns [s] itself when there
     * is nothing to strip: the common case by far, kept allocation-free since
     * this runs on every derived field of every event.
     */
    fun sanitize(s: String): String {
        if (StorableText.isStorable(s)) return s
        val out = StringBuilder(s.length)
        var i = 0
        while (i < s.length) {
            val cp = s.codePointAt(i)
            if (StorableText.isStorable(cp)) out.appendCodePoint(cp)
            i += Character.charCount(cp)
        }
        return out.toString()
    }
}
