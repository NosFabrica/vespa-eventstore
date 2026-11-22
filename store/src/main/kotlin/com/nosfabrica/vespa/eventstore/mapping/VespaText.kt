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

import com.nosfabrica.vespa.eventstore.engine.text.StorableText

/**
 * The store's scrub for DERIVED search fields, on the engine's storability rule
 * ([StorableText]). Derived fields are a lossy projection already, so [sanitize]
 * drops a code point the engine refuses and the profile stays searchable.
 *
 * The VERBATIM fields are not this file's business any more: `content` and `tags`
 * are covered byte for byte by the signature, so the document writer stores an
 * event carrying such a code point escaped instead of raw (`EventDoc.CONTENT_JSON`)
 * and serves it back exact. Until then the store refused the whole event, and
 * NIP-01's own `\b` and `\f` escapes were enough to be refused.
 *
 * NOT implied by the verbatim fields being clean: `content` holds JSON, so an
 * escape like `\u0016` sits in it as six storable characters and only becomes the
 * refused code point once parsed into the derived field.
 */
internal object VespaText {
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
