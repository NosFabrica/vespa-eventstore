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
 * WHAT SEPARATES TWO WORDS, wherever this engine splits text into them: the
 * query's terms ([com.nosfabrica.vespa.eventstore.engine.query.EventYql]), the
 * reference matcher ([com.nosfabrica.vespa.eventstore.engine.doc.SearchFields.matches]),
 * the doc-side tokens ([NearText.tokens]) and the per-term load ledger — one
 * rule, so the spec, the wire and the derived fields cannot split a string
 * three ways.
 *
 * UNICODE whitespace, exactly Kotlin's [Char.isWhitespace] — the rule `trim()`
 * has already applied to the same strings: `\s` alone is Java's ASCII class
 * (`[ \t\n\x0B\f\r]`), which misses U+3000 IDEOGRAPHIC SPACE (what a Japanese
 * or Chinese IME types between words), U+00A0 and U+2000–U+200A. So
 * `ビットコイン　価格` was ONE word: the reference required the literal run and
 * the engine was sent one term holding both. `\p{Z}` is the space separators
 * plus U+2028/U+2029; U+001C–U+001F are the four information separators Java's
 * `isWhitespace` also counts. NearTextTest pins the equivalence over the BMP.
 *
 * Quartz's `SearchQuery` still splits on `\s+` upstream, so such a query
 * arrives here as one quartz term (and an extension after a U+3000 is not read
 * as one); this rule is what makes the store's own split right regardless.
 */
internal val WHITESPACE = Regex("[\\s\\p{Z}\\u001C-\\u001F]+")
