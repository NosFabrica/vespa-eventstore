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
package com.nosfabrica.vespa.eventstore.vespa.client

import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc

/**
 * One search hit with its ranking explanation ([VespaEventIndex.searchScored]):
 * the engine's relevance score and the match TIER the hit arrived through —
 * `"name"` (exact whole-token), `"near"` (prefix/typo on the *_parts and
 * *_tokens attributes), `"weak"` (bounded infix / hashtag prefix), `"identity"`
 * (nip05/lud16), `"affiliation"` (bio/website), `"gram"` (trigram net only) —
 * or null when the profile served no match-features. The inspector/harness
 * surface: a hit labeled by its band instead of being an opaque position.
 */
data class ScoredHit(
    val doc: EventDoc,
    val relevance: Double,
    val tier: String?,
)
