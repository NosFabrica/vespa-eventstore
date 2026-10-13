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
package com.nosfabrica.vespa.eventstore

/**
 * A profile search's hits with each one's OWN engine score, and the lens that
 * scored them — see [NostrSemanticsStore.scoredProfileSearch]. Scores compare
 * only within one answer: they are one rank profile's relevance for one query.
 */
data class ScoredSearch(
    val hits: List<Hit>,
    val lens: Lens,
) {
    data class Hit(
        val pubkey: String,
        val score: Double,
    )

    /**
     * The observer's resolved `30382:rank` service and the `created_at` of the
     * 10040 that named it; both null when [status] is [LensStatus.NO_LIST].
     */
    data class Lens(
        val status: LensStatus,
        val service: String?,
        val listCreatedAt: Long? = null,
    )

    enum class LensStatus {
        /** The observer's 10040 names a rank service with at least one projected, ranked card. */
        READY,

        /** No stored 10040 resolves a rank service for the observer. */
        NO_LIST,

        /** The 10040 resolves, but no ranked card of the service is projected yet. */
        NO_CARDS,
    }
}
