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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress
import com.vitorpamplona.quartz.nip01Core.store.IEventStore

/**
 * WHAT THIS STORE ANSWERS BEYOND QUARTZ'S [IEventStore], in one type both of
 * its handles implement: [NostrSemanticsStore] (the store itself, which a test
 * builds over the in-memory index) and [VespaEventStore] (the front door
 * `open()` returns, which delegates every member to it).
 *
 * WHY AN INTERFACE. A consumer holds an `IEventStore` — that is the type Quartz's
 * relay server, its sync and its tests are written against — and needs these
 * members when, and only when, the store behind it is this one. Before this type
 * each such call site wrote its own switch (`is VespaEventStore ->
 * store.store::liveGate; is NostrSemanticsStore -> store::liveGate`), or reached
 * through `VespaEventStore.store` and so only worked against the front door:
 * vespa-relay had four of them. With it, `(store as? LensedEventStore)` is the
 * whole test, and it holds for either handle.
 *
 * Every member is documented where it is implemented, on [NostrSemanticsStore].
 * A capability a consumer's production path needs from this store and cannot
 * reach from here is a missing member HERE — not a reason to hold the concrete
 * class, and not one for the `engine` hatch, which is read-only, un-lensed and
 * for ops and benchmarks.
 */
interface LensedEventStore : IEventStore {
    /**
     * The observer gate for [filters]' LIVE events, or null when nothing gates
     * them — see [NostrSemanticsStore.liveGate].
     */
    suspend fun liveGate(filters: List<Filter>): LiveGate?

    /**
     * Profile hits for [text] under [observer]'s lens with their own engine
     * scores — see [NostrSemanticsStore.scoredProfileSearch].
     */
    suspend fun scoredProfileSearch(
        text: String,
        observer: String,
        limit: Int,
    ): ScoredSearch

    /**
     * The full-text reindex over [kinds] only, to completion — see
     * [NostrSemanticsStore.reindexFullTextSearch].
     */
    suspend fun reindexFullTextSearch(kinds: List<Int>)

    /**
     * One resumable page of the kind-scoped reindex; the cursor is only valid
     * for the same [kinds]. Quartz's `IEventStore` carries the unscoped walk
     * only, which is why this overload needs this type to be reachable.
     */
    suspend fun reindexFullTextSearch(
        kinds: List<Int>,
        resumeFrom: String?,
        batchSize: Int = IEventStore.DEFAULT_FTS_REINDEX_BATCH,
    ): FtsReindexProgress

    /**
     * Every distinct value of [tagName] at [valueIndex] across [filter]'s
     * matches, aggregated by the engine where the shape allows — see
     * [NostrSemanticsStore.distinctTagValues] for when [unconditional] may be
     * claimed, and why it sits before [where].
     */
    suspend fun distinctTagValues(
        filter: Filter,
        tagName: String,
        valueIndex: Int = 1,
        unconditional: Boolean = false,
        where: (List<String>) -> Boolean = { true },
    ): Set<String>

    /**
     * [pubkey]'s trust chain as this store follows it — see [TrustLens] and
     * [NostrSemanticsStore.lensOf]. Light enough for a per-sign-in check.
     */
    suspend fun lensOf(pubkey: String): TrustLens
}
