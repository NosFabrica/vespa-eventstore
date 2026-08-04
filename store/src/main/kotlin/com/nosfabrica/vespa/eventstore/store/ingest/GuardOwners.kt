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
package com.nosfabrica.vespa.eventstore.store.ingest

import com.nosfabrica.vespa.eventstore.vespa.client.EventIndex
import com.nosfabrica.vespa.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * The owners with a stored tombstone / stored vanish — the keys that let the
 * admission path SKIP its NIP-09/NIP-62 guard probes for everyone else (nearly
 * everyone). Each probe queries kind-5/62 docs by author, so "no stored guard
 * doc of that kind by this author" proves the probe would come back empty.
 *
 * TWO blooms, not one: vanishers are far rarer than deleters and the probes
 * gate independently; a conflated set forces the vanish probe on every
 * deleter's event.
 *
 * Safety:
 *  - OVER-flagging only costs a probe. UNDER-flagging cannot happen within the
 *    store's write model: the sets are preloaded from the engine, every guard
 *    subsequently stored is added via [noteDeletionStored]/[noteVanishStored],
 *    and a single writer — or one owner-sharded lane
 *    (docs/multi-node-consistency.md) — always sees its owners' guards.
 *  - [GuardBloom]s scale to millions of deleters in a few MB; the Bloom's
 *    no-false-negative property is exactly the under-flag prohibition. The
 *    load must be EXHAUSTIVE, so it uses the continuation-paged
 *    [EventIndex.scanAuthors], not [EventIndex.distinctAuthors] (one-response).
 *  - A FOREIGN feeder writing kind 5/62 directly to the engine bypasses the
 *    note hooks; that deployment must set `GUARD_OWNERS_DISABLE=1` so every
 *    insert probes.
 */
internal class GuardOwners(
    private val index: EventIndex,
) {
    private class Blooms(
        val deleters: GuardBloom,
        val vanishers: GuardBloom,
    )

    @Volatile
    private var blooms: Blooms? = null

    // Config-only (foreign direct-feeder deployments). Accepts "1" as well as
    // "true": the docs prescribe =1, and a kill switch that silently ignores
    // its documented value re-accepts deleted events.
    @Volatile
    private var disabled = System.getenv("GUARD_OWNERS_DISABLE").let { it == "1" || it?.toBooleanStrictOrNull() == true }

    private val loadLock = Mutex()

    /** False only when this owner provably has no stored tombstone — the NIP-09 probe can be skipped. */
    suspend fun mightBeDeleted(owner: String): Boolean {
        if (disabled) return true
        val b = loaded() ?: return true
        return b.deleters.mightContain(owner)
    }

    /** False only when this owner provably has no stored vanish — the NIP-62 probe can be skipped. */
    suspend fun mightHaveVanished(owner: String): Boolean {
        if (disabled) return true
        val b = loaded() ?: return true
        return b.vanishers.mightContain(owner)
    }

    /** The subset of [owners] that can have stored tombstones at all (bulk paths: query only these). */
    suspend fun filterFlaggedDeleters(owners: Collection<String>): Collection<String> {
        if (disabled) return owners
        val b = loaded() ?: return owners
        return owners.filter { b.deleters.mightContain(it) }
    }

    /** The subset of [owners] that can have stored vanishes at all (bulk paths: query only these). */
    suspend fun filterFlaggedVanishers(owners: Collection<String>): Collection<String> {
        if (disabled) return owners
        val b = loaded() ?: return owners
        return owners.filter { b.vanishers.mightContain(it) }
    }

    /** A kind 5 by [author] was just stored — their events must probe NIP-09 from now on. */
    fun noteDeletionStored(author: String) {
        blooms?.deleters?.add(author)
    }

    /** A kind 62 by [author] was just stored — their events must probe NIP-62 from now on. */
    fun noteVanishStored(author: String) {
        blooms?.vanishers?.add(author)
    }

    private suspend fun loaded(): Blooms? {
        blooms?.let { return it }
        if (disabled) return null
        loadLock.withLock {
            blooms?.let { return it }
            // Independent corpus walks run CONCURRENTLY: the first-insert
            // stall is one walk's wall time, not two in series.
            val b =
                coroutineScope {
                    val deleters = async { index.scanAuthors(EventQuery(kinds = listOf(DeletionEvent.KIND))) }
                    val vanishers = async { index.scanAuthors(EventQuery(kinds = listOf(RequestToVanishEvent.KIND))) }
                    Blooms(bloomOf(deleters.await()), bloomOf(vanishers.await()))
                }
            blooms = b
            return b
        }
    }

    /**
     * Sized for the loaded set plus headroom for guards stored this run; overfill
     * only raises the (harmless) false-positive rate, never yields a false negative.
     */
    private fun bloomOf(authors: Set<String>): GuardBloom {
        val bloom = GuardBloom(expectedInsertions = authors.size * 4 + 4096)
        authors.forEach(bloom::add)
        return bloom
    }
}
