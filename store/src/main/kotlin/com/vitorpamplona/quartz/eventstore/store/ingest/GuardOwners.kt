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
package com.vitorpamplona.quartz.eventstore.store.ingest

import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * The owners with a stored tombstone, and the owners with a stored vanish — the
 * keys that let the admission path SKIP its NIP-09 and NIP-62 guard probes for
 * everyone else, which is nearly everyone (content authors seldom publish
 * deletions). Each guard probe queries kind-5/62 docs whose AUTHOR is the
 * inserted event's owner, so "no stored guard doc of that kind by this author"
 * proves that probe would come back empty.
 *
 * TWO blooms, not one: vanishers are orders of magnitude rarer than deleters,
 * and the two probes gate independently. A conflated set forces the vanish
 * probe on every deleter's event — on a deletion-heavy sync that is one wasted
 * query (or an O(snapshot) scan in the bulk replay) per event, proving over and
 * over that owners who never vanished didn't vanish.
 *
 * Why this is safe:
 *  - OVER-flagging (an owner in a set with no live guard) only costs a probe.
 *  - UNDER-flagging cannot happen within the store's write model: the sets are
 *    preloaded from the engine (two corpus walks, run concurrently) and every
 *    guard this store subsequently stores is added via [noteDeletionStored] /
 *    [noteVanishStored]. A single writer — or one owner-sharded lane, since
 *    lanes never insert another lane's owners (docs/multi-node-consistency.md)
 *    — therefore always sees its owners' guards.
 *  - Scale: the flagged owners live in [GuardBloom]s, not exact sets, so a
 *    relay with millions of distinct deleters costs a few MB and the guard-skip
 *    KEEPS WORKING. The Bloom's no-false-negative property is exactly the
 *    UNDER-flag prohibition above; a false positive is just the harmless
 *    over-flag (a wasted probe). The load must be EXHAUSTIVE and it walks the
 *    WHOLE corpus, so it uses [EventIndex.scanAuthors] — the continuation-paged
 *    visit — rather than [EventIndex.distinctAuthors], whose grouping is equally
 *    complete but builds its entire answer in one response.
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

    // Config-only now (a foreign direct-feeder deployment). No longer tripped by
    // deleter cardinality — the Bloom scales where the old exact set gave up.
    // Accepts "1" as well as "true": the docs above prescribe =1, and a kill
    // switch that silently ignores its documented value re-accepts deleted events.
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
            // The two corpus walks are independent, so they run CONCURRENTLY:
            // the first-insert stall is one walk's wall time, not two in series.
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
