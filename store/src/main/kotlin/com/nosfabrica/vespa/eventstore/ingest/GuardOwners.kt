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
package com.nosfabrica.vespa.eventstore.ingest

import com.nosfabrica.vespa.eventstore.BackgroundFailures
import com.nosfabrica.vespa.eventstore.DEFAULT_GUARD_REFRESH_MILLIS
import com.nosfabrica.vespa.eventstore.WriterTopology
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

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
 *  - OVER-flagging only costs a probe. UNDER-flagging re-admits an erased
 *    event, so it is the one thing the design forbids: the sets are preloaded
 *    from the engine and every guard stored afterwards is added via
 *    [noteDeletionStored]/[noteVanishStored].
 *  - [GuardBloom]s scale to millions of deleters in a few MB; the Bloom's
 *    no-false-negative property is exactly the under-flag prohibition. The
 *    load must be EXHAUSTIVE, so it uses the continuation-paged
 *    [EventIndex.scanAuthors], not [EventIndex.countByAuthor] (one-response).
 *
 * WHAT THE NOTE HOOKS CANNOT SEE — and why the mode is an argument, not a
 * guess: they only see writes made THROUGH this store. A second process feeding
 * the same index stores tombstones this cache never hears about, and the miss
 * never heals, since the load runs once. [WriterTopology] is how the deployment
 * says which case it is:
 *
 *  - [WriterTopology.SHARED_STRICT] (DEFAULT) — no cache at all; every insert
 *    probes. Forced by `GUARD_OWNERS_DISABLE=1` regardless of the argument.
 *  - [WriterTopology.SINGLE_WRITER] — load once, never refresh (the note hooks
 *    are then complete by construction, so there is no window).
 *  - [WriterTopology.SHARED] — [refresh] rebuilds both sets every
 *    [refreshMillis], so a foreign guard is honoured after at most one rebuild.
 *    A BOUNDED window, not no window.
 */
internal class GuardOwners(
    private val index: EventIndex,
    topology: WriterTopology = WriterTopology.SHARED_STRICT,
    private val refreshMillis: Long = DEFAULT_GUARD_REFRESH_MILLIS,
) {
    private class Blooms(
        val deleters: GuardBloom,
        val vanishers: GuardBloom,
    )

    /**
     * Guards noted while a rebuild is in flight. The scan cannot be trusted to
     * have seen them (it may already have paged past that author), and the
     * blooms it produces replace the ones the note landed in — so they are
     * buffered here and folded into the replacement before it is published.
     */
    private class Noted {
        val deleters: MutableSet<String> = ConcurrentHashMap.newKeySet()
        val vanishers: MutableSet<String> = ConcurrentHashMap.newKeySet()
    }

    /** The env override outranks the caller: an operator must be able to force the strict floor. */
    private val mode = envOverride() ?: topology

    @Volatile
    private var blooms: Blooms? = null

    /** Non-null exactly while [refresh] is between starting its scan and publishing the result. */
    @Volatile
    private var noting: Noted? = null

    /** Serializes the load and every rebuild — one corpus walk at a time. */
    private val loadLock = Mutex()

    /**
     * Held across a note and across the swap, so a note is either fully before
     * the swap (buffered in [noting], folded in under this lock) or fully after
     * it (landing directly in the published blooms). Never on the read path.
     */
    private val swapLock = Any()

    private val refreshScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val refresherStarted = AtomicBoolean(false)

    /** False only when this owner provably has no stored tombstone — the NIP-09 probe can be skipped. */
    suspend fun mightBeDeleted(owner: String): Boolean {
        if (mode == WriterTopology.SHARED_STRICT) return true
        val b = loaded() ?: return true
        return b.deleters.mightContain(owner)
    }

    /** False only when this owner provably has no stored vanish — the NIP-62 probe can be skipped. */
    suspend fun mightHaveVanished(owner: String): Boolean {
        if (mode == WriterTopology.SHARED_STRICT) return true
        val b = loaded() ?: return true
        return b.vanishers.mightContain(owner)
    }

    /** The subset of [owners] that can have stored tombstones at all (bulk paths: query only these). */
    suspend fun filterFlaggedDeleters(owners: Collection<String>): Collection<String> {
        if (mode == WriterTopology.SHARED_STRICT) return owners
        val b = loaded() ?: return owners
        return owners.filter { b.deleters.mightContain(it) }
    }

    /** The subset of [owners] that can have stored vanishes at all (bulk paths: query only these). */
    suspend fun filterFlaggedVanishers(owners: Collection<String>): Collection<String> {
        if (mode == WriterTopology.SHARED_STRICT) return owners
        val b = loaded() ?: return owners
        return owners.filter { b.vanishers.mightContain(it) }
    }

    /** A kind 5 by [author] was just stored — their events must probe NIP-09 from now on. */
    fun noteDeletionStored(author: String) {
        synchronized(swapLock) {
            noting?.deleters?.add(author)
            blooms?.deleters?.add(author)
        }
    }

    /** A kind 62 by [author] was just stored — their events must probe NIP-62 from now on. */
    fun noteVanishStored(author: String) {
        synchronized(swapLock) {
            noting?.vanishers?.add(author)
            blooms?.vanishers?.add(author)
        }
    }

    /**
     * Rebuild both sets from the corpus and publish them — the only thing that
     * can pick up a guard another writer stored. Also RE-SIZES the filters
     * ([GuardBloom] is fixed-capacity, so a long run that accumulates deleters
     * past its sizing degrades its false-positive rate until something rebuilds
     * it), and drops nothing: the swap is union-only, so a rebuild can never
     * turn a flagged owner unflagged.
     *
     * Called on the [refreshMillis] cadence under [WriterTopology.SHARED], and
     * on demand (`NostrSemanticsStore.refreshGuardOwners`) — after a known
     * foreign write, or from a test that will not wait out an interval.
     * A no-op under [WriterTopology.SHARED_STRICT], which caches nothing.
     */
    suspend fun refresh() {
        if (mode == WriterTopology.SHARED_STRICT) return
        // Nothing loaded yet: the first load IS a from-scratch rebuild, and it
        // starts the refresher. Doing both would walk the corpus twice.
        if (blooms == null) {
            loaded()
            return
        }
        loadLock.withLock {
            val buffered = Noted()
            synchronized(swapLock) { noting = buffered }
            val fresh =
                try {
                    scan()
                } catch (t: Throwable) {
                    // The buffered keys are already in the LIVE blooms (a note
                    // writes both), so abandoning the buffer loses nothing.
                    synchronized(swapLock) { noting = null }
                    throw t
                }
            synchronized(swapLock) {
                buffered.deleters.forEach(fresh.deleters::add)
                buffered.vanishers.forEach(fresh.vanishers::add)
                blooms = fresh
                noting = null
            }
        }
    }

    /** Stops the background refresher. Idempotent; the store owns the call. */
    fun close() {
        refreshScope.cancel()
    }

    private suspend fun loaded(): Blooms? {
        blooms?.let { return it }
        if (mode == WriterTopology.SHARED_STRICT) return null
        loadLock.withLock {
            blooms?.let { return it }
            // A note racing the FIRST load has nowhere to land (`blooms` is
            // still null), so buffer it exactly as a rebuild does.
            val buffered = Noted()
            synchronized(swapLock) { noting = buffered }
            val b =
                try {
                    scan()
                } catch (t: Throwable) {
                    synchronized(swapLock) { noting = null }
                    throw t
                }
            synchronized(swapLock) {
                buffered.deleters.forEach(b.deleters::add)
                buffered.vanishers.forEach(b.vanishers::add)
                blooms = b
                noting = null
            }
            startRefresher()
            return b
        }
    }

    /**
     * The refresher starts with the first load, not at construction: building a
     * store must not touch the engine, and a store that never inserts never
     * needs the sets at all.
     */
    private fun startRefresher() {
        if (mode != WriterTopology.SHARED || refreshMillis <= 0L) return
        if (!refresherStarted.compareAndSet(false, true)) return
        refreshScope.launch {
            while (isActive) {
                delay(refreshMillis)
                try {
                    // Declared HERE and not only on the public
                    // `refreshGuardOwners()`: this loop calls `refresh()`
                    // directly, so without it a full-corpus distinct-author
                    // scan per guard kind books to Activity.Other — the
                    // "nobody declared" bucket — which is exactly the kind of
                    // background cost the attribution exists to name.
                    withActivity(Activity.GuardRefresh) { refresh() }
                    BackgroundFailures.succeeded(BackgroundFailures.GUARD_REFRESH)
                } catch (e: CancellationException) {
                    throw e
                } catch (t: Throwable) {
                    // An engine hiccup leaves the previous sets in place — stale
                    // by another interval, never wrong in the forbidden
                    // direction — and the next tick tries again. Counted so a
                    // refresher that never succeeds is visible instead of
                    // indistinguishable from a quiet one, since what it silently
                    // costs is the staleness bound SHARED promises.
                    BackgroundFailures.record(BackgroundFailures.GUARD_REFRESH, t)
                }
            }
        }
    }

    private suspend fun scan(): Blooms =
        // Independent corpus walks run CONCURRENTLY: the first-insert
        // stall is one walk's wall time, not two in series.
        coroutineScope {
            val deleters = async { index.scanAuthors(EventQuery(kinds = listOf(DeletionEvent.KIND))) }
            val vanishers = async { index.scanAuthors(EventQuery(kinds = listOf(RequestToVanishEvent.KIND))) }
            Blooms(bloomOf(deleters.await()), bloomOf(vanishers.await()))
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

    internal companion object {
        private const val ENV_DISABLE = "GUARD_OWNERS_DISABLE"

        private val TRUTHY = setOf("1", "true", "yes", "on")

        private val FALSY = setOf("", "0", "false", "no", "off")

        /**
         * `GUARD_OWNERS_DISABLE` — the operator's escape hatch, forcing
         * [WriterTopology.SHARED_STRICT]; `null` = no override, the caller's
         * topology stands.
         *
         * An UNPARSEABLE value FAILS THE OPEN rather than reading as "cache on".
         * The switch was silently inert once already — the docs prescribed `=1`
         * while the parse was `toBooleanStrictOrNull`, which returns null for
         * "1" — and a correctness mechanism must not have a quiet failure mode.
         */
        fun envOverride(): WriterTopology? = parseDisable(System.getenv(ENV_DISABLE))

        /** [envOverride]'s parse, split out so a test can reach it without mutating the process environment. */
        fun parseDisable(raw: String?): WriterTopology? {
            if (raw == null) return null
            return when (raw.trim().lowercase()) {
                in TRUTHY -> WriterTopology.SHARED_STRICT

                in FALSY -> null

                else -> throw IllegalArgumentException(
                    "$ENV_DISABLE=\"$raw\" is not a boolean. Use one of ${TRUTHY.sorted()} to force every " +
                        "insert to probe NIP-09/NIP-62, or one of ${FALSY.filter { it.isNotEmpty() }.sorted()} to " +
                        "leave the guard-owner cache to the WriterTopology passed to the store.",
                )
            }
        }
    }
}
