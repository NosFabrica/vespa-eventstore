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
package com.nosfabrica.vespa.eventstore.engine.metrics

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * WHAT THE STORE WAS ASKED TO DO — the "who" dimension of every cost.
 *
 * A port decorator sees `search(query)`; it cannot see that this particular
 * search is the dedup probe inside a `batchInsert`. That has to come from the
 * caller, and the whole point of carrying it in the coroutine context
 * ([ActivityContext]) is that nothing between the entry point and the port has
 * to know it exists.
 *
 * CLOSED SET, deliberately: every memory figure in docs/telemetry.md rests on
 * the key space being finite. Adding a value here is fine; deriving one from a
 * search term, a pubkey or a filter shape is the one thing that turns a fixed
 * budget into a leak.
 */
enum class Activity {
    /** A single-event `insert()`. */
    Insert,

    /** `batchInsert()` / `transaction {}` — the amortized write path. */
    BatchInsert,

    /** A REQ: `query()` in any of its shapes, including NIP-50 search. */
    Query,

    /** A NIP-45 COUNT. */
    Count,

    /** `delete()` — filter-driven removal. */
    Delete,

    /** A negentropy / sync snapshot walk. */
    Snapshot,

    /** The background trust-projection drain (DirtLedger). */
    Drain,

    /** `reconcileTrust` / `rebuildTrust` / `verifyTrust`. */
    Reconcile,

    /** The orphan-score sweep. */
    Sweep,

    /** The guard-owner cache refresh. */
    GuardRefresh,

    /** The one-time `max_rank` walk. */
    Backfill,

    /**
     * Work that reached the port with no activity declared. Not a bug by
     * itself — a bare index in a test has no store above it — but a busy
     * `Other` in production means an entry point forgot [withActivity].
     */
    Other,
    ;

    companion object {
        /** Interned once: [values] allocates a fresh array per call, and this is read on the hot path. */
        val ALL: List<Activity> = entries.toList()
    }
}

/**
 * The ambient carrier for [Activity].
 *
 * Modelled on Quartz's `StoreQueryContext`, which already rides
 * `coroutineContext` through this store to carry the observer into `query()`.
 * Same mechanism, same kind of payload: the caller's intent, set once at the
 * entry point.
 */
class ActivityContext(
    val activity: Activity,
) : AbstractCoroutineContextElement(ActivityContext) {
    companion object Key : CoroutineContext.Key<ActivityContext>
}

/** The activity in flight, or [Activity.Other] when nobody declared one. */
suspend fun currentActivity(): Activity = coroutineContext[ActivityContext]?.activity ?: Activity.Other

/**
 * Run [body] as [activity]. Nests honestly: an inner declaration wins for its
 * own extent, so a drain that runs inside a write lock is booked to `Drain`
 * rather than to whoever happened to hold the gate.
 *
 * THE SHORT-CIRCUIT IS THE POINT. Installing a context element is not cheap —
 * `withContext` runs the full coroutine state-machine transition, measured
 * 2026-09-04 at **956 ns** per call on `Dispatchers.Default` (and the same
 * under `runBlocking`, so it is not a harness artifact). Re-declaring the
 * activity that is ALREADY ambient is a no-op semantically, so it skips
 * straight to the body: **42.7 ns** rather than 933, a 22x saving on every
 * nested declaration.
 *
 * That matters because this is meant to be applied liberally — at every entry
 * point, without a caller having to know whether it is the outermost one.
 * `reindexFullTextSearch()` already calls its own paged overload once per page,
 * and any future entry point that delegates to another gets the same treatment
 * for free.
 *
 * A top-level declaration still pays the full transition, which is ~0.1 % of a
 * read that crosses a network. See docs/telemetry.md §5.4.
 *
 * NOT inline-with-non-local-return: `crossinline` forbids a `return` out of
 * [body], so an entry point whose body returns early extracts it into a private
 * function (`queryUnder`, `countUnder`, and friends).
 */
suspend inline fun <T> withActivity(
    activity: Activity,
    crossinline body: suspend () -> T,
): T =
    if (coroutineContext[ActivityContext]?.activity == activity) {
        body()
    } else {
        kotlinx.coroutines.withContext(ActivityContext(activity)) { body() }
    }

/**
 * A CALL THROUGH THE PORT — the "what" dimension.
 *
 * One entry per shape of engine work, not per method: `putAll` and `put` are
 * both [Put] because the question they answer ("how much writing") is the
 * same, while the round-trip count that distinguishes them is a separate
 * counter on the same slot.
 */
enum class PortCall {
    /** `get` — a single document by id. */
    Get,

    /** `put` / `putAll` / `putIfNewer`'s store. */
    Put,

    /** `remove` / `removeAll` / `removeDocs`. */
    Remove,

    /** `search` / `rawSearch` / `searchRanked` — the recall path. */
    Search,

    /** `count` — the match-set size. */
    Count,

    /** `existingIds` — the dedup probe. */
    Exists,

    /** `visitIds` / `visitTags` / `visitDocsPage` — full-corpus walks. */
    Visit,

    /** `countByAuthor` / `scanAuthors` — server-side grouping. */
    Group,
    ;

    companion object {
        val ALL: List<PortCall> = entries.toList()
    }
}
