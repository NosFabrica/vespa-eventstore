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
package com.nosfabrica.vespa.eventstore.trust

import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.nosfabrica.vespa.eventstore.runtime.BackgroundFailures
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

/**
 * THE PROVIDER PASS'S AGE BOUND under a shared writer topology — the loop that
 * makes a kind-10040 stored by ANOTHER process reach this one's lenses, its
 * Trusted List gate and its card projection within [intervalMillis].
 *
 * [ProviderMap] is invalidated by every 10040 THIS process writes, and by
 * nothing else. vespa-relay runs its serving relay and its sync router as two
 * processes over one Vespa, and most 10040s arrive through the sync; the
 * relay's pass then stayed as old as the last list written through its own
 * socket — nine days on staging, where 24 of 47 recent observers resolved no
 * lens and so got EMPTY ranked pages under the observer gate (#145). The
 * other direction broke too: the sync skipped cards by a service first named
 * on the relay's socket, since its own pass did not name it.
 *
 * WHY NOT STOP CACHING, the way `WriterTopology.SHARED_STRICT` treats the
 * guard owners: every observer read would pay a `complete` pass over every
 * stored 10040, and any moment of partial coverage would fail all of them
 * instead of serving the last good pass. So under both shared topologies the
 * pass is cached with a bounded age; [WriterTopology.SINGLE_WRITER] asserts no
 * other writer exists and keeps the unbounded cache.
 *
 * WHY A TIMER AND NOT A CHEAPER FRESHNESS PROBE: a `(count, max created_at)`
 * fingerprint misses a replacement dated before the newest stored list — count
 * and max both stay put, and mirrored backfill produces exactly that shape —
 * and the exact form (an id-set compare) needs a new engine port member to
 * save one small parse per interval. A shared version stamp is exact but costs
 * a write per 10040 batch and still needs this interval as the backstop for
 * feeders that bypass the store.
 *
 * Cost per interval: one `/search/` of every non-expired 10040 plus a tag
 * parse — the same query a local 10040 write already triggers — and, only
 * when a pass names a service nobody here named, one ledger bracket queueing
 * its walk. A failed tick keeps the previous pass and is counted in
 * [BackgroundFailures] under [BackgroundFailures.PROVIDER_REFRESH].
 */
internal class ProviderRefresher(
    private val trust: TrustProjection,
    private val intervalMillis: Long,
    /** Both writer locks, gate first — see [TrustProjection.refreshProviders]. */
    private val underTrustLock: suspend (suspend () -> Unit) -> Unit,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    init {
        if (intervalMillis > 0L) {
            scope.launch {
                while (isActive) {
                    // Delay FIRST: building a store must not touch the engine,
                    // and the first read builds the first pass on its own.
                    delay(intervalMillis)
                    try {
                        refresh()
                        BackgroundFailures.succeeded(BackgroundFailures.PROVIDER_REFRESH)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (t: Throwable) {
                        // The previous pass stays in place — stale by another
                        // interval, never emptied — and the next tick retries.
                        BackgroundFailures.record(BackgroundFailures.PROVIDER_REFRESH, t)
                    }
                }
            }
        }
    }

    /** One tick, on demand — for an operator, and for a test that must not wait on a clock. */
    suspend fun refresh() = withActivity(Activity.ProviderRefresh) { trust.refreshProviders(underTrustLock) }

    fun close() = scope.cancel()
}
