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

import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.MeteredEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.nosfabrica.vespa.eventstore.trust.MaxRankBackfill
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.nosfabrica.vespa.eventstore.trust.TrustReconciler
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.net.URI

/**
 * The library's public handle and front door: [open] wires the whole
 * NostrSemanticsStore(TrustProjection(VespaEventIndex, VespaReputationIndex))
 * stack over a running Vespa (deploying the schema on first run by default) and
 * delegates the full [IEventStore] surface to it. Vespa itself is a
 * prerequisite, like a database. Closeable, so `open(...).use { ... }` works.
 */
class VespaEventStore internal constructor(
    /**
     * The concrete store, exposed for Vespa-specific capabilities beyond the
     * Quartz interface (e.g. `distinctTagValues`).
     */
    val store: NostrSemanticsStore,
    /**
     * The raw engine index, NOT trust-projected — for status/health metrics
     * that only count and never mutate trust data.
     */
    val eventIndex: VespaEventIndex,
    /** Repair tool for the trust view over [eventIndex]; see [reconcileTrust]. */
    private val reconciler: TrustReconciler,
    /** The projection, for the deferred-mode drain barrier ([awaitTrustProjection]). */
    private val trust: TrustProjection,
    /** The background drain worker's scope in deferred mode; null when the projection settles inline. */
    private val drainScope: CoroutineScope? = null,
    /** The `max_rank` backfill's job, so a test or a boot line can wait for the descent to be on — see [awaitTrustDescent]. */
    private val backfill: kotlinx.coroutines.Deferred<Int>? = null,
    /** Whether the descent is switched on for this store — [open]'s `trustDescent`; the walk runs either way. */
    val trustDescent: Boolean = true,
) : IEventStore by store {
    /** The engine's feed-health status line (bulk-ingest backpressure), for progress/status output. */
    fun feedStatus(): String = eventIndex.feedStatus()

    /**
     * WHERE THIS STORE'S RESOURCES GO — the structured, cumulative record
     * behind an operator dashboard. See docs/telemetry.md.
     *
     * Counters and gauges, never ratios: a window is the difference between two
     * snapshots, so a caller may sample this as often as it likes and nothing
     * is consumed by being read. Pair it with [IngestStats.snapshot] for the
     * write-path stage split and [IngestStats.blockedSplit] for what ingest was
     * waiting behind.
     */
    fun metrics(): CostLedger.Snapshot = store.metrics.snapshot()

    /**
     * Wait until the trust descent may serve pages: the one-time walk that
     * writes `max_rank` onto every reputation document fed before the field
     * existed (MaxRankBackfill). Returns how many documents it wrote — 0 on a
     * store that already carried it. A walk the engine refuses is retried
     * until it finishes ([MaxRankBackfill.runUntilDone]), so this returns only
     * with the walk done, or throws [CancellationException] when the store
     * closed first; nothing served in the meantime is any different from
     * before, since [VespaEventIndex.trustDescent] is only set on success —
     * and only when [trustDescent] is on, which a boot line should say.
     * Until then, [backgroundStatus] names each refused attempt.
     */
    suspend fun awaitTrustDescent(): Int = backfill?.await() ?: 0

    /**
     * The background workers' failure line — EMPTY while they are healthy, so a
     * status display can splice it in unconditionally.
     *
     * The trust drain and the guard refresh retry forever and keep their state
     * safe when they fail, which makes a permanently broken one silent: ranking
     * quietly stops tracking trust writes, or [WriterTopology.SHARED]'s
     * staleness bound quietly stops holding. This is the only place that says so
     * — see [BackgroundFailures].
     */
    fun backgroundStatus(): String = BackgroundFailures.statusLine()

    /**
     * Repair the trust view: drain queued projection work a crashed process left
     * behind (see DirtLedger), then re-derive any service whose scores are not
     * projected under every observer naming it. Worth running at startup — dedup
     * means a corpus mirrored before its provider lists arrived stays silently
     * unprojected, and every ranked search comes back empty.
     */
    suspend fun reconcileTrust(onProgress: ((inspected: Int, total: Int, rebuilt: Int, derivedInService: Int) -> Unit)? = null): TrustReconciler.Reconciliation = withActivity(Activity.Reconcile) { reconciler.reconcile(onProgress = onProgress) }

    /**
     * Re-derive the WHOLE trust view from the stored scores — the operator's
     * hammer, bounded only by the corpus. [reconcileTrust] does the same repair
     * per affected service and normally finds nothing to do.
     */
    suspend fun rebuildTrust() = withActivity(Activity.Reconcile) { reconciler.rebuildAll() }

    /**
     * Full audit: does every reputation doc match its 10040+30382 records?
     * Drains queued deferred work (lag, not drift), compares each stored parent
     * against a fresh derivation, and finds orphan docs with no records behind
     * them. Read-only beyond that drain; with [repair], drifted subjects are
     * re-derived in place — the targeted alternative to [rebuildTrust]. See
     * [TrustReconciler.verify] for the full contract.
     */
    suspend fun verifyTrust(
        repair: Boolean = false,
        onProgress: ((subjectsChecked: Int) -> Unit)? = null,
    ): TrustReconciler.TrustAudit = withActivity(Activity.Reconcile) { reconciler.verify(repair, onProgress) }

    /**
     * DELETE the orphan scores: every stored kind-30382 signed by a service no
     * stored kind-10040 names. Those cards can never become a tensor cell for
     * any observer, so a mirror that syncs 30382s by kind can reclaim them
     * wholesale. [dryRun] gives the same report with no writes; it is not a
     * tombstone, so pair the sweep with narrowing the sync.
     *
     * Guardrails (in [TrustReconciler.sweepOrphanScores]): a store with NO
     * readable 10040 sweeps nothing — indistinguishable from "no provider list
     * mirrored yet", where deleting would take the whole score corpus — and a
     * service claimed mid-sweep is dropped at the next page boundary. Deletions
     * take the writer lock a page at a time, sharing the store with live
     * ingest. Operator action only — [reconcileTrust] never calls it.
     */
    suspend fun sweepOrphanScores(
        dryRun: Boolean = false,
        onProgress: ((servicesDone: Int, totalServices: Int, scoresSwept: Int, totalScores: Int) -> Unit)? = null,
    ): TrustReconciler.OrphanSweep = withActivity(Activity.Sweep) { reconciler.sweepOrphanScores(dryRun, onProgress) }

    /**
     * The deferred-projection barrier: drain every queued trust reaction so
     * ranking reflects all inserts acked before the call (not the writes racing
     * it — the drain chases the queue until a pass finds nothing new). Safe
     * alongside the background drainer (draining is idempotent; both take the
     * writer lock per batch); a no-op with deferral off.
     */
    suspend fun awaitTrustProjection() = trust.dirt.drain { store.withWriteLock(it) }

    /**
     * The guard-cache barrier, the counterpart to [awaitTrustProjection] for
     * NIP-09/NIP-62 admission: rebuild the set of owners with a stored
     * tombstone/vanish from the corpus NOW, instead of waiting out
     * `guardRefreshSeconds`. For a deployment where something ELSE writes this
     * Vespa ([WriterTopology.SHARED]) and knows when — call it after a sync
     * round that mirrored kinds 5/62, and the next insert honours what it
     * brought in. Costs one distinct-author scan per guard kind; union-only, so
     * it can never unflag an owner.
     */
    suspend fun refreshGuardOwners() = store.refreshGuardOwners()

    override fun close() {
        // Drainer first: queued work survives in the persisted marker for the
        // next open — shutdown must not block on a six-figure walk.
        drainScope?.cancel()
        backfill?.cancel()
        store.close()
    }

    companion object {
        /**
         * Open a store over the Vespa at [url] (its query/document endpoint).
         *
         * [autoDeploy] (default) deploys the bundled schema to [configUrl] on
         * first contact; turn it off when an operator owns schema deployment.
         * [relay] is the store's own relay url (NIP-62 vanish scope / NIP-42
         * identity); null for a bare store. [endpoints] names every container
         * endpoint of a multi-container cluster — the feed client spreads its
         * HTTP/2 connections across all of them, which beats one load-balancer
         * address (see docs/scaling.md).
         *
         * [deferTrustProjection] (default ON) moves the projection's read-based
         * reactions (a 10040's service walk can hold the writer lock for
         * minutes) onto a background drainer. Inserts get faster and reactions
         * coalesce; ranked search lags trust writes by the drain cycle — the
         * events themselves are always read-your-writes, and
         * [awaitTrustProjection] is the explicit barrier. Correctness is
         * identical in both modes: the same crash-safe persisted marker is
         * drained here, at the next write, or by [reconcileTrust].
         *
         * [writers] states whether ANY OTHER process feeds this same Vespa — a
         * second store instance, a sync router, a mirror. It cannot be detected
         * from here, and the guard-owner cache it governs is a pure read
         * optimization whose only failure is serving an event a tombstone
         * covers, so it defaults to [WriterTopology.SHARED_STRICT]: no cache,
         * every insert probes NIP-09/NIP-62. A deployment that owns every write
         * for its owners buys the read savings back — with no window either — by
         * asserting [WriterTopology.SINGLE_WRITER]; [WriterTopology.SHARED] is
         * the middle ground, accepting a window bounded by
         * [guardRefreshSeconds].
         *
         * The store imposes no result cap of its own: bounding a query's cost
         * belongs to whoever writes the filter.
         */
        fun open(
            url: String = "http://localhost:8080",
            relay: NormalizedRelayUrl? = null,
            autoDeploy: Boolean = true,
            configUrl: String = deriveConfigUrl(url),
            endpoints: List<String> = emptyList(),
            deferTrustProjection: Boolean = true,
            writers: WriterTopology = WriterTopology.SHARED_STRICT,
            guardRefreshSeconds: Long = DEFAULT_GUARD_REFRESH_MILLIS / 1000,
            /**
             * How much of a searching read's answer may be events it POINTS AT
             * — a label's subject, a Trusted List's members. The caps and the
             * placement are the OPERATOR's call, so they arrive here rather than
             * being decided in the store; what the store owns is applying them.
             * Only ever engages on a read carrying terms, so a mirror's paging
             * and a NIP-77 catch-up are untouched whatever this says.
             */
            searchExpansion: SearchExpansionLimits = SearchExpansionLimits.Default,
            /**
             * How many hits ONE AUTHOR may hold on a ranked page, or null — the
             * default — for no cap. Off because a cap drops events a filter
             * matched, which is an editorial decision an operator makes and a
             * library does not; see `NostrSemanticsStore.maxHitsPerAuthor` for
             * what it is for (one mirror bot taking 27 of a page's top 50) and
             * why it never touches a recency-ordered recall.
             */
            maxHitsPerAuthor: Int? = null,
            /**
             * Whether a ranked search may take the trust descent (TrustDescent)
             * once its `max_rank` walk has run. An OPERATOR'S switch, not a
             * knob in the answer: the descent serves the exact page at every
             * rung, so off and on differ only in cost — and on a cluster where
             * the engine does not drive a rung by the imported `author_max_rank`
             * range, each rung is a full text walk and the descent costs two to
             * three of them (staging, 2026-09-04: `bitcoin` 2.7–4.4 s → 8.4 s).
             * Off keeps the schema, the walk and the upkeep, so turning it back
             * on is a restart. Defaults to `VESPA_TRUST_DESCENT` (`off`, `false`
             * or `0` disable; unset is on) — see [trustDescentFromEnv].
             */
            trustDescent: Boolean = trustDescentFromEnv(),
            /**
             * Capture reads slower than this many milliseconds in the
             * slow-query ring, or null (the default) to capture none.
             *
             * OFF BY DEFAULT because the ring is the one place this store
             * retains a QUERY STRING, and a query string is user data. An
             * operator choosing to keep a record of what people searched for
             * should have to say so rather than discover it. Bounded by the
             * ring rather than by how many distinct queries exist, so turning
             * it on cannot grow without limit; observer keys are truncated
             * where they are captured.
             */
            slowQueryThresholdMillis: Long? = null,
        ): VespaEventStore {
            if (autoDeploy) SchemaDeployer(configUrl).deployIfAbsent(url)
            val ledger = CostLedger(slowQueryThresholdNanos = slowQueryThresholdMillis?.let { it * 1_000_000 })
            // The ledger reaches the engine client too: rank profile, docs
            // matched and Vespa's own timing are only knowable from inside the
            // thing that speaks to Vespa (docs/telemetry.md §3.5).
            val eventIndex = VespaEventIndex(url, endpoints = endpoints, ledger = ledger)
            val reputations = VespaReputationIndex(url)
            // METERED AT THE ENGINE SEAM: everything below this line is what
            // actually reached Vespa, whichever route asked for it — the
            // store's own reads, the projection's, a sweep's. Nothing above
            // needs a call-site timer.
            val trust = TrustProjection(MeteredEventIndex(ledger, eventIndex), reputations)
            val store =
                NostrSemanticsStore(
                    trust,
                    relay = relay,
                    writers = writers,
                    guardRefreshMillis = guardRefreshSeconds * 1000,
                    searchExpansion = searchExpansion,
                    maxHitsPerAuthor = maxHitsPerAuthor,
                    metrics = ledger,
                )
            // GAUGES: instantaneous, pulled at snapshot time, and read from
            // their owners rather than mirrored — a queue depth has no
            // cumulative form and must never be diffed like a counter.
            ledger.gauge("trust.pending.subjects") { trust.dirt.pendingSubjects() }
            ledger.gauge("trust.pending.services") { trust.dirt.pendingServices() }
            ledger.gauge("feed.inflight") { eventIndex.feedInflight() }
            ledger.gauge("lock.held") { IngestStats.allHeld().size.toLong() }
            // The reconciler's and drainer's mutating batches take the store's
            // writer lock (the gate): repairs must not race live inserts'
            // recomputes.
            val gate: suspend (suspend () -> Unit) -> Unit = { store.withWriteLock(it) }
            val reconciler = TrustReconciler(eventIndex, reputations, trust.recompute, trust.dirt, gate = gate)
            val drainScope = if (deferTrustProjection) startDrainer(trust, gate) else null
            // The descent is off until every reputation document carries the
            // scalar it cuts on. One walk, once, in the background — and the
            // switch is thrown only when it returns, so a boot that finds the
            // marker already there is on within one read. A boot that finds
            // the engine still coming up retries the walk until it is there
            // (the failure accounting is inside runUntilDone).
            val backfill =
                CoroutineScope(SupervisorJob() + Dispatchers.Default).async {
                    // The walk runs even with the descent switched off: it keeps
                    // the invariant the descent needs, so switching on later is
                    // a restart and not a migration.
                    val written = withActivity(Activity.Backfill) { MaxRankBackfill(reputations).runUntilDone(BACKFILL_RETRY_MILLIS) }
                    if (trustDescent) eventIndex.trustDescent = true
                    written
                }
            return VespaEventStore(store, eventIndex, reconciler, trust, drainScope, backfill, trustDescent)
        }

        /**
         * The deferred-mode drain worker: a CONFLATED signal (a burst of writes
         * = one wake-up; drain processes everything pending anyway), retrying
         * with fixed backoff on engine failure — the marker keeps the work, so
         * a failed drain loses nothing. Fired once at startup so work a
         * previous process left queued doesn't wait for the first write.
         */
        private fun startDrainer(
            trust: TrustProjection,
            gate: suspend (suspend () -> Unit) -> Unit,
        ): CoroutineScope {
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val signal = Channel<Unit>(Channel.CONFLATED)
            trust.dirt.deferTo { signal.trySend(Unit) }
            scope.launch {
                for (wake in signal) {
                    try {
                        withActivity(Activity.Drain) { trust.dirt.drain(gate) }
                        BackgroundFailures.succeeded(BackgroundFailures.TRUST_DRAIN)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (t: Throwable) {
                        // Engine hiccup mid-drain: the marker still names the
                        // work, so retrying loses nothing. Counted rather than
                        // swallowed — a drain that never succeeds stops ranking
                        // from tracking trust writes, and looks exactly like a
                        // drain with nothing to do (see [BackgroundFailures]).
                        BackgroundFailures.record(BackgroundFailures.TRUST_DRAIN, t)
                        delay(DRAIN_RETRY_MILLIS)
                        signal.trySend(Unit)
                    }
                }
            }
            signal.trySend(Unit)
            return scope
        }

        /** Backoff between drain retries after an engine failure. */
        private const val DRAIN_RETRY_MILLIS = 5_000L

        /** Backoff between `max_rank` walk attempts after an engine failure — the same cadence as the drain's, for the same reason. */
        private const val BACKFILL_RETRY_MILLIS = 5_000L

        /** The environment's name for the descent switch — see [open]'s `trustDescent`. */
        const val TRUST_DESCENT_ENV = "VESPA_TRUST_DESCENT"

        /** `VESPA_TRUST_DESCENT` read the way [open] reads it: unset is on; `off`, `false` and `0` (any case, trimmed) are off; anything else is on. */
        fun trustDescentFromEnv(value: String? = System.getenv(TRUST_DESCENT_ENV)): Boolean = value?.trim()?.lowercase() !in setOf("off", "false", "0")

        /** The config server sits on :19071 by convention, on the same host as the :8080 query endpoint. */
        internal fun deriveConfigUrl(queryUrl: String): String {
            val u = URI.create(queryUrl)
            return URI(u.scheme, null, u.host, 19071, null, null, null).toString()
        }
    }
}
