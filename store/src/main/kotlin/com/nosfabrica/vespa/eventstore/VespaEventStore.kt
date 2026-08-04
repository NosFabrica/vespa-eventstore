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

import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.nosfabrica.vespa.eventstore.trust.TrustReconciler
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
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
     * Quartz interface (e.g. `distinctDTags`).
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
) : IEventStore by store {
    /** The engine's feed-health status line (bulk-ingest backpressure), for progress/status output. */
    fun feedStatus(): String = eventIndex.feedStatus()

    /**
     * Repair the trust view: drain queued projection work a crashed process left
     * behind (see DirtLedger), then re-derive any service whose scores are not
     * projected under every observer naming it. Worth running at startup — dedup
     * means a corpus mirrored before its provider lists arrived stays silently
     * unprojected, and every ranked search comes back empty.
     */
    suspend fun reconcileTrust(onProgress: ((inspected: Int, total: Int, rebuilt: Int, derivedInService: Int) -> Unit)? = null): TrustReconciler.Reconciliation = reconciler.reconcile(onProgress = onProgress)

    /**
     * Re-derive the WHOLE trust view from the stored scores — the operator's
     * hammer, bounded only by the corpus. [reconcileTrust] does the same repair
     * per affected service and normally finds nothing to do.
     */
    suspend fun rebuildTrust() = reconciler.rebuildAll()

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
    ): TrustReconciler.TrustAudit = reconciler.verify(repair, onProgress)

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
    ): TrustReconciler.OrphanSweep = reconciler.sweepOrphanScores(dryRun, onProgress)

    /**
     * The deferred-projection barrier: drain every queued trust reaction so
     * ranking reflects all inserts acked before the call (not the writes racing
     * it — the drain chases the queue until a pass finds nothing new). Safe
     * alongside the background drainer (draining is idempotent; both take the
     * writer lock per batch); a no-op with deferral off.
     */
    suspend fun awaitTrustProjection() = trust.dirt.drain { store.withWriteLock(it) }

    override fun close() {
        // Drainer first: queued work survives in the persisted marker for the
        // next open — shutdown must not block on a six-figure walk.
        drainScope?.cancel()
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
        ): VespaEventStore {
            if (autoDeploy) SchemaDeployer(configUrl).deployIfAbsent(url)
            val eventIndex = VespaEventIndex(url, endpoints = endpoints)
            val reputations = VespaReputationIndex(url)
            val trust = TrustProjection(eventIndex, reputations)
            val store = NostrSemanticsStore(trust, relay = relay)
            // The reconciler's and drainer's mutating batches take the store's
            // writer lock (the gate): repairs must not race live inserts'
            // recomputes.
            val gate: suspend (suspend () -> Unit) -> Unit = { store.withWriteLock(it) }
            val reconciler = TrustReconciler(eventIndex, reputations, trust.recompute, trust.dirt, gate = gate)
            val drainScope = if (deferTrustProjection) startDrainer(trust, gate) else null
            return VespaEventStore(store, eventIndex, reconciler, trust, drainScope)
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
                        trust.dirt.drain(gate)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (t: Throwable) {
                        // Engine hiccup mid-drain: the marker still names the work.
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

        /** The config server sits on :19071 by convention, on the same host as the :8080 query endpoint. */
        internal fun deriveConfigUrl(queryUrl: String): String {
            val u = URI.create(queryUrl)
            return URI(u.scheme, null, u.host, 19071, null, null, null).toString()
        }
    }
}
