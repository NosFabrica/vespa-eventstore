/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store

import com.vitorpamplona.quartz.eventstore.store.trust.TrustProjection
import com.vitorpamplona.quartz.eventstore.store.trust.TrustReconciler
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaReputationIndex
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
 * The library's public handle AND front door: a ready [IEventStore] backed by
 * Vespa. [open] wires the whole NostrSemanticsStore(TrustProjection(VespaEventIndex,
 * VespaReputationIndex)) stack over a running Vespa in one call (plus, by default,
 * a first-run schema deploy), and this handle delegates the entire [IEventStore]
 * surface to it — so a consumer programs against the Quartz interface and never
 * sees the stack.
 *
 * Vespa itself is a prerequisite, like a database: point [open] at one that is
 * already running.
 *
 * Closeable (via [IEventStore]'s AutoCloseable), so `open(...).use { ... }` works.
 */
class VespaEventStore internal constructor(
    /**
     * The concrete store — the [IEventStore] this handle delegates, exposed for
     * the Vespa-specific capabilities that live beyond the Quartz interface (e.g.
     * `distinctDTags`, used by trust-graph walks). Most consumers can ignore it
     * and use this handle directly as an [IEventStore].
     */
    val store: NostrSemanticsStore,
    /**
     * The raw engine index, NOT trust-projected. Reads through it skip the
     * projection decorator — status/health metrics query it directly, since they
     * only count and never mutate trust data.
     */
    val events: VespaEventIndex,
    /**
     * The repair tool for the trust view over [events]. Held here because the
     * view is derived on WRITE and therefore cannot always repair itself: see
     * [reconcileTrust].
     */
    private val reconciler: TrustReconciler,
    /** The projection, for the deferred-mode drain barrier ([awaitTrustProjection]). */
    private val trust: TrustProjection,
    /** The background drain worker's scope in deferred mode; null when the projection settles inline. */
    private val drainScope: CoroutineScope? = null,
) : IEventStore by store {
    /** The engine's feed-health status line (bulk-ingest backpressure), for progress/status output. */
    fun feedGauge(): String = events.feedGauge()

    /**
     * Repair the trust view: drain any projection work a crashed or shut-down
     * process left queued (see DirtLedger), then re-derive any service whose
     * scores are not projected under EVERY observer currently naming it, and
     * report what it had to fix.
     *
     * Worth running at startup. The projection is maintained by write triggers,
     * and dedup means an event already in the store never reaches them again —
     * so a corpus that was mirrored before its provider lists arrived stays
     * unprojected, silently, and every ranked search comes back empty.
     */
    suspend fun reconcileTrust(onProgress: ((inspected: Int, total: Int, rebuilt: Int, derivedInService: Int) -> Unit)? = null): TrustReconciler.Reconciliation = reconciler.reconcile(onProgress = onProgress)

    /**
     * Re-derive the WHOLE trust view from the stored scores. Bounded only by the
     * corpus, so this is the operator's hammer — [reconcileTrust] does the same
     * repair per affected service and normally finds nothing to do.
     */
    suspend fun rebuildTrust() = reconciler.rebuildAll()

    /**
     * FULL audit: does every reputation doc match its 10040+30382 records?
     * Drains any queued deferred work (lag, not drift), then compares the
     * stored parent of every scored subject against a fresh derivation from the
     * records, and sweeps the reputation corpus for orphan docs with no records
     * behind them. Read-only beyond that drain; with [repair] the drifted
     * subjects are re-derived in place — the targeted alternative to
     * [rebuildTrust]. The report carries complete counts and the first examples
     * of each mismatch. See [TrustReconciler.verify] for the full contract.
     */
    suspend fun verifyTrust(
        repair: Boolean = false,
        onProgress: ((subjectsChecked: Int) -> Unit)? = null,
    ): TrustReconciler.TrustAudit = reconciler.verify(repair, onProgress)

    /**
     * DELETE the orphan scores: every stored kind-30382 signed by a service that
     * no stored kind-10040 names, for either dimension. Those cards can never
     * become a tensor cell for any observer — they rank nothing, gate nothing
     * and are never read — so a mirror that syncs 30382s by kind (and therefore
     * pulls every publishing service on the network, not just the ones its users
     * trust) can reclaim them wholesale.
     *
     * Pass [dryRun] to get the same report — which services, how many cards —
     * with no writes. Both forms cost one grouping query up front; the dry run
     * is nothing but that query.
     *
     * A deletion is not a tombstone. A mirror that keeps syncing 30382s by kind
     * re-downloads what this freed, so pair the sweep with narrowing that sync
     * to the services your 10040s actually name.
     *
     * Two guardrails, both in [TrustReconciler.sweepOrphanScores]: a store with
     * NO readable 10040 sweeps nothing (that state is indistinguishable from "no
     * provider list has been mirrored yet", where deleting would take the whole
     * score corpus), and a service some 10040 claims mid-sweep is dropped from
     * the sweep at the next page boundary. Deletions take the writer lock a page
     * at a time, so a long sweep shares the store with live ingest.
     *
     * This is an operator action and never automatic — [reconcileTrust] does not
     * call it.
     */
    suspend fun sweepOrphanScores(
        dryRun: Boolean = false,
        onProgress: ((servicesDone: Int, totalServices: Int, scoresSwept: Int, totalScores: Int) -> Unit)? = null,
    ): TrustReconciler.OrphanSweep = reconciler.sweepOrphanScores(dryRun, onProgress)

    /**
     * The deferred-projection BARRIER: drain every queued trust reaction before
     * returning, so ranking reflects all inserts acked so far — the
     * read-your-writes moment a caller occasionally needs (a test, a "publish
     * then search" API) under a store opened with `deferTrustProjection`. A
     * no-op when the queue is empty, and safe alongside the background drainer
     * (draining is idempotent; both take the writer lock per batch). With
     * deferral off this returns immediately — inline settles leave no queue.
     * Note the barrier chases the queue: under a sustained stream of trust
     * writes it keeps draining until a pass finds nothing new, so it bounds
     * "everything acked BEFORE the call", not the writes racing it.
     */
    suspend fun awaitTrustProjection() = trust.dirt.drain { store.withWriteLock(it) }

    override fun close() {
        // Stop the drainer FIRST, then the store: queued work survives in the
        // persisted marker and is drained by the next open's startup signal (or
        // reconcileTrust) — shutdown must not block on a six-figure walk.
        drainScope?.cancel()
        store.close()
    }

    companion object {
        /**
         * Open a store over the Vespa at [url] (its query/document endpoint).
         *
         * With [autoDeploy] (the default), the bundled schema is deployed to
         * [configUrl] the first time — a fresh Vespa becomes queryable with no
         * separate deploy step. Turn it off when an operator owns schema deployment
         * out of band; then a missing schema surfaces as a query error, not a silent
         * one. [configUrl] defaults to the config server's conventional :19071 on the
         * same host as [url].
         *
         * [relay] is the store's own relay url (NIP-62 vanish scope / NIP-42 identity)
         * when it sits behind a relay; leave it null for a bare store.
         *
         * [endpoints] names EVERY container endpoint of a multi-container
         * cluster: the feed client spreads its HTTP/2 connections across all of
         * them and reads round-robin, which beats funnelling writes through one
         * load-balancer address. Empty (the default) = just [url]. See
         * docs/scaling.md; a multi-node deployment pairs this with
         * `autoDeploy = false` and an operator-owned application package.
         *
         * [deferTrustProjection] (default ON) moves the trust projection's
         * read-based reactions — a 10040's service walk (which could otherwise
         * hold the writer lock for minutes), single-score re-derives, retraction
         * and deletion re-derives — off the insert path onto a background
         * drainer that batches them under the writer lock. Inserts get faster
         * and reactions about the same subjects coalesce; the cost is that
         * ranked search lags trust writes by the drain cycle (the events
         * themselves are always read-your-writes). [awaitTrustProjection] is
         * the explicit barrier. The bulk zero-read cell path stays inline
         * either way, so mirror ingest keeps immediate ranking. Turn OFF for
         * strict read-your-writes ranking on every insert. Correctness is
         * identical in both modes: the work queue is the same crash-safe
         * persisted marker, drained here, at the next write, or by
         * [reconcileTrust] — whichever comes first.
         *
         * The store imposes no result cap of its own: a filter with a `limit`
         * gets that many events, one without gets every match. Bounding what a
         * query costs belongs to whoever writes the filter.
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
            val events = VespaEventIndex(url, endpoints = endpoints)
            val reputations = VespaReputationIndex(url)
            val trust = TrustProjection(events, reputations)
            val store = NostrSemanticsStore(trust, relay = relay)
            // The reconciler's and drainer's mutating batches take the store's
            // writer lock (the gate): repairs must not race live inserts'
            // recomputes.
            val gate: suspend (suspend () -> Unit) -> Unit = { store.withWriteLock(it) }
            val reconciler = TrustReconciler(events, reputations, trust.recompute, trust.dirt, gate = gate)
            val drainScope = if (deferTrustProjection) startDrainer(trust, gate) else null
            return VespaEventStore(store, events, reconciler, trust, drainScope)
        }

        /**
         * The deferred-mode drain worker: a CONFLATED signal (a burst of writes
         * = one wake-up; drain always processes everything pending anyway) and
         * one loop that drains on each signal, retrying with a fixed backoff on
         * engine failure — the marker keeps the work, so a failed drain loses
         * nothing. Fired once at startup: work a previous process left queued
         * must not wait for the first write.
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
