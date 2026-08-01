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

import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaReputationIndex
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import java.net.URI

/**
 * The library's public handle AND front door: a ready [IEventStore] backed by
 * Vespa. [open] wires the whole NostrEventStore(TrustProjection(VespaEventIndex,
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
    val store: NostrEventStore,
    /**
     * The raw engine index, NOT trust-projected. Reads through it skip the
     * projection decorator — status/health metrics query it directly, since they
     * only count and never mutate trust data.
     */
    val events: VespaEventIndex,
    /**
     * The trust view over [events]. Exposed because it is derived on WRITE and
     * therefore cannot always repair itself: see [reconcileTrust].
     */
    private val trust: TrustProjection,
) : IEventStore by store {
    /** The engine's feed-health status line (bulk-ingest backpressure), for progress/status output. */
    fun feedGauge(): String = events.feedGauge()

    /**
     * The negentropy id set, reporting the running count after every page.
     *
     * Exposed on the handle because the walk is the longest silent phase a mirror
     * has — minutes on a large corpus — and a caller that cannot see inside it
     * cannot tell a working snapshot from a hung one. See [NostrEventStore].
     */
    suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int? = null,
        onProgress: ((collected: Int) -> Unit)? = null,
    ): List<IdAndTime> = store.snapshotIdsForNegentropy(filters, maxEntries, onProgress)

    /**
     * Re-derive the trust view for any service whose scores are not projected
     * under the observer currently mapped to it, and report what it had to fix.
     *
     * Worth running at startup. The projection is maintained by write triggers,
     * and dedup means an event already in the store never reaches them again —
     * so a corpus that was mirrored before its provider lists arrived stays
     * unprojected, silently, and every ranked search comes back empty.
     */
    suspend fun reconcileTrust(): TrustProjection.Reconciliation = trust.reconcile()

    /**
     * Re-derive the WHOLE trust view from the stored scores. Bounded only by the
     * corpus, so this is the operator's hammer — [reconcileTrust] does the same
     * repair per affected service and normally finds nothing to do.
     */
    suspend fun rebuildTrust() = trust.rebuildAll()

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
        ): VespaEventStore {
            if (autoDeploy) SchemaDeployer(configUrl).deployIfAbsent(url)
            val events = VespaEventIndex(url, endpoints = endpoints)
            val trust = TrustProjection(events, VespaReputationIndex(url))
            val store = NostrEventStore(trust, relay = relay)
            return VespaEventStore(store, events, trust)
        }

        /** The config server sits on :19071 by convention, on the same host as the :8080 query endpoint. */
        internal fun deriveConfigUrl(queryUrl: String): String {
            val u = URI.create(queryUrl)
            return URI(u.scheme, null, u.host, 19071, null, null, null).toString()
        }
    }
}
