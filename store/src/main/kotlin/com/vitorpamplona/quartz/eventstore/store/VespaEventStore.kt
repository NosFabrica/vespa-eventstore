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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
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
) : IEventStore by store {
    /** The engine's feed-health status line (bulk-ingest backpressure), for progress/status output. */
    fun feedGauge(): String = events.feedGauge()

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
         * [maxHits] bounds a query that carries no `limit` of its own. Null (the
         * default) means unbounded: the store returns every match rather than a
         * page size it chose for you. Set it when a deployment would rather cap
         * the cost of an unbounded recall — the decision is yours, not the
         * library's. A filter's own `limit` always wins over it.
         */
        fun open(
            url: String = "http://localhost:8080",
            relay: NormalizedRelayUrl? = null,
            autoDeploy: Boolean = true,
            configUrl: String = deriveConfigUrl(url),
            endpoints: List<String> = emptyList(),
            maxHits: Int? = null,
        ): VespaEventStore {
            if (autoDeploy) SchemaDeployer(configUrl).deployIfAbsent(url)
            val events = VespaEventIndex(url, maxHits = maxHits, endpoints = endpoints)
            val store = NostrEventStore(TrustProjection(events, VespaReputationIndex(url)), relay = relay)
            return VespaEventStore(store, events)
        }

        /** The config server sits on :19071 by convention, on the same host as the :8080 query endpoint. */
        internal fun deriveConfigUrl(queryUrl: String): String {
            val u = URI.create(queryUrl)
            return URI(u.scheme, null, u.host, 19071, null, null, null).toString()
        }
    }
}
