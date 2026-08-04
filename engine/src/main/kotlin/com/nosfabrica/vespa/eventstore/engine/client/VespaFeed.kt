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
package com.nosfabrica.vespa.eventstore.engine.client

import ai.vespa.feed.client.FeedClient
import ai.vespa.feed.client.FeedClientBuilder
import java.net.URI

/**
 * The WRITE-side transport to Vespa: builds and tunes the official feed client
 * (HTTP/2 multiplexed, per-doc ordering, retries built in) and reads its
 * health back. What gets written stays in [VespaEventIndex]; this owns the
 * measured connection budget and throttle behavior.
 */
internal class VespaFeed(
    urls: List<String>,
) {
    /**
     * Connections per endpoint. The tuned 32 is a CLUSTER budget, split across
     * [urls], not multiplied: the client sizes one shared Jetty pool at
     * `max(min(cores, 64), 8) + connectionsPerEndpoint * endpoints` and throws
     * at construction unless that total strictly exceeds the threads leased
     * ("Insufficient configured threads: required=76 < max=76"). Splitting
     * keeps total parallelism — what the figure was measured against —
     * identical however the cluster is named. `VESPA_FEED_CONNECTIONS`
     * overrides per endpoint and is NOT divided (deployment tuning is
     * explicit); it can hit the same ceiling but fails loudly at startup.
     */
    private val connectionsPerEndpoint: Int =
        System.getenv("VESPA_FEED_CONNECTIONS")?.toIntOrNull()
            ?: (32 / urls.size).coerceAtLeast(1)

    /** The tuned client. Operations go straight through it; only construction and health live here. */
    val client: FeedClient =
        FeedClientBuilder
            .create(urls.map { URI.create(it) })
            // The throttle FLOOR (minInflight = 2 x connectionsPerEndpoint) is
            // what pins our bursty batched ingest — the throttler never sustains
            // its upward probe, so it idles there. At 8 connections that floor
            // was ~16 in flight, ~1.2k docs/s with the engine ~5x idle; more
            // connections raise the floor AND real HTTP/2 parallelism, and the
            // throttler still adapts DOWN under pushback. Too many starve the
            // client's own Jetty pool on small-core hosts — see
            // [connectionsPerEndpoint]. Overridable for deployment tuning:
            // VESPA_FEED_CONNECTIONS / VESPA_FEED_STREAMS /
            // VESPA_FEED_INFLIGHT_FACTOR; defaults are the measured sweet spot
            // for a small-core single-node host.
            .setConnectionsPerEndpoint(connectionsPerEndpoint)
            .setMaxStreamPerConnection(System.getenv("VESPA_FEED_STREAMS")?.toIntOrNull() ?: 128)
            .apply { System.getenv("VESPA_FEED_INFLIGHT_FACTOR")?.toIntOrNull()?.let { setInitialInflightFactor(it) } }
            .setRetryStrategy(
                object : FeedClient.RetryStrategy {
                    // Bounded: a dead Vespa should surface as failed ops, not a hang.
                    override fun retries() = 5
                },
            ).build()

    /**
     * One-line feed health: cumulative acks, live in-flight window, per-request
     * latency — tells "engine slow" (big window, high latency) from "client not
     * pushing" (tiny window, low latency) at a glance.
     */
    fun statusLine(): String {
        val s = client.stats()
        // Non-2xx responses get retried and usually succeed (pushback, not
        // loss); only transport exceptions are worth shouting about.
        val retried = s.responses() - s.successes()
        return "feed ok ${s.successes()} inflight ${s.inflight()} lat ${s.averageLatencyMillis()}ms" +
            (if (retried > 0) " retry $retried" else "") +
            if (s.exceptions() > 0) " EXC ${s.exceptions()}" else ""
    }

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    fun close() = client.close(true)
}
