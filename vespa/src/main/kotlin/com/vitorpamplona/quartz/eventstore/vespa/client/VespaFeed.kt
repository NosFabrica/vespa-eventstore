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
package com.vitorpamplona.quartz.eventstore.vespa.client

import ai.vespa.feed.client.FeedClient
import ai.vespa.feed.client.FeedClientBuilder
import java.net.URI

/**
 * The WRITE-side transport to Vespa: builds and tunes the official feed client
 * (HTTP/2 multiplexed, per-doc ordering, retries built in) and reads its health
 * back out. What gets written — document ids, fields, conditions — stays in
 * [VespaEventIndex]; this class owns the connection budget and the throttle
 * behavior, which were measured, not guessed, and change for transport reasons
 * only.
 */
internal class VespaFeed(
    urls: List<String>,
) {
    /**
     * Connections the feed client opens to EACH endpoint.
     *
     * The tuned 32 is a budget for the cluster, not for one host, so it is split
     * across [urls] rather than multiplied by them. That distinction is load
     * bearing: the client sizes ONE shared Jetty pool at
     * `max(min(cores, 64), 8) + connectionsPerEndpoint * endpoints`, and Jetty
     * refuses to start unless that total is strictly greater than the threads the
     * HTTP client leases from it. Two endpoints at 32 each on a 12-core host is
     * `12 + 64 = 76` against a required 76 — one thread short, and the client
     * throws from its constructor:
     *
     *     Insufficient configured threads: required=76 < max=76
     *
     * Splitting keeps the total parallelism (and therefore the pool) identical
     * whether the cluster is named as one endpoint or five, which is what the
     * figure was measured against in the first place.
     *
     * `VESPA_FEED_CONNECTIONS` overrides it per endpoint and is NOT divided —
     * deployment tuning is explicit by definition, and the benchmark sweeps it.
     * Setting it high across many endpoints can reach the same Jetty ceiling; it
     * fails loudly at startup with the message above rather than degrading.
     */
    private val connectionsPerEndpoint: Int =
        System.getenv("VESPA_FEED_CONNECTIONS")?.toIntOrNull()
            ?: (32 / urls.size).coerceAtLeast(1)

    /** The tuned client. Operations go straight through it; only construction and health live here. */
    val client: FeedClient =
        FeedClientBuilder
            .create(urls.map { URI.create(it) })
            // The throttle FLOOR is what pins bulk ingest, and it is hard-wired to
            // minInflight = 2 x connectionsPerEndpoint. Under our bursty batched
            // writes (putAll bursts, then a gap while the next chunk dedups) the
            // dynamic throttler never sustains its upward probe, so it idles at that
            // floor. At the old 8 connections that floor was ~16 in flight — about
            // 1.2k docs/s while the engine sat at ~2.4 of 12 cores, ~5x idle. Raising
            // the connection count raises the floor (64 in flight here) AND the real
            // HTTP/2 parallelism, so ingest drives the engine harder. The throttler
            // still adapts DOWN if Vespa pushes back (retries absorb any overshoot).
            //
            // The client sizes its own Jetty pool from the connection count, so on a
            // small-core host too many connections starve that pool; 32 across the
            // cluster keeps headroom. See [connectionsPerEndpoint]. (The old reflective
            // setInitialInflightFactor knob was dead: the 8.7 throttler ignores it —
            // the initial target is already maxInflight.)
            // Overridable for deployment tuning (and the benchmark's feed-window
            // grid): VESPA_FEED_CONNECTIONS / VESPA_FEED_STREAMS /
            // VESPA_FEED_INFLIGHT_FACTOR. Defaults are the measured sweet spot
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
     * One-line feed-client health for status lines: cumulative acks, the LIVE
     * in-flight window, and per-request HTTP latency. Together these tell "the
     * engine is slow" apart from "the client isn't pushing" at a glance. A
     * starved window shows tiny inflight at low latency; a saturated engine
     * shows a big window at high latency.
     */
    fun gauge(): String {
        val s = client.stats()
        // Non-2xx responses get retried and usually succeed: pushback, not
        // loss (a big window ramping down shows a burst of 429s here). Only
        // transport exceptions are worth shouting about.
        val retried = s.responses() - s.successes()
        return "feed ok ${s.successes()} inflight ${s.inflight()} lat ${s.averageLatencyMillis()}ms" +
            (if (retried > 0) " retry $retried" else "") +
            if (s.exceptions() > 0) " EXC ${s.exceptions()}" else ""
    }

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    fun close() = client.close(true)
}
