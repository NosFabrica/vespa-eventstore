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
import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager

/**
 * The WRITE-side transport to Vespa: builds and tunes the official feed client
 * (HTTP/2 multiplexed, per-doc ordering, retries built in) and reads its
 * health back. What gets written stays in [VespaEventIndex]; this owns the
 * measured connection budget and throttle behavior.
 */
internal class VespaFeed(
    urls: List<String>,
    /**
     * Build the client now rather than on the first write. True for a store
     * that writes, so a feed setting the client refuses (a connection budget
     * past its thread pool) still fails the boot rather than the first write.
     * False for a read-only store, which never writes and so never pays for
     * the client's Jetty pool and HTTP/2 connections at all.
     */
    eager: Boolean = true,
    /** The feed settings; a field left null reads its `VESPA_FEED_*` variable as before ([ClientTuning]). */
    private val tuning: ClientTuning = ClientTuning(),
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
        tuning.feedConnectionsOrEnv()
            ?: (32 / urls.size).coerceAtLeast(1)

    /**
     * `VESPA_FEED_COMPRESSION`: `auto` (the client's own default, gzip for any body over 512
     * bytes), `gzip` or `none`. Read strictly: a misspelling fails at startup rather than
     * silently keeping the default.
     */
    private val compression: FeedClientBuilder.Compression = tuning.feedCompressionOrEnv()

    private val built: Lazy<FeedClient> = lazy { build(urls.map { URI.create(it) }) }

    /** The tuned client. Operations go straight through it; only construction and health live here. */
    val client: FeedClient get() = built.value

    init {
        if (eager) built.value
    }

    private fun build(uris: List<URI>): FeedClient =
        FeedClientBuilder
            .create(uris)
            // Every endpoint plaintext (h2c), so TLS is never spoken, but the client builds an
            // SSL context whatever the scheme. Its own builder loads BouncyCastle and verifies the
            // signed jars that ship it: profiled at a relay's boot, about 1 s of a 1.9 s store
            // open. An inert JDK context costs next to nothing.
            .apply { if (uris.all { it.scheme.equals("http", ignoreCase = true) }) setSslContext(PLAINTEXT_ONLY) }
            // Bursty batched ingest idles at the throttle FLOOR (minInflight =
            // 2 x connectionsPerEndpoint) — the throttler never sustains its
            // upward probe. At 8 connections that floor was ~16 in flight,
            // ~1.2k docs/s with the engine ~5x idle; more connections raise the
            // floor AND real HTTP/2 parallelism, and the throttler still adapts
            // DOWN under pushback. Too many starve the client's own Jetty pool
            // on small-core hosts (see [connectionsPerEndpoint]). The defaults
            // are the measured sweet spot for a small-core single-node host.
            .setConnectionsPerEndpoint(connectionsPerEndpoint)
            .setMaxStreamPerConnection(tuning.feedStreamsOrEnv())
            .setCompression(compression)
            // Jetty asks for gzip on every request, so each ~150-byte ack came back gzipped and
            // was inflated with a fresh Inflater and an unpooled 8 KiB buffer per document.
            // A builder header replaces Jetty's own.
            .addRequestHeader("Accept-Encoding", "identity")
            .apply { tuning.feedInflightFactorOrEnv()?.let { setInitialInflightFactor(it) } }
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
        if (!built.isInitialized()) return "feed idle (no writes yet)"
        val s = client.stats()
        // Non-2xx responses get retried and usually succeed (pushback, not
        // loss); only transport exceptions are worth shouting about.
        val retried = s.responses() - s.successes()
        return "feed ok ${s.successes()} inflight ${s.inflight()} lat ${s.averageLatencyMillis()}ms" +
            (if (retried > 0) " retry $retried" else "") +
            if (s.exceptions() > 0) " EXC ${s.exceptions()}" else ""
    }

    /** Feed operations in flight RIGHT NOW — a gauge, not a counter: it has no cumulative form. */
    fun inflight(): Long = if (built.isInitialized()) client.stats().inflight() else 0

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    fun close() {
        if (built.isInitialized()) client.close(true)
    }

    private companion object {
        /**
         * Trusts no certificate and presents none: for a client whose endpoints are all
         * `http://`, where no handshake ever happens. Empty arrays, not nulls, which would load
         * the default trust store for nothing.
         */
        val PLAINTEXT_ONLY: SSLContext by lazy {
            SSLContext.getInstance("TLS").apply { init(arrayOf<KeyManager>(), arrayOf<TrustManager>(), null) }
        }
    }
}
