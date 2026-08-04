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
package com.nosfabrica.vespa.eventstore.vespa.client

import kotlinx.coroutines.delay
import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import java.net.InetAddress
import java.net.Proxy
import java.net.Socket
import java.time.Duration
import javax.net.SocketFactory
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * The READ-side transport to Vespa: how bytes move and fail, with no knowledge
 * of what a request means ([VespaEventIndex]/[VespaReputationIndex] decide
 * that). Owns three transport-correctness decisions: clear-text HTTP/2 by
 * prior knowledge (the only way Vespa serves h2c), no read/call deadline (with
 * HTTP/2 PING liveness instead), and brief retries for transient overload
 * (5xx/429/IOException) so one failed page cannot kill a multi-hour walk.
 */
internal class VespaHttp {
    // Prior-knowledge h2c: Vespa serves h2c only that way, not via the
    // `Upgrade` handshake — a negotiating client (e.g. the JDK HttpClient)
    // silently falls back to HTTP/1.1, one TCP connection per in-flight read.
    // No fallback list: every endpoint here is Vespa.
    private val http =
        OkHttpClient
            .Builder()
            // Every request goes to ONE host, so OkHttp's per-host default of 5
            // was the real ceiling; with no read deadline (below), five wedged
            // requests froze the store permanently (observed: snapshots stalled
            // for minutes, relay idle at 2.5% CPU).
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_REQUESTS
                    maxRequestsPerHost = MAX_CONCURRENT_REQUESTS
                },
            ).protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
            .connectTimeout(Duration.ofSeconds(5))
            .writeTimeout(Duration.ofSeconds(60))
            // NO read/call deadline: an unlimited query may legitimately take as
            // long as it takes, and OkHttp cannot tell "engine still matching"
            // from "connection dead". Dead peers are caught by HTTP/2 PINGs
            // instead — unanswered pings fail the connection as a retryable
            // IOException in seconds.
            .readTimeout(Duration.ZERO)
            .callTimeout(Duration.ZERO)
            .pingInterval(Duration.ofSeconds(PING_INTERVAL_SECONDS))
            // Vespa is local; never route through the egress proxy.
            .proxy(Proxy.NO_PROXY)
            // TCP_NODELAY: OkHttp never sets it, so Nagle stays ON and a
            // multi-segment POST (a 500-id existence query is ~35KB) can stall
            // a delayed-ACK quantum (~40ms) mid-upload. Measured (benchmark
            // transportProbe): 55.6ms -> 10.7ms via docker-proxy; 26.2ms mean /
            // 78ms p95 -> ~10 / 11 direct. Writes are already frame-batched by
            // okio/h2c, so Nagle has nothing to save us from.
            .socketFactory(NoDelaySocketFactory)
            .build()

    /**
     * The visit walk's client: [http] plus a READ timeout, which visits need
     * and queries must not have. Visit pages are small and stream steadily, so
     * silence means a wedged visitor session (measured live: pings stay
     * answered while the response never comes — without a deadline the walk
     * hangs FOREVER). The timeout surfaces the wedge as an IOException the
     * paged retry / streamed resume machinery handles.
     */
    private val visitHttp =
        http
            .newBuilder()
            .readTimeout(Duration.ofSeconds(VISIT_READ_TIMEOUT_SECONDS))
            .build()

    private val jsonMedia = "application/json; charset=utf-8".toMediaType()

    /** GET [url], retrying transient overload — the document-API get path. */
    suspend fun get(url: String): HttpResp =
        send(
            Request
                .Builder()
                .url(url)
                .get()
                .build(),
        )

    /** [get] on the visit client — visit page requests carry a read deadline (see [visitHttp]). */
    suspend fun getVisit(url: String): HttpResp =
        send(
            Request
                .Builder()
                .url(url)
                .get()
                .build(),
            visitHttp,
        )

    /**
     * A raw call on the visit client, for the streamed JSON-Lines walk: the
     * caller runs the blocking read loop and owns the call's lifecycle
     * (cancel on coroutine cancellation). Same read deadline as [getVisit].
     */
    fun newVisitCall(req: Request): Call = visitHttp.newCall(req)

    /** POST [body] as JSON to [url], retrying transient overload — the `/search/` query path. */
    suspend fun postJson(
        url: String,
        body: String,
    ): HttpResp =
        send(
            Request
                .Builder()
                .url(url)
                .post(body.toRequestBody(jsonMedia))
                .build(),
        )

    /**
     * Send [req], briefly retrying transient overload: 5xx (engine load-shed),
     * 429 (document-API pushback past 256 enqueued — not failure), and
     * transport [IOException]s (a body stalling past the read timeout is the
     * same overload arriving as an exception). Shared by every read path — one
     * 504/429 page must not abort a full-corpus walk.
     */
    private suspend fun send(
        req: Request,
        client: OkHttpClient = http,
    ): HttpResp {
        var attempt = 0
        while (true) {
            val resp =
                try {
                    client.newCall(req).await()
                } catch (e: IOException) {
                    if (attempt++ >= QUERY_RETRIES) throw e
                    delay(500L * attempt)
                    continue
                }
            if ((resp.statusCode() in 500..599 || resp.statusCode() == 429) && attempt++ < QUERY_RETRIES) {
                delay(500L * attempt)
                continue
            }
            return resp
        }
    }

    /** Minimal response holder so the get/search/visit/count call sites keep their JDK-style statusCode()/body() shape. */
    internal class HttpResp(
        private val code: Int,
        private val bodyText: String,
    ) {
        fun statusCode() = code

        fun body() = bodyText
    }

    /**
     * Bridge OkHttp's async [Call.enqueue] to a cancellable suspend; the body
     * is read on the callback thread inside [Response.use]. INVARIANT:
     * [onResponse] must complete the continuation on EVERY path — OkHttp never
     * routes an exception thrown there to [onFailure] (only logs it), so an
     * unguarded body read that throws would leave this coroutine suspended
     * forever, hanging the single-writer store.
     */
    private suspend fun Call.await(): HttpResp =
        suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation { runCatching { cancel() } }
            enqueue(
                object : Callback {
                    override fun onFailure(
                        call: Call,
                        e: IOException,
                    ) {
                        if (!cont.isCancelled) cont.resumeWithException(e)
                    }

                    override fun onResponse(
                        call: Call,
                        response: Response,
                    ) {
                        val result = runCatching { response.use { HttpResp(it.code, it.body.string()) } }
                        if (cont.isCancelled) return
                        result
                            .onSuccess { cont.resume(it) }
                            .onFailure { cont.resumeWithException(it) }
                    }
                },
            )
        }

    /** Plain sockets with `tcpNoDelay = true` — see the builder comment. OkHttp calls the no-arg variant and connects the socket itself. */
    private object NoDelaySocketFactory : SocketFactory() {
        private fun on(s: Socket): Socket = s.apply { tcpNoDelay = true }

        override fun createSocket(): Socket = on(Socket())

        override fun createSocket(
            host: String?,
            port: Int,
        ): Socket = on(Socket(host, port))

        override fun createSocket(
            host: String?,
            port: Int,
            localHost: InetAddress?,
            localPort: Int,
        ): Socket = on(Socket(host, port, localHost, localPort))

        override fun createSocket(
            address: InetAddress?,
            port: Int,
        ): Socket = on(Socket(address, port))

        override fun createSocket(
            address: InetAddress?,
            port: Int,
            localAddress: InetAddress?,
            localPort: Int,
        ): Socket = on(Socket(address, port, localAddress, localPort))
    }

    private companion object {
        /** Concurrent engine requests, total and per host — one host, so the per-host limit is the one that binds. */
        const val MAX_CONCURRENT_REQUESTS = 1024

        /** Brief 5xx retries per query (transient engine load-shedding, not correctness). */
        const val QUERY_RETRIES = 3

        /**
         * Visit read deadline ([visitHttp]): pages stream continuously, so this
         * much silence means a wedged visitor session, not a busy engine.
         */
        const val VISIT_READ_TIMEOUT_SECONDS = 120L

        /**
         * Liveness in place of read deadlines: unanswered HTTP/2 PINGs fail the
         * connection with a ProtocolException [send] retries, so a black-holed
         * socket dies in seconds while a legitimately long query runs
         * undisturbed — a distinction a deadline cannot make.
         */
        const val PING_INTERVAL_SECONDS = 15L
    }
}
