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
 * The READ-side transport to Vespa: how request bytes move and how they fail,
 * with no knowledge of what any request means. [VespaEventIndex] and
 * [VespaReputationIndex] decide WHAT to ask; this class owns the three
 * transport-correctness decisions:
 *
 *  - clear-text HTTP/2 by PRIOR KNOWLEDGE (Vespa's container serves h2c only
 *    that way, not via the `Upgrade` handshake — a negotiating client silently
 *    falls back to HTTP/1.1 and one TCP connection per in-flight read);
 *  - NO read/call deadline, with HTTP/2 PING liveness instead (an unbounded
 *    query is allowed to take as long as it takes; a dead peer still surfaces
 *    in seconds as a retryable failure);
 *  - brief retries for transient overload (5xx load-shedding, 429 pushback,
 *    transport IOExceptions), so one failed page cannot kill a multi-hour walk.
 */
internal class VespaHttp {
    // OkHttp pinned to clear-text HTTP/2 by PRIOR KNOWLEDGE: Vespa's container
    // serves h2c only via prior knowledge, not the `Upgrade: h2c` handshake the
    // JDK HttpClient uses — so the JDK client silently ran every query on
    // HTTP/1.1 (one TCP connection per in-flight read). Prior-knowledge h2c
    // makes concurrent reads multiplex over a single connection, matching the
    // feed client's write path. No fallback list: every endpoint here is Vespa.
    private val http =
        OkHttpClient
            .Builder()
            // Every request this client makes goes to ONE host, so OkHttp's
            // per-host default of 5 is the real ceiling — not maxRequests. The
            // whole store was capped at five concurrent engine requests, shared
            // by every snapshot visit, every search and every count.
            //
            // With no read deadline (below), five requests that never return
            // wedge the store permanently. Observed: three snapshots frozen at
            // 94k, 2.5M and 3.1M ids with zero visit requests reaching the
            // engine for minutes, the relay idle at 2.5% CPU.
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_REQUESTS
                    maxRequestsPerHost = MAX_CONCURRENT_REQUESTS
                },
            ).protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
            .connectTimeout(Duration.ofSeconds(5))
            .writeTimeout(Duration.ofSeconds(60))
            // NO read or whole-call deadline. A query with no `limit` asks for the
            // whole match set and is allowed to take as long as that takes — any
            // finite deadline here is a duration cap on the CALLER's query, decided
            // by the library, and OkHttp cannot tell "engine still matching" from
            // "connection idle" (both are just no-bytes-yet on the socket).
            //
            // A dead peer is caught without capping duration: HTTP/2 PING frames.
            // Unanswered pings fail the connection, so a black-holed socket still
            // surfaces as a retryable IOException in seconds rather than hanging
            // forever — which is what the deadlines were really there to catch.
            .readTimeout(Duration.ZERO)
            .callTimeout(Duration.ZERO)
            .pingInterval(Duration.ofSeconds(PING_INTERVAL_SECONDS))
            // Vespa is local; never route through the egress proxy.
            .proxy(Proxy.NO_PROXY)
            // TCP_NODELAY. OkHttp never sets it (verified against okhttp-jvm
            // 5.4.0: no reference to the option anywhere), so its sockets keep
            // Java's default — Nagle ON — while the JDK HttpClient turns it off.
            // On this request/response protocol Nagle only ADDS latency: a
            // multi-segment POST (a 500-id existence query is a ~35KB body) can
            // stall a full delayed-ACK quantum (~40ms) mid-upload waiting for an
            // ACK the peer is deliberately withholding. Measured (benchmark
            // transportProbe): a 500-id existence query through docker-proxy
            // drops 55.6ms -> 10.7ms with NODELAY; on a direct link 26.2ms
            // mean / 78ms p95 -> 10.1 / 11.3 — the stall is deterministic
            // behind a userspace proxy hop and tail-shaped on a direct one.
            // Writes are already frame-batched by okio/h2c, so there is no
            // small-write flood for Nagle to be saving us from.
            .socketFactory(NoDelaySocketFactory)
            .build()

    /**
     * The visit walk's client: the query client plus a READ timeout, which
     * visits need and queries must not have. A query is one response that may
     * legitimately take minutes of engine time before its first byte, so the
     * query client carries no read deadline. A visit is the opposite shape:
     * page responses are small and continuous, and a streamed slice delivers
     * lines steadily — silence means a wedged visitor session, not a
     * hard-working engine. Measured live: enough concurrent visitor sessions
     * wedge a small node's document API mid-response, HTTP/2 pings keep the
     * connection "alive" (they are answered; it is the response that never
     * comes), and without a read deadline the walk hangs FOREVER. With one,
     * the wedge surfaces as an IOException that the paged retry / streamed
     * resume machinery handles — recover or fail loudly, never hang.
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
     * Send [req], briefly retrying transient overload: 5xx (the engine sheds
     * load under heavy concurrent summary fills) AND 429 (the document API
     * rejects past 256 enqueued requests — pushback, not failure). Shared by the
     * query, get, and visit paths. The full-corpus visit walk is exactly a place
     * where one 504/429 page must not abort the whole scan.
     *
     * Transport [IOException]s are retried on the same budget. A response body that
     * stalls past the read timeout is the same class of transient overload as a 503
     * — the engine was too busy to finish streaming — and it arrives as an exception
     * rather than a status code, so treating it as fatal would abort a visit walk
     * for a condition the next attempt usually clears.
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
     * Bridge OkHttp's async [Call.enqueue] to a cancellable suspend. The body is
     * read on OkHttp's callback thread (inside [Response.use] so the connection is
     * released), so the whole read stays non-blocking, exactly as the old
     * `sendAsync(...).await()` did.
     *
     * [onResponse] must complete the continuation on EVERY path, including a body
     * read that throws. OkHttp sets its internal `signalledCallback` flag *before*
     * invoking [onResponse], so anything thrown in here is only logged
     * ("Callback failure for call to …", at INFO) and is NEVER routed to
     * [onFailure]. An unguarded `body.string()` that times out mid-stream would
     * therefore leave this coroutine suspended forever — the same
     * hang-behind-the-single-writer-store deadlock that the feed's per-operation
     * timeout guards on the write path, reached by a different door.
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
        /**
         * Concurrent requests to the engine, total and per host — the same
         * number, because every request goes to the same host and the per-host
         * limit is therefore the only one that binds.
         */
        const val MAX_CONCURRENT_REQUESTS = 1024

        /** Brief 5xx retries per query (transient engine load-shedding, not correctness). */
        const val QUERY_RETRIES = 3

        /**
         * Read deadline for visit requests ([visitHttp]): pages are small and
         * streams deliver continuously, so this much silence means a wedged
         * visitor session, not a busy engine. Generous enough for a loaded
         * node's worst honest page.
         */
        const val VISIT_READ_TIMEOUT_SECONDS = 120L

        /**
         * Liveness probe on the read connection, in place of the read/whole-call
         * deadlines a query is no longer allowed to have. Unanswered HTTP/2 PINGs
         * fail the connection with a ProtocolException, which [send] retries — so
         * a severed or black-holed socket is caught in seconds while a
         * legitimately long query runs undisturbed. That distinction is the whole
         * point: a deadline cannot make it, a ping can.
         */
        const val PING_INTERVAL_SECONDS = 15L
    }
}
