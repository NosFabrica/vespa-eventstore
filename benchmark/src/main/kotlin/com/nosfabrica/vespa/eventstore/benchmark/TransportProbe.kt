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
package com.nosfabrica.vespa.eventstore.benchmark

import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.net.InetAddress
import java.net.Socket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import javax.net.SocketFactory

/**
 * Transport isolation for the per-query gap DedupProbe surfaced between the
 * production read stack (OkHttp, h2c prior knowledge) and the identical query
 * over a plain JDK HTTP/1.1 client. Same YQL, same body, same engine — only
 * the client library / protocol / socket options vary, across request-body
 * sizes (id-count), so a request-upload pathology separates from a
 * per-request constant. p50/p95 are printed because the difference under
 * test (Nagle stalls) is TAIL-shaped — a mean hides it on a fast link.
 *
 * The `okhttp-h2c-nodelay` row is the production configuration (VespaHttp
 * sets TCP_NODELAY via a delegating socket factory; OkHttp itself never
 * touches the option, while the JDK client always enables it — which was the
 * one structural difference between the original A/B's clients).
 *
 * Every response's status is checked: a schema without the `dedup` summary
 * class answers 400 fast, which would otherwise masquerade as excellent
 * transport numbers.
 *
 * Env: VESPA_URL (http://localhost:8080), TRANSPORT_REPS (40),
 * TRANSPORT_NS ("5,50,500,2000").
 */
object TransportProbe {
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

    @JvmStatic
    fun main(args: Array<String>) {
        val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
        val reps = System.getenv("TRANSPORT_REPS")?.toIntOrNull() ?: 40

        val jdk = HttpClient.newBuilder().proxy(java.net.ProxySelector.of(null)).build()
        val jsonMedia = "application/json; charset=utf-8".toMediaType()

        fun ok(
            protocols: List<Protocol>,
            noDelay: Boolean = false,
        ) = OkHttpClient
            .Builder()
            .protocols(protocols)
            .proxy(java.net.Proxy.NO_PROXY)
            .apply { if (noDelay) socketFactory(NoDelaySocketFactory) }
            .build()
        val okH2c = ok(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        val okH1 = ok(listOf(Protocol.HTTP_1_1))
        val okH2cNoDelay = ok(listOf(Protocol.H2_PRIOR_KNOWLEDGE), noDelay = true)

        // Some stored ids to query (band 0x1 from the loaded corpus); misses are
        // fine here — transport cost is what's under test, not hit count.
        val ids = { n: Int -> List(n) { "1%063x".format(it.toLong()) } }

        fun body(idList: List<String>): String {
            val vq = EventYql.buildExistence(idList)!!
            return buildJsonObject {
                put("yql", vq.yql)
                put("hits", Int.MAX_VALUE.toString())
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
            }.toString()
        }

        fun viaJdk(payload: String): Int {
            val req =
                HttpRequest
                    .newBuilder(URI.create("$url/search/"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build()
            val resp = jdk.send(req, HttpResponse.BodyHandlers.ofString())
            require(resp.statusCode() < 400) { "jdk ${resp.statusCode()}: ${resp.body().take(200)}" }
            return resp.body().length
        }

        fun viaOk(
            client: OkHttpClient,
            payload: String,
        ): Int {
            val req =
                Request
                    .Builder()
                    .url("$url/search/")
                    .post(payload.toRequestBody(jsonMedia))
                    .build()
            return client.newCall(req).execute().use {
                val text = it.body.string()
                require(it.code < 400) { "okhttp ${it.code}: ${text.take(200)}" }
                text.length
            }
        }

        val clients =
            listOf<Pair<String, (String) -> Int>>(
                "jdk-h1" to { p -> viaJdk(p) },
                "okhttp-h1" to { p -> viaOk(okH1, p) },
                "okhttp-h2c" to { p -> viaOk(okH2c, p) },
                "okhttp-h2c-nodelay" to { p -> viaOk(okH2cNoDelay, p) },
            )

        fun timed(
            call: (String) -> Int,
            payload: String,
        ): Triple<Double, Double, Double> {
            repeat(5) { call(payload) }
            val lat = DoubleArray(reps)
            for (i in 0 until reps) {
                val t0 = System.nanoTime()
                call(payload)
                lat[i] = (System.nanoTime() - t0) / 1e6
            }
            lat.sort()
            return Triple(lat.average(), lat[reps / 2], lat[(reps * 95) / 100 - 1])
        }

        val ns = (System.getenv("TRANSPORT_NS") ?: "5,50,500,2000").split(",").mapNotNull { it.trim().toIntOrNull() }
        println("existence query (select id, dedup summary) per client × id-count; mean/p50/p95 ms")
        println(String.format("%-20s %s", "client", ns.joinToString("") { String.format("%22s", "n=$it") }))
        for ((name, call) in clients) {
            val cells =
                ns.map { n ->
                    val (mean, p50, p95) = timed(call, body(ids(n)))
                    String.format("%7.2f/%5.2f/%6.2f", mean, p50, p95)
                }
            println(String.format("%-20s %s", name, cells.joinToString("") { String.format("%22s", it) }))
        }
        println("(request body sizes: n=5 ≈ ${body(ids(5)).length} B, n=500 ≈ ${body(ids(500)).length} B, n=2000 ≈ ${body(ids(2000)).length} B)")

        // Direction split: a big REQUEST with a near-empty response (all-miss id
        // band) vs a tiny request with a big RESPONSE (match-all limit'd recall).
        val missBody = body(List(500) { "f%063x".format(it.toLong()) })
        val bigResp = """{"yql":"select id from event where true limit 3000","hits":"3000","ranking":"unranked","presentation.summary":"dedup"}"""
        println("\ndirection split (mean/p50/p95 ms): big-request/tiny-response vs tiny-request/big-response")
        println(String.format("%-20s %22s %22s", "client", "req35K/resp~0", "req~0/resp~500K"))
        for ((name, call) in clients) {
            val (am, a50, a95) = timed(call, missBody)
            val (bm, b50, b95) = timed(call, bigResp)
            println(
                String.format(
                    "%-20s %22s %22s",
                    name,
                    String.format("%7.2f/%5.2f/%6.2f", am, a50, a95),
                    String.format("%7.2f/%5.2f/%6.2f", bm, b50, b95),
                ),
            )
        }
        println("(all-miss response ≈ ${viaJdk(missBody)} B, match-all response ≈ ${viaJdk(bigResp)} B)")
    }
}
