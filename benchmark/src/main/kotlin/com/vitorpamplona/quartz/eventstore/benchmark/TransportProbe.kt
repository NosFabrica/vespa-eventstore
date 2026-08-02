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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.system.measureNanoTime

/**
 * Transport isolation for the ~40ms/query gap DedupProbe surfaced between the
 * production read stack (OkHttp, h2c prior knowledge) and the identical query
 * over a plain JDK HTTP/1.1 client. Same YQL, same body, same engine — only
 * the client library / protocol varies, across request-body sizes (id-count),
 * so a request-upload pathology separates from a per-request constant.
 *
 * Env: VESPA_URL (http://localhost:8080), TRANSPORT_REPS (40).
 */
object TransportProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
        val reps = System.getenv("TRANSPORT_REPS")?.toIntOrNull() ?: 40

        val jdk = HttpClient.newBuilder().proxy(java.net.ProxySelector.of(null)).build()
        val jsonMedia = "application/json; charset=utf-8".toMediaType()

        fun ok(protocols: List<Protocol>) =
            OkHttpClient
                .Builder()
                .protocols(protocols)
                .proxy(java.net.Proxy.NO_PROXY)
                .build()
        val okH2c = ok(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        val okH1 = ok(listOf(Protocol.HTTP_1_1))

        // Some stored ids to query (band 0x1 from the loaded corpus); misses are
        // fine here — transport cost is what's under test, not hit count.
        val ids = { n: Int -> List(n) { "1%063x".format(it.toLong()) } }

        fun body(n: Int): String {
            val vq = EventYql.buildExistence(ids(n))!!
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
            return jdk.send(req, HttpResponse.BodyHandlers.ofString()).body().length
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
            return client.newCall(req).execute().use { it.body.string().length }
        }

        val clients =
            listOf<Pair<String, (String) -> Int>>(
                "jdk-h1" to { p -> viaJdk(p) },
                "okhttp-h1" to { p -> viaOk(okH1, p) },
                "okhttp-h2c" to { p -> viaOk(okH2c, p) },
            )

        val ns = (System.getenv("TRANSPORT_NS") ?: "5,50,500,2000").split(",").mapNotNull { it.trim().toIntOrNull() }
        println("existence query (select id, dedup summary) per client × id-count; ms/query")
        println(String.format("%-12s %s", "client", ns.joinToString("") { String.format("%10s", "n=$it") }))
        for ((name, call) in clients) {
            val cells =
                ns.map { n ->
                    val payload = body(n)
                    repeat(5) { call(payload) }
                    val nanos = measureNanoTime { repeat(reps) { call(payload) } }
                    nanos / 1e6 / reps
                }
            println(String.format("%-12s %s", name, cells.joinToString("") { String.format("%10.2f", it) }))
        }
        println("(request body sizes: n=5 ≈ ${body(5).length} B, n=500 ≈ ${body(500).length} B, n=2000 ≈ ${body(2000).length} B)")

        // Direction split: a big REQUEST with a near-empty response (all-miss id
        // band) vs a tiny request with a big RESPONSE (match-all limit'd recall).
        fun missBody(n: Int): String {
            val vq = EventYql.buildExistence(List(n) { "f%063x".format(it.toLong()) })!!
            return buildJsonObject {
                put("yql", vq.yql)
                put("hits", Int.MAX_VALUE.toString())
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
            }.toString()
        }

        val bigResp = """{"yql":"select id from event where true limit 3000","hits":"3000","ranking":"unranked","presentation.summary":"dedup"}"""
        println("\ndirection split (ms/query): big-request/tiny-response vs tiny-request/big-response")
        println(String.format("%-12s %14s %14s", "client", "req35K/resp~0", "req~0/resp~500K"))
        for ((name, call) in clients) {
            val m = missBody(500)
            repeat(5) { call(m) }
            val a = measureNanoTime { repeat(reps) { call(m) } } / 1e6 / reps
            repeat(5) { call(bigResp) }
            val b = measureNanoTime { repeat(reps) { call(bigResp) } } / 1e6 / reps
            println(String.format("%-12s %14.2f %14.2f", name, a, b))
        }
        println("(all-miss response ≈ ${viaJdk(missBody(500))} B, match-all response ≈ ${viaJdk(bigResp)} B)")
    }
}
