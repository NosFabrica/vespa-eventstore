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
package com.nosfabrica.vespa.eventstore.benchmark.probe

import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.security.MessageDigest
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import kotlin.random.Random

/**
 * WHAT THE TRUST COPY COSTS PER CELL, and how its disk grows under churn — the two questions
 * behind production's 4.4 GiB of `reputation` memory and 38 GiB of its disk per node
 * (`docs/author-shards.md`). Against a FRESH Vespa, one configuration per run:
 *
 *  - `BENCH_FOLLOWERS` (`none` | `float` | `bfloat16`): whether each cell also carries a follower
 *    count, and in which cell type (`bfloat16` rewrites `follower_counts` in reputation.sd);
 *  - loads [SUBJECTS] reputation documents, each scored by ~60% of [SERVICES] services;
 *  - reads the reputation document type's documents, memory, disk and transaction log off
 *    `/metrics/v2/values` once settled;
 *  - with `BENCH_CHURN_ROUNDS=n`, rewrites a random tenth of the cells n times (a provider
 *    re-publishing its cards) and reads disk and the transaction log after each round.
 *
 * Prints `RESULT {json}`. Env: BENCH_VESPA_URL, BENCH_CONFIG_URL, BENCH_SUBJECTS (100000),
 * BENCH_SERVICES (60), BENCH_FOLLOWERS (float), BENCH_CHURN_ROUNDS (0).
 */
object TrustFootprintProbe {
    private val http: HttpClient = HttpClient.newHttpClient()

    private fun hex(seed: String): String = MessageDigest.getInstance("SHA-256").digest(seed.encodeToByteArray()).joinToString("") { "%02x".format(it) }

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val configUrl = System.getenv("BENCH_CONFIG_URL") ?: "http://127.0.0.1:19071"
            val subjects = System.getenv("BENCH_SUBJECTS")?.toInt() ?: 100_000
            val serviceCount = System.getenv("BENCH_SERVICES")?.toInt() ?: 60
            val followers = System.getenv("BENCH_FOLLOWERS") ?: "float"
            val rounds = System.getenv("BENCH_CHURN_ROUNDS")?.toInt() ?: 0
            require(followers in setOf("none", "float", "bfloat16")) { "BENCH_FOLLOWERS=$followers" }

            var zip = VespaApp.zipBytes(null)
            if (followers == "bfloat16") zip = rewrite(zip, "schemas/reputation.sd") { it.replace("field follower_counts type tensor<float>(user{})", "field follower_counts type tensor<bfloat16>(user{})") }
            val deployer = SchemaDeployer(configUrl)
            check(!deployer.isServing(url)) { "run against a fresh Vespa" }
            deployer.deployIfAbsent(url, zip)

            val services = List(serviceCount) { ServiceKey(hex("svc-$it")) }
            val keys = List(subjects) { hex("subject-$it") }
            var cells = 0L
            VespaReputationIndex(url).use { rep ->
                keys.chunked(2_000).forEach { chunk ->
                    rep.putAll(
                        chunk.map { k ->
                            val r = Random(k.hashCode())
                            val mine = services.filter { r.nextInt(10) < 6 }
                            cells += mine.size
                            ReputationDoc(
                                k,
                                influenceScores = mine.associateWith { r.nextInt(101) },
                                followerCounts = if (followers == "none") emptyMap() else mine.associateWith { r.nextInt(1, 5_000_000).toDouble() },
                            )
                        },
                    )
                }
                val loaded = settled(url, subjects.toLong())
                println("loaded $subjects subjects, $cells cells: $loaded")

                val churn = ArrayList<Map<String, Long>>()
                val rnd = Random(3)
                repeat(rounds) { round ->
                    val updates =
                        keys.flatMap { k ->
                            services.filter { rnd.nextInt(10) == 0 }.map { s ->
                                ReputationCells(k, s, influence = rnd.nextInt(101), followers = if (followers == "none") null else rnd.nextInt(1, 5_000_000).toDouble())
                            }
                        }
                    updates.chunked(20_000).forEach { rep.updateCells(it) }
                    delay(5_000)
                    val m = reputationMetrics(url)
                    println("churn round ${round + 1}: ${updates.size} cell updates -> $m")
                    churn += m
                }
                println(
                    "RESULT {\"followers\":\"$followers\",\"subjects\":$subjects,\"cells\":$cells," +
                        "\"loaded\":${json(loaded)},\"bytesPerCell\":${loaded.getValue("memory") / cells.coerceAtLeast(1)}," +
                        "\"churn\":[${churn.joinToString(",") { json(it) }}]}",
                )
            }
        }

    /** The reputation type's metrics once the document count reaches [expected] and memory stops moving. */
    private suspend fun settled(
        url: String,
        expected: Long,
    ): Map<String, Long> {
        var last = emptyMap<String, Long>()
        repeat(120) {
            val m = reputationMetrics(url)
            if ((m["documents"] ?: 0) >= expected && m["memory"] == last["memory"]) return m
            last = m
            delay(5_000)
        }
        return last
    }

    private fun reputationMetrics(url: String): Map<String, Long> {
        val body = http.send(HttpRequest.newBuilder(URI.create("$url/metrics/v2/values")).GET().build(), HttpResponse.BodyHandlers.ofString()).body()
        val out = HashMap<String, Long>()
        for (node in Json.parseToJsonElement(body).jsonObject["nodes"]!!.jsonArray) {
            for (service in node.jsonObject["services"]!!.jsonArray) {
                for (metric in service.jsonObject["metrics"]!!.jsonArray) {
                    val dims = metric.jsonObject["dimensions"]?.jsonObject ?: continue
                    if (dims["documenttype"]?.jsonPrimitive?.content != "reputation") continue
                    for ((name, v) in metric.jsonObject["values"]!!.jsonObject) {
                        val key =
                            when (name) {
                                "content.proton.documentdb.documents.total.last" -> "documents"
                                "content.proton.documentdb.memory_usage.allocated_bytes.last" -> "memory"
                                "content.proton.documentdb.disk_usage.last" -> "disk"
                                "content.proton.transactionlog.disk_usage.last" -> "tlog"
                                else -> null
                            } ?: continue
                        out[key] = (out[key] ?: 0) + (
                            v.jsonPrimitive.longOrNull ?: v.jsonPrimitive.content
                                .toDouble()
                                .toLong()
                        )
                    }
                }
            }
        }
        return out
    }

    private fun json(m: Map<String, Long>): String = m.entries.sortedBy { it.key }.joinToString(",", "{", "}") { "\"${it.key}\":${it.value}" }

    private fun rewrite(
        zip: ByteArray,
        target: String,
        transform: (String) -> String,
    ): ByteArray {
        val out = ByteArrayOutputStream()
        ZipOutputStream(out).use { zos ->
            ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
                var e = zis.nextEntry
                while (e != null) {
                    var bytes = zis.readBytes()
                    if (e.name == target) {
                        val before = bytes.decodeToString()
                        val after = transform(before)
                        check(after != before) { "$target: the rewrite matched nothing" }
                        bytes = after.encodeToByteArray()
                    }
                    zos.putNextEntry(ZipEntry(e.name))
                    if (!e.isDirectory) zos.write(bytes)
                    zos.closeEntry()
                    e = zis.nextEntry
                }
            }
        }
        return out.toByteArray()
    }
}
