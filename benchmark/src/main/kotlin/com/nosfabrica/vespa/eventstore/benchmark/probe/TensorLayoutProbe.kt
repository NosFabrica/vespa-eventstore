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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.security.MessageDigest
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.random.Random

/**
 * ONE TRUST TENSOR OR TWO: what a reputation document's cells cost in each layout, the same
 * cells fed to each (`docs/author-shards.md`, "The trust copy's size"). `reputation.sd`
 * stores a subject's trust as TWO mapped tensors keyed by service, `influence_scores` (`int8`)
 * and `follower_counts` (`float`), so every (subject, service) address is stored twice. The
 * layouts:
 *
 *  - `two`: today's schema (the bundled `reputation.sd`, rank profile and all);
 *  - `mixed`: one `tensor<float>(user{}, v[2])`, rank and followers under ONE address;
 *  - `mixed-bf16`: the same in `bfloat16`, which holds every rank 0..100 exactly and a follower
 *    count to ~0.4%, enough for the log scale ranking reads it on.
 *
 * Against a FRESH Vespa, one layout per run: a minimal application (this schema alone), then
 * [SUBJECTS] subjects each scored by ~60% of [SERVICES] services, then every attribute's
 * allocated memory read off the search node's state API once memory stops moving, and the whole
 * document type's memory, disk and transaction log off `/metrics/v2/values`.
 * Prints `RESULT {json}`. Env: BENCH_VESPA_URL, BENCH_CONFIG_URL, BENCH_LAYOUT (two),
 * BENCH_SUBJECTS (100000), BENCH_SERVICES (60), BENCH_CONTAINER (vespa-tl).
 */
object TensorLayoutProbe {
    private val http: HttpClient = HttpClient.newHttpClient()

    private fun hex(seed: String): String = MessageDigest.getInstance("SHA-256").digest(seed.encodeToByteArray()).joinToString("") { "%02x".format(it) }

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val configUrl = System.getenv("BENCH_CONFIG_URL") ?: "http://127.0.0.1:19071"
            val layout = System.getenv("BENCH_LAYOUT") ?: "two"
            val subjects = System.getenv("BENCH_SUBJECTS")?.toInt() ?: 100_000
            val serviceCount = System.getenv("BENCH_SERVICES")?.toInt() ?: 60
            val container = System.getenv("BENCH_CONTAINER") ?: "vespa-tl"
            require(layout in setOf("two", "mixed", "mixed-bf16")) { "BENCH_LAYOUT=$layout" }

            val deployer = SchemaDeployer(configUrl)
            check(!deployer.isServing(url)) { "run against a fresh Vespa" }
            deployer.deployIfAbsent(url, app(layout))

            val services = List(serviceCount) { hex("svc-$it") }
            var cells = 0L
            val gate = Semaphore(128)
            val t0 = System.nanoTime()
            withContext(Dispatchers.IO) {
                (0 until subjects).chunked(2_000).forEach { chunk ->
                    chunk
                        .map { i ->
                            val key = hex("subject-$i")
                            val r = Random(key.hashCode())
                            val mine = services.filter { r.nextInt(10) < 6 }.map { s -> Triple(s, r.nextInt(101), r.nextInt(1, 5_000_000)) }
                            cells += mine.size
                            async { gate.withPermit { put(url, key, body(layout, key, mine)) } }
                        }.awaitAll()
                }
            }
            val fed = (System.nanoTime() - t0) / 1e9
            val attrs = settled(container)
            val total = attrs.values.sum()
            val type = settledType(url)
            println(
                "RESULT {\"layout\":\"$layout\",\"subjects\":$subjects,\"cells\":$cells,\"feedSeconds\":${"%.1f".format(fed)}," +
                    "\"attributeBytes\":{${attrs.entries.joinToString(",") { "\"${it.key}\":${it.value}" }}}," +
                    "\"documentType\":{${type.entries.joinToString(",") { "\"${it.key}\":${it.value}" }}},\"typeBytesPerCell\":${"%.1f".format((type["memory"] ?: 0L).toDouble() / cells)}," +
                    "\"trustBytes\":${attrs.filterKeys { it != "pubkey" }.values.sum()},\"totalBytes\":$total," +
                    "\"trustBytesPerCell\":${"%.1f".format(
                        attrs
                            .filterKeys { it != "pubkey" }
                            .values
                            .sum()
                            .toDouble() / cells,
                    )}}",
            )
        }

    /** The feed body: the cells in the layout's own field(s), verbose cell form for the mapped tensors, blocks for the mixed one. */
    private fun body(
        layout: String,
        key: String,
        cells: List<Triple<String, Int, Int>>,
    ): String =
        if (layout == "two") {
            val inf = cells.joinToString(",") { (s, r, _) -> "{\"address\":{\"user\":\"$s\"},\"value\":$r}" }
            val fol = cells.joinToString(",") { (s, _, f) -> "{\"address\":{\"user\":\"$s\"},\"value\":$f}" }
            "{\"fields\":{\"pubkey\":\"$key\",\"influence_scores\":{\"cells\":[$inf]},\"follower_counts\":{\"cells\":[$fol]}}}"
        } else {
            val blocks = cells.joinToString(",") { (s, r, f) -> "\"$s\":[$r,$f]" }
            "{\"fields\":{\"pubkey\":\"$key\",\"trust\":{\"blocks\":{$blocks}}}}"
        }

    private suspend fun put(
        url: String,
        key: String,
        json: String,
    ) {
        val req =
            HttpRequest
                .newBuilder(URI.create("$url/document/v1/reputation/reputation/docid/$key"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build()
        repeat(5) {
            val res = http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).await()
            if (res.statusCode() == 200) return
            if (res.statusCode() < 500 && res.statusCode() != 429) error("put $key: ${res.statusCode()} ${res.body().take(300)}")
            delay(200L * (it + 1))
        }
        error("put $key kept failing")
    }

    /** Every attribute's allocated bytes, once two reads 20 s apart agree (attributes report live, not in snapshots). */
    private suspend fun settled(container: String): Map<String, Long> {
        var last = attributes(container)
        repeat(30) {
            delay(20_000)
            val now = attributes(container)
            if (now.isNotEmpty() && now == last) return now
            last = now
        }
        return last
    }

    /**
     * The whole reputation document type, as `StoreTelemetry` reads it: memory (attributes plus the
     * document store, metadata and caches), document disk and transaction log. Metrics come in
     * ~60 s snapshots, so two reads a snapshot apart must agree.
     */
    private suspend fun settledType(url: String): Map<String, Long> {
        var last = typeMetrics(url)
        repeat(10) {
            delay(65_000)
            val now = typeMetrics(url)
            if (now.isNotEmpty() && now["memory"] == last["memory"]) return now
            last = now
        }
        return last
    }

    private fun typeMetrics(url: String): Map<String, Long> {
        val body = http.send(HttpRequest.newBuilder(URI.create("$url/metrics/v2/values")).GET().build(), HttpResponse.BodyHandlers.ofString()).body()
        val out = java.util.TreeMap<String, Long>()
        for (node in Json.parseToJsonElement(body).jsonObject["nodes"]!!.jsonArray) {
            for (service in node.jsonObject["services"]!!.jsonArray) {
                for (metric in service.jsonObject["metrics"]!!.jsonArray) {
                    val dims = metric.jsonObject["dimensions"]?.jsonObject ?: continue
                    if (dims["documenttype"]?.jsonPrimitive?.content != "reputation") continue
                    for ((name, v) in metric.jsonObject["values"]!!.jsonObject) {
                        val key =
                            when (name) {
                                "content.proton.documentdb.memory_usage.allocated_bytes.last" -> "memory"
                                "content.proton.documentdb.disk_usage.last" -> "disk"
                                "content.proton.transactionlog.disk_usage.last" -> "tlog"
                                else -> null
                            } ?: continue
                        out[key] = (out[key] ?: 0) + (
                            v.jsonPrimitive.content
                                .toDoubleOrNull()
                                ?.toLong() ?: 0L
                        )
                    }
                }
            }
        }
        return out
    }

    private fun attributes(container: String): Map<String, Long> {
        // The tool also prints warnings ("…hard limit: 20000"), so the port is matched, not cut.
        val port =
            Regex(""":(\d+) \(STATE""").find(sh("docker", "exec", container, "bash", "-c", "vespa-model-inspect service searchnode 2>/dev/null"))?.groupValues?.get(1)
                ?: error("no searchnode state port in $container")
        val body = sh("docker", "exec", container, "bash", "-c", "curl -s localhost:$port/state/v1/custom/component/documentdb/reputation/subdb/ready/attribute")
        return Json
            .parseToJsonElement(body)
            .jsonObject
            .mapValues { (_, v) -> v.jsonObject["allocated_bytes"]?.jsonPrimitive?.longOrNull ?: 0L }
            .toSortedMap()
    }

    private fun sh(vararg cmd: String): String {
        val p = ProcessBuilder(*cmd).redirectError(ProcessBuilder.Redirect.DISCARD).start()
        return p.inputStream
            .bufferedReader()
            .readText()
            .also { p.waitFor() }
    }

    /** A minimal application: the reputation type alone, global as production holds it, in [layout]. */
    private fun app(layout: String): ByteArray {
        val schema =
            when (layout) {
                "two" -> {
                    TensorLayoutProbe::class.java.getResourceAsStream("/vespa-app.zip")!!.use { zipEntry(it.readBytes(), "schemas/reputation.sd") }
                }

                else -> {
                    val cell = if (layout == "mixed") "float" else "bfloat16"
                    """
                    schema reputation {
                        document reputation {
                            field pubkey type string {
                                indexing: attribute | summary
                                attribute: fast-search
                            }
                            field trust type tensor<$cell>(user{}, v[2]) {
                                indexing: attribute
                            }
                        }
                        rank-profile trust_cell {
                            inputs {
                                query(user_q) tensor<float>(user{})
                            }
                            first-phase {
                                expression: sum(query(user_q) * attribute(trust){v:0})
                            }
                        }
                    }
                    """.trimIndent()
                }
            }
        val services =
            """
            <?xml version="1.0" encoding="UTF-8" ?>
            <services version="1.0">
              <container id="default" version="1.0"><search /><document-api /><nodes><node hostalias="node1" /></nodes></container>
              <content id="content" version="1.0">
                <redundancy>1</redundancy>
                <documents><document type="reputation" mode="index" global="true" /></documents>
                <nodes><node distribution-key="0" hostalias="node1" /></nodes>
                <tuning><resource-limits><disk>0.985</disk><memory>0.92</memory></resource-limits></tuning>
              </content>
            </services>
            """.trimIndent()
        val out = ByteArrayOutputStream()
        ZipOutputStream(out).use { z ->
            z.putNextEntry(ZipEntry("services.xml"))
            z.write(services.encodeToByteArray())
            z.closeEntry()
            z.putNextEntry(ZipEntry("schemas/reputation.sd"))
            z.write(schema.encodeToByteArray())
            z.closeEntry()
        }
        return out.toByteArray()
    }

    private fun zipEntry(
        zip: ByteArray,
        name: String,
    ): String {
        java.util.zip.ZipInputStream(zip.inputStream()).use { z ->
            var e = z.nextEntry
            while (e != null) {
                if (e.name == name) return z.readBytes().decodeToString()
                e = z.nextEntry
            }
        }
        error("$name not in the bundled package")
    }
}
