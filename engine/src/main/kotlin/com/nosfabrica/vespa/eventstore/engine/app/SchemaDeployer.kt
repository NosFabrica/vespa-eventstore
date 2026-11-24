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
package com.nosfabrica.vespa.eventstore.engine.app

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import java.io.ByteArrayInputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.zip.ZipInputStream

/**
 * Applies the bundled Vespa application package ([VespaApp]) to a running config
 * server — the schema travels with the code, so no out-of-band `vespa deploy`.
 * JDK HTTP client only (no curl/tar), so it works anywhere the store runs.
 */
class SchemaDeployer(
    private val configUrl: String,
    private val http: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build(),
) {
    /**
     * If nothing is serving at [queryUrl], deploy the bundled package and wait
     * until it actually serves. A Vespa already serving an app is left untouched
     * (a schema upgrade is an explicit [deploy], not a silent redeploy on boot).
     * The wait matters: `prepareandactivate` returns before the container serves,
     * and building the feed client in that gap fails its handshake.
     */
    fun deployIfAbsent(
        queryUrl: String,
        /** The package to deploy; [VespaApp.zipBytes] (which reads `VESPA_ACCESS_LOG`) unless the caller built one. */
        zip: ByteArray = VespaApp.zipBytes(),
    ) {
        if (isServing(queryUrl)) return
        deploy(zip)
        awaitServing(queryUrl)
    }

    /** [configUrl] without a trailing slash, so a path appended to it has exactly one. */
    private val root = configUrl.trimEnd('/')

    /**
     * Deploy [zip] unless Vespa already has it active ([serves]), then wait until every service
     * runs it ([awaitConverged]) and the container answers. Returns whether it deployed.
     *
     * A boot that redeploys an unchanged package is not free: `prepareandactivate` took 2-3 s of a
     * relay's start, and it still activates a new session, a new config generation every service
     * reloads, on every restart. Comparing the active package's files is ~80 ms.
     */
    fun deployIfChanged(
        queryUrl: String,
        zip: ByteArray = VespaApp.zipBytes(),
        convergeTimeout: Duration = Duration.ofMinutes(2),
    ): Boolean {
        if (isServing(queryUrl) && serves(zip)) return false
        checkShardChange(queryUrl, zip)
        val generation = generationOf(deploy(zip))
        awaitConverged(generation, convergeTimeout)
        awaitServing(queryUrl)
        return true
    }

    /**
     * Whether the application Vespa has ACTIVE is [zip], file for file and byte for byte, read
     * back through the config server's content API. False on any doubt (no application, a
     * config server that does not answer, a listing it cannot read), since the caller deploys on
     * false and a needless deploy only costs time.
     */
    fun serves(zip: ByteArray = VespaApp.zipBytes()): Boolean =
        runCatching {
            val wanted = files(zip)
            val content = "$root$INSTANCE/content/"
            val active =
                (Json.parseToJsonElement(get("$content?recursive=true").decodeToString()) as JsonArray)
                    .map { it.jsonPrimitive.content }
                    // Directories are listed with a trailing slash; the package is its files.
                    .filterNot { it.endsWith("/") }
                    // Each url names the host the config server thinks it is, which a proxy or a
                    // different spelling of [configUrl] does not match: the path after the content
                    // root is the file, and it is read back through [configUrl] like the listing.
                    .map { url -> url.substringAfter("$INSTANCE/content/", missingDelimiterValue = "") }
            "" !in active && active.size == wanted.size && active.toSet() == wanted.keys &&
                active.all { path -> get(content + path).contentEquals(wanted.getValue(path)) }
        }.getOrDefault(false)

    /**
     * Refuse to deploy [zip] over a serving application when that would MOVE documents: the two
     * split the corpus across different author-shard clusters ([AuthorShards]) and the serving one
     * holds documents. A cluster's selection is also what garbage collection keeps, so the new
     * package would delete every document its selections send elsewhere, and nothing feeds them
     * again. An empty deployment changes its shard count freely; so does one that never involved
     * author sharding on either side (one cluster before and after, whatever its id).
     *
     * An active package that cannot be read while [zip] is sharded is refused too, unless the
     * deployment holds nothing: the guard cannot prove the change safe. Throws
     * [IllegalStateException]; deploys nothing either way.
     */
    fun checkShardChange(
        queryUrl: String,
        zip: ByteArray,
    ) {
        val wanted = files(zip)[SERVICES]?.decodeToString()?.let(VespaApp::contentClusters) ?: return
        val active = activeContentClusters()
        if (active == wanted) return
        if (wanted.size <= 1 && (active == null || active.size <= 1)) return
        val stored = storedDocuments(queryUrl)
        if (stored == 0L) return
        error(
            "refusing to deploy content clusters $wanted over ${active ?: "an unreadable package"} holding " +
                "${stored ?: "an unknown number of"} documents: changing the author-shard count makes Vespa's garbage " +
                "collection delete every document the new selections move to another cluster. Feed a fresh deployment instead " +
                "(docs/author-shards.md, \"Not a migration\")",
        )
    }

    /** The content cluster ids of the application Vespa has ACTIVE, off its services.xml; null when it cannot be read. */
    fun activeContentClusters(): List<String>? = runCatching { VespaApp.contentClusters(get("$root$INSTANCE/content/$SERVICES").decodeToString()) }.getOrNull()

    /** Every document [queryUrl] serves, of every type in every cluster; null when the count cannot be read. */
    fun storedDocuments(queryUrl: String): Long? =
        runCatching {
            val req =
                HttpRequest
                    .newBuilder(URI.create("$queryUrl/search/?yql=${java.net.URLEncoder.encode("select * from sources * where true", "UTF-8")}&hits=0&timeout=10s"))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build()
            val res = http.send(req, HttpResponse.BodyHandlers.ofString())
            require(res.statusCode() == 200) { "count answered ${res.statusCode()}" }
            val root = (Json.parseToJsonElement(res.body()) as JsonObject)["root"] as JsonObject
            ((root["fields"] as JsonObject)["totalCount"]!!.jsonPrimitive).longOrNull
        }.getOrNull()

    /**
     * Wait until every service of the application reports config [generation] or later (the
     * config server's `serviceconverge`): `prepareandactivate` returns once the session is ACTIVE,
     * not once the content nodes and containers run it, and a write carrying a field the old
     * generation lacks is refused in that gap. [generation] null waits for whatever is wanted.
     *
     * Returns false, without throwing, on [timeout]: a service that never takes the generation (a
     * node down) is the operator's to act on, and the previous generation keeps serving meanwhile,
     * so it is logged rather than a boot refused. A change that needs a restart is not that case:
     * measured, one changing the container's `<jvm options>` converged at once, since the service
     * takes the generation and only the JVM flags wait for the restart.
     */
    fun awaitConverged(
        generation: Long?,
        timeout: Duration = Duration.ofMinutes(2),
        poll: Duration = Duration.ofMillis(250),
    ): Boolean {
        val deadlineNanos = System.nanoTime() + timeout.toNanos()
        while (true) {
            if (converged(generation)) return true
            if (System.nanoTime() >= deadlineNanos) {
                System.err.println(
                    "schema: the services at $configUrl did not all reach config generation ${generation ?: "(wanted)"} within $timeout; " +
                        "the previous generation keeps serving where they did not",
                )
                return false
            }
            Thread.sleep(poll.toMillis())
        }
    }

    private fun converged(generation: Long?): Boolean =
        runCatching {
            val state = Json.parseToJsonElement(get("$root$INSTANCE/serviceconverge").decodeToString()) as JsonObject
            val current = state["currentGeneration"]?.jsonPrimitive?.longOrNull ?: return false
            state["converged"]?.jsonPrimitive?.booleanOrNull == true && (generation == null || current >= generation)
        }.getOrDefault(false)

    private fun get(url: String): ByteArray {
        val req =
            HttpRequest
                .newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofByteArray())
        require(res.statusCode() == 200) { "GET $url answered ${res.statusCode()}" }
        return res.body()
    }

    /** Poll [isServing] until an application serves at [queryUrl], or throw after [timeout]. */
    fun awaitServing(
        queryUrl: String,
        timeout: Duration = Duration.ofMinutes(2),
        poll: Duration = Duration.ofSeconds(2),
    ) {
        val deadlineNanos = System.nanoTime() + timeout.toNanos()
        while (true) {
            if (isServing(queryUrl)) return
            require(System.nanoTime() < deadlineNanos) { "Vespa at $queryUrl did not start serving within $timeout after deploy" }
            Thread.sleep(poll.toMillis())
        }
    }

    /** Whether an application is live at [queryUrl] (Vespa answers /ApplicationStatus with 200 once an app is active). */
    fun isServing(queryUrl: String): Boolean =
        runCatching {
            val req =
                HttpRequest
                    .newBuilder(URI.create("$queryUrl/ApplicationStatus"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build()
            http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode() == 200
        }.getOrDefault(false)

    /**
     * POST the [zip] package to prepareandactivate — the same request `vespa
     * deploy` makes. Throws on non-2xx so a failed deploy surfaces instead of a
     * store that silently can't query. Returns the response body.
     */
    fun deploy(zip: ByteArray = VespaApp.zipBytes()): String {
        val req =
            HttpRequest
                .newBuilder(URI.create("$root/application/v2/tenant/default/prepareandactivate"))
                .header("Content-Type", "application/zip")
                .POST(HttpRequest.BodyPublishers.ofByteArray(zip))
                .build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofString())
        require(res.statusCode() in 200..299) { "Vespa schema deploy failed (${res.statusCode()}) at $configUrl: ${res.body()}" }
        return res.body()
    }

    companion object {
        private const val SERVICES = "services.xml"

        /** The one application instance a self-hosted Vespa runs, as the config server's REST paths name it. */
        private const val INSTANCE = "/application/v2/tenant/default/application/default/environment/prod/region/default/instance/default"

        /**
         * The config generation a [deploy] response activated, or null if it does not say. The
         * session id IS the generation the services converge to.
         */
        fun generationOf(deployResponse: String): Long? =
            runCatching {
                (Json.parseToJsonElement(deployResponse) as JsonObject)["session-id"]?.jsonPrimitive?.content?.toLongOrNull()
            }.getOrNull()

        /** A package's files by path; directories carry no bytes and are left out, as the content listing leaves them out. */
        internal fun files(zip: ByteArray): Map<String, ByteArray> =
            buildMap {
                ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
                    var e = zis.nextEntry
                    while (e != null) {
                        if (!e.isDirectory) put(e.name, zis.readBytes())
                        e = zis.nextEntry
                    }
                }
            }
    }
}
