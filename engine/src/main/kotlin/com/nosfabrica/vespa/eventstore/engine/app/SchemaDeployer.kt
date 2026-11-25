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
     * Refuse to deploy [zip] over a serving application when that would DELETE documents: a
     * cluster's selection is also what garbage collection keeps, so a package under which some
     * content cluster keeps FEWER author buckets than it does now makes the engine delete every
     * document in the buckets it lost, and nothing feeds them again. Compared by meaning, through
     * the bucket map each sharded package carries ([VespaApp.bucketMapOf]): a schema change, or a
     * deploy that only WIDENS what clusters keep (a new cluster starting to receive buckets it is
     * being filled with), goes through. An empty deployment changes freely.
     *
     * [allowNarrowing] is the move tool's cut-over, made only after it verified that the buckets'
     * new owner holds every document ([com.nosfabrica.vespa.eventstore.engine.app.AuthorShards.BucketMap]).
     * A package whose clusters are not this store's (`content`, `content1`, …) is compared by
     * cluster ids. A content node the active package has and does not mark `retired="true"`,
     * dropped by [zip], is refused the same way and never excused by [allowNarrowing]: at
     * redundancy 1 it is the only copy of what it holds. With no application deployed it passes; with an active one that cannot be read
     * it is judged by the document count alone, so it passes only over an empty deployment.
     * Throws [IllegalStateException]; deploys nothing either way.
     */
    fun checkShardChange(
        queryUrl: String,
        zip: ByteArray,
        allowNarrowing: Boolean = false,
    ) {
        val wantedXml = files(zip)[SERVICES]?.decodeToString() ?: return
        val activeXml =
            when (val read = activePackage()) {
                // Nothing deployed: no cluster keeps anything, so no package can drop it.
                ActivePackage.None -> return

                ActivePackage.Unreadable -> null

                is ActivePackage.Services -> read.xml
            }
        val wanted = keptBy(wantedXml)
        val active = activeXml?.let(::keptBy)
        val lost: Map<String, Set<Int>>? =
            if (active != null && wanted != null) {
                active.mapValues { (cluster, kept) -> kept - wanted[cluster].orEmpty() }.filterValues { it.isNotEmpty() }
            } else {
                // A package that cannot be read is NOT taken for a compatible one (the first cut
                // did, for a one-cluster package): it falls to the document count below.
                val wantedIds = VespaApp.contentClusters(wantedXml)
                val activeIds = activeXml?.let(VespaApp::contentClusters)
                when {
                    activeIds != null -> if (activeIds == wantedIds || (wantedIds.size <= 1 && activeIds.size <= 1)) emptyMap() else null

                    // Unreadable: the container names the clusters it serves. Only one cluster to
                    // one cluster is decided by that alone (it keeps every bucket either way); a
                    // sharded side would need the bucket maps, so the count decides.
                    wantedIds.size <= 1 && servedContentClusters(queryUrl)?.size == 1 -> emptyMap()

                    else -> null
                }
            }
        // A content node the active package has, not retired, that the new one drops: with
        // redundancy 1 it is the only copy of what it holds, whatever the buckets say. Not excused
        // by allowNarrowing, which is about buckets: Vespa's way is to retire it first.
        val dropped: Map<String, List<Int>> =
            activeXml
                ?.let(VespaApp::contentNodes)
                ?.mapValues { (cluster, nodes) ->
                    val kept = VespaApp.contentNodes(wantedXml)[cluster]?.keys.orEmpty()
                    nodes.filter { (key, retired) -> !retired && key !in kept }.keys.sorted()
                }?.filterValues { it.isNotEmpty() }
                .orEmpty()
        if (dropped.isEmpty() && lost != null && (lost.isEmpty() || allowNarrowing)) return
        val stored = storedDocuments(queryUrl)
        if (stored == 0L) return
        // Every reason that applies, buckets first: a shard-count change drops a whole cluster's
        // nodes as well, and the bucket reason is the one that says what was attempted.
        val bucketsLost = lost == null || (lost.isNotEmpty() && !allowNarrowing)
        val reasons = ArrayList<String>()
        if (bucketsLost) {
            reasons +=
                lost?.entries?.joinToString("; ") { (c, b) -> "'$c' would drop ${b.size} bucket(s)" }
                    ?: "content clusters ${VespaApp.contentClusters(wantedXml)} over ${activeXml?.let(VespaApp::contentClusters) ?: "an unreadable package"}"
        }
        if (dropped.isNotEmpty()) reasons += dropped.entries.joinToString("; ") { (c, k) -> "'$c' would drop node(s) $k" }
        val why = ArrayList<String>()
        if (bucketsLost) {
            why += "Changing the author-shard count or bucket map makes Vespa's garbage collection delete every document a cluster stops keeping. " +
                "Feed a fresh deployment, or move the buckets with the move tool (docs/author-shards.md)"
        }
        if (dropped.isNotEmpty()) {
            why += "A content node holding data is the only copy of what it holds at redundancy 1: mark it retired=\"true\" and deploy, " +
                "wait until it holds nothing, then remove it"
        }
        error("refusing to deploy: ${reasons.joinToString("; ")} while the deployment holds ${stored ?: "an unknown number of"} documents. ${why.joinToString(". ")}")
    }

    /** What each content cluster of [servicesXml] keeps, by author bucket; null when the package is not laid out by this store. */
    private fun keptBy(servicesXml: String): Map<String, Set<Int>>? {
        val ids = VespaApp.contentClusters(servicesXml)
        val map = VespaApp.bucketMapOf(servicesXml)
        if (map == null) return if (ids == listOf(AuthorShards.BASE_CLUSTER)) mapOf(AuthorShards.BASE_CLUSTER to (0 until 256).toSet()) else null
        if (ids != AuthorShards.clusters(map.clusters)) return null
        return ids.withIndex().associate { (i, id) -> id to map.bucketsOf(i).toSet() }
    }

    /**
     * The content clusters the container at [queryUrl] serves, read off its refusal of a visit to
     * one that does not exist ([AuthorShards.deployedClustersOf]); null when it does not say.
     */
    private fun servedContentClusters(queryUrl: String): List<String>? =
        runCatching {
            val req =
                HttpRequest
                    .newBuilder(URI.create("$queryUrl/document/v1/event/event/docid?cluster=$NO_SUCH_CLUSTER&wantedDocumentCount=1&fieldSet=%5Bid%5D"))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build()
            AuthorShards.deployedClustersOf(http.send(req, HttpResponse.BodyHandlers.ofString()).body())
        }.getOrNull()

    /** What the config server says is ACTIVE: no application, one it could not hand back, or its services.xml. */
    private sealed interface ActivePackage {
        data object None : ActivePackage

        data object Unreadable : ActivePackage

        class Services(
            val xml: String,
        ) : ActivePackage
    }

    /**
     * The ACTIVE application's services.xml. A deployed package always has one, so a 404
     * `NOT_FOUND` is no application at all (measured, Vespa 8: "Unknown application id"); any
     * other failure is [ActivePackage.Unreadable], which the guard never reads as either.
     */
    private fun activePackage(): ActivePackage =
        runCatching {
            val req =
                HttpRequest
                    .newBuilder(URI.create("$root$INSTANCE/content/$SERVICES"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build()
            val res = http.send(req, HttpResponse.BodyHandlers.ofString())
            when {
                res.statusCode() == 200 -> ActivePackage.Services(res.body())
                res.statusCode() == 404 && "NOT_FOUND" in res.body() -> ActivePackage.None
                else -> ActivePackage.Unreadable
            }
        }.getOrDefault(ActivePackage.Unreadable)

    /** The content cluster ids of the application Vespa has ACTIVE, off its services.xml; null when it cannot be read. */
    fun activeContentClusters(): List<String>? = runCatching { VespaApp.contentClusters(get("$root$INSTANCE/content/$SERVICES").decodeToString()) }.getOrNull()

    /** Every document [queryUrl] serves, of every type in every cluster; null when the count cannot be read, or not from every node. */
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
            // A count is only as good as the nodes that answered it: with the content nodes down the
            // container still answers 200, reading 0 documents. The guard lets anything through
            // over 0, so a count without full coverage, or with errors, is unknown.
            require(root["errors"] == null) { "the count carried errors" }
            val coverage = root["coverage"] as? JsonObject
            require(coverage?.get("full")?.jsonPrimitive?.content == "true") { "the count did not cover every node" }
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

        /** A cluster name no package of this store declares, for [servedContentClusters]. */
        private const val NO_SUCH_CLUSTER = "no-such-cluster"

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
