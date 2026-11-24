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

import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.query.AuthorFilter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.net.URLEncoder

/**
 * PROTOTYPE author sharding ([AuthorShards]), from the client's side: which content clusters the
 * deployment has, CHECKED against what this client was configured for before anything relies on
 * it, and which of them a query restricted to some authors needs.
 *
 * The check exists because a client that disagrees with the deployment fails silently in the two
 * places it names clusters. A visit over fewer clusters than are deployed skips the rest (a
 * reindex or a trust walk that never sees a shard), and a query restricted by author to the
 * clusters THIS count maps the authors to reads the wrong cluster under another count. Writes
 * and unrestricted queries route server-side and need no check.
 *
 * Asked through the document API, which a reader has as well as a writer: a visit naming a
 * cluster one past this client's last is answered "no content cluster 'contentK', only 'content',
 * …" with every deployed cluster named (measured), and answered with documents when the
 * deployment has that cluster. Checked once, on first use; a deployment that changes its shard
 * count under a running client is the deploy guard's to refuse (`SchemaDeployer.checkShardChange`).
 */
internal class AuthorClusters(
    /** The bucket map this client was configured with ([AuthorShards.BucketMap]). */
    val map: AuthorShards.BucketMap,
    private val http: VespaHttp,
    private val endpoint: () -> String,
) {
    /** One constructor for the even split of [shards], as tests and the bundled single cluster use it. */
    constructor(shards: Int, http: VespaHttp, endpoint: () -> String) : this(AuthorShards.BucketMap.even(shards), http, endpoint)

    /** Every cluster the map names, pending ones included: what the deployment must have. */
    val all: List<String> = AuthorShards.clusters(map.clusters)

    /** The clusters that serve reads. A pending cluster is being filled by a move and is read by no one. */
    val serving: List<String> = map.serving.map(AuthorShards::clusterName)

    val sharded: Boolean get() = map.clusters > 1

    /** The clusters a visit walks: `[null]` (no `cluster` parameter, the request as it always was) on one cluster; else [serving]. */
    val visitClusters: List<String?> = if (sharded) serving else listOf(null)

    @Volatile private var verified = !sharded

    private val lock = Mutex()

    /** Throw unless the deployment's content clusters are exactly [all]; a no-op after the first success, and always on one cluster. */
    suspend fun verify() {
        if (verified) return
        lock.withLock {
            if (verified) return
            val beyond = AuthorShards.clusterName(all.size)
            // The visit client (HTTP/1.1, with a read deadline): this IS a visit request.
            val resp =
                http.getVisit(
                    "${endpoint()}/document/v1/$EVENT_NAMESPACE/$EVENT_DOCTYPE/docid" +
                        "?cluster=${URLEncoder.encode(beyond, "UTF-8")}&wantedDocumentCount=1&fieldSet=${URLEncoder.encode("[id]", "UTF-8")}",
                )
            val deployed = if (resp.statusCode() >= 400) deployedOf(resp.body()) else null
            check(deployed != null) {
                if (resp.statusCode() < 400) {
                    "this client is configured for ${all.size} author-shard clusters, but the deployment also has '$beyond'"
                } else {
                    "cannot read the deployment's content clusters (${resp.statusCode()}: ${resp.body().take(200)})"
                }
            }
            // Exactly the map's clusters, or — while a move starts — its serving ones before the
            // pending cluster deploys: stores switch to the overlap map FIRST, and reading only the
            // serving clusters is right whether the pending one exists yet or not.
            check(deployed.toSet() == all.toSet() || (map.pending.isNotEmpty() && deployed.toSet() == serving.toSet())) {
                "this client is configured for author-shard clusters $all, but the deployment has $deployed; " +
                    "visits and author-scoped queries would miss documents"
            }
            verified = true
        }
    }

    /**
     * The clusters a query with author clauses [filter] reads — its authors' serving clusters — or
     * null to read every cluster (`from event`, the query as written). One cluster is checked FIRST,
     * so a one-cluster store never normalizes an author list. With a move in flight, a query with
     * no author scope still names the [serving] clusters, so the pending copy is never read.
     */
    fun clustersFor(filter: AuthorFilter?): List<String>? {
        if (!sharded) return null
        return clustersFor(filter?.scope())
    }

    /** [clustersFor] over an already-normalized author set. */
    fun clustersFor(authors: Set<String>?): List<String>? {
        if (!sharded) return null
        val everyServing = if (map.pending.isEmpty()) null else serving
        if (authors.isNullOrEmpty()) return everyServing
        val shards = authors.mapTo(sortedSetOf()) { map.shardOf(it) }
        if (shards.size == map.serving.size) return everyServing
        return shards.map(AuthorShards::clusterName)
    }

    companion object {
        private val ONLY = Regex("""no content cluster '[^']*', only (.+)""")
        private val QUOTED = Regex("""'([^']+)'""")

        /** The clusters a "no content cluster 'x', only 'a', 'b'" refusal names; null for any other body. */
        fun deployedOf(body: String): List<String>? =
            ONLY
                .find(body)
                ?.groupValues
                ?.get(1)
                ?.let { list -> QUOTED.findAll(list).map { it.groupValues[1] }.toList() }
                ?.takeIf { it.isNotEmpty() }
    }
}
