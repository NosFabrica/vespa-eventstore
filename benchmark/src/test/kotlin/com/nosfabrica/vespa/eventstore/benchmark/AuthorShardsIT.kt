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

import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.eventstore.engine.client.ClientTuning
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * PROTOTYPE author sharding (`docs/author-shards.md`) against a REAL Vespa: the bundled package
 * split into [SHARDS] content clusters by the pubkey's middle byte, everything the mock cannot
 * show because it has one cluster and no routing:
 *
 *  - PLACEMENT: every event lives in the cluster of its author's middle byte and nowhere else,
 *    and so does every reputation document — including the cell UPDATES that create one without
 *    a `pubkey` field (routed by id) and a non-hex marker key (the shard-0 catch-all; without it
 *    Vespa answers 200 and stores nothing);
 *  - the IMPORT: the observer gate reads the author's trust in each cluster from that cluster's
 *    own global copy, so a trusted author in either shard passes and a low one in either drops;
 *  - the MERGES the client owns: a grouping COUNT, the per-author and the distinct-tag groupings
 *    (one root per cluster before [com.nosfabrica.vespa.eventstore.engine.client.GroupingResults.mergeRoots]);
 *  - the VISITS: id walks, the resumable full-document page walk, and the reputation walks each
 *    cover every cluster exactly once (Vespa visits one cluster per request).
 *
 * The rest of the store over this topology is `VespaParityIT` and `FilterMatrixIT` with
 * `-DitAuthorShards=2` ([ItAuthorShards]).
 */
@Tag("integration")
class AuthorShardsIT {
    @Test
    fun `two author shards place, gate, count and walk as one corpus`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the author shards IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}").deployIfAbsent(queryUrl, VespaApp.zipBytes(null, SHARDS))
                val tuning = ClientTuning(authorShards = SHARDS)
                VespaEventIndex(queryUrl, tuning = tuning).use { index ->
                    VespaReputationIndex(queryUrl, tuning = tuning).use { reputation ->
                        runBlocking { exercise(queryUrl, index, reputation) }
                    }
                }
            }
    }

    private suspend fun exercise(
        queryUrl: String,
        index: VespaEventIndex,
        reputation: VespaReputationIndex,
    ) {
        val authors = listOf(TRUSTED_0, LOW_0, TRUSTED_1, LOW_1, UNRANKED_1)
        assertEquals(listOf(0, 0, 1, 1, 1), authors.map { AuthorShards.shardOf(it, SHARDS) }, "the fixture spans both shards")
        val notes = (0 until 60).map { n -> note(n, authors[n % authors.size]) }
        index.putAll(notes)
        // Two ways a reputation document is born: a whole put, and a cell update that creates it
        // and carries no `pubkey` field — the projection's hot path, routed by id alone.
        reputation.putAll(listOf(ReputationDoc(TRUSTED_0, influenceScores = serviceCells(OBSERVER to 50))))
        reputation.updateCells(
            listOf(LOW_0 to 1, TRUSTED_1 to 50, LOW_1 to 1).map { (subject, rank) ->
                ReputationCells(subject, ServiceKey(OBSERVER), influence = rank, followers = null)
            },
        )
        reputation.putAll(listOf(ReputationDoc(MARKER)))
        awaitCount(index, notes.size)

        // ---- placement: each cluster holds exactly its own authors ----
        val placed = (0 until SHARDS).map { visitIds(queryUrl, "event", AuthorShards.clusterName(it), "event:pubkey", "pubkey") }
        for ((shard, pubkeys) in placed.withIndex()) {
            assertTrue(pubkeys.isNotEmpty(), "cluster ${AuthorShards.clusterName(shard)} holds events")
            assertEquals(setOf(shard), pubkeys.map { AuthorShards.shardOf(it, SHARDS) }.toSet(), "events in ${AuthorShards.clusterName(shard)}")
        }
        assertEquals(notes.size, placed.sumOf { it.size }, "every event stored once")
        val reputations = (0 until SHARDS).map { visitIds(queryUrl, "reputation", AuthorShards.clusterName(it), "[id]", null) }
        assertEquals(setOf(TRUSTED_0, LOW_0, MARKER), reputations[0].toSet(), "shard 0's reputation documents, the marker among them")
        assertEquals(setOf(TRUSTED_1, LOW_1), reputations[1].toSet(), "shard 1's reputation documents")
        assertEquals(MARKER, reputation.get(MARKER)?.pubkey, "a non-hex key is stored, not dropped")

        // ---- the import: each cluster gates on its own copy ----
        val newestFirst = notes.sortedWith(compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id })
        assertEquals(
            newestFirst.filter { it.pubkey == TRUSTED_0 || it.pubkey == TRUSTED_1 }.map { it.id },
            index.search(gated()).map { it.id },
            "the gate keeps the trusted author of EACH shard, newest first across both",
        )
        assertEquals(
            newestFirst.take(7).map { it.id },
            index.search(EventQuery(kinds = listOf(1), limit = 7)).map { it.id },
            "an ungated page merges both clusters' newest",
        )

        // ---- the groupings the client merges ----
        assertEquals(notes.size, index.count(EventQuery(kinds = listOf(1))), "a grouping COUNT adds the clusters")
        assertEquals(notes.groupingBy { it.pubkey }.eachCount(), index.countByAuthor(EventQuery(kinds = listOf(1))), "per-author counts")
        assertEquals(
            setOf("shared", "only-0", "only-1"),
            index.distinctTagIndexValues(EventQuery(kinds = listOf(1)), "t"),
            "a tag value both clusters hold, and one each",
        )

        // ---- the visits: every cluster, once ----
        val walked = ArrayList<String>()
        index.visitIds(EventQuery(kinds = listOf(1))) { page ->
            walked += page.map { it.id }
            true
        }
        assertEquals(notes.map { it.id }.sorted(), walked.sorted(), "an id walk covers both clusters exactly once")
        val paged = ArrayList<String>()
        var token: String? = null
        var pages = 0
        do {
            val page = index.visitDocsPage(EventQuery(kinds = listOf(1)), token, maxDocs = 7)
            paged += page.docs.map { it.id }
            token = page.continuation
            check(++pages < 1_000) { "the page walk never ended" }
        } while (token != null)
        assertEquals(notes.map { it.id }.sorted(), paged.sorted(), "the resumable page walk crosses into the second cluster")
        val visitedReputations = ArrayList<String>()
        reputation.visitPubkeys { page ->
            visitedReputations += page
            true
        }
        assertEquals(setOf(TRUSTED_0, LOW_0, MARKER, TRUSTED_1, LOW_1), visitedReputations.toSet(), "the reputation walk covers both clusters")
        assertEquals(visitedReputations.size, visitedReputations.toSet().size, "and visits each document once")
    }

    private fun gated() = EventQuery(kinds = listOf(1), limit = null, ranking = EventYql.RANK_RECENCY_GATED, minRank = 2.0, observer = OBSERVER, rankKey = OBSERVER, followersKey = OBSERVER)

    /** A note by [pubkey], tagged `shared` and its shard's own value. */
    private fun note(
        n: Int,
        pubkey: String,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = 1_700_000_000L + n,
        kind = 1,
        tags = listOf(listOf("t", "shared"), listOf("t", "only-${AuthorShards.shardOf(pubkey, SHARDS)}")),
        content = "note $n",
        sig = "e".repeat(128),
        search = SearchFields(text = "note $n"),
    )

    /** One cluster's documents, read straight off the document API: [field] of each, or the id's specific part. */
    private fun visitIds(
        queryUrl: String,
        type: String,
        cluster: String,
        fieldSet: String,
        field: String?,
    ): List<String> {
        val out = ArrayList<String>()
        var continuation: String? = null
        do {
            val url =
                "$queryUrl/document/v1/$type/$type/docid?cluster=$cluster&wantedDocumentCount=1024" +
                    "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}" + (continuation?.let { "&continuation=$it" } ?: "")
            val body = HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString())
            check(body.statusCode() < 400) { "visit $cluster: ${body.statusCode()} ${body.body().take(300)}" }
            val env = Json.parseToJsonElement(body.body()).jsonObject
            env["documents"]?.jsonArray.orEmpty().forEach { doc ->
                val o = doc.jsonObject
                out += if (field == null) o["id"]!!.jsonPrimitive.content.substringAfter("::") else o["fields"]!!.jsonObject[field]!!.jsonPrimitive.content
            }
            continuation = env["continuation"]?.jsonPrimitive?.content
        } while (continuation != null)
        return out
    }

    private suspend fun awaitCount(
        index: VespaEventIndex,
        expected: Int,
    ) {
        repeat(120) {
            if (index.search(EventQuery(kinds = listOf(1))).size >= expected) return
            delay(500)
        }
        error("corpus never became searchable ($expected docs)")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val SHARDS = 2

        val HTTP: HttpClient = HttpClient.newHttpClient()

        val OBSERVER = "c".repeat(64)

        /** A key whose middle byte ([AuthorShards.BUCKET_BYTE]) is [middle], ends left as a vanity grinder might set them. */
        fun key(
            middle: String,
            tail: Char,
        ) = "0".repeat(32) + middle + tail.toString().repeat(30)

        val TRUSTED_0 = key("10", '1')
        val LOW_0 = key("7f", '2')
        val TRUSTED_1 = key("80", '3')
        val LOW_1 = key("f0", '4')
        val UNRANKED_1 = key("ff", '5')

        /** A trust-ledger marker: not hex where the shard is read, so it is shard 0's. */
        const val MARKER = "dirt-ledger-marker"

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
