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
import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.OperationParameters
import com.nosfabrica.vespa.eventstore.vespa.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.vespa.doc.ReputationDoc
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URLEncoder
import java.time.Duration

/**
 * The real [ReputationIndex]: the `reputation` document type over Vespa HTTP —
 * literally the same wiring as [VespaEventIndex]: writes through [VespaFeed],
 * reads through [VespaHttp].
 */
class VespaReputationIndex(
    private val baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
) : ReputationIndex {
    private val feed = VespaFeed(listOf(baseUrl))

    private val http = VespaHttp()

    override suspend fun get(pubkey: String): ReputationDoc? {
        val resp = http.get("$baseUrl/document/v1/$NAMESPACE/$DOCTYPE/docid/$pubkey")
        if (resp.statusCode() == 404) return null
        require(resp.statusCode() < 400) { "vespa reputation get ${resp.statusCode()}: ${resp.body().take(300)}" }
        val fields = Json.parseToJsonElement(resp.body()).jsonObject["fields"]?.jsonObject ?: return null
        return ReputationDoc.fromSummary(fields)
    }

    private fun putOp(reputation: ReputationDoc) =
        feed.client.put(
            DocumentId.of(NAMESPACE, DOCTYPE, reputation.pubkey),
            buildJsonObject { put("fields", reputation.indexFields()) }.toString(),
            feedParams(),
        )

    override suspend fun put(reputation: ReputationDoc) {
        putOp(reputation).await()
    }

    /** All puts stay in flight together — the feed client multiplexes them over HTTP/2. */
    override suspend fun putAll(reputations: List<ReputationDoc>) {
        reputations.map { putOp(it) }.forEach { it.await() }
    }

    /**
     * Pipelined tensor-cell upserts (Vespa `add` update, create-if-missing).
     * `add` overwrites an existing cell and creates absent ones. The feed
     * client keeps per-document ordering, so same-subject updates land in list
     * order, which is exactly the [ReputationIndex.updateCells] contract.
     */
    override suspend fun updateCells(updates: List<ReputationCells>) {
        updates
            .map { u ->
                val fields =
                    buildJsonObject {
                        put("pubkey", buildJsonObject { put("assign", u.subject) })
                        u.influence?.let { q ->
                            put("influence_scores", buildJsonObject { put("add", buildJsonObject { put("cells", buildJsonObject { put(u.observer, q) }) }) })
                        }
                        u.followers?.let { f ->
                            put("follower_counts", buildJsonObject { put("add", buildJsonObject { put("cells", buildJsonObject { put(u.observer, f) }) }) })
                        }
                    }
                feed.client.update(
                    DocumentId.of(NAMESPACE, DOCTYPE, u.subject),
                    buildJsonObject { put("fields", fields) }.toString(),
                    feedParams().createIfNonExistent(true),
                )
            }.forEach { it.await() }
    }

    override suspend fun remove(pubkey: String) {
        feed.client.remove(DocumentId.of(NAMESPACE, DOCTYPE, pubkey), feedParams()).await()
    }

    /**
     * The document-API visit over the reputation corpus, projecting only
     * `pubkey` — the paged walk the orphan sweep runs. Same shape as the event
     * client's paged visit: one continuation chain, the server timing out under
     * the client's read deadline so a slow page returns partial + continuation
     * instead of dying.
     */
    override suspend fun visitPubkeys(onPage: suspend (List<String>) -> Boolean) {
        val base =
            "$baseUrl/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(DOCTYPE, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE" +
                "&fieldSet=${URLEncoder.encode("$DOCTYPE:pubkey", "UTF-8")}" +
                "&timeout=$VISIT_SERVER_TIMEOUT_SECONDS" +
                "&concurrency=$VISIT_CONCURRENCY"
        var continuation: String? = null
        while (true) {
            val resp = http.getVisit(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa reputation visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val env = Json.parseToJsonElement(resp.body()).jsonObject
            val pubkeys =
                env["documents"]?.jsonArray.orEmpty().mapNotNull {
                    it.jsonObject["fields"]
                        ?.jsonObject
                        ?.get("pubkey")
                        ?.jsonPrimitive
                        ?.content
                }
            if (pubkeys.isNotEmpty() && !onPage(pubkeys)) return
            continuation = env["continuation"]?.jsonPrimitive?.content ?: return
        }
    }

    override fun close() = feed.close()

    private companion object {
        const val NAMESPACE = "reputation"
        const val DOCTYPE = "reputation"

        /** Docs asked for per visit response (Vespa's per-request ceiling is 1024). */
        const val VISIT_PAGE = 1024

        /** Server-side visit timeout, strictly under VespaHttp's visit read deadline (see VespaEventIndex.pagedWalk). */
        const val VISIT_SERVER_TIMEOUT_SECONDS = 90L

        /** Backend buckets read in parallel per visit page — the event walk's measured default. */
        const val VISIT_CONCURRENCY = 8

        /** Per-op deadline so a half-dead HTTP/2 connection fails instead of hanging the writer forever (see VespaEventIndex). */
        fun feedParams(): OperationParameters = OperationParameters.empty().timeout(Duration.ofSeconds(30))
    }
}
