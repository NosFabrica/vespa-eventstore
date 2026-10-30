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
import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.OperationParameters
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.JsonChars
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URLEncoder
import java.time.Duration

/**
 * The real [ReputationIndex]: the `reputation` document type over Vespa HTTP —
 * writes through [VespaFeed], reads through [VespaHttp], same wiring as
 * [VespaEventIndex].
 *
 * Every request it makes is a document-API one (put, update, get, visit), so it
 * only ever needs the containers serving `<document-api/>`: on a deployment that
 * splits feed and query containers, [endpoints] are the FEED ones.
 */
class VespaReputationIndex(
    baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
    /** The document-API containers; empty = just [baseUrl]. Requests round-robin across them. */
    endpoints: List<String> = emptyList(),
    /** Build the feed client with the index, or on the first write: [VespaEventIndex]'s `eagerFeed`. */
    eagerFeed: Boolean = true,
    /** The feed settings this index's own feed client is built with — the same ones [VespaEventIndex] takes. */
    tuning: ClientTuning = ClientTuning(),
) : ReputationIndex {
    private val documentApi = EndpointRotation(endpoints, baseUrl)

    private val feed = VespaFeed(documentApi.urls, eager = eagerFeed, tuning = tuning)

    private val http = VespaHttp()

    override suspend fun get(pubkey: String): ReputationDoc? = fields(pubkey)?.let(ReputationDoc::fromSummary)

    private suspend fun fields(pubkey: String): JsonObject? {
        val resp = http.get("${documentApi.next()}/document/v1/$NAMESPACE/$DOCTYPE/docid/$pubkey")
        if (resp.statusCode() == 404 && isAbsentDocument(resp.body())) return null
        require(resp.statusCode() < 400) { "vespa reputation get ${resp.statusCode()}: ${resp.body().take(300)}" }
        return resp.decode(Json, JsonObject.serializer())["fields"]?.jsonObject
    }

    private fun putOp(reputation: ReputationDoc) =
        feed.client.put(
            DocumentId.of(NAMESPACE, DOCTYPE, reputation.pubkey),
            reputation.feedJson(),
            feedParams(),
        )

    override suspend fun put(reputation: ReputationDoc) {
        putOp(reputation).await()
    }

    /** All puts stay in flight together — the feed client multiplexes them over HTTP/2. */
    override suspend fun putAll(reputations: List<ReputationDoc>) {
        // Chunked for the enqueue-time deadline, as VespaEventIndex.putAll is.
        reputations.chunked(VespaEventIndex.FEED_CHUNK).forEach { chunk -> chunk.map { putOp(it) }.forEach { it.await() } }
    }

    /**
     * Pipelined tensor-cell upserts (Vespa `add`, create-if-missing; `add`
     * overwrites existing cells). The feed client keeps per-document ordering,
     * satisfying [ReputationIndex.updateCells]'s list-order contract.
     */
    override suspend fun updateCells(updates: List<ReputationCells>) {
        documentUpdates(updates).chunked(VespaEventIndex.FEED_CHUNK).forEach { chunk -> updateCellsChunk(chunk) }
    }

    /** One document update: its body, and whether it may create the document. */
    private class CellUpdate(
        val subject: String,
        val body: String,
        val creates: Boolean,
    )

    /**
     * GROUPED BY DOCUMENT, as [removeCells] is: one subject's plain adds merge
     * into one update per [MAX_CELLS_PER_UPDATE], a later cell winning as it would in sequence. A subject
     * with a retraction keeps one update per entry, in order, since one field
     * cannot carry both an `add` and a `remove`.
     */
    private fun documentUpdates(updates: List<ReputationCells>): List<CellUpdate> {
        val out = ArrayList<CellUpdate>(updates.size)
        updates.groupBy { it.subject }.forEach { (subject, cells) ->
            if (cells.size > 1 && cells.all { it.isPlainAdd() }) {
                // Pieces of MAX_CELLS_PER_UPDATE, in order: a bulk write-ahead names thousands of
                // subjects, and the feed client's per-document ordering keeps a later cell winning.
                cells.chunked(MAX_CELLS_PER_UPDATE).forEach { piece ->
                    val influence = LinkedHashMap<String, Int>()
                    val followers = LinkedHashMap<String, Double>()
                    piece.forEach { u ->
                        u.influence?.let { influence[u.key.hex] = it }
                        u.followers?.let { followers[u.key.hex] = it }
                    }
                    out += CellUpdate(subject, cellsJson(subject, influence, followers, emptyList(), emptyList()), creates = true)
                }
            } else {
                cells.forEach { u ->
                    // A retracted dimension leaves in the same atomic update.
                    val dropInfluence = listOfNotNull(u.key.hex.takeIf { u.influence == null && u.dropInfluence })
                    val dropFollowers = listOfNotNull(u.key.hex.takeIf { u.followers == null && u.dropFollowers })
                    val influence = u.influence?.let { mapOf(u.key.hex to it) }.orEmpty()
                    val followers = u.followers?.let { mapOf(u.key.hex to it) }.orEmpty()
                    // A retraction alone (both values null) must not conjure a
                    // document: no create, and a missing document is a no-op.
                    out += CellUpdate(subject, cellsJson(subject, influence, followers, dropInfluence, dropFollowers), creates = u.influence != null || u.followers != null)
                }
            }
        }
        return out
    }

    private fun ReputationCells.isPlainAdd(): Boolean = (influence != null || followers != null) && !(influence == null && dropInfluence) && !(followers == null && dropFollowers)

    private suspend fun updateCellsChunk(updates: List<CellUpdate>) {
        updates
            .map { u -> u.creates to feed.client.update(DocumentId.of(NAMESPACE, DOCTYPE, u.subject), u.body, feedParams().createIfNonExistent(u.creates)) }
            .forEach { (creates, op) ->
                if (creates) op.await() else runCatching { op.await() }.onFailure { if (!it.isAbsentDocumentFailure()) throw it }
            }
    }

    /**
     * Pipelined tensor-cell removes (Vespa `remove` by address); a missing
     * document is left missing.
     *
     * GROUPED BY DOCUMENT: a tensor `remove` takes a LIST of addresses, so
     * every cell leaving one document goes in one update, up to [MAX_CELLS_PER_UPDATE]. Callers do send
     * hundreds of cells for a single document — [DirtLedger] retires a whole
     * drain slice out of its marker in one call — and one update per cell
     * there is not merely n round trips but n CONCURRENT updates to the same
     * document id, which Vespa serialises behind each other.
     */
    override suspend fun removeCells(removals: List<CellRemoval>) {
        val byDocument =
            removals
                .groupBy { it.subject }
                // At most MAX_CELLS_PER_UPDATE addresses an update; removes commute, so pieces need no order.
                .flatMap { (subject, cells) -> cells.chunked(MAX_CELLS_PER_UPDATE).map { piece -> subject to (piece.filter { it.influence }.map { it.key } to piece.filter { it.followers }.map { it.key }) } }
        byDocument.chunked(VespaEventIndex.FEED_CHUNK).forEach { chunk ->
            chunk
                .map { (subject, dimensions) ->
                    val (influence, followers) = dimensions
                    val body = cellsJson(subject, emptyMap(), emptyMap(), influence.map { it.hex }, followers.map { it.hex }, assignPubkey = false)
                    feed.client.update(DocumentId.of(NAMESPACE, DOCTYPE, subject), body, feedParams())
                }.forEach { op ->
                    // Nothing to remove from a document that never existed is the intended
                    // outcome. Vespa 8 answers that 200; a Vespa that answered 404 is still
                    // tolerated, in its missing-document shape only.
                    runCatching { op.await() }.onFailure { if (!it.isAbsentDocumentFailure()) throw it }
                }
        }
    }

    /**
     * A partial-update body, one tensor op per field: an `add` of its cells,
     * or else a `remove` of its drop addresses. No caller asks for both.
     */
    private fun cellsJson(
        subject: String,
        influence: Map<String, Int>,
        followers: Map<String, Double>,
        dropInfluence: List<String>,
        dropFollowers: List<String>,
        assignPubkey: Boolean = true,
    ): String {
        val out = JsonChars(128 + (influence.size + followers.size + dropInfluence.size + dropFollowers.size) * 90).raw("{\"fields\":{")
        var first = true
        if (assignPubkey) {
            out
                .key("pubkey", first)
                .raw("{\"assign\":")
                .string(subject)
                .raw('}')
            first = false
        }

        fun field(
            name: String,
            adds: Map<String, *>,
            drops: List<String>,
        ) {
            if (adds.isEmpty() && drops.isEmpty()) return
            out.key(name, first)
            first = false
            if (adds.isNotEmpty()) {
                out.raw("{\"add\":{\"cells\":{")
                var i = 0
                adds.forEach { (key, value) -> out.key(key, i++ == 0).raw(value.toString()) }
                out.raw("}}}")
            } else {
                out.raw("{\"remove\":{\"addresses\":[")
                drops.forEachIndexed { i, key -> out.raw(if (i == 0) "{\"user\":" else ",{\"user\":").string(key).raw('}') }
                out.raw("]}}")
            }
        }
        field("influence_scores", influence, dropInfluence)
        field("follower_counts", followers, dropFollowers)
        return out.raw("}}").toString()
    }

    override suspend fun remove(pubkey: String) {
        feed.client.remove(DocumentId.of(NAMESPACE, DOCTYPE, pubkey), feedParams()).await()
    }

    /** All removes in flight together over HTTP/2, like [putAll]. */
    override suspend fun removeAll(pubkeys: List<String>) {
        pubkeys.chunked(VespaEventIndex.FEED_CHUNK).forEach { chunk ->
            chunk.map { feed.client.remove(DocumentId.of(NAMESPACE, DOCTYPE, it), feedParams()) }.forEach { it.await() }
        }
    }

    /**
     * Document-API visit projecting only `pubkey` — the orphan sweep's paged
     * walk. The server times out under the client's read deadline, so a slow
     * page returns partial + continuation instead of dying.
     */
    override suspend fun visitPubkeys(onPage: suspend (List<String>) -> Boolean) {
        val base =
            "${documentApi.next()}/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(DOCTYPE, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE" +
                "&fieldSet=${URLEncoder.encode("$DOCTYPE:pubkey", "UTF-8")}" +
                "&timeout=$VISIT_SERVER_TIMEOUT_SECONDS" +
                "&concurrency=$VISIT_CONCURRENCY"
        var continuation: String? = null
        while (true) {
            val resp = http.getVisit(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa reputation visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val env = resp.decode(Json, JsonObject.serializer())
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

    override suspend fun visitDocs(onPage: suspend (List<ReputationDoc>) -> Boolean) {
        val base =
            "${documentApi.next()}/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(DOCTYPE, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE" +
                "&timeout=$VISIT_SERVER_TIMEOUT_SECONDS" +
                "&concurrency=$VISIT_CONCURRENCY"
        var continuation: String? = null
        while (true) {
            val resp = http.getVisit(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa reputation visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val env = resp.decode(Json, JsonObject.serializer())
            val docs = env["documents"]?.jsonArray.orEmpty().mapNotNull { it.jsonObject["fields"]?.jsonObject?.let(ReputationDoc::fromSummary) }
            if (docs.isNotEmpty() && !onPage(docs)) return
            continuation = env["continuation"]?.jsonPrimitive?.content ?: return
        }
    }

    override fun close() = feed.close()

    private companion object {
        const val NAMESPACE = "reputation"
        const val DOCTYPE = "reputation"

        /** Cells merged into one update at most: about 90 KB of body, well under a G1 region. */
        const val MAX_CELLS_PER_UPDATE = 1_024

        /** Docs asked for per visit response (Vespa's per-request ceiling is 1024). */
        const val VISIT_PAGE = 1024

        /** Server-side visit timeout, strictly under VespaHttp's visit read deadline (see VespaVisits.pagedWalk). */
        const val VISIT_SERVER_TIMEOUT_SECONDS = 90L

        /** Backend buckets read in parallel per visit page — the event walk's measured default. */
        const val VISIT_CONCURRENCY = 8

        /** Per-op deadline so a half-dead HTTP/2 connection fails instead of hanging the writer forever (see VespaEventIndex). */
        fun feedParams(): OperationParameters = OperationParameters.empty().timeout(Duration.ofSeconds(30))
    }
}
