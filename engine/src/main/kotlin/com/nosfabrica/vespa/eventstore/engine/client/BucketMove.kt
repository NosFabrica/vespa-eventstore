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
import ai.vespa.feed.client.Result
import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URLEncoder
import java.time.Duration

/**
 * PROTOTYPE: moving author buckets onto NEW content clusters while the store keeps serving
 * (`docs/author-shards.md`, "Moving buckets"). Vespa never moves a document between clusters,
 * so a move is a copy, a check, a switch of who serves, and a delete — each step measured to
 * behave as used here (Vespa 8, `AuthorShardsMoveIT`):
 *
 *  1. Every store switches to [overlap]: the moving buckets keep their owner and are also
 *     written to their destination, which serves nothing yet and so is PENDING — read by no one.
 *     Then the overlap package deploys; it only widens what clusters keep, so the deploy guard
 *     lets it through over data. From here on every new write lands in both clusters.
 *  2. [copy]: each moving bucket's events and reputation documents, read from the source
 *     cluster, put into the destination only where ABSENT (`condition=false`, `create=true`, sent
 *     with `route=<destination>`), so a newer write that already landed there is never
 *     overwritten by the older copy.
 *  3. [diff] with repair, until clean: a write racing the copy can leave the destination short
 *     (a cell update creating a reputation document before the copy reaches it, a delete landing
 *     between the copy's read and its put). The source is complete and keeps receiving every
 *     write, so it is the reference: what the destination lacks or holds differently is copied,
 *     what it holds and the source does not is removed (`route=<destination>`).
 *  4. Every store switches to [to]: the destinations serve. Until step 6 the sources still hold
 *     copies, so a read naming both may meet a document twice; a sharded client drops repeated
 *     hits by id, and a COUNT over-reports for that window.
 *  5. The final package deploys. It NARROWS the sources, which the guard refuses unless told the
 *     copy was verified (`SchemaDeployer.checkShardChange(allowNarrowing = true)`).
 *  6. [cleanup]: the moved documents removed from their source with `route=<source>`, which
 *     reaches that cluster alone whatever its selection now says (measured), instead of waiting
 *     for garbage collection (hourly by default). [remaining] confirms none are left.
 *
 * Holds, per bucket, the destination's ids (and reputation documents) in memory during [diff]:
 * at production scale a bucket is ~1/256 of a cluster, so this is a prototype's shape, not a
 * production tool's. Writes go through the feed client, one document at a time, pipelined.
 */
class BucketMove(
    baseUrl: String,
    /** The map the deployment runs now, at rest. */
    val from: AuthorShards.BucketMap,
    /** The map it moves to, at rest; its new clusters receive the moving buckets. */
    val to: AuthorShards.BucketMap,
    /** Document-API containers; empty = just [baseUrl]. */
    endpoints: List<String> = emptyList(),
    tuning: ClientTuning = ClientTuning(),
) : AutoCloseable {
    /** bucket -> (source cluster, destination cluster). */
    val moves: Map<Int, Pair<Int, Int>> = from.movesTo(to)

    /** The map while the move fills its destinations (step 1). */
    val overlap: AuthorShards.BucketMap = from.overlapTo(to)

    private val api = EndpointRotation(endpoints, baseUrl)

    private val http = VespaHttp()

    private val feed = VespaFeed(api.urls, eager = true, tuning = tuning)

    /** Documents an operation touched, per type. */
    class Counts(
        val events: Long,
        val reputations: Long,
    ) {
        override fun toString(): String = "events=$events reputations=$reputations"
    }

    /** How a destination differs from its source over the moving buckets. */
    class Diff(
        val missing: Long,
        val differing: Long,
        val extra: Long,
    ) {
        val clean: Boolean get() = missing == 0L && differing == 0L && extra == 0L

        override fun toString(): String = "missing=$missing differing=$differing extra=$extra"
    }

    private class Type(
        val namespace: String,
        val doctype: String,
        /** What a selection tests the author by: the event's `pubkey` field, the reputation document's id. */
        val keyField: String,
        /** Compare whole documents in [diff] (reputation: its cells change in place), or ids only (events). */
        val compareFields: Boolean,
    )

    private val types =
        listOf(
            Type(EVENT_NAMESPACE, EVENT_DOCTYPE, "$EVENT_DOCTYPE.pubkey", compareFields = false),
            Type(REPUTATION, REPUTATION, "id.specific", compareFields = true),
        )

    /** Step 2: the moving buckets' documents into their destinations, where absent. */
    suspend fun copy(): Counts =
        counts { type, buckets, src, dst ->
            var n = 0L
            visit(type, src, buckets, "[document]") { docs ->
                // A page's puts in flight together: the feed client pipelines them over HTTP/2.
                n += coroutineScope { docs.map { d -> async { put(type, d, dst, ifAbsent = true) } }.awaitAll() }.count { it }
            }
            n
        }

    /**
     * Step 3: how each destination differs from its source, counting only differences that
     * PERSIST. Under live writes two snapshots read seconds apart disagree on whatever was written
     * between them — and in the overlap every write lands in both clusters, so that is no
     * divergence. Each candidate is therefore read again from both clusters, point reads close
     * together, and counted (and, with [repair], fixed from the source's fresh read) only when it
     * still differs. A race can still slip past a pass; the next pass catches it.
     */
    suspend fun diff(repair: Boolean = false): Diff {
        var missing = 0L
        var differing = 0L
        var extra = 0L
        for ((pair, all) in pairs) {
            val (src, dst) = pair
            for (buckets in all.chunked(BATCH)) {
                for (type in types) {
                    val fieldSet = if (type.compareFields) "[document]" else "[id]"
                    val there = HashMap<String, String>()
                    visit(type, dst, buckets, fieldSet) { docs -> docs.forEach { there[idOf(it)] = canonical(it["fields"]) } }
                    val candidates = ArrayList<String>()
                    visit(type, src, buckets, fieldSet) { docs ->
                        for (d in docs) {
                            val id = idOf(d)
                            val theirs = there.remove(id)
                            if (theirs == null || (type.compareFields && theirs != canonical(d["fields"]))) candidates += id
                        }
                    }
                    candidates += there.keys
                    val confirmed =
                        coroutineScope {
                            candidates
                                .map { id ->
                                    async {
                                        val a = read(type, id, src)
                                        val b = read(type, id, dst)
                                        when {
                                            a != null && b == null -> Fix.MISSING.also { if (repair) put(type, a, dst, ifAbsent = true) }
                                            a != null && type.compareFields && canonical(a["fields"]) != canonical(b!!["fields"]) -> Fix.DIFFERING.also { if (repair) put(type, a, dst, ifAbsent = false) }
                                            a == null && b != null -> Fix.EXTRA.also { if (repair) remove(type, id, dst) }
                                            else -> null
                                        }
                                    }
                                }.awaitAll()
                        }
                    missing += confirmed.count { it == Fix.MISSING }
                    differing += confirmed.count { it == Fix.DIFFERING }
                    extra += confirmed.count { it == Fix.EXTRA }
                }
            }
        }
        return Diff(missing, differing, extra)
    }

    private enum class Fix { MISSING, DIFFERING, EXTRA }

    /** Step 6: the moved documents removed from their sources, which serve them no longer. */
    suspend fun cleanup(): Counts =
        counts { type, buckets, src, _ ->
            var n = 0L
            visit(type, src, buckets, "[id]") { docs -> n += coroutineScope { docs.map { d -> async { remove(type, idOf(d), src) } }.awaitAll() }.count { it } }
            n
        }

    /** The moving buckets' documents still in their sources: zero after [cleanup]. */
    suspend fun remaining(): Counts =
        counts { type, buckets, src, _ ->
            var n = 0L
            visit(type, src, buckets, "[id]") { docs -> n += docs.size }
            n
        }

    /** (source, destination) -> the buckets moving between them. */
    private val pairs: Map<Pair<Int, Int>, List<Int>> = moves.entries.groupBy({ it.value }, { it.key }).mapValues { (_, b) -> b.sorted() }

    /** [per] over every pair's buckets, [BATCH] at a time: one visit per batch, not per bucket, since a visit's selection is evaluated over the whole cluster. */
    private suspend fun counts(per: suspend (Type, List<Int>, Int, Int) -> Long): Counts {
        var events = 0L
        var reputations = 0L
        for ((pair, all) in pairs) {
            for (buckets in all.chunked(BATCH)) {
                events += per(types[0], buckets, pair.first, pair.second)
                reputations += per(types[1], buckets, pair.first, pair.second)
            }
        }
        return Counts(events, reputations)
    }

    /** [buckets]' documents of [type] in [cluster], page by page, projected to [fieldSet]. */
    private suspend fun visit(
        type: Type,
        cluster: Int,
        buckets: List<Int>,
        fieldSet: String,
        onPage: suspend (List<JsonObject>) -> Unit,
    ) {
        val selection = to.bucketTest(type.keyField, buckets)
        val base =
            "${api.next()}/document/v1/${type.namespace}/${type.doctype}/docid" +
                "?cluster=${URLEncoder.encode(AuthorShards.clusterName(cluster), "UTF-8")}" +
                "&selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}" +
                "&wantedDocumentCount=$PAGE&timeout=$SERVER_TIMEOUT_SECONDS"
        var continuation: String? = null
        while (true) {
            val resp = http.getVisit(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "move visit of ${type.doctype} in ${AuthorShards.clusterName(cluster)}: ${resp.statusCode()} ${resp.body().take(300)}" }
            val env = Json.parseToJsonElement(resp.body()).jsonObject
            val docs = env["documents"]?.jsonArray.orEmpty().map { it.jsonObject }
            if (docs.isNotEmpty()) onPage(docs)
            continuation = env["continuation"]?.jsonPrimitive?.content ?: return
        }
    }

    /**
     * [id] as [cluster] holds it now, or null when it holds none. An id that cannot be a URL path
     * segment ([docIdSegment]) reads as present everywhere, so it is never counted as a difference
     * the point reads could not confirm.
     */
    private suspend fun read(
        type: Type,
        id: String,
        cluster: Int,
    ): JsonObject? {
        val segment = docIdSegment(id) ?: return JsonObject(mapOf("id" to JsonPrimitive("id:${type.namespace}:${type.doctype}::$id")))
        val resp = http.get("${api.next()}/document/v1/${type.namespace}/${type.doctype}/docid/$segment?cluster=${URLEncoder.encode(AuthorShards.clusterName(cluster), "UTF-8")}")
        if (resp.statusCode() == 404) return null
        require(resp.statusCode() < 400) { "move read of $id in ${AuthorShards.clusterName(cluster)}: ${resp.statusCode()}" }
        return Json.parseToJsonElement(resp.body()).jsonObject.takeIf { "fields" in it }
    }

    /** Put [doc] into [cluster] alone; [ifAbsent] only where no version exists there. True when it wrote. */
    private suspend fun put(
        type: Type,
        doc: JsonObject,
        cluster: Int,
        ifAbsent: Boolean,
    ): Boolean {
        val fields = (doc["fields"] as? JsonObject)?.let(::writable) ?: return false
        var params = OperationParameters.empty().route(AuthorShards.clusterName(cluster)).timeout(TIMEOUT)
        if (ifAbsent) params = params.testAndSetCondition("false").createIfNonExistent(true)
        val result =
            feed.client
                .put(DocumentId.of(type.namespace, type.doctype, idOf(doc)), "{\"fields\":$fields}", params)
                .await()
        return result.type() == Result.Type.success
    }

    /** Remove [id] from [cluster] alone. True when the engine acknowledged it. */
    private suspend fun remove(
        type: Type,
        id: String,
        cluster: Int,
    ): Boolean =
        feed.client
            .remove(DocumentId.of(type.namespace, type.doctype, id), OperationParameters.empty().route(AuthorShards.clusterName(cluster)).timeout(TIMEOUT))
            .await()
            .type() == Result.Type.success

    override fun close() {
        try {
            feed.close()
        } finally {
            http.close()
        }
    }

    private companion object {
        const val REPUTATION = "reputation"
        const val PAGE = 1024

        /** Buckets per visit: each visit evaluates its selection over the whole source cluster. */
        const val BATCH = 16
        const val SERVER_TIMEOUT_SECONDS = 90L
        val TIMEOUT: Duration = Duration.ofSeconds(30)

        /** The document id's specific part: what follows `id:<namespace>:<type>::`. */
        fun idOf(doc: JsonObject): String = doc["id"]!!.jsonPrimitive.content.substringAfter("::")

        /**
         * [fields] as a put takes them: a visit writes a tensor as `{"type": …, "cells": …}`, and the
         * type belongs to the schema, not the document.
         */
        fun writable(fields: JsonObject): String =
            JsonObject(
                fields.mapValues { (_, v) ->
                    if (v is JsonObject && "type" in v && ("cells" in v || "blocks" in v || "values" in v)) JsonObject(v - "type") else v
                },
            ).toString()

        /** [e] with object keys sorted at every depth, so two equal documents compare equal as strings. */
        fun canonical(e: JsonElement?): String =
            when (e) {
                null -> ""
                is JsonObject -> e.entries.sortedBy { it.key }.joinToString(",", "{", "}") { (k, v) -> "\"$k\":${canonical(v)}" }
                is JsonArray -> e.joinToString(",", "[", "]") { canonical(it) }
                is JsonPrimitive -> e.toString()
            }
    }
}
