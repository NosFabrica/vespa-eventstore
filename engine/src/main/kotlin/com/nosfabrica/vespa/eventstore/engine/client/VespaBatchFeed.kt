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

import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.Serializable

/**
 * PUTS IN BATCHES, through the request handler this store ships in its application package
 * (`:bundle`, `BatchFeedHandler`, bound at [PATH]): one HTTP request carries [batchSize]
 * documents instead of one. On a warm engine most of what a `/document/v1` put costs Vespa is the
 * request around the document; batching 100 per request cut Vespa's CPU per live-relay event by
 * about 31% (benchmark/README.md, "More than one document per request").
 *
 * The handler answers a batch only once every operation in it is answered, so a document it
 * reports stored is stored and visible, the same promise a feed-client ack makes. Anything it
 * does not report stored is handed back to the caller, which re-puts it through the feed client:
 * a put is idempotent, so a document written twice is written once, and the feed client's own
 * retries and the schema-fallback net ([SchemaFallbacks]) stay the one place those decisions are
 * made. A deployment whose application package predates the handler answers 404, and this
 * client stops trying ([available]) for the life of the process.
 */
internal class VespaBatchFeed(
    private val http: VespaHttp,
    private val endpoint: () -> String,
    val batchSize: Int,
    private val concurrency: Int,
) {
    /** False once the endpoint answered 404: the deployed application package has no handler. */
    @Volatile var available = true
        private set

    /** Batches the handler answered, and the documents in them it reported stored. For tests and stats. */
    @Volatile var batchesSent = 0L
        private set

    /**
     * Writes [docs] in batches of [batchSize], [concurrency] in flight, each document as [body]
     * renders it under the document id [docId] gives. Returns the documents NOT confirmed stored:
     * those of a batch that failed whole, or that the handler listed as failed.
     */
    suspend fun putAll(
        docs: List<EventDoc>,
        docId: (EventDoc) -> String,
        body: (EventDoc) -> String,
    ): List<EventDoc> {
        if (!available || docs.isEmpty()) return docs
        val gate = Semaphore(concurrency)
        return coroutineScope {
            docs
                .chunked(batchSize)
                .map { batch -> async { gate.withPermit { send(batch, docId, body) } } }
                .awaitAll()
                .flatten()
        }
    }

    private suspend fun send(
        batch: List<EventDoc>,
        docId: (EventDoc) -> String,
        body: (EventDoc) -> String,
    ): List<EventDoc> {
        if (!available) return batch
        val json = StringBuilder(batch.size * 1024).append('[')
        batch.forEachIndexed { i, doc ->
            if (i > 0) json.append(',')
            // feedJson() is `{"fields":{…}}`: the operation is the same object with the id in front.
            json
                .append("{\"put\":\"id:")
                .append(EVENT_NAMESPACE)
                .append(':')
                .append(EVENT_DOCTYPE)
                .append("::")
                .append(docId(doc))
                .append("\",")
            val fields = body(doc)
            json.append(fields, 1, fields.length)
        }
        json.append(']')
        val resp = runCatching { http.postJson(endpoint() + PATH, json.toString()) }.getOrNull() ?: return batch
        when (resp.statusCode()) {
            200 -> {}

            404 -> {
                available = false
                return batch
            }

            // A 400 is a body the handler would not parse — typically a field the serving schema
            // lacks; the feed client's path answers it document by document and flips the fallbacks.
            else -> {
                return batch
            }
        }
        val answer = runCatching { VESPA_JSON.decodeFromString<BatchAnswer>(resp.body()) }.getOrNull() ?: return batch
        if (answer.count != batch.size) return batch
        batchesSent++
        return answer.failed.map { batch[it.index] }
    }

    @Serializable
    private data class BatchAnswer(
        val count: Int,
        val failed: List<Failure> = emptyList(),
    )

    @Serializable
    private data class Failure(
        val index: Int,
        val outcome: String? = null,
        val message: String? = null,
    )

    companion object {
        /** Where `BatchFeedHandler` is bound (engine/app/services.xml). */
        const val PATH = "/eventstore/v1/feed"
    }
}
