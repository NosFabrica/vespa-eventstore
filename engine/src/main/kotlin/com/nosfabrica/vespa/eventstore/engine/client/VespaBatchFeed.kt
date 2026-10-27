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
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody
import okio.BufferedSink
import java.util.concurrent.atomic.AtomicLong

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
 * made.
 *
 * A container whose application package has no handler answers 404, and the batch path PARKS
 * for [retryAfterMs] ([available] is false, [statusLine] and the `feed.batch.parked` gauge say
 * so), then the next batch tries again. It used to switch off for the life of the process,
 * silently: a 404 met once (a package that predates the handler, a container back from a restart
 * before its handlers bound, one container of several mid-rollout) cost the ~30% for good, and
 * nothing outside this object could tell. Parking costs a package that never gets the handler
 * one 404'd batch per interval, re-put through the feed client like any refused batch.
 */
internal class VespaBatchFeed(
    private val http: VespaHttp,
    private val endpoint: () -> String,
    val batchSize: Int,
    private val concurrency: Int,
    /** How long a 404 parks the batch path; see the class KDoc. */
    private val retryAfterMs: Long = RETRY_AFTER_MS,
    /**
     * Milliseconds on a MONOTONIC clock. The wall clock steps (an NTP correction, a VM resumed
     * from suspend), and a park measured on it would last its interval plus the step, an hour's
     * step an hour without batching. `nanoTime` has an arbitrary origin and may be negative, so
     * times are only ever subtracted, never compared to a sentinel.
     */
    private val clock: () -> Long = { System.nanoTime() / 1_000_000 },
) {
    /** When the batch path may be tried again, on [clock]; null until a 404 parks it. */
    @Volatile private var parkedUntil: Long? = null

    /** False while a 404 has the batch path parked: the documents go through the feed client instead. */
    val available: Boolean get() = parkedUntil?.let { clock() - it >= 0 } ?: true

    private val notFoundCount = AtomicLong()

    /** 404s answered since this client opened; each one parked the batch path. */
    val notFound: Long get() = notFoundCount.get()

    private val sentCount = AtomicLong()

    /** Batches the handler answered, and the documents in them it reported stored. For tests and stats. */
    val batchesSent: Long get() = sentCount.get()

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
        val bodies = batch.map(body)
        val ids = batch.map(docId)
        val resp = runCatching { http.post(endpoint() + PATH, operations(ids, bodies)) }.getOrNull() ?: return batch
        when (resp.statusCode()) {
            200 -> {}

            404 -> {
                notFoundCount.incrementAndGet()
                parkedUntil = clock() + retryAfterMs
                return batch
            }

            // A 400 is a body the handler would not parse — typically a field the serving schema
            // lacks; the feed client's path answers it document by document and flips the fallbacks.
            else -> {
                return batch
            }
        }
        val answer = runCatching { resp.decode(VESPA_JSON, BatchAnswer.serializer()) }.getOrNull() ?: return batch
        if (answer.count != batch.size) return batch
        sentCount.incrementAndGet()
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

    /**
     * The batch path for a status line: `batch 12 sent`, or `batch PARKED (404 x3), retry in 240s`
     * while a 404 has it parked, so a feed that lost its batching shows it next to the feed
     * client's own line ([VespaFeed.statusLine]).
     */
    fun statusLine(): String {
        val wait = parkedUntil?.let { it - clock() } ?: 0
        return if (wait > 0) "batch PARKED (404 x$notFound), retry in ${(wait + 999) / 1000}s" else "batch $batchesSent sent"
    }

    companion object {
        /** Where `BatchFeedHandler` is bound (engine/app/services.xml). */
        const val PATH = "/eventstore/v1/feed"

        /**
         * The batch as a JSON array of `{"put": id, "fields": …}`, written into the socket document by
         * document: a hundred follow lists are megabytes, which as one string were three humongous
         * copies (the builder, its string, its bytes). Repeatable, as a retried request needs.
         */
        internal fun operations(
            ids: List<String>,
            bodies: List<String>,
        ): RequestBody =
            object : RequestBody() {
                override fun contentType() = JSON_MEDIA

                override fun writeTo(sink: BufferedSink) {
                    sink.writeByte('['.code)
                    for (i in bodies.indices) {
                        if (i > 0) sink.writeByte(','.code)
                        // feedJson() is `{"fields":{…}}`: the operation is the same object with the id in front.
                        // Escaped: an address-keyed id carries the author's own d tag.
                        sink.writeUtf8(OPERATION_PREFIX).writeJsonEscaped(ids[i]).writeUtf8("\",")
                        sink.writeUtf8(bodies[i], 1, bodies[i].length)
                    }
                    sink.writeByte(']'.code)
                }
            }

        private const val OPERATION_PREFIX = "{\"put\":\"id:$EVENT_NAMESPACE:$EVENT_DOCTYPE::"

        private val JSON_MEDIA = "application/json; charset=utf-8".toMediaType()

        /**
         * Five minutes: a handler that arrives with a deploy, or a container back from a restart,
         * is batching again within one interval, and a package that never gets one pays a 404 per
         * interval, one batch re-put through the feed client.
         */
        const val RETRY_AFTER_MS = 300_000L
    }
}
