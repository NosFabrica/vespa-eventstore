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

import com.nosfabrica.vespa.eventstore.engine.WriterLockHeld
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.Serializable
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody
import okio.BufferedSink
import okio.utf8Size
import java.math.BigDecimal
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 * PUTS AND UPDATES IN BATCHES, through the request handler this store ships in its application package
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
    /** The `timeout` each batch sent under a writer lock names to the handler; see [TIMEOUT]. A test passes milliseconds. */
    private val timeout: Duration = TIMEOUT,
    /** How long this client waits for a locked batch's answer, headers and body, before its body's share; see [CALL_BOUND]. */
    private val callBound: Duration = CALL_BOUND,
    /** The rate a locked batch's body is granted on top of [callBound]; see [BODY_BYTES_PER_SECOND]. */
    private val bodyBytesPerSecond: Long = BODY_BYTES_PER_SECOND,
) {
    /** The handler's url with [timeout] named, in the seconds form `BatchFeedHandler.timeout` parses (`5s`, `0.2s`). */
    private fun lockedUrl(): String = endpoint() + PATH + "?timeout=" + BigDecimal.valueOf(timeout.toMillis(), 3).stripTrailingZeros().toPlainString() + "s"

    /** How long this client waits for a locked batch of [body]: [callBound], and the body's upload and parse at [bodyBytesPerSecond]. */
    private fun callBoundFor(body: BatchBody): Duration = callBound + Duration.ofMillis(body.bytes * 1_000 / bodyBytesPerSecond)

    /** When the batch path may be tried again, on [clock]; null until a 404 parks it. */
    @Volatile private var parkedUntil: Long? = null

    /** False while a 404 has the batch path parked: the documents go through the feed client instead. */
    val available: Boolean get() = parkedUntil?.let { clock() - it >= 0 } ?: true

    /** When updates may be batched again; null until a handler that predates them refuses a whole batch. */
    @Volatile private var updatesParkedUntil: Long? = null

    /** False while a handler without updates has the update path parked: they go through the feed client. */
    val updatesAvailable: Boolean get() = available && (updatesParkedUntil?.let { clock() - it >= 0 } ?: true)

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
    ): List<EventDoc> =
        sendAll(docs) { batch ->
            val ids = batch.map(docId)
            operations(ids, batch.map(body))
        }

    /**
     * [putAll] with a test-and-set condition on every put ([condition] renders it, e.g.
     * `event.id == "<id>"`), never created: the reindex's re-put ([VespaEventIndex.putAllIfStored]).
     * The handler passes the condition through the JSON feed reader, and a put whose document is
     * missing or fails the condition is answered `CONDITION_FAILED` with nothing written (measured
     * on Vespa 8.781.11). Returns the documents whose condition FAILED, then the ones not confirmed
     * either way (a failed batch, any other outcome), which the caller re-sends by the feed client.
     */
    suspend fun putAllIfStored(
        docs: List<EventDoc>,
        docId: (EventDoc) -> String,
        body: (EventDoc) -> String,
        condition: (EventDoc) -> String,
    ): Pair<List<EventDoc>, List<EventDoc>> {
        val failures = sendAllAnswered(docs) { batch -> operations(batch.map(docId), batch.map(body), batch.map(condition)) }
        val (refused, unconfirmed) = failures.partition { it.second == CONDITION_FAILED }
        return refused.map { it.first } to unconfirmed.map { it.first }
    }

    /**
     * Partial UPDATES in batches, as [putAll] sends puts: each item's update under the full
     * document id [id] gives, its `{"fields": …}` [body], created when absent if [creates]. The
     * handler dispatches a batch's operations at once, so an item must be the only update to its
     * document in the call. Returns the items NOT confirmed applied; an update to a document that
     * does not exist is applied, as a no-op, unless it creates one.
     */
    suspend fun <T> updateAll(
        items: List<T>,
        id: (T) -> String,
        body: (T) -> String,
        creates: (T) -> Boolean,
    ): List<T> {
        if (!updatesAvailable) return items
        return sendAll(items) { batch -> updates(batch.map(id), batch.map(body), batch.map(creates)) }
    }

    /** [items] in batches of [batchSize], [concurrency] in flight, each batch as [request] writes it; returns those not confirmed. */
    private suspend fun <T> sendAll(
        items: List<T>,
        request: (List<T>) -> BatchBody,
    ): List<T> = sendAllAnswered(items, request).map { it.first }

    /**
     * [sendAll], keeping WHY each item was not confirmed: the outcome the handler named for it
     * (`CONDITION_FAILED`, `ERROR`, …), or null when its whole batch failed unanswered.
     */
    private suspend fun <T> sendAllAnswered(
        items: List<T>,
        request: (List<T>) -> BatchBody,
    ): List<Pair<T, String?>> {
        if (!available || items.isEmpty()) return items.map { it to null }
        val gate = Semaphore(concurrency)
        val answered =
            coroutineScope {
                items
                    .chunked(batchSize)
                    .map { batch -> async { gate.withPermit { send(batch, request) } } }
                    .awaitAll()
                    .flatten()
            }
        // A document refused for want of space is the CLUSTER refusing every write (a feed block is
        // cluster-wide): re-putting it through the feed client cannot succeed, and its retries held a
        // refused batch of 100 for 5 s, 1,000 for 11 s, so a relay under a block left most publishes
        // unanswered. Said at once instead, so the caller answers them all now.
        answered.firstOrNull { it.second == INSUFFICIENT_STORAGE }?.let { throw EngineRefusesWrites("the engine is refusing writes ($INSUFFICIENT_STORAGE): its feed is blocked for disk or memory") }
        return answered
    }

    private suspend fun <T> send(
        batch: List<T>,
        request: (List<T>) -> BatchBody,
    ): List<Pair<T, String?>> {
        val unanswered = { batch.map { it to null } }
        if (!available) return unanswered()
        val body = request(batch)
        // Under a writer lock, the batch names [timeout] and is given up on past [callBoundFor]:
        // unanswered by then is an IOException, so the batch is handed back like any other that
        // failed whole, and under the lock that is one try ([VespaHttp.send]). Unlocked (a write
        // outside a store commit), neither: the handler's own default and no client bound, as
        // before the mark was read here. Holding nobody, a slow batch costs no other writer
        // anything, and a bound would only re-POST a batch the handler may still be applying,
        // through send()'s unlocked retries, then re-put it through the feed client.
        val locked = currentCoroutineContext()[WriterLockHeld.Key] != null
        val resp =
            runCatching {
                if (locked) http.post(lockedUrl(), body, callTimeout = callBoundFor(body)) else http.post(endpoint() + PATH, body)
            }.getOrNull() ?: return unanswered()
        when (resp.statusCode()) {
            200 -> {}

            404 -> {
                notFoundCount.incrementAndGet()
                parkedUntil = clock() + retryAfterMs
                return unanswered()
            }

            // A 400 is a body the handler would not parse — typically a field the serving schema
            // lacks; the feed client's path answers it document by document and flips the fallbacks.
            else -> {
                return unanswered()
            }
        }
        val answer = runCatching { resp.decode(VESPA_JSON, BatchAnswer.serializer()) }.getOrNull() ?: return unanswered()
        if (answer.count != batch.size) return unanswered()
        // A handler that predates updates refuses every one before dispatch; asking it again only costs the round trip.
        if (answer.failed.size == batch.size && answer.failed.all { it.message?.startsWith(NO_UPDATES) == true }) {
            updatesParkedUntil = clock() + retryAfterMs
            return unanswered()
        }
        sentCount.incrementAndGet()
        return answer.failed.map { batch[it.index] to it.outcome }
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

    /**
     * A batch's request body, which knows about how many bytes it writes ([bytes]: the UTF-8 of
     * its ids, documents and conditions, without the few bytes of framing per operation), so a
     * locked call's bound can grant its upload and parse ([callBoundFor]). Read only by a locked
     * call, and then once: lazy, so an unlocked batch pays no pass over its documents.
     */
    internal abstract class BatchBody(
        private val parts: () -> Long,
    ) : RequestBody() {
        val bytes: Long by lazy(LazyThreadSafetyMode.NONE) { parts() }
    }

    companion object {
        /** The handler's outcome for a put whose test-and-set condition failed, the document missing included. */
        const val CONDITION_FAILED = "CONDITION_FAILED"

        /** How a `BatchFeedHandler` that takes only puts and removes refuses an update. */
        private const val NO_UPDATES = "only put and remove are supported"

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
            conditions: List<String>? = null,
        ): BatchBody =
            object : BatchBody({ ids.sumOf { it.utf8Size() } + bodies.sumOf { it.utf8Size() } + (conditions?.sumOf { it.utf8Size() } ?: 0L) }) {
                override fun contentType() = JSON_MEDIA

                override fun writeTo(sink: BufferedSink) {
                    sink.writeByte('['.code)
                    for (i in bodies.indices) {
                        if (i > 0) sink.writeByte(','.code)
                        // feedJson() is `{"fields":{…}}`: the operation is the same object with the id in front.
                        // Escaped: an address-keyed id carries the author's own d tag.
                        sink.writeUtf8(OPERATION_PREFIX).writeJsonEscaped(ids[i]).writeUtf8("\",")
                        // A test-and-set condition rides beside the id, as the JSON feed format puts it.
                        conditions?.let { sink.writeUtf8("\"condition\":\"").writeJsonEscaped(it[i]).writeUtf8("\",") }
                        sink.writeUtf8(bodies[i], 1, bodies[i].length)
                    }
                    sink.writeByte(']'.code)
                }
            }

        private const val OPERATION_PREFIX = "{\"put\":\"id:$EVENT_NAMESPACE:$EVENT_DOCTYPE::"

        /**
         * The batch as a JSON array of `{"update": id, "create": …, "fields": …}`: [bodies] are each
         * `{"fields":{…}}`, as `/document/v1` takes an update, and [ids] are full document ids.
         */
        internal fun updates(
            ids: List<String>,
            bodies: List<String>,
            creates: List<Boolean>,
        ): BatchBody =
            object : BatchBody({ ids.sumOf { it.utf8Size() } + bodies.sumOf { it.utf8Size() } }) {
                override fun contentType() = JSON_MEDIA

                override fun writeTo(sink: BufferedSink) {
                    sink.writeByte('['.code)
                    for (i in bodies.indices) {
                        if (i > 0) sink.writeByte(','.code)
                        sink.writeUtf8("{\"update\":\"").writeJsonEscaped(ids[i]).writeUtf8("\",\"create\":${creates[i]},")
                        sink.writeUtf8(bodies[i], 1, bodies[i].length)
                    }
                    sink.writeByte(']'.code)
                }
            }

        private val JSON_MEDIA = "application/json; charset=utf-8".toMediaType()

        /**
         * Five minutes: a handler that arrives with a deploy, or a container back from a restart,
         * is batching again within one interval, and a package that never gets one pays a 404 per
         * interval, one batch re-put through the feed client.
         */
        const val RETRY_AFTER_MS = 300_000L

        /** The handler's outcome for an operation refused because a content node is out of disk or memory. */
        const val INSUFFICIENT_STORAGE = "INSUFFICIENT_STORAGE"

        /**
         * The `timeout` a batch sent under a store writer lock ([WriterLockHeld]) names, which
         * `BatchFeedHandler` passes to every operation in it as their deadline. Without one it
         * waits its `DEFAULT_TIMEOUT`, 30 s (plus 5 s of slack before its 504), while the caller
         * holds the lock and every other writer waits. A batch sent without the mark names none,
         * and keeps that default: nobody is queued behind it.
         *
         * Five seconds: an order of magnitude over the slowest healthy batch writes measured
         * (benchmark/README.md: the `write` stage of a 1,000-event `batchInsert` ~0.5 s on the
         * 400k ingest profile, 255 ms for a 1,000-card batch, a 1,000-document reindex page's
         * batched puts p95 153-180 ms; a small relay batch ~7 ms), and a sixth of the handler's
         * default. What it costs: an engine slower than that for a whole batch (an overloaded
         * single node measured p50 15 s per batch at 32 writers, before the batch handler) has the
         * operations it did not answer re-put through the feed client instead, which under a
         * writer lock is bounded too ([VespaEventIndex.putAll]). Not an operator setting, like
         * [VespaEventIndex.FEED_TIMEOUT] beside it: it is a bound on a failure, not a tuning.
         */
        val TIMEOUT: Duration = Duration.ofSeconds(5)

        /** `BatchFeedHandler`'s wait past the deadline for a reply already on its way, before it answers 504. */
        val HANDLER_SLACK: Duration = Duration.ofSeconds(5)

        /**
         * How long the client waits for a locked batch's answer, before its body's share
         * ([BODY_BYTES_PER_SECOND]): the handler's whole wait ([TIMEOUT] plus [HANDLER_SLACK]) and
         * a second for the answer's way back. This client has no read deadline ([VespaHttp]), so
         * without it a handler that never answers (a container wedged, a connection whose PINGs
         * still pass) held the writer lock for as long as it stayed silent. Unlocked batches have
         * no bound, as they had none before it.
         */
        val CALL_BOUND: Duration = TIMEOUT + HANDLER_SLACK + Duration.ofSeconds(1)

        /**
         * The rate a locked batch's body is granted on top of [CALL_BOUND]: one second per 10 MB.
         *
         * The client's bound starts when OkHttp is handed the call, but `BatchFeedHandler` starts
         * its deadline only once it has read and parsed the whole body, so the upload and the
         * parse come out of the bound's one second of slack. A small batch's do not notice; a
         * hundred kind 3s are megabytes, and `VESPA_FEED_BATCH` allows 4,096 documents a batch.
         * Without a share, a batch whose upload and parse took over a second was given up on
         * while the handler was still going to apply it, then re-put whole.
         *
         * Ten MB/s is a fifth of the rate this client itself writes a feed body (a 9.6 KB kind 3
         * in 183-206 µs, ~50 MB/s, benchmark/README.md "The feed body, written in one pass") and
         * an order of magnitude under the transfer measured through Docker Desktop's port (120-155
         * MB/s). The handler's own parse rate is NOT measured here; this is a margin, not a
         * prediction. What it costs is bounded: a 1 MB batch waits 0.1 s more before it is given
         * up on, a 4,096-document batch of 9.6 KB kind 3s (39 MB) ~3.9 s more.
         */
        const val BODY_BYTES_PER_SECOND = 10_000_000L
    }
}
