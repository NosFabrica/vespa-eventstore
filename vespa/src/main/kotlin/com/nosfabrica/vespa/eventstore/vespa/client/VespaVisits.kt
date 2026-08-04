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

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.decodeFromString
import okhttp3.Request
import java.io.IOException
import java.net.URLEncoder

/**
 * The document-API visit: a streaming, selection-filtered scan with
 * continuation tokens — the walk behind every full-corpus read (sync
 * snapshots, tag discovery, reindex).
 *
 * Two transports, chosen by what the A/B against a live corpus measured:
 * [slices] STREAMED slices walked concurrently (2.2x the serial paged walk;
 * the plateau was the node's cores, not the transport), or ONE serial paged
 * walk when streaming is off or the server doesn't speak JSON Lines. The
 * paged fallback is deliberately UNSLICED: sliced paged requests return
 * roughly one small bucket per round trip instead of a full page — measured
 * 11x SLOWER than the serial walk. Slicing pays only when the slice streams.
 */
internal class VespaVisits(
    private val http: VespaHttp,
    /** The endpoint for one HTTP read — the owner round-robins across the cluster. */
    private val endpoint: () -> String,
    /** Concurrent streamed slices (document-API `slices`/`sliceId`); see [VespaEventIndex]'s constructor for the sizing rationale. */
    private val slices: Int,
    /** Backend bucket parallelism per paged request; used only by the paged fallback (streamed slices pin it to 1 for exactly-once resume). */
    private val bucketConcurrency: Int,
    /** Whether to try the streamed JSON-Lines transport at all (`VESPA_VISIT_STREAM=0` forces the paged fallback). */
    private val streaming: Boolean,
) {
    /**
     * Page every match of [selection] out of the visit, calling [onDocuments]
     * with lists of lean [VisitedDoc]s; a false return stops the walk.
     * Producer slices meet a single consumer through a channel, so
     * [onDocuments] runs strictly serially (callers mutate plain collections)
     * and an early stop cancels every in-flight slice.
     */
    suspend fun pages(
        selection: String,
        fieldSet: String,
        onDocuments: suspend (List<VisitedDoc>) -> Boolean,
    ): Unit =
        coroutineScope {
            val pages = Channel<List<VisitedDoc>>(slices)
            val producers =
                launch {
                    try {
                        val streamed =
                            streaming &&
                                coroutineScope {
                                    (0 until slices)
                                        .map { sliceId -> async { streamedSlice(selection, fieldSet, sliceId) { pages.send(it) } } }
                                        .awaitAll()
                                }.also { oks ->
                                    // A server either speaks JSON Lines or doesn't — for
                                    // every slice alike. A mixed answer means some slice
                                    // DELIVERED while another wants the whole walk redone
                                    // as paged, which would duplicate; refuse loudly.
                                    check(oks.all { it } || oks.none { it }) { "vespa answered JSON Lines for only some visit slices" }
                                }.all { it }
                        if (!streamed) pagedWalk(selection, fieldSet) { pages.send(it) }
                    } finally {
                        // Close on completion AND failure: a stuck-open channel
                        // would leave the consumer below suspended forever.
                        pages.close()
                    }
                }
            for (page in pages) {
                if (!onDocuments(page)) {
                    producers.cancelAndJoin()
                    break
                }
            }
        }

    /**
     * One page of FULL docs — the reindex primitive. `[document]` selects every
     * real document field (the search columns included: they are stored
     * fields), so the page decodes through the same [VespaSummary] a get
     * returns. The continuation token is opaque to callers.
     */
    suspend fun docsPage(
        selection: String,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage {
        val base =
            "${endpoint()}/document/v1/$EVENT_NAMESPACE/$EVENT_DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=${maxDocs.coerceIn(1, VISIT_PAGE)}" +
                "&fieldSet=${URLEncoder.encode("[document]", "UTF-8")}" +
                "&timeout=$SERVER_TIMEOUT_SECONDS" +
                "&concurrency=$bucketConcurrency"
        val resp = http.getVisit(resumeFrom?.let { "$base&continuation=$it" } ?: base)
        require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
        val env = VESPA_JSON.decodeFromString<DocVisitEnvelope>(resp.body())
        return DocsPage(env.documents.mapNotNull { it.fields?.toDoc(withNearState = true) }, env.continuation)
    }

    /**
     * The serial paged walk: each round trip returns up to [VISIT_PAGE] docs
     * plus a continuation token, with the backend reading [bucketConcurrency]
     * buckets in parallel to fill each page.
     */
    private suspend fun pagedWalk(
        selection: String,
        fieldSet: String,
        emit: suspend (List<VisitedDoc>) -> Unit,
    ) {
        val base =
            "${endpoint()}/document/v1/$EVENT_NAMESPACE/$EVENT_DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE" +
                "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}" +
                // UNDER the client's read deadline: a sparse selection can
                // honestly spend ages filling a page, and the server's default
                // (180s) outlives the client's 120s read timeout — the client
                // would kill and retry the identical request forever. With the
                // server timing out first, it returns a partial page plus a
                // continuation and the walk keeps moving.
                "&timeout=$SERVER_TIMEOUT_SECONDS" +
                "&concurrency=$bucketConcurrency"
        var continuation: String? = null
        while (true) {
            val resp = http.getVisit(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val env = VESPA_JSON.decodeFromString<PagedVisitEnvelope>(resp.body())
            if (env.documents.isNotEmpty()) emit(env.documents.map { VisitedDoc(it.id, it.fields) })
            continuation = env.continuation ?: return
        }
    }

    /** How one streamed response ended — see [streamedSlice] for what each means to the resume loop. */
    private enum class StreamEnd { COMPLETE, INTERRUPTED, NOT_JSONL }

    /**
     * Walk one slice as a single streamed JSON-Lines response (`stream=true` +
     * `Accept: application/jsonl`): put lines arrive as the backend visits,
     * with NO per-page round trip. Returns false (nothing consumed, nothing
     * emitted) when the server doesn't answer in JSON Lines, so the caller can
     * fall back to the paged walk against an older Vespa.
     *
     * EXACTLY-ONCE across broken streams: Vespa emits a `continuation` line
     * whenever a backend bucket completes, and resuming from a token
     * re-streams every bucket still ACTIVE when the stream broke. Bucket
     * concurrency is pinned to 1 precisely so "active" is at most ONE bucket:
     * docs are buffered until their bucket's continuation line certifies them,
     * so a broken stream drops the uncertified buffer and resumes from the
     * last token — nothing delivered twice, nothing lost. The retry budget
     * counts CONSECUTIVE failures (progress resets it), so a dead engine still
     * fails loudly instead of looping.
     */
    private suspend fun streamedSlice(
        selection: String,
        fieldSet: String,
        sliceId: Int,
        emit: suspend (List<VisitedDoc>) -> Unit,
    ): Boolean {
        val base =
            "${endpoint()}/document/v1/$EVENT_NAMESPACE/$EVENT_DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}" +
                "&stream=true&concurrency=1&slices=$slices&sliceId=$sliceId"
        var token: String? = null
        var delivered = false
        var failures = 0
        // Docs since the last continuation line: certain to be exactly the one
        // in-flight bucket (concurrency=1), so at most a few hundred docs.
        val uncertified = ArrayList<VisitedDoc>()

        suspend fun certify(upToToken: String?) {
            if (uncertified.isNotEmpty()) {
                uncertified.chunked(VISIT_PAGE).forEach { emit(it) }
                uncertified.clear()
                delivered = true
            }
            if (upToToken != null) {
                token = upToToken
                failures = 0
            }
        }

        while (true) {
            val url = token?.let { "$base&continuation=$it" } ?: base
            uncertified.clear()
            var thrown: IOException? = null
            val end =
                try {
                    streamOnce(url, into = uncertified, onContinuation = { certify(it) })
                } catch (e: IOException) {
                    currentCoroutineContext().ensureActive()
                    thrown = e
                    StreamEnd.INTERRUPTED
                }
            when (end) {
                StreamEnd.COMPLETE -> {
                    // A conforming stream has nothing uncertified after its
                    // final marker; if put lines ever trail it, deliver them
                    // rather than silently dropping real docs.
                    certify(null)
                    return true
                }

                // Resume from the last certified token — on a BUDGET even when
                // the stream ended without an exception (a clean close before
                // the final marker): with no token advance, an unbudgeted loop
                // would replay the identical request forever.
                StreamEnd.INTERRUPTED -> {
                    if (++failures > STREAM_RETRIES) {
                        throw thrown ?: IOException("vespa streamed visit slice $sliceId kept ending without progress")
                    }
                    delay(500L * failures)
                }

                StreamEnd.NOT_JSONL -> {
                    // Only a fallback signal while nothing has been consumed;
                    // mid-walk it is a server misbehaving, not a version gap.
                    check(token == null && !delivered) { "vespa streamed visit refused mid-walk (400 or non-JSONL answer after progress)" }
                    return false
                }
            }
        }
    }

    /**
     * One streamed request: open, read JSON-Lines until the stream ends, and
     * classify how it ended. Put lines land in [into]; each continuation line
     * fires [onContinuation] with its token (null on the final 100% marker,
     * which certifies the tail). Blocking reads run on [Dispatchers.IO]; a
     * cancelled coroutine aborts the in-flight call via the guard child, which
     * is the only thing that unblocks a socket read.
     */
    private suspend fun streamOnce(
        url: String,
        into: MutableList<VisitedDoc>,
        onContinuation: suspend (String?) -> Unit,
    ): StreamEnd =
        withContext(Dispatchers.IO) {
            val call =
                http.newVisitCall(
                    Request
                        .Builder()
                        .url(url)
                        .header("Accept", "application/jsonl")
                        .get()
                        .build(),
                )
            // The guard aborts the blocking read when this coroutine is
            // cancelled; after normal completion its cancel() is a no-op.
            // UNCONFINED, not the surrounding IO dispatcher: the abort must not
            // wait for an IO thread when every IO thread is parked in these
            // blocking reads — unconfined runs the finally on the cancelling
            // thread immediately.
            val guard =
                launch(Dispatchers.Unconfined, start = CoroutineStart.UNDISPATCHED) {
                    try {
                        awaitCancellation()
                    } finally {
                        call.cancel()
                    }
                }
            try {
                call.execute().use { resp ->
                    if (resp.code >= 400) {
                        val preview = runCatching { resp.body.string().take(300) }.getOrDefault("")
                        // 400 = the server refused the request shape (no
                        // stream support) -> fall back; 5xx/429 = transient.
                        if (resp.code == 400) return@use StreamEnd.NOT_JSONL
                        throw IOException("vespa streamed visit ${resp.code}: $preview")
                    }
                    if (resp.header("Content-Type")?.contains("jsonl") != true) return@use StreamEnd.NOT_JSONL
                    val source = resp.body.source()
                    var complete = false
                    while (true) {
                        val line = source.readUtf8Line() ?: break
                        if (line.isEmpty()) continue
                        // A stream cut mid-line leaves a truncated tail that no
                        // longer parses — an interruption to resume from, not a
                        // fatal decode error.
                        val obj = runCatching { VESPA_JSON.decodeFromString<StreamLine>(line) }.getOrNull() ?: break
                        when {
                            obj.put != null -> {
                                into += VisitedDoc(obj.put, obj.fields ?: continue)
                            }

                            obj.continuation != null -> {
                                val t = obj.continuation.token
                                onContinuation(t)
                                if (t == null) complete = true // the final 100% marker carries no token
                            }

                            obj.message != null -> {
                                if (obj.message.severity == "error") {
                                    throw IOException("vespa streamed visit error: ${obj.message.text}")
                                }
                            }

                            else -> {} // sessionStats and friends
                        }
                    }
                    if (complete) StreamEnd.COMPLETE else StreamEnd.INTERRUPTED
                }
            } finally {
                guard.cancel()
            }
        }

    companion object {
        /** Docs asked for per visit response (Vespa's per-request ceiling is 1024). */
        const val VISIT_PAGE = 1024

        /**
         * Bounds on the derived streamed-slice default (2 x host cores). The
         * floor keeps tiny hosts from serializing the walk; the cap keeps huge
         * hosts from opening visitor sessions far past where scaling stopped
         * in the A/B, and comfortably below the ~64-session pressure that
         * wedged a small node's document API.
         */
        const val SLICES_MIN = 4
        const val SLICES_MAX = 32

        /** Default bucket concurrency for the paged fallback: 8 halved its wall clock in the A/B; the measured wedge needed ~64 sessions. */
        const val DEFAULT_BUCKET_CONCURRENCY = 8

        /** Server-side timeout on visit requests — strictly under the client's visit read deadline, see [pagedWalk]. */
        const val SERVER_TIMEOUT_SECONDS = 90L

        /** Consecutive stream interruptions tolerated before the walk fails loudly. */
        const val STREAM_RETRIES = 3
    }
}
