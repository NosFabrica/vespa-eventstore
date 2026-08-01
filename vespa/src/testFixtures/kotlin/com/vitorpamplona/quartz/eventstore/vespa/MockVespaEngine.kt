/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.vespa
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventSelection
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.eclipse.jetty.http.HttpHeader
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.eclipse.jetty.io.Content
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.HttpConnectionFactory
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Response
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.util.Callback
import java.io.IOException
import java.net.URLDecoder
import java.util.zip.GZIPInputStream

/**
 * The slice of Vespa [VespaEventIndex] talks to, backed by an
 * [InMemoryEventIndex]: document-API put/get/remove on `/document/v1` plus
 * `/search/` — whose YQL is PARSED back into an [EventQuery] and evaluated by
 * the reference index. That closes the loop: a query only works over the wire
 * if [EventYql]'s output round-trips through a grammar-faithful parser, and
 * the results must agree with the in-memory spec. (It is still a mock: real
 * Vespa acceptance of the YQL is the deploy-time integration test's job.)
 *
 * Runs on Jetty with HTTP/1.1 AND clear-text HTTP/2 on the same port: the
 * feed client writes over h2c (it refuses HTTP/1.1) while the query side and
 * the feed client's `?dryRun=true` handshake stay plain.
 */
class MockVespaEngine {
    val inner = InMemoryEventIndex()

    val url: String get() = "http://127.0.0.1:$port"

    /**
     * Abort the next N response bodies mid-stream: send headers and a fragment of
     * JSON, then fail the exchange so the client's body read throws *after* it has
     * already accepted the response.
     *
     * This is the shape that a stalled engine produces, and it lands in a place
     * ordinary error handling misses: the exception surfaces inside OkHttp's
     * `onResponse` callback, which OkHttp only logs ("Callback failure for call
     * to …") instead of routing to `onFailure`. A client that reads the body there
     * without completing its continuation on the failure path hangs forever.
     */
    @Volatile var abortBodiesMidStream: Int = 0

    /**
     * Answer every search with a DEGRADED coverage block naming this reason
     * (`"timeout"`, `"match-phase"`, …) instead of a complete one. Real Vespa does
     * this whenever it gives up on a query: HTTP 200, fewer hits, `full: false`.
     * Null (the default) means full coverage.
     */
    @Volatile var degradeCoverage: String? = null

    /**
     * Serve streamed visits (`stream=true`) the paged JSON shape instead of
     * JSON Lines — an older Vespa that doesn't speak the streamed protocol.
     * The client must detect the content type and fall back to the paged walk.
     */
    @Volatile var ignoreStreamedVisits: Boolean = false

    /**
     * Refuse `ranking=recency` queries the way a schema deployed before the
     * match-phase profile does (HTTP 400 naming the profile). The client must
     * demote the query to `unranked` and remember, not fail the REQ.
     */
    @Volatile var rejectRecencyProfile: Boolean = false

    /**
     * Answer `ranking=recency` queries with only this many hits, marked
     * match-phase-degraded — real Vespa's documented under-delivery on an
     * unevenly distributed corpus ("you risk sometimes getting less than the
     * configured hits back", with no automatic re-run). The client must rerun
     * the query unranked instead of serving the short page. Unranked queries
     * are untouched, so the rerun sees the true answer. 0 = off.
     */
    @Volatile var matchPhaseUnderdeliver: Int = 0

    /**
     * Nodes reported in [matchPhaseUnderdeliver]'s coverage block. max-hits is
     * per content node, so a FULL degraded page proves exactness only on ONE
     * node — with more, the client must rerun exact even when the page is full.
     */
    @Volatile var matchPhaseNodes: Int = 1

    /**
     * Serve recall hits with the id order REVERSED within each equal-created_at
     * group — the engine's single-key sort leaves tie order ARBITRARY, and the
     * client must restore `id asc` (and exact boundary membership under a
     * limit) itself. Reversal is the adversarial arbitrary order: any client
     * that leans on the mock's naturally sorted ties passes by accident; one
     * that resolves them works against real Vespa too.
     */
    @Volatile var scrambleTieOrder: Boolean = false

    /**
     * Cut the NEXT streamed visit response after this many put lines,
     * mid-bucket and without a final marker — a broken connection. The client
     * must resume from the last continuation token it saw and deliver the
     * complete set exactly once (the cut bucket re-streams in full; its
     * pre-cut docs were never certified). One-shot: resets to 0 when it fires.
     */
    @Volatile var cutStreamedVisitAfterDocs: Int = 0

    private class Reply(
        val status: Int,
        val body: String,
        val contentType: String = "application/json",
    )

    private val server =
        Server().apply {
            val config = HttpConfiguration()
            addConnector(
                ServerConnector(this, HttpConnectionFactory(config), HTTP2CServerConnectionFactory(config)).apply {
                    host = "127.0.0.1"
                    port = 0
                },
            )
            handler =
                object : Handler.Abstract() {
                    override fun handle(
                        request: Request,
                        response: Response,
                        callback: Callback,
                    ): Boolean {
                        if (abortBodiesMidStream > 0) {
                            abortBodiesMidStream--
                            response.status = 200
                            response.headers.put(HttpHeader.CONTENT_TYPE, "application/json")
                            // A length the fragment below will never satisfy, so the
                            // client commits to reading more than will ever arrive.
                            response.headers.put(HttpHeader.CONTENT_LENGTH, 4096L)
                            Content.Sink.write(response, false, """{"root":{"children":[""", Callback.NOOP)
                            callback.failed(IOException("simulated mid-body abort"))
                            return true
                        }
                        val reply =
                            try {
                                // The feed client gzips its request bodies; real Vespa inflates them.
                                val raw = Content.Source.asInputStream(request).readBytes()
                                val body =
                                    if (request.headers.get(HttpHeader.CONTENT_ENCODING) == "gzip") {
                                        GZIPInputStream(raw.inputStream()).readBytes().decodeToString()
                                    } else {
                                        raw.decodeToString()
                                    }
                                handle(request.method, request.httpURI.path, request.httpURI.query, body)
                            } catch (e: Exception) {
                                Reply(500, buildJsonObject { put("message", e.toString()) }.toString())
                            }
                        response.status = reply.status
                        response.headers.put(HttpHeader.CONTENT_TYPE, reply.contentType)
                        Content.Sink.write(response, true, reply.body, callback)
                        return true
                    }
                }
            start()
        }

    val port: Int get() = (server.connectors[0] as ServerConnector).localPort

    fun stop() = server.stop()

    private fun handle(
        method: String,
        path: String,
        rawQuery: String?,
        body: String,
    ): Reply {
        // The feed client opens with a no-op handshake request (?dryRun=true).
        if (rawQuery.orEmpty().contains("dryRun=true")) return Reply(200, "{}")
        val docPrefix = "/document/v1/event/event/docid/"
        return when {
            method == "POST" && path.startsWith(docPrefix) -> {
                val fields =
                    Json
                        .parseToJsonElement(body)
                        .jsonObject
                        .getValue("fields")
                        .jsonObject
                runBlocking { inner.put(EventDoc.fromSummary(fields)) }
                Reply(200, """{"id":"$path"}""")
            }

            method == "DELETE" && path.startsWith(docPrefix) -> {
                runBlocking { inner.remove(path.removePrefix(docPrefix)) }
                Reply(200, """{"id":"$path"}""")
            }

            method == "GET" && path.startsWith(docPrefix) -> {
                val doc = runBlocking { inner.get(path.removePrefix(docPrefix)) }
                if (doc == null) {
                    Reply(404, """{"message":"not found"}""")
                } else {
                    Reply(
                        200,
                        buildJsonObject {
                            put("id", path)
                            put("fields", doc.indexFields())
                        }.toString(),
                    )
                }
            }

            // The visit walk: /document/v1/…/docid (no doc id) with a selection.
            method == "GET" && path == docPrefix.removeSuffix("/") -> {
                visit(params(rawQuery.orEmpty()))
            }

            method == "GET" && path == "/search/" -> {
                search(params(rawQuery.orEmpty()))
            }

            // The real client POSTs its queries (URL-length safety); the body is
            // a flat JSON object of the same request parameters.
            method == "POST" && path == "/search/" -> {
                val json = Json.parseToJsonElement(body).jsonObject
                search(json.mapValues { (_, v) -> v.jsonPrimitive.content })
            }

            else -> {
                Reply(400, """{"message":"unexpected request: $method $path"}""")
            }
        }
    }

    private fun search(params: Map<String, String>): Reply {
        PROFILE_ONLY_PARAMS.firstOrNull { it in params }?.let {
            return Reply(400, """{"message":"$it must be specified in a query profile."}""")
        }
        // A schema deployed before the `recency` match-phase profile: real
        // Vespa 400s the query naming the missing profile, and the client must
        // demote to unranked instead of failing the REQ.
        if (rejectRecencyProfile && params["ranking"] == "recency") {
            return Reply(400, """{"message":"Requested rank profile 'recency' is undefined for document type 'event'"}""")
        }
        val yql = params["yql"] ?: return Reply(400, """{"message":"missing yql"}""")
        val hits = params["hits"]?.toIntOrNull() ?: 10
        // The exact-count query (EventYql.buildCount): "… limit 0 | all(output(count()))".
        // The distinct-author query (EventYql.buildDistinctCount): "… | all(group(pubkey) output(count()))".
        // Both grouping counts scan the FULL match set, so ignore the hit-limiting `limit 0`.
        // The kind histogram (EventYql.buildKindHistogram): "… | all(group(kind) max(N) each(output(count())))".
        val isCount = yql.contains("all(output(count()))")
        val isDistinct = yql.contains("group(pubkey)")
        val isKindHistogram = yql.contains("group(kind)")
        val grouped = isCount || isDistinct || isKindHistogram
        val query = MockYql.parse(yql.substringBefore("|").trim(), params).let { if (grouped) it.copy(limit = null) else it }
        // The limit cut must be applied AFTER any tie scrambling: real Vespa's
        // single-key sort picks boundary-tie MEMBERSHIP arbitrarily, and a cut
        // inside the reference's tiebroken order would hide exactly the bug
        // [scrambleTieOrder] exists to surface.
        val matches =
            runBlocking { inner.search(query.copy(limit = null)) }
                .let { sorted ->
                    if (scrambleTieOrder && !grouped) {
                        // created_at groups stay newest-first; ids within each
                        // group flip — see [scrambleTieOrder].
                        sorted.groupBy { it.createdAt }.values.flatMap { it.reversed() }
                    } else {
                        sorted
                    }
                }.let { ordered -> query.limit?.let { ordered.take(it) } ?: ordered }
        // Simulated match-phase under-delivery: fewer hits than the limit, with
        // ONLY the match-phase degradation flag set (see [matchPhaseUnderdeliver]).
        val underdeliver = matchPhaseUnderdeliver > 0 && !grouped && params["ranking"] == "recency"
        val served = if (underdeliver) minOf(matchPhaseUnderdeliver, hits) else hits
        val children =
            when {
                // Nest count() under the group list, exactly where real Vespa's
                // grouping puts it — the client's recursive scan must find it there.
                isDistinct -> groupCountChildren(matches.map { it.pubkey }.distinct().size)

                isKindHistogram -> kindHistogramChildren(matches.groupingBy { it.kind }.eachCount())

                isCount -> countChildren(matches.size)

                else -> JsonArray(matches.take(served).map { doc -> buildJsonObject { put("fields", doc.indexFields()) } })
            }
        val root =
            buildJsonObject {
                put(
                    "root",
                    buildJsonObject {
                        put("fields", buildJsonObject { put("totalCount", JsonPrimitive(matches.size)) })
                        put("coverage", if (underdeliver) matchPhaseCoverage(matches.size) else coverage(matches.size))
                        put("children", children)
                    },
                )
            }
        return Reply(200, root.toString())
    }

    /** Real Vespa's match-phase coverage: full=false, all flags listed, only match-phase set. */
    private fun matchPhaseCoverage(documents: Int): JsonObject =
        buildJsonObject {
            put("coverage", JsonPrimitive(1))
            put("documents", JsonPrimitive(documents))
            put("full", JsonPrimitive(false))
            put("nodes", JsonPrimitive(matchPhaseNodes))
            put("results", JsonPrimitive(1))
            put("resultsFull", JsonPrimitive(0))
            put(
                "degraded",
                buildJsonObject {
                    put("match-phase", JsonPrimitive(true))
                    put("timeout", JsonPrimitive(false))
                    put("adaptive-timeout", JsonPrimitive(false))
                    put("non-ideal-state", JsonPrimitive(false))
                },
            )
        }

    /**
     * The coverage block real Vespa puts on every search response. Complete unless
     * a test asks for degradation via [degradeCoverage] — which is how a caller
     * gets to prove it REFUSES a partial answer, the one thing that looks exactly
     * like a small result set from the outside.
     */
    private fun coverage(documents: Int): JsonObject =
        buildJsonObject {
            val complete = degradeCoverage == null
            put("coverage", JsonPrimitive(if (complete) 100 else 42))
            put("documents", JsonPrimitive(documents))
            put("full", JsonPrimitive(complete))
            put("nodes", JsonPrimitive(1))
            put("results", JsonPrimitive(1))
            put("resultsFull", JsonPrimitive(if (complete) 1 else 0))
            degradeCoverage?.let { reason ->
                put("degraded", buildJsonObject { put(reason, JsonPrimitive(true)) })
            }
        }

    /** `all(output(count()))`: a single group:root node carrying the doc count() directly. */
    private fun countChildren(count: Int): JsonArray =
        JsonArray(
            listOf(
                buildJsonObject {
                    put("id", JsonPrimitive("group:root:0"))
                    put("fields", buildJsonObject { put("count()", JsonPrimitive(count)) })
                },
            ),
        )

    /** `all(group(pubkey) output(count()))`: the group:root wraps a grouplist whose count() is the number of distinct groups. */
    private fun groupCountChildren(distinct: Int): JsonArray =
        JsonArray(
            listOf(
                buildJsonObject {
                    put("id", JsonPrimitive("group:root:0"))
                    put(
                        "children",
                        JsonArray(
                            listOf(
                                buildJsonObject {
                                    put("id", JsonPrimitive("grouplist:pubkey"))
                                    put("label", JsonPrimitive("pubkey"))
                                    put("fields", buildJsonObject { put("count()", JsonPrimitive(distinct)) })
                                },
                            ),
                        ),
                    )
                },
            ),
        )

    /** `all(group(kind) each(output(count())))`: the group:root wraps a grouplist of one leaf group per kind, each with its `value` and `count()`. */
    private fun kindHistogramChildren(counts: Map<Int, Int>): JsonArray =
        JsonArray(
            listOf(
                buildJsonObject {
                    put("id", JsonPrimitive("group:root:0"))
                    put(
                        "children",
                        JsonArray(
                            listOf(
                                buildJsonObject {
                                    put("id", JsonPrimitive("grouplist:kind"))
                                    put("label", JsonPrimitive("kind"))
                                    put(
                                        "children",
                                        JsonArray(
                                            counts.map { (kind, count) ->
                                                buildJsonObject {
                                                    put("id", JsonPrimitive("group:kind:$kind"))
                                                    put("value", JsonPrimitive(kind))
                                                    put("fields", buildJsonObject { put("count()", JsonPrimitive(count)) })
                                                }
                                            },
                                        ),
                                    )
                                },
                            ),
                        ),
                    )
                },
            ),
        )

    /**
     * The document-API visit, paged: the selection is PARSED back into an
     * [EventQuery] (same drift alarm as the YQL side), and pages are kept
     * deliberately small so clients must actually follow continuation tokens
     * — real Vespa also returns fewer documents than asked for.
     */
    private fun visit(params: Map<String, String>): Reply {
        val selection = params["selection"] ?: return Reply(400, """{"message":"missing selection"}""")
        val fieldSet = params["fieldSet"].orEmpty()
        // Drift alarm: real Vespa's fieldSet is "<doctype>:<field>,<field>,…" — the
        // doctype prefixes the list ONCE. A repeated "event:" on a later field (the
        // classic bug) is ILLEGAL_PARAMETERS, so reject it here too instead of
        // leniently matching `.contains("tag_index")`.
        val fullDocument = fieldSet == "[document]" // every real document field, like a doc-API get
        if (fieldSet.isNotEmpty() && !fullDocument) {
            val fields = fieldSet.substringAfter(":", "").split(",")
            if (!fieldSet.contains(":") || fields.any { ":" in it }) {
                return Reply(400, """{"message":"ILLEGAL_PARAMETERS: bad fieldSet '$fieldSet'"}""")
            }
        }
        val withTagIndex = fieldSet.contains("tag_index")
        val query = MockSelection.parse(selection)
        // Sliced visiting (`slices`/`sliceId`): each slice sees a disjoint
        // partition of the match set, so the union across all slices is exactly
        // the whole set — the property the parallel client walk depends on.
        // Real Vespa partitions by bucket; a stable id hash models that here.
        val slices = params["slices"]?.toIntOrNull() ?: 1
        val sliceId = params["sliceId"]?.toIntOrNull() ?: 0
        if (slices < 1 || sliceId !in 0 until slices) {
            return Reply(400, """{"message":"ILLEGAL_PARAMETERS: sliceId $sliceId of $slices slices"}""")
        }
        val all =
            runBlocking { inner.search(query) }
                .filter { Math.floorMod(it.id.hashCode(), slices) == sliceId }
        val offset = params["continuation"]?.toIntOrNull() ?: 0
        if (params["stream"] == "true" && !ignoreStreamedVisits) {
            return streamedVisit(all, offset, withTagIndex)
        }
        val wanted = params["wantedDocumentCount"]?.toIntOrNull() ?: 1
        val page = all.drop(offset).take(minOf(wanted, VISIT_PAGE_CAP))
        val body =
            buildJsonObject {
                put("pathId", JsonPrimitive("/document/v1/event/event/docid"))
                put(
                    "documents",
                    JsonArray(
                        page.map { doc ->
                            buildJsonObject {
                                put("id", JsonPrimitive("id:event:event::${doc.id}"))
                                put(
                                    "fields",
                                    if (fullDocument) {
                                        doc.indexFields()
                                    } else {
                                        buildJsonObject {
                                            put("created_at", JsonPrimitive(doc.createdAt))
                                            if (withTagIndex) put("tag_index", JsonArray(doc.tagIndex().map(::JsonPrimitive)))
                                        }
                                    },
                                )
                            }
                        },
                    ),
                )
                if (offset + page.size < all.size) put("continuation", JsonPrimitive((offset + page.size).toString()))
            }
        return Reply(200, body.toString())
    }

    /**
     * The streamed visit (`stream=true` + JSON Lines): put lines as the backend
     * "visits", a continuation line after every completed [STREAM_BUCKET]-doc
     * bucket (mirroring real Vespa's per-bucket tokens), and a final marker
     * with `percentFinished: 100` and NO token. [cutStreamedVisitAfterDocs]
     * simulates a broken connection: the response just stops mid-bucket, so the
     * client must resume from the last token — and because the cut bucket never
     * got its continuation line, resuming re-streams it IN FULL, which is
     * exactly the duplicate-delivery hazard the client's certification buffer
     * must absorb.
     */
    private fun streamedVisit(
        all: List<EventDoc>,
        offset: Int,
        withTagIndex: Boolean,
    ): Reply {
        val out = StringBuilder()
        val cut = cutStreamedVisitAfterDocs
        var emitted = 0
        var pos = offset
        for (bucket in all.drop(offset).chunked(STREAM_BUCKET)) {
            for (doc in bucket) {
                if (cut > 0 && emitted == cut) {
                    cutStreamedVisitAfterDocs = 0
                    return Reply(200, out.toString(), "application/jsonl")
                }
                out.append(putLine(doc, withTagIndex)).append('\n')
                emitted++
            }
            pos += bucket.size
            val pct = pos * 100.0 / all.size
            out.append("""{"continuation":{"token":"$pos","percentFinished":$pct}}""").append('\n')
        }
        out.append("""{"continuation":{"percentFinished":100.0}}""").append('\n')
        out.append("""{"sessionStats":{"documentCount":$emitted}}""").append('\n')
        return Reply(200, out.toString(), "application/jsonl")
    }

    private fun putLine(
        doc: EventDoc,
        withTagIndex: Boolean,
    ): String =
        buildJsonObject {
            put("put", JsonPrimitive("id:event:event::${doc.id}"))
            put(
                "fields",
                buildJsonObject {
                    put("created_at", JsonPrimitive(doc.createdAt))
                    if (withTagIndex) put("tag_index", JsonArray(doc.tagIndex().map(::JsonPrimitive)))
                },
            )
        }.toString()

    private fun params(rawQuery: String): Map<String, String> =
        rawQuery
            .split("&")
            .filter { it.isNotEmpty() }
            .associate { part ->
                val k = part.substringBefore("=")
                val v = URLDecoder.decode(part.substringAfter("=", ""), "UTF-8")
                k to v
            }

    private companion object {
        /** Max docs per visit response — small enough that tests always cross a page boundary. */
        const val VISIT_PAGE_CAP = 7

        /** Docs per simulated backend bucket in a STREAMED visit — small so tests cross several continuation tokens. */
        const val STREAM_BUCKET = 5

        /**
         * Settings real Vespa REFUSES to take from a request — it fails the query
         * with `<name> must be specified in a query profile.` rather than applying
         * it (`DefaultProperties.requireNotPresentIn`, `GroupingQueryParser.validate`).
         * Modelled here because a mock that quietly ignores query parameters made
         * exactly this bug invisible: sending `grouping.globalMaxGroups` 400'd every
         * aggregation against a real Vespa while these tests stayed green.
         */
        val PROFILE_ONLY_PARAMS = listOf("maxHits", "maxOffset", "grouping.globalMaxGroups")
    }
}

/**
 * Parses exactly the document-selection grammar [EventSelection] emits back
 * into an [EventQuery] — the visit-side twin of [MockYql], and the same drift
 * alarm: an unparseable selection means builder and spec disagree.
 */
object MockSelection {
    fun parse(selection: String): EventQuery {
        if (selection == "true") return EventQuery()
        var q = EventQuery()
        for (clause in splitTopLevel(selection)) {
            when {
                clause.startsWith("(event.kind==") -> {
                    q = q.copy(kinds = orGroup(clause, "event.kind==").map { it.toInt() })
                }

                clause.startsWith("(event.kind!=") -> {
                    q = q.copy(notKinds = clause.removeSurrounding("(", ")").split(" and ").map { it.removePrefix("event.kind!=").toInt() })
                }

                clause.startsWith("(event.pubkey==") -> {
                    q = q.copy(authors = orGroup(clause, "event.pubkey==").map(::unquote))
                }

                clause.startsWith("(event.owner==") -> {
                    q = q.copy(owners = orGroup(clause, "event.owner==").map(::unquote))
                }

                clause.startsWith("event.created_at>=") -> {
                    q = q.copy(since = clause.removePrefix("event.created_at>=").toLong())
                }

                clause.startsWith("event.created_at<=") -> {
                    q = q.copy(until = clause.removePrefix("event.created_at<=").toLong())
                }

                clause.startsWith("event.expires_at>") -> {
                    q = q.copy(notExpiredAt = clause.removePrefix("event.expires_at>").toLong())
                }

                else -> {
                    error("unparseable selection clause: $clause")
                }
            }
        }
        return q
    }

    /** Split on ` and ` at parenthesis depth zero. */
    private fun splitTopLevel(s: String): List<String> {
        val parts = ArrayList<String>()
        var depth = 0
        var start = 0
        var i = 0
        while (i < s.length) {
            when {
                s[i] == '(' -> {
                    depth++
                }

                s[i] == ')' -> {
                    depth--
                }

                depth == 0 && s.startsWith(" and ", i) -> {
                    parts += s.substring(start, i)
                    i += 5
                    start = i
                    continue
                }
            }
            i++
        }
        parts += s.substring(start)
        return parts
    }

    /** `(event.f==v1 or event.f==v2 …)` -> the raw values. */
    private fun orGroup(
        clause: String,
        prefix: String,
    ): List<String> =
        clause
            .removeSurrounding("(", ")")
            .split(" or ")
            .map {
                require(it.startsWith(prefix)) { "mixed or-group: $clause" }
                it.removePrefix(prefix)
            }

    private fun unquote(s: String): String {
        require(s.length >= 2 && s.first() == '"' && s.last() == '"') { "expected quoted value: $s" }
        return s.substring(1, s.length - 1)
    }
}

/**
 * Parses exactly the YQL grammar [EventYql] emits back into an [EventQuery].
 * Throws on anything else — an unparseable query means the builder and this
 * spec drifted apart, which is precisely what the wire tests must catch.
 */
object MockYql {
    fun parse(
        yql: String,
        params: Map<String, String>,
    ): EventQuery {
        // Accept any select list (`select *` or the trimmed reconstruction fields
        // EventYql now emits) — only the WHERE clause carries the query.
        val marker = " from event where "
        require(marker in yql) { "not an event query: $yql" }
        var rest = yql.substringAfter(marker).trim()

        var limit: Int? = null
        Regex(""" limit (\d+)$""").find(rest)?.let {
            limit = it.groupValues[1].toInt()
            rest = rest.removeRange(it.range)
        }
        rest = rest.removeSuffix(" order by created_at desc")

        var q = EventQuery(limit = limit)
        if (rest == "true") return q
        for (clause in splitTopLevel(rest, " and ")) {
            q =
                when {
                    clause.startsWith("id in (") -> q.copy(ids = strings(clause))

                    clause.startsWith("pubkey in (") -> q.copy(authors = strings(clause))

                    clause.startsWith("owner in (") -> q.copy(owners = strings(clause))

                    clause.startsWith("kind in (") -> q.copy(kinds = ints(clause))

                    clause.startsWith("!(kind in (") -> q.copy(notKinds = ints(clause.removePrefix("!(").removeSuffix(")")))

                    clause.startsWith("created_at >= ") -> q.copy(since = clause.substringAfterLast(' ').toLong())

                    clause.startsWith("created_at <= ") -> q.copy(until = clause.substringAfterLast(' ').toLong())

                    clause.startsWith("expires_at < ") -> q.copy(expiresBefore = clause.substringAfterLast(' ').toLong())

                    clause.startsWith("expires_at > ") -> q.copy(notExpiredAt = clause.substringAfterLast(' ').toLong())

                    // The word-group search clause (its first sub-clause is always a
                    // field-annotated userInput): reconstruct the term from the
                    // per-word parameters (@w0..@wN — @wj/@wp* are derived variants).
                    clause.startsWith("((({defaultIndex:") -> q.copy(search = searchWords(params))

                    clause.startsWith("(tag_index contains ") -> tagGroup(q, clause)

                    clause.startsWith("tag_index in (") -> tagInGroup(q, clause)

                    else -> error("unparseable clause: $clause")
                }
        }
        return q
    }

    /** w0, w1, … until the first gap — the builder emits one per word, with no upper bound. */
    private fun searchWords(params: Map<String, String>): String {
        val words =
            generateSequence(0) { it + 1 }
                .map { params["w$it"] }
                .takeWhile { it != null }
                .filterNotNull()
                .toList()
        require(words.isNotEmpty()) { "search clause without w0.. parameters" }
        return words.joinToString(" ")
    }

    /** One parenthesized tag group: `or`-joined -> tags, `and`-joined -> tagsAll (identical when single). */
    private fun tagGroup(
        q: EventQuery,
        clause: String,
    ): EventQuery {
        val body = clause.removePrefix("(").removeSuffix(")")
        val all = " and " in splitMarker(body)
        val pairs =
            splitTopLevel(body, if (all) " and " else " or ").map { term ->
                val value = unquote(term.removePrefix("tag_index contains "))
                value.substringBefore(":") to value.substringAfter(":")
            }
        val name = pairs.first().first
        require(pairs.all { it.first == name }) { "mixed tag names in one group: $clause" }
        val values = pairs.map { it.second }
        return if (all) q.copy(tagsAll = q.tagsAll + (name to values)) else q.copy(tags = q.tags + (name to values))
    }

    /** The multi-value OR form EventYql emits for NIP-01 tag lists: `tag_index in ("n:v1", "n:v2", …)` -> tags. */
    private fun tagInGroup(
        q: EventQuery,
        clause: String,
    ): EventQuery {
        val body = clause.removePrefix("tag_index in (").removeSuffix(")")
        val pairs =
            splitTopLevel(body, ", ").map { literal ->
                val value = unquote(literal.trim())
                value.substringBefore(":") to value.substringAfter(":")
            }
        val name = pairs.first().first
        require(pairs.all { it.first == name }) { "mixed tag names in one in-list: $clause" }
        return q.copy(tags = q.tags + (name to pairs.map { it.second }))
    }

    /** Which joiner a group uses, looking only OUTSIDE string literals. */
    private fun splitMarker(body: String): String {
        var inQuote = false
        var i = 0
        while (i < body.length) {
            when {
                inQuote && body[i] == '\\' -> i++
                body[i] == '"' -> inQuote = !inQuote
                !inQuote && body.startsWith(" and ", i) -> return " and "
            }
            i++
        }
        return " or "
    }

    /** Split on [sep] at paren-depth 0, outside string literals. */
    private fun splitTopLevel(
        s: String,
        sep: String,
    ): List<String> {
        val parts = ArrayList<String>()
        var depth = 0
        var inQuote = false
        var start = 0
        var i = 0
        while (i < s.length) {
            val c = s[i]
            when {
                inQuote && c == '\\' -> {
                    i++
                }

                c == '"' -> {
                    inQuote = !inQuote
                }

                !inQuote && c == '(' -> {
                    depth++
                }

                !inQuote && c == ')' -> {
                    depth--
                }

                !inQuote && depth == 0 && s.startsWith(sep, i) -> {
                    parts += s.substring(start, i)
                    i += sep.length - 1
                    start = i + 1
                }
            }
            i++
        }
        parts += s.substring(start)
        return parts
    }

    private fun strings(clause: String): List<String> =
        clause
            .substringAfter('(')
            .substringBeforeLast(')')
            .split(", ")
            .map { it.removeSurrounding("\"") }

    private fun ints(clause: String): List<Int> =
        clause
            .substringAfter('(')
            .substringBeforeLast(')')
            .split(", ")
            .map { it.toInt() }

    /** Reverse of EventYql.quote: strip the quotes and undo its escapes. */
    private fun unquote(literal: String): String {
        require(literal.length >= 2 && literal.startsWith('"') && literal.endsWith('"')) { "not a string literal: $literal" }
        val s = literal.substring(1, literal.length - 1)
        val out = StringBuilder(s.length)
        var i = 0
        while (i < s.length) {
            val c = s[i]
            if (c == '\\' && i + 1 < s.length) {
                i++
                out.append(
                    when (s[i]) {
                        'n' -> '\n'
                        'r' -> '\r'
                        't' -> '\t'
                        else -> s[i]
                    },
                )
            } else {
                out.append(c)
            }
            i++
        }
        return out.toString()
    }
}
