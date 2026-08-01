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
package com.vitorpamplona.quartz.eventstore.vespa.client
import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.FeedClient
import ai.vespa.feed.client.FeedClientBuilder
import ai.vespa.feed.client.OperationParameters
import ai.vespa.feed.client.Result
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.SearchFields
import com.vitorpamplona.quartz.eventstore.vespa.mapBounded
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventSelection
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import com.vitorpamplona.quartz.eventstore.vespa.query.VespaQuery
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.utils.Hex
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
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import java.net.Proxy
import java.net.URI
import java.net.URLEncoder
import java.time.Duration
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * The real [EventIndex]: Vespa over HTTP. Writes go through Vespa's official
 * feed client (HTTP/2 multiplexed, per-doc ordering, retries built in) and are
 * AWAITED before returning. The store's read-your-writes contract needs the
 * ack, and proton makes an acked write visible to search. Reads use the plain
 * document API (get) and `/search/` (query), non-blocking via the JDK client's
 * async sends on virtual threads.
 *
 * There is no hit ceiling here, configurable or otherwise: a query with a
 * `limit` gets exactly that, and one without gets every match. Bounding what a
 * query costs is the caller's job, expressed per query through
 * [EventQuery.limit]. A full-corpus walk should still go through [visitIds]
 * (the document API's visit), which streams rather than materializing the whole
 * match set in one response.
 *
 * Counts use a grouping `count()` over the full match set (see
 * [EventYql.buildCount]) — NOT `root.totalCount`, which the recency `order by`'s
 * match-phase caps on a large corpus (a 10x+ undercount). Exact for
 * attribute-only recall; still approximate under a weakAnd search term, the same
 * caveat Vespa itself carries.
 */
class VespaEventIndex(
    baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
    /**
     * All container endpoints of the cluster; empty = just [baseUrl]. On a
     * multi-container deployment, naming every endpoint here beats a load
     * balancer for the WRITE path: the feed client natively spreads its HTTP/2
     * connections across all of them (connections-per-endpoint applies to
     * each). Reads round-robin per request. A single URL pointing at a load
     * balancer also works — this is the more direct option, not the only one.
     */
    endpoints: List<String> = emptyList(),
    /**
     * Independent STREAMED slices a full-corpus visit is split into
     * (document-API `slices`/`sliceId`), each walked concurrently as its own
     * JSON-Lines stream. Slicing multiplies streamed throughput until the
     * content node saturates (8 already saturated a 4-core node in the A/B);
     * it does NOT apply to the paged fallback, where it was measured 11x
     * SLOWER than a serial walk (each sliced request returns roughly one
     * small bucket, not a full page). `VESPA_VISIT_SLICES` overrides for
     * deployment tuning.
     */
    private val visitSlices: Int =
        (System.getenv("VESPA_VISIT_SLICES")?.toIntOrNull() ?: VISIT_SLICES)
            .coerceAtLeast(1),
    /**
     * Backend bucket parallelism WITHIN each paged visit request (document-API
     * `concurrency`), used only by the serial paged fallback ([pagedWalk]).
     * Distribution buckets hold only a few hundred docs each on a large corpus,
     * so filling a 1024-doc page at concurrency 1 reads several buckets
     * back-to-back; 8 halved the fallback's wall clock in the A/B. Streamed
     * visits do NOT use it — they pin bucket concurrency to 1, which is what
     * makes their resume exactly-once (see [streamedSlice]). Total visitor
     * pressure is what wedges a small node's document API (see [visitHttp]),
     * so keep the product of concurrent visits and this figure modest.
     * `VESPA_VISIT_CONCURRENCY` overrides (Vespa accepts 1..100).
     */
    private val visitConcurrency: Int =
        (System.getenv("VESPA_VISIT_CONCURRENCY")?.toIntOrNull() ?: VISIT_CONCURRENCY)
            .coerceIn(1, 100),
    /**
     * Stream each visit slice as one long JSON-Lines response
     * ([streamedSlice]) instead of paging through continuation round trips.
     * Streaming removes the per-page round trip entirely — the dominant cost
     * of a corpus-sized walk. When the server doesn't answer in JSON Lines (an
     * older Vespa), the whole walk falls back to ONE serial paged chain
     * ([pagedWalk]). `VESPA_VISIT_STREAM=0` forces that fallback.
     */
    private val visitStreaming: Boolean =
        System.getenv("VESPA_VISIT_STREAM")?.let { it != "0" && !it.equals("false", ignoreCase = true) } ?: true,
) : EventIndex {
    private val urls: List<String> = endpoints.ifEmpty { listOf(baseUrl) }.map { it.trimEnd('/') }

    private val nextUrl =
        java.util.concurrent.atomic
            .AtomicInteger()

    /** The endpoint for one HTTP read — round-robin across [urls]. */
    private fun endpoint(): String = urls[Math.floorMod(nextUrl.getAndIncrement(), urls.size)]

    /**
     * Connections the feed client opens to EACH endpoint.
     *
     * The tuned 32 is a budget for the cluster, not for one host, so it is split
     * across [urls] rather than multiplied by them. That distinction is load
     * bearing: the client sizes ONE shared Jetty pool at
     * `max(min(cores, 64), 8) + connectionsPerEndpoint * endpoints`, and Jetty
     * refuses to start unless that total is strictly greater than the threads the
     * HTTP client leases from it. Two endpoints at 32 each on a 12-core host is
     * `12 + 64 = 76` against a required 76 — one thread short, and the client
     * throws from its constructor:
     *
     *     Insufficient configured threads: required=76 < max=76
     *
     * Splitting keeps the total parallelism (and therefore the pool) identical
     * whether the cluster is named as one endpoint or five, which is what the
     * figure was measured against in the first place.
     *
     * `VESPA_FEED_CONNECTIONS` overrides it per endpoint and is NOT divided —
     * deployment tuning is explicit by definition, and the benchmark sweeps it.
     * Setting it high across many endpoints can reach the same Jetty ceiling; it
     * fails loudly at startup with the message above rather than degrading.
     */
    private val feedConnectionsPerEndpoint: Int =
        System.getenv("VESPA_FEED_CONNECTIONS")?.toIntOrNull()
            ?: (32 / urls.size).coerceAtLeast(1)

    private val feed: FeedClient =
        FeedClientBuilder
            .create(urls.map { URI.create(it) })
            // The throttle FLOOR is what pins bulk ingest, and it is hard-wired to
            // minInflight = 2 x connectionsPerEndpoint. Under our bursty batched
            // writes (putAll bursts, then a gap while the next chunk dedups) the
            // dynamic throttler never sustains its upward probe, so it idles at that
            // floor. At the old 8 connections that floor was ~16 in flight — about
            // 1.2k docs/s while the engine sat at ~2.4 of 12 cores, ~5x idle. Raising
            // the connection count raises the floor (64 in flight here) AND the real
            // HTTP/2 parallelism, so ingest drives the engine harder. The throttler
            // still adapts DOWN if Vespa pushes back (retries absorb any overshoot).
            //
            // The client sizes its own Jetty pool from the connection count, so on a
            // small-core host too many connections starve that pool; 32 across the
            // cluster keeps headroom. See [feedConnectionsPerEndpoint]. (The old reflective
            // setInitialInflightFactor knob was dead: the 8.7 throttler ignores it —
            // the initial target is already maxInflight.)
            // Overridable for deployment tuning (and the benchmark's feed-window
            // grid): VESPA_FEED_CONNECTIONS / VESPA_FEED_STREAMS /
            // VESPA_FEED_INFLIGHT_FACTOR. Defaults are the measured sweet spot
            // for a small-core single-node host.
            .setConnectionsPerEndpoint(feedConnectionsPerEndpoint)
            .setMaxStreamPerConnection(System.getenv("VESPA_FEED_STREAMS")?.toIntOrNull() ?: 128)
            .apply { System.getenv("VESPA_FEED_INFLIGHT_FACTOR")?.toIntOrNull()?.let { setInitialInflightFactor(it) } }
            .setRetryStrategy(
                object : FeedClient.RetryStrategy {
                    // Bounded: a dead Vespa should surface as failed ops, not a hang.
                    override fun retries() = 5
                },
            ).build()

    // Read client. OkHttp pinned to clear-text HTTP/2 by PRIOR KNOWLEDGE: Vespa's
    // container serves h2c only via prior knowledge, not the `Upgrade: h2c`
    // handshake the JDK HttpClient uses — so the JDK client silently ran every
    // query on HTTP/1.1 (one TCP connection per in-flight read). Prior-knowledge
    // h2c makes concurrent reads multiplex over a single connection, matching the
    // feed client's write path. No fallback list: every endpoint here is Vespa.
    private val http =
        OkHttpClient
            .Builder()
            // Every request this client makes goes to ONE host, so OkHttp's
            // per-host default of 5 is the real ceiling — not maxRequests. The
            // whole store was capped at five concurrent engine requests, shared
            // by every snapshot visit, every search and every count.
            //
            // With no read deadline (below), five requests that never return
            // wedge the store permanently. Observed: three snapshots frozen at
            // 94k, 2.5M and 3.1M ids with zero visit requests reaching the
            // engine for minutes, the relay idle at 2.5% CPU.
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = MAX_CONCURRENT_REQUESTS
                    maxRequestsPerHost = MAX_CONCURRENT_REQUESTS
                },
            ).protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
            .connectTimeout(Duration.ofSeconds(5))
            .writeTimeout(Duration.ofSeconds(60))
            // NO read or whole-call deadline. A query with no `limit` asks for the
            // whole match set and is allowed to take as long as that takes — any
            // finite deadline here is a duration cap on the CALLER's query, decided
            // by the library, and OkHttp cannot tell "engine still matching" from
            // "connection idle" (both are just no-bytes-yet on the socket).
            //
            // A dead peer is caught without capping duration: HTTP/2 PING frames.
            // Unanswered pings fail the connection, so a black-holed socket still
            // surfaces as a retryable IOException in seconds rather than hanging
            // forever — which is what the deadlines were really there to catch.
            .readTimeout(Duration.ZERO)
            .callTimeout(Duration.ZERO)
            .pingInterval(Duration.ofSeconds(PING_INTERVAL_SECONDS))
            // Vespa is local; never route through the egress proxy.
            .proxy(Proxy.NO_PROXY)
            .build()

    /**
     * The visit walk's client: [http] plus a READ timeout, which visits need
     * and queries must not have. A query is one response that may legitimately
     * take minutes of engine time before its first byte, so [http] carries no
     * read deadline. A visit is the opposite shape: page responses are small
     * and continuous, and a streamed slice delivers lines steadily — silence
     * means a wedged visitor session, not a hard-working engine. Measured live:
     * enough concurrent visitor sessions wedge a small node's document API
     * mid-response, HTTP/2 pings keep the connection "alive" (they are
     * answered; it is the response that never comes), and without a read
     * deadline the walk hangs FOREVER. With one, the wedge surfaces as an
     * IOException that the paged retry / streamed resume machinery handles —
     * recover or fail loudly, never hang.
     */
    private val visitHttp =
        http
            .newBuilder()
            .readTimeout(Duration.ofSeconds(VISIT_READ_TIMEOUT_SECONDS))
            .build()

    private val jsonMedia = "application/json; charset=utf-8".toMediaType()

    // ADDRESS-KEYED mode (VESPA_ADDRESS_KEYED=1): replaceable/addressable events
    // are stored under their NIP-01 address as the document id, so the engine
    // enforces newest-wins with a conditional put (see [putIfNewer]) instead of
    // the client's read-then-supersede. Regular events stay id-keyed. Default OFF
    // — the whole scheme is opt-in until measured. When ON, id lookups can no
    // longer ride the document-API get (replaceables live under an address docid),
    // so they route through the `id`-attribute search, which finds both.
    private val addressKeyed = System.getenv("VESPA_ADDRESS_KEYED")?.toBooleanStrictOrNull() ?: false

    // Under address-keying the engine enforces newest-wins (conditional put), so
    // the bulk path skips its version-read stage and calls putIfNewer instead.
    override val supersedesViaPut: Boolean get() = addressKeyed

    /** The document id for [doc]: its address when address-keyed and replaceable, else its event id. */
    private fun docIdOf(doc: EventDoc): String = if (addressKeyed) doc.addressOrNull() ?: doc.id else doc.id

    override suspend fun get(id: String): EventDoc? {
        // Address-keyed replaceables aren't at the id docid; resolve by the id
        // attribute instead (finds regular AND replaceable, no doc-API get).
        if (addressKeyed) return searchById(id)
        val resp = send("${endpoint()}/document/v1/$NAMESPACE/$DOCTYPE/docid/$id")
        if (resp.statusCode() == 404) return null
        require(resp.statusCode() < 400) { "vespa get ${resp.statusCode()}: ${resp.body().take(300)}" }
        return DECODER.decodeFromString<DocEnvelope>(resp.body()).fields?.toDoc()
    }

    /** One doc by its `id` attribute via the search stack — the id-lookup path under address-keying (no doc-API get, no [getByIds] recursion). */
    private suspend fun searchById(id: String): EventDoc? {
        val vq = EventYql.build(EventQuery(ids = listOf(id))) ?: return null
        return searchRoot(vq, hits = 1)
            .children
            .firstOrNull()
            ?.fields
            ?.toDoc()
    }

    private fun putOp(doc: EventDoc) =
        feed.put(
            DocumentId.of(NAMESPACE, DOCTYPE, docIdOf(doc)),
            buildJsonObject { put("fields", doc.indexFields()) }.toString(),
            feedParams(),
        )

    private fun removeOp(id: String) = feed.remove(DocumentId.of(NAMESPACE, DOCTYPE, id), feedParams())

    override suspend fun put(doc: EventDoc) {
        putOp(doc).await()
    }

    /** All puts stay in flight together — the feed client multiplexes them over HTTP/2. */
    override suspend fun putAll(docs: List<EventDoc>) {
        docs.map { putOp(it) }.forEach { it.await() }
    }

    /**
     * Address-keyed newest-wins as a single server-side conditional put: create if
     * absent, else overwrite only when the incoming version beats the stored one
     * (higher created_at, or same created_at and a LOWER id — NIP-01). Stale
     * versions come back `conditionNotMet` (rejected by the engine, no client
     * read). Falls back to the search-then-supersede default when address-keying
     * is off or the doc is not replaceable.
     */
    override suspend fun putIfNewer(doc: EventDoc): Boolean {
        val address = doc.addressOrNull()
        if (!addressKeyed || address == null) return super.putIfNewer(doc)
        val condition =
            "event.created_at < ${doc.createdAt} or " +
                "(event.created_at == ${doc.createdAt} and event.id > \"${doc.id}\")"
        val result =
            feed
                .put(
                    DocumentId.of(NAMESPACE, DOCTYPE, address),
                    buildJsonObject { put("fields", doc.indexFields()) }.toString(),
                    feedParams().createIfNonExistent(true).testAndSetCondition(condition),
                ).await()
        // success = stored (created or newest-wins overwrite); conditionNotMet =
        // a same-or-newer version already held the address, so [doc] is stale.
        // A transport/engine failure completes the future exceptionally (never a
        // Result here), so the else guards only a future enum addition.
        return when (result.type()) {
            Result.Type.success -> true
            Result.Type.conditionNotMet -> false
            else -> error("vespa conditional put for $address returned ${result.type()}")
        }
    }

    override suspend fun remove(id: String) {
        // Under address-keying the doc may live at an address docid, so resolve it
        // (regular -> id docid, replaceable -> address docid) before removing.
        val docId = if (addressKeyed) get(id)?.let { docIdOf(it) } ?: id else id
        feed.remove(DocumentId.of(NAMESPACE, DOCTYPE, docId), feedParams()).await()
    }

    /** All removes in flight together over HTTP/2, like [putAll]. */
    override suspend fun removeAll(ids: List<String>) {
        // Address-keyed replaceables live at an address docid, not their event id,
        // so a raw removeOp(id) would silently miss them. Resolve each the way
        // remove() does (get -> docIdOf), bounded-concurrent. Regular events are
        // id-keyed and just pipeline by id.
        if (addressKeyed) {
            ids.mapBounded(ID_GET_FANOUT) { remove(it) }
            return
        }
        ids.map { removeOp(it) }.forEach { it.await() }
    }

    override suspend fun search(query: EventQuery): List<EventDoc> {
        // Pure-id recall bypasses /search/: each id is a direct document-API key
        // lookup (~35% faster than a search over the id attribute here), which is
        // what a REQ-by-id and the bulk dedup preload both do. The moment ANY other
        // constraint is present it falls through to the search stack below. The
        // expiry filter and newest-first order are applied exactly as YQL would, so
        // results are identical to the search path.
        if (query.isPureIdLookup()) return getByIds(query)
        val vq = EventYql.build(query) ?: return emptyList()
        // Stream the hits straight into docs (no full JsonElement tree): the
        // response is decoded into flat DTOs, allocating the target objects
        // directly instead of a JsonObject/JsonArray/JsonPrimitive wrapper per
        // field. This is the query hot path, so that saved garbage matters.
        return searchRoot(vq, hits = hitsFor(query))
            .children
            .mapNotNull { it.fields?.toDoc() }
    }

    /**
     * Raw recall: decode each hit straight to a [RawEvent], keeping `tags` as the
     * stored JSON string. This skips the one field [toDoc] still parses per hit
     * AND the EventDoc/Event object model — the relay read path splices that tag
     * string straight onto the wire, so parsing then re-serializing it is pure
     * waste that scales with result size. The pure-id fast path has no summary to
     * decode (it gets documents), so it reuses [getByIds] and projects the docs.
     */
    override suspend fun rawSearch(query: EventQuery): List<RawEvent> {
        if (query.isPureIdLookup()) return getByIds(query).map { it.toRawEvent() }
        val vq = EventYql.build(query) ?: return emptyList()
        return searchRoot(vq, hits = hitsFor(query))
            .children
            .mapNotNull { it.fields?.toRaw() }
    }

    /**
     * The `hits` to ask Vespa for: the query's own [EventQuery.limit], else
     * everything. Int.MAX_VALUE is how "no limit" is spelled — Vespa's `hits`
     * defaults to TEN when omitted, so an unbounded query has to name a number,
     * and the bundled query profile raises `maxHits` to let it through.
     */
    private fun hitsFor(query: EventQuery): Int = query.limit ?: Int.MAX_VALUE

    /**
     * Only ids constrain the query (an expiry guard may still ride along), and few
     * enough to resolve in a single concurrent get wave — the direct-lookup fast
     * path. The size cap matters: above it, ONE `id in (…)` search is a single
     * round trip while N gets are N, so the bulk-insert dedup preload (500-id
     * chunks) must stay on the search path or ingest would collapse.
     */
    private fun EventQuery.isPureIdLookup(): Boolean =
        !addressKeyed && // address-keyed replaceables aren't at the id docid — route ids through search
            ids.isNotEmpty() && ids.size <= ID_GET_FANOUT &&
            kinds.isEmpty() && notKinds.isEmpty() && authors.isEmpty() && owners.isEmpty() &&
            tags.isEmpty() && tagsAll.isEmpty() &&
            since == null && until == null && expiresBefore == null &&
            search == null && ranking == null

    /** Resolve [EventQuery.ids] through parallel document-API gets, then filter expiry, order, and cap like the search path. */
    private suspend fun getByIds(query: EventQuery): List<EventDoc> {
        val hexes =
            query.ids
                .map { it.lowercase() }
                .filter(Hex::isHex64)
                .distinct()
        val docs =
            when {
                hexes.isEmpty() -> return emptyList()

                // The overwhelmingly common REQ-by-id is a SINGLE id: skip the
                // fan-out machinery (coroutineScope + Semaphore + async/awaitAll
                // allocate per call) and just get it. At thousands of id lookups a
                // second that saved garbage is the difference the GC feels.
                hexes.size == 1 -> listOfNotNull(get(hexes[0]))

                else -> hexes.mapBounded(ID_GET_FANOUT) { get(it) }.filterNotNull()
            }
        // NIP-40: never serve an event already expired at the query's cutoff — the
        // same guard EventYql emits as `expires_at > notExpiredAt`.
        val live = query.notExpiredAt?.let { cut -> docs.filter { (it.expiresAt() ?: EventDoc.NO_EXPIRATION) > cut } } ?: docs
        val ordered = live.sortedWith(NEWEST_FIRST)
        return query.limit?.let(ordered::take) ?: ordered
    }

    /**
     * The document-API visit: a streaming scan with a selection expression and
     * continuation tokens. It STREAMS and does not rank, which is exactly what a
     * full-corpus id walk needs. Queries a selection can't express fall back to
     * the search default, which returns the same set in a single page.
     *
     * The walk is SLICED ([visitSlices] parallel continuation chains, see
     * [visitPages]); [onPage] is still invoked serially, and returning false
     * still stops the whole walk. Cross-page order is arbitrary — which the
     * [EventIndex.visitIds] contract already grants.
     */
    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        val selection = EventSelection.build(query) ?: return super.visitIds(query, withDTag, onPage)
        // Vespa fieldSet syntax is "<doctype>:<field>,<field>,…" — the doctype
        // prefixes the list ONCE, not each field (else: ILLEGAL_PARAMETERS).
        val fieldSet = "$DOCTYPE:created_at" + if (withDTag) ",tag_index" else ""
        visitPages(selection, fieldSet) { documents ->
            val page =
                documents.mapNotNull { d ->
                    val obj = d.jsonObject
                    val id = obj["id"]?.jsonPrimitive?.content?.substringAfterLast(":") ?: return@mapNotNull null
                    val fields = obj["fields"]?.jsonObject
                    val at = fields?.get("created_at")?.jsonPrimitive?.long ?: return@mapNotNull null
                    val dTag =
                        if (withDTag) {
                            fields["tag_index"]
                                ?.jsonArray
                                ?.firstNotNullOfOrNull { t ->
                                    t.jsonPrimitive.content
                                        .takeIf { it.startsWith("d:") }
                                        ?.substring(2)
                                }
                        } else {
                            null
                        }
                    DocRef(id, at, dTag)
                }
            page.isEmpty() || onPage(page)
        }
    }

    /**
     * Page every match of [selection] out of the document-API visit, calling
     * [onDocuments] with lists of raw document objects (`{"id": …, "fields": …}`);
     * a false return stops the walk. The shared engine behind [visitIds] and
     * [scanAuthors].
     *
     * Two transports, chosen by what the A/B against a live corpus measured:
     * [visitSlices] STREAMED slices walked concurrently (2.2x the serial paged
     * walk, and the plateau was the node's cores, not the transport), or ONE
     * serial paged walk when streaming is off or the server doesn't speak
     * JSON Lines. The paged fallback is deliberately UNSLICED: measured on the
     * same corpus, sliced paged requests return roughly one small bucket per
     * round trip instead of a full page — 11x SLOWER than the serial walk they
     * were meant to speed up. Slicing pays only when the slice streams.
     * Producer pages meet a single consumer through a channel, so
     * [onDocuments] runs strictly serially (callers mutate plain collections)
     * and an early stop cancels every in-flight slice.
     */
    private suspend fun visitPages(
        selection: String,
        fieldSet: String,
        onDocuments: suspend (List<JsonElement>) -> Boolean,
    ): Unit =
        coroutineScope {
            val pages = Channel<List<JsonElement>>(visitSlices)
            val producers =
                launch {
                    try {
                        val streamed =
                            visitStreaming &&
                                coroutineScope {
                                    (0 until visitSlices)
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
     * The serial paged walk: each round trip returns up to [VISIT_PAGE] docs
     * plus a continuation token for the next, with the backend reading
     * [visitConcurrency] buckets in parallel to fill each page. One chain, no
     * slices — see [visitPages] for why slicing this path is a measured loss.
     */
    private suspend fun pagedWalk(
        selection: String,
        fieldSet: String,
        emit: suspend (List<JsonElement>) -> Unit,
    ) {
        val base =
            "${endpoint()}/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE" +
                "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}" +
                "&concurrency=$visitConcurrency"
        var continuation: String? = null
        while (true) {
            val resp = sendVisit(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val json = Json.parseToJsonElement(resp.body()).jsonObject
            json["documents"]?.jsonArray?.takeIf { it.isNotEmpty() }?.let { emit(it) }
            continuation = json["continuation"]?.jsonPrimitive?.content ?: return
        }
    }

    /** How one streamed response ended — see [streamedSlice] for what each means to the resume loop. */
    private enum class StreamEnd { COMPLETE, INTERRUPTED, NOT_JSONL }

    /**
     * Walk one slice as a single streamed JSON-Lines response (`stream=true` +
     * `Accept: application/jsonl`): put lines arrive as the backend visits,
     * with NO per-page round trip — the cost that dominates a paged walk of a
     * large corpus. Returns false (nothing consumed, nothing emitted) when the
     * server doesn't answer in JSON Lines, so the caller can fall back to the
     * paged walk against an older Vespa.
     *
     * EXACTLY-ONCE across broken streams. Vespa emits a `continuation` line
     * whenever a backend bucket completes, and resuming from a token re-streams
     * every bucket still ACTIVE when the stream broke. Bucket concurrency is
     * pinned to 1 here precisely so "active" is at most ONE bucket: docs are
     * buffered until their bucket's continuation line certifies them and only
     * then delivered, so a broken stream (transport error, server timeout,
     * error-severity message) drops the uncertified buffer and resumes from the
     * last token — which re-streams exactly the dropped docs. Nothing delivered
     * twice, nothing lost. The retry budget counts CONSECUTIVE failures
     * (progress resets it): a long walk may hit many transient drops, but a
     * dead engine still fails loudly instead of looping.
     */
    private suspend fun streamedSlice(
        selection: String,
        fieldSet: String,
        sliceId: Int,
        emit: suspend (List<JsonElement>) -> Unit,
    ): Boolean {
        val base =
            "${endpoint()}/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}" +
                "&stream=true&concurrency=1&slices=$visitSlices&sliceId=$sliceId"
        var token: String? = null
        var delivered = false
        var failures = 0
        // Docs since the last continuation line: certain to be exactly the one
        // in-flight bucket (concurrency=1), so at most a few hundred docs.
        val uncertified = ArrayList<JsonElement>()

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
            val end =
                try {
                    streamOnce(url, into = uncertified, onContinuation = { certify(it) })
                } catch (e: IOException) {
                    currentCoroutineContext().ensureActive()
                    if (++failures > QUERY_RETRIES) throw e
                    delay(500L * failures)
                    StreamEnd.INTERRUPTED
                }
            when (end) {
                StreamEnd.COMPLETE -> {
                    return true
                }

                // Loop again: the next request resumes from the last certified token.
                StreamEnd.INTERRUPTED -> {}

                StreamEnd.NOT_JSONL -> {
                    // Only a fallback signal while nothing has been consumed;
                    // mid-walk it is a server misbehaving, not a version gap.
                    require(token == null && !delivered) { "vespa streamed visit stopped answering JSON Lines mid-walk" }
                    return false
                }
            }
        }
    }

    /**
     * One streamed request: open, read JSON-Lines until the stream ends, and
     * classify how it ended. Put lines land in [into] (adapted to the paged
     * walk's `{"id": …, "fields": …}` document shape); each continuation line
     * fires [onContinuation] with its token (null on the final 100% marker,
     * which certifies the tail). Blocking reads run on [Dispatchers.IO]; a
     * cancelled coroutine aborts the in-flight call via the guard child, which
     * is the only thing that unblocks a socket read.
     */
    private suspend fun streamOnce(
        url: String,
        into: MutableList<JsonElement>,
        onContinuation: suspend (String?) -> Unit,
    ): StreamEnd =
        withContext(Dispatchers.IO) {
            val call =
                visitHttp.newCall(
                    Request
                        .Builder()
                        .url(url)
                        .header("Accept", "application/jsonl")
                        .get()
                        .build(),
                )
            // The guard aborts the blocking read when this coroutine is
            // cancelled (early stop, sibling failure) — after normal completion
            // its cancel() is a no-op on the finished call.
            val guard =
                launch(start = CoroutineStart.UNDISPATCHED) {
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
                        // longer parses — that is an interruption to resume
                        // from, not a fatal decode error.
                        val obj = runCatching { Json.parseToJsonElement(line).jsonObject }.getOrNull() ?: break
                        when {
                            "put" in obj -> {
                                val fields = obj["fields"] ?: continue
                                into += JsonObject(mapOf("id" to (obj["put"] ?: continue), "fields" to fields))
                            }

                            "continuation" in obj -> {
                                val c = obj["continuation"]?.jsonObject
                                val t = c?.get("token")?.jsonPrimitive?.content
                                onContinuation(t)
                                if (t == null) complete = true // the final 100% marker carries no token
                            }

                            "message" in obj -> {
                                val m = obj["message"]?.jsonObject
                                if (m?.get("severity")?.jsonPrimitive?.content == "error") {
                                    throw IOException("vespa streamed visit error: ${m["text"]?.jsonPrimitive?.content}")
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

    override suspend fun count(query: EventQuery): Int {
        // A grouping count() over the full match set — NOT root.totalCount, which
        // Vespa caps under the recency `order by`'s match-phase (a 10x+ undercount
        // on large kinds). See [EventYql.buildCount].
        val root = queryRoot(EventYql.buildCount(query) ?: return 0, hits = 0) ?: return 0
        return countIn(root) ?: 0
    }

    override suspend fun countDistinctAuthors(query: EventQuery): Int {
        // `all(group(pubkey) output(count()))` counts the GROUPS — i.e. the
        // distinct pubkeys — not the docs. The count() lands one level deeper
        // than [count]'s (inside the group list), so both share [countIn]'s
        // recursive scan. See [EventYql.buildDistinctCount].
        val root = queryRoot(EventYql.buildDistinctCount(query, "pubkey") ?: return 0, hits = 0) ?: return 0
        return countIn(root) ?: 0
    }

    override suspend fun countByKind(query: EventQuery): Map<Int, Int> {
        // `all(group(kind) each(output(count())))` yields one leaf group per kind,
        // each carrying its `value` (the kind) and a `count()`. See [EventYql.buildKindHistogram].
        val root = queryRoot(EventYql.buildKindHistogram(query) ?: return emptyMap(), hits = 0) ?: return emptyMap()
        val out = LinkedHashMap<Int, Int>()
        kindCountsInto(root, out)
        return out
    }

    /** Collect every leaf group's (value -> count()) pair anywhere under this node. */
    private fun kindCountsInto(
        node: JsonElement,
        out: MutableMap<Int, Int>,
    ) {
        when (node) {
            is JsonObject -> {
                val value = node["value"]?.jsonPrimitive?.intOrNull
                val count =
                    node["fields"]
                        ?.jsonObject
                        ?.get("count()")
                        ?.jsonPrimitive
                        ?.intOrNull
                if (value != null && count != null) out[value] = count
                node["children"]?.let { kindCountsInto(it, out) }
            }

            is JsonArray -> {
                node.forEach { kindCountsInto(it, out) }
            }

            else -> {}
        }
    }

    /** The first `count()` grouping output anywhere under this node — flat for [count], nested under the group list for [countDistinctAuthors]. */
    private fun countIn(node: JsonElement): Int? =
        when (node) {
            is JsonObject -> {
                node["fields"]
                    ?.jsonObject
                    ?.get("count()")
                    ?.jsonPrimitive
                    ?.intOrNull
                    ?: node["children"]?.let { countIn(it) }
            }

            is JsonArray -> {
                node.firstNotNullOfOrNull { countIn(it) }
            }

            else -> {
                null
            }
        }

    override suspend fun distinctAuthors(query: EventQuery): Set<String> {
        val root = queryRoot(EventYql.buildDistinctAuthors(query) ?: return emptySet(), hits = 0) ?: return emptySet()
        // group(pubkey) nests a grouplist whose leaf `group:` nodes each carry a
        // pubkey as `value`. Walk the tree and collect every group value.
        val authors = LinkedHashSet<String>()

        fun collect(node: JsonObject) {
            (node["value"] as? JsonPrimitive)?.let { if (node["id"]?.jsonPrimitive?.content?.startsWith("group:") == true) authors += it.content }
            node["children"]?.jsonArray?.forEach { collect(it.jsonObject) }
        }
        collect(root)
        return authors
    }

    /**
     * Complete author scan via the document-API visit (sliced and
     * continuation-paged, see [visitPages]), projecting only `pubkey`.
     * [distinctAuthors]'s grouping is complete too, but it materializes every
     * group in one response; this streams, which is what the corpus-wide
     * guard-owner Bloom preload needs — a missed author would be a false
     * negative.
     */
    override suspend fun scanAuthors(query: EventQuery): Set<String> {
        val selection = EventSelection.build(query) ?: return super.scanAuthors(query)
        val authors = HashSet<String>()
        visitPages(selection, "$DOCTYPE:pubkey") { documents ->
            documents.forEach { d ->
                d.jsonObject["fields"]
                    ?.jsonObject
                    ?.get("pubkey")
                    ?.jsonPrimitive
                    ?.content
                    ?.let { authors += it }
            }
            true
        }
        return authors
    }

    /**
     * POST [vq] to `/search/` and return the raw response body. POSTs because a
     * filter with hundreds of ids or authors builds YQL far past any sane URL
     * length. The recall path ([search]) streams this straight into typed docs;
     * the grouping paths wrap it in [queryRoot] for tree walking.
     */
    private suspend fun queryBody(
        vq: VespaQuery,
        hits: Int,
    ): String {
        val body =
            buildJsonObject {
                put("yql", vq.yql)
                put("hits", hits.toString())
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
            }.toString()
        val req =
            Request
                .Builder()
                .url("${endpoint()}/search/")
                .post(body.toRequestBody(jsonMedia))
                .build()
        // A busy engine sheds load transiently (504 "Summary data is
        // incomplete" under heavy concurrent summary fills). One failed page
        // must not kill a whole multi-hour sync, so 5xx gets brief retries.
        val resp = sendRetrying(req)
        require(resp.statusCode() < 400) { "vespa search ${resp.statusCode()}: ${resp.body().take(300)}" }
        return resp.body()
    }

    /**
     * The two funnels every `/search/` response passes through — this one for the
     * recall paths (streamed straight into DTOs) and [queryRoot] for the
     * grouping/count paths (which need the tree) — both verify coverage, so no
     * caller can accidentally accept a degraded answer.
     */
    private suspend fun searchRoot(
        vq: VespaQuery,
        hits: Int,
    ): SearchRoot =
        DECODER
            .decodeFromString<SearchEnvelope>(queryBody(vq, hits))
            .root
            .also { it.coverage.requireComplete() }

    /** The grouping/count paths need the full tree; [searchRoot] does not (it decodes hits directly). */
    private suspend fun queryRoot(
        vq: VespaQuery,
        hits: Int,
    ): JsonObject? =
        Json
            .parseToJsonElement(queryBody(vq, hits))
            .jsonObject["root"]
            ?.jsonObject
            ?.also { root -> root["coverage"]?.let { DECODER.decodeFromJsonElement(SearchCoverage.serializer(), it) }?.requireComplete() }

    /**
     * A degraded response is a WRONG answer, not a slow one. Vespa does not fail a
     * query it cannot finish — it returns HTTP 200 with however many hits it had
     * when it gave up and `coverage.full: false`. That is indistinguishable, at
     * every call site, from a filter that genuinely matched that few. On the read
     * path it silently under-delivers; on a write path it is worse, because the
     * dedup and NIP-09/62 guards decide by "did the query find it" and a partial
     * answer resurrects a deleted event. So it fails loudly instead.
     */
    private fun SearchCoverage.requireComplete() =
        require(full) {
            "vespa searched only $coverage% of the corpus (degraded: ${degraded ?: "unspecified"}); " +
                "the response is a PARTIAL answer, not a small one, so it is refused rather than returned"
        }

    /**
     * Rebuild a doc from the decoded summary. `tags` is the one field still parsed
     * per hit — the store keeps it as a JSON string. The search columns map
     * positionally to [SearchFields] (all-null on the trimmed recall path, so it
     * equals [SearchFields.NONE]; populated on a full get).
     */
    private fun VespaSummary.toDoc(): EventDoc? {
        if (id.isEmpty()) return null
        return EventDoc(
            id = id,
            pubkey = pubkey,
            createdAt = createdAt,
            kind = kind,
            tags = Json.parseToJsonElement(tags).jsonArray.map { row -> row.jsonArray.map { it.jsonPrimitive.content } },
            content = content,
            sig = sig,
            owner = owner ?: pubkey,
            search = SearchFields(name, displayName, about, nip05, lud16, website, primary, secondary, text, location),
        )
    }

    /**
     * The summary as a [RawEvent] with NO tag parse: `tags` rides through as the
     * exact JSON string Vespa stored (`[["p","…"],…]`), which is precisely what a
     * relay's serializer splices after `"tags":`. This is the whole raw-recall
     * win — the tag column is never turned into per-tag objects and back.
     */
    private fun VespaSummary.toRaw(): RawEvent? {
        if (id.isEmpty()) return null
        return RawEvent(id, pubkey, createdAt, kind, tags, content, sig)
    }

    private suspend fun send(url: String): HttpResp =
        sendRetrying(
            Request
                .Builder()
                .url(url)
                .get()
                .build(),
        )

    /** [send] on the [visitHttp] client — visit page requests carry a read deadline (see [visitHttp]). */
    private suspend fun sendVisit(url: String): HttpResp =
        sendRetrying(
            Request
                .Builder()
                .url(url)
                .get()
                .build(),
            visitHttp,
        )

    /**
     * Send [req], briefly retrying transient overload: 5xx (the engine sheds
     * load under heavy concurrent summary fills) AND 429 (the document API
     * rejects past 256 enqueued requests — pushback, not failure). Shared by the
     * query, get, and visit paths. The full-corpus visit walk is exactly a place
     * where one 504/429 page must not abort the whole scan.
     *
     * Transport [IOException]s are retried on the same budget. A response body that
     * stalls past the read timeout is the same class of transient overload as a 503
     * — the engine was too busy to finish streaming — and it arrives as an exception
     * rather than a status code, so treating it as fatal would abort a visit walk
     * for a condition the next attempt usually clears.
     */
    private suspend fun sendRetrying(
        req: Request,
        client: OkHttpClient = http,
    ): HttpResp {
        var attempt = 0
        while (true) {
            val resp =
                try {
                    client.newCall(req).await()
                } catch (e: IOException) {
                    if (attempt++ >= QUERY_RETRIES) throw e
                    delay(500L * attempt)
                    continue
                }
            if ((resp.statusCode() in 500..599 || resp.statusCode() == 429) && attempt++ < QUERY_RETRIES) {
                delay(500L * attempt)
                continue
            }
            return resp
        }
    }

    /** Minimal response holder so the get/search/visit/count call sites keep their JDK-style statusCode()/body() shape. */
    private class HttpResp(
        private val code: Int,
        private val bodyText: String,
    ) {
        fun statusCode() = code

        fun body() = bodyText
    }

    /**
     * Bridge OkHttp's async [Call.enqueue] to a cancellable suspend. The body is
     * read on OkHttp's callback thread (inside [Response.use] so the connection is
     * released), so the whole read stays non-blocking, exactly as the old
     * `sendAsync(...).await()` did.
     *
     * [onResponse] must complete the continuation on EVERY path, including a body
     * read that throws. OkHttp sets its internal `signalledCallback` flag *before*
     * invoking [onResponse], so anything thrown in here is only logged
     * ("Callback failure for call to …", at INFO) and is NEVER routed to
     * [onFailure]. An unguarded `body.string()` that times out mid-stream would
     * therefore leave this coroutine suspended forever — the same
     * hang-behind-the-single-writer-store deadlock that [feedParams] guards on the
     * write path, reached by a different door.
     */
    private suspend fun Call.await(): HttpResp =
        suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation { runCatching { cancel() } }
            enqueue(
                object : Callback {
                    override fun onFailure(
                        call: Call,
                        e: IOException,
                    ) {
                        if (!cont.isCancelled) cont.resumeWithException(e)
                    }

                    override fun onResponse(
                        call: Call,
                        response: Response,
                    ) {
                        val result = runCatching { response.use { HttpResp(it.code, it.body.string()) } }
                        if (cont.isCancelled) return
                        result
                            .onSuccess { cont.resume(it) }
                            .onFailure { cont.resumeWithException(it) }
                    }
                },
            )
        }

    /**
     * One-line feed-client health for status lines: cumulative acks, the LIVE
     * in-flight window, and per-request HTTP latency. Together these tell "the
     * engine is slow" apart from "the client isn't pushing" at a glance. A
     * starved window shows tiny inflight at low latency; a saturated engine
     * shows a big window at high latency.
     */
    fun feedGauge(): String {
        val s = feed.stats()
        // Non-2xx responses get retried and usually succeed: pushback, not
        // loss (a big window ramping down shows a burst of 429s here). Only
        // transport exceptions are worth shouting about.
        val retried = s.responses() - s.successes()
        return "feed ok ${s.successes()} inflight ${s.inflight()} lat ${s.averageLatencyMillis()}ms" +
            (if (retried > 0) " retry $retried" else "") +
            if (s.exceptions() > 0) " EXC ${s.exceptions()}" else ""
    }

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    override fun close() = feed.close(true)

    // --- streaming decode DTOs (avoid the JsonElement tree on the recall path) ---

    /** `/search/` response: the hit children plus [SearchCoverage]; grouping/meta are ignored. */
    @Serializable
    private class SearchEnvelope(
        val root: SearchRoot = SearchRoot(),
    )

    @Serializable
    private class SearchRoot(
        val children: List<SearchHit> = emptyList(),
        val coverage: SearchCoverage = SearchCoverage(),
    )

    /**
     * How much of the corpus the engine actually searched. Vespa DEGRADES rather
     * than failing — a query it gives up on comes back HTTP 200 with fewer hits
     * and `full: false` — so this is the only thing separating "here is
     * everything that matched" from "here is some of it". Defaults to complete:
     * responses with no coverage block at all (document gets, the test double's
     * older shapes) are not search results and have nothing to degrade.
     */
    @Serializable
    private class SearchCoverage(
        val full: Boolean = true,
        val coverage: Int = 100,
        val degraded: JsonObject? = null,
    )

    @Serializable
    private class SearchHit(
        val fields: VespaSummary? = null,
    )

    /** `/document/v1/…` get response. */
    @Serializable
    private class DocEnvelope(
        val fields: VespaSummary? = null,
    )

    /**
     * The summary a hit/doc carries (unknown keys ignored by [DECODER]). The
     * recall path's trimmed select returns only the NIP-01 fields, so the search
     * columns decode as null there (-> SearchFields.NONE); a full document get
     * carries them, keeping get() lossless.
     */
    @Serializable
    private class VespaSummary(
        val id: String = "",
        val pubkey: String = "",
        @SerialName("created_at") val createdAt: Long = 0,
        val kind: Int = 0,
        val tags: String = "[]",
        val content: String = "",
        val sig: String = "",
        val owner: String? = null,
        val name: String? = null,
        @SerialName("display_name") val displayName: String? = null,
        val about: String? = null,
        val nip05: String? = null,
        val lud16: String? = null,
        val website: String? = null,
        @SerialName("search_primary") val primary: String? = null,
        @SerialName("search_secondary") val secondary: String? = null,
        @SerialName("search_text") val text: String? = null,
        @SerialName("search_location") val location: String? = null,
    )

    private companion object {
        const val NAMESPACE = "event"
        const val DOCTYPE = "event"

        /** Lenient decoder: the response carries documentid/sddocname/relevance and, on some paths, extra fields we don't model. */
        val DECODER = Json { ignoreUnknownKeys = true }

        /** Concurrent document-API gets for a pure-id lookup. Gets are light (no summary stage to overrun like big searches), so this floats well above QUERY_FANOUT. */
        const val ID_GET_FANOUT = 32

        /** Newest first (created_at desc, id asc tiebreak) — the same order the search path and the store apply. */
        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)

        /**
         * Concurrent requests to the engine, total and per host — the same
         * number, because every request goes to the same host and the per-host
         * limit is therefore the only one that binds.
         */
        const val MAX_CONCURRENT_REQUESTS = 1024

        /** Docs asked for per visit response (Vespa's per-request ceiling is 1024). */
        const val VISIT_PAGE = 1024

        /** Default parallel STREAMED visit slices — see [visitSlices] for why and the env override. */
        const val VISIT_SLICES = 8

        /**
         * Default bucket concurrency for the serial paged fallback — see
         * [visitConcurrency]. 8 halved the fallback's wall clock in the A/B and
         * keeps total visitor pressure at 8 sessions (one serial walk); the
         * measured wedge needed ~64.
         */
        const val VISIT_CONCURRENCY = 8

        /**
         * Read deadline for visit requests ([visitHttp]): pages are small and
         * streams deliver continuously, so this much silence means a wedged
         * visitor session, not a busy engine. Generous enough for a loaded
         * node's worst honest page.
         */
        const val VISIT_READ_TIMEOUT_SECONDS = 120L

        /** Brief 5xx retries per query (transient engine load-shedding, not correctness). */
        const val QUERY_RETRIES = 3

        /**
         * Liveness probe on the read connection, in place of the read/whole-call
         * deadlines a query is no longer allowed to have. Unanswered HTTP/2 PINGs
         * fail the connection with a ProtocolException, which [sendRetrying]
         * retries — so a severed or black-holed socket is caught in seconds while a
         * legitimately long query runs undisturbed. That distinction is the whole
         * point: a deadline cannot make it, a ping can.
         */
        const val PING_INTERVAL_SECONDS = 15L

        /**
         * Per-operation feed deadline. The feed client's retry strategy handles
         * transient errors, but a silently half-dead HTTP/2 connection (for
         * example, one severed by an engine restart) makes `await()` hang
         * FOREVER with no deadline, which deadlocks the single-writer store
         * behind it. A timeout turns that hang into a retryable failure.
         */
        fun feedParams(): OperationParameters = OperationParameters.empty().timeout(Duration.ofSeconds(30))
    }
}
