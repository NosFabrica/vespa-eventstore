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
import kotlinx.coroutines.future.await
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
import kotlinx.serialization.json.put
import java.net.URLEncoder
import java.time.Duration

/**
 * The real [EventIndex]: Vespa over HTTP. Writes go through Vespa's official
 * feed client (HTTP/2 multiplexed, per-doc ordering, retries built in) and are
 * AWAITED before returning. The store's read-your-writes contract needs the
 * ack, and proton makes an acked write visible to search. Reads use the plain
 * document API (get) and `/search/` (query). Transport concerns live beside
 * this class: [VespaFeed] builds and tunes the feed client, [VespaHttp] owns
 * the read connection's protocol, liveness and retry behavior.
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
) : EventIndex {
    private val urls: List<String> = endpoints.ifEmpty { listOf(baseUrl) }.map { it.trimEnd('/') }

    private val nextUrl =
        java.util.concurrent.atomic
            .AtomicInteger()

    /** The endpoint for one HTTP read — round-robin across [urls]. */
    private fun endpoint(): String = urls[Math.floorMod(nextUrl.getAndIncrement(), urls.size)]

    // Feed-client construction and tuning (connection budget, throttle floor,
    // retry strategy) live in [VespaFeed]; operations below go through its client.
    private val feed = VespaFeed(urls)

    // Read transport (h2c prior knowledge, ping liveness, transient-overload
    // retries) lives in [VespaHttp]; the read paths below only build URLs/bodies.
    private val http = VespaHttp()

    // ADDRESS-KEYED mode (VESPA_ADDRESS_KEYED=1): replaceable/addressable events
    // are stored under their NIP-01 address as the document id, so the engine
    // enforces newest-wins with a conditional put (see [putIfNewer]) instead of
    // the client's read-then-supersede. Regular events stay id-keyed. Default OFF
    // — the whole scheme is opt-in until measured. When ON, id lookups can no
    // longer ride the document-API get (replaceables live under an address docid),
    // so they route through the `id`-attribute search, which finds both.
    // Accepts "1" as well as "true" — the comment above prescribes =1, and the
    // multi-writer deployment that needs engine-side supersession for
    // correctness must not silently run without it.
    private val addressKeyed = System.getenv("VESPA_ADDRESS_KEYED").let { it == "1" || it?.toBooleanStrictOrNull() == true }

    // Under address-keying the engine enforces newest-wins (conditional put), so
    // the bulk path skips its version-read stage and calls putIfNewer instead.
    override val supersedesViaPut: Boolean get() = addressKeyed

    /** The document id for [doc]: its address when address-keyed and replaceable, else its event id. */
    private fun docIdOf(doc: EventDoc): String = if (addressKeyed) doc.addressOrNull() ?: doc.id else doc.id

    override suspend fun get(id: String): EventDoc? {
        // Address-keyed replaceables aren't at the id docid; resolve by the id
        // attribute instead (finds regular AND replaceable, no doc-API get).
        if (addressKeyed) return searchById(id)
        val resp = http.get("${endpoint()}/document/v1/$NAMESPACE/$DOCTYPE/docid/$id")
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
        feed.client.put(
            DocumentId.of(NAMESPACE, DOCTYPE, docIdOf(doc)),
            buildJsonObject { put("fields", doc.indexFields()) }.toString(),
            feedParams(),
        )

    private fun removeOp(id: String) = feed.client.remove(DocumentId.of(NAMESPACE, DOCTYPE, id), feedParams())

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
        // The id is interpolated into the engine condition below, so it obeys
        // the module's injection rule (64-hex before it reaches an expression —
        // see EventYql.hexIn). Store-built docs always pass; a non-hex id from
        // a direct caller falls back to the read-then-supersede default, which
        // builds no expression from it.
        if (!Hex.isHex64(doc.id)) return super.putIfNewer(doc)
        val condition =
            "event.created_at < ${doc.createdAt} or " +
                "(event.created_at == ${doc.createdAt} and event.id > \"${doc.id}\")"
        val result =
            feed.client
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
        feed.client.remove(DocumentId.of(NAMESPACE, DOCTYPE, docId), feedParams()).await()
    }

    /**
     * Bulk remove with the docs in hand: the docid comes straight from each doc
     * ([docIdOf]), so the address-keyed resolve-by-get that [removeAll] must do
     * per id disappears. Same pipelining as [putAll].
     */
    override suspend fun removeDocs(docs: List<EventDoc>) {
        docs.map { feed.client.remove(DocumentId.of(NAMESPACE, DOCTYPE, docIdOf(it)), feedParams()) }.forEach { it.await() }
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
            // A present limit <= 0 is the "matches nothing" sentinel; only the
            // search path implements it (EventYql.build -> null), so it must not
            // take this shortcut (List.take(-1) throws).
            (limit == null || limit > 0) &&
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
        val base =
            "${endpoint()}/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}"
        var continuation: String? = null
        while (true) {
            val resp = http.get(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            // Streamed into flat DTOs, like the search path: a 25M-doc walk is
            // ~24k pages, and a JsonElement tree per page is pure garbage here.
            val env = DECODER.decodeFromString<VisitEnvelope>(resp.body())
            val page =
                env.documents.mapNotNull { d ->
                    if (d.id.isEmpty()) return@mapNotNull null
                    val at = d.fields?.createdAt ?: return@mapNotNull null
                    val dTag =
                        if (withDTag) {
                            d.fields.tagIndex?.firstNotNullOfOrNull { t -> t.takeIf { it.startsWith("d:") }?.substring(2) }
                        } else {
                            null
                        }
                    DocRef(d.id.substringAfterLast(":"), at, dTag)
                }
            if (page.isNotEmpty() && !onPage(page)) return
            continuation = env.continuation ?: return
        }
    }

    /**
     * One page of FULL docs through the document API's visit — the reindex
     * primitive. `[document]` selects every real document field (the search
     * columns included: they are stored fields, written by [putOp]), so the
     * page decodes through the same [VespaSummary] a get returns. The
     * continuation token rides in [DocsPage.continuation], opaque to callers.
     */
    override suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage {
        val selection = EventSelection.build(query) ?: return super.visitDocsPage(query, resumeFrom, maxDocs)
        val base =
            "${endpoint()}/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=${maxDocs.coerceIn(1, VISIT_PAGE)}&fieldSet=${URLEncoder.encode("[document]", "UTF-8")}"
        val resp = http.get(resumeFrom?.let { "$base&continuation=$it" } ?: base)
        require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
        val env = DECODER.decodeFromString<DocVisitEnvelope>(resp.body())
        return DocsPage(env.documents.mapNotNull { it.fields?.toDoc() }, env.continuation)
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
     * Complete author scan via the document-API visit (continuation-paged),
     * projecting only `pubkey`. [distinctAuthors]'s grouping is complete too, but
     * it materializes every group in one response; this streams, which is what
     * the corpus-wide guard-owner Bloom preload needs — a missed author would be
     * a false negative.
     */
    override suspend fun scanAuthors(query: EventQuery): Set<String> {
        val selection = EventSelection.build(query) ?: return super.scanAuthors(query)
        val fieldSet = "$DOCTYPE:pubkey"
        val base =
            "${endpoint()}/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}"
        val authors = HashSet<String>()
        var continuation: String? = null
        while (true) {
            val resp = http.get(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val env = DECODER.decodeFromString<VisitEnvelope>(resp.body())
            env.documents.forEach { d -> d.fields?.pubkey?.let { authors += it } }
            continuation = env.continuation ?: return authors
        }
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
        // A busy engine sheds load transiently (504 "Summary data is
        // incomplete" under heavy concurrent summary fills). One failed page
        // must not kill a whole multi-hour sync, so 5xx gets brief retries
        // (inside VespaHttp).
        val resp = http.postJson("${endpoint()}/search/", body)
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

    /** One-line feed-client health for status lines; see [VespaFeed.gauge]. */
    fun feedGauge(): String = feed.gauge()

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    override fun close() = feed.close()

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

    /** A visit page for the projected walks ([visitIds]/[scanAuthors]) — flat DTOs, no JsonElement tree. */
    @Serializable
    private class VisitEnvelope(
        val documents: List<VisitDoc> = emptyList(),
        val continuation: String? = null,
    )

    @Serializable
    private class VisitDoc(
        val id: String = "",
        val fields: VisitFields? = null,
    )

    @Serializable
    private class VisitFields(
        @SerialName("created_at") val createdAt: Long? = null,
        @SerialName("tag_index") val tagIndex: List<String>? = null,
        val pubkey: String? = null,
    )

    /** A `[document]` visit page: full summaries plus the continuation ([visitDocsPage]). */
    @Serializable
    private class DocVisitEnvelope(
        val documents: List<DocVisitDoc> = emptyList(),
        val continuation: String? = null,
    )

    @Serializable
    private class DocVisitDoc(
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

        /** Docs asked for per visit response (Vespa's per-request ceiling is 1024). */
        const val VISIT_PAGE = 1024

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
