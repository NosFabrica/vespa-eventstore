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
import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.ScoredHit
import com.nosfabrica.vespa.eventstore.engine.async.mapBounded
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.DegradedReads
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.engine.metrics.currentActivity
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventSelection
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.engine.query.VespaQuery
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.coroutines.future.await
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.time.Duration
import java.util.concurrent.CompletableFuture

/**
 * The real [EventIndex]: Vespa over HTTP. Writes go through Vespa's official
 * feed client (HTTP/2 multiplexed, per-doc ordering, retries built in) and are
 * AWAITED before returning — the store's read-your-writes contract needs the
 * ack, and proton makes an acked write visible to search. Reads use the plain
 * document API (get) and `/search/` (query).
 *
 * The collaborators each own one concern: [VespaFeed] builds and tunes the
 * feed client, [VespaHttp] owns the read connections' protocol and retry
 * behavior, [VespaVisits] walks full-corpus visits, [SchemaFallbacks] demotes
 * queries a legacy serving schema would 400, and [RecencyPlanner] windows bare
 * recency scans via count probes.
 *
 * There is no hit ceiling here: a query with a `limit` gets exactly that, and
 * one without gets every match — bounding cost is the caller's job, per query.
 * Counts use a grouping `count()` over the full match set (see
 * [EventYql.buildCount]) — NOT `root.totalCount`, which the recency
 * `order by`'s match-phase caps on a large corpus (a 10x+ undercount).
 */
class VespaEventIndex(
    baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
    /**
     * All container endpoints of the cluster; empty = just [baseUrl]. On a
     * multi-container deployment, naming every endpoint here beats a load
     * balancer for the WRITE path: the feed client natively spreads its HTTP/2
     * connections across all of them. Reads round-robin per request.
     */
    endpoints: List<String> = emptyList(),
    // Ids per cursor page of a snapshot walk (visitIds). Exposed so a test can
    // drive the multi-page and tie-group paths without seeding tens of
    // thousands of docs; production has no reason to change it.
    private val idPageSize: Int = PAGE_IDS,
    private val probeIds: Int = PROBE_IDS,
    /**
     * Independent STREAMED slices a full-corpus visit is split into, each
     * walked concurrently as its own JSON-Lines stream. Each slice is roughly
     * one visitor thread on the content node, so the walk scales near-linearly
     * until slices cover the node's cores, then ~15% more from 2x
     * oversubscription, then nothing (measured). The default is 2 x this
     * host's cores — right when the store runs beside its content node; set
     * `VESPA_VISIT_SLICES` to 2 x the CONTENT NODE's cores when they differ.
     */
    private val visitSlices: Int =
        (
            System.getenv("VESPA_VISIT_SLICES")?.toIntOrNull()
                ?: (2 * Runtime.getRuntime().availableProcessors()).coerceIn(VespaVisits.SLICES_MIN, VespaVisits.SLICES_MAX)
        ).coerceAtLeast(1),
    /**
     * Backend bucket parallelism WITHIN each paged visit request, used only by
     * the paged fallback. `VESPA_VISIT_CONCURRENCY` overrides (Vespa accepts
     * 1..100). See [VespaVisits] for the measured rationale.
     */
    private val visitConcurrency: Int =
        (System.getenv("VESPA_VISIT_CONCURRENCY")?.toIntOrNull() ?: VespaVisits.DEFAULT_BUCKET_CONCURRENCY)
            .coerceIn(1, 100),
    /** Stream visit slices as JSON-Lines instead of paging; `VESPA_VISIT_STREAM=0` forces the paged fallback. See [VespaVisits]. */
    private val visitStreaming: Boolean =
        System.getenv("VESPA_VISIT_STREAM")?.let { it != "0" && !it.equals("false", ignoreCase = true) } ?: true,
    /** Window-plan bare recency scans via count probes; `VESPA_QUERY_PLANNER=0` disables. See [RecencyPlanner]. */
    private val queryPlanning: Boolean =
        System.getenv("VESPA_QUERY_PLANNER")?.let { it != "0" && !it.equals("false", ignoreCase = true) } ?: true,
    /**
     * WHERE ENGINE-LEVEL COST IS PUBLISHED, or null to publish none.
     *
     * The rank profile, the documents matched and Vespa's own timing split are
     * knowable only here: a port decorator sees an `EventQuery`, not the
     * profile `EventYql` compiled it into, and `totalCount` and coverage live
     * in the response. That is the correct split rather than a compromise —
     * port-level metering stays engine-agnostic and works for
     * `InMemoryEventIndex`, which has no rank profiles at all.
     */
    private val ledger: CostLedger? = null,
) : EventIndex {
    private val urls: List<String> = endpoints.ifEmpty { listOf(baseUrl) }.map { it.trimEnd('/') }

    private val nextUrl =
        java.util.concurrent.atomic
            .AtomicInteger()

    /** The endpoint for one HTTP read — round-robin across [urls]. */
    private fun endpoint(): String = urls[Math.floorMod(nextUrl.getAndIncrement(), urls.size)]

    private val feed = VespaFeed(urls)

    private val http = VespaHttp()

    /**
     * The headroom probe, on the endpoint this client already uses:
     * `/metrics/v2/values` reports EVERY content node, so one endpoint answers
     * for the cluster. Built here because this is where `http` and the
     * endpoint rotation live.
     */
    private val resources = EngineResources(http, ::endpoint)

    private val visits = VespaVisits(http, ::endpoint, visitSlices, visitConcurrency, visitStreaming)

    private val fallbacks = SchemaFallbacks()

    private val planner = RecencyPlanner(queryPlanning, fallbacks) { count(it) }

    // ADDRESS-KEYED mode (VESPA_ADDRESS_KEYED=1): replaceable/addressable events
    // are stored under their NIP-01 address as the document id, so the engine
    // enforces newest-wins with a conditional put (see [putIfNewer]) instead of
    // the client's read-then-supersede. Regular events stay id-keyed. Default
    // OFF. When ON, id lookups can no longer ride the document-API get, so they
    // route through the `id`-attribute search, which finds both.
    private val addressKeyed = System.getenv("VESPA_ADDRESS_KEYED").let { it == "1" || it?.toBooleanStrictOrNull() == true }

    // Under address-keying the engine enforces newest-wins (conditional put), so
    // the bulk path skips its version-read stage and calls putIfNewer instead.
    override val supersedesViaPut: Boolean get() = addressKeyed

    /** The document id for [doc]: its address when address-keyed and replaceable, else its event id. */
    private fun docIdOf(doc: EventDoc): String = if (addressKeyed) doc.addressOrNull() ?: doc.id else doc.id

    // ---- get / put / remove -------------------------------------------------

    override suspend fun get(id: String): EventDoc? {
        // Address-keyed replaceables aren't at the id docid; resolve by the id
        // attribute instead (finds regular AND replaceable).
        if (addressKeyed) return searchById(id)
        val resp = http.get("${endpoint()}/document/v1/$EVENT_NAMESPACE/$EVENT_DOCTYPE/docid/$id")
        if (resp.statusCode() == 404) return null
        require(resp.statusCode() < 400) { "vespa get ${resp.statusCode()}: ${resp.body().take(300)}" }
        return VESPA_JSON.decodeFromString<DocEnvelope>(resp.body()).fields?.toDoc(withNearState = true)
    }

    /** One doc by its `id` attribute via the search stack — the id-lookup path under address-keying. */
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
            DocumentId.of(EVENT_NAMESPACE, EVENT_DOCTYPE, docIdOf(doc)),
            buildJsonObject { put("fields", doc.indexFields(includeNear = fallbacks.nearFieldsAvailable)) }.toString(),
            feedParams(),
        )

    /**
     * The write-side twin of [SchemaFallbacks.withNearFallback]: await [ops],
     * and if the engine refused a document for naming a near column it does not
     * have, drop those columns and feed again.
     *
     * Without this net, upgrading the library against an already-serving cluster
     * (where `deployIfAbsent` deliberately does NOT redeploy — the operator owns
     * services.xml) made every insert of a searchable event throw `Field
     * 'name_near' is not defined in document type 'event'`, stopping ingest dead
     * on a jar upgrade. Pinned by VespaEventIndexTest.
     *
     * FAIL OPEN, like every other demotion here: the document lands without its
     * prefix/fuzzy columns — the state a pre-near corpus is already in, which
     * reindexFullTextSearch repairs once the schema catches up.
     *
     * The retry re-feeds EVERY document, not just the refused ones: puts are
     * idempotent overwrites, the flag is now flipped, and this happens at most
     * once per process — tracking which futures failed would cost more than the
     * duplicate pass.
     */
    private suspend fun awaitPuts(
        docs: List<EventDoc>,
        ops: List<CompletableFuture<Result>>,
    ) {
        var refused: Throwable? = null
        for (op in ops) {
            try {
                op.await()
            } catch (e: Throwable) {
                // Await the rest before reacting: they are already in flight,
                // and leaving futures unawaited would surface later as
                // unhandled completions on an unrelated call.
                if (!fallbacks.isMissingNearField(e.message)) throw e
                refused = e
            }
        }
        if (refused == null) return
        fallbacks.markNearFieldsMissing()
        docs.map { putOp(it) }.forEach { it.await() }
    }

    private fun removeOp(id: String) = feed.client.remove(DocumentId.of(EVENT_NAMESPACE, EVENT_DOCTYPE, id), feedParams())

    override suspend fun put(doc: EventDoc) {
        awaitPuts(listOf(doc), listOf(putOp(doc)))
    }

    /**
     * Puts stay in flight together — the feed client multiplexes them over
     * HTTP/2 — in chunks of [FEED_CHUNK]: the per-operation deadline
     * ([feedParams]) is measured from ENQUEUE, not from dispatch, and the
     * throttler admits about `connections x streams` at a time, so a single
     * call that enqueued far more than that had its tail expire in the queue
     * before its first dispatch — most of the batch landed and the call still
     * threw. A chunk the size of the admission window loses no pipelining.
     */
    override suspend fun putAll(docs: List<EventDoc>) {
        docs.chunked(FEED_CHUNK).forEach { chunk -> awaitPuts(chunk, chunk.map { putOp(it) }) }
    }

    /**
     * Address-keyed newest-wins as a single server-side conditional put: create
     * if absent, else overwrite only when the incoming version beats the stored
     * one (higher created_at, or same created_at and a LOWER id — NIP-01).
     * Stale versions come back `conditionNotMet` — rejected by the engine, no
     * client read. Falls back to the search-then-supersede default when
     * address-keying is off or the doc is not replaceable.
     */
    override suspend fun putIfNewer(doc: EventDoc): Boolean {
        val address = doc.addressOrNull()
        if (!addressKeyed || address == null) return super.putIfNewer(doc)
        // The id is interpolated into the engine condition, so it obeys the
        // module's injection rule (64-hex before it reaches an expression).
        // A non-hex id from a direct caller falls back to the default, which
        // builds no expression from it.
        if (!Hex.isHex64(doc.id)) return super.putIfNewer(doc)
        val condition =
            "event.created_at < ${doc.createdAt} or " +
                "(event.created_at == ${doc.createdAt} and event.id > \"${doc.id}\")"

        // Same write-side near-column net as [awaitPuts]; this path builds its
        // own operation (the test-and-set condition), so it carries its own.
        suspend fun attempt(): Result =
            feed.client
                .put(
                    DocumentId.of(EVENT_NAMESPACE, EVENT_DOCTYPE, address),
                    buildJsonObject { put("fields", doc.indexFields(includeNear = fallbacks.nearFieldsAvailable)) }.toString(),
                    feedParams().createIfNonExistent(true).testAndSetCondition(condition),
                ).await()
        val result =
            try {
                attempt()
            } catch (e: Throwable) {
                if (!fallbacks.isMissingNearField(e.message)) throw e
                fallbacks.markNearFieldsMissing()
                attempt()
            }
        // A transport/engine failure completes the future exceptionally (never
        // a Result here), so the else guards only a future enum addition.
        return when (result.type()) {
            Result.Type.success -> true
            Result.Type.conditionNotMet -> false
            else -> error("vespa conditional put for $address returned ${result.type()}")
        }
    }

    override suspend fun remove(id: String) {
        // Under address-keying the doc may live at an address docid, so resolve
        // it before removing.
        val docId = if (addressKeyed) get(id)?.let { docIdOf(it) } ?: id else id
        feed.client.remove(DocumentId.of(EVENT_NAMESPACE, EVENT_DOCTYPE, docId), feedParams()).await()
    }

    /** All removes in flight together over HTTP/2, like [putAll]. */
    override suspend fun removeAll(ids: List<String>) {
        // Address-keyed replaceables live at an address docid, not their event
        // id, so a raw removeOp(id) would silently miss them: resolve each the
        // way remove() does, bounded-concurrent.
        if (addressKeyed) {
            ids.mapBounded(ID_GET_FANOUT) { remove(it) }
            return
        }
        ids.chunked(FEED_CHUNK).forEach { chunk -> chunk.map { removeOp(it) }.forEach { it.await() } }
    }

    /** Bulk remove with the docs in hand: the docid comes straight from each doc, so no address-keyed resolve-by-get per id. Chunked like [putAll]. */
    override suspend fun removeDocs(docs: List<EventDoc>) {
        docs.chunked(FEED_CHUNK).forEach { chunk ->
            chunk.map { feed.client.remove(DocumentId.of(EVENT_NAMESPACE, EVENT_DOCTYPE, docIdOf(it)), feedParams()) }.forEach { it.await() }
        }
    }

    // ---- recall -------------------------------------------------------------

    /**
     * The summary-free existence check: `select id` under the attribute-only
     * `dedup` summary class, so proton answers membership from the id attribute
     * in memory — no disk summary fetch, ~76% less response to transfer; 2.2x
     * the full-summary path end to end (see benchmark/README.md). Works
     * identically under address-keying: the `id` ATTRIBUTE carries the event id
     * whatever the docid is — the reason this stays on the search stack.
     */
    override suspend fun existingIds(ids: List<String>): Set<String> {
        // Demotion first: a demoted client must not build (and discard) the
        // ~35KB YQL per chunk — super rides search(), which handles the
        // no-valid-ids case itself.
        if (!fallbacks.dedupSummaryAvailable) return super.existingIds(ids)
        val vq = EventYql.buildExistence(ids) ?: return emptySet()
        val root =
            try {
                // The answer cannot exceed the ids asked about, so ask for
                // exactly that many — a bounded `hits` also keeps this hot
                // path legal under an operator-capped maxHits (see hitsFor).
                searchRoot(vq, hits = ids.size)
            } catch (e: IllegalArgumentException) {
                if (!fallbacks.isMissingDedupSummary(e)) throw e
                fallbacks.markDedupSummaryMissing()
                return super.existingIds(ids)
            }
        return root.children.mapNotNullTo(HashSet()) { hit -> hit.fields?.id?.takeIf { it.isNotEmpty() } }
    }

    override suspend fun search(query: EventQuery): List<EventDoc> {
        // Pure-id recall bypasses /search/: each id is a direct document-API
        // key lookup (~35% faster than a search over the id attribute), which
        // is what a REQ-by-id and the bulk dedup preload both do.
        if (query.isPureIdLookup()) return getByIds(query)
        // demoteGated BEFORE plan: once the gated-profile flag has flipped
        // (legacy schema), the demoted ranking-free query must regain the
        // recency profile / count-probe planner it would have had pre-gate.
        return fallbacks.withNearFallback(query) { qn ->
            fallbacks.withProfileFallback(planner.plan(fallbacks.demoteGated(qn))) { q ->
                recallSummaries(q).mapNotNull { it.toDoc() }
            }
        }
    }

    /**
     * Raw recall: decode each hit straight to a [RawEvent], keeping `tags` as
     * the stored JSON string — the relay read path splices that string straight
     * onto the wire, so parsing then re-serializing it is pure waste.
     */
    override suspend fun rawSearch(query: EventQuery): List<RawEvent> {
        if (query.isPureIdLookup()) return getByIds(query).map { it.toRawEvent() }
        return fallbacks.withNearFallback(query) { qn ->
            fallbacks.withProfileFallback(planner.plan(fallbacks.demoteGated(qn))) { q ->
                recallSummaries(q).mapNotNull { it.toRaw() }
            }
        }
    }

    /**
     * Recency-ordered recall with the id tiebreak applied CLIENT-SIDE, restoring
     * the exact `created_at desc, id asc` contract. The engine sorts by
     * `created_at desc` alone — compound-sorting by the id string attribute paid
     * UCA collation over the whole match set (0.22s -> 1.3s on a 2M-match scan).
     *
     * With a single-key sort, every doc STRICTLY newer than the boundary
     * timestamp T (the limit-th hit's created_at) is guaranteed present; only
     * membership among the docs AT T is engine-arbitrary. So the query
     * overfetches [TIE_SLACK] extra hits — if anything older than T arrived, or
     * the engine ran out of matches, the whole T tie group is in hand and an
     * in-memory sort resolves the boundary. Only a wider tie group pays one
     * extra `[T,T]` window query.
     *
     * Ranked queries keep the engine's score order untouched. The gated profiles
     * are recency-ordered too (score IS created_at), so they take this path.
     */
    private suspend fun recallSummaries(q: EventQuery): List<VespaSummary> {
        if (!q.isRecencyOrdered()) return rankedHits(q).mapNotNull { it.fields }
        val limit = q.limit
        if (limit == null) {
            val all = recallRoot(q)?.children?.mapNotNull { it.fields }?.filter { it.id.isNotEmpty() } ?: emptyList()
            return all.sortedWith(SUMMARY_NEWEST_FIRST)
        }
        // A non-positive limit matches nothing (EventYql.build's contract) —
        // the overfetch must not resurrect it into a real query.
        if (limit <= 0) return emptyList()
        // Past the match-phase band one query is the wrong shape: the profiles
        // that tolerate a cut cannot serve it, and the unranked alternative is a
        // profile whose degradation this client refuses. Page the band instead —
        // the caller's limit is honoured either way, which is the contract.
        if (limit > EventYql.MATCH_PHASE_BAND && q.ranking == null) return pagedRecency(q, limit)
        val fetch = q.copy(limit = limit + TIE_SLACK).keepingProfileOf(q)
        var hits =
            recallRoot(fetch)?.children?.mapNotNull { it.fields }?.filter { it.id.isNotEmpty() }
                ?: return emptyList()
        if (hits.size > limit) {
            val t = hits[limit - 1].createdAt
            // The tie group at T arrived complete if the engine either ran out
            // of matches before the overfetch limit or already emitted a doc
            // strictly older than T.
            val complete = hits.size < fetch.limit!! || hits.last().createdAt < t
            if (!complete) {
                // A gated query's tie window must stay gated (the exact variant
                // — a [t,t] window is tiny) or the rerun would resurrect
                // below-floor authors into the boundary group.
                val tieRanking = if (q.usesGatedProfile()) EventYql.RANK_RECENCY_GATED_EXACT else EventYql.RANK_UNRANKED
                val ties =
                    recallRoot(q.copy(since = t, until = t, limit = null, ranking = tieRanking))
                        ?.children
                        ?.mapNotNull { it.fields }
                        ?.filter { it.id.isNotEmpty() }
                        ?: emptyList()
                hits = hits.filter { it.createdAt > t } + ties
            }
        }
        return hits.sortedWith(SUMMARY_NEWEST_FIRST).take(limit)
    }

    /**
     * The overfetch must not change WHICH PROFILE serves the query. [TIE_SLACK]
     * is this client's own tie-resolution detail; letting it push a query over
     * [EventYql.MATCH_PHASE_BAND] silently moved the real ceiling to
     * `band - TIE_SLACK` and — far worse — moved the query onto a profile whose
     * match-phase degradation this client REFUSES rather than reconciles, so one
     * extra unit of `limit` turned a served page into an outright error.
     * MEASURED against a production cluster (2026-08-18): limit 1936 served,
     * limit 1937 refused.
     *
     * Only ever stamps the profile [EventYql.build] would have chosen for the
     * CALLER's limit, so the band still means what it says; the slack rides
     * above it on the engine's own headroom, which is an order of magnitude.
     */
    private fun EventQuery.keepingProfileOf(caller: EventQuery): EventQuery =
        when {
            ranking == null && EventYql.usesRecencyProfile(caller) && !EventYql.usesRecencyProfile(this) -> copy(ranking = EventYql.RANK_RECENCY)

            // The gated twin has the same cliff: a caller's limit inside the
            // band plus the slack demoted the overfetch to the full-scan
            // variant (14.5 s against 1.4 s, per EventYql) for the last
            // TIE_SLACK units of the band, invisibly — recallRoot reads the
            // ranking field, which still said "gated".
            ranking == EventYql.RANK_RECENCY_GATED && EventYql.usesGatedMatchPhase(caller) && !EventYql.usesGatedMatchPhase(this) -> copy(keepMatchPhase = true)

            else -> this
        }

    /**
     * Serve a limit past [EventYql.MATCH_PHASE_BAND] by PAGING the band on a
     * `created_at` cursor, rather than asking the engine for one oversized page
     * it may answer partially.
     *
     * The contract is "a query with a limit gets exactly that", and one query
     * cannot keep it here: inside the band the match-phase profile serves the
     * page and its cut is reconciled ([recallRoot]); outside it the only
     * remaining shape is unranked, where any degradation is refused. Paging
     * keeps every page inside the band, so every page is one the engine and this
     * client already agree how to serve.
     *
     * TIES are why each round pays a second query. A page boundary can land
     * mid-second, and the corpora that reach these limits are exactly the ones
     * that bulk-publish on ONE timestamp (a real 30382 provider puts 242k cards
     * at a single `created_at`), so the boundary group is taken WHOLE through an
     * unbounded `[T, T]` window — the same resolution [visitIds] and
     * [recallSummaries] already use, and the reason a single-timestamp corpus
     * pages correctly instead of looping. That window is skipped whenever the
     * strictly-newer docs already fill the limit; the residual cost — a page
     * that lands INSIDE a group far wider than the limit reads the whole group —
     * is inherited from [recallSummaries] and is the price of never truncating
     * a tie silently.
     */
    private suspend fun pagedRecency(
        q: EventQuery,
        limit: Int,
    ): List<VespaSummary> {
        val page = EventYql.MATCH_PHASE_BAND
        // Keyed by id: the boundary window re-reads docs the page already
        // carried, and a caller's limit counts DISTINCT events.
        val out = LinkedHashMap<String, VespaSummary>()
        var until = q.until
        while (out.size < limit) {
            val hits = recallSummaries(q.copy(until = until, limit = page))
            if (hits.isEmpty()) break
            // Short of a page: the engine ran out, so this is the whole tail.
            if (hits.size < page) {
                hits.forEach { out[it.id] = it }
                break
            }
            val boundary = hits.last().createdAt
            hits.forEach { if (it.createdAt > boundary) out[it.id] = it }
            // Everything in the boundary second is OLDER than every doc already
            // held, so once the strictly-newer docs alone fill the limit the
            // group cannot contain a hit and is not worth reading — which is
            // what keeps the unbounded window below off the common path.
            if (out.size >= limit) break
            recallSummaries(q.copy(since = boundary, until = boundary, limit = null)).forEach { out[it.id] = it }
            // Strictly past the group just taken in full — `boundary - 1` is what
            // makes the cursor terminate on a corpus that shares one timestamp.
            if (boundary <= (q.since ?: Long.MIN_VALUE)) break
            until = boundary - 1
        }
        return out.values.sortedWith(SUMMARY_NEWEST_FIRST).take(limit)
    }

    /**
     * Whether the engine's hit order for [this] is recency rather than a rank
     * score — which is also whether there IS a score worth carrying. The gated
     * profiles rank BY created_at, so they count as recency here.
     */
    private fun EventQuery.isRecencyOrdered(): Boolean =
        (ranking == null && search.isNullOrBlank() && phrases.isEmpty()) ||
            ranking == EventYql.RANK_UNRANKED ||
            ranking == EventYql.RANK_RECENCY ||
            usesGatedProfile()

    /** The engine's hits for a query it ORDERS — no recency machinery, and the relevance still attached. */
    private suspend fun rankedHits(q: EventQuery): List<SearchHit> = recallRoot(q)?.children?.filter { !it.fields?.id.isNullOrEmpty() } ?: emptyList()

    /**
     * [searchRanked]/[rawSearchRanked]: the same recall as [search], with each
     * hit's relevance kept so the caller can merge across queries.
     *
     * A recency-ordered query has no score to give and takes the ordinary path
     * — including its tie-slack overfetch, which the ranked branch does not
     * need and must not lose.
     */
    private suspend fun <R : Any> rankedRecall(
        query: EventQuery,
        project: (VespaSummary) -> R?,
    ): List<Ranked<R>> =
        fallbacks.withNearFallback(query) { qn ->
            fallbacks.withProfileFallback(planner.plan(fallbacks.demoteGated(qn))) { q ->
                if (q.isRecencyOrdered()) {
                    recallSummaries(q).mapNotNull { project(it) }.map { Ranked(it, null) }
                } else {
                    rankedHits(q).mapNotNull { hit ->
                        hit.fields?.let { fields -> project(fields)?.let { Ranked(it, hit.relevance, textScoreOf(fields.matchfeatures)) } }
                    }
                }
            }
        }

    // A pure-id lookup never reaches the search endpoint (it is a document-API
    // key fetch) and has no relevance to report; the plain paths answer it.
    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = if (query.isPureIdLookup()) search(query).map { Ranked(it, null) } else rankedRecall(query) { it.toDoc() }

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = if (query.isPureIdLookup()) rawSearch(query).map { Ranked(it, null) } else rankedRecall(query) { it.toRaw() }

    /**
     * The recall query, guarded against match-phase UNDER-DELIVERY. A
     * match-phase-limited query can return fewer hits than asked and Vespa
     * does not re-run it on its own — so a degraded response short of the
     * limit is rerun exact (unranked, or the full-scan gated variant for gated
     * recall: the gate must survive the rerun or under-delivery would serve
     * spam). A degraded response with a FULL page needs no rerun on a single
     * node: everything the cut excluded is older than everything returned.
     */
    private suspend fun recallRoot(q: EventQuery): SearchRoot? {
        val vq = EventYql.build(q) ?: return null
        val root = searchRoot(vq, hits = hitsFor(q))
        val matchPhased = vq.ranking == EventYql.RANK_RECENCY || vq.ranking == EventYql.RANK_RECENCY_GATED
        if (matchPhased && root.coverage.matchPhaseDegraded) {
            // A FULL page proves exactness only on ONE content node: max-hits
            // is per node and each node picks its own cut threshold. A
            // boundary TIE also demotes: the match phase keeps candidates by
            // created_at alone, so a tie straddling the cut can drop a
            // lower-id member while a higher-id one fills the page.
            val timestamps = root.children.mapNotNull { it.fields?.createdAt }
            val oldest = timestamps.minOrNull()
            val boundaryTied = oldest != null && timestamps.count { it == oldest } > 1
            val provablyExact = root.children.size >= (q.limit ?: 0) && root.coverage.nodes <= 1 && !boundaryTied
            if (!provablyExact) {
                val exactRanking = if (vq.ranking == EventYql.RANK_RECENCY_GATED) EventYql.RANK_RECENCY_GATED_EXACT else EventYql.RANK_UNRANKED
                val exact = q.copy(ranking = exactRanking)
                // The rerun must be exact but need not be UNBOUNDED:
                //  - FULL page: any true top-`limit` doc the cut excluded is
                //    newer than at least one returned doc, so `since = oldest
                //    returned created_at` (ties included) bounds the rerun to a
                //    page-sized window, provably lossless — gated too.
                //  - SHORT page: nothing returned bounds the miss. Plain
                //    recency routes through the count-probe ladder; a gated
                //    short page cannot (the probes count the UNGATED match
                //    set) and pays the full-scan gated rerun.
                val rerun =
                    when {
                        root.children.size >= (q.limit ?: 0) && oldest != null -> {
                            exact.copy(since = maxOf(q.since ?: Long.MIN_VALUE, oldest))
                        }

                        // The shape test must lift the exactness stamp first:
                        // isBareRecencyScan reads `ranking == null` as the
                        // planner opt-out, and `exact` always carries
                        // RANK_UNRANKED here.
                        planner.enabled && exactRanking == EventYql.RANK_UNRANKED && exact.copy(ranking = null).isBareRecencyScan() -> {
                            planner.window(exact)
                        }

                        else -> {
                            exact
                        }
                    }
                val rerunVq = EventYql.build(rerun) ?: return root
                return searchRoot(rerunVq, hits = hitsFor(q))
            }
        }
        return root
    }

    /**
     * [search] plus the WHY: each hit carries the engine's relevance score and
     * the match TIER it arrived through (exact name > near > weak > identity >
     * affiliation > gram), derived from the rank profile's declared
     * match-features. This is the inspector/harness surface — "which band did
     * this hit come from" is the first question of every ranking
     * investigation. Tier is null when the profile declares no match-features
     * (unranked/recency) or the serving schema predates them.
     */
    suspend fun searchScored(query: EventQuery): List<ScoredHit> =
        fallbacks.withNearFallback(query) { qn ->
            // A term-less scored query auto-selects the recency profile, which
            // an old serving schema 400s — same net as search().
            fallbacks.withProfileFallback(qn) { q ->
                val vq = EventYql.build(q) ?: return@withProfileFallback emptyList()
                searchRoot(vq, hits = q.limit ?: DEFAULT_SCORED_HITS)
                    .children
                    .mapNotNull { hit -> hit.fields?.let { f -> f.toDoc()?.let { ScoredHit(it, hit.relevance, tierOf(f.matchfeatures)) } } }
            }
        }

    /**
     * The TEXT band behind a hit's relevance — `text_score`, straight off the
     * match-features the trust profiles already serialize. Null on a profile
     * that declares none (every termless/recency shape), which is exactly where
     * a caller must fall back rather than invent a number.
     */
    private fun textScoreOf(mf: JsonObject?): Double? = (mf?.get("text_score") as? JsonPrimitive)?.content?.toDoubleOrNull()

    /** The rank band a hit arrived through, from the profile's match-features; null when none were served. */
    private fun tierOf(mf: JsonObject?): String? {
        if (mf == null) return null

        fun on(feature: String): Boolean = ((mf[feature] as? JsonPrimitive)?.content?.toDoubleOrNull() ?: 0.0) > 0.0
        return when {
            // BEFORE name: a scattered match (event.sd §12.2 — no naming field
            // answered the query on its own, another kind of column carried the
            // rest) is still a token match, so name_match/any_token_match are 1
            // for it too and testing them first would swallow the more specific
            // route. It is a RUNG of its own, though — w_split_tier, one below
            // the token band — so "why is this here" deserves its own answer.
            // Reads 0, hence "name", on a serving schema that predates the rung.
            on("scattered_match") -> "split"

            on("any_token_match") || on("name_match") -> "name"

            on("any_near_match") || on("near_name_match") -> "near"

            // BEFORE weak: identity (nip05/lud16) is one of the weak band's own
            // signals, so weak_match is 1 for these docs too and testing it
            // first would swallow the more specific route. The doc scores in
            // the weak band either way; this only names how it got there.
            on("identity_match") -> "identity"

            on("weak_match") -> "weak"

            on("affiliation_match") -> "affiliation"

            // Same RUNG as affiliation (event.sd max()es the two into one
            // weight), a different route: the profile group's bio/website vs the
            // generic group's body/location. Ordered after it so a doc filling
            // both reports the profile-side label, as the tiers above do.
            on("tier_body_match") -> "body"

            // Matched a real column but NO rung claimed it — the floor band
            // (event.sd floored_text_score). Last, because real_match is 1 for
            // every branch above too, so it only means "unclaimed" once they
            // have all failed. A hit landing here is a ranking gap worth
            // reporting, unlike "gram" — a doc no column matched at all.
            on("real_match") -> "floor"

            else -> "gram"
        }
    }

    /**
     * The `hits` to ask Vespa for: the query's own limit, else "everything".
     *
     * "Everything" is Int.MAX_VALUE by default — Vespa's `hits` defaults to
     * TEN when omitted, and the bundled query profile raises `maxHits` to let
     * the big value through. That is safe on ONE content node because the hit
     * collector sizes from what MATCHES, not from what was asked (measured —
     * see the bundled query profile's comment). Multi-node dispatch does NOT
     * have that property: its merge path allocates by the REQUESTED hits, so
     * Int.MAX_VALUE kills the jdisc container outright with "Requested array
     * size exceeds VM limit" (observed 2026-08-17, 2-node content cluster —
     * a crash loop re-triggered on every recovery by whichever fetch-all
     * caller retried first).
     *
     * Operators who cap `maxHits` in their deployed query profile (the
     * multi-node survival move) must set VESPA_UNBOUNDED_HITS to the same
     * value: no-limit queries then stay inside the engine's ceiling, and a
     * caller's explicit limit above it still fails loudly at the engine —
     * that rejection is the operator's cap working, not a bug here.
     */
    private val unboundedHits: Int = System.getenv("VESPA_UNBOUNDED_HITS")?.toIntOrNull() ?: Int.MAX_VALUE

    private fun hitsFor(query: EventQuery): Int = query.limit ?: unboundedHits

    // ---- pure-id fast path --------------------------------------------------

    /**
     * Only ids constrain the query (an expiry guard may still ride along), and
     * few enough to resolve in a single concurrent get wave. The size cap
     * matters: above it, ONE `id in (…)` search is a single round trip while N
     * gets are N, so the bulk-insert dedup preload must stay on the search path.
     */
    private fun EventQuery.isPureIdLookup(): Boolean =
        !addressKeyed && // address-keyed replaceables aren't at the id docid — route ids through search
            // A present limit <= 0 is the "matches nothing" sentinel; only the
            // search path implements it, so it must not take this shortcut.
            (limit == null || limit > 0) &&
            ids.isNotEmpty() && ids.size <= ID_GET_FANOUT &&
            kinds.isEmpty() && authors.isEmpty() && owners.isEmpty() &&
            tags.isEmpty() && tagsAll.isEmpty() &&
            // The WEIGHTED key sets are recall constraints too (a dotProduct
            // over `id` / `pubkey`), and a document-API get sees neither them
            // nor the rawScore the member profile ranks on — so a query
            // carrying one must take the search path even when its `ids` alone
            // would have qualified.
            idWeights.isEmpty() && authorWeights.isEmpty() &&
            since == null && until == null && expiresBefore == null &&
            // Text constraints of EVERY polarity: a doc-API get never sees the
            // search fields, so an id lookup riding a phrase requirement or a
            // notSearch exclusion must take the search path to honor it.
            search == null && phrases.isEmpty() && notSearch.isEmpty() && ranking == null

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
                // fan-out machinery (its allocations matter at thousands of id
                // lookups a second) and just get it.
                hexes.size == 1 -> listOfNotNull(get(hexes[0]))

                else -> hexes.mapBounded(ID_GET_FANOUT) { get(it) }.filterNotNull()
            }
        // NIP-40: never serve an event already expired at the query's cutoff —
        // the same guard EventYql emits as `expires_at > notExpiredAt`.
        val live = query.notExpiredAt?.let { cut -> docs.filter { (it.expiresAt() ?: EventDoc.NO_EXPIRATION) > cut } } ?: docs
        val ordered = live.sortedWith(EventDoc.NEWEST_FIRST)
        return query.limit?.let(ordered::take) ?: ordered
    }

    // ---- visits (full-corpus walks) -----------------------------------------

    /**
     * Every match's (id, created_at[, d tag]), paged on a created_at CURSOR
     * through the attribute index.
     *
     * The alternative, a document-API scan ([visitIdsByScan]), evaluates a
     * selection per document with no index behind it, so its cost grows as the
     * filter gets NARROWER: on a 42.5M-doc corpus a one-author filter (0.35% of
     * the corpus) took 11.5s to fill a 1000-doc page against 1.3s for an
     * all-authors filter (47%). That whole one-author set costs 9.1s through the
     * index, against ~28 minutes by scan.
     *
     * TIES ARE THE WHOLE DIFFICULTY, and they are not rare — one second in that
     * corpus holds 41,329 events by a single author. A cursor of `created_at <=
     * T` re-reads the same page forever when a tie group is wider than the page,
     * and `< T` silently drops the rest of the group; both were observed. So
     * this borrows [recallSummaries]'s resolution: overfetch by [TIE_SLACK], and
     * pay one exact `[T, T]` window query only when the boundary group might be
     * cut short. The engine sorts on created_at alone — a compound id sort pays
     * UCA collation over the whole match set (0.22s -> 1.3s on 2M matches).
     */
    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        // A limit'd walk is the caller asking for the newest N — a relay
        // stamps `limit: 100000` on every COUNT and NIP-77 filter — and a
        // ranked one has to be served by the search path (the ids are ranked
        // there). A PLAIN limited walk takes one ordered id query when it fits
        // in a page, and pages the cursor with a budget when it does not:
        // it used to hand the query to search(), which fetched a full document
        // summary per id to produce an id list — measured on a staging slice,
        // a two-filter COUNT over 51k events took 40 s against 0.2 s for the
        // same filters counted one at a time.
        if (query.limit != null) {
            if (query.limit <= 0) return
            val unlimited = query.copy(limit = null)
            // A LIMIT IS AN ORDER, not just a count: "the newest N". Only two
            // walks can honour that — the search path (ranked ids ARE its
            // order) and the cursor, which pages `created_at desc`. The scan
            // cannot: it is a document-API visit in BUCKET order, so budgeting
            // it returned an arbitrary N that looked like a page. A COUNT is
            // where that showed: `countUnder` unions each filter's served ids,
            // and two arbitrary samples of overlapping filters overlap less
            // than two newest-N pages do, so the count came back ABOVE what
            // the REQ it describes would serve.
            // A ranked walk is the search path's by definition: the ids ARE its
            // ranking, so nothing else can produce them.
            if (query.isRankedShape()) return super.visitIds(query, withDTag, onPage)
            // A LIMIT THAT FITS IN ONE PAGE IS ONE QUERY — no probe, no cursor.
            // `buildIdTime` is unranked and ordered `created_at desc` and
            // honours the limit, so the newest N arrive in a single round trip,
            // which is the same answer both branches below produce for this
            // shape and the cheapest way to produce it.
            //
            // Two costs it takes out, and #134 made the first of them ten times
            // worse: the cursor fetches a whole `idPageSize + TIE_SLACK` page
            // whatever the caller asked for, so a `limit: 500` walk serialized
            // 20,064 id rows to serve 500 (0.227s against 0.122s on the page-size
            // measurements in [PAGE_IDS]) — and paid a 2,000-row decision probe
            // to get there. The decision cannot change the answer here: a walk
            // that stops inside its first page never crosses a boundary, so
            // there is no tie group to resolve and nothing for the scan to be
            // better at.
            //
            // A relay's `limit: 100000` is still bigger than a page and still
            // takes the cursor below, which is the shape the paging was measured
            // on.
            if (query.limit <= idPageSize) {
                IngestStats.timed("walk.limited.onepage") { onPage(idTimeHits(query, withDTag)) }
                return
            }
            // The probe is timed here for the reason the unlimited path times
            // it below: a walk stuck in the decision looks exactly like a walk
            // with nothing to do, and a COUNT's id walk arrives on THIS branch.
            if (IngestStats.timed("walk.cursor.decide") { planFor(unlimited, withDTag) } != WalkPlan.CURSOR) {
                // ONE ORDERED, UNRANKED, ID-ONLY QUERY — not `super.visitIds`,
                // which is `search(query)`: that materializes a full document
                // summary per id (content, sig, tags) to produce an id list, on
                // whatever profile the recency planner picks, and a match phase
                // may then cap the total and drop hits SILENTLY. On the shape
                // that lands here — a relay's `limit: 100000` over a tie-dense
                // author — that is up to 100,000 summaries fetched to count,
                // and a count that reads UNDER. `buildIdTime` is unranked and
                // ordered for exactly this reason, and it already honours the
                // limit, so the newest N arrive in one round trip.
                onPage(idTimeHits(query, withDTag))
                return
            }
            var budget: Int = query.limit
            return visitIdsByCursor(unlimited, withDTag) { page ->
                val take = page.take(budget)
                budget -= take.size
                (take.isEmpty() || onPage(take)) && budget > 0
            }
        }
        // TIE DENSITY decides, measured on this walk rather than guessed from
        // the query's shape. What costs the cursor is a boundary group so wide
        // it needs an unbounded [T,T] window query: unkeyed 30382 walks hit
        // seconds holding tens of thousands of docs (services bulk-publish
        // scores on one timestamp), while unkeyed kind 1 and kind 0/10002 spread
        // over their seconds and pay nothing. The earlier shape rule (cursor for
        // keyed walks, scan for unkeyed) therefore sent sparse unkeyed walks to
        // the scan for no reason — on {kinds:[0,10002]}, 988 ids/s by cursor
        // against 8 ids/s by scan during a disk-index fusion.
        // (`limit` is already known null — the bounded case returned above.)
        // TIMED SEPARATELY, because a walk that never reaches its first page
        // looks exactly like a walk with nothing to do. On staging a service
        // walk sat in this read path for twenty minutes without one gate hold
        // or one write, and nothing named which read it was in.
        when (IngestStats.timed("walk.cursor.decide") { planFor(query, withDTag) }) {
            WalkPlan.CURSOR -> visitIdsByCursor(query, withDTag, onPage)
            WalkPlan.SCAN_DENSE_TIES -> visitIdsByScan(query, withDTag, onPage)
            WalkPlan.PARTIAL -> walkAroundPartialAnswer(query, withDTag, onPage)
        }
    }

    /**
     * A partial probe is not a verdict about the data, so it must not buy a
     * whole-corpus read. The scan costs the corpus WHATEVER the filter says —
     * measured against this same predicate, 7.1s of visiting found 4 of 8,765
     * matches that one indexed query answered completely in 6ms — so trading a
     * fifth of a second of index work for ~18 minutes of scanning is a bargain
     * the walk should almost never take.
     *
     * The ladder instead:
     *
     *   1. ASK AGAIN. `non-ideal-state` and a bare coverage shortfall are the
     *      cluster settling and clear on their own; the same 140-kind, 23-hour
     *      probe that failed answers 943,949 matches at 100% coverage in 0.13s.
     *   2. HALVE THE WINDOW and recurse. Two cheaper questions where one was
     *      refused — which is the pager's own strategy one level up, borrowed.
     *   3. Only at a ONE-SECOND window, where there is nothing left to halve,
     *      take the scan. That is the case it was built for: a service that
     *      bulk-published 148,130 cards on a single timestamp cannot be split
     *      by time, and the visit is the only mechanism that can finish it.
     *
     * The split is invisible above this call: the walk still delivers every id
     * in the window, so no cursor key, coverage band or sweep identity moves.
     * That holds only because the halves carry the CALLER's bounds and not the
     * numbers the midpoint was computed from — an unbounded half stays
     * unbounded. Filling one in silently truncated the walk; see below.
     */
    private suspend fun walkAroundPartialAnswer(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        when (IngestStats.timed("walk.partial.retry") { planFor(query, withDTag) }) {
            WalkPlan.CURSOR -> return visitIdsByCursor(query, withDTag, onPage)
            WalkPlan.SCAN_DENSE_TIES -> return visitIdsByScan(query, withDTag, onPage)
            WalkPlan.PARTIAL -> Unit
        }

        // A MIDPOINT NEEDS A NUMBER; THE HALVES MUST NOT INHERIT ONE.
        //
        // Cutting a window in two requires a second to cut at, so an absent
        // bound is filled in here with the widest the walk could mean — FOR THE
        // ARITHMETIC ONLY. The halves then carry the CALLER's bounds, null
        // included, because an absent bound is not a wide one: it is the
        // absence of one.
        //
        // Handing the derived `until` to the newer half is how this dropped
        // documents. An unbounded walk means "no upper bound", and this corpus
        // holds events dated past the clock — staging carries notes stamped in
        // the year 2100 — so `until = now` matched the original query and
        // NEITHER half. Measured on the mock: 40 of 41 ids delivered, with no
        // error, no degradation named and no scan. On the NIP-77 path that id
        // set is precisely what a peer reconciles against, so a silently
        // dropped id is a peer that offers it forever and a mirror that never
        // converges — intermittently, only on the degraded path.
        //
        // `nowSecs` is a poor second choice for the same reason: it is the
        // RANKING instant (see EventQuery.nowSecs), not a bound on created_at.
        // It stays only because a query that carries one has already declared
        // which instant it means.
        val cutUntil = query.until ?: query.nowSecs ?: (System.currentTimeMillis() / 1000)
        val cutSince = query.since ?: 0L
        // NOTHING LEFT TO HALVE — asked BEFORE the halves are probed, so a
        // window this narrow does not buy two engine queries on the way to the
        // scan it was always taking. It also rules out an INVERTED derived
        // window (a `since` past the clock on an unbounded walk), where the
        // midpoint would land below `since` and the newer half would reach
        // further back than the caller asked.
        if (cutUntil - cutSince < 2) {
            IngestStats.timed("walk.partial.unsplittable") { }
            return visitIdsByScan(query, withDTag, onPage)
        }
        val mid = cutSince + (cutUntil - cutSince) / 2
        val newer = query.copy(since = mid + 1, until = query.until)
        val older = query.copy(since = query.since, until = mid)

        // BOTH HALVES ARE PROBED BEFORE EITHER IS WALKED, and the split is taken
        // only if both come back clean. Two properties depend on it.
        //
        // It can never cost more than today. A scan reads the corpus whatever
        // its window says, so scanning two halves costs TWICE what scanning the
        // whole once does — the opposite of the intended saving. Deciding up
        // front means the fallback is still one scan of the original window,
        // and the attempt has cost three probes, about half a second.
        //
        // And it cannot recurse away. Splitting on a partial that is a property
        // of the SHAPE rather than the moment — `match-phase` on a match set
        // the engine will keep cutting — would halve, fail, halve again, and
        // fan out to billions of walks against an unbounded window. Bisection
        // is attempted once, here, and never from inside itself.
        val plans =
            IngestStats.timed("walk.partial.probe.halves") {
                listOf(planFor(newer, withDTag), planFor(older, withDTag))
            }
        if (plans.any { it == WalkPlan.PARTIAL }) {
            IngestStats.timed("walk.partial.unsplittable") { }
            return visitIdsByScan(query, withDTag, onPage)
        }

        IngestStats.timed("walk.partial.bisect") { }
        // A false return from `onPage` stops the WHOLE walk, not just the half
        // it arrived in — the caller has what it asked for and the older half
        // must not run on regardless.
        var stopped = false
        val half: suspend (List<DocRef>) -> Boolean = { page ->
            val carryOn = onPage(page)
            if (!carryOn) stopped = true
            carryOn
        }
        // Newest first, so the walk keeps descending across the split. Each half
        // takes the mechanism its own probe just named, never this path again.
        walkByPlan(plans[0], newer, withDTag, half)
        if (!stopped) walkByPlan(plans[1], older, withDTag, half)
    }

    /** One half, on the mechanism its probe named. Never re-enters the partial ladder. */
    private suspend fun walkByPlan(
        plan: WalkPlan,
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) = when (plan) {
        WalkPlan.SCAN_DENSE_TIES -> visitIdsByScan(query, withDTag, onPage)
        else -> visitIdsByCursor(query, withDTag, onPage)
    }

    /**
     * The cursor walk: `created_at desc` a page at a time, resolving the tie
     * group at each boundary. Its ORDER is why a limited walk is allowed to
     * budget-truncate it and not the scan (see [visitIds]).
     *
     * Split out so the limited branch can take it directly: routing back
     * through [visitIds] would re-run [planFor]'s probe query for a decision
     * already made.
     */
    private suspend fun visitIdsByCursor(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        var until: Long? = query.until
        while (true) {
            val fetchLimit = idPageSize + TIE_SLACK
            val hits = IngestStats.timed("walk.ids.page") { idTimeHits(query.copy(until = until, limit = fetchLimit), withDTag) }
            if (hits.isEmpty()) return

            // Fewer than asked for: the engine ran out, so this range is
            // complete and there is nothing older to cursor to.
            if (hits.size < fetchLimit) {
                onPage(hits)
                return
            }

            val boundary = hits[idPageSize - 1].createdAt
            // The boundary group arrived complete only if the engine already
            // emitted something strictly older than it.
            val page =
                if (hits.last().createdAt < boundary) {
                    hits.filter { it.createdAt >= boundary }
                } else {
                    // Unbounded [T,T] window: one second's group, however wide.
                    // Sized by the engine, never by a guessed limit — a guessed
                    // one truncates and the walk then steps past the remainder
                    // without ever reporting a loss.
                    hits.filter { it.createdAt > boundary } +
                        // The unbounded [T,T] window: sized by the engine, so
                        // its cost is the corpus's and not this walk's choice.
                        IngestStats.timed("walk.ids.tiegroup") { idTimeHits(query.copy(since = boundary, until = boundary, limit = null), withDTag) }
                }
            if (!onPage(page)) return
            // Strictly past the group just emitted in full.
            if (boundary <= (query.since ?: Long.MIN_VALUE)) return
            until = boundary - 1
        }
    }

    /** Whether the ids are the ENGINE's ordering to give — terms, phrases or an explicit profile — rather than plain recency. */
    private fun EventQuery.isRankedShape(): Boolean = !search.isNullOrBlank() || phrases.isNotEmpty() || ranking != null

    /**
     * The document-API visit: a streaming scan with a selection expression,
     * evaluated per document with no index behind it. Kept for UNKEYED walks,
     * where it beats the cursor — see [visitIds] for the measurements.
     * Queries a selection can't express fall back to the search default.
     */
    private suspend fun visitIdsByScan(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        // Counted apart: this walk never reaches the scan at all, and folding it
        // into `walk.scan` would report search time as scan time.
        val selection =
            EventSelection.build(query)
                ?: return IngestStats.timed("walk.scan.unselectable") { super.visitIds(query, withDTag, onPage) }
        // Vespa fieldSet syntax is "<doctype>:<field>,<field>,…" — the doctype
        // prefixes the list ONCE (else: ILLEGAL_PARAMETERS).
        // `id` is PROJECTED, not parsed off the docid: under address-keying a
        // replaceable's docid is its address (`<kind>:<pubkey>[:<d>]`), and the
        // tail of that is the d-tag or the author — every NIP-77 snapshot of a
        // provider's cards, which all sit on one timestamp and so take this
        // walk, offered a peer ids that exist nowhere. The cursor walk reads
        // the attribute; this one now does too.
        val fieldSet = "$EVENT_DOCTYPE:id,created_at" + if (withDTag) ",tag_index" else ""
        // THE ONE WALK NOTHING COULD SEE. Every other branch here is timed, and
        // this — the most expensive of them — was not: a document-API visit is
        // not a query, so it books no engine time against any caller on the
        // pulse, and it had no stage of its own either. A walk could evaluate a
        // selection against every document in the corpus and appear, on every
        // instrument this process has, as a walk doing nothing.
        //
        // `walk.scan` calls are also the SCAN RATE: `walk.cursor.decide` counts
        // every choice, so decide-minus-scan is how often the cursor won.
        //
        // It matters because the scan cannot narrow. A selection is evaluated
        // per document, so `created_at` bounds shrink what is RETURNED and not
        // what is READ — an age-banded walk costs a banded walk's price on the
        // cursor and the whole corpus's price here.
        IngestStats.timed("walk.scan") {
            visits.pages(selection, fieldSet) { documents ->
                IngestStats.timed("walk.scan.page") {
                    val page =
                        documents.mapNotNull { d ->
                            if (d.id.isEmpty()) return@mapNotNull null
                            val at = d.fields?.createdAt ?: return@mapNotNull null
                            val dTag =
                                if (withDTag) {
                                    d.fields.tagIndex?.firstNotNullOfOrNull { t -> t.takeIf { it.startsWith("d:") }?.substring(2) }
                                } else {
                                    null
                                }
                            DocRef(d.fields.id ?: d.id.substringAfterLast(":"), at, dTag)
                        }
                    page.isEmpty() || onPage(page)
                }
            }
        }
    }

    /**
     * Which mechanism this walk gets, and — where it is not the cursor — WHY.
     * The two non-cursor outcomes were one boolean for a long time, and they
     * want opposite treatment: a dense tie group is a property of the data that
     * re-splitting cannot help, while a partial answer is usually the cluster
     * having a moment.
     */
    private enum class WalkPlan {
        /** Page the index on a created_at cursor. */
        CURSOR,

        /** A boundary group too wide to resolve on the cursor: the scan is the honest answer. */
        SCAN_DENSE_TIES,

        /** The probe came back incomplete. Says nothing about the data — ask again, smaller. */
        PARTIAL,
    }

    /**
     * Which walk [query] gets, decided from the corpus rather than the query's
     * shape: sample the corpus, and when the sample's boundary second is tied,
     * measure how wide that group is. A group past [TIE_DENSE_FACTOR] PAGES
     * means every boundary on this walk risks an unbounded window query, which
     * is the one thing the scan is genuinely better at.
     *
     * THE SAMPLE IS [probeIds]; THE THRESHOLD IS [idPageSize]. They are
     * different questions and must not be read off one number: how many rows it
     * takes to SEE a tie is a sampling choice, while how wide a group the cursor
     * can AFFORD is a property of the page that resolves it. See [PROBE_IDS].
     *
     * Returns the REASON and not a boolean, because the caller does different
     * things with the two: a dense tie group is a fact about the data that
     * re-asking cannot change, while a partial answer is usually the cluster
     * having a moment and is worth asking again.
     */
    private suspend fun planFor(
        query: EventQuery,
        withDTag: Boolean,
    ): WalkPlan {
        // THE ENGINE CUTTING THE PROBE IS AN ANSWER, NOT AN ERROR. The cursor
        // reads through search with `order by created_at desc`, and attribute
        // sorting trips proton's match-phase limiter on a large match set: the
        // response comes back partial, the coverage guard refuses it, and the
        // walk dies — with the document-API scan sitting right there, which has
        // no match phase because it is not a search at all.
        //
        // Measured on staging: `kinds=[30382], authors=[service]` over a
        // 148,130-card service came back at 54% coverage with
        // `match-phase: true`, and every service walk large enough to matter
        // failed this way. Small services walked fine, which is why the
        // projection filled in for 236 of 342 services and then stopped —
        // exactly the ones an observer's lens most often names were the ones
        // too big to read this way.
        val probe =
            try {
                idTimeHits(query.copy(limit = probeIds), withDTag)
            } catch (cut: PartialAnswer) {
                // Booked BY DEGRADATION, because the label decides whether this
                // can be waited out: `non-ideal-state` and a bare `coverage`
                // shortfall are a cluster settling, `match-phase` is the engine
                // refusing this shape and will say so again.
                IngestStats.timed("walk.plan.partial.${cut.degradation ?: "unspecified"}") { }
                return WalkPlan.PARTIAL
            }
        // Short of a page: the whole match set is tiny, so the cursor's single
        // round trip beats spinning up a visit.
        if (probe.size < probeIds) return WalkPlan.CURSOR
        val boundary = probe.last().createdAt
        // Not tied at the boundary — no window query will ever be needed here.
        if (probe.first().createdAt != boundary && probe.count { it.createdAt == boundary } == 1) return WalkPlan.CURSOR
        val group = countAt(query, boundary)
        // AGAINST THE PAGE, NOT THE PROBE. What a wide group costs is paid by
        // the CURSOR's page: a group inside one page never needs a window query
        // at all, and a larger one is fetched by a single `[T,T]` query whose
        // ceiling is the query profile's `maxHits` — which is what sizes this
        // (see [PAGE_IDS]). The probe only decides how many rows it takes to
        // notice the tie. Reading the threshold off [probeIds] made it 8,000
        // where the page had already moved it to 80,000, and sent every group
        // between the two to a document-API read of the whole corpus — ~17.9
        // minutes on the staging corpus against 6 ms for the indexed query, on
        // walks a 20,064-row page absorbs without one window query.
        if (group <= idPageSize * TIE_DENSE_FACTOR) return WalkPlan.CURSOR
        IngestStats.timed("walk.plan.scan.ties") { }
        return WalkPlan.SCAN_DENSE_TIES
    }

    /** How many of [query]'s matches share exactly [at] — the boundary group's true width. */
    private suspend fun countAt(
        query: EventQuery,
        at: Long,
    ): Int = IngestStats.timed("walk.cursor.countat") { count(query.copy(since = at, until = at, limit = null)) }

    /** One [EventYql.buildIdTime] recall, decoded to [DocRef] and newest-first. */
    private suspend fun idTimeHits(
        q: EventQuery,
        withDTag: Boolean,
    ): List<DocRef> {
        val vq = EventYql.buildIdTime(q, withDTag) ?: return emptyList()
        return searchRoot(vq, hits = q.limit ?: unboundedHits)
            .children
            .mapNotNull { hit ->
                val f = hit.fields ?: return@mapNotNull null
                if (f.id.isEmpty()) return@mapNotNull null
                val dTag =
                    if (withDTag) {
                        f.tagIndex?.firstNotNullOfOrNull { t -> t.takeIf { it.startsWith("d:") }?.substring(2) }
                    } else {
                        null
                    }
                DocRef(f.id, f.createdAt, dTag)
            }.sortedWith(compareByDescending<DocRef> { it.createdAt }.thenBy { it.id })
    }

    /**
     * The tags projection via the same sliced visit as [visitIds], fieldSet
     * `event:tags`: each match contributes ONLY its stored tag JSON — content,
     * sig, and the search columns never cross the wire.
     */
    override suspend fun visitTags(
        query: EventQuery,
        onPage: suspend (List<List<List<String>>>) -> Boolean,
    ) {
        val selection = EventSelection.build(query) ?: return super.visitTags(query, onPage)
        visits.pages(selection, "$EVENT_DOCTYPE:tags") { documents ->
            val page =
                documents.mapNotNull { d ->
                    d.fields?.tags?.let { raw ->
                        Json.parseToJsonElement(raw).jsonArray.map { tag -> tag.jsonArray.map { it.jsonPrimitive.content } }
                    }
                }
            page.isEmpty() || onPage(page)
        }
    }

    /** One page of FULL docs through the visit — the reindex primitive. See [VespaVisits.docsPage]. */
    override suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage {
        val selection = EventSelection.build(query) ?: return super.visitDocsPage(query, resumeFrom, maxDocs)
        return visits.docsPage(selection, resumeFrom, maxDocs)
    }

    /**
     * Complete author scan via the visit, projecting only `pubkey`.
     * [countByAuthor]'s grouping is complete too, but it materializes every
     * group in one response; this streams, which is what the corpus-wide
     * guard-owner Bloom preload needs — a missed author would be a false
     * negative.
     */
    override suspend fun scanAuthors(query: EventQuery): Set<String> {
        val selection = EventSelection.build(query) ?: return super.scanAuthors(query)
        val authors = HashSet<String>()
        visits.pages(selection, "$EVENT_DOCTYPE:pubkey") { documents ->
            documents.forEach { d -> d.fields?.pubkey?.let { authors += it } }
            true
        }
        return authors
    }

    // ---- counts and groupings -----------------------------------------------

    /**
     * How many documents [query] would SERVE — the match set for a plain filter,
     * and the GATED page for a ranked one, which is smaller whenever the
     * observer's trust floor deletes hits.
     *
     * Two engine shapes, because one number has two exact answers and the cheap
     * one is not always available:
     *
     *  - **Unranked** ([EventYql.countProfileOf] null): the grouping count over
     *    the match set. Exact, and no rank phase runs at all.
     *  - **Ranked**: the query's own profile with ZERO hits, reading the
     *    response's `totalCount` — which the profile's drop-limit has already
     *    subtracted from. The grouping cannot answer this shape: it counts the
     *    UNGATED match set and would over-report every trust-lensed count.
     *
     * The ranked shape used to be answered by materializing the page and taking
     * its size (`rawSearch(q).size` in NostrSemanticsStore.count). It was exact
     * for the same reason this is — the served page IS the gated set — but it
     * paid a full document summary, over the wire and through the JSON decoder,
     * per counted event. MEASURED on the production relay (2026-09-01): a COUNT
     * for "bitcoin" under the relay's injected `limit: 100000` took 76.0s
     * against 4.9s for the same search, and "nostr" 84.7s against 19.2s — the
     * excess IDENTICAL on both terms (~0.7 ms x 100,000 documents) although
     * nostr's match set is three times bitcoin's, because what the extra time
     * bought was documents, not matching. Locally, 39 ms against 4,559 ms.
     *
     * The limit is dropped before building: a positive limit bounds HITS, never
     * a count (the contract [EventYql.grouping] states and
     * [InMemoryEventIndex.count] mirrors), and here it would also decide which
     * profile serves the query — a `sort:recent` count would land on the
     * match-phase variant whose `totalCount` is capped.
     */
    override suspend fun count(query: EventQuery): Int {
        // The "matches nothing" sentinel, checked BEFORE the limit is dropped
        // below — dropping it would turn the sentinel into a real query.
        if ((query.limit ?: 1) <= 0) return 0
        return fallbacks.withNearFallback(query) { qn ->
            // The profile net too: a count is now a ranked query on the paths
            // where the gate applies, so a serving schema that predates the
            // gated profiles must demote it the same way a recall does.
            fallbacks.withProfileFallback(qn.copy(limit = null)) { q ->
                val ranked = EventYql.countProfileOf(q)
                if (ranked == null) {
                    val root = EventYql.buildCount(q)?.let { queryRoot(it, hits = 0) }
                    root?.let { GroupingResults.firstCount(it) } ?: 0
                } else {
                    val vq = EventYql.build(q.copy(ranking = ranked)) ?: return@withProfileFallback 0
                    searchRoot(vq, hits = 0).fields.totalCount
                }
            }
        }
    }

    /**
     * [EventIndex.distinctTagIndexValues] against the engine: ONE grouping over
     * `tag_index`, then the `"<letter>:"` prefix stripped off each group.
     *
     * The groups come back for EVERY tag letter the match set carries, not just
     * the asked-for one — `tag_index` is one attribute — so the prefix filter
     * here is what makes the answer the caller's question rather than a
     * superset of it. Cheap: it runs over distinct values, of which a relay
     * corpus has tens of thousands, not over the documents.
     */
    override suspend fun distinctTagIndexValues(
        query: EventQuery,
        tagName: String,
    ): Set<String>? {
        val vq = EventYql.buildDistinctTagValues(query) ?: return emptySet()
        val root = queryRoot(vq, hits = 0) ?: return emptySet()
        // The grouping is over EVERY letter (see the builder): narrowing to the
        // asked-for one is this side's job, and dropping this filter would
        // silently widen the answer.
        val prefix = "$tagName:"
        return GroupingResults
            .groupCounts(root)
            .keys
            .asSequence()
            .filter { it.startsWith(prefix) }
            .map { it.removePrefix(prefix) }
            .filter { it.isNotEmpty() }
            .toSet()
    }

    /**
     * `all(group(pubkey) each(output(count())))` — one leaf group per distinct
     * author, each carrying its doc count. So the author set AND the per-author
     * counts cost exactly one query.
     */
    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> =
        fallbacks.withNearFallback(query) { q ->
            val root = EventYql.buildDistinctAuthors(q)?.let { queryRoot(it, hits = 0) } ?: return@withNearFallback emptyMap()
            GroupingResults.groupCounts(root)
        }

    // ---- transport funnels --------------------------------------------------

    /**
     * POST [vq] to `/search/` and return the raw response body. POSTs because a
     * filter with hundreds of ids or authors builds YQL far past any sane URL
     * length.
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
                // Vespa's own split of the time it spent. Asked for only when
                // somebody is listening, so a store with no ledger sends the
                // byte-identical request it always did.
                if (ledger != null) put("presentation.timing", "true")
                vq.params.forEach { (k, v) -> put(k, v) }
            }.toString()
        // A busy engine sheds load transiently (504 under heavy concurrent
        // summary fills); one failed page must not kill a multi-hour sync, so
        // 5xx gets brief retries inside VespaHttp.
        val resp = http.postJson("${endpoint()}/search/", body)
        require(resp.statusCode() < 400) { "vespa search ${resp.statusCode()}: ${resp.body().take(300)}" }
        return resp.body()
    }

    /**
     * The recall paths' `/search/` funnel, streamed straight into DTOs.
     * Together with [queryRoot] (the grouping/count paths, which need the tree)
     * it is the only way a response reaches a caller, and both verify coverage —
     * so nothing can accidentally accept a degraded answer.
     */
    private suspend fun searchRoot(
        vq: VespaQuery,
        hits: Int,
    ): SearchRoot {
        val t0 = if (ledger?.slowQueryThresholdNanos != null) System.nanoTime() else 0L
        val envelope = VESPA_JSON.decodeFromString<SearchEnvelope>(queryBody(vq, hits))
        val root = envelope.root
        // Published BEFORE the coverage check, deliberately: a degraded answer
        // is the one an operator most wants to see in the numbers, and
        // requireComplete throws.
        // WHO ASKED. `currentActivity()` is a suspend read of the coroutine
        // context, so it can only be taken here — `publish` is not suspend and
        // the ambient activity is gone by the time the ledger sees the numbers.
        // Without it the engine table says what the engine did and nothing says
        // who made it do that, which on 2026-09-06 cost an ablation (scale the
        // mirror to zero and watch) to answer.
        publish(vq, root, envelope.timing, currentActivity())
        captureSlow(vq, t0, envelope.timing, root.children.size.toLong(), root.fields.totalCount.toLong())
        // `sampled` joins the recency profiles here rather than bypassing the
        // check: a truncated page is still RECORDED below, so a cluster
        // degrading every read shows up in the numbers either way — it just
        // stops killing the reads that a subset already answers.
        val allowMatchPhase = vq.sampled || vq.ranking == EventYql.RANK_RECENCY || vq.ranking == EventYql.RANK_RECENCY_GATED
        // RECORDED WHETHER OR NOT IT THROWS. A match-phase cut on a recency
        // profile is ALLOWED and returned silently, so a cluster degrading
        // every read shows up here before anything refuses and long before
        // anyone notices ranked pages getting shorter.
        if (!root.coverage.undegraded) {
            DegradedReads.record(
                profile = vq.ranking,
                flags = root.coverage.setFlags,
                shape = shapeOf(vq),
                refused = !allowMatchPhase,
                coverage = root.coverage.coverage,
                documents = root.coverage.documents,
            )
        }
        root.coverage.requireComplete(allowMatchPhase = allowMatchPhase)
        if (vq.complete) root.requireEverything()
        return root
    }

    /**
     * The query's SHAPE for a degraded-read tally: which clause kinds it
     * carried, never their values. A yql holds what somebody searched for, and
     * this is read from places a search term must not reach.
     *
     * A FIELD READ now, where it used to be nine substring searches of the yql
     * per query and eighteen on a degraded one — on a string that runs to
     * hundreds of kilobytes when the clause is an id list. It is also finally
     * RIGHT: see [VespaQuery.shape] for the three markers that matched nothing
     * the builder has ever emitted.
     */
    private fun shapeOf(vq: VespaQuery): String = if (vq.complete) "complete,${vq.shape}" else vq.shape

    /** Book one engine query against the ledger, keyed by the rank profile that priced it. */
    private fun publish(
        vq: VespaQuery,
        root: SearchRoot,
        timing: VespaTiming?,
        activity: Activity? = null,
    ) {
        val l = ledger ?: return
        l.engineQuery(
            activity = activity,
            // The clause SHAPE, never the values — same rule as DegradedReads.
            shape = activity?.let { shapeOf(vq) },
            profile = vq.ranking,
            engineNanos = timing?.totalNanos() ?: 0L,
            summaryNanos = timing?.summaryNanos() ?: 0L,
            docsMatched = root.fields.totalCount.toLong(),
            hitsServed = root.children.size.toLong(),
            degraded = !root.coverage.undegraded,
        )
    }

    /** The grouping/count paths need the full tree; [searchRoot] does not (it decodes hits directly). */
    private suspend fun queryRoot(
        vq: VespaQuery,
        hits: Int,
    ): JsonObject? {
        val t0 = if (ledger?.slowQueryThresholdNanos != null) System.nanoTime() else 0L
        val text = queryBody(vq, hits)
        val envelope = Json.parseToJsonElement(text).jsonObject
        // Published here too, not just in [searchRoot]. COUNT and the
        // distinct-author grouping come through this path, so without it a
        // NIP-45 count was invisible in the engine altitude while still paying
        // for the `presentation.timing` it asked for and threw away.
        publishGrouping(vq, envelope)
        captureSlow(
            vq,
            t0,
            envelope["timing"]?.let { runCatching { VESPA_JSON.decodeFromJsonElement(VespaTiming.serializer(), it) }.getOrNull() },
            hitsServed = 0L,
            docsMatched =
                (
                    envelope["root"]
                        ?.jsonObject
                        ?.get("fields")
                        ?.jsonObject
                        ?.get("totalCount") as? JsonPrimitive
                )?.content?.toLongOrNull() ?: 0L,
        )
        return envelope["root"]
            ?.jsonObject
            ?.also { root -> root["coverage"]?.let { VESPA_JSON.decodeFromJsonElement(SearchCoverage.serializer(), it) }?.requireComplete() }
    }

    /** [publish] for the grouping/count shape, whose tree is walked generically rather than decoded into [SearchRoot]. */
    private fun publishGrouping(
        vq: VespaQuery,
        envelope: JsonObject,
    ) {
        val l = ledger ?: return
        val timing = envelope["timing"]?.let { runCatching { VESPA_JSON.decodeFromJsonElement(VespaTiming.serializer(), it) }.getOrNull() }
        val fields = envelope["root"]?.jsonObject?.get("fields")?.jsonObject
        val matched = (fields?.get("totalCount") as? JsonPrimitive)?.content?.toLongOrNull() ?: 0L
        l.engineQuery(
            profile = vq.ranking,
            engineNanos = timing?.totalNanos() ?: 0L,
            summaryNanos = timing?.summaryNanos() ?: 0L,
            docsMatched = matched,
            hitsServed = 0L,
            degraded = false,
        )
    }

    /**
     * Capture this engine call in the slow-read ring, if it beat the store's
     * threshold.
     *
     * AT THIS ALTITUDE because this is where the fields exist: the rank profile
     * that priced the query, Vespa's own split of its time, and how much it
     * matched against how much it served. The store above knows the wall clock
     * of the whole read and nothing about which of its engine calls was the
     * slow one; the histograms already carry that whole-read distribution.
     *
     * A SLOW READ IS THEREFORE A SLOW ENGINE CALL, not a slow `query()` — the
     * page says so, and it is the more actionable of the two: one REQ fans out
     * into companion queries and admission probes, and "which one" is the
     * question a 4-second search leaves you with.
     *
     * [t0] of 0 means the store keeps no ring; this then costs one comparison.
     */
    private suspend fun captureSlow(
        vq: VespaQuery,
        t0: Long,
        timing: VespaTiming?,
        hitsServed: Long,
        docsMatched: Long,
    ) {
        if (t0 == 0L) return
        val l = ledger ?: return
        val wall = System.nanoTime() - t0
        // Checked here as well as inside `slowRead`, so a fast query does not
        // pay the coroutine-context read that names the activity.
        if (wall < (l.slowQueryThresholdNanos ?: return)) return
        l.slowRead(
            activity = currentActivity(),
            profile = vq.ranking,
            wallNanos = wall,
            engineNanos = timing?.totalNanos() ?: 0L,
            summaryNanos = timing?.summaryNanos() ?: 0L,
            hits = hitsServed,
            docsMatched = docsMatched,
            // The YQL, bounded. It is the query, which is what makes this row
            // worth keeping and also what makes the whole ring client data —
            // the store keeps none of it unless an operator sets a threshold.
            detail = vq.yql.take(SLOW_DETAIL_CHARS),
        )
    }

    /** One-line feed-client health for status lines; see [VespaFeed.statusLine]. */
    fun feedStatus(): String = feed.statusLine()

    /**
     * WHAT THE ENGINE HAS LEFT — proton's memory and disk against the limits
     * that block feed, per content node. Null until the first probe succeeds.
     *
     * Suspend, and deliberately not folded into [CostLedger.snapshot]: the
     * snapshot is synchronous and must stay that way, so a page that wants
     * headroom asks for it rather than making every counter read do I/O.
     * [EngineResources] caches, so calling this per page render is one request
     * per [EngineResources.TTL_MILLIS] and no more.
     */
    suspend fun engineHeadroom(): EngineResources.Usage? = resources.usage()

    /** Feed operations in flight right now — the gauge behind a backpressure tile. */
    fun feedInflight(): Long = feed.inflight()

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    override fun close() = feed.close()

    internal companion object {
        /** Concurrent document-API gets for a pure-id lookup. Gets are light (no summary stage to overrun), so this floats above QUERY_FANOUT. */
        const val ID_GET_FANOUT = 32

        /**
         * How much of a slow read's YQL is kept. Bounded because the ring is
         * bounded in ROWS and a YQL with a thousand ids in it would make the
         * ring's memory a function of the query rather than of the ring.
         */
        const val SLOW_DETAIL_CHARS = 400

        /**
         * Extra hits a limit'd recency recall asks for beyond its limit, so
         * the boundary timestamp's tie group usually arrives complete and the
         * id tiebreak resolves in memory (see [recallSummaries]).
         */
        const val TIE_SLACK = 64

        /**
         * Ids per cursor page of a snapshot walk — see [visitIds].
         *
         * THE ENGINE CHARGES FOR THE RANGE, NOT THE ROWS, so this constant
         * divides the cost of every id walk in the process. A page is `order by
         * created_at desc` with a limit: proton matches every document in the
         * cursor's remaining range, sorts, and returns the top N. The limit
         * never touches the matching, which is nearly all of the cost.
         *
         * Measured on staging (2026-09-10), one unranked walk shape, kind 1
         * over a week — `totalCount` 2,809,568 and `coverage.full` at every
         * size, so these differ only in rows returned:
         *
         *     limit    per page   pages    total work   vs 2,000
         *      2,000     0.122s    N/2k       1.00
         *     20,000     0.227s    N/20k      0.186        5.4x less
         *     50,000     0.363s    N/50k      0.119        8.4x less
         *    100,000     0.643s    N/100k     0.105        9.5x less
         *
         * Ten times the rows for 1.86x the time. What that was costing: the
         * mirror's `Snapshot` walks were 79.3% of ALL engine time on staging
         * (10.9 of 13.7 core-hours in 14.5h) across 653,476 pages that matched
         * 148,493,275,456 documents between them — 227,196 matched per page to
         * return 2,000 ids. With no users on the cluster at all.
         *
         * 20,000 and not further: the gain past it is small and the costs are
         * not. [TIE_DENSE_FACTOR] multiplies this into the tie-group threshold,
         * and a group accepted there is fetched by an UNBOUNDED window query.
         * The hard ceiling is the query profile's `maxHits` (100,000): the
         * fetch is `PAGE_IDS + TIE_SLACK`, and a page over that cap is refused
         * outright rather than trimmed, so 100,000 is not a value this may take.
         */
        const val PAGE_IDS = 20_000

        /**
         * Rows the walk's DECISION probe asks for — deliberately NOT [PAGE_IDS].
         *
         * These were one number, and raising the page raised the probe with it.
         * That coupling is wrong in both directions: the decision needs a
         * SAMPLE, not a page, and tying the two means tuning throughput also
         * moves the sensitivity of a choice whose wrong branch costs a
         * whole-corpus read. 2,000 is what the probe asked for before the page
         * was widened, and is measured to answer this shape completely:
         * 140 kinds over a 23h window, 943,949 matches, 100% coverage, 0.13s.
         *
         * THE TIE THRESHOLD IS NOT THIS, and splitting these constants once
         * moved it here by accident. How many rows it takes to SEE a tie group
         * is this number; how wide a group the cursor can AFFORD is
         * [PAGE_IDS] × [TIE_DENSE_FACTOR], because the page is what pays for it.
         * Lowering this must never make the walk scan more.
         */
        const val PROBE_IDS = 2_000

        // Pages' worth of one tied second past which the scan is the cheaper
        // walk — see visitIds.
        const val TIE_DENSE_FACTOR = 4

        /** Hits a [searchScored] call fetches when the query names no limit — an inspection surface, not a recall path. */
        const val DEFAULT_SCORED_HITS = 100

        /**
         * Per-operation feed deadline. A silently half-dead HTTP/2 connection
         * (e.g. severed by an engine restart) makes `await()` hang FOREVER,
         * which deadlocks the single-writer store behind it; a timeout turns
         * that hang into a retryable failure.
         */
        fun feedParams(): OperationParameters = OperationParameters.empty().timeout(Duration.ofSeconds(30))

        /**
         * Operations enqueued per awaited chunk on every pipelined feed call.
         * At or above the throttler's admission window (the default 32
         * connections x 128 streams), so nothing is serialised that would have
         * overlapped — and no operation waits in the client's queue past the
         * deadline that starts when it is enqueued (see [putAll]).
         */
        const val FEED_CHUNK = 4_096
    }
}
