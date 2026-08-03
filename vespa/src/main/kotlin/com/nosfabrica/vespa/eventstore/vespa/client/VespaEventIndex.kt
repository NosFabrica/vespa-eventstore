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
import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.OperationParameters
import ai.vespa.feed.client.Result
import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc
import com.nosfabrica.vespa.eventstore.vespa.doc.SearchFields
import com.nosfabrica.vespa.eventstore.vespa.mapBounded
import com.nosfabrica.vespa.eventstore.vespa.query.EventQuery
import com.nosfabrica.vespa.eventstore.vespa.query.EventSelection
import com.nosfabrica.vespa.eventstore.vespa.query.EventYql
import com.nosfabrica.vespa.eventstore.vespa.query.FuzzyWordGroup
import com.nosfabrica.vespa.eventstore.vespa.query.VespaQuery
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
import kotlinx.serialization.json.put
import okhttp3.Request
import java.io.IOException
import java.net.URLEncoder
import java.time.Duration

/**
 * The real [EventIndex]: Vespa over HTTP. Writes go through Vespa's official
 * feed client (HTTP/2 multiplexed, per-doc ordering, retries built in) and are
 * AWAITED before returning. The store's read-your-writes contract needs the
 * ack, and proton makes an acked write visible to search. Reads use the plain
 * document API (get) and `/search/` (query). Transport concerns live beside
 * this class: [VespaFeed] builds and tunes the feed client, [VespaHttp] owns
 * the read connections' protocol, liveness, deadlines and retry behavior.
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
     * JSON-Lines stream. Each slice is roughly one visitor thread on the
     * content node, so the walk scales near-linearly with slices until they
     * cover the node's cores, then ~15% more from 2x oversubscription, then
     * nothing (measured on a 4-core node: 1->4 slices 2.0x, 4->8 1.15x,
     * 8->16 flat). The default is therefore 2 x this host's cores — right
     * when the store runs beside its content node, a floor when the cluster
     * is bigger; set `VESPA_VISIT_SLICES` to 2 x the CONTENT NODE's cores
     * when they differ. Slicing does NOT apply to the paged fallback, where
     * it was measured 11x SLOWER than a serial walk (each sliced request
     * returns roughly one small bucket, not a full page).
     */
    private val visitSlices: Int =
        (
            System.getenv("VESPA_VISIT_SLICES")?.toIntOrNull()
                ?: (2 * Runtime.getRuntime().availableProcessors()).coerceIn(VISIT_SLICES_MIN, VISIT_SLICES_MAX)
        ).coerceAtLeast(1),
    /**
     * Backend bucket parallelism WITHIN each paged visit request (document-API
     * `concurrency`), used only by the serial paged fallback ([pagedWalk]).
     * Distribution buckets hold only a few hundred docs each on a large corpus,
     * so filling a 1024-doc page at concurrency 1 reads several buckets
     * back-to-back; 8 halved the fallback's wall clock in the A/B. Streamed
     * visits do NOT use it — they pin bucket concurrency to 1, which is what
     * makes their resume exactly-once (see [streamedSlice]). Total visitor
     * pressure is what wedges a small node's document API (see VespaHttp's visit client),
     * so keep the product of concurrent visits and this figure modest
     * (the read-deadline rationale lives on VespaHttp's visit client).
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
    /**
     * Window-plan bare recency scans ([planRecency]): a limit'd query with no
     * selective dimension pays the engine's match phase over EVERY posting the
     * kinds match (measured ~100ms per million) just to keep the newest few.
     * The planner probes exact [count]s (~5ms) to find a `since` window proven
     * to hold >= limit matches, then runs the query windowed — same result set,
     * ~10x less match work on a live corpus. `VESPA_QUERY_PLANNER=0` disables.
     */
    private val queryPlanning: Boolean =
        System.getenv("VESPA_QUERY_PLANNER")?.let { it != "0" && !it.equals("false", ignoreCase = true) } ?: true,
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

    // Read transport (h2c prior knowledge, ping liveness, the visit client's
    // read deadline, transient-overload retries) lives in [VespaHttp]; the
    // read paths below only build URLs/bodies.
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
        return DECODER.decodeFromString<DocEnvelope>(resp.body()).fields?.toDoc(withNearState = true)
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

    /**
     * Bulk remove with the docs in hand: the docid comes straight from each doc
     * ([docIdOf]), so the address-keyed resolve-by-get that [removeAll] must do
     * per id disappears. Same pipelining as [putAll].
     */
    override suspend fun removeDocs(docs: List<EventDoc>) {
        docs.map { feed.client.remove(DocumentId.of(NAMESPACE, DOCTYPE, docIdOf(it)), feedParams()) }.forEach { it.await() }
    }

    /**
     * False from the first 400 naming the `recency` rank profile: the serving
     * schema predates it (deployIfAbsent never redeploys onto a serving
     * cluster), so limit'd recall demotes to `unranked` — the pre-profile
     * behavior — and the count-probe planner takes the small limits back
     * (see [planRecency]). One failed query flips it for the life of this
     * client; a schema redeploy plus restart restores the profile path.
     */
    @Volatile private var recencyProfileAvailable = true

    /** [q] rebuilt for a schema without the `recency` profile — a no-op while the profile serves. */
    private fun demoteRecency(q: EventQuery): EventQuery = if (!recencyProfileAvailable && EventYql.usesRecencyProfile(q)) q.copy(ranking = EventYql.RANK_UNRANKED) else q

    /**
     * False from the first 400 naming a `recency_gated*` profile: the serving
     * schema predates the observer gate (deployIfAbsent never redeploys onto a
     * serving cluster), so gated recall demotes to plain ranking-free recall —
     * FAIL-OPEN, the pre-gate behavior: the feed serves ungated until the
     * operator redeploys the schema, consistent with how every other missing
     * trust input degrades (an absent observer, an unranked author). One
     * failed query flips it for the life of this client; a schema redeploy
     * plus restart restores the gate.
     */
    @Volatile private var gatedProfileAvailable = true

    private fun EventQuery.isGated(): Boolean = ranking == EventYql.RANK_RECENCY_GATED || ranking == EventYql.RANK_RECENCY_GATED_EXACT

    /**
     * [q] rebuilt for a schema without the gated profiles — a no-op while they
     * serve. Demotes to a RANKING-FREE query, NOT [EventYql.RANK_UNRANKED]:
     * the fallback must regain the recency profile and the count-probe
     * planner a plain query would have (both key on `ranking == null`), or
     * every legacy-schema feed query would run as a bare unranked scan —
     * slower than the pre-gate behavior it falls back to.
     */
    private fun demoteGated(q: EventQuery): EventQuery = if (!gatedProfileAvailable && q.isGated()) q.copy(ranking = null) else q

    /**
     * Run [attempt] with the profile compatibility nets: a 400 naming the
     * `recency` or `recency_gated*` profile means the serving schema predates
     * it — flip the matching flag and rerun the demoted query instead of
     * failing the REQ. Any other failure propagates untouched.
     */
    private suspend fun <T> recencySafe(
        q: EventQuery,
        attempt: suspend (EventQuery) -> T,
    ): T =
        try {
            // demoteGated FIRST: it strips the ranking, which is what lets
            // demoteRecency (and the profile selection it guards) see the
            // fallback as the plain query it now is.
            attempt(demoteRecency(demoteGated(q)))
        } catch (e: IllegalArgumentException) {
            // queryBody's status guard is a require(), hence IllegalArgument.
            // No flag check here: an attempt that actually ran demoted was
            // unranked and can never 400 naming the profile, so this match
            // already proves the attempt used it — and re-reading the flag
            // would race a concurrent query's flip into a spurious failure.
            // The two nets can't cross-fire: a gated query never satisfies
            // usesRecencyProfile (it carries a ranking), and a plain recency
            // 400's message ("recency") never contains "recency_gated".
            val is400 = e.message?.contains("400") == true
            when {
                is400 && q.isGated() && e.message?.contains(EventYql.RANK_RECENCY_GATED) == true -> {
                    gatedProfileAvailable = false
                    attempt(demoteRecency(demoteGated(q)))
                }

                is400 && EventYql.usesRecencyProfile(q) && e.message?.contains(EventYql.RANK_RECENCY) == true -> {
                    recencyProfileAvailable = false
                    attempt(demoteRecency(q))
                }

                else -> {
                    throw e
                }
            }
        }

    /**
     * False from the first 400 naming a near attribute field (name_parts/…):
     * the serving schema predates the prefix/fuzzy fields (deployIfAbsent
     * never redeploys onto a serving cluster), so search queries demote to
     * exact + gram matching — the pre-near behavior — instead of failing
     * every REQ. One failed query flips it for the life of this client; a
     * schema redeploy plus restart restores the near tier. Same shape as
     * [recencyProfileAvailable].
     */
    @Volatile private var nearFieldsAvailable = true

    /** [q] rebuilt for a schema without the near attribute fields — a no-op while they serve. */
    private fun demoteNear(q: EventQuery): EventQuery = if (!nearFieldsAvailable && q.nearMatching) q.copy(nearMatching = false) else q

    /**
     * Run [attempt] with the near-fields compatibility net: a 400 naming one
     * of [FuzzyWordGroup.NEAR_FIELDS] means the serving schema predates them —
     * flip [nearFieldsAvailable] and rerun the demoted query instead of
     * failing the REQ. Any other failure propagates untouched. Only queries
     * that actually carry a search term can hit this (near clauses are only
     * emitted for search words), but the net wraps every path that routes a
     * term: search, rawSearch, and the count/grouping family.
     */
    private suspend fun <T> nearSafe(
        q: EventQuery,
        attempt: suspend (EventQuery) -> T,
    ): T =
        try {
            attempt(demoteNear(q))
        } catch (e: IllegalArgumentException) {
            // queryBody's status guard is a require(), hence IllegalArgument.
            // The message check proves the attempt ran WITH the near clauses
            // (a demoted attempt cannot 400 naming a field it never sent), so
            // no flag re-read — same reasoning as recencySafe.
            val missingField = e.message?.contains("400") == true && FuzzyWordGroup.ALL_NEAR_FIELDS.any { e.message?.contains(it) == true }
            if (q.search.isNullOrBlank() || !q.nearMatching || !missingField) throw e
            nearFieldsAvailable = false
            attempt(demoteNear(q))
        }

    /**
     * False from the first 400 naming the `dedup` document-summary: the serving
     * schema predates it (deployIfAbsent never redeploys onto a serving
     * cluster), so existence checks demote to the full-summary search — the
     * pre-summary-class behavior, correct just slower. One failed query flips
     * it for the life of this client; a schema redeploy plus restart restores
     * the summary-free path. Same shape as [recencyProfileAvailable].
     */
    @Volatile private var dedupSummaryAvailable = true

    /**
     * The summary-free existence check: `select id` under the attribute-only
     * `dedup` summary class ([EventYql.buildExistence]), so proton answers
     * membership from the id attribute in memory — no disk summary fetch, no
     * document reconstruction, ~76% less response to transfer and none of it
     * decoded into documents at the mirror workload's hit rate; 2.2–2.3× the
     * full-summary path end to end (measured: see benchmark/README.md,
     * "Dedup / existence-check A/B"). Works identically under address-keying:
     * the `id` ATTRIBUTE carries the event id whatever the docid is, which a
     * document-API get here would not (an address-keyed replaceable is not at
     * its id docid — the reason this stays on the search stack).
     */
    override suspend fun existingIds(ids: List<String>): Set<String> {
        // Demotion first: a demoted client must not build (and discard) the
        // ~35KB YQL per chunk — super rides search(), which handles the
        // no-valid-ids case through EventYql.build's own null contract.
        if (!dedupSummaryAvailable) return super.existingIds(ids)
        val vq = EventYql.buildExistence(ids) ?: return emptySet()
        val root =
            try {
                searchRoot(vq, hits = Int.MAX_VALUE)
            } catch (e: IllegalArgumentException) {
                // queryBody's status guard is a require(), hence IllegalArgument.
                // A 400 naming the class proves the attempt used it — same
                // reasoning as recencySafe; anything else propagates untouched.
                val missingClass = e.message?.contains("400") == true && e.message?.contains(EventYql.SUMMARY_DEDUP) == true
                if (!missingClass) throw e
                dedupSummaryAvailable = false
                return super.existingIds(ids)
            }
        return root.children.mapNotNullTo(HashSet()) { hit -> hit.fields?.id?.takeIf { it.isNotEmpty() } }
    }

    override suspend fun search(query: EventQuery): List<EventDoc> {
        // Pure-id recall bypasses /search/: each id is a direct document-API key
        // lookup (~35% faster than a search over the id attribute here), which is
        // what a REQ-by-id and the bulk dedup preload both do. The moment ANY other
        // constraint is present it falls through to the search stack below. The
        // expiry filter and newest-first order are applied exactly as YQL would, so
        // results are identical to the search path.
        if (query.isPureIdLookup()) return getByIds(query)
        // demoteGated BEFORE planRecency: once the gated-profile flag has
        // flipped (legacy schema), the demoted (ranking-free) query must
        // regain the recency profile / count-probe planner it would have had
        // pre-gate — planned on the gated original, the planner stands down
        // (ranking is set) and every fallback feed query would run as a bare
        // unranked scan. A no-op while the profiles serve; recencySafe's own
        // demote stays for the first failing query, whose flag flips
        // mid-flight.
        return nearSafe(query) { qn ->
            recencySafe(planRecency(demoteGated(qn))) { q ->
                // Stream the hits straight into docs (no full JsonElement tree): the
                // response is decoded into flat DTOs, allocating the target objects
                // directly instead of a JsonObject/JsonArray/JsonPrimitive wrapper per
                // field. This is the query hot path, so that saved garbage matters.
                recallSummaries(q).mapNotNull { it.toDoc() }
            }
        }
    }

    /**
     * Recency-ordered recall with the id tiebreak applied CLIENT-SIDE. The
     * engine sorts by `created_at desc` alone — compound-sorting by the id
     * string attribute paid UCA collation over the whole match set (0.22s ->
     * 1.3s on a 2M-match scan) — and this restores the exact
     * `created_at desc, id asc` contract from the page:
     *
     * With a single-key sort, every doc STRICTLY newer than the boundary
     * timestamp T (the limit-th hit's created_at) is guaranteed present; only
     * membership among the docs AT T is engine-arbitrary. So the query
     * overfetches [TIE_SLACK] extra hits — if anything older than T arrived,
     * or the engine ran out of matches, the whole T tie group is in hand and
     * an in-memory sort resolves the boundary exactly. Only a tie group wider
     * than the slack (more than TIE_SLACK same-second matches at the boundary)
     * pays one extra `[T,T]` window query, which recalls just that second.
     *
     * Ranked queries (search terms, trust sorts) keep the engine's score
     * order untouched. Unlimited recency queries have no boundary to resolve
     * — the sort alone restores the contract.
     */
    private suspend fun recallSummaries(q: EventQuery): List<VespaSummary> {
        // The gated profiles are recency-ordered too (score IS created_at), so
        // they take the same overfetch + tie-resolution path — their engine
        // score ties are exactly as arbitrary as the single-key sort's.
        val recencyOrdered =
            (q.ranking == null && q.search.isNullOrBlank() && q.phrases.isEmpty()) ||
                q.ranking == EventYql.RANK_UNRANKED ||
                q.ranking == EventYql.RANK_RECENCY ||
                q.ranking == EventYql.RANK_RECENCY_GATED ||
                q.ranking == EventYql.RANK_RECENCY_GATED_EXACT
        if (!recencyOrdered) {
            return recallRoot(q)?.children?.mapNotNull { it.fields }?.filter { it.id.isNotEmpty() } ?: emptyList()
        }
        val limit = q.limit
        if (limit == null) {
            val all = recallRoot(q)?.children?.mapNotNull { it.fields }?.filter { it.id.isNotEmpty() } ?: emptyList()
            return all.sortedWith(SUMMARY_NEWEST_FIRST)
        }
        // A non-positive limit matches nothing (EventYql.build's contract) —
        // the overfetch must not resurrect it into a real query.
        if (limit <= 0) return emptyList()
        val fetch = q.copy(limit = limit + TIE_SLACK)
        var hits =
            recallRoot(fetch)?.children?.mapNotNull { it.fields }?.filter { it.id.isNotEmpty() }
                ?: return emptyList()
        if (hits.size > limit) {
            val t = hits[limit - 1].createdAt
            // The tie group at T arrived complete if the engine either ran out
            // of matches before the overfetch limit or already emitted a doc
            // strictly older than T (all ==T docs sort before any <T doc).
            val complete = hits.size < fetch.limit!! || hits.last().createdAt < t
            if (!complete) {
                // A gated query's tie window must stay gated (the exact
                // variant — a [t,t] window is tiny) or the rerun would
                // resurrect below-floor authors into the boundary group.
                val gated = q.ranking == EventYql.RANK_RECENCY_GATED || q.ranking == EventYql.RANK_RECENCY_GATED_EXACT
                val tieRanking = if (gated) EventYql.RANK_RECENCY_GATED_EXACT else EventYql.RANK_UNRANKED
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
     * The recall query, guarded against match-phase UNDER-DELIVERY. Vespa
     * documents that a match-phase-limited query "risk[s] sometimes getting
     * less than the configured hits back" on unevenly distributed corpora, and
     * it does NOT re-run on its own — so a `recency`-profile response that is
     * match-phase-degraded AND short of the limit is rerun `unranked` (exact).
     * A degraded response with a FULL page needs no rerun: everything the cut
     * excluded is older than everything returned, so a full page IS the exact
     * top-`limit`. This is what makes accepting match-phase degradation sound
     * for every filter shape, skewed ones included — the degradation can cost
     * a second query, never a result.
     */
    private suspend fun recallRoot(q: EventQuery): SearchRoot? {
        val vq = EventYql.build(q) ?: return null
        val root = searchRoot(vq, hits = hitsFor(q))
        // Both match-phase profiles share the degradation guard; they differ
        // only in what "exact" means for the rerun — unranked for plain
        // recency, the full-scan gated variant for gated recall (the gate must
        // survive the rerun or under-delivery would serve spam).
        val matchPhased = vq.ranking == EventYql.RANK_RECENCY || vq.ranking == EventYql.RANK_RECENCY_GATED
        if (matchPhased && root.coverage.matchPhaseDegraded) {
            // A FULL page proves exactness only on ONE content node: max-hits
            // is per node and each node picks its own cut threshold, so with
            // several nodes one node's overshoot can drop mid-page docs while
            // the others fill the page. Short page anywhere, or full page on a
            // multi-node cluster -> rerun exact. A boundary TIE also demotes:
            // the match-phase keeps candidates by created_at alone, so a tie
            // straddling the cut can drop a lower-id member while a higher-id
            // one fills the page — if the page's oldest timestamp appears more
            // than once, ties at the boundary provably exist. (A dropped tie
            // behind a UNIQUE returned boundary needs the estimator to cut
            // inside a tiny tie group 10x under max-hits — residual, accepted.)
            val timestamps = root.children.mapNotNull { it.fields?.createdAt }
            val oldest = timestamps.minOrNull()
            val boundaryTied = oldest != null && timestamps.count { it == oldest } > 1
            val provablyExact = root.children.size >= (q.limit ?: 0) && root.coverage.nodes <= 1 && !boundaryTied
            if (!provablyExact) {
                val exactRanking = if (vq.ranking == EventYql.RANK_RECENCY_GATED) EventYql.RANK_RECENCY_GATED_EXACT else EventYql.RANK_UNRANKED
                val exact = q.copy(ranking = exactRanking)
                // The rerun must be exact but need not be UNBOUNDED — a full
                // corpus-order scan here would make the profile a net loss on
                // every cluster where degradation is routine:
                //  - FULL page: any true top-`limit` doc the cut excluded is
                //    newer than at least one returned doc, so `since = oldest
                //    returned created_at` (inclusive — ties included) bounds
                //    the rerun to a page-sized window, provably lossless.
                //    (Holds gated too: the gate only shrinks what the cut kept,
                //    and the rerun applies the same gate.)
                //  - SHORT page: nothing returned bounds the miss. Plain
                //    recency routes through the count-probe ladder the profile
                //    normally skips; a gated short page cannot (the probes
                //    count the UNGATED match set) and pays the full-scan gated
                //    rerun — the one genuinely expensive path, reached only
                //    when the newest ~max-hits candidates hold fewer than
                //    `limit` trusted hits.
                val rerun =
                    when {
                        root.children.size >= (q.limit ?: 0) && oldest != null -> {
                            exact.copy(since = maxOf(q.since ?: Long.MIN_VALUE, oldest))
                        }

                        // The shape test must lift the exactness stamp first:
                        // isBareRecencyScan reads `ranking == null` as the
                        // planner opt-out (see sweep), and `exact` always
                        // carries RANK_UNRANKED here — testing it stamped made
                        // this branch dead and every short page paid the full
                        // scan. UNRANKED only: a gated short page must NOT be
                        // windowed (the probes count the UNGATED match set),
                        // exactly as the comment above promises.
                        queryPlanning && exactRanking == EventYql.RANK_UNRANKED && exact.copy(ranking = null).isBareRecencyScan() -> {
                            planWindow(exact)
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
     * investigation, and without it every partial match reads as noise. Tier
     * is null when the profile declares no match-features (unranked/recency)
     * or the serving schema predates them.
     */
    suspend fun searchScored(query: EventQuery): List<ScoredHit> =
        nearSafe(query) { qn ->
            // recencySafe: a term-less scored query auto-selects the recency
            // profile, which an old serving schema 400s — same net as search().
            recencySafe(qn) { q ->
                val vq = EventYql.build(q) ?: return@recencySafe emptyList()
                searchRoot(vq, hits = q.limit ?: DEFAULT_SCORED_HITS)
                    .children
                    .mapNotNull { hit -> hit.fields?.let { f -> f.toDoc()?.let { ScoredHit(it, hit.relevance, tierOf(f.matchfeatures)) } } }
            }
        }

    /** The rank band a hit arrived through, from the profile's match-features; null when none were served. */
    private fun tierOf(mf: JsonObject?): String? {
        if (mf == null) return null

        fun on(feature: String): Boolean = ((mf[feature] as? JsonPrimitive)?.content?.toDoubleOrNull() ?: 0.0) > 0.0
        return when {
            on("any_token_match") || on("name_match") || on("has_token_match") -> "name"
            on("any_near_match") || on("near_name_match") -> "near"
            on("weak_match") -> "weak"
            on("identity_match") -> "identity"
            on("affiliation_match_text") || on("affiliation_match") -> "affiliation"
            else -> "gram"
        }
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
        // Same demoteGated-before-planRecency ordering as [search] — see there.
        return nearSafe(query) { qn ->
            recencySafe(planRecency(demoteGated(qn))) { q ->
                recallSummaries(q).mapNotNull { it.toRaw() }
            }
        }
    }

    /**
     * A bare recency scan: limit'd, unranked, and with no selective dimension —
     * the REQ shape that makes the engine's match phase visit EVERY posting its
     * kinds have (measured ~100ms per million) just to keep the newest few.
     * Everything selective (ids, authors, tags, search) already prunes the
     * match phase and is measured in single-digit milliseconds.
     */
    private fun EventQuery.isBareRecencyScan(): Boolean =
        (limit ?: 0) > 0 &&
            // An explicit rank profile is never a recency scan. Trust-sorted
            // profiles (sort:rank) aren't recency-ordered at all — windowing
            // one silently drops every higher-ranked older hit — and the
            // recency_gated profile drops hits the count probe counted: a
            // window proven full of MATCHES isn't proven full of ABOVE-FLOOR
            // matches, so windowing it could starve a page while older trusted
            // hits exist. Only ranking-free queries are recency scans. (This is
            // also the opt-out: internal reads stamp RANK_UNRANKED to skip the
            // planner — see NostrSemanticsStore.sweep.)
            ranking == null &&
            search == null &&
            // Phrases are search text (already selective, and relevance-ordered
            // — a recency window would silently drop older relevant hits);
            // notSearch is NOT excluded: an exclusion-only query is still a
            // recency scan, and the count probes carry the same exclusion
            // clause, so a proven window stays proven.
            phrases.isEmpty() &&
            ids.isEmpty() &&
            authors.isEmpty() &&
            owners.isEmpty() &&
            tags.isEmpty() &&
            tagsAll.isEmpty() &&
            expiresBefore == null

    /**
     * Query planning for bare recency scans: find a `since` window PROVEN (by
     * an exact [count] probe, ~5ms) to hold at least `limit` matches, and run
     * the query inside it. Correctness is structural, not statistical: the
     * window is anchored at the query's newest end (`until`, else now), so
     * every event outside it is strictly older than every event inside — the
     * top-`limit` of a full window IS the top-`limit` of the unbounded query,
     * the same events in the same order. A window is only used when its probe
     * says >= limit, so no result can be lost; if no ladder rung is provably
     * full, the query runs unchanged. The ladder is geometric (hour, day,
     * month): a rung that overshoots still visits at most a fraction of what
     * the unbounded scan would, and probes on a dead or sparse corpus cost
     * three counts (~15ms) before falling through — the live-relay case this
     * exists for anchors near now and exits on the first rung.
     */
    private suspend fun planRecency(q: EventQuery): EventQuery {
        if (!queryPlanning || !q.isBareRecencyScan()) return q
        // Division of labor with the match-phase `recency` profile: the profile
        // owns the small limits it covers — probing there costs more than it
        // saves (measured 0.6x on a shape match-phase serves in 15ms). The
        // planner windows what match-phase can't (limits past the 10x-headroom
        // gate), and takes everything back when the serving schema lacks the
        // profile.
        if (recencyProfileAvailable && EventYql.usesRecencyProfile(q)) return q
        return planWindow(q)
    }

    /**
     * The count-probe ladder itself, shared by [planRecency] and [recallRoot]'s
     * short-page rerun (which windows queries [usesRecencyProfile] told
     * [planRecency] to skip). Requires an unranked, limit'd [q].
     *
     * The window is proven >= limit at PROBE time; a deletion committing
     * between the probe and the windowed query can transiently shrink it below
     * the limit while older matches survive outside — one short page on a rare
     * interleaving, which a paginating client's next `until` request recovers.
     * Accepted: reads never hold the writer lock, so no probe can be atomic
     * with its query.
     */
    private suspend fun planWindow(q: EventQuery): EventQuery {
        val anchor = q.until ?: (System.currentTimeMillis() / 1000)
        for (window in PLANNER_WINDOWS) {
            val since = anchor - window
            // An existing `since` at least this tight makes the rung (and any
            // wider one) pointless — the query is already windowed.
            if (q.since != null && since <= q.since) return q
            // A failed probe (degraded coverage, transient error) just means
            // "don't window" — planning is an optimization, and the real query
            // still carries its own guarantees. Cancellation is NOT a failed
            // probe: swallowing it would enqueue one more engine request on a
            // job that is already dead.
            val matches =
                try {
                    count(q.copy(since = since, limit = null))
                } catch (e: kotlin.coroutines.cancellation.CancellationException) {
                    throw e
                } catch (_: Exception) {
                    return q
                }
            if (matches >= q.limit!!) return q.copy(since = since)
        }
        return q
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
            page.isEmpty() || onPage(page)
        }
    }

    /**
     * The tags projection via the same sliced visit as [visitIds], fieldSet
     * `event:tags`: each match contributes ONLY its stored tag JSON (decoded
     * here to the tag array) — content, sig, and the search columns never
     * cross the wire. Queries a selection can't express fall back to the
     * search default, which returns the same tags in a single page.
     */
    override suspend fun visitTags(
        query: EventQuery,
        onPage: suspend (List<List<List<String>>>) -> Boolean,
    ) {
        val selection = EventSelection.build(query) ?: return super.visitTags(query, onPage)
        visitPages(selection, "$DOCTYPE:tags") { documents ->
            val page =
                documents.mapNotNull { d ->
                    d.fields?.tags?.let { raw ->
                        Json.parseToJsonElement(raw).jsonArray.map { tag -> tag.jsonArray.map { it.jsonPrimitive.content } }
                    }
                }
            page.isEmpty() || onPage(page)
        }
    }

    /**
     * Page every match of [selection] out of the document-API visit, calling
     * [onDocuments] with lists of lean [VisitedDoc]s (typed line/page decode —
     * no JsonElement tree per doc on a path that walks whole corpora);
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
        onDocuments: suspend (List<VisitedDoc>) -> Boolean,
    ): Unit =
        coroutineScope {
            val pages = Channel<List<VisitedDoc>>(visitSlices)
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
        emit: suspend (List<VisitedDoc>) -> Unit,
    ) {
        val base =
            "${endpoint()}/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE" +
                "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}" +
                // UNDER the client's read deadline (VespaHttp's visit client): a sparse
                // selection can honestly spend ages filling a page, and the
                // server's default (180s) outlives the 120s read timeout — the
                // client would kill and retry the identical request forever.
                // With the server timing out first, it returns a partial page
                // plus a continuation and the walk keeps moving.
                "&timeout=$VISIT_SERVER_TIMEOUT_SECONDS" +
                "&concurrency=$visitConcurrency"
        var continuation: String? = null
        while (true) {
            val resp = http.getVisit(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val env = DECODER.decodeFromString<PagedVisitEnvelope>(resp.body())
            if (env.documents.isNotEmpty()) emit(env.documents.map { VisitedDoc(it.id, it.fields) })
            continuation = env.continuation ?: return
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
        emit: suspend (List<VisitedDoc>) -> Unit,
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

                // Resume from the last certified token — but on a BUDGET even
                // when the stream ended without an exception (a clean close or
                // truncated line before the final marker): with no token
                // advance, an unbudgeted loop would replay the identical
                // request forever against a server that keeps doing it.
                // Progress resets the count (see [certify]).
                StreamEnd.INTERRUPTED -> {
                    if (++failures > QUERY_RETRIES) {
                        throw thrown ?: IOException("vespa streamed visit slice $sliceId kept ending without progress")
                    }
                    delay(500L * failures)
                }

                StreamEnd.NOT_JSONL -> {
                    // Only a fallback signal while nothing has been consumed;
                    // mid-walk it is a server misbehaving (or a refused resume
                    // request), not a version gap.
                    check(token == null && !delivered) { "vespa streamed visit refused mid-walk (400 or non-JSONL answer after progress)" }
                    return false
                }
            }
        }
    }

    /**
     * One streamed request: open, read JSON-Lines until the stream ends, and
     * classify how it ended. Put lines land in [into] as lean [VisitedDoc]s
     * (typed decode, no JsonElement tree per doc); each continuation line
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
            // cancelled (early stop, sibling failure) — after normal completion
            // its cancel() is a no-op on the finished call. UNCONFINED, not the
            // surrounding IO dispatcher: the abort must not wait for an IO
            // thread when every IO thread is exactly what's parked in these
            // blocking reads (many slices, wedged node) — unconfined runs the
            // finally on the cancelling thread immediately.
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
                        // longer parses — that is an interruption to resume
                        // from, not a fatal decode error.
                        val obj = runCatching { DECODER.decodeFromString<StreamLine>(line) }.getOrNull() ?: break
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

    /**
     * One page of FULL docs through the document API's visit — the reindex
     * primitive. `[document]` selects every real document field (the search
     * columns included: they are stored fields, written by [putOp]), so the
     * page decodes through the same [VespaSummary] a get returns. The
     * continuation token rides in [DocsPage.continuation], opaque to callers.
     * Server timeout and bucket concurrency follow [pagedWalk]'s reasoning.
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
                "&wantedDocumentCount=${maxDocs.coerceIn(1, VISIT_PAGE)}" +
                "&fieldSet=${URLEncoder.encode("[document]", "UTF-8")}" +
                "&timeout=$VISIT_SERVER_TIMEOUT_SECONDS" +
                "&concurrency=$visitConcurrency"
        val resp = http.getVisit(resumeFrom?.let { "$base&continuation=$it" } ?: base)
        require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
        val env = DECODER.decodeFromString<DocVisitEnvelope>(resp.body())
        return DocsPage(env.documents.mapNotNull { it.fields?.toDoc(withNearState = true) }, env.continuation)
    }

    override suspend fun count(query: EventQuery): Int =
        nearSafe(query) { q ->
            // A grouping count() over the full match set — NOT root.totalCount, which
            // Vespa caps under the recency `order by`'s match-phase (a 10x+ undercount
            // on large kinds). See [EventYql.buildCount].
            val root = EventYql.buildCount(q)?.let { queryRoot(it, hits = 0) }
            root?.let { countIn(it) } ?: 0
        }

    override suspend fun countDistinctAuthors(query: EventQuery): Int =
        nearSafe(query) { q ->
            // `all(group(pubkey) output(count()))` counts the GROUPS — i.e. the
            // distinct pubkeys — not the docs. The count() lands one level deeper
            // than [count]'s (inside the group list), so both share [countIn]'s
            // recursive scan. See [EventYql.buildDistinctCount].
            val root = EventYql.buildDistinctCount(q, "pubkey")?.let { queryRoot(it, hits = 0) }
            root?.let { countIn(it) } ?: 0
        }

    override suspend fun countByKind(query: EventQuery): Map<Int, Int> =
        nearSafe(query) { q ->
            // `all(group(kind) each(output(count())))` yields one leaf group per kind,
            // each carrying its `value` (the kind) and a `count()`. See [EventYql.buildKindHistogram].
            val root = EventYql.buildKindHistogram(q)?.let { queryRoot(it, hits = 0) }
            val out = LinkedHashMap<Int, Int>()
            root?.let { kindCountsInto(it, out) }
            out
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
        return nearSafe(query) { q ->
            val root = EventYql.buildDistinctAuthors(q)?.let { queryRoot(it, hits = 0) } ?: return@nearSafe emptySet()
            // group(pubkey) nests a grouplist whose leaf `group:` nodes each carry a
            // pubkey as `value`. Walk the tree and collect every group value.
            val authors = LinkedHashSet<String>()

            fun collect(node: JsonObject) {
                (node["value"] as? JsonPrimitive)?.let { if (node["id"]?.jsonPrimitive?.content?.startsWith("group:") == true) authors += it.content }
                node["children"]?.jsonArray?.forEach { collect(it.jsonObject) }
            }
            collect(root)
            authors
        }
    }

    /**
     * The same grouping as [distinctAuthors], keeping each group's `count()` —
     * which [EventYql.buildDistinctAuthors] already asks for (a group needs a
     * payload to be emitted at all) and [distinctAuthors] drops on the floor. So
     * the author set WITH per-author doc counts costs exactly one query, the
     * same one.
     */
    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> =
        nearSafe(query) { q ->
            val root = EventYql.buildDistinctAuthors(q)?.let { queryRoot(it, hits = 0) } ?: return@nearSafe emptyMap()
            val out = LinkedHashMap<String, Int>()

            // Leaf `group:` nodes only — the same discriminator [distinctAuthors]
            // uses, so an intermediate node carrying an aggregate can never be
            // mistaken for an author.
            fun collect(node: JsonObject) {
                if (node["id"]?.jsonPrimitive?.content?.startsWith("group:") == true) {
                    val author = (node["value"] as? JsonPrimitive)?.content
                    val count =
                        node["fields"]
                            ?.jsonObject
                            ?.get("count()")
                            ?.jsonPrimitive
                            ?.intOrNull
                    if (author != null && count != null) out[author] = count
                }
                node["children"]?.jsonArray?.forEach { collect(it.jsonObject) }
            }
            collect(root)
            out
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
            documents.forEach { d -> d.fields?.pubkey?.let { authors += it } }
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
            .also { it.coverage.requireComplete(allowMatchPhase = vq.ranking == EventYql.RANK_RECENCY || vq.ranking == EventYql.RANK_RECENCY_GATED) }

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
     *
     * ONE deliberate exception: [allowMatchPhase]. The match-phase profiles
     * (`recency`, `recency_gated`) ASK the engine to cut the match phase to
     * the newest ~max-hits candidates (see [EventYql.RANK_RECENCY] /
     * [EventYql.RANK_RECENCY_GATED]) — for those profiles, match-phase
     * degradation is the optimization working as designed, and [recallRoot]
     * separately verifies the page or reruns it exact. Any OTHER degradation
     * (timeout, non-ideal-state), or match-phase on a query that didn't opt
     * in, is still refused.
     */
    private fun SearchCoverage.requireComplete(allowMatchPhase: Boolean = false) {
        if (full) return
        // Vespa lists every degradation flag, false ones included — judge by
        // the flags that are actually SET, not by key presence.
        val set = degraded?.mapValues { (it.value as? JsonPrimitive)?.content == "true" }.orEmpty()
        val onlyMatchPhase = set["match-phase"] == true && set.none { (flag, on) -> on && flag != "match-phase" }
        require(allowMatchPhase && onlyMatchPhase) {
            "vespa searched only $coverage% of the corpus (degraded: ${degraded ?: "unspecified"}); " +
                "the response is a PARTIAL answer, not a small one, so it is refused rather than returned"
        }
    }

    /**
     * Rebuild a doc from the decoded summary. `tags` is the one field still parsed
     * per hit — the store keeps it as a JSON string. The search columns map
     * positionally to [SearchFields] (all-null on the trimmed recall path, so it
     * equals [SearchFields.NONE]; populated on a full get).
     *
     * [withNearState] stamps [EventDoc.storedNearFields] from the summary's
     * near arrays, and must be passed ONLY on document-API reads (get, the
     * `[document]` visit), where the response is the complete stored document —
     * there, an absent array is genuinely absent from the doc. Search summaries
     * never carry the near fields (no `| summary`), so stamping there would
     * claim every doc predates the near tier.
     */
    private fun VespaSummary.toDoc(withNearState: Boolean = false): EventDoc? {
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
            // normalized(): the feed writes "" for an absent name/display_name
            // sibling (SearchFields.fields), so fold it back to null on decode.
            search = SearchFields(name, displayName, about, nip05, lud16, website, primary, secondary, text, location).normalized(),
        ).also { if (withNearState) it.storedNearFields = nearArrays() }
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
        val nodes: Int = 1,
        val degraded: JsonObject? = null,
    ) {
        /** The engine cut the match phase — the one degradation [recallRoot] may act on rather than refuse. */
        val matchPhaseDegraded: Boolean
            get() = (degraded?.get("match-phase") as? JsonPrimitive)?.content == "true"
    }

    @Serializable
    private class SearchHit(
        val fields: VespaSummary? = null,
        val relevance: Double = 0.0,
    )

    /** `/document/v1/…` get response. */
    @Serializable
    private class DocEnvelope(
        val fields: VespaSummary? = null,
    )

    /** One projected doc off a visit page or stream — the lean carrier [visitPages] hands its consumer. */
    private class VisitedDoc(
        val id: String,
        val fields: VisitFields?,
    )

    /** The projected fields the walks ask for (`created_at[,tag_index]` / `pubkey` / `tags`); everything else is server-trimmed. */
    @Serializable
    private class VisitFields(
        @SerialName("created_at") val createdAt: Long? = null,
        @SerialName("tag_index") val tagIndex: List<String>? = null,
        val pubkey: String? = null,
        /** The stored tag JSON string ([visitTags]'s projection), decoded by the caller. */
        val tags: String? = null,
    )

    /** A paged visit response: projected docs plus the continuation. */
    @Serializable
    private class PagedVisitEnvelope(
        val documents: List<PagedVisitDoc> = emptyList(),
        val continuation: String? = null,
    )

    @Serializable
    private class PagedVisitDoc(
        val id: String = "",
        val fields: VisitFields? = null,
    )

    /** One JSON-Lines stream line: exactly one of put/continuation/message is set; anything else (sessionStats) decodes all-null. */
    @Serializable
    private class StreamLine(
        val put: String? = null,
        val fields: VisitFields? = null,
        val continuation: StreamContinuation? = null,
        val message: StreamMessage? = null,
    )

    @Serializable
    private class StreamContinuation(
        val token: String? = null,
    )

    @Serializable
    private class StreamMessage(
        val severity: String? = null,
        val text: String? = null,
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
        // The near-tier attribute arrays, present only on document-API reads
        // (no `| summary` in the schema, so search hits never carry them).
        // Decoded solely to stamp EventDoc.storedNearFields — the reindex's
        // evidence of whether this doc predates the near tier.
        @SerialName("name_parts") val nameParts: List<String>? = null,
        @SerialName("name_tokens") val nameTokens: List<String>? = null,
        @SerialName("search_primary_parts") val primaryParts: List<String>? = null,
        @SerialName("search_primary_tokens") val primaryTokens: List<String>? = null,
        @SerialName("search_secondary_tokens") val secondaryTokens: List<String>? = null,
        @SerialName("affil_tokens") val affilTokens: List<String>? = null,
        /** The rank profile's declared match-features, when the profile has any (see event.sd). */
        val matchfeatures: JsonObject? = null,
    ) {
        /** The near arrays this summary carries, keyed as fed. Only meaningful on document-API reads (see [toDoc]'s withNearState). */
        fun nearArrays(): Map<String, List<String>> =
            buildMap {
                nameParts?.let { put("name_parts", it) }
                nameTokens?.let { put("name_tokens", it) }
                primaryParts?.let { put("search_primary_parts", it) }
                primaryTokens?.let { put("search_primary_tokens", it) }
                secondaryTokens?.let { put("search_secondary_tokens", it) }
                affilTokens?.let { put("affil_tokens", it) }
            }
    }

    private companion object {
        const val NAMESPACE = "event"
        const val DOCTYPE = "event"

        /** Lenient decoder: the response carries documentid/sddocname/relevance and, on some paths, extra fields we don't model. */
        val DECODER = Json { ignoreUnknownKeys = true }

        /** Concurrent document-API gets for a pure-id lookup. Gets are light (no summary stage to overrun like big searches), so this floats well above QUERY_FANOUT. */
        const val ID_GET_FANOUT = 32

        /** Newest first (created_at desc, id asc tiebreak) — the same order the search path and the store apply. */
        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)

        /** [NEWEST_FIRST] over raw summaries — [recallSummaries] applies the id tiebreak here, not in the engine. */
        val SUMMARY_NEWEST_FIRST = compareByDescending(VespaSummary::createdAt).thenBy(VespaSummary::id)

        /**
         * Extra hits a limit'd recency recall asks for beyond its limit, so the
         * boundary timestamp's tie group usually arrives complete and the id
         * tiebreak resolves in memory ([recallSummaries]). Costs ~1ms of extra
         * summaries; only a boundary second with MORE matching events than this
         * pays the [t,t] follow-up query.
         */
        const val TIE_SLACK = 64

        /** Docs asked for per visit response (Vespa's per-request ceiling is 1024). */
        const val VISIT_PAGE = 1024

        /**
         * Bounds on the derived streamed-slice default (2 x host cores — see
         * [visitSlices]). The floor keeps tiny hosts from serializing the walk;
         * the cap keeps huge hosts from opening visitor sessions far past where
         * scaling stopped in the A/B, and comfortably below the ~64-session
         * pressure that wedged a small node's document API.
         */
        const val VISIT_SLICES_MIN = 4
        const val VISIT_SLICES_MAX = 32

        /**
         * Default bucket concurrency for the serial paged fallback — see
         * [visitConcurrency]. 8 halved the fallback's wall clock in the A/B and
         * keeps total visitor pressure at 8 sessions (one serial walk); the
         * measured wedge needed ~64.
         */
        const val VISIT_CONCURRENCY = 8

        /** Server-side timeout on paged visit requests — strictly under VespaHttp's visit read deadline, see [pagedWalk]. */
        const val VISIT_SERVER_TIMEOUT_SECONDS = 90L

        /**
         * The [planRecency] probe ladder, in seconds before the query's anchor:
         * an hour, a day, a month. Geometric so a live corpus exits on the
         * first rung that fits its event rate, and a probe miss costs one more
         * ~5ms count, not a rescan.
         */
        val PLANNER_WINDOWS = longArrayOf(3_600L, 86_400L, 2_592_000L)

        /** Brief 5xx retries per query (transient engine load-shedding, not correctness). */
        const val QUERY_RETRIES = 3

        /** Hits a [searchScored] call fetches when the query names no limit — an inspection surface, not a recall path. */
        const val DEFAULT_SCORED_HITS = 100

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
