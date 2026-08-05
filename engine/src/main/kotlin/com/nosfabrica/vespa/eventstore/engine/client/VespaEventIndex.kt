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
import com.nosfabrica.vespa.eventstore.engine.ScoredHit
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.mapBounded
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
) : EventIndex {
    private val urls: List<String> = endpoints.ifEmpty { listOf(baseUrl) }.map { it.trimEnd('/') }

    private val nextUrl =
        java.util.concurrent.atomic
            .AtomicInteger()

    /** The endpoint for one HTTP read — round-robin across [urls]. */
    private fun endpoint(): String = urls[Math.floorMod(nextUrl.getAndIncrement(), urls.size)]

    private val feed = VespaFeed(urls)

    private val http = VespaHttp()

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
            buildJsonObject { put("fields", doc.indexFields()) }.toString(),
            feedParams(),
        )

    private fun removeOp(id: String) = feed.client.remove(DocumentId.of(EVENT_NAMESPACE, EVENT_DOCTYPE, id), feedParams())

    override suspend fun put(doc: EventDoc) {
        putOp(doc).await()
    }

    /** All puts stay in flight together — the feed client multiplexes them over HTTP/2. */
    override suspend fun putAll(docs: List<EventDoc>) {
        docs.map { putOp(it) }.forEach { it.await() }
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
        val result =
            feed.client
                .put(
                    DocumentId.of(EVENT_NAMESPACE, EVENT_DOCTYPE, address),
                    buildJsonObject { put("fields", doc.indexFields()) }.toString(),
                    feedParams().createIfNonExistent(true).testAndSetCondition(condition),
                ).await()
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
        ids.map { removeOp(it) }.forEach { it.await() }
    }

    /** Bulk remove with the docs in hand: the docid comes straight from each doc, so no address-keyed resolve-by-get per id. */
    override suspend fun removeDocs(docs: List<EventDoc>) {
        docs.map { feed.client.remove(DocumentId.of(EVENT_NAMESPACE, EVENT_DOCTYPE, docIdOf(it)), feedParams()) }.forEach { it.await() }
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
                searchRoot(vq, hits = Int.MAX_VALUE)
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
     * Recency-ordered recall with the id tiebreak applied CLIENT-SIDE. The
     * engine sorts by `created_at desc` alone — compound-sorting by the id
     * string attribute paid UCA collation over the whole match set (0.22s ->
     * 1.3s on a 2M-match scan) — and this restores the exact
     * `created_at desc, id asc` contract:
     *
     * With a single-key sort, every doc STRICTLY newer than the boundary
     * timestamp T (the limit-th hit's created_at) is guaranteed present; only
     * membership among the docs AT T is engine-arbitrary. So the query
     * overfetches [TIE_SLACK] extra hits — if anything older than T arrived,
     * or the engine ran out of matches, the whole T tie group is in hand and
     * an in-memory sort resolves the boundary exactly. Only a tie group wider
     * than the slack pays one extra `[T,T]` window query.
     *
     * Ranked queries (search terms, trust sorts) keep the engine's score order
     * untouched. The gated profiles are recency-ordered too (score IS
     * created_at), so they take the same path.
     */
    private suspend fun recallSummaries(q: EventQuery): List<VespaSummary> {
        val recencyOrdered =
            (q.ranking == null && q.search.isNullOrBlank() && q.phrases.isEmpty()) ||
                q.ranking == EventYql.RANK_UNRANKED ||
                q.ranking == EventYql.RANK_RECENCY ||
                q.usesGatedProfile()
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

    /** The rank band a hit arrived through, from the profile's match-features; null when none were served. */
    private fun tierOf(mf: JsonObject?): String? {
        if (mf == null) return null

        fun on(feature: String): Boolean = ((mf[feature] as? JsonPrimitive)?.content?.toDoubleOrNull() ?: 0.0) > 0.0
        return when {
            on("any_token_match") || on("name_match") -> "name"

            on("any_near_match") || on("near_name_match") -> "near"

            // BEFORE weak, and that ordering is the whole point. identity_match
            // means nip05/lud16 matched; weak_match is also 1 for such a doc
            // (identity is one of the weak band's signals), so testing weak
            // first would swallow the more specific route — which is exactly
            // how this branch stayed unreachable from the day it was written:
            // identity implied the old has_token_match, so "name" claimed it.
            // The doc scores in the weak band either way; this only names the
            // route that got it there.
            on("identity_match") -> "identity"

            on("weak_match") -> "weak"

            on("affiliation_match_text") || on("affiliation_match") -> "affiliation"

            // Same RUNG as affiliation (event.sd max()es the two into one
            // weight), a different route: the profile group's bio/website vs
            // the generic group's body/location. Ordered after it so a doc
            // that somehow fills both reports the profile-side label, as the
            // tiers above already do.
            on("tier_body_match") -> "body"

            // Matched a real column but NO rung claimed it — the floor band
            // (event.sd floored_text_score). Checked last on purpose:
            // real_match is 1 for every branch above too, so it only means
            // "unclaimed" once they have all failed. A hit landing here is a
            // ranking gap worth reporting, unlike "gram" — a doc no column
            // matched at all.
            on("real_match") -> "floor"

            else -> "gram"
        }
    }

    /**
     * The `hits` to ask Vespa for: the query's own limit, else everything.
     * Int.MAX_VALUE is how "no limit" is spelled — Vespa's `hits` defaults to
     * TEN when omitted, and the bundled query profile raises `maxHits` to let
     * the big value through.
     */
    private fun hitsFor(query: EventQuery): Int = query.limit ?: Int.MAX_VALUE

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
                // fan-out machinery (its allocations matter at thousands of id
                // lookups a second) and just get it.
                hexes.size == 1 -> listOfNotNull(get(hexes[0]))

                else -> hexes.mapBounded(ID_GET_FANOUT) { get(it) }.filterNotNull()
            }
        // NIP-40: never serve an event already expired at the query's cutoff —
        // the same guard EventYql emits as `expires_at > notExpiredAt`.
        val live = query.notExpiredAt?.let { cut -> docs.filter { (it.expiresAt() ?: EventDoc.NO_EXPIRATION) > cut } } ?: docs
        val ordered = live.sortedWith(NEWEST_FIRST)
        return query.limit?.let(ordered::take) ?: ordered
    }

    // ---- visits (full-corpus walks) -----------------------------------------

    /**
     * The document-API visit: a streaming scan with a selection expression —
     * exactly what a full-corpus id walk needs (it streams and does not rank).
     * Queries a selection can't express fall back to the search default, which
     * returns the same set in a single page. [onPage] is invoked serially;
     * returning false stops the walk. Cross-page order is arbitrary, which the
     * [EventIndex.visitIds] contract already grants.
     */
    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        val selection = EventSelection.build(query) ?: return super.visitIds(query, withDTag, onPage)
        // Vespa fieldSet syntax is "<doctype>:<field>,<field>,…" — the doctype
        // prefixes the list ONCE (else: ILLEGAL_PARAMETERS).
        val fieldSet = "$EVENT_DOCTYPE:created_at" + if (withDTag) ",tag_index" else ""
        visits.pages(selection, fieldSet) { documents ->
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
     * [distinctAuthors]'s grouping is complete too, but it materializes every
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

    override suspend fun count(query: EventQuery): Int =
        fallbacks.withNearFallback(query) { q ->
            val root = EventYql.buildCount(q)?.let { queryRoot(it, hits = 0) }
            root?.let { GroupingResults.firstCount(it) } ?: 0
        }

    override suspend fun countDistinctAuthors(query: EventQuery): Int =
        fallbacks.withNearFallback(query) { q ->
            // `all(group(pubkey) output(count()))` counts the GROUPS — the
            // distinct pubkeys — not the docs.
            val root = EventYql.buildDistinctCount(q, "pubkey")?.let { queryRoot(it, hits = 0) }
            root?.let { GroupingResults.firstCount(it) } ?: 0
        }

    override suspend fun countByKind(query: EventQuery): Map<Int, Int> =
        fallbacks.withNearFallback(query) { q ->
            // `all(group(kind) each(output(count())))` yields one leaf group
            // per kind, each carrying its `value` and a `count()`.
            val root = EventYql.buildKindHistogram(q)?.let { queryRoot(it, hits = 0) }
            val out = LinkedHashMap<Int, Int>()
            root?.let { GroupingResults.intCountsInto(it, out) }
            out
        }

    override suspend fun distinctAuthors(query: EventQuery): Set<String> =
        fallbacks.withNearFallback(query) { q ->
            val root = EventYql.buildDistinctAuthors(q)?.let { queryRoot(it, hits = 0) } ?: return@withNearFallback emptySet()
            GroupingResults.groupValues(root)
        }

    /**
     * The same grouping as [distinctAuthors], keeping each group's `count()` —
     * which the YQL already asks for and [distinctAuthors] drops. So the
     * author set WITH per-author doc counts costs exactly one query, the same one.
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
     * The two funnels every `/search/` response passes through — this one for
     * the recall paths (streamed straight into DTOs) and [queryRoot] for the
     * grouping/count paths (which need the tree) — both verify coverage, so no
     * caller can accidentally accept a degraded answer.
     */
    private suspend fun searchRoot(
        vq: VespaQuery,
        hits: Int,
    ): SearchRoot =
        VESPA_JSON
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
            ?.also { root -> root["coverage"]?.let { VESPA_JSON.decodeFromJsonElement(SearchCoverage.serializer(), it) }?.requireComplete() }

    /** One-line feed-client health for status lines; see [VespaFeed.statusLine]. */
    fun feedStatus(): String = feed.statusLine()

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    override fun close() = feed.close()

    private companion object {
        /** Concurrent document-API gets for a pure-id lookup. Gets are light (no summary stage to overrun), so this floats above QUERY_FANOUT. */
        const val ID_GET_FANOUT = 32

        /** Newest first (created_at desc, id asc tiebreak) — the same order the search path and the store apply. */
        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)

        /**
         * Extra hits a limit'd recency recall asks for beyond its limit, so
         * the boundary timestamp's tie group usually arrives complete and the
         * id tiebreak resolves in memory (see [recallSummaries]).
         */
        const val TIE_SLACK = 64

        /** Hits a [searchScored] call fetches when the query names no limit — an inspection surface, not a recall path. */
        const val DEFAULT_SCORED_HITS = 100

        /**
         * Per-operation feed deadline. A silently half-dead HTTP/2 connection
         * (e.g. severed by an engine restart) makes `await()` hang FOREVER,
         * which deadlocks the single-writer store behind it; a timeout turns
         * that hang into a retryable failure.
         */
        fun feedParams(): OperationParameters = OperationParameters.empty().timeout(Duration.ofSeconds(30))
    }
}
