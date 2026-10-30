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
package com.nosfabrica.vespa.eventstore

import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.eventstore.engine.client.ClientTuning
import com.nosfabrica.vespa.eventstore.engine.client.FeedCompression
import com.nosfabrica.vespa.eventstore.engine.client.RecencyStrategy
import com.nosfabrica.vespa.eventstore.engine.tuning.ProcessKnobs
import com.nosfabrica.vespa.eventstore.ingest.GuardOwners
import com.nosfabrica.vespa.eventstore.runtime.StoreKnobs
import com.nosfabrica.vespa.eventstore.runtime.DEFAULT_DEDUP_CHUNK as STORE_DEFAULT_DEDUP_CHUNK

/**
 * EVERY OPERATOR SETTING THIS LIBRARY READS, as one typed value for
 * [VespaEventStore.open] — and [fromEnv], the strict way to read them from an
 * environment.
 *
 * WHY. The store read ~25 `VESPA_*` variables itself, in the class that used
 * each one, and LENIENTLY: `VESPA_FEED_BATCH=off` turned batching off without
 * a word, `VESPA_DEDUP_CHUNK=5OO` kept 500. A setting read as its default is a
 * setting nobody can see is not applied, so vespa-relay re-parsed the ones it
 * cared about strictly before opening the store, with its own copies of the
 * ranges (`MAX_FEED_BATCH`, `DEFAULT_DEDUP_CHUNK`). [fromEnv] is that strict
 * reading, here, once, with the ranges this code actually has — and the
 * constants below are the ones a consumer used to mirror.
 *
 * NULL MEANS "NOT SET HERE". A field left null is read the way `open()` always
 * read it — the environment variable, leniently, else this build's default —
 * so `EngineTuning()` is exactly the old behaviour, and [fromEnv] of a map
 * that lacks a variable leaves that one to the PROCESS environment. Pass
 * `System.getenv()` (what a deployment means by "the environment") and the
 * two agree.
 *
 * THREE SCOPES, which the field groups below follow:
 *
 *  - PER STORE: the engine client's feed, visit and recency settings and the
 *    guard-owner switch — applied to the store this tuning opens.
 *  - PER PROCESS: fan-out widths, the match-phase depth, the dedup chunk and
 *    the trust walk's page and slice. The code reads these as constants from
 *    places no store instance reaches, so `open()` PINS them for the process
 *    ([com.nosfabrica.vespa.eventstore.engine.tuning.ProcessKnob]); a second
 *    `open()` asking for a different value fails rather than silently
 *    re-tuning the first store.
 *  - PER DEPLOY: [accessLog], which rewrites the application package `open()`
 *    deploys.
 *
 * Each field is documented with its variable; the measured reason for each
 * default is at the code that reads it (named in the field's KDoc).
 */
class EngineTuning(
    // ---- per store: the engine client --------------------------------------
    /** `VESPA_FEED_BATCH` — documents per batch-handler request, 0..[MAX_FEED_BATCH]; 0 or 1 writes one per request. Default [DEFAULT_FEED_BATCH] (`VespaEventIndex`, `VespaBatchFeed`). */
    val feedBatch: Int? = null,
    /** `VESPA_FEED_CONNECTIONS` — feed connections PER ENDPOINT, at least 1. Default: 32 split across the feed endpoints (`VespaFeed`). */
    val feedConnections: Int? = null,
    /** `VESPA_FEED_COMPRESSION` — `auto`, `gzip` or `none`. Default `auto` (`VespaFeed`). */
    val feedCompression: FeedCompression? = null,
    /** `VESPA_FEED_STREAMS` — HTTP/2 streams per feed connection, at least 1. Default 128 (`VespaFeed`). */
    val feedStreams: Int? = null,
    /** `VESPA_FEED_INFLIGHT_FACTOR` — the feed throttler's initial in-flight factor, at least 1. Default: the client's (`VespaFeed`). */
    val feedInflightFactor: Int? = null,
    /** `VESPA_VISIT_SLICES` — concurrent streamed slices of a full-corpus visit, at least 1. Default 2 x cores, 4..32 (`VespaVisits`). */
    val visitSlices: Int? = null,
    /** `VESPA_VISIT_CONCURRENCY` — bucket parallelism inside one paged visit, 1..100. Default 8 (`VespaVisits`). */
    val visitConcurrency: Int? = null,
    /** `VESPA_VISIT_STREAM` — stream visits as JSON-Lines; false forces the paged fallback. Default on (`VespaVisits`). */
    val visitStreaming: Boolean? = null,
    /** `VESPA_QUERY_PLANNER` — window-plan bare recency scans by count probes. Default on (`RecencyPlanner`). */
    val queryPlanning: Boolean? = null,
    /** `VESPA_RECENCY_STRATEGY` — how limit'd newest-first reads are bounded. Default `speculative` (`RecencyStrategy`). */
    val recencyStrategy: RecencyStrategy? = null,
    /** `VESPA_ID_FIRST` — resolve recency pages off the attribute-only summary first. Default on (`VespaEventIndex.recallSummaries`). */
    val idFirst: Boolean? = null,
    /** `VESPA_RECENCY_MEMORY` — remember per read shape where its page was found. Default on (`RecencyWindowMemory`). */
    val recencyMemory: Boolean? = null,
    /** `VESPA_BODY_EDGE` — PROTOTYPE (#161): feed the body's edge n-grams. Default off. */
    val bodyEdgeFeed: Boolean? = null,
    /** `VESPA_ADDRESS_KEYED` — store replaceables under their NIP-01 address. Default off (`VespaEventIndex.putIfNewer`). */
    val addressKeyed: Boolean? = null,
    /** `VESPA_UNBOUNDED_HITS` — the hit ceiling for a query with no limit, at least 1; set it to a deployed `maxHits` cap. Default none. */
    val unboundedHits: Int? = null,
    // ---- per store: policy --------------------------------------------------
    /** `GUARD_OWNERS_DISABLE` — true forces every insert to probe NIP-09/NIP-62 whatever `writers` says. Default off (`GuardOwners`). */
    val guardOwnersDisabled: Boolean? = null,
    // ---- per process --------------------------------------------------------
    /** `VESPA_QUERY_FANOUT` — engine queries a batch stage keeps in flight, at least 1. Default [DEFAULT_QUERY_FANOUT] (`engine.async.QUERY_FANOUT`). */
    val queryFanout: Int? = null,
    /** `VESPA_PUT_FANOUT` — concurrent conditional puts on the address-keyed path, at least 1. Default 32 (`engine.async.PUT_FANOUT`). */
    val putFanout: Int? = null,
    /** `VESPA_MATCH_PHASE_MAX_HITS` — must MIRROR the serving schema's recency match-phase `max-hits`, at least 1. Default 20000 (`EventYql`). */
    val matchPhaseMaxHits: Int? = null,
    /** `VESPA_DEDUP_CHUNK` — ids per dedup probe query, at least 1. Default [DEFAULT_DEDUP_CHUNK] (`BulkRecordInsert`). */
    val dedupChunk: Int? = null,
    /** `VESPA_TRUST_WALK_PAGE` — cards per gate hold in a trust service walk, at least 1. Default 250 (`TrustRecompute`). */
    val trustWalkPage: Int? = null,
    /** `VESPA_TRUST_GATE_SLICE` — subjects per write-gate hold in a recompute, at least 1. Default 500 (`TrustRecompute`). */
    val trustGateSlice: Int? = null,
    // ---- per deploy ---------------------------------------------------------
    /** `VESPA_ACCESS_LOG` — `default` or `disabled`: the container's HTTP access log in the package `open()` deploys (`VespaApp`). */
    val accessLog: String? = null,
) {
    init {
        feedBatch?.let { require(it in 0..MAX_FEED_BATCH) { "feedBatch $it is outside 0..$MAX_FEED_BATCH" } }
        accessLog?.let { require(it.trim().lowercase() in VespaApp.ACCESS_LOG_VALUES) { "accessLog '$it' is not one of ${VespaApp.ACCESS_LOG_VALUES.sorted()}" } }
        for ((name, v) in positives()) v?.let { require(it >= 1) { "$name $it is below 1" } }
        visitConcurrency?.let { require(it in 1..100) { "visitConcurrency $it is outside 1..100" } }
    }

    private fun positives() =
        listOf(
            "feedConnections" to feedConnections,
            "feedStreams" to feedStreams,
            "feedInflightFactor" to feedInflightFactor,
            "visitSlices" to visitSlices,
            "unboundedHits" to unboundedHits,
            "queryFanout" to queryFanout,
            "putFanout" to putFanout,
            "matchPhaseMaxHits" to matchPhaseMaxHits,
            "dedupChunk" to dedupChunk,
            "trustWalkPage" to trustWalkPage,
            "trustGateSlice" to trustGateSlice,
        )

    /** The engine client's share, for `VespaEventIndex` and `VespaReputationIndex`. */
    internal fun client(): ClientTuning =
        ClientTuning(
            feedBatch = feedBatch,
            feedConnections = feedConnections,
            feedCompression = feedCompression,
            feedStreams = feedStreams,
            feedInflightFactor = feedInflightFactor,
            visitSlices = visitSlices,
            visitConcurrency = visitConcurrency,
            visitStreaming = visitStreaming,
            queryPlanning = queryPlanning,
            recencyStrategy = recencyStrategy,
            idFirst = idFirst,
            recencyMemory = recencyMemory,
            bodyEdgeFeed = bodyEdgeFeed,
            addressKeyed = addressKeyed,
            unboundedHits = unboundedHits,
        )

    /**
     * Fix the per-process settings this tuning names, BEFORE anything reads
     * them. Checked as a whole first, so a conflict leaves nothing half-pinned.
     */
    internal fun pinProcessKnobs() {
        val pins =
            listOfNotNull(
                queryFanout?.let { { ProcessKnobs.queryFanout.pin(it) } },
                putFanout?.let { { ProcessKnobs.putFanout.pin(it) } },
                matchPhaseMaxHits?.let { { ProcessKnobs.matchPhaseMaxHits.pin(it) } },
                dedupChunk?.let { { StoreKnobs.dedupChunk.pin(it) } },
                trustWalkPage?.let { { StoreKnobs.trustWalkPage.pin(it) } },
                trustGateSlice?.let { { StoreKnobs.trustGateSlice.pin(it) } },
            )
        synchronized(PIN_LOCK) { pins.forEach { it() } }
    }

    /** The access-log choice for the deployed package: this tuning's, else `VESPA_ACCESS_LOG` as before. */
    internal fun accessLogOrEnv(): String? = accessLog ?: System.getenv(VespaApp.ACCESS_LOG_ENV)

    /** The settings this tuning names, for a boot line; empty when it names none. */
    override fun toString(): String =
        listOf(
            "feedBatch" to feedBatch,
            "feedConnections" to feedConnections,
            "feedCompression" to feedCompression,
            "feedStreams" to feedStreams,
            "feedInflightFactor" to feedInflightFactor,
            "visitSlices" to visitSlices,
            "visitConcurrency" to visitConcurrency,
            "visitStreaming" to visitStreaming,
            "queryPlanning" to queryPlanning,
            "recencyStrategy" to recencyStrategy,
            "idFirst" to idFirst,
            "recencyMemory" to recencyMemory,
            "bodyEdgeFeed" to bodyEdgeFeed,
            "addressKeyed" to addressKeyed,
            "unboundedHits" to unboundedHits,
            "guardOwnersDisabled" to guardOwnersDisabled,
            "queryFanout" to queryFanout,
            "putFanout" to putFanout,
            "matchPhaseMaxHits" to matchPhaseMaxHits,
            "dedupChunk" to dedupChunk,
            "trustWalkPage" to trustWalkPage,
            "trustGateSlice" to trustGateSlice,
            "accessLog" to accessLog,
        ).filter { it.second != null }
            .joinToString(", ", prefix = "EngineTuning(", postfix = ")") { (k, v) -> "$k=$v" }

    companion object {
        /** `VESPA_FEED_BATCH`'s default: past 100 a request the gain flattens (benchmark/README.md). */
        const val DEFAULT_FEED_BATCH = ClientTuning.DEFAULT_FEED_BATCH

        /** The largest useful `VESPA_FEED_BATCH`: the client's feed chunk, so a larger batch would only be split again. */
        const val MAX_FEED_BATCH = ClientTuning.MAX_FEED_BATCH

        /** `VESPA_DEDUP_CHUNK`'s default: the width the REQ-latency A/B was measured at. */
        const val DEFAULT_DEDUP_CHUNK = STORE_DEFAULT_DEDUP_CHUNK

        /** `VESPA_QUERY_FANOUT`'s default: past it proton's summary stage times out on bulk-stage queries. */
        const val DEFAULT_QUERY_FANOUT = 4

        /** Every variable [fromEnv] reads, so a deployment's docs can list exactly these. */
        val VARIABLES: List<String> =
            listOf(
                ClientTuning.FEED_BATCH,
                ClientTuning.FEED_CONNECTIONS,
                ClientTuning.FEED_COMPRESSION,
                ClientTuning.FEED_STREAMS,
                ClientTuning.FEED_INFLIGHT_FACTOR,
                ClientTuning.VISIT_SLICES,
                ClientTuning.VISIT_CONCURRENCY,
                ClientTuning.VISIT_STREAM,
                ClientTuning.QUERY_PLANNER,
                ClientTuning.RECENCY_STRATEGY,
                ClientTuning.ID_FIRST,
                ClientTuning.RECENCY_MEMORY,
                ClientTuning.BODY_EDGE,
                ClientTuning.ADDRESS_KEYED,
                ClientTuning.UNBOUNDED_HITS,
                GuardOwners.ENV_DISABLE,
                ProcessKnobs.queryFanout.env,
                ProcessKnobs.putFanout.env,
                ProcessKnobs.matchPhaseMaxHits.env,
                StoreKnobs.dedupChunk.env,
                StoreKnobs.trustWalkPage.env,
                StoreKnobs.trustGateSlice.env,
                VespaApp.ACCESS_LOG_ENV,
            )

        private val PIN_LOCK = Any()

        /**
         * [env] read STRICTLY: unset or blank is null (the default applies), and
         * anything else parses inside its range or is refused — every bad
         * variable at once, in one [IllegalArgumentException] naming each, so a
         * deployment fixes its environment in one boot rather than one per typo.
         * Switches take `true`/`1`/`yes`/`on` and `false`/`0`/`no`/`off`.
         */
        fun fromEnv(env: Map<String, String>): EngineTuning {
            val problems = ArrayList<String>()

            fun setting(key: String): String? = env[key]?.trim()?.takeIf { it.isNotEmpty() }

            fun int(
                key: String,
                range: IntRange,
            ): Int? =
                setting(key)?.let { raw ->
                    raw.toIntOrNull()?.takeIf { it in range } ?: null.also {
                        problems += "$key='$raw' is not a whole number " + (if (range.last == Int.MAX_VALUE) "of at least ${range.first}" else "in ${range.first}..${range.last}")
                    }
                }

            fun flag(key: String): Boolean? =
                setting(key)?.let { raw ->
                    when (raw.lowercase()) {
                        "true", "1", "yes", "on" -> true
                        "false", "0", "no", "off" -> false
                        else -> null.also { problems += "$key='$raw' is not a switch; use true or false" }
                    }
                }

            fun <T> choice(
                key: String,
                parse: (String) -> T?,
                choices: String,
            ): T? = setting(key)?.let { raw -> parse(raw) ?: null.also { problems += "$key='$raw' is not one of $choices" } }

            val atLeastOne = 1..Int.MAX_VALUE
            val tuning =
                EngineTuning(
                    feedBatch = int(ClientTuning.FEED_BATCH, 0..MAX_FEED_BATCH),
                    feedConnections = int(ClientTuning.FEED_CONNECTIONS, atLeastOne),
                    feedCompression =
                        choice(ClientTuning.FEED_COMPRESSION, { v -> FeedCompression.entries.firstOrNull { it.name.equals(v, ignoreCase = true) } }, "auto, gzip, none"),
                    feedStreams = int(ClientTuning.FEED_STREAMS, atLeastOne),
                    feedInflightFactor = int(ClientTuning.FEED_INFLIGHT_FACTOR, atLeastOne),
                    visitSlices = int(ClientTuning.VISIT_SLICES, atLeastOne),
                    visitConcurrency = int(ClientTuning.VISIT_CONCURRENCY, 1..100),
                    visitStreaming = flag(ClientTuning.VISIT_STREAM),
                    queryPlanning = flag(ClientTuning.QUERY_PLANNER),
                    recencyStrategy =
                        choice(
                            ClientTuning.RECENCY_STRATEGY,
                            { v -> RecencyStrategy.entries.firstOrNull { it.name.equals(v, ignoreCase = true) } },
                            RecencyStrategy.entries.joinToString { it.name.lowercase() },
                        ),
                    idFirst = flag(ClientTuning.ID_FIRST),
                    recencyMemory = flag(ClientTuning.RECENCY_MEMORY),
                    bodyEdgeFeed = flag(ClientTuning.BODY_EDGE),
                    addressKeyed = flag(ClientTuning.ADDRESS_KEYED),
                    unboundedHits = int(ClientTuning.UNBOUNDED_HITS, atLeastOne),
                    guardOwnersDisabled = flag(GuardOwners.ENV_DISABLE),
                    queryFanout = int(ProcessKnobs.queryFanout.env, atLeastOne),
                    putFanout = int(ProcessKnobs.putFanout.env, atLeastOne),
                    matchPhaseMaxHits = int(ProcessKnobs.matchPhaseMaxHits.env, atLeastOne),
                    dedupChunk = int(StoreKnobs.dedupChunk.env, atLeastOne),
                    trustWalkPage = int(StoreKnobs.trustWalkPage.env, atLeastOne),
                    trustGateSlice = int(StoreKnobs.trustGateSlice.env, atLeastOne),
                    accessLog =
                        choice(VespaApp.ACCESS_LOG_ENV, { v -> v.lowercase().takeIf { it in VespaApp.ACCESS_LOG_VALUES } }, VespaApp.ACCESS_LOG_VALUES.sorted().joinToString()),
                )
            require(problems.isEmpty()) { "the store's environment has ${problems.size} bad setting(s): ${problems.joinToString("; ")}" }
            return tuning
        }
    }
}
