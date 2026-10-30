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

import ai.vespa.feed.client.FeedClientBuilder

/**
 * The Vespa client's PER-INSTANCE settings, as values rather than environment
 * reads: what [VespaEventIndex] and [VespaReputationIndex] are built with.
 *
 * EVERY FIELD NULL MEANS "NOT SET HERE": the client then reads its environment
 * variable exactly as it always has — leniently, and falling back to this
 * build's default — so `ClientTuning()` is the old behaviour and a caller who
 * sets one field changes one thing. The STRICT reading of the same variables,
 * which refuses a value it cannot parse instead of quietly ignoring it, is the
 * store's `EngineTuning.fromEnv`; this type only carries what it parsed.
 *
 * Each field says what it tunes; the measured reasons live at the parameter of
 * [VespaEventIndex] (or of the feed client) that reads it.
 */
class ClientTuning(
    /** `VESPA_FEED_BATCH`: documents per batch-handler request; 0 or 1 writes one per request. 0..[VespaEventIndex.FEED_CHUNK]. */
    val feedBatch: Int? = null,
    /** `VESPA_FEED_CONNECTIONS`: feed connections PER ENDPOINT, not divided across endpoints. At least 1. */
    val feedConnections: Int? = null,
    /** `VESPA_FEED_COMPRESSION`: how the feed client sends documents. */
    val feedCompression: FeedCompression? = null,
    /** `VESPA_FEED_STREAMS`: HTTP/2 streams per feed connection. At least 1. */
    val feedStreams: Int? = null,
    /** `VESPA_FEED_INFLIGHT_FACTOR`: the feed throttler's initial in-flight factor. At least 1. */
    val feedInflightFactor: Int? = null,
    /** `VESPA_VISIT_SLICES`: concurrent streamed slices of a full-corpus visit. At least 1. */
    val visitSlices: Int? = null,
    /** `VESPA_VISIT_CONCURRENCY`: bucket parallelism inside one paged visit request. 1..100. */
    val visitConcurrency: Int? = null,
    /** `VESPA_VISIT_STREAM`: stream visit slices as JSON-Lines (false: the paged fallback). */
    val visitStreaming: Boolean? = null,
    /** `VESPA_QUERY_PLANNER`: window-plan bare recency scans with count probes. */
    val queryPlanning: Boolean? = null,
    /** `VESPA_RECENCY_STRATEGY`: how limit'd newest-first reads are bounded. */
    val recencyStrategy: RecencyStrategy? = null,
    /** `VESPA_ID_FIRST`: resolve recency pages off the `idtime` summary first. */
    val idFirst: Boolean? = null,
    /** `VESPA_RECENCY_MEMORY`: remember per read shape where the last page was found. */
    val recencyMemory: Boolean? = null,
    /** `VESPA_BODY_EDGE`: PROTOTYPE, feed the body's edge n-grams. */
    val bodyEdgeFeed: Boolean? = null,
    /** `VESPA_ADDRESS_KEYED`: store replaceables under their NIP-01 address (engine-atomic supersession). */
    val addressKeyed: Boolean? = null,
    /** `VESPA_UNBOUNDED_HITS`: the hit ceiling sent for a query with no limit. At least 1. */
    val unboundedHits: Int? = null,
) {
    // ---- resolution: the field, else the historical lenient read ------------
    //
    // Each of these is the expression the client's constructor used to evaluate
    // inline, moved here verbatim so a client built without a tuning behaves
    // byte for byte as before.

    internal fun feedBatchOrEnv(): Int = feedBatch ?: VespaEventIndex.feedBatchOf(env(FEED_BATCH))

    internal fun feedConnectionsOrEnv(): Int? = feedConnections ?: env(FEED_CONNECTIONS)?.toIntOrNull()

    /** Strict even from the environment, as it always was: a misspelt compression fails the client. */
    internal fun feedCompressionOrEnv(): FeedClientBuilder.Compression = (feedCompression ?: env(FEED_COMPRESSION)?.trim()?.let(::compressionOf) ?: FeedCompression.AUTO).client

    internal fun feedStreamsOrEnv(): Int = feedStreams ?: env(FEED_STREAMS)?.toIntOrNull() ?: DEFAULT_FEED_STREAMS

    internal fun feedInflightFactorOrEnv(): Int? = feedInflightFactor ?: env(FEED_INFLIGHT_FACTOR)?.toIntOrNull()

    internal fun visitSlicesOrEnv(): Int =
        (
            visitSlices ?: env(VISIT_SLICES)?.toIntOrNull()
                ?: (2 * Runtime.getRuntime().availableProcessors()).coerceIn(VespaVisits.SLICES_MIN, VespaVisits.SLICES_MAX)
        ).coerceAtLeast(1)

    internal fun visitConcurrencyOrEnv(): Int = (visitConcurrency ?: env(VISIT_CONCURRENCY)?.toIntOrNull() ?: VespaVisits.DEFAULT_BUCKET_CONCURRENCY).coerceIn(1, 100)

    internal fun visitStreamingOrEnv(): Boolean = visitStreaming ?: envOn(VISIT_STREAM) ?: true

    internal fun queryPlanningOrEnv(): Boolean = queryPlanning ?: envOn(QUERY_PLANNER) ?: true

    internal fun recencyStrategyOrEnv(): RecencyStrategy = recencyStrategy ?: RecencyStrategy.fromEnv()

    internal fun idFirstOrEnv(): Boolean = idFirst ?: envOn(ID_FIRST) ?: true

    internal fun recencyMemoryOrEnv(): Boolean = recencyMemory ?: envOn(RECENCY_MEMORY) ?: true

    /** Opt-IN, so only an explicit yes turns it on — the old parse, kept. */
    internal fun bodyEdgeFeedOrEnv(): Boolean = bodyEdgeFeed ?: env(BODY_EDGE)?.let { it == "1" || it.equals("true", ignoreCase = true) } ?: false

    /** Opt-IN, like [bodyEdgeFeedOrEnv]. */
    internal fun addressKeyedOrEnv(): Boolean = addressKeyed ?: env(ADDRESS_KEYED).let { it == "1" || it?.toBooleanStrictOrNull() == true }

    internal fun unboundedHitsOrEnv(): Int = unboundedHits ?: env(UNBOUNDED_HITS)?.toIntOrNull() ?: Int.MAX_VALUE

    companion object {
        const val FEED_BATCH = "VESPA_FEED_BATCH"
        const val FEED_CONNECTIONS = "VESPA_FEED_CONNECTIONS"
        const val FEED_COMPRESSION = "VESPA_FEED_COMPRESSION"
        const val FEED_STREAMS = "VESPA_FEED_STREAMS"
        const val FEED_INFLIGHT_FACTOR = "VESPA_FEED_INFLIGHT_FACTOR"
        const val VISIT_SLICES = "VESPA_VISIT_SLICES"
        const val VISIT_CONCURRENCY = "VESPA_VISIT_CONCURRENCY"
        const val VISIT_STREAM = "VESPA_VISIT_STREAM"
        const val QUERY_PLANNER = "VESPA_QUERY_PLANNER"
        const val RECENCY_STRATEGY = "VESPA_RECENCY_STRATEGY"
        const val ID_FIRST = "VESPA_ID_FIRST"
        const val RECENCY_MEMORY = "VESPA_RECENCY_MEMORY"
        const val BODY_EDGE = "VESPA_BODY_EDGE"
        const val ADDRESS_KEYED = "VESPA_ADDRESS_KEYED"
        const val UNBOUNDED_HITS = "VESPA_UNBOUNDED_HITS"

        /**
         * `VESPA_FEED_BATCH`'s default (the client's `DEFAULT_FEED_BATCH`): past
         * 100 a request the gain flattens (benchmark/README.md).
         */
        const val DEFAULT_FEED_BATCH = 100

        /**
         * The client's feed chunk (`FEED_CHUNK`): the most documents one
         * `putAll` enqueues at once, and so the largest batch that is not split
         * again — the ceiling of a useful `VESPA_FEED_BATCH`.
         */
        const val MAX_FEED_BATCH = 4_096

        /** HTTP/2 streams per feed connection when nothing says otherwise — the measured small-host value (VespaFeed). */
        const val DEFAULT_FEED_STREAMS = 128

        /** `auto`, `gzip` or `none`, in any case; anything else is refused with the variable's name. */
        fun compressionOf(value: String): FeedCompression =
            FeedCompression.entries.firstOrNull { it.name.equals(value.trim(), ignoreCase = true) }
                ?: throw IllegalArgumentException("$FEED_COMPRESSION must be auto, gzip or none, was '$value'")

        private fun env(name: String): String? = System.getenv(name)

        /** The old switch parse for a default-ON knob: anything but `0`/`false` is on. */
        private fun envOn(name: String): Boolean? = env(name)?.let { it != "0" && !it.equals("false", ignoreCase = true) }
    }
}

/**
 * `VESPA_FEED_COMPRESSION`'s three values, as this library's own type: the feed
 * client is an `implementation` dependency, so its enum cannot appear in a
 * signature a consumer compiles against.
 */
enum class FeedCompression(
    internal val client: FeedClientBuilder.Compression,
) {
    /** The client's default: gzip any body over 512 bytes. */
    AUTO(FeedClientBuilder.Compression.auto),

    /** Every body gzipped — tag-heavy documents across a WAN. */
    GZIP(FeedClientBuilder.Compression.gzip),

    /** Nothing gzipped — a Vespa on the same host, where inflating costs more than the bytes. */
    NONE(FeedClientBuilder.Compression.none),
}
