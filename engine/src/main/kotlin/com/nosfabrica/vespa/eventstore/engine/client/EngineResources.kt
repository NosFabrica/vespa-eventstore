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

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.atomic.AtomicReference

/**
 * WHAT THE ENGINE ITSELF IS CARRYING — proton's memory and disk against the
 * limits it blocks feed at, per content node.
 *
 * The store's pages reported what the store was DOING and never what the
 * engine underneath had left. On 2026-09-07 that gap cost two content-node
 * OOM kills: the first during an app-package activation at 0.810, the second
 * on a routine restart at 0.808, and in both cases the numbers that would have
 * predicted it were only reachable by hand
 * (`kubectl exec … /metrics/v2/values`). A page that says "the walk is running"
 * beside "we are at 0.81 of the feed-block limit" is the whole story in one
 * glance; two pages that each hold half of it are how an afternoon disappears.
 *
 * NO NEW CONFIGURATION. `/metrics/v2/values` answers on the SAME endpoint the
 * store already queries, and reports every content node, so a single-node
 * client sees the whole cluster. Verified on staging: two nodes, memory and
 * disk each, from `:8080`.
 *
 * CACHED, and deliberately stale: this is read by a status page, and a page
 * refresh must not become a metrics fan-out. A read older than [TTL_MILLIS]
 * refreshes; anything newer is served as it stands, with [atMillis] saying how
 * old. A probe that fails leaves the last good answer in place rather than
 * blanking the panel — the moment this matters most is the moment the engine
 * is least able to answer.
 *
 * ONE REFRESH AT A TIME, and a BOUNDED one. The query client this rides
 * deliberately has no deadline on the wait for an answer ([VespaHttp]), so a
 * metrics endpoint that accepted the request and never answered held its
 * caller forever, and while the cache was stale every page render started its
 * own GET: renders piled up, each holding a slot of the client's shared
 * request budget. Now the first caller past the TTL starts the one refresh,
 * every caller meanwhile awaits that same one, and it gives up after
 * [deadlineMillis] (a failure like any other: the last good answer stands).
 */
class EngineResources internal constructor(
    internal val http: VespaHttp,
    internal val endpoint: () -> String,
    /** How long one refresh may take, retries included; see [DEADLINE_MILLIS]. */
    private val deadlineMillis: Long = DEADLINE_MILLIS,
) {
    /** One content node's headroom, as proton reports it: 0.0-1.0 of the limit that blocks feed. */
    class NodeUsage(
        val host: String,
        val memory: Double,
        val disk: Double,
        val feedBlocked: Boolean,
    )

    /** Every node's usage plus when it was read, so a page can say how stale it is. */
    class Usage(
        val nodes: List<NodeUsage>,
        val atMillis: Long,
    ) {
        /** The worst node, which is the one that decides whether the cluster feeds. */
        val peakMemory: Double get() = nodes.maxOfOrNull { it.memory } ?: 0.0

        val peakDisk: Double get() = nodes.maxOfOrNull { it.disk } ?: 0.0

        val anyFeedBlocked: Boolean get() = nodes.any { it.feedBlocked }
    }

    private val cached = AtomicReference<Usage?>(null)

    /** The refresh in flight, if any: what every caller past the TTL awaits. */
    private val refreshing = AtomicReference<Deferred<Usage?>?>(null)

    /**
     * Where a refresh runs: OWNED BY NO CALLER, so a caller cancelled while it
     * waits (a page render abandoned) cancels only its own wait, and the
     * others still get the answer. Nothing to close: a refresh ends within
     * [deadlineMillis] whatever the engine does.
     */
    private val refreshes = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("engine-resources"))

    /**
     * The last reading, refreshed when older than [TTL_MILLIS]. Null only
     * before the first success. Callers that find it stale share one refresh
     * (see the class KDoc); each may be cancelled alone.
     */
    suspend fun usage(nowMillis: Long = System.currentTimeMillis()): Usage? {
        cached.get()?.let { if (nowMillis - it.atMillis < TTL_MILLIS) return it }
        // Keep the last good answer on failure: see the class KDoc.
        return refresh(nowMillis).await() ?: cached.get()
    }

    /**
     * The refresh in flight, or a new one. The refresh catches every failure
     * of its own, its deadline included, and completes with null, so a waiter
     * only ever receives a reading or null: never another coroutine's
     * exception, nor a cancellation. Started LAZY so that a caller losing the
     * race to install it drops it without a request having been sent.
     */
    private fun refresh(nowMillis: Long): Deferred<Usage?> {
        while (true) {
            refreshing.get()?.let { return it }
            val mine =
                refreshes.async(start = CoroutineStart.LAZY) {
                    val fresh =
                        try {
                            // A missed deadline is null here; a failed read throws, and is null below.
                            withTimeoutOrNull(deadlineMillis) { read(nowMillis) }
                        } catch (e: CancellationException) {
                            throw e
                        } catch (_: Exception) {
                            null
                        }
                    if (fresh != null) cached.set(fresh)
                    fresh
                }
            if (refreshing.compareAndSet(null, mine)) {
                // Cleared when it completes, so the next stale read starts afresh.
                mine.invokeOnCompletion { refreshing.compareAndSet(mine, null) }
                mine.start()
                return mine
            }
            mine.cancel()
        }
    }

    private suspend fun read(nowMillis: Long): Usage {
        val resp = http.get("${endpoint()}$PATH")
        require(resp.statusCode() < 400) { "vespa metrics ${resp.statusCode()}" }
        return parse(resp.body(), nowMillis)
    }

    companion object {
        /**
         * PURE, and on the companion so it stays that way: it reads no instance
         * state, and a test that wants the parser should not have to build an
         * HTTP client to reach it. The old `parseForTest` did exactly that —
         * `EngineResources(VespaHttp(), { "" })` per call, from main source, so
         * every assertion about the panel's shape constructed two OkHttp
         * clients nothing ever closed.
         */
        internal fun parse(
            body: String,
            nowMillis: Long,
        ): Usage {
            val root = VESPA_JSON.parseToJsonElement(body).jsonObject
            val nodes =
                root["nodes"]?.jsonArray.orEmpty().mapNotNull { node ->
                    val o = node.jsonObject
                    val host = o["hostname"]?.jsonPrimitive?.contentOrNull ?: return@mapNotNull null
                    var memory = 0.0
                    var disk = 0.0
                    var blocked = false
                    for (service in o["services"]?.jsonArray.orEmpty()) {
                        for (metric in service.jsonObject["metrics"]?.jsonArray.orEmpty()) {
                            val values = metric.jsonObject["values"]?.jsonObject ?: continue
                            for ((name, raw) in values) {
                                val v = raw.jsonPrimitive.doubleOrNull ?: continue
                                when {
                                    MEMORY in name -> memory = maxOf(memory, v)
                                    DISK in name -> disk = maxOf(disk, v)
                                    FEED_BLOCK in name && v > 0.0 -> blocked = true
                                }
                            }
                        }
                    }
                    NodeUsage(host = host.substringBefore('.'), memory = memory, disk = disk, feedBlocked = blocked)
                }
            require(nodes.isNotEmpty()) { "vespa metrics carried no nodes" }
            return Usage(nodes, nowMillis)
        }

        /** The aggregate metrics endpoint — every node, on the query port the store already uses. */
        const val PATH = "/metrics/v2/values"

        /** How stale a reading may be before a caller triggers another. A status page, not an alarm. */
        const val TTL_MILLIS = 15_000L

        /**
         * One refresh's deadline: how long a status page waits on the engine
         * before showing the last good reading. A page's patience, chosen, not
         * measured: this endpoint's latency has not been measured here, and
         * should be well under it.
         */
        const val DEADLINE_MILLIS = 5_000L

        private const val MEMORY = "resource_usage.memory"
        private const val DISK = "resource_usage.disk"

        // The names Vespa actually serves, read off the cluster rather than
        // guessed — the first cut said "feed_block", which matches nothing.
        // These substrings are also narrow enough to miss the cluster
        // controller's `max_memory_utilization` / `max_disk_utilization`,
        // which are a different measurement on a different scale.
        private const val FEED_BLOCK = "resource_usage.feeding_blocked"
    }
}
