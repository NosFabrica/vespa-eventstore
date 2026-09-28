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
package com.nosfabrica.vespa.eventstore.benchmark.probe

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.client.RecencyStrategy
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * WHAT A CLIENT SEES, against what the engine does — read-only:
 *
 *  - `wire`: REQ -> EOSE over a relay's WebSocket, beside the engine client
 *    ([VespaEventIndex.rawSearch]) on the same filter at the same clock; the
 *    difference is everything outside the engine (the store's page assembly,
 *    serialization, the socket). Also checks the relay served the engine's page.
 *  - `store`: [NostrSemanticsStore.rawQuery] in-process — the call a relay
 *    serves a REQ with — against [VespaEventIndex.rawSearch] under it, both on
 *    the speculative strategy with memory, clocked at [BENCH_NOW] (a frozen
 *    corpus read as if it were live): the store's own overhead on THIS code.
 *    Built without `VespaEventStore.open()`, so no trust projection, no
 *    migration, no writes — ungated shapes only (the lens needs the projection).
 *
 * Env: BENCH_VESPA_URL, BENCH_RELAY_WS (ws://127.0.0.1:7777), BENCH_NOW,
 * BENCH_OBSERVER, BENCH_REPS (7), BENCH_SECTIONS (wire,store).
 */
object RelayE2EProbe {
    private class Shape(
        val name: String,
        val filter: Filter,
        val query: EventQuery,
    )

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking<Unit> {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val ws = System.getenv("BENCH_RELAY_WS") ?: "ws://127.0.0.1:7777"
            val reps = System.getenv("BENCH_REPS")?.toIntOrNull() ?: 7
            val observer = System.getenv("BENCH_OBSERVER")?.lowercase()
            val sections = (System.getenv("BENCH_SECTIONS") ?: "wire,store").split(",").map { it.trim() }.toSet()

            val shipped = VespaEventIndex(url, recencyStrategy = RecencyStrategy.MATCH_PHASE, recencyMemory = false)
            val wall = System.currentTimeMillis() / 1000
            val end =
                System.getenv("BENCH_NOW")?.toLongOrNull()
                    ?: shipped.search(EventQuery(kinds = listOf(1), until = wall, limit = 1, nowSecs = wall)).first().createdAt
            val byVolume =
                shipped
                    .countByAuthor(EventQuery(kinds = listOf(1)))
                    .entries
                    .sortedByDescending { it.value }
                    .map { it.key }
            val follow300 = byVolume.subList(200, 500)
            val profiles100 = byVolume.subList(1_000, 1_100)
            val mentioned = byVolume[3]
            val shapes =
                listOf(
                    Shape("global k1 lim50", Filter(kinds = listOf(1), limit = 50), EventQuery(kinds = listOf(1), limit = 50)),
                    Shape("global k1/6/7 lim100", Filter(kinds = listOf(1, 6, 7), limit = 100), EventQuery(kinds = listOf(1, 6, 7), limit = 100)),
                    Shape("follow300 lim500", Filter(kinds = listOf(1, 6, 7), authors = follow300, limit = 500), EventQuery(kinds = listOf(1, 6, 7), authors = follow300, limit = 500)),
                    Shape("#t:bitcoin lim50", Filter(kinds = listOf(1), tags = mapOf("t" to listOf("bitcoin")), limit = 50), EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf("bitcoin")), limit = 50)),
                    Shape("#p notif lim100", Filter(kinds = listOf(1, 6, 7, 9735), tags = mapOf("p" to listOf(mentioned)), limit = 100), EventQuery(kinds = listOf(1, 6, 7, 9735), tags = mapOf("p" to listOf(mentioned)), limit = 100)),
                    Shape("profiles 100 authors", Filter(kinds = listOf(0), authors = profiles100, limit = 100), EventQuery(kinds = listOf(0), authors = profiles100, limit = 100)),
                )

            fun median(xs: List<Long>) = xs.sorted()[xs.size / 2] / 1e6

            if ("wire" in sections) {
                // The relay reads at ITS clock — the wall clock — so the engine side does too.
                val speculative = VespaEventIndex(url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = true)
                val wire = Wire(ws)
                println("\n== wire: REQ -> EOSE over $ws, beside the engine client on the same filter (clock: wall, as the relay's)")
                println(String.format("%-24s %7s | %22s | %22s | %22s | %s", "shape", "events", "relay (ws) ms", "engine shipped ms", "engine spec+mem ms", "relay page = engine page"))
                val wireShapes =
                    shapes +
                        listOfNotNull(
                            observer?.let {
                                Shape(
                                    "GATED global k1 lim50",
                                    Filter(kinds = listOf(1), limit = 50, search = "observer:$it"),
                                    EventQuery(kinds = listOf(1), limit = 50, ranking = EventYql.RANK_RECENCY_GATED, observer = it, rankKey = it, minRank = 2.0),
                                )
                            },
                        )
                for (s in wireShapes) {
                    val q = s.query.copy(nowSecs = wall, notExpiredAt = wall)
                    val relayIds = wire.req(s.filter.toJson()).second
                    val engineIds = shipped.rawSearch(q).map { it.id }
                    repeat(2) {
                        wire.req(s.filter.toJson())
                        shipped.rawSearch(q)
                        speculative.rawSearch(q)
                    }
                    val w = (0 until reps).map { wire.req(s.filter.toJson()).first }
                    val e = (0 until reps).map { timed { shipped.rawSearch(q) } }
                    val d = (0 until reps).map { timed { speculative.rawSearch(q) } }
                    println(
                        String.format(
                            "%-24s %7d | %22.1f | %22.1f | %22.1f | %s",
                            s.name,
                            relayIds.size,
                            median(w),
                            median(e),
                            median(d),
                            if (relayIds == engineIds) "yes" else "NO (relay ${relayIds.size}, engine ${engineIds.size})",
                        ),
                    )
                }
                wire.close()
                speculative.close()
            }

            if ("store" in sections) {
                val index = VespaEventIndex(url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = true)
                val store = NostrSemanticsStore(index, nowSecs = { end })
                println("\n== store: NostrSemanticsStore.rawQuery in-process vs VespaEventIndex.rawSearch under it (speculative + memory, clock pinned at $end)")
                println(String.format("%-24s %7s | %14s | %14s | %14s | %s", "shape", "events", "store ms", "engine ms", "store overhead", "same page"))
                for (s in shapes) {
                    val q = s.query.copy(nowSecs = end, notExpiredAt = end)

                    suspend fun viaStore(): List<String> {
                        val out = ArrayList<String>()
                        store.rawQuery(listOf(s.filter)) { e: RawEvent -> out += e.id }
                        return out
                    }
                    val storeIds = viaStore()
                    val engineIds = index.rawSearch(q).map { it.id }
                    repeat(2) {
                        viaStore()
                        index.rawSearch(q)
                    }
                    val st = ArrayList<Long>()
                    val en = ArrayList<Long>()
                    repeat(reps) {
                        st += timed { viaStore() }
                        en += timed { index.rawSearch(q) }
                    }
                    println(
                        String.format(
                            "%-24s %7d | %14.1f | %14.1f | %13.1f  | %s",
                            s.name,
                            storeIds.size,
                            median(st),
                            median(en),
                            median(st) - median(en),
                            if (storeIds == engineIds) "yes" else "NO (store ${storeIds.size}, engine ${engineIds.size})",
                        ),
                    )
                }
                index.close()
            }
            shipped.close()
            kotlin.system.exitProcess(0)
        }

    private suspend inline fun timed(block: () -> Unit): Long {
        val t0 = System.nanoTime()
        block()
        return System.nanoTime() - t0
    }

    /** A Nostr filter as NIP-01 JSON — the handful of fields these shapes use. */
    private fun Filter.toJson(): String =
        buildString {
            append('{')
            val parts = ArrayList<String>()
            kinds?.let { parts += "\"kinds\":${it.joinToString(",", "[", "]")}" }
            authors?.let { parts += "\"authors\":${it.joinToString(",", "[", "]") { a -> "\"$a\"" }}" }
            tags?.forEach { (k, v) -> parts += "\"#$k\":${v.joinToString(",", "[", "]") { x -> "\"$x\"" }}" }
            limit?.let { parts += "\"limit\":$it" }
            search?.let { parts += "\"search\":\"$it\"" }
            append(parts.joinToString(","))
            append('}')
        }

    /** One WebSocket to a relay, one REQ at a time: REQ -> the ids it streamed -> EOSE. */
    private class Wire(
        url: String,
    ) {
        private val http = OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build()
        private val pending = ConcurrentHashMap<String, Pair<MutableList<String>, CompletableDeferred<Unit>>>()
        private val opened = CompletableDeferred<Unit>()
        private var seq = 0
        private val socket: WebSocket =
            http.newWebSocket(
                Request.Builder().url(url).build(),
                object : WebSocketListener() {
                    override fun onOpen(
                        webSocket: WebSocket,
                        response: Response,
                    ) {
                        opened.complete(Unit)
                    }

                    override fun onMessage(
                        webSocket: WebSocket,
                        text: String,
                    ) {
                        val msg = Json.parseToJsonElement(text).jsonArray
                        val type = msg[0].jsonPrimitive.content
                        val sub = msg.getOrNull(1)?.jsonPrimitive?.content ?: return
                        val slot = pending[sub] ?: return
                        when (type) {
                            "EVENT" -> slot.first += (msg[2].jsonObject["id"]!!.jsonPrimitive.content)
                            "EOSE" -> slot.second.complete(Unit)
                            "CLOSED" -> slot.second.completeExceptionally(IllegalStateException("CLOSED: $text"))
                        }
                    }

                    override fun onFailure(
                        webSocket: WebSocket,
                        t: Throwable,
                        response: Response?,
                    ) {
                        opened.completeExceptionally(t)
                        pending.values.forEach { it.second.completeExceptionally(t) }
                    }
                },
            )

        /** (nanos to EOSE, ids in the order streamed). */
        suspend fun req(filterJson: String): Pair<Long, List<String>> {
            withTimeout(30_000) { opened.await() }
            val sub = "p${seq++}"
            val slot = ArrayList<String>() to CompletableDeferred<Unit>()
            pending[sub] = slot
            val t0 = System.nanoTime()
            socket.send("[\"REQ\",\"$sub\",$filterJson]")
            withTimeout(120_000) { slot.second.await() }
            val took = System.nanoTime() - t0
            socket.send("[\"CLOSE\",\"$sub\"]")
            pending.remove(sub)
            return took to slot.first.toList()
        }

        fun close() {
            socket.close(1000, null)
            http.dispatcher.executorService.shutdown()
        }
    }
}
