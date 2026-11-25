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
package com.nosfabrica.vespa.eventstore.benchmark.load

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAll
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.io.File
import java.util.concurrent.TimeUnit

/**
 * A REAL SLICE of the production relay, read-only, for the growth simulation
 * (`docs/growth-simulation.md`): what a store holds when real users are enrolled, so the
 * simulation can add more of them to real data instead of to a generated corpus.
 *
 * Pulled over plain NIP-01 from the relay (never from its Vespa, and never written to it):
 *  - `lists.jsonl`: every kind 10040 (trust-provider list) the relay holds;
 *  - `cards-<service>.jsonl`: the kind 30382 cards of the [SLICE_SERVICES] services those lists
 *    name most (each a personalized service key: one per enrolled user), at most
 *    `SLICE_CARDS` each. These are the TEMPLATES the simulation clones new users from;
 *  - `events.jsonl`: the newest `SLICE_EVENTS` events of the kinds a relay serves most (notes,
 *    reposts, reactions, profiles, follow lists).
 *
 * One event per line, as the relay sent it. Env: SLICE_RELAY (wss://search.brainstorm.world),
 * SLICE_DIR (build/relay-slice), SLICE_SERVICES (3), SLICE_CARDS (150000), SLICE_EVENTS (200000).
 */
object RelaySlice {
    /** The canonical lens's provider (AGENTS.md, "The live system"): always among the templates. */
    const val CANONICAL_PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"

    /**
     * The relay answers through a web of trust and refuses an anonymous REQ (`auth-required`); the
     * NIP-50 token `include:spam` asks for the corpus unranked and unfiltered, which a slice must be.
     */
    private const val UNRANKED = "include:spam"

    private class Enough : RuntimeException()

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking {
            val relay = (System.getenv("SLICE_RELAY") ?: "wss://search.brainstorm.world").normalizeRelayUrl()!!
            val dir = File(System.getenv("SLICE_DIR") ?: "build/relay-slice").apply { mkdirs() }
            val serviceCount = System.getenv("SLICE_SERVICES")?.toInt() ?: 3
            val cardCap = System.getenv("SLICE_CARDS")?.toInt() ?: 150_000
            val eventCap = System.getenv("SLICE_EVENTS")?.toInt() ?: 200_000

            val okHttp = OkHttpClient.Builder().readTimeout(120, TimeUnit.SECONDS).build()
            val client = NostrClient(BasicOkHttpWebSocket.Builder({ okHttp }, { true }))
            try {
                val lists = pull(client, relay, Filter(kinds = listOf(10040), search = UNRANKED), Int.MAX_VALUE, File(dir, "lists.jsonl"))
                val named =
                    lists
                        .flatMap { l -> l.tags.filter { it.size >= 2 && it[0] == "30382:rank" }.map { it[1].lowercase() } }
                        .groupingBy { it }
                        .eachCount()
                        .entries
                        .sortedByDescending { it.value }
                        .map { it.key }
                val services = (listOf(CANONICAL_PROVIDER) + named).distinct().take(serviceCount)
                println("lists: ${lists.size}, services named: ${named.size}, templates: $services")
                for (s in services) pull(client, relay, Filter(kinds = listOf(30382), authors = listOf(s), search = UNRANKED), cardCap, File(dir, "cards-$s.jsonl"))
                pullWindows(client, relay, listOf(1, 6, 7, 0, 3), eventCap, File(dir, "events.jsonl"))
            } finally {
                client.disconnect()
                okHttp.dispatcher.executorService.shutdown()
                okHttp.connectionPool.evictAll()
            }
        }

    /**
     * The newest [cap] events of [kinds], walked back in time WINDOWS: the relay does not answer an
     * unranked multi-kind search in `created_at` order, so `until` paging stops after a page
     * (quartz refuses to page an out-of-order answer, rightly). A window that comes back full (the
     * relay's 5,000 per REQ) is halved and asked again, so nothing inside it is skipped.
     */
    private suspend fun pullWindows(
        client: NostrClient,
        relay: com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl,
        kinds: List<Int>,
        cap: Int,
        out: File,
    ) {
        val seen = HashSet<String>()
        var until = System.currentTimeMillis() / 1000
        var window = 600L
        val t0 = System.nanoTime()
        out.bufferedWriter().use { w ->
            while (seen.size < cap && until > 1_600_000_000L) {
                val since = until - window
                val page = client.fetchAll(relay, Filter(kinds = kinds, since = since, until = until, limit = PAGE, search = UNRANKED))
                if (page.size >= PAGE && window > 1) {
                    window /= 2
                    continue
                }
                for (e in page) {
                    if (seen.add(e.id)) {
                        w.write(e.toJson())
                        w.newLine()
                    }
                }
                until = since - 1
                if (page.size < PAGE / 4) window = (window * 2).coerceAtMost(86_400L)
                if (seen.size / 50_000 != (seen.size - page.size) / 50_000) println("  ${out.name}: ${seen.size}")
            }
        }
        println("${out.name}: ${seen.size} events in %.0fs".format((System.nanoTime() - t0) / 1e9))
    }

    /** The relay's `max_limit` (NIP-11). */
    private const val PAGE = 5_000

    /** Page [filter] off [relay] into [out], one event per line, stopping at [cap]; returns what it wrote. */
    private suspend fun pull(
        client: NostrClient,
        relay: com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl,
        filter: Filter,
        cap: Int,
        out: File,
    ): List<Event> {
        val got = ArrayList<Event>()
        val seen = HashSet<String>()
        val t0 = System.nanoTime()
        out.bufferedWriter().use { w ->
            try {
                client.fetchAllPages(relay, listOf(filter)) { e ->
                    if (seen.add(e.id)) {
                        got += e
                        w.write(e.toJson())
                        w.newLine()
                        if (got.size % 50_000 == 0) println("  ${out.name}: ${got.size}")
                        if (got.size >= cap) throw Enough()
                    }
                }
            } catch (_: Enough) {
            }
        }
        println("${out.name}: ${got.size} events in %.0fs".format((System.nanoTime() - t0) / 1e9))
        return got
    }
}
