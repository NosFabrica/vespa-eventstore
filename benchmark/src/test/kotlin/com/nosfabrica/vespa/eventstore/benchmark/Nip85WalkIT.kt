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
package com.nosfabrica.vespa.eventstore.benchmark

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.random.Random

/**
 * Times walking one provider's NIP-85 cards (kind 30382) for `(d, rank)` on a
 * real Vespa: NQL paged by `events.d`, one unpaged NQL query, and REQ pages of
 * whole events. Measures only; off unless `NIP85_VESPA=<cards>`.
 */
@Tag("integration")
class Nip85WalkIT {
    private inline fun ms(block: () -> Unit): Long {
        val s = System.nanoTime()
        block()
        return (System.nanoTime() - s) / 1_000_000
    }

    @Test
    fun `walk one provider's rank assertions`() {
        val n = System.getenv("NIP85_VESPA")?.toIntOrNull()
        assumeTrue(n != null, "set NIP85_VESPA=<cards> to run")
        assumeTrue(runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false), "Docker not available")
        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                VespaEventStore.open(url = queryUrl, autoDeploy = true, configUrl = configUrl).use { store ->
                    val service = NostrSignerSync()
                    val other = NostrSignerSync()
                    val rnd = Random(3)
                    val cards = ArrayList<Event>()
                    repeat(n!!) { i ->
                        val pk = Random(i).nextBytes(32).joinToString("") { b -> "%02x".format(b) }

                        fun card(s: NostrSignerSync) =
                            s.sign<Event>(
                                1_700_000_000L + i,
                                30382,
                                arrayOf(arrayOf("d", pk), arrayOf("rank", rnd.nextInt(101).toString()), arrayOf("followers", rnd.nextInt(5000).toString())),
                                "",
                            )
                        cards += card(service)
                        if (i % 6 == 0) cards += card(other)
                    }
                    val load = ms { runBlocking { cards.chunked(1_000).forEach { store.batchInsert(it) } } }
                    println("NIP85V load ${cards.size} cards in $load ms")
                    val svc = service.pubKey

                    val page =
                        "SELECT e.d AS target, CAST(r.t1 AS INTEGER) AS rank FROM events AS e JOIN tags AS r ON r.event_id = e.id AND r.t0 = 'rank' " +
                            "WHERE e.kind = 30382 AND e.pubkey = ? AND e.d > ? ORDER BY target LIMIT 1000"
                    var rows = 0
                    var pages = 0
                    var first = 0L
                    val walk =
                        ms {
                            var last = ""
                            while (true) {
                                var got = 0
                                val t =
                                    ms {
                                        val r = runBlocking { store.nql(page, listOf(svc, last)) }
                                        got = r.rows.size
                                        if (got > 0) last = r.rows.last()[0] as String
                                    }
                                if (pages == 0) first = t
                                if (got == 0) break
                                rows += got
                                pages++
                            }
                        }
                    println("NIP85V nql by d: $rows rows, $pages pages, $walk ms (first page $first ms)")

                    val all =
                        "SELECT e.d AS target, CAST(r.t1 AS INTEGER) AS rank FROM events AS e JOIN tags AS r ON r.event_id = e.id AND r.t0 = 'rank' " +
                            "WHERE e.kind = 30382 AND e.pubkey = ?"
                    var one = 0
                    println("NIP85V nql one query: ${ms { one = runBlocking { store.nql(all, listOf(svc), maxRows = null) }.rows.size }} ms, $one rows")

                    var req = 0
                    val reqMs =
                        ms {
                            var until: Long? = null
                            val seen = HashSet<String>()
                            while (true) {
                                val evs = runBlocking { store.query<Event>(Filter(kinds = listOf(30382), authors = listOf(svc), until = until, limit = 1000)) }
                                val fresh = evs.filter { seen.add(it.id) }
                                if (fresh.isEmpty()) break
                                req += fresh.size
                                until = evs.minOf { it.createdAt }
                            }
                        }
                    println("NIP85V REQ pages: $req events, $reqMs ms")
                }
            }
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
    }
}
