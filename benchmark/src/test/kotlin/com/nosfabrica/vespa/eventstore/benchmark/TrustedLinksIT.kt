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
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertEquals

/**
 * TRUSTED LINKS ON A REAL VESPA — what only the engine can answer about the
 * `sort:links` rerank (store/search/LinkRerank.kt):
 *
 *  - `EventIndex.countLinks`' grouping DEPLOYS and parses: the cross-letter key
 *    OR, the `filter(regex())` on the grouping, key then kind;
 *  - THE GATE REACHES THE GROUPING at `hits=0`: a below-floor author's link and
 *    an unranked author's link are not counted under the lens, and are without
 *    it. (A `totalCount` at `hits=0` skips the ranking pass on Vespa 8.731 —
 *    see VespaEventIndex.count — so this is pinned, not assumed);
 *  - a duplicated tag counts per entry, the contract the in-memory reference
 *    states (CountLinksTest);
 *  - end to end through `open()`: a search page reorders by the trusted links
 *    alone — an under-ranked note with five trusted reactions overtakes one
 *    with twenty-one untrusted ones, even at `limit 1` (the widened recall is
 *    what lets it in), and `sort:links:0` is the engine's order.
 *
 * Tagged `integration`; run with `-Pintegration` where Docker is available.
 */
@Tag("integration")
class TrustedLinksIT {
    @Test
    fun `links are counted per key and kind through the lens, and a page reorders by them`() {
        assumeTrue(dockerAvailable(), "Docker not available - skipping the trusted links IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                SchemaDeployer(configUrl).deployIfAbsent(queryUrl)
                VespaEventStore.open(url = queryUrl, autoDeploy = false, configUrl = configUrl).use { store ->
                    runBlocking {
                        val now = System.currentTimeMillis() / 1000
                        store.batchInsert(
                            listOf(
                                TrustProviderListEvent(hexId(), OBSERVER, now - 5_000, arrayOf(arrayOf("30382:rank", PROVIDER, "wss://scores.test/")), "", ""),
                                card(TRUSTED, 50, now),
                                card(LOW, 1, now),
                            ),
                        )
                        val dusk = event(TRUSTED, 1, "zebra crossing at dusk", now - 3_600)
                        val dawn = event(TRUSTED, 1, "zebra crossing at dawn", now - 30 * 86_400)
                        val article = event(TRUSTED, 30023, "a zebra guide", now - 7_200, arrayOf("d", "zebra-guide"))
                        store.batchInsert(listOf(dusk, dawn, article))
                        store.awaitTrustProjection()

                        val search = "zebra crossing observer:$OBSERVER"
                        val base = store.query<Event>(Filter(kinds = listOf(1), search = search, limit = 2)).map { it.id }
                        assertEquals(2, base.size, "both notes recall through the lens: $base")
                        val (top, under) = base
                        val address = "30023:$TRUSTED:zebra-guide"

                        store.batchInsert(
                            List(5) { event(TRUSTED, 7, "+", now - 60 + it, arrayOf("e", under)) } +
                                List(20) { event(LOW, 7, "+", now - 60 + it, arrayOf("e", top)) } +
                                event(UNRANKED, 7, "+", now - 30, arrayOf("e", top)) +
                                // A client that wrote the same `e` twice: two entries, two counts.
                                event(TRUSTED, 1, "agreed", now - 20, arrayOf("e", under), arrayOf("e", under)) +
                                event(TRUSTED, 1, "good read", now - 10, arrayOf("a", address)) +
                                event(LOW, 1, "meh", now - 10, arrayOf("a", address)),
                        )

                        val keys = listOf("e:$top", "e:$under", "a:$address")
                        val lensed = EventQuery(observer = OBSERVER, rankKey = PROVIDER, minRank = 2.0, ranking = EventYql.RANK_RECENCY_GATED)
                        assertEquals(
                            mapOf("e:$under" to mapOf(7 to 5, 1 to 2), "a:$address" to mapOf(1 to 1)),
                            store.engine.countLinks(keys, lensed),
                            "under the lens only the trusted author's links count; top's 21 untrusted reactions are absent",
                        )
                        assertEquals(
                            mapOf("e:$top" to mapOf(7 to 21), "e:$under" to mapOf(7 to 5, 1 to 2), "a:$address" to mapOf(1 to 2)),
                            store.engine.countLinks(keys, EventQuery()),
                            "with no lens every link counts",
                        )

                        assertEquals(base, store.query<Event>(Filter(kinds = listOf(1), search = search, limit = 2)).map { it.id }, "no token, the engine's order")
                        assertEquals(listOf(under, top), store.query<Event>(Filter(kinds = listOf(1), search = "$search sort:links", limit = 2)).map { it.id })
                        assertEquals(listOf(under), store.query<Event>(Filter(kinds = listOf(1), search = "$search sort:links", limit = 1)).map { it.id }, "the widened recall lifts a hit the limit alone would have cut")
                        assertEquals(base, store.query<Event>(Filter(kinds = listOf(1), search = "$search sort:links:0", limit = 2)).map { it.id }, "strength 0 is the engine's order")
                    }
                }
            }
    }

    private fun card(
        subject: String,
        rank: Int,
        now: Long,
    ) = UserAssertionEvent(hexId(), PROVIDER, now - 4_000, arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString())), "", "")

    private fun event(
        author: String,
        kind: Int,
        content: String,
        at: Long,
        vararg tags: Array<String>,
    ): Event = EventFactory.create(hexId(), author, at, kind, arrayOf(*tags), content, "e".repeat(128))

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        val OBSERVER = "c".repeat(64)
        val PROVIDER = "5e".repeat(32)
        val TRUSTED = "b1".repeat(32)
        val LOW = "b2".repeat(32)
        val UNRANKED = "b3".repeat(32)

        private var seq = 0

        fun hexId() = (++seq).toString(16).padStart(64, 'a')

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
