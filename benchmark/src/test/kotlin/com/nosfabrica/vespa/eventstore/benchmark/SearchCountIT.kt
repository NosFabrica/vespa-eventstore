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

import com.nosfabrica.vespa.eventstore.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A COUNT IS THE SERVED PAGE'S SIZE, AND IT COSTS NO DOCUMENTS.
 *
 * [com.nosfabrica.vespa.eventstore.engine.EventIndex.count] answers a ranked
 * query from the engine's own `totalCount` with zero hits, on the premise that
 * Vespa reports that number NET of the trust gate — `wot_mult()` maps a
 * below-floor author to 0 and `rank-score-drop-limit` deletes the hit, and a
 * deleted hit is gone from `totalCount` too. That premise is a property of the
 * deployed rank profile, so nothing but a real Vespa can hold it: the in-memory
 * reference has no trust data to gate on and the mock does not rank at all.
 *
 * Which is exactly how the ruinous version got there. Counting used to
 * materialize the page and take its size — correct for the same reason this is,
 * since the served page IS the gated set, but it fetched a full document summary
 * per counted event. On the production relay, where a COUNT arrives carrying
 * `limit: 100000`, that was 76.0s for "bitcoin" against 4.9s for the same
 * search (2026-09-01).
 *
 * So the assertions are in pairs: the count must EQUAL the page the same query
 * serves, and — the anti-regression that matters — it must be strictly SMALLER
 * than the raw match set wherever the floor bites. An unranked grouping count
 * would satisfy the first pair's arithmetic on an ungated query and fail the
 * second on every gated one.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class SearchCountIT {
    @Test
    fun `a searching count equals the gated page it summarizes`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the search count IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}").deployIfAbsent(queryUrl)
                VespaEventIndex(queryUrl).use { index ->
                    runBlocking {
                        // One tier per author, notes interleaved so no assertion
                        // can pass by accident of ordering.
                        val authors = listOf(TRUSTED, MARGINAL, LOW, UNRANKED)
                        val notes = (0 until 40).map { n -> note(n, authors[n % authors.size], "pizza") }
                        index.putAll(notes)
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(
                                listOf(
                                    ReputationDoc(TRUSTED, influenceScores = mapOf(OBSERVER to 50)),
                                    ReputationDoc(MARGINAL, influenceScores = mapOf(OBSERVER to 2)),
                                    ReputationDoc(LOW, influenceScores = mapOf(OBSERVER to 1)),
                                ),
                            )
                        }
                        awaitCorpus(index, notes.size)

                        // The raw match set: every note carries the term.
                        val matched = notes.size
                        assertEquals(
                            matched,
                            index.count(EventQuery(kinds = listOf(1), search = "pizza")),
                            "no lens, nothing gated: the count is the whole match set",
                        )

                        // The DEFAULT trust profile, at three floors. Each count
                        // must equal the page, and shrink as the floor rises.
                        var previous = Int.MAX_VALUE
                        for (floor in listOf(0.0, 2.0, 20.0)) {
                            val q = EventQuery(kinds = listOf(1), search = "pizza", observer = OBSERVER, rankKey = OBSERVER, followersKey = OBSERVER, minRank = floor)
                            val served = index.search(q).size
                            assertEquals(served, index.count(q), "search profile, floor $floor: count == served page")
                            assertTrue(served < previous || floor == 0.0, "floor $floor should not serve more than a lower one")
                            previous = served
                        }

                        // The floor BITES — without this the pair above would be
                        // satisfied by an ungated grouping count.
                        val gated = EventQuery(kinds = listOf(1), search = "pizza", observer = OBSERVER, rankKey = OBSERVER, followersKey = OBSERVER, minRank = 20.0)
                        val gatedCount = index.count(gated)
                        assertEquals(10, gatedCount, "only the trusted(50) author's notes clear a floor of 20")
                        assertTrue(gatedCount < matched, "a gated count is strictly smaller than the match set")

                        // `sort:recent`: gated AND match-phased. Its own
                        // totalCount would be capped by the cut, so
                        // EventYql.countProfileOf moves it to the full-scan twin
                        // — and the number has to survive that move.
                        val recent =
                            EventQuery(
                                kinds = listOf(1),
                                search = "pizza",
                                observer = OBSERVER,
                                rankKey = OBSERVER,
                                followersKey = OBSERVER,
                                minRank = 20.0,
                                ranking = EventYql.RANK_RECENCY_GATED,
                                limit = 50,
                            )
                        assertEquals(
                            index.search(recent).size,
                            index.count(recent),
                            "sort:recent: the count follows the same gate onto the exact profile",
                        )
                        assertEquals(gatedCount, index.count(recent), "same gate, same match set, same number")
                    }
                }
            }
    }

    /** A kind-1 note whose [text] is indexed, so a term recalls it. */
    private fun note(
        n: Int,
        pubkey: String,
        text: String,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = 1_700_000_000L + n,
        kind = 1,
        tags = emptyList(),
        content = text,
        sig = "e".repeat(128),
        search = SearchFields(text = text),
    )

    private suspend fun awaitCorpus(
        index: VespaEventIndex,
        expected: Int,
    ) {
        repeat(120) {
            if (index.count(EventQuery(kinds = listOf(1))) >= expected) return
            delay(500)
        }
        error("corpus never became searchable ($expected docs)")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"
        val TRUSTED = "11".repeat(32)
        val MARGINAL = "22".repeat(32)
        val LOW = "33".repeat(32)
        val UNRANKED = "44".repeat(32)

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
