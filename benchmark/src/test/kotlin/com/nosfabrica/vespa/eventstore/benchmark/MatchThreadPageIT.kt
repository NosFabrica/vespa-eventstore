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
import kotlinx.serialization.json.double
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
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
 * THE PAGE DOES NOT DEPEND ON HOW MANY THREADS MATCHED IT.
 *
 * services.xml ships `numthreadspersearch` above 1 so a NIP-50 relevance search
 * — the one shape whose cost is the whole match set, twice over — can spread
 * that work across cores, while [EventYql.MATCH_THREADS] asks for a single
 * thread on every other shape. The latency that buys is measured
 * (benchmark/README.md); what it must not buy is a different answer, and that
 * is a property of the ENGINE, so only a real Vespa can pin it.
 *
 * The query instant is pinned on both arms. Without that the two runs are
 * scored against two different `now`, the recency multiplier moves under them,
 * and a page that differs proves nothing about threads — the trap this test was
 * written after walking into (2026-09-01).
 *
 * TIES ARE THE LIMIT OF THE CLAIM, and the corpus is built so the test states
 * it honestly rather than hiding it: trust and recency both vary per document,
 * so the `search` profile's scores are distinct and the whole page is pinned
 * position by position. On a profile that leaves hits genuinely tied — pure
 * `text` on a corpus of identical documents — which member of an equal-score
 * group makes the cut is not defined by anything, thread count included, and
 * this test does not pretend otherwise: it asserts the SCORE SEQUENCE there,
 * which is the part that is defined.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class MatchThreadPageIT {
    @Test
    fun `a relevance page is identical at one thread and at the cluster ceiling`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the match-thread page IT")

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
                        val notes = corpus()
                        notes.chunked(500).forEach { index.putAll(it) }
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(
                                (0 until AUTHORS).map { a ->
                                    // A distinct trust score per author, so wot_mult
                                    // separates documents that share their text.
                                    ReputationDoc(author(a), influenceScores = mapOf(OBSERVER to 1 + a))
                                },
                            )
                        }
                        awaitCorpus(index, notes.size)

                        for (term in TERMS) {
                            for (limit in listOf(50, 200)) {
                                val q =
                                    EventQuery(
                                        kinds = listOf(1),
                                        limit = limit,
                                        search = term,
                                        observer = OBSERVER,
                                        minRank = 0.0,
                                        nowSecs = NOW,
                                    )
                                val ceiling = page(queryUrl, q, threads = null)
                                val single = page(queryUrl, q, threads = 1)
                                assertTrue(ceiling.isNotEmpty(), "\"$term\" recalled nothing — the corpus cannot pin anything")
                                assertEquals(
                                    single,
                                    ceiling,
                                    "\"$term\" limit $limit: the page must not depend on the match-thread count",
                                )
                            }
                        }

                        // The other half of the contract: the shapes that ASK for
                        // one thread get exactly what they asked for, and their
                        // page is the same one the ceiling would have produced.
                        val recall = EventQuery(kinds = listOf(1), limit = 50, nowSecs = NOW)
                        assertEquals(
                            EventYql.SINGLE_MATCH_THREAD,
                            EventYql.build(recall)!!.params[EventYql.MATCH_THREADS],
                            "plain recall opts out of the ceiling",
                        )
                        assertEquals(
                            page(queryUrl, recall, threads = 4),
                            page(queryUrl, recall, threads = 1),
                            "plain recall's page does not depend on threads either",
                        )
                    }
                }
            }
    }

    /**
     * The served page as (id, relevance) rows, through the store's OWN assembled
     * query with [threads] substituted for whatever [EventYql] asked for — null
     * meaning "send no parameter", which is how a relevance search takes the
     * cluster ceiling.
     */
    private fun page(
        url: String,
        q: EventQuery,
        threads: Int?,
    ): List<Pair<String, Double>> {
        val built = EventYql.build(q) ?: error("query matches nothing")
        val params = LinkedHashMap(built.params)
        if (threads == null) params.remove(EventYql.MATCH_THREADS) else params[EventYql.MATCH_THREADS] = threads.toString()
        val root = SearchTrace.post(url, built.yql, params, built.ranking)["root"]!!.jsonObject
        val children = root["children"]?.jsonArray ?: return emptyList()
        return children.mapNotNull { hit ->
            val fields = hit.jsonObject["fields"]?.jsonObject ?: return@mapNotNull null
            val id = fields["id"]?.jsonPrimitive?.content ?: return@mapNotNull null
            id to hit.jsonObject["relevance"]!!.jsonPrimitive.double
        }
    }

    /**
     * [DOCS] notes across [AUTHORS] authors and [TERMS], each note a different
     * second and a different author, so trust and recency both separate the
     * hits that share a word. Enough documents that Vespa splits the docid space
     * across match threads rather than handing one thread the lot.
     */
    private fun corpus(): List<EventDoc> =
        (0 until DOCS).map { n ->
            val term = TERMS[n % TERMS.size]
            EventDoc(
                id = n.toString(16).padStart(64, '0'),
                pubkey = author(n % AUTHORS),
                createdAt = NOW - n,
                kind = 1,
                tags = emptyList(),
                content = "note $n about $term",
                sig = "e".repeat(128),
                // Both tiers, so the page is ordered by a real ladder rather than
                // by one band with everything in it.
                search = SearchFields(text = "note $n about $term", secondary = if (n % 3 == 0) term else null),
            )
        }

    private fun author(a: Int) = a.toString(16).padStart(64, 'a')

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
        const val DOCS = 4000
        const val AUTHORS = 40
        const val NOW = 1_800_000_000L
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"
        val TERMS = listOf("bitcoin", "nostr", "lightning", "zap")

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
