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

import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.client.RecencyStrategy
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
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
 * EVERY [RecencyStrategy] SERVES THE EXPECTED PAGE, ON A REAL VESPA, GATE
 * INCLUDED.
 *
 * The mock tests pin that the four strategies agree with EACH OTHER; they
 * cannot pin that the agreed page is RIGHT where only the engine decides it —
 * the trust gate (a rank profile's `rank-score-drop-limit`), the match-phase
 * profiles, the sorting degrader the full scan turns off, and the gated count
 * the count-probe strategy windows by. So every page here is compared against
 * the answer computed straight from the seeded corpus: filter by the query,
 * drop below-floor authors for a gated read, order `created_at desc, id asc`,
 * take the limit.
 *
 * The corpus has one density per strategy branch: a busy half-hour (the first
 * window fills), a week at one note per 8 hours (the window widens), and
 * 1970-era notes (the narrow-read rule falls back) — each spread over four
 * trust tiers (trusted 50, marginal 2 == the default floor, low 1, unranked),
 * so a window that proved itself on UNGATED matches would serve a short or
 * wrong gated page, and fail here.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class RecencyStrategyIT {
    @Test
    fun `every recency strategy serves the expected page`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the recency strategy IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}").deployIfAbsent(queryUrl)
                val clients = RecencyStrategy.entries.associateWith { VespaEventIndex(queryUrl, recencyStrategy = it) }
                try {
                    runBlocking {
                        val now = System.currentTimeMillis() / 1000
                        val tiers = listOf(TRUSTED, MARGINAL, LOW, UNRANKED)
                        var n = 0
                        val busy = (1..60).map { note(n++, tiers[it % 4], at = now - it * 30L) }
                        val weekly = (1..21).map { note(n++, tiers[it % 4], at = now - it * 8 * 3_600L) }
                        val ancient = (1..8).map { note(n++, tiers[it % 4], at = 1_000L + it) }
                        // A tie group straddling a page boundary: four notes on ONE second.
                        val tied = (0 until 4).map { note(n++, tiers[it], at = now - 45 * 30L) }
                        val corpus = busy + weekly + ancient + tied
                        val any = clients.getValue(RecencyStrategy.MATCH_PHASE)
                        any.putAll(corpus)
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(
                                listOf(
                                    ReputationDoc(TRUSTED, influenceScores = serviceCells(OBSERVER to 50)),
                                    ReputationDoc(MARGINAL, influenceScores = serviceCells(OBSERVER to 2)),
                                    ReputationDoc(LOW, influenceScores = serviceCells(OBSERVER to 1)),
                                ),
                            )
                        }
                        awaitCorpus(any, corpus.size)

                        val shapes =
                            listOf(
                                EventQuery(kinds = listOf(1), limit = 10),
                                EventQuery(kinds = listOf(1), limit = 44),
                                EventQuery(kinds = listOf(1), limit = 75),
                                EventQuery(authors = listOf(TRUSTED, LOW), limit = 12),
                                EventQuery(authors = listOf(UNRANKED), limit = 20),
                                EventQuery(kinds = listOf(1), until = now - 2 * 86_400L, limit = 6),
                                EventQuery(kinds = listOf(1), since = now - 600, limit = 50),
                                EventQuery(kinds = listOf(1), limit = 500),
                                // Past the match-phase band: plain pages it; gated windows it.
                                EventQuery(kinds = listOf(1), limit = EventYql.MATCH_PHASE_BAND + 500),
                            )
                        val gatedShapes = shapes.map { it.copy(ranking = EventYql.RANK_RECENCY_GATED, observer = OBSERVER, rankKey = OBSERVER, followersKey = OBSERVER, minRank = 2.0) }
                        for (shape in shapes + gatedShapes) {
                            val q = shape.copy(nowSecs = now)
                            val expected = expectedPage(corpus, q)
                            // Every UNGATED shape matches something; a gated one may
                            // rightly serve nothing (the unranked author's feed), and
                            // every strategy must then agree on the empty page too.
                            assertTrue(expected.isNotEmpty() || q.ranking == EventYql.RANK_RECENCY_GATED, "the shape must match something: $q")
                            for ((strategy, client) in clients) {
                                assertEquals(expected, client.search(q).map { it.id }, "$strategy: $q")
                            }
                        }

                        // The count the count-probe strategy windows by must be
                        // the GATED number, or it proves windows the gated read
                        // cannot fill (it did, on Vespa 8.731 — see count()).
                        val gatedAll = gatedShapes.first().copy(limit = null, nowSecs = now)
                        assertEquals(expectedPage(corpus, gatedAll).size, any.count(gatedAll), "a gated count is the gated page's size")
                    }
                } finally {
                    clients.values.forEach { it.close() }
                }
            }
    }

    // ------------------------------------------------------------------

    /** The page [q] must serve, computed from the corpus alone: NIP-01 order, the observer gate, the limit. */
    private fun expectedPage(
        corpus: List<EventDoc>,
        q: EventQuery,
    ): List<String> {
        val gated = q.ranking == EventYql.RANK_RECENCY_GATED
        return corpus
            .asSequence()
            .filter { q.kinds.isEmpty() || it.kind in q.kinds }
            .filter { q.authors.isEmpty() || it.pubkey in q.authors }
            .filter { q.since == null || it.createdAt >= q.since!! }
            .filter { q.until == null || it.createdAt <= q.until!! }
            .filter { !gated || it.pubkey == TRUSTED || it.pubkey == MARGINAL }
            .sortedWith(compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id })
            .let { s -> q.limit?.let { s.take(it) } ?: s }
            .map { it.id }
            .toList()
    }

    private fun note(
        n: Int,
        pubkey: String,
        at: Long,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = at,
        kind = 1,
        tags = emptyList(),
        content = "note $n",
        sig = "e".repeat(128),
        search = SearchFields.NONE,
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
