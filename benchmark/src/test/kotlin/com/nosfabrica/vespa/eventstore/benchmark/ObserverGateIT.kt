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

/**
 * THE OBSERVER GATE against a REAL Vespa — the engine-side behavior the mock
 * cannot cover (it does not rank, hold trust scores, or run a match-phase):
 *
 *  - both gated profiles (`recency_gated` with its match-phase,
 *    `recency_gated_exact`) actually DEPLOY;
 *  - the gate drops exactly the authors below `min_rank` — unranked authors
 *    included — and keeps newest-first order among the survivors, on the
 *    match-phase AND the full-scan variant;
 *  - an explicit floor moves the cut;
 *  - the same query without the lens still recalls everything;
 *  - the store's `sort:recent` shape — the gated profile carrying SEARCH
 *    terms — deploys, recalls, and orders by created_at rather than relevance.
 *
 * It also times the dominant relay shape — "kind 1, limit 50" — anonymous vs
 * gated over a [BENCH_DOCS]-note corpus and prints the medians. The timings
 * are INFORMATIONAL (a containerized single-node Vespa is too noisy for a
 * latency assertion); the regression gate is the correctness above, plus your
 * eyes on the printed ratio when this runs against a staging cluster. The
 * match-phase DEGRADED path is not reachable here (it needs a match set well
 * past max-hits per node, ~20k docs x headroom) — that plumbing is pinned by
 * the mock tests; only a production-sized corpus exercises it end to end.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class ObserverGateIT {
    @Test
    fun `the gate drops below-floor authors, keeps recency order, and prices like ungated recall`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the observer gate IT")

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
                        // Four authors, one trust tier each, notes interleaved in
                        // time so every ordering assertion crosses authors:
                        //   trusted 50 / marginal 2 (== the default floor, kept) /
                        //   low 1 (dropped) / unranked (no doc at all, dropped).
                        val authors = listOf(TRUSTED, MARGINAL, LOW, UNRANKED)
                        val notes = (0 until 40).map { n -> note(n, pubkey = authors[n % authors.size]) }
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

                        val newestFirst = notes.sortedWith(compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id })
                        val trustedOnly = newestFirst.filter { it.pubkey == TRUSTED || it.pubkey == MARGINAL }

                        // Ungated: the lens is absent, everything recalls.
                        assertEquals(
                            newestFirst.take(10).map { it.id },
                            index.search(EventQuery(kinds = listOf(1), limit = 10)).map { it.id },
                            "no lens: plain NIP-01 recall",
                        )

                        // The gate on the MATCH-PHASE profile (small limit).
                        assertEquals(
                            trustedOnly.take(10).map { it.id },
                            index.search(gated(limit = 10)).map { it.id },
                            "gated limit'd feed: only authors >= the floor, newest first",
                        )

                        // The gate on the FULL-SCAN profile (no limit demotes to
                        // recency_gated_exact in EventYql.build).
                        assertEquals(
                            trustedOnly.map { it.id },
                            index.search(gated(limit = null)).map { it.id },
                            "gated unlimited recall: the exact profile applies the same gate",
                        )

                        // An explicit floor moves the cut: 10 keeps only trusted(50).
                        assertEquals(
                            newestFirst.filter { it.pubkey == TRUSTED }.take(10).map { it.id },
                            index.search(gated(limit = 10, minRank = 10.0)).map { it.id },
                            "filter:rank floor: marginal(2) drops, trusted(50) stays",
                        )

                        // ---- `sort:recent`: the same gate, pointed at a SEARCH ----
                        //
                        // The store maps `sort:recent` to this profile WITH
                        // query terms — the one shape that reaches a gated
                        // profile carrying text clauses and the text-ranking
                        // query features EventYql emits beside them. Only a
                        // real Vespa proves the deployed profile accepts that
                        // (features it never declares ride along unused) and
                        // that created_at, not relevance, orders the page.
                        val searchable = (0 until 12).map { n -> note(100 + n, pubkey = authors[n % authors.size], text = "pizza") }
                        index.putAll(searchable)
                        awaitCorpus(index, notes.size + searchable.size)

                        val chronological = searchable.sortedWith(compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id })
                        val trustedPizza = chronological.filter { it.pubkey == TRUSTED || it.pubkey == MARGINAL }

                        assertEquals(
                            trustedPizza.take(5).map { it.id },
                            index.search(gated(limit = 5).copy(search = "pizza")).map { it.id },
                            "sort:recent: the search's match set, gated and newest-first",
                        )
                        assertEquals(
                            trustedPizza.map { it.id },
                            index.search(gated(limit = null).copy(search = "pizza")).map { it.id },
                            "the full-scan variant takes terms too",
                        )
                        assertEquals(
                            chronological.map { it.id },
                            index.search(EventQuery(kinds = listOf(1), search = "pizza", ranking = EventYql.RANK_RECENCY_GATED)).map { it.id },
                            "no lens: nothing gates, and the profile is pure recency",
                        )

                        // ---- timing: the dominant relay shape, anonymous vs gated ----
                        // The bench authors alternate trusted/spam so the gated page
                        // has to skip half the candidates it scores.
                        val bench = (0 until BENCH_DOCS).map { n -> note(1_000 + n, pubkey = if (n % 2 == 0) TRUSTED else UNRANKED) }
                        index.putAll(bench)
                        awaitCorpus(index, notes.size + bench.size)

                        val anonymous = EventQuery(kinds = listOf(1), limit = 50)
                        val anonMs = medianMs { index.search(anonymous) }
                        val gatedMs = medianMs { index.search(gated(limit = 50)) }
                        println("observer gate @ ${notes.size + bench.size} notes — kind 1, limit 50:")
                        println("  anonymous (recency):      %.2f ms median".format(anonMs))
                        println("  gated (recency_gated):    %.2f ms median (%.1fx)".format(gatedMs, gatedMs / anonMs))
                    }
                }
            }
    }

    // ------------------------------------------------------------------

    private fun gated(
        limit: Int?,
        minRank: Double = 2.0,
    ) = EventQuery(kinds = listOf(1), limit = limit, ranking = EventYql.RANK_RECENCY_GATED, minRank = minRank, observer = OBSERVER)

    /** A note; [text] (when given) also feeds the search index, making it recallable by term. */
    private fun note(
        n: Int,
        pubkey: String,
        text: String? = null,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = 1_700_000_000L + n,
        kind = 1,
        tags = emptyList(),
        content = text ?: "note $n",
        sig = "e".repeat(128),
        search = SearchFields(text = text),
    )

    /** Poll until the whole corpus (events fed so far) is searchable. */
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

    /** Median wall-clock of [runs] executions after [warmup] discarded ones. */
    private fun medianMs(
        warmup: Int = 5,
        runs: Int = 30,
        block: suspend () -> Unit,
    ): Double {
        val times = DoubleArray(runs)
        runBlocking {
            repeat(warmup) { block() }
            repeat(runs) { i ->
                val t0 = System.nanoTime()
                block()
                times[i] = (System.nanoTime() - t0) / 1e6
            }
        }
        times.sort()
        return times[runs / 2]
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** Notes fed for the timing comparison (on top of the 40 correctness notes). */
        const val BENCH_DOCS = 10_000

        /** The ranking lens; reputation tensor cells are keyed by observer. */
        val OBSERVER = "c".repeat(64)

        val TRUSTED = "1".padStart(64, 'b')
        val MARGINAL = "2".padStart(64, 'b')
        val LOW = "3".padStart(64, 'b')
        val UNRANKED = "4".padStart(64, 'b')

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
