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
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
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
 * THE JSON `select` FORM SERVES THE PAGE YQL SERVES, against a real Vespa.
 *
 * A text-free read goes to the engine as `select` (`VespaQuery.select`, `SelectForm`) because
 * the query container parses it more cheaply than YQL. `EventYqlTest` proves the where-node
 * renders back to the YQL clause; this proves the engine reads the two the same: every shape
 * here, across every clause the form writes (ids, kinds, authors, owners, OR and AND tags,
 * since/until, both expiry bounds, the gated profile, a tie group straddling a page boundary),
 * is read by two clients identical but for `selectQueries`, under the default strategy and the
 * shipped match-phase one, and must return the same ids in the same order. The engine must
 * never refuse `select` (no `query.select.refused`), or the select client was reading YQL.
 *
 * And THE LEAN SEARCH CHAIN serves the page the default chain serves: the select client sends its
 * reads on the package's `lean` chain (VespaApp.LEAN_CHAIN), which skips federation and the
 * searchers only text needs, and a third client sends the same `select` on the default chain.
 * The engine must never refuse the chain (no `query.leanchain.refused`).
 */
@Tag("integration")
class SelectQueryIT {
    @Test
    fun `select and yql serve the same pages`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the select query IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}").deployIfAbsent(queryUrl)
                val strategies = listOf(RecencyStrategy.SPECULATIVE, RecencyStrategy.MATCH_PHASE)
                val clients =
                    strategies.associateWith { strategy ->
                        VespaEventIndex(queryUrl, recencyStrategy = strategy, selectQueries = true) to
                            VespaEventIndex(queryUrl, recencyStrategy = strategy, selectQueries = false)
                    }
                val defaultChain = strategies.associateWith { VespaEventIndex(queryUrl, recencyStrategy = it, selectQueries = true, leanChain = false) }
                try {
                    runBlocking {
                        val now = System.currentTimeMillis() / 1000
                        val authors = listOf(A, B, C, D)
                        val hashtags = listOf("bitcoin", "nostr", "art", "music")
                        var n = 0
                        val corpus = ArrayList<EventDoc>()
                        for (i in 1..120) {
                            val kind = listOf(1, 7, 30023)[i % 3]
                            val tags = ArrayList<List<String>>()
                            tags += listOf("t", hashtags[i % 4])
                            if (i % 2 == 0) tags += listOf("t", hashtags[(i + 1) % 4])
                            if (i % 5 == 0) tags += listOf("p", authors[(i + 2) % 4])
                            if (i % 7 == 0) tags += listOf("e", E)
                            if (i % 11 == 0) tags += listOf("expiration", (now + 86_400L).toString())
                            if (i % 13 == 0) tags += listOf("expiration", (now + 600L).toString())
                            corpus += note(n++, authors[i % 4], at = now - i * 60L, kind = kind, tags = tags)
                        }
                        // A tie group straddling a page boundary: five notes on one second.
                        val tieAt = now - 30 * 60L + 7
                        repeat(5) { corpus += note(n++, authors[it % 4], at = tieAt, kind = 1, tags = listOf(listOf("t", "bitcoin"))) }
                        val any = clients.getValue(RecencyStrategy.SPECULATIVE).second
                        any.putAll(corpus)
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(listOf(ReputationDoc(A, influenceScores = serviceCells(OBSERVER to 50)), ReputationDoc(B, influenceScores = serviceCells(OBSERVER to 2))))
                        }
                        awaitCorpus(any, corpus.size)

                        val ids = corpus.shuffled(kotlin.random.Random(1)).take(9).map { it.id }
                        val shapes =
                            listOf(
                                EventQuery(limit = 10),
                                EventQuery(kinds = listOf(1), limit = 7),
                                EventQuery(kinds = listOf(1, 30023), authors = listOf(A, C), limit = 15),
                                EventQuery(authors = listOf(B.uppercase(), B, "not-hex"), limit = 5),
                                EventQuery(ids = ids),
                                EventQuery(ids = ids, kinds = listOf(1)),
                                EventQuery(owners = listOf(D), limit = 9),
                                EventQuery(tags = mapOf("t" to listOf("bitcoin")), limit = 12),
                                EventQuery(kinds = listOf(1), tags = mapOf("t" to listOf("bitcoin")), limit = 3),
                                EventQuery(tags = mapOf("t" to listOf("art", "music"), "p" to listOf(A, B)), limit = 20),
                                EventQuery(tagsAll = mapOf("t" to listOf("bitcoin", "nostr")), limit = 20),
                                EventQuery(tagsAll = mapOf("t" to listOf("art")), tags = mapOf("e" to listOf(E)), limit = 20),
                                EventQuery(kinds = listOf(1, 7), since = now - 40 * 60L, until = now - 10 * 60L, limit = 50),
                                EventQuery(kinds = listOf(1), until = tieAt, limit = 4),
                                EventQuery(kinds = listOf(1, 7, 30023), notExpiredAt = now + 3_600L, limit = 100),
                                EventQuery(expiresBefore = now + 3_600L),
                                EventQuery(kinds = listOf(1, 7, 30023), limit = 200),
                            )
                        val gated = shapes.filter { it.limit != null }.map { it.copy(ranking = EventYql.RANK_RECENCY_GATED, observer = OBSERVER, rankKey = OBSERVER, followersKey = OBSERVER, minRank = 2.0) }
                        val refusedBefore = IngestStats.snapshot()[SELECT_REFUSED]?.calls ?: 0L
                        val chainRefusedBefore = IngestStats.snapshot()[LEAN_CHAIN_REFUSED]?.calls ?: 0L
                        var gatedNonEmpty = 0
                        for ((strategy, pair) in clients) {
                            val (select, yql) = pair
                            for (shape in shapes + gated) {
                                val q = shape.copy(nowSecs = now)
                                assertTrue(EventYql.build(q)?.select != null, "every shape here has a select form: $q")
                                val viaYql = yql.search(q).map { it.id }
                                val viaSelect = select.search(q).map { it.id }
                                assertEquals(viaYql, viaSelect, "$strategy: $q")
                                assertEquals(viaSelect, defaultChain.getValue(strategy).search(q).map { it.id }, "$strategy, lean chain against the default one: $q")
                                // Every ungated shape matches documents; a gated one may be empty
                                // (only A and B are trusted above the floor), but not all of them.
                                if (shape in shapes) assertTrue(viaYql.isNotEmpty(), "$strategy, an ungated shape matches documents: $q")
                                if (shape !in shapes && viaYql.isNotEmpty()) gatedNonEmpty++
                            }
                        }
                        assertTrue(gatedNonEmpty >= gated.size, "the gated shapes match documents too ($gatedNonEmpty non-empty)")
                        assertEquals(refusedBefore, IngestStats.snapshot()[SELECT_REFUSED]?.calls ?: 0L, "the engine took every select query")
                        assertEquals(chainRefusedBefore, IngestStats.snapshot()[LEAN_CHAIN_REFUSED]?.calls ?: 0L, "the package defines the lean chain")
                    }
                } finally {
                    clients.values.forEach { (a, b) ->
                        a.close()
                        b.close()
                    }
                    defaultChain.values.forEach { it.close() }
                }
            }
    }

    private fun note(
        n: Int,
        pubkey: String,
        at: Long,
        kind: Int,
        tags: List<List<String>>,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = at,
        kind = kind,
        tags = tags,
        content = "note $n",
        sig = "e".repeat(128),
        search = SearchFields.NONE,
    )

    private suspend fun awaitCorpus(
        index: VespaEventIndex,
        expected: Int,
    ) {
        repeat(120) {
            if (index.count(EventQuery()) >= expected) return
            delay(500)
        }
        error("corpus never became searchable ($expected docs)")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

        /** VespaEventIndex.SELECT_REFUSED_STAGE, booked when the engine refuses a `select`. */
        const val SELECT_REFUSED = "query.select.refused"
        const val LEAN_CHAIN_REFUSED = "query.leanchain.refused"
        val A = "a1".repeat(32)
        val B = "b2".repeat(32)
        val C = "c3".repeat(32)
        val D = "d4".repeat(32)
        val E = "e5".repeat(32)

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
