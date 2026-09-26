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
import com.nosfabrica.vespa.eventstore.benchmark.harness.Backends
import com.nosfabrica.vespa.eventstore.benchmark.harness.NostrCorpus
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nipXXSql.FilterSql
import com.vitorpamplona.quartz.nipXXSql.SqlException
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertTrue

/**
 * NQL (NIP-FF) on a REAL Vespa, through Quartz's interpreter over this store's
 * backend, against Quartz's SQLite store, which compiles it to SQLite. Every query here is one the
 * pushdown answers through a different engine path: native counts and groupings,
 * newest-first LIMIT, `visitIds` walks, join-key propagation, and the filter
 * spellings (`FilterSql`) the relay's own mirror and monitor read through. The
 * in-memory index can't see a Vespa-only divergence in any of them.
 *
 * Same container and skip rules as [VespaParityIT].
 */
@Tag("integration")
class SqlParityIT {
    @Test
    fun `real Vespa answers the SQL profile exactly as SQLite`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the real-Vespa SQL parity IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                VespaEventStore.open(url = queryUrl, autoDeploy = true, configUrl = configUrl).use { store ->
                    val corpus = NostrCorpus.generate(NostrCorpus.Config(size = CORPUS_SIZE, seed = 42))
                    runBlocking { corpus.chunked(1_000).forEach { store.batchInsert(it) } }
                    val sqlite = Backends.sqliteMemory()
                    runBlocking { corpus.chunked(1_000).forEach { sqlite.batchInsert(it) } }

                    val mismatches = ArrayList<String>()
                    var compared = 0
                    for ((sql, params) in queries(corpus)) {
                        val expected = rows(sqlite, sql, params)
                        val actual =
                            try {
                                rows(store, sql, params)
                            } catch (e: SqlException) {
                                mismatches += "refused: ${e.message}\n    $sql"
                                continue
                            }
                        compared++
                        if (expected != actual) {
                            mismatches += "rows differ (sqlite ${expected.size}, vespa ${actual.size}; first sqlite ${expected.take(2)}, vespa ${actual.take(2)})\n    $sql"
                        }
                    }
                    sqlite.close()

                    println("[${if (mismatches.isEmpty()) "PASS" else "FAIL"}] SQL parity Vespa vs SQLite: ${compared - mismatches.size}/$compared queries agree")
                    mismatches.forEach { println("   ✗ $it") }
                    assertTrue(mismatches.isEmpty(), "Vespa's SQL diverged from SQLite:\n" + mismatches.joinToString("\n"))
                    assertTrue(compared >= 20, "only $compared queries compared")
                }
            }
    }

    private fun rows(
        store: IEventStore,
        sql: String,
        params: List<Any?>,
    ): List<List<Any?>> = runBlocking { store.nql(sql, params) }.rows

    /** Relay-shaped queries and filter spellings, parameterized from what the corpus holds. */
    private fun queries(corpus: List<Event>): List<Pair<String, List<Any?>>> {
        val authors = corpus.map { it.pubKey }.distinct().shuffled(kotlin.random.Random(3))
        val times = corpus.map { it.createdAt }.sorted()
        val (lo, hi) = times[times.size / 4] to times[times.size * 3 / 4]
        val pValues = corpus.flatMap { e -> e.tags.filter { it.size > 1 && it[0] == "p" }.map { it[1] } }.distinct().shuffled(kotlin.random.Random(4))
        val ids = corpus.map { it.id }.shuffled(kotlin.random.Random(5))
        val q = ArrayList<Pair<String, List<Any?>>>()

        // Native shapes: engine count, countByAuthor, the tag_index grouping.
        q += "SELECT count(*) AS n FROM events WHERE kind = 1" to emptyList()
        q += "SELECT count(*) AS n FROM events WHERE kind IN (1, 7) AND created_at BETWEEN ? AND ?" to listOf(lo, hi)
        q += "SELECT pubkey, count(*) AS n FROM events WHERE kind = 1 GROUP BY pubkey ORDER BY n DESC, pubkey" to emptyList()
        q += "SELECT DISTINCT t1 FROM tags WHERE kind = 10002 AND t0 = 'r' AND t1 <> '' ORDER BY t1" to emptyList()
        q += "SELECT DISTINCT t1 FROM tags WHERE kind = 1 AND t0 = 'p' AND t1 <> '' ORDER BY t1 LIMIT 50" to emptyList()
        // Scans: newest-first LIMIT (with its tie group), whole walks, positional tag reads.
        q += "SELECT id, created_at FROM events WHERE kind = 1 ORDER BY created_at DESC, id LIMIT 25" to emptyList()
        q += "SELECT id FROM events WHERE kind = 7 ORDER BY created_at DESC, id LIMIT 10 OFFSET 5" to emptyList()
        q += "SELECT kind, count(*) AS n, min(created_at) AS first, max(created_at) AS last FROM events WHERE kind IN (0, 1, 3, 7) AND created_at >= ? GROUP BY kind ORDER BY kind" to listOf(lo)
        q += "SELECT DISTINCT t1, t2 FROM tags WHERE kind = 10002 AND t0 = 'r' AND (t2 IS NULL OR t2 = 'write') ORDER BY t1, t2" to emptyList()
        q += "SELECT pubkey, created_at, id FROM events WHERE kind = 0 AND pubkey IN (?, ?, ?, ?) ORDER BY pubkey" to authors.take(4)
        q += "SELECT id FROM events WHERE id IN (?, ?, ?, ?) ORDER BY id" to ids.take(3) + "f".repeat(64)
        // Math functions: zap-style arithmetic over tag values and grouped timestamps.
        q +=
            "SELECT kind, round(sqrt(avg(created_at)) * 1000) / 1000 AS root, floor(log10(count(*) + 1)) AS digits, pow(2, kind % 5) AS p, " +
            "max(created_at) % 7 AS m, abs(min(created_at) - ?) AS a, round(exp(ln(count(*)))) AS n FROM events WHERE kind IN (0, 1, 3, 7) GROUP BY kind ORDER BY kind" to listOf(lo)
        // Joins whose second side only the join key makes selective.
        q += "SELECT count(*) AS n FROM tags AS t JOIN events AS n ON n.id = t.t1 WHERE t.kind = 7 AND t.t0 = 'e'" to emptyList()
        q +=
            "SELECT d.t1, count(*) AS n FROM tags AS l JOIN tags AS d ON d.event_id = l.event_id AND d.t0 = 'p' " +
            "WHERE l.kind = 1 AND l.t0 = 'e' GROUP BY d.t1 ORDER BY n DESC, t1 LIMIT 20" to emptyList()
        q += "SELECT e.id FROM events AS e WHERE e.kind = 1 AND EXISTS (SELECT 1 AS x FROM tags AS t WHERE t.event_id = e.id AND t.t0 = 'p' AND t.t1 = ?) ORDER BY e.id" to pValues.take(1)

        // The filter spellings the mirror and the monitor read through.
        val filters =
            listOf(
                Filter(kinds = listOf(1), limit = 40),
                Filter(kinds = listOf(1, 7), since = lo, until = hi),
                Filter(kinds = listOf(1), authors = authors.take(5)),
                Filter(kinds = listOf(1), tags = mapOf("p" to pValues.take(3))),
                Filter(kinds = listOf(1, 7), tags = mapOf("p" to pValues.take(5)), since = lo, limit = 15),
                Filter(kinds = listOf(10002)),
                Filter(ids = ids.take(20)),
            )
        for (f in filters) {
            FilterSql.ids(f).let { q += it.nql to it.params }
            FilterSql.count(f).let { q += it.nql to it.params }
        }
        FilterSql.events(ids.take(FilterSql.HYDRATE_CHUNK)).let { q += it.nql to it.params }
        FilterSql.tags(ids.take(FilterSql.HYDRATE_CHUNK)).let { q += it.nql to it.params }
        return q
    }

    private fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val CORPUS_SIZE = 4_000
    }
}
