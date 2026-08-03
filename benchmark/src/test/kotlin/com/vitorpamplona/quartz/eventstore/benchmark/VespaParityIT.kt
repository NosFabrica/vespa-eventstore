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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
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
 * The correctness gate as a CI test: stand up a REAL Vespa in a container,
 * ingest the corpus, and assert this store answers every NIP-01 filter exactly
 * as Quartz's SQLite store does ([ParityCheck]). This is what proves the Vespa
 * query path — YQL, the streaming decode, the get fast path, reconstruction —
 * stays correct end to end; the in-memory reference can't catch a real-Vespa-only
 * divergence (as the empty-`sig` bug showed).
 *
 * Skips cleanly where Docker is unavailable, so it never fails for lack of a
 * daemon. Tagged `integration` and EXCLUDED from the default `:benchmark:test`
 * (which stays fast, unit-only); the dedicated CI job runs it with
 * `-Pintegration`, where a real Docker daemon is present.
 */
@Tag("integration")
class VespaParityIT {
    @Test
    fun `real Vespa answers every NIP-01 filter exactly as SQLite`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the real-Vespa parity IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val host = vespa.host
                val queryUrl = "http://$host:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://$host:${vespa.getMappedPort(CONFIG_PORT)}"

                // open() deploys the bundled schema (config server on a mapped port,
                // so it must be passed explicitly) and waits until queries serve.
                val store = VespaEventStore.open(url = queryUrl, autoDeploy = true, configUrl = configUrl)
                store.use {
                    val corpus = NostrCorpus.generate(NostrCorpus.Config(size = CORPUS_SIZE, seed = 42))
                    runBlocking { corpus.chunked(1_000).forEach { store.batchInsert(it) } }

                    val sqlite = Backends.sqliteMemory()
                    runBlocking { corpus.chunked(1_000).forEach { sqlite.batchInsert(it) } }
                    val result = ParityCheck.run(corpus, "SQLite", sqlite, "Vespa", store)
                    sqlite.close()

                    ParityCheck.report("Vespa", "SQLite", result)
                    assertTrue(result.ok, "Vespa diverged from SQLite:\n" + result.mismatches.joinToString("\n"))
                }
            }
    }

    private fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        // Small enough to ingest and diff quickly in CI, large enough to exercise
        // supersession, deletions, tags, and time windows across all corpus kinds.
        const val CORPUS_SIZE = 4_000
    }
}
