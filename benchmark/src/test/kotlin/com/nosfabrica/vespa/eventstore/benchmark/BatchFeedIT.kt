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
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * The bundled batch handler (`:bundle`, `BatchFeedHandler`) against a REAL Vespa: the package
 * deploys with its bundle, the client's batched puts go through it rather than quietly falling
 * back, and what it stores is exactly what was put — fields, search columns, and the routing
 * selection's treatment of an expired event. The mock cannot run any of this: it has no OSGi
 * container, no indexing chain and no routing.
 */
@Tag("integration")
class BatchFeedIT {
    @Test
    fun `batched puts are stored and found exactly as put`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the batch-feed IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}").deployIfAbsent(queryUrl)
                VespaEventIndex(queryUrl, feedBatch = 100).use { index ->
                    runBlocking {
                        val notes = List(250) { note(it, "batched note number $it about tarantellas") }
                        // Already expired: the routing selection drops it on any path, and the
                        // handler must count that as done rather than fail its batch.
                        val expired = note(999, "gone before it arrived", tags = listOf(listOf("expiration", "5")))
                        index.putAll(notes + expired)

                        assertEquals(3L, index.feedBatchesSent, "251 documents went through the handler in three batches")
                        for (n in notes) {
                            val got = index.get(n.id)!!
                            assertEquals(n.toEventJson(), got.toEventJson(), "the event, byte for byte")
                            assertEquals(n.search, got.search, "the search columns")
                        }
                        assertNull(index.get(expired.id), "the expired event was dropped by the routing selection")
                        assertEquals(
                            notes.map { it.id }.toSet(),
                            index.search(EventQuery(kinds = listOf(1), search = "tarantellas", limit = 400)).map { it.id }.toSet(),
                            "every batched note is found by search",
                        )
                    }
                }
            }
    }

    private fun note(
        n: Int,
        content: String,
        tags: List<List<String>> = emptyList(),
    ) = EventDoc(
        id = (n + 1).toString(16).padStart(64, '0'),
        pubkey = "a".repeat(64),
        createdAt = 1_700_000_000L + n,
        kind = 1,
        tags = tags,
        content = content,
        sig = "e".repeat(128),
        search = SearchFields(text = content),
    )

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
