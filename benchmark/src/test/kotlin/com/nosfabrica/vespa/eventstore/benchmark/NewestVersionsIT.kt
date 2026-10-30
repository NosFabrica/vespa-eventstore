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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.security.MessageDigest
import java.time.Duration
import kotlin.test.assertEquals

/**
 * `EventIndex.newestVersions` against a REAL Vespa, held to the in-memory
 * reference's answer: the grouping (author -> newest second -> ids) is YQL the
 * mock cannot run, and its two load-bearing details — `max(1)` ordered by
 * `-max(created_at)` keeping the NEWEST second, and the ids at that second
 * coming back whole so the tie goes to the lowest — are only checkable here.
 * Drift is fed on purpose (several versions per address, a tie at the newest
 * second), and the author list crosses the 500-author chunk.
 */
@Tag("integration")
class NewestVersionsIT {
    @Test
    fun `the engine's newest versions equal the reference's`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the newest-versions IT")

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
                        val authors = List(520) { "%064x".format(it + 1) }
                        val docs =
                            authors.flatMapIndexed { i, author ->
                                buildList {
                                    // Every author: an old version and a newer one.
                                    add(doc(author, 3, 1_000L + i, "old"))
                                    add(doc(author, 3, 2_000L + i, "new"))
                                    // Every third: a TIE at the newest second.
                                    if (i % 3 == 0) add(doc(author, 3, 2_000L + i, "tie"))
                                    // Every fifth: a kind 0, another address of the same author.
                                    if (i % 5 == 0) add(doc(author, 0, 9_000L + i, "profile"))
                                }
                            }
                        index.putAll(docs)
                        val reference = InMemoryEventIndex().also { it.putAll(docs) }
                        // Half of them asked about, plus some with nothing stored.
                        val asked = authors.filterIndexed { i, _ -> i % 2 == 0 } + List(5) { "%064x".format(10_000 + it) }
                        for (kind in listOf(3, 0)) {
                            val expected = reference.newestVersions(kind, asked)
                            assertEquals(expected, index.newestVersions(kind, asked), "kind $kind")
                        }
                        assertEquals(260, index.newestVersions(3, asked).size, "every asked author with a version, across the chunk")
                    }
                }
            }
    }

    private fun doc(
        author: String,
        kind: Int,
        at: Long,
        salt: String,
    ): EventDoc {
        val id = sha256Hex("$author:$kind:$at:$salt")
        return EventDoc(id = id, pubkey = author, createdAt = at, kind = kind, tags = emptyList(), content = salt, sig = "e".repeat(128), owner = author)
    }

    private fun sha256Hex(s: String): String =
        MessageDigest
            .getInstance("SHA-256")
            .digest(s.toByteArray())
            .joinToString("") { "%02x".format(it) }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
