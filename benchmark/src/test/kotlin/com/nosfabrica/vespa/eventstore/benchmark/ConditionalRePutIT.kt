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
import kotlin.test.assertTrue

/**
 * `VespaEventIndex.putAllIfStored`, the reindex's re-put (#160), against a REAL Vespa: one
 * test-and-set put per doc, conditioned on `event.id == "<id>"` and never created. The mock
 * models the condition; only the engine can show what it does with a document that is gone,
 * and with one whose docid now holds a newer event (address-keyed storage).
 *
 * Measured by hand first on Vespa 8.781.11: 412 for a missing document and for one holding
 * another event, nothing written in either case; 200 and written when the id matches.
 */
@Tag("integration")
class ConditionalRePutIT {
    @Test
    fun `a re-put lands only where its event is still stored, id-keyed and address-keyed`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the conditional re-put IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}").deployIfAbsent(queryUrl)
                // Both client paths: batched through the bundled handler, which passes the
                // condition through its JSON feed reader, and one test-and-set put per request.
                var pass = 0
                for (addressKeyed in listOf(false, true)) {
                    for (feedBatch in listOf(100, 1)) {
                        VespaEventIndex(queryUrl, feedBatch = feedBatch, addressKeyed = addressKeyed).use { index ->
                            runBlocking {
                                val batchesBefore = index.feedBatchesSent
                                check(index, addressKeyed, feedBatch, pass++)
                                if (feedBatch > 1) assertTrue(index.feedBatchesSent > batchesBefore, "the batched pass went through the handler")
                            }
                        }
                    }
                }
            }
    }

    private suspend fun check(
        index: VespaEventIndex,
        addressKeyed: Boolean,
        feedBatch: Int,
        pass: Int,
    ) {
        // Distinct ids and authors per pass, so the passes share a cluster without meeting.
        val base = pass * 0x100
        val author = (pass + 1).toString(16).padStart(64, 'c')
        val kept = note(base + 1, "kept", author)
        val deleted = note(base + 2, "deleted", author)
        val oldProfile = profile(base + 3, createdAt = 1_700_000_000L)
        val newProfile = profile(base + 4, createdAt = 1_700_000_100L, author = oldProfile.pubkey)
        index.putAll(listOf(kept, deleted))
        index.putIfNewer(oldProfile)

        // What a reindex page read, before anything moved.
        val page = listOf(kept, deleted, oldProfile).map { it.copy(search = SearchFields(text = "rederived ${it.content}")) }

        // Since the read: one note deleted, the profile superseded.
        index.remove(deleted.id)
        index.putIfNewer(newProfile)

        val written = index.putAllIfStored(page)
        val layout = (if (addressKeyed) "address-keyed" else "id-keyed") + if (feedBatch > 1) ", batched" else ", one per request"
        assertEquals(setOf(kept.id), written, "$layout: only the still-stored note is written")
        // Read back through search, not get: under address-keying get resolves by a search
        // summary, which does not carry the search columns.
        assertEquals(
            listOf(kept.id),
            index.search(EventQuery(kinds = listOf(1), authors = listOf(kept.pubkey), search = "rederived")).map { it.id },
            "$layout: the kept note carries the re-derived column",
        )
        assertEquals(null, index.get(deleted.id), "$layout: the deleted note was not brought back")
        assertEquals(
            listOf(newProfile.id),
            index.search(EventQuery(kinds = listOf(0), authors = listOf(oldProfile.pubkey))).map { it.id },
            "$layout: the superseded profile was not re-put beside, or over, its replacement",
        )
    }

    private fun note(
        n: Int,
        content: String,
        author: String,
    ) = EventDoc(
        id = (n + 1).toString(16).padStart(64, '0'),
        pubkey = author,
        createdAt = 1_700_000_000L + n,
        kind = 1,
        tags = emptyList(),
        content = content,
        sig = "e".repeat(128),
        search = SearchFields(text = content),
    )

    private fun profile(
        n: Int,
        createdAt: Long,
        author: String = (n + 1).toString(16).padStart(64, 'b'),
    ) = EventDoc(
        id = (n + 1).toString(16).padStart(64, '0'),
        pubkey = author,
        createdAt = createdAt,
        kind = 0,
        tags = emptyList(),
        content = """{"name":"profile $n"}""",
        sig = "e".repeat(128),
    )

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
