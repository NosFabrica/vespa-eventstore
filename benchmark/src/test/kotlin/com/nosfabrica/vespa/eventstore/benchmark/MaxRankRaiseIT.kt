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
import com.nosfabrica.vespa.eventstore.benchmark.AccessLogIT.Companion.CONFIG_PORT
import com.nosfabrica.vespa.eventstore.benchmark.AccessLogIT.Companion.QUERY_PORT
import com.nosfabrica.vespa.eventstore.benchmark.AccessLogIT.Companion.dockerAvailable
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * The reputation index's two bulk write shapes the mock cannot judge: the
 * CONDITIONAL `max_rank` raise (a test-and-set the engine decides at write
 * time — the backfill's write, which must never lower the scalar below a cell
 * a live raise put there since the page was read) and the pipelined remove.
 * Only a real Vespa executes a condition, so this is the gate for both.
 */
@Tag("integration")
class MaxRankRaiseIT {
    @Test
    fun `a raise moves max_rank up, never down, and only on documents that exist`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the max_rank raise IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}").deployIfAbsent(queryUrl)
                val reputations = VespaReputationIndex(queryUrl)
                reputations.use {
                    runBlocking {
                        val a = "a".repeat(64)
                        val b = "b".repeat(64)
                        val observer = "0b".repeat(32)
                        reputations.put(ReputationDoc(a, mapOf(observer to 40)))
                        reputations.put(ReputationDoc(b, mapOf(observer to 10)))
                        assertEquals(40, reputations.storedMaxRank(a), "a whole-document put stores the cells' maximum")

                        // A live cell raise lands first: the document is at 70.
                        reputations.updateCells(listOf(ReputationCells(a, observer, 70, null, maxRank = 70)))
                        assertEquals(70, reputations.storedMaxRank(a))

                        // The backfill's page-old bound (40) must NOT win — the
                        // condition is not met, and that is a normal outcome, not
                        // an error. A bound above the stored value does win, and a
                        // subject with no document gets none.
                        reputations.raiseMaxRank(mapOf(a to 40, b to 25, "c".repeat(64) to 50))
                        assertEquals(70, reputations.storedMaxRank(a), "a lower floor leaves a raised document alone")
                        assertEquals(25, reputations.storedMaxRank(b), "a higher floor raises")
                        assertNull(reputations.get("c".repeat(64)), "no document is created for a raise")

                        reputations.removeAll(listOf(a, b))
                        assertNull(reputations.get(a))
                        assertNull(reputations.get(b))
                    }
                }
            }
    }
}
