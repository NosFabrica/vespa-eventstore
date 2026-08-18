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
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
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
 * THE ORPHAN-SCORE SWEEP against a REAL Vespa. The sweep deletes, so the two
 * engine-side facts it rests on cannot be left to the in-memory reference:
 *
 *  - `countByAuthor` is a server-side GROUPING, and its completeness decides
 *    the candidate list. The in-memory index answers it by scanning a map; only
 *    a real Vespa can show the grouping returning every signer over a match set
 *    of thousands (a truncated group list would silently leave orphans behind —
 *    and a grouping that returned the WRONG keys would delete live scores).
 *  - the paged delete loop (`search(author, unranked, limit)` -> `removeDocs`)
 *    must actually shrink the corpus round after round; the loop's own
 *    non-shrinking check fails the run if an acked delete is not visible.
 *
 * The refusal on an unreadable attribution map is asserted FIRST, against a
 * store that holds scores and no 10040 — the mirror-before-lists state, where
 * sweeping would delete the entire score corpus.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class OrphanSweepIT {
    @Test
    fun `the sweep deletes exactly the scores no 10040 attributes`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the orphan sweep IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val host = vespa.host
                val queryUrl = "http://$host:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://$host:${vespa.getMappedPort(CONFIG_PORT)}"

                // Inline projection: every assertion below is about state after a
                // write, and the drain barrier is not what this IT is pinning.
                VespaEventStore
                    .open(url = queryUrl, autoDeploy = true, configUrl = configUrl, deferTrustProjection = false)
                    .use { store ->
                        runBlocking {
                            val cards =
                                (listOf(MAPPED_SERVICE) + ORPHAN_SERVICES).flatMap { service ->
                                    (0 until CARDS_PER_SERVICE).map { n -> card(service, subject(n)) }
                                }
                            cards.chunked(1_000).forEach { store.batchInsert(it) }
                            val total = cards.size
                            awaitCards(store, total)

                            // 1. No 10040 stored: every signer looks orphaned, which is
                            // exactly when the sweep must refuse.
                            val refused = store.sweepOrphanScores()
                            assertTrue(refused.refused, "no attribution readable -> refuse")
                            assertEquals(0, refused.scoresSwept)
                            assertEquals(total, storedCards(store), "the whole corpus survives the refusal")

                            // 2. One observer names ONE service. Every other signer's
                            // cards are now unattributable by anyone.
                            store.insert(providerList())
                            val dry = store.sweepOrphanScores(dryRun = true)
                            assertEquals(ORPHAN_SERVICES.toSet(), dry.orphans.toSet(), "the grouping found every unnamed signer")
                            assertEquals(1 + ORPHAN_SERVICES.size, dry.servicesSeen)
                            assertEquals(ORPHAN_SERVICES.size * CARDS_PER_SERVICE, dry.scoresSwept)
                            assertEquals(total, storedCards(store), "a dry run writes nothing")

                            // 3. The real thing.
                            val swept = store.sweepOrphanScores()
                            assertEquals(ORPHAN_SERVICES.toSet(), swept.orphans.toSet())
                            assertTrue(swept.remapped.isEmpty(), "nothing was claimed mid-sweep")
                            assertEquals(ORPHAN_SERVICES.size * CARDS_PER_SERVICE, swept.scoresSwept)
                            assertEquals(CARDS_PER_SERVICE, storedCards(store), "only the named service's cards remain")
                            assertEquals(
                                CARDS_PER_SERVICE,
                                store.eventIndex.count(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(MAPPED_SERVICE))),
                                "and they are the named service's",
                            )

                            // 4. The projection is untouched by the deletion: an
                            // orphan never held a cell, so every subject still
                            // derives exactly what it derived before.
                            assertTrue(store.verifyTrust().isClean(), "trust view consistent after the sweep")
                            assertTrue(store.sweepOrphanScores().isClean(), "a second sweep finds nothing")
                        }
                    }
            }
    }

    // ------------------------------------------------------------------

    private suspend fun storedCards(store: VespaEventStore) = store.eventIndex.count(EventQuery(kinds = listOf(ContactCardEvent.KIND)))

    /** Poll until every fed card is searchable — the grouping must see the whole corpus. */
    private suspend fun awaitCards(
        store: VespaEventStore,
        expected: Int,
    ) {
        repeat(120) {
            if (storedCards(store) >= expected) return
            delay(500)
        }
        error("card corpus never became searchable ($expected docs)")
    }

    private fun providerList() =
        TrustProviderListEvent(
            id(),
            OBSERVER,
            1_700_000_000L,
            arrayOf(
                arrayOf("30382:rank", MAPPED_SERVICE, "wss://scores.example.com/"),
                arrayOf("30382:followers", MAPPED_SERVICE, "wss://scores.example.com/"),
            ),
            "",
            "",
        )

    private fun card(
        service: String,
        about: String,
    ) = ContactCardEvent(
        id(),
        service,
        1_700_000_000L,
        arrayOf(arrayOf("d", about), arrayOf("rank", "80"), arrayOf("followers", "1000")),
        "",
        "",
    )

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** Enough that the grouping answers over thousands of docs, not a handful. */
        const val CARDS_PER_SERVICE = 500

        val OBSERVER = "c".repeat(64)
        val MAPPED_SERVICE = "1".padStart(64, 'a')
        val ORPHAN_SERVICES = (2..5).map { it.toString().padStart(64, 'a') }

        var seq = 0

        fun id() = (++seq).toString(16).padStart(64, '0')

        fun subject(n: Int) = "f" + n.toString(16).padStart(63, '0')

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
