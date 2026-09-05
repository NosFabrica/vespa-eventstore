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
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.trust.TrustKeyingMigration
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * SERVICE-KEYED CELLS ON A REAL VESPA (docs/service-keyed-trust.md), through
 * the store's public front door and its real write and read paths:
 *
 *  - the PROVIDER SWAP: an observer's page follows the provider their CURRENT
 *    kind 10040 names, before and after they re-point it — the newly named
 *    provider's stored cards are walked into cells (the one O(cards) reaction
 *    left), and the old provider's cells stay where they are, keyed by it;
 *  - a RETRACTION: a card that lost its rank tag removes its cell in the same
 *    update its other tag lands (a tensor `remove` by address), and a
 *    retraction alone creates no document;
 *  - the KEYING MIGRATION: a document as the old observer-keyed model wrote
 *    it is re-keyed, swept and marked by `open()` itself, on the engine's own
 *    tensor operations, and verifies clean against the exact derive.
 *
 * Tagged `integration`; run with `-Pintegration` where Docker is available.
 */
@Tag("integration")
class ServiceKeyedTrustIT {
    @Test
    fun `a provider swap is a query-time resolution, a retraction is a cell remove, and an old store migrates at open`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the service-keyed trust IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                // Deploy before any client opens a feed connection: the feed
                // client handshakes on construction, and there is nothing to
                // handshake with until the application serves.
                SchemaDeployer(configUrl).deployIfAbsent(queryUrl)
                VespaReputationIndex(queryUrl).use { reputations ->
                    VespaEventStore.open(url = queryUrl, autoDeploy = false, configUrl = configUrl).use { store ->
                        runBlocking {
                            // ---- the swap ------------------------------------------------
                            // Two providers rank the same five authors in OPPOSITE orders;
                            // P2's cards are stored before anyone names P2.
                            store.batchInsert(SUBJECTS.mapIndexed { i, s -> card(P1, s, RANKS[i]) } + SUBJECTS.mapIndexed { i, s -> card(P2, s, RANKS[RANKS.size - 1 - i]) })
                            store.batchInsert(SUBJECTS.mapIndexed { i, s -> note(i, s) })
                            store.insert(list10040(P1, at = 100))
                            store.awaitTrustProjection()
                            assertEquals(SUBJECTS, page(store), "under P1 the page is P1's order")
                            SUBJECTS.forEach { s ->
                                assertEquals(setOf(P1), reputations.get(s)?.influenceScores?.keys, "P2 is named by nobody: its card is dead storage")
                            }

                            store.insert(list10040(P2, at = 200))
                            store.awaitTrustProjection()
                            assertEquals(SUBJECTS.reversed(), page(store), "the same store, the same notes: the page follows the CURRENT list")
                            SUBJECTS.forEachIndexed { i, s ->
                                assertEquals(mapOf(P1 to RANKS[i], P2 to RANKS[RANKS.size - 1 - i]), reputations.get(s)?.influenceScores, "both providers' cells stand, keyed by service")
                            }

                            // ---- the retraction ------------------------------------------
                            val retracted = SUBJECTS[0]
                            store.insert(ContactCardEvent(hexId(), P2, 5_000, arrayOf(arrayOf("d", retracted), arrayOf("followers", "7")), "", ""))
                            store.awaitTrustProjection()
                            val doc = reputations.get(retracted)
                            assertEquals(mapOf(P1 to RANKS[0]), doc?.influenceScores, "P2's rank cell is gone in the same update its followers landed")
                            assertEquals(7.0, doc?.followerCounts?.get(P2))
                            assertEquals(SUBJECTS.reversed().dropLast(1), page(store), "an author the lens no longer ranks leaves the page")

                            val unknown = "9".repeat(64)
                            store.insert(ContactCardEvent(hexId(), P2, 5_001, arrayOf(arrayOf("d", unknown)), "", ""))
                            store.awaitTrustProjection()
                            assertNull(reputations.get(unknown), "a retraction alone creates no document")

                            // A store born on this model: open() found nothing to migrate and marked it.
                            assertFalse(store.awaitTrustKeying().refused)
                            assertNotNull(reputations.get(TrustKeyingMigration.MARKER_KEY))
                        }
                    }

                    // ---- the migration -------------------------------------------
                    // Rewrite one parent as the OLD model wrote it — the observer's key,
                    // none of the services' — and take the marker down: the shape of a
                    // store fed before the change, as the next boot finds it.
                    val legacy = SUBJECTS[4]
                    runBlocking {
                        reputations.put(ReputationDoc(legacy, mapOf(OBSERVER to 55), mapOf(OBSERVER to 3.0)))
                        reputations.remove(TrustKeyingMigration.MARKER_KEY)
                    }
                    VespaEventStore.open(url = queryUrl, autoDeploy = false, configUrl = configUrl).use { reopened ->
                        runBlocking {
                            val migrated = reopened.awaitTrustKeying()
                            assertFalse(migrated.refused)
                            assertTrue(migrated.keysRemoved >= 2, "the observer-keyed cells were swept: $migrated")
                            val fixed = reputations.get(legacy)
                            // P1 is named by nobody since the swap, so only P2's cell comes back:
                            // the migration projects named services and sweeps every other key.
                            assertEquals(mapOf(P2 to RANKS[0]), fixed?.influenceScores, "re-keyed by service, the observer's cells swept")
                            assertFalse(fixed?.followerCounts?.containsKey(OBSERVER) == true)
                            assertNotNull(reputations.get(TrustKeyingMigration.MARKER_KEY), "marked")
                            assertTrue(reopened.verifyTrust().isClean(), "the engine's tensors match the exact derive")
                            assertEquals(SUBJECTS.reversed().dropLast(1), page(reopened), "and the lens serves the same page")
                        }
                    }
                }
            }
    }

    /** The observer's `sort:rank` page over the five notes: authors, best first. */
    private suspend fun page(store: VespaEventStore): List<String> = store.query<Event>(Filter(kinds = listOf(1), limit = 10, search = "observer:$OBSERVER sort:rank")).map { it.pubKey }

    private fun list10040(
        service: String,
        at: Long,
    ) = TrustProviderListEvent(hexId(), OBSERVER, at, arrayOf(arrayOf("30382:rank", service, "wss://scores.test/"), arrayOf("30382:followers", service, "wss://scores.test/")), "", "")

    private fun card(
        service: String,
        subject: String,
        rank: Int,
    ) = ContactCardEvent(hexId(), service, 1_000L + rank, arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString()), arrayOf("followers", "12")), "", "")

    private fun note(
        n: Int,
        author: String,
    ) = TextNoteEvent(hexId(), author, 1_700_000_000L + n, emptyArray(), "note $n", "e".repeat(128))

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        val OBSERVER = "c".repeat(64)
        val P1 = "5e".repeat(32)
        val P2 = "6e".repeat(32)

        /** Five authors; P1 ranks them 90, 70, 50, 30, 10 in this order, P2 the reverse. */
        val SUBJECTS = (1..5).map { it.toString(16).padStart(64, 'b') }
        val RANKS = listOf(90, 70, 50, 30, 10)

        private var seq = 0

        fun hexId() = (++seq).toString(16).padStart(64, 'a')

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
