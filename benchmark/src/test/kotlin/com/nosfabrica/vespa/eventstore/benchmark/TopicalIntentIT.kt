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

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
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
 * THE TOPICAL INTENT end to end on a real Vespa (event.sd `topical_perfect_drop()`,
 * store `search/QueryIntent`, docs/embedding-experiment.md §8.5 and §10), through
 * the store's own read path: the observer's kind 10040 resolves the lens, the
 * probe asks whether a notable entity is named the query, and the `search`
 * profile ranks with the answer.
 *
 * The fixture is 55 production events from three topics' pages (captured
 * 2026-11-25, verbatim and signed), the reporting observer's 10040, and the
 * provider's kind 30382 cards for all 28 authors, which carry each author's
 * rank AND follower count. It pins:
 *
 *  - `lightning` reads TOPICAL (no profile or app in the fixture is named it), so
 *    the sets and lists TITLED "Lightning" stop leading and an article about
 *    lightning does; with the probe off, a set leads again, as production served it;
 *  - `the art of war` keeps the wiki article titled exactly that in the top 2:
 *    a reference document keeps the rung (reference_kind());
 *  - `odell` reads NAVIGATIONAL (ODELL's profile, 26 041 followers), so its page
 *    is the page the store served before the intent existed.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class TopicalIntentIT {
    @Test
    fun `a topical query stops ranking entities named the topic first, a navigational one is untouched`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the topical-intent IT")
        val events =
            Json
                .parseToJsonElement(javaClass.getResource("/topical_intent_export.json")!!.readText())
                .jsonArray
                .map { Event.fromJson(it.toString()) }

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                runBlocking {
                    VespaEventStore.open(queryUrl, configUrl = configUrl).use { store ->
                        store.batchInsert(events)
                        store.awaitTrustProjection()
                        var waited = 0
                        while (page(store, "odell").none { it.id == ODELL } && waited++ < 120) delay(500)

                        val lightning = page(store, "lightning")
                        val firstReference = lightning.indexOfFirst { it.kind in REFERENCE_KINDS }
                        assertEquals(0, firstReference, "an article about lightning leads: ${lightning.describe()}")
                        for (set in LIGHTNING_SETS) {
                            val at = lightning.indexOfFirst { it.id == set }
                            assertTrue(at == -1 || at > firstReference, "the set titled Lightning (${set.take(8)}) is below the first article: ${lightning.describe()}")
                        }

                        val artOfWar = page(store, "the art of war")
                        assertTrue(artOfWar.take(2).any { it.id == ART_OF_WAR_WIKI }, "the wiki page titled the query keeps the rung: ${artOfWar.describe()}")

                        val odell = page(store, "odell")
                        assertEquals(ODELL, odell.first().id, "ODELL leads his own name: ${odell.describe()}")

                        VespaEventStore.open(queryUrl, configUrl = configUrl, tuning = EngineTuning(intentProbe = false)).use { off ->
                            val before = page(off, "lightning")
                            assertTrue(before.first().kind in SET_KINDS, "without the probe a set named Lightning leads, as production served it: ${before.describe()}")
                            assertEquals(page(off, "odell").map { it.id }, odell.map { it.id }, "a navigational query's page is the page as it was")
                        }
                    }
                }
            }
    }

    private suspend fun page(
        store: VespaEventStore,
        text: String,
    ): List<Event> = store.query(Filter(search = "$text observer:$OBSERVER", limit = 20))

    private fun List<Event>.describe() = joinToString(prefix = "[", postfix = "]") { "${it.kind}:${it.id.take(8)}" }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** The reporting observer (AGENTS.md, "The live system"); its 10040 is in the fixture. */
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

        const val ODELL = "dbb082bfa6e295d317d46c829b59b0a7cb971509dfa64c057b6f4e15c4375329"
        const val ART_OF_WAR_WIKI = "790ccfd7e9653858492b4aaf095b752895a3b2cda0dd048dc6623c545886bad0"
        val LIGHTNING_SETS =
            listOf(
                "580996dbdffde930e1ab0dceb34cd2284accd4a0e750a19e0e330fb2a0c32235",
                "9153f9fa3af7fc9ccd9a8a0726806211e5ab375c5649b1ffff7227a06b6f0a3b",
                "c0597eb26f1e424767aa4628ab4af4a6c94f44c66e84bda1b8d5fc19fa491305",
            )

        /** event.sd reference_kind(). */
        val REFERENCE_KINDS = setOf(30023, 30818)

        /** Follow sets, bookmark sets, curation sets. */
        val SET_KINDS = setOf(30000, 30003, 30004)

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
