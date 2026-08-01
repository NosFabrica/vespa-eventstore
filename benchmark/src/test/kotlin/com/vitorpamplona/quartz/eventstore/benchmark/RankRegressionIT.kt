/*
 * Copyright (c) 2026 Vitor Pamplona
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

import com.vitorpamplona.quartz.eventstore.store.SchemaDeployer
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.SearchFields
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
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
 * The SEARCH-QUALITY gate against a REAL Vespa: deploy the bundled schema,
 * feed the canonical regression corpus, and assert where each expected profile
 * lands and through which match TIER it arrived. Every case here is a query
 * shape that was silently broken before the near-tier work (or a bound that
 * must not regress); the mock engine cannot cover any of this — it does not
 * rank — and this IT is also what proves the schema's rank profiles (the
 * second-phase fieldMatch features included) actually DEPLOY.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class RankRegressionIT {
    // One profile per interesting shape. Ids are sequential; names carry the shape.
    private val corpus =
        listOf(
            profile(1, name = "Ode"),
            profile(2, name = "ODELL"),
            profile(3, name = "Odessa"),
            profile(4, name = "model"),
            profile(5, name = "code"),
            profile(6, name = "VitorPamplona"),
            profile(7, name = "Vitor Pamplona"),
            profile(8, name = "Vitor-Pamplona"),
            profile(9, name = "BitcoinMemeTreasury"),
            profile(10, name = "CoffeeLover"),
            profile(11, name = "CITADELDISPATCH"),
            profile(12, name = "José"),
            profile(13, name = "中村太郎"),
            profile(14, name = "Ode Fan Club"),
            profile(15, name = "podcaster", about = "a show hosted by ODELL every week"),
            profile(16, name = "coffee"),
            // A note whose hashtags ride search_secondary — the weak tier's prefix reach.
            doc(17, kind = 1, search = SearchFields(primary = "morning thoughts", secondary = "#bitcoin #nostr")),
        )

    @Test
    fun `prefix, typo, infix, folded and CJK shapes land in the right tier and order`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the rank regression IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"

                // deployIfAbsent proves the schema — including the second-phase
                // fieldMatch features — is ACCEPTED by a real config server.
                SchemaDeployer(configUrl).deployIfAbsent(queryUrl)
                val index = VespaEventIndex(queryUrl)
                index.use {
                    runBlocking {
                        corpus.forEach { index.put(it) }
                        awaitSearchable(index)

                        // --- the original outage shapes ---
                        expect("odell", top = "ODELL", tier = "name")
                        expect("Ode", top = "Ode", tier = "name")
                        expect("Ode", has = "ODELL", tier = "near")
                        expect("Odel", top = "ODELL", tier = "near")
                        expect("VitorP", has = "VitorPamplona", tier = "near")
                        expect("meme", has = "BitcoinMemeTreasury", tier = "near")
                        expect("lover", has = "CoffeeLover", tier = "near")

                        // --- the noise the old OR net let in must stay out ---
                        absent("Ode", "model")
                        absent("Ode", "code")
                        absent("Odel", "CITADELDISPATCH")

                        // --- bounded infix (the weak tier) ---
                        expect("dell", has = "ODELL", tier = "weak")

                        // --- diacritic folding + compound-name variants ---
                        expect("jose", has = "José", tier = "near")
                        expect("vitorp", has = "Vitor Pamplona", tier = "near")
                        expect("vitorp", has = "Vitor-Pamplona", tier = "near")

                        // --- CJK: 2-char floor + run suffixes ---
                        expect("中村", has = "中村太郎", tier = "near")
                        expect("太郎", has = "中村太郎", tier = "near")

                        // --- hashtag prefix reach (search_secondary_tokens) ---
                        expect("bitco", hasId = id(17), tier = "weak")

                        // --- tier order: name beats near beats affiliation ---
                        val odell = search("odell")
                        val names = odell.map { nameOf(it.doc) }
                        assertTrue(
                            names.indexOf("ODELL") < names.indexOf("podcaster"),
                            "the account named ODELL must beat the bio mention: $names",
                        )
                        assertEquals("affiliation", odell.first { nameOf(it.doc) == "podcaster" }.tier)

                        // --- exactness (second phase): the whole-field match wins its band ---
                        val ode = search("Ode").map { nameOf(it.doc) }
                        assertTrue(
                            ode.indexOf("Ode") < ode.indexOf("Ode Fan Club"),
                            "exact whole-name must beat the longer name in the same band: $ode",
                        )

                        // --- typo bound: over-budget hits never match ---
                        absent("odelll", "Odessa")
                    }
                }
            }
    }

    // ------------------------------------------------------------------

    private lateinit var indexRef: VespaEventIndex

    private suspend fun awaitSearchable(index: VespaEventIndex) {
        indexRef = index
        // Feeds are visible sub-second; poll for the last doc to be searchable.
        repeat(60) {
            if (index.search(EventQuery(search = "coffee")).isNotEmpty()) return
            delay(500)
        }
        error("corpus never became searchable")
    }

    private suspend fun search(text: String) = indexRef.searchScored(EventQuery(search = text, ranking = EventYql.RANK_TEXT))

    private suspend fun expect(
        query: String,
        top: String? = null,
        has: String? = null,
        hasId: String? = null,
        tier: String? = null,
    ) {
        val hits = search(query)
        top?.let { assertEquals(it, nameOf(hits.first().doc), "top hit for \"$query\": ${hits.map { h -> nameOf(h.doc) }}") }
        val wanted = has ?: top
        val hit =
            when {
                hasId != null -> hits.firstOrNull { it.doc.id == hasId }
                wanted != null -> hits.firstOrNull { nameOf(it.doc) == wanted }
                else -> null
            }
        assertTrue(hit != null, "\"$query\" must recall ${wanted ?: hasId}: ${hits.map { h -> nameOf(h.doc) }}")
        tier?.let { assertEquals(it, hit.tier, "\"$query\" -> ${wanted ?: hasId} arrived through the wrong tier") }
    }

    private suspend fun absent(
        query: String,
        name: String,
    ) {
        val hits = search(query).map { nameOf(it.doc) }
        assertTrue(name !in hits, "\"$query\" must NOT recall $name (unbounded-matcher noise): $hits")
    }

    /** The doc's name, resolved by id from the seeded corpus (summaries don't carry search fields). */
    private fun nameOf(doc: EventDoc): String = corpus.first { it.id == doc.id }.search.name ?: doc.id

    private fun id(n: Int) = n.toString(16).padStart(64, '0')

    private fun profile(
        n: Int,
        name: String,
        about: String? = null,
    ) = doc(n, kind = 0, search = SearchFields(name = name, about = about))

    private fun doc(
        n: Int,
        kind: Int,
        search: SearchFields,
    ) = EventDoc(
        id = id(n),
        pubkey = "a1".repeat(32),
        createdAt = 1_700_000_000L + n,
        kind = kind,
        tags = emptyList(),
        content = "",
        sig = "e".repeat(128),
        search = search,
    )

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
