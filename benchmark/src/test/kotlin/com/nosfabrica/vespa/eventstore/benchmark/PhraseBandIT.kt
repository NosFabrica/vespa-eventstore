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
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * THE PHRASE BAND on a real Vespa (`event.sd` phrase_match(),
 * query(w_phrase_mult)), on the events of the report that motivated it —
 * production's `Pico 8` search, 2026-11-03 (benchmark/rank_cases.json `pico 8`).
 *
 * The fixture is 14 of that export's 160 events, verbatim and signed, each
 * with the trust its author had under the reporting observer's lens. Production
 * put the first two on top:
 *
 *  - a Spanish ETF article: "8%" in its title, "pico" ("peak") deep in its body
 *    — the SPLIT rung, one word of two in a title;
 *  - a nosli list titled "Posts including pic of …" whose body is `#[3] … #[8]`
 *    — "pico" reaching "pic" by one typo, the NEAR rung.
 *
 * Fed through `VespaEventStore.open()`'s own write path (so every search column
 * is what production derives), it pins:
 *
 *  - every detector: the body phrase ("pico-8", "pico 8"), the title phrase (the
 *    wiki page titled "PICO-8"), the joined spelling ("pico8" in a body and as
 *    `#pico8`), and 0 on both intruders;
 *  - the band: every phrase match outranks both intruders, the wiki page — the
 *    title that IS the query, from a trust-32 author — and the Amethyst release
 *    notes (trust 98, the phrase only in a body heading) among them;
 *  - the guard: the title phrase item leaves the wiki page's perfect_match at
 *    1.0 (primary_perfect(); fieldMatch's orderness read 0.833 without it);
 *  - and that the band is what moves the page: with query(w_phrase_mult) = 1
 *    the reported order comes back, both intruders first.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class PhraseBandIT {
    @Test
    fun `a phrase match outranks the query's words scattered or typo'd`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the phrase-band IT")
        val fixture = Json.parseToJsonElement(javaClass.getResource("/pico8_export.json")!!.readText()).jsonObject
        val events = fixture.getValue("events").jsonArray.map { Event.fromJson(it.toString()) }
        val scores = fixture.getValue("scores").jsonObject.mapValues { it.value.jsonPrimitive.int }

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
                        var waited = 0
                        while (store.count(Filter()).toInt() < events.size && waited++ < 120) delay(500)
                        assertEquals(events.size, store.count(Filter()).toInt(), "the whole fixture is admitted")
                    }
                    VespaReputationIndex(queryUrl).use { reputation ->
                        reputation.putAll(scores.map { (pk, rank) -> ReputationDoc(pk, influenceScores = mapOf(ServiceKey(PROVIDER) to rank)) })
                    }
                    awaitTrust(queryUrl, events.size)
                }

                val page = page(queryUrl, "Pico 8")
                val ids = page.map { it.id }
                val intruders = listOf(ETF, NOSLI)
                for (intruder in intruders) {
                    assertEquals(0.0, page.of(intruder).phraseMatch, "$intruder holds the words apart, not the phrase")
                }
                assertEquals(1.0, page.of(WIKI).titlePhrase, "the title PICO-8 holds the phrase")
                assertEquals(1.0, page.of(WIKI).perfectMatch, "the title phrase item must not disturb the title's order factor")
                assertEquals(1.0, page.of(AMETHYST_RELEASE).bodyPhrase, "the release notes say it in a heading")
                assertEquals(1.0, page.of(JOINED_BODY).joinedPhrase, "\"pico8\" in a body is the phrase joined")
                assertEquals(1.0, page.of(JOINED_TAG).joinedPhrase, "#pico8 is the phrase joined")

                val firstIntruder = intruders.minOf { ids.indexOf(it) }
                val phrased = page.filter { it.phraseMatch > 0 }
                assertTrue(phrased.size >= 10, "most of the fixture holds the phrase: ${phrased.size}")
                for (hit in phrased) {
                    assertTrue(ids.indexOf(hit.id) < firstIntruder, "phrase match ${hit.id.take(8)} must outrank both intruders: ${ids.map { it.take(8) }}")
                }
                assertEquals(WIKI, ids.first(), "the page titled the query leads: ${ids.map { it.take(8) }}")

                // Off, the band leaves the page as production served it.
                val off = page(queryUrl, "Pico 8", mapOf("w_phrase_mult" to "1.0")).map { it.id }
                assertEquals(setOf(ETF, NOSLI), off.take(2).toSet(), "without the band the intruders lead again: ${off.map { it.take(8) }}")
            }
    }

    private class Hit(
        val id: String,
        val features: JsonObject,
    ) {
        private fun f(name: String) =
            features
                .getValue(name)
                .jsonPrimitive.content
                .toDouble()

        val phraseMatch get() = f("phrase_match")
        val bodyPhrase get() = f("body_phrase")
        val titlePhrase get() = f("title_phrase")
        val joinedPhrase get() = f("joined_phrase")
        val perfectMatch get() = f("perfect_match")
    }

    private fun List<Hit>.of(id: String) = firstOrNull { it.id == id } ?: error("${id.take(8)} is not on the page: ${map { it.id.take(8) }}")

    /** The `search` profile's page — EventYql's own YQL, as the store sends it for this observer — with its match-features. */
    private fun page(
        queryUrl: String,
        text: String,
        features: Map<String, String> = emptyMap(),
    ): List<Hit> {
        val vq = EventYql.build(EventQuery(search = text, observer = OBSERVER, rankKey = PROVIDER, minRank = 2.0, nowSecs = NOW, limit = 50))!!
        val body =
            buildJsonObject {
                put("yql", vq.yql)
                put("hits", "50")
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
                features.forEach { (k, v) -> put("ranking.features.query($k)", v) }
            }.toString()
        val root =
            Json
                .parseToJsonElement(post(queryUrl, body))
                .jsonObject
                .getValue("root")
                .jsonObject
        assertTrue(root["errors"] == null, "query failed: ${root["errors"]}")
        return root["children"]?.jsonArray.orEmpty().map {
            val fields = it.jsonObject.getValue("fields").jsonObject
            Hit(fields.getValue("id").jsonPrimitive.content, fields.getValue("matchfeatures").jsonObject)
        }
    }

    private fun post(
        queryUrl: String,
        body: String,
    ): String =
        HttpClient
            .newHttpClient()
            .send(
                HttpRequest
                    .newBuilder(URI.create("$queryUrl/search/"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            ).body()

    /** Until every fixture event is visible under the lens: the reputation parent feeds the author scores asynchronously. */
    private suspend fun awaitTrust(
        queryUrl: String,
        expected: Int,
    ) {
        repeat(120) {
            if (page(queryUrl, "Pico 8").count {
                    it.features
                        .getValue("user_score")
                        .jsonPrimitive.content
                        .toDouble() > 0
                } >= expected
            ) {
                return
            }
            delay(500)
        }
        error("the author scores never became visible to ranking")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** The reporting observer and the one provider its kind 10040 names (AGENTS.md, "The live system"). */
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"
        const val PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"

        /** The export's own instant, 2026-11-03T01:02:09Z, so recency reads as it did in production. */
        const val NOW = 1_793_667_729L

        const val ETF = "41712b68a3663c850f77b744f0e12b2f2b957dbbeb75f8127705cf43fac5d7f4"
        const val NOSLI = "a1569a6a0a1ff018d703166162044c8209f5da27d0a863dc11ff553bfd26da19"
        const val WIKI = "8e549fdbb8a998c2f9294c3637ad5eca15dcda60642cf3457b2d46e956d872e4"
        const val AMETHYST_RELEASE = "90b44719853e22e4910581676aa294fa362d5c3655dd56888a6b323643537c89"
        const val JOINED_BODY = "4ff461f3d1c4a226e1c862df68c5d0b0311f8c7dc1e68f2975d77d3239fe95d9"
        const val JOINED_TAG = "4a00c8142864a172ecc03b9328accbc50b0aeefbc87302f903a52f1fd763f8c0"

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
