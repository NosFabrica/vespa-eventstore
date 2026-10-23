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
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
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
 * `event.sd`'s perfect_share() on a real Vespa — the perfect-match rung damped
 * by how common the query word is AS A NAME (query(perfect_df_lo), measured and
 * confirmed in docs/search-evaluation.md §9). The statistics it reads,
 * queryTermDocumentFrequency and num_docs_indexed, exist only in a real index,
 * so this is the only place the expression can be executed.
 *
 * It pins the three things the shipped behaviour rests on:
 *
 *  - THE SMALL-INDEX GUARD. Under query(perfect_df_min_docs) the share is 1 —
 *    on a 40-document corpus every word is common, and without the guard the
 *    damping would strip the rung from every small relay and every other
 *    ranking IT. That is why RankRegressionIT's single-word pins (amethyst,
 *    Primal, odell) hold with the damping shipped.
 *  - THE DAMPING ITSELF, guard lifted: a word labelling most of the corpus
 *    loses the rung, a rare name keeps it, a multi-word query keeps it however
 *    common its words, and the loss reaches the relevance, not only the feature.
 *  - THE OFF SWITCH: lo >= hi restores the rung exactly.
 *
 * The corpus is sized so the shipped (2e-4, 6e-4) range would call every word
 * common; the damped cases therefore send a range scaled to it (0.1, 0.5).
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class NameFrequencyRungIT {
    @Test
    fun `the perfect-match rung follows how common the word is as a name`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the name-frequency IT")

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
                        // 30 bookmark sets LABELLED "music" (30/40 = 0.75 name a
                        // label word), one profile and one app NAMED "zeus" (one per
                        // field, so 1/40), and eight unrelated notes for the denominator.
                        val labels = (1..30).map { doc(it, kind = 30003, primary = "music") }
                        val zeus = listOf(doc(31, kind = 0, name = "zeus"), doc(32, kind = 32267, primary = "zeus"))
                        val filler = (33..40).map { doc(it, kind = 1, text = "an unrelated note number $it") }
                        index.putAll(labels + zeus + filler)
                        awaitCorpus(index, 40)

                        // THE GUARD: shipped defaults on a 40-document index.
                        val guarded = features(queryUrl, "music")
                        assertTrue(guarded.isNotEmpty(), "the music labels are recalled")
                        guarded.forEach { assertEquals(1.0, it.share, 1e-9, "under perfect_df_min_docs the rung is whole") }

                        // THE DAMPING, guard lifted, range scaled to the corpus.
                        val scaled = mapOf("perfect_df_lo" to 0.1, "perfect_df_hi" to 0.5, "perfect_df_min_docs" to 0.0)
                        val music = features(queryUrl, "music", scaled)
                        music.forEach {
                            assertEquals(0.75, it.ratio, 1e-9, "30 of 40 documents carry `music` in a naming field")
                            assertEquals(0.0, it.share, 1e-9, "a word above hi pays none of the rung")
                        }
                        features(queryUrl, "zeus", scaled).forEach {
                            // The LARGEST single field, not the sum: one `zeus` in
                            // `name`, one in `search_primary`, reads 1/40 — the same
                            // expression the production sample was measured with.
                            assertEquals(0.025, it.ratio, 1e-9, "the most `zeus` any one naming field holds is 1 of 40")
                            assertEquals(1.0, it.share, 1e-9, "a name below lo keeps the whole rung")
                        }
                        features(queryUrl, "music zeus", scaled).forEach {
                            assertEquals(1.0, it.share, 1e-9, "a multi-word query keeps the whole rung")
                        }

                        // ...and it reaches the score: the same perfect-match label
                        // ranks lower damped than whole.
                        val whole = guarded.associate { it.id to it.relevance }
                        music.forEach {
                            assertTrue(it.relevance < whole.getValue(it.id), "damping lowers ${it.id}'s relevance (${it.relevance} vs ${whole.getValue(it.id)})")
                        }

                        // THE OFF SWITCH: lo >= hi.
                        features(queryUrl, "music", scaled + ("perfect_df_hi" to 0.1)).forEach {
                            assertEquals(1.0, it.share, 1e-9, "lo >= hi disables the damping")
                        }
                    }
                }
            }
    }

    private class Hit(
        val id: String,
        val relevance: Double,
        val ratio: Double,
        val share: Double,
    )

    /** The `search` profile's page for [text] — EventYql's own YQL — with [overrides], read back with its match-features. */
    private fun features(
        queryUrl: String,
        text: String,
        overrides: Map<String, Double> = emptyMap(),
    ): List<Hit> {
        val vq = EventYql.build(EventQuery(search = text, observer = OBSERVER, rankKey = OBSERVER, minRank = 0.0, nowSecs = NOW, limit = 50))!!
        val body =
            buildJsonObject {
                put("yql", vq.yql)
                put("hits", "50")
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
                overrides.forEach { (k, v) -> put("ranking.features.query($k)", v.toString()) }
            }.toString()
        val resp =
            HttpClient.newHttpClient().send(
                HttpRequest
                    .newBuilder(URI.create("$queryUrl/search/"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        val root =
            Json
                .parseToJsonElement(resp.body())
                .jsonObject
                .getValue("root")
                .jsonObject
        assertTrue(root["errors"] == null, "query failed: ${root["errors"]}")
        return root["children"]
            ?.jsonArray
            .orEmpty()
            .map { it.jsonObject }
            // Only the documents that actually matched in a naming field read the rung.
            .filter { (it.getValue("fields").jsonObject.getValue("matchfeatures") as JsonObject)["perfect_match"]!!.jsonPrimitive.content.toDouble() > 0.999 }
            .map {
                val fields = it.getValue("fields").jsonObject
                val mf = fields.getValue("matchfeatures").jsonObject
                Hit(
                    id = fields.getValue("id").jsonPrimitive.content,
                    relevance =
                        it
                            .getValue("relevance")
                            .jsonPrimitive.content
                            .toDouble(),
                    ratio =
                        mf
                            .getValue("name_df_ratio")
                            .jsonPrimitive.content
                            .toDouble(),
                    share =
                        mf
                            .getValue("perfect_share")
                            .jsonPrimitive.content
                            .toDouble(),
                )
            }
    }

    private fun doc(
        n: Int,
        kind: Int,
        name: String? = null,
        primary: String? = null,
        text: String? = null,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = "%064x".format(n * 7919L),
        createdAt = NOW - 86_400L * 30,
        kind = kind,
        tags = emptyList(),
        content = """{"name":"${name.orEmpty()}"}""",
        sig = "e".repeat(128),
        search = SearchFields(name = name, primary = primary, text = text),
    )

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        val OBSERVER = "0b5e".repeat(16)

        /** Pinned so recency_mult() is a constant across the corpus. */
        const val NOW = 1_780_000_000L

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)

        suspend fun awaitCorpus(
            index: VespaEventIndex,
            expected: Int,
        ) {
            repeat(120) {
                if (index.count(EventQuery()) >= expected) return
                delay(500)
            }
            error("corpus never became searchable ($expected docs)")
        }
    }
}
