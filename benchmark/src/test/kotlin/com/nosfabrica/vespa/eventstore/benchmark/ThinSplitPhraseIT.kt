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
 * The two `book of daniel` fixes on a real Vespa (`event.sd`
 * query(w_split_min_coverage), query(w_phrase_pop); docs/search-evaluation.md
 * §9). Reported on production: the unquoted query's whole top 15 was titles
 * holding ONE of its three words — mostly `of` — with the rest scattered
 * through a body, while the page titled exactly `Book of Daniel` and notes
 * quoting the phrase sat far below.
 *
 * With one trust for every author, so the page is a pure text ladder, it pins:
 *
 *  - a title carrying one word of three is a THIN split: it is scored on the
 *    body rung, not the split rung, so a note holding the phrase outranks it;
 *  - a body holding the words adjacent and in order reads body_phrase 1 — which
 *    depends on EventYql's rank-only phrase item, since the word groups put
 *    synthetic joined pairs between the typed words — and the scattered one 0;
 *  - the title that IS the query still leads;
 *  - a TWO-word query is untouched: one word of two is coverage 0.5, the floor;
 *  - a PUNCTUATED word is several tokens: on `don't trust` a body saying
 *    "don't" with "trust" elsewhere is not the phrase (EventYql's
 *    phraseTokens; against the word count, 58 of 74 real matches read as it).
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class ThinSplitPhraseIT {
    @Test
    fun `a thin split falls to the body rung and a body phrase rises above it`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the thin-split IT")

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
                        index.putAll(
                            listOf(
                                // The reported shape: `of` in the title, the rest of the query in a long body.
                                doc(1, kind = 30023, primary = "The Praxeology of Privacy", text = "a book chapter on privacy, read aloud by Daniel at the meetup"),
                                // A note quoting the phrase.
                                doc(2, kind = 1, text = "last night I read the book of Daniel again, the dreams chapter"),
                                // The page whose title IS the query.
                                doc(3, kind = 30818, primary = "Book of Daniel", text = "The Book of Daniel is a biblical apocalypse"),
                                // For the two-word check: one of two words in a title, the other in the body.
                                doc(4, kind = 30023, primary = "Book Signing", text = "signed copies, hosted by Daniel"),
                                // For the punctuated query: "don't" without the phrase, then with it.
                                doc(5, kind = 1, text = "I don't know who to trust any more"),
                                doc(6, kind = 1, text = "rule one: don't trust, verify"),
                            ),
                        )
                        awaitCorpus(index, 6)

                        val page = page(queryUrl, "book of daniel")
                        val ids = page.map { it.id }
                        assertEquals(1.0, page.first { it.id == id(1) }.thinSplit, "one word of three in a title is a thin split")
                        assertEquals(1.0, page.first { it.id == id(2) }.bodyPhrase, "the note holds the phrase")
                        assertEquals(0.0, page.first { it.id == id(1) }.bodyPhrase, "scattered words are not the phrase")
                        assertEquals(id(3), ids.first(), "the title that IS the query leads: $ids")
                        assertTrue(ids.indexOf(id(2)) < ids.indexOf(id(1)), "the phrase note outranks the thin split: $ids")

                        val two = page(queryUrl, "book daniel")
                        assertEquals(0.0, two.first { it.id == id(4) }.thinSplit, "one word of two is the floor itself, not thin")

                        val punctuated = page(queryUrl, "don't trust")
                        assertEquals(0.0, punctuated.first { it.id == id(5) }.bodyPhrase, "`don t` alone is two tokens of three, not the phrase")
                        assertEquals(1.0, punctuated.first { it.id == id(6) }.bodyPhrase, "the body holding `don't trust` is the phrase")
                    }
                }
            }
    }

    private class Hit(
        val id: String,
        val thinSplit: Double,
        val bodyPhrase: Double,
    )

    /** The `search` profile's page — EventYql's own YQL, shipped defaults — with its match-features. */
    private fun page(
        queryUrl: String,
        text: String,
    ): List<Hit> {
        val vq = EventYql.build(EventQuery(search = text, observer = OBSERVER, rankKey = OBSERVER, minRank = 0.0, nowSecs = NOW, limit = 50))!!
        val body =
            buildJsonObject {
                put("yql", vq.yql)
                put("hits", "50")
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
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
        return root["children"]?.jsonArray.orEmpty().map {
            val fields = it.jsonObject.getValue("fields").jsonObject
            val mf = fields.getValue("matchfeatures").jsonObject
            Hit(
                id = fields.getValue("id").jsonPrimitive.content,
                thinSplit =
                    mf
                        .getValue("thin_split")
                        .jsonPrimitive.content
                        .toDouble(),
                bodyPhrase =
                    mf
                        .getValue("body_phrase")
                        .jsonPrimitive.content
                        .toDouble(),
            )
        }
    }

    private fun id(n: Int) = n.toString(16).padStart(64, '0')

    private fun doc(
        n: Int,
        kind: Int,
        primary: String? = null,
        text: String? = null,
    ) = EventDoc(
        id = id(n),
        pubkey = "%064x".format(n * 7919L),
        createdAt = NOW - 86_400L * 30,
        kind = kind,
        tags = emptyList(),
        content = text.orEmpty(),
        sig = "e".repeat(128),
        search = SearchFields(primary = primary, text = text),
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
