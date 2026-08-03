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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.store.SchemaDeployer
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.SearchFields
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
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

/**
 * The exact-text clauses — `phrases` (the store's `"exact words"` quotes) and
 * `notSearch` (its `-word` minus) — against a REAL Vespa: the engine-side
 * facts the wire mock cannot prove, because its parser accepts by
 * construction whatever EventYql emits:
 *
 *  - the phrase-grammar `userInput` clause is ACCEPTED by the deployed
 *    schema, required and negated alike, along with the exclusion-only
 *    `where true and !(…)` shape (YQL acceptance is exactly what only a real
 *    engine can check);
 *  - Vespa's own tokenizer draws the exact-match line where the in-memory
 *    reference says it should: whole tokens in adjacency match a phrase and
 *    drop an exclusion, substrings do neither, and a punctuated unit
 *    ("e-cash") behaves as the adjacent phrase, not an anywhere-in-doc AND
 *    of its pieces.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class SearchExactTextIT {
    @Test
    fun `phrases require adjacency, exclusions drop exact hits, and both shapes are accepted`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the exact-text IT")

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
                        val pamplona = profile(1, name = "vitor", about = "pamplona dev")
                        val model = profile(2, name = "vitor", about = "model builder")
                        val ecash = profile(3, name = "carol", about = "e-cash rocks")
                        val cashOnly = profile(4, name = "dave", about = "cash first, e later")
                        val unsearchable = note(5)
                        index.putAll(listOf(pamplona, model, ecash, cashOnly, unsearchable))
                        awaitCorpus(index, 5)

                        // Sanity: without the exclusion, both vitors recall.
                        assertEquals(
                            setOf(pamplona.id, model.id),
                            index.search(EventQuery(search = "vitor")).map { it.id }.toSet(),
                        )

                        // The exclusion drops the exact word, wherever it sits.
                        assertEquals(
                            listOf(model.id),
                            index.search(EventQuery(search = "vitor", notSearch = listOf("pamplona"))).map { it.id },
                            "-pamplona must drop the doc whose about carries the word",
                        )

                        // Exact-token only: "ode" is a substring of "model" but
                        // not a token of it — the positive side's looseness
                        // (prefix/fuzzy/grams) must never widen an exclusion.
                        assertEquals(
                            listOf(model.id),
                            index.search(EventQuery(search = "model", notSearch = listOf("ode"))).map { it.id },
                            "a substring exclusion must not drop the doc",
                        )

                        // A punctuated exclusion is the adjacent phrase: "e-cash"
                        // drops the doc where the tokens sit together, keeps the
                        // one where they are scattered.
                        assertEquals(
                            listOf(cashOnly.id),
                            index.search(EventQuery(kinds = listOf(0), search = "cash", notSearch = listOf("e-cash"))).map { it.id },
                            "phrase exclusion: adjacency drops, scattered tokens stay",
                        )

                        // Exclusion-only = the `where true and !(…)` shape: plain
                        // recall minus the word — and a doc with no search fields
                        // holds no word, so it is never excluded.
                        assertEquals(
                            listOf(unsearchable.id, cashOnly.id, ecash.id, model.id).sorted(),
                            index.search(EventQuery(notSearch = listOf("pamplona"))).map { it.id }.sorted(),
                            "exclusion-only recall: everything but the excluded hit",
                        )

                        // ---- the REQUIRED phrase clause, same grammar, positive ----

                        // Adjacent, in order: only the doc whose about reads
                        // "pamplona dev" — not the one with the words apart.
                        assertEquals(
                            listOf(pamplona.id),
                            index.search(EventQuery(phrases = listOf("pamplona dev"))).map { it.id },
                            "a quoted phrase requires adjacency",
                        )
                        assertEquals(
                            emptyList(),
                            index.search(EventQuery(phrases = listOf("dev pamplona"))).map { it.id },
                            "…and order",
                        )

                        // A quoted single word is the fuzzy opt-out: the exact
                        // token matches, its prefix does not.
                        assertEquals(
                            setOf(pamplona.id, model.id),
                            index.search(EventQuery(phrases = listOf("vitor"))).map { it.id }.toSet(),
                        )
                        assertEquals(
                            emptyList(),
                            index.search(EventQuery(phrases = listOf("vito"))).map { it.id },
                            "no prefix/typo reach inside quotes",
                        )

                        // Phrase + loose word + exclusion in one query — the
                        // full clause surface the store can emit at once.
                        assertEquals(
                            listOf(model.id),
                            index
                                .search(EventQuery(search = "vitor", phrases = listOf("model builder"), notSearch = listOf("pamplona")))
                                .map { it.id },
                            "all three text clause kinds compose",
                        )
                    }
                }
            }
    }

    // ------------------------------------------------------------------

    private fun profile(
        n: Int,
        name: String,
        about: String,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = "a".repeat(64),
        createdAt = 1_700_000_000L + n,
        kind = 0,
        tags = emptyList(),
        content = """{"name":"$name","about":"$about"}""",
        sig = "e".repeat(128),
        search = SearchFields(name = name, about = about),
    )

    private fun note(n: Int) =
        EventDoc(
            id = n.toString(16).padStart(64, '0'),
            pubkey = "a".repeat(64),
            createdAt = 1_700_000_000L + n,
            kind = 1,
            tags = emptyList(),
            // The word under exclusion, deliberately: this doc carries NO
            // search fields, so it holds no word and must never be excluded
            // — however loudly its raw content shouts the excluded term.
            content = "pamplona pamplona",
            sig = "e".repeat(128),
        )

    /** Poll until the whole corpus (events fed so far) is searchable. */
    private suspend fun awaitCorpus(
        index: VespaEventIndex,
        expected: Int,
    ) {
        repeat(120) {
            if (index.count(EventQuery()) >= expected) return
            delay(500)
        }
        error("corpus never became searchable ($expected docs)")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
