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
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
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
 * The BODY's partial-word reach (`search_text_gram`) against a REAL Vespa —
 * the only place this can be proven. `InMemoryEventIndex` matches by naive
 * substring, which is a LOOSER superset of every engine matcher, so it answers
 * "found" for both the right and the wrong reason; and `MockVespaEngine` parses
 * the word group as one unit without executing a matcher at all. Neither can
 * tell a phrase from an AND, which is the entire distinction this column rests
 * on.
 *
 * The gap this closes (reported 2026-08-14): a kind 1 reading "Testing post to
 * #vegans group!" — no subject tag, no hashtags, so `search_text` is its ONLY
 * indexed column — was found by "testing" and not by "testin". Body text is
 * exact-token-only without this field: the prefix/fuzzy clauses target the
 * `*_near` ATTRIBUTES, which the body deliberately has none of (an attribute
 * there is filled by every document and models at more than this schema's whole
 * attribute budget — docs/attribute-memory.md), and the AND-gram nets target
 * about/secondary, not the body.
 *
 * Both halves are pinned here, because shipping only the first would have been
 * a regression dressed as a fix:
 *
 *  - RECALL — the reported query now finds the post, at any body length and
 *    with no element cap, because the column is an inverted index rather than a
 *    per-document array.
 *  - PRECISION — the phrase matcher does not invent hits. ANDing the trigrams
 *    independently, which is what every other `*_gram` field in the schema
 *    does, matched "Take a vitamin and open the editor" for the query "vitor"
 *    (`vit` from vitamin, `ito`/`tor` from editor). Measured on bodies not
 *    containing the query word, that shape's false-positive rate climbs to 4.0%
 *    as the body grows to 2000 words; the phrase form measures 0.0% at every
 *    length. A test asserting only recall would pass with all of it intact.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class SearchBodyGramIT {
    @Test
    fun `a partial word reaches the body, and the phrase matcher invents nothing`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the body-gram IT")

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
                        // The reported event, verbatim in shape: content only.
                        val reported = note(1, "Testing post to #vegans group!")
                        // The trap. Holds `vit`, `ito` and `tor` as scattered
                        // trigrams and the word "vitor" nowhere.
                        val trap = note(2, "Take a vitamin and open the editor")
                        val real = note(3, "Vitor Pamplona shipped it")
                        // A body far past NearText.MAX_ELEMENTS (48), with the
                        // target word LAST: an attribute column would be capped
                        // and could never reach it, an inverted index has no cap.
                        val longPost = note(4, LOREM + " bitcoinmaximalism")
                        index.putAll(listOf(reported, trap, real, longPost))
                        awaitCorpus(index, 4)

                        // ---- recall: the reported gap ----

                        assertEquals(
                            listOf(reported.id),
                            index.search(EventQuery(search = "testing")).map { it.id },
                            "the exact token still recalls, unchanged",
                        )
                        assertEquals(
                            listOf(reported.id),
                            index.search(EventQuery(search = "testin")).map { it.id },
                            "THE REPORTED BUG: a partial word must reach the body",
                        )

                        // ---- precision: the AND net's false positive ----

                        assertEquals(
                            listOf(real.id),
                            index.search(EventQuery(search = "vitor")).map { it.id },
                            "the phrase matcher must NOT match 'vitamin … editor'",
                        )

                        // ---- no element cap, unlike an attribute column ----

                        assertEquals(
                            listOf(longPost.id),
                            index.search(EventQuery(search = "bitcoinmaximalis")).map { it.id },
                            "a word past the 48-element near cap must still be reachable",
                        )

                        // ---- the floor: two trigrams, not three ----

                        // A 4-character partial word reaches the body. The
                        // AND-net floor (MIN_AND_GRAMS_TEXT, 3 trigrams) would
                        // have emitted no clause at all here.
                        assertTrue(
                            reported.id in index.search(EventQuery(search = "test")).map { it.id },
                            "a 4-character partial word must reach the body",
                        )

                        // ---- verified boundary property, pinned so it cannot drift ----

                        // Trigrams are generated WITHIN a token, so a phrase
                        // cannot bridge one: `edp`/`dph` are never indexed.
                        val compound = note(5, "Never share your seed phrase with anyone")
                        index.putAll(listOf(compound))
                        awaitCorpus(index, 5)
                        assertEquals(
                            emptyList(),
                            index.search(EventQuery(search = "seedphrase")).map { it.id },
                            "the gram net does not join compounds across a space",
                        )
                    }
                }
            }
    }

    // ------------------------------------------------------------------

    /** A kind 1 whose ONLY indexed column is its content — the reported shape. */
    private fun note(
        n: Int,
        content: String,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = "a".repeat(64),
        createdAt = 1_700_000_000L + n,
        kind = 1,
        tags = emptyList(),
        content = content,
        sig = "e".repeat(128),
        search = SearchFields(text = content),
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

        /** ~90 words, so the target word that follows sits far past the 48-element near cap. */
        val LOREM =
            (1..90).joinToString(" ") { "filler$it" }

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
