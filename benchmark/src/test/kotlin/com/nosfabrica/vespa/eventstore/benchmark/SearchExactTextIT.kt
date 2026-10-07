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

                        // ---- phrases Vespa would refuse (PhraseRuns) ----

                        // The container's InputCheckingSearcher answers a
                        // phrase repeating one term more than five times in a
                        // row, or more than ten times anywhere, with an HTTP
                        // 400 for the whole query. Every query below used to be
                        // that 400 (or would have been), all reachable from a
                        // search box; "feeeeeeeeed" is a word from a real
                        // kind-31890 title.
                        val said = (1..11).joinToString(" ") { "the w$it" }
                        val runs = profile(6, name = "erin", about = "the feeeeeeeeed says no no no no no no")
                        val laugh = profile(7, name = "frank", about = "${"ha".repeat(12)} $said")
                        val coffee = profile(8, name = "gina", about = "gm ☕☕☕☕☕☕ friends")
                        index.putAll(listOf(runs, laugh, coffee))
                        awaitCorpus(index, 8)
                        assertEquals(
                            listOf(runs.id),
                            index.search(EventQuery(search = "feeeeeeeeed")).map { it.id },
                            "eight e's: no body phrase, and the word still finds its doc",
                        )
                        assertEquals(
                            listOf(laugh.id),
                            index.search(EventQuery(search = "ha".repeat(12))).map { it.id },
                            "eleven alternating hah grams: no body phrase, and the word still finds its doc",
                        )
                        assertEquals(
                            listOf(runs.id),
                            index.search(EventQuery(phrases = listOf("no no no no no no no"))).map { it.id },
                            "a phrase of seven repeated words runs as the five Vespa accepts",
                        )
                        assertEquals(
                            listOf(laugh.id),
                            index.search(EventQuery(phrases = listOf(said))).map { it.id },
                            "eleven the's: split into required pieces, still exact within each",
                        )
                        assertEquals(
                            emptyList(),
                            index.search(EventQuery(phrases = listOf(said.replace("w11", "w12")))).map { it.id },
                            "…and every piece is required",
                        )
                        assertEquals(
                            emptyList(),
                            index.search(EventQuery(search = "erin", notSearch = listOf("no-no-no-no-no-no"))).map { it.id },
                            "an exclusion tokenizing into six repeated words is cut the same way, and drops the doc",
                        )
                        assertEquals(
                            setOf(pamplona.id, model.id),
                            index.search(EventQuery(search = "vitor", notSearch = listOf("no-no-no-no-no-no"))).map { it.id }.toSet(),
                            "…and leaves the docs that never say it",
                        )
                        assertEquals(
                            emptyList(),
                            index.search(EventQuery(search = "frank", notSearch = listOf(said.replace(' ', '-')))).map { it.id },
                            "an exclusion in pieces — !(a and b) — drops the doc holding all of them",
                        )
                        // Each emoji is a word to Vespa's query tokenizer: six in
                        // a row is the same refusal, so the phrase runs cut to five.
                        index.search(EventQuery(phrases = listOf("gm ☕☕☕☕☕☕")))
                        index.search(EventQuery(search = "gina", notSearch = listOf("lol😂😂😂😂😂😂")))
                        // ---- emoji are indexed; ₿ and a prefix star are not words ----

                        // Vespa 8.763 indexes every other-symbol code point as a
                        // term of its own (IndexableChars): an emoji is searched
                        // for, alone or beside words, and excluded.
                        val zap = profile(9, name = "hal", about = "zap⚡ sent with ❤️")
                        val plainHeart = profile(10, name = "ivy", about = "made with ❤ and 🔥")
                        index.putAll(listOf(zap, plainHeart))
                        awaitCorpus(index, 10)
                        assertEquals(listOf(zap.id), index.search(EventQuery(search = "⚡")).map { it.id }, "an emoji alone is a search")
                        assertEquals(listOf(zap.id), index.search(EventQuery(search = "hal ⚡")).map { it.id })
                        assertEquals(
                            setOf(zap.id, plainHeart.id),
                            index.search(EventQuery(search = "❤️")).map { it.id }.toSet(),
                            "the variation selector is stripped, so ❤️ finds the plain heart too",
                        )
                        assertEquals(
                            listOf(zap.id),
                            index.search(EventQuery(search = "❤", notSearch = listOf("🔥"))).map { it.id },
                            "-🔥 excludes: the emoji is in the index",
                        )
                        // A currency sign is in no index: dropped, never a 400.
                        assertEquals(listOf(zap.id), index.search(EventQuery(search = "hal ₿")).map { it.id })
                        assertEquals(emptyList(), index.search(EventQuery(search = "₿")).map { it.id })
                        // A trailing star was prefix syntax, an HTTP 400 on an index
                        // field; stripped, the word keeps its prefix reach anyway.
                        assertEquals(listOf(zap.id), index.search(EventQuery(search = "hal*")).map { it.id })

                        // The selector is a combining mark: glued to a digit or a
                        // letter it is PART of the indexed word ("1️", "️zap"), and
                        // stripping it there lost these notes — only a lone one goes.
                        val keycap = profile(11, name = "jo", about = "1️⃣ first")
                        val glued = profile(12, name = "kai", about = "⚡️zap glued")
                        val hearts = profile(13, name = "lu", about = "❤️❤️ double")
                        val star = profile(14, name = "max", about = "f*ck star")
                        val thumbs = profile(15, name = "ned", about = "👍 nice")
                        index.putAll(listOf(keycap, glued, hearts, star, thumbs))
                        awaitCorpus(index, 15)
                        assertEquals(listOf(keycap.id), index.search(EventQuery(search = "1️⃣")).map { it.id })
                        assertEquals(listOf(glued.id), index.search(EventQuery(search = "⚡️zap")).map { it.id })
                        // In a run the selectors sit BETWEEN the hearts, so the
                        // exclusion rides as typed and still drops the note.
                        assertEquals(emptyList(), index.search(EventQuery(search = "lu", notSearch = listOf("❤️❤️"))).map { it.id })
                        // A star inside a word separates, as the indexer did.
                        assertEquals(listOf(star.id), index.search(EventQuery(search = "f*ck")).map { it.id })
                        // A skin tone is ignored: 👍🏽 finds the plain 👍.
                        assertEquals(listOf(thumbs.id), index.search(EventQuery(search = "👍🏽")).map { it.id })

                        // A quote character inside a positive word turned the rest
                        // of it into a phrase: stripped, these are plain words again.
                        index.search(EventQuery(search = "x\"no-no-no-no-no-no"))
                        index.search(EventQuery(search = "“ha-ha-ha-ha-ha-ha”"))
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
