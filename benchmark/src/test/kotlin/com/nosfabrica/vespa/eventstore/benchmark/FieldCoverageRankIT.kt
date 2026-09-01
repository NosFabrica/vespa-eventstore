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
import com.nosfabrica.vespa.eventstore.engine.ScoredHit
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
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
 * §12.2 OF `event.sd`: WHICH FIELD ANSWERED THE QUERY, on a real Vespa — the
 * one thing that can execute a rank profile.
 *
 * A multi-word query has three ways to land, and before the split rung they
 * were one band. Reported 2026-09-01 (`Verified Human`, the staging People
 * tab): a profile with "Verified" in its display name and "human" in its bio
 * stood above a Trusted List titled exactly `Verified Human` and every member
 * that list vouched for, because `matchCount(name) > 0` bought the whole
 * 130 000 rung for ONE word out of two, and the second phase's <= 89 000 —
 * well under a rung — was all that separated them. ~14% of trust delta then
 * decided it, and the profile's author had it.
 *
 * The ladder this pins, from the same corpus this file seeds (relative
 * relevance at one trust for every author, MEASURED 2026-09-01):
 *
 *              shape                                 before   after
 *     perfect  `Verified Human`                       1.000   1.000
 *     same     `Verified Human Bot`                   0.869   0.787
 *     same     `Human Verified` (right words, wrong   0.817   0.726
 *              order)
 *     split    `Verified` + bio "a human being"       0.812   0.230
 *     split    `DotardTed :verified:` + "humanity"    0.706   0.147
 *
 * ...and, in trust terms: a split match now needs ~1.93x the trust DELTA of a
 * whole-field match to overtake it (one rung, w_split_tier = w_near_tier),
 * where ~1.14x used to do it — a trust-60 author beat a trust-50 perfect
 * match before this and does not now.
 *
 * The tests below assert the ORDER and the two things that must NOT move:
 * a compound handle (`@johncarvalho` for "John Carvalho") stays whole, and a
 * single-word query is untouched — naming_coverage() reads 1.0 on one word,
 * which is why every pin in RankRegressionIT holds.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class FieldCoverageRankIT {
    @Test
    fun `one field answering the query outranks several fields sharing it`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the field-coverage IT")

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
                        index.putAll(SHAPES.mapIndexed { i, s -> s.doc(i + 1) })
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(
                                SHAPES.mapIndexed { i, s -> ReputationDoc(author(i + 1), influenceScores = mapOf(OBSERVER to s.trust)) },
                            )
                        }
                        awaitCorpus(index, SHAPES.size)

                        // ---- the two-word page: the ladder itself ----
                        val page = index.scored("verified human")
                        println("  verified human: " + page.joinToString(" > ") { "${label(it)}[${it.tier}]" })

                        // ONE FIELD ANSWERED, in the query's own order and with
                        // nothing else in it: the top of the band, above the
                        // same words out of order and above a field that holds
                        // more than the query. Read across the shapes that
                        // share one trust — the two that name their own are
                        // the rung's trust test below, and belong to it.
                        // EITHER whole-field answer may lead: `event.sd` sizes
                        // the generic tier so "an exact title/subject token
                        // match ~ an exact name match", and the two measure
                        // 0.01% apart — a bm25 difference between two columns,
                        // not a rung. Pinning one over the other would pin
                        // noise.
                        assertTrue(
                            label(page.first { label(it) !in TRUST_VARIANTS }) in setOf("perfect", "title_perfect"),
                            "a whole-field answer leads the equal-trust page: ${page.map { label(it) }}",
                        )
                        assertOrder(page, "perfect", "extra_token", "the field that IS the query beats the field that merely contains it")
                        assertOrder(page, "perfect", "reversed", "right words, right order beats right words, wrong order")

                        // SEVERAL FIELDS SHARED IT: a rung down, whatever the
                        // second phase thinks of the pieces.
                        assertOrder(page, "reversed", "split_bio", "a whole-field answer outranks a name+bio split")
                        assertOrder(page, "extra_token", "split_nip05", "…and a name+handle split")
                        assertOrder(page, "title_perfect", "title_split", "the generic tier gets the same rung: title vs title+body")
                        assertEquals("split", tier(page, "split_bio"), "a split match reports its own band")
                        assertEquals("split", tier(page, "dotard"), "…including the shape that was reported")
                        assertEquals("name", tier(page, "perfect"), "a whole-field answer stays in the token band")

                        // THE RUNG, IN TRUST: 30 points of trust delta no longer
                        // buy a split match a whole-field answer's place, where
                        // 10 used to. An OVERWHELMING advantage still does —
                        // that is what a rung means, not a wall.
                        assertOrder(page, "perfect", "split_bio_t80", "a rung is worth more than a 30-point trust advantage")
                        assertOrder(page, "split_bio_t100", "perfect", "…and less than a 50-point one: the rung is crossable")

                        // WHAT MUST NOT MOVE (1): a COMPOUND handle answers the
                        // whole query with one term against a one-token field,
                        // so its naming_coverage() reads 0.5 like a split — and
                        // only "no other kind of column matched" tells them
                        // apart. Demote this and "John Carvalho" stops finding
                        // @johncarvalho.
                        assertOrder(page, "compound", "split_bio", "a compound handle is not a split")
                        assertEquals("name", tier(page, "compound"), "…and keeps the token band")
                        assertEquals("name", tier(page, "compound_bio"), "…even with a bio that matched nothing")

                        // WHAT MUST NOT MOVE (2): name and display_name are ONE
                        // naming surface. A doc that spread the query across
                        // them split nothing.
                        assertEquals("name", tier(page, "name_plus_display"), "name + display_name is one surface")

                        // ---- the one-word page: untouched, by construction ----
                        // naming_coverage() is 1.0 whenever query(n_words) is 1,
                        // so the split rung cannot fire and the band is the one
                        // every RankRegressionIT pin was calibrated against.
                        val single = index.scored("verified")
                        println("  verified: " + single.joinToString(" > ") { "${label(it)}[${it.tier}]" })
                        assertTrue(
                            SPLIT_SHAPES.none { tier(single, it) == "split" },
                            "no single-word query may reach the split rung: ${single.map { label(it) to it.tier }}",
                        )
                        assertEquals("name", tier(single, "split_bio"), "an exact one-word name match is a name match")
                        assertOrder(single, "split_bio", "compound", "…and outranks the near-tier prefix hit it beat before")
                    }
                }
            }
    }

    // ------------------------------------------------------------------

    private suspend fun VespaEventIndex.scored(text: String): List<ScoredHit> = searchScored(EventQuery(search = text, observer = OBSERVER, minRank = 2.0, nowSecs = NOW, limit = 50))

    private fun label(hit: ScoredHit): String =
        SHAPES
            .getOrNull(
                hit.doc.id
                    .toLong(16)
                    .toInt() - 1,
            )?.label ?: hit.doc.id.take(8)

    private fun tier(
        page: List<ScoredHit>,
        label: String,
    ): String? = page.firstOrNull { label(it) == label }?.tier

    /** [first] must rank strictly above [second], with the whole page in the message when it does not. */
    private fun assertOrder(
        page: List<ScoredHit>,
        first: String,
        second: String,
        why: String,
    ) {
        val a = page.indexOfFirst { label(it) == first }
        val b = page.indexOfFirst { label(it) == second }
        assertTrue(a >= 0, "$first missing from the page: ${page.map { label(it) }}")
        assertTrue(b >= 0, "$second missing from the page: ${page.map { label(it) }}")
        assertTrue(a < b, "$why — expected $first above $second, got ${page.map { "${label(it)}(${"%.3g".format(it.relevance)})" }}")
    }

    private data class Shape(
        val label: String,
        val trust: Int = 50,
        val name: String? = null,
        val displayName: String? = null,
        val about: String? = null,
        val nip05: String? = null,
        val primary: String? = null,
        val text: String? = null,
        val kind: Int = 0,
    ) {
        fun doc(n: Int) =
            EventDoc(
                id = n.toString(16).padStart(64, '0'),
                pubkey = author(n),
                createdAt = NOW - 86_400L * 30,
                kind = kind,
                tags = emptyList(),
                content = """{"name":"${name.orEmpty()}"}""",
                sig = "e".repeat(128),
                search = SearchFields(name = name, displayName = displayName, about = about, nip05 = nip05, primary = primary, text = text),
            )
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** A hex-64 lens: EventYql only emits user_q/min_rank for a well-formed observer, and a bad one saturates wot_mult(). */
        val OBSERVER = "0b5e".repeat(16)

        /** Pinned so recency_mult() is a constant across the corpus — every shape is stamped the same age. */
        const val NOW = 1_780_000_000L

        fun author(n: Int) = "%064x".format(n * 7919L)

        /**
         * ONE TRUST FOR EVERY AUTHOR except where the label says otherwise, so
         * the page reads as a pure text ladder — wot_mult() is a constant
         * factor and cancels out of every comparison but the three that name a
         * trust deliberately.
         */
        val SHAPES =
            listOf(
                // one field answers the whole query
                Shape("perfect", name = "Verified Human"),
                Shape("reversed", name = "Human Verified"),
                Shape("extra_token", name = "Verified Human Bot"),
                Shape("long_name", name = "The Official Verified Human Registry of Nostr"),
                Shape("compound", name = "VerifiedHuman"),
                Shape("compound_bio", name = "VerifiedHuman", about = "a bio that answers nothing"),
                Shape("name_plus_display", name = "Verified", displayName = "Human"),
                Shape("title_perfect", primary = "Verified Human", kind = 30392),
                Shape("title_long", primary = "Verified Human Trusted List of Nostr", kind = 30392),
                // several fields share it
                Shape("split_bio", name = "Verified", about = "a human being who posts"),
                Shape("split_nip05", name = "Human", nip05 = "me@verified-nostr.com"),
                Shape("dotard", name = "DotardTed :verified:", about = "raw humanity by combining fair use footage"),
                Shape("title_split", primary = "Verified list", text = "for every human", kind = 30392),
                // no naming field at all — the affiliation/body floor
                Shape("bio_only", name = "Somebody", about = "verified human, honest"),
                Shape("body_only", text = "a verified human wrote this note", kind = 1),
                // the same split shape, up the trust scale: where the rung bends
                Shape("split_bio_t80", trust = 80, name = "Verified", about = "a human being who posts"),
                Shape("split_bio_t100", trust = 100, name = "Verified", about = "a human being who posts"),
            )

        /** The two shapes that carry their own trust: they answer the "how big is the rung" question, not the text one. */
        val TRUST_VARIANTS = listOf("split_bio_t80", "split_bio_t100")

        /** Every shape whose words really are scattered — none of them may reach the split rung on a one-word query. */
        val SPLIT_SHAPES = listOf("split_bio", "split_nip05", "dotard", "title_split", "split_bio_t80", "split_bio_t100")

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)

        /** Poll until the whole corpus is searchable — an acked put is visible to search, but the feed is still async. */
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
