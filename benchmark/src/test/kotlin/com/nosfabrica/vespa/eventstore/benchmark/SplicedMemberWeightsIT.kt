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
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
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
 * A MEMBER'S CONFIDENCE, CARRIED BY ITS OWN KEY — the half of `spliced_member`
 * that only a real Vespa executes.
 *
 * The store used to group members by quantized confidence and pay a round trip
 * per group, because a rank feature is a property of the QUERY. A weighted
 * recall moves the number onto the key: `dotProduct(pubkey, {member: 0..100})`
 * recalls exactly those members and hands the ranking each one's own weight
 * back as `rawScore(pubkey)`. That is the claim this file pins, and it is not
 * one the in-memory reference can make good on — it MODELS the arithmetic, it
 * does not execute Vespa's.
 *
 * Two things are checked because two things could break independently:
 *
 *  - RECALL is exactly the key set, a zero weight included (a publisher's
 *    honest 0 must still bring its member, or the page loses a person for
 *    saying something true about them);
 *  - the RANKING reads the weights back per document, so one query orders a
 *    whole list by what its publisher said — descending confidence, unrounded.
 *
 * The operator matters and is measured rather than assumed: `weightedSet`
 * recalls the same rows on a single-value attribute and leaves `rawScore` at 0,
 * which would silently place every member at zero confidence. `EventYql` emits
 * `dotProduct` for that reason, and this test is what would catch a change of
 * mind about it.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class SplicedMemberWeightsIT {
    @Test
    fun `a weighted recall brings every key and ranks each member by its own confidence`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the spliced-member weights IT")

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
                        // Five people a list names, plus one it does not: the
                        // sixth is what proves the recall is the KEY SET and not
                        // "every profile the query could admit".
                        val profiles = MEMBERS.map { (key, _) -> profile(key) } + profile(OUTSIDER)
                        index.putAll(profiles)
                        awaitCorpus(index, profiles.size)

                        val weighted =
                            EventQuery(
                                kinds = listOf(0),
                                authorWeights = MEMBERS.toMap(),
                                ranking = EventYql.RANK_SPLICED_MEMBER,
                                // No observer: wot_mult() is then a constant for
                                // everyone (the profile pins min_rank to 0 for
                                // exactly this case), so what is left ordering
                                // the page is the confidence — which is the
                                // thing under test.
                                rankFeatures = mapOf(EventYql.F_DOC_CONF to 1.0),
                            )
                        val hits = index.searchRanked(weighted)

                        assertEquals(
                            MEMBERS.map { it.first }.toSet(),
                            hits.map { it.hit.pubkey }.toSet(),
                            "the recall is exactly the weighted keys — the zero-weight member included, the outsider not",
                        )

                        // ONE query, and it comes back ordered by what the
                        // publisher said about each person. Under the old
                        // per-query confidence this needed one round trip per
                        // distinct value.
                        assertEquals(
                            MEMBERS.sortedByDescending { it.second }.map { it.first },
                            hits.sortedByDescending { it.score ?: 0.0 }.map { it.hit.pubkey },
                            "one lookup, every member placed by its own confidence",
                        )

                        // The rung's own arithmetic, end to end: 550 + 3450 x c.
                        // Checked at both ends because a mis-read weight is far
                        // likelier to collapse the span than to shift it.
                        val byKey = hits.associate { it.hit.pubkey to (it.score ?: 0.0) }
                        assertTrue(byKey.getValue(FULL) > 3_900, "full confidence reaches the top of the member band: $byKey")
                        assertTrue(byKey.getValue(NONE) < 600, "and a zero-confidence member sits at its floor: $byKey")

                        // THE FLOOR, which is the reason the pointer's relevance
                        // is a query feature at all: with one sent, every member
                        // is lifted to at least a rung below it, ordered inside
                        // that span by the same confidence.
                        val floored =
                            index.searchRanked(
                                weighted.copy(
                                    rankFeatures =
                                        weighted.rankFeatures +
                                            mapOf(EventYql.F_POINTER_REL to 130_000.0, EventYql.F_SUBJECT_FLOOR_SPAN to 0.1769),
                                ),
                            )
                        val flooredByKey = floored.associate { it.hit.pubkey to (it.score ?: 0.0) }
                        assertEquals(130_000.0, flooredByKey.getValue(FULL), 1.0, "a full-confidence member ties its pointer")
                        assertTrue(
                            flooredByKey.getValue(NONE) in 22_000.0..24_000.0,
                            "and a zero-confidence one lands a rung below it, not on its own rung: $flooredByKey",
                        )
                        assertEquals(
                            MEMBERS.sortedByDescending { it.second }.map { it.first },
                            floored.sortedByDescending { it.score ?: 0.0 }.map { it.hit.pubkey },
                            "the floor reorders nothing inside the block: confidence still orders it",
                        )

                        // THE OTHER KEYED SHAPE. A Trusted List of EVENTS names
                        // its members by id, so the same claim has to hold for
                        // `dotProduct(id, ...)` -> `rawScore(id)`. It is a
                        // separate measurement rather than an inference from the
                        // pubkey case: the two fields are declared identically in
                        // `event.sd`, which is exactly the kind of "should behave
                        // the same" that only a real engine settles. (These
                        // fixtures use the pubkey as the id, so one corpus proves
                        // both.)
                        val byIdWeight =
                            index.searchRanked(
                                weighted.copy(authorWeights = emptyMap(), idWeights = MEMBERS.toMap()),
                            )
                        assertEquals(
                            MEMBERS.map { it.first }.toSet(),
                            byIdWeight.map { it.hit.id }.toSet(),
                            "an id-weighted recall is the key set too, zero weight included",
                        )
                        val idScores = byIdWeight.associate { it.hit.id to (it.score ?: 0.0) }
                        assertEquals(4_000.0, idScores.getValue(FULL), 1.0, "rawScore(id) reaches the top of the member band")
                        assertEquals(550.0, idScores.getValue(NONE), 1.0, "and a zero weight scores the rung's floor, not a lost number")

                        // TRUST DOES NOT MOVE A MEMBER — the property `event.sd`
                        // §13 was changed to have, and the one that would go
                        // back silently.
                        //
                        // The rung used to be multiplied by wot_mult(), which
                        // put it in the MEMBER's trust units while the floor
                        // stayed in the SIGNER's — a Trusted List is signed by a
                        // service key nobody follows, so the floor never bound
                        // and a well-trusted member outran the page by four
                        // orders of magnitude. Give one member a reputation the
                        // gate can see and read the same two numbers back: they
                        // must not budge.
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(listOf(ReputationDoc(FULL, influenceScores = mapOf(OBSERVER to 90))))
                            awaitInfluence(index, weighted)

                            val lensed = index.searchRanked(weighted.copy(observer = OBSERVER, minRank = 0.0))
                            val lensedScores = lensed.associate { it.hit.pubkey to (it.score ?: 0.0) }
                            assertEquals(
                                4_000.0,
                                lensedScores.getValue(FULL),
                                1.0,
                                "a member the reader trusts at 90 sits on the same rung as one they have never heard of: $lensedScores",
                            )
                            assertEquals(
                                MEMBERS.sortedByDescending { it.second }.map { it.first },
                                lensed.sortedByDescending { it.score ?: 0.0 }.map { it.hit.pubkey },
                                "and the publisher's confidence still orders the block, not the reader's trust",
                            )

                            // ...BUT AN EXPLICIT FLOOR STILL BINDS. Trust not
                            // PLACING a member is the decision above; trust not
                            // ADMITTING one is a different question, and the
                            // answer had gone missing with the multiply: this
                            // profile declares no rank-score-drop-limit, so
                            // wot_mult()=0 never deleted anything, and
                            // `max(member_rung(), …)` floored an untrusted
                            // member straight back into the affiliation band. A
                            // reader asking `filter:rank:gte:20` got it honoured
                            // for every row on the page except the spliced ones.
                            //
                            // FULL is the only member with a reputation the
                            // observer can see (90, set just above); every other
                            // one reads 0.
                            val gated = index.searchRanked(weighted.copy(observer = OBSERVER, minRank = 0.0, memberFloor = 20.0))
                            assertEquals(
                                listOf(FULL),
                                gated.map { it.hit.pubkey },
                                "an explicit floor drops every member under it, spliced or not: ${gated.map { it.hit.pubkey }}",
                            )
                            assertEquals(
                                4_000.0,
                                (gated.single().score ?: 0.0),
                                1.0,
                                "and the survivor is placed exactly where it was — the gate admits, it does not rank",
                            )
                            assertEquals(
                                MEMBERS.size,
                                index.searchRanked(weighted.copy(observer = OBSERVER, minRank = 0.0)).size,
                                "no explicit floor, no gate: the default read is untouched",
                            )
                        }
                    }
                }
            }
    }

    private fun profile(pubkey: String) =
        EventDoc(
            id = pubkey,
            pubkey = pubkey,
            createdAt = 1_700_000_000L,
            kind = 0,
            tags = emptyList(),
            content = """{"name":"member"}""",
            sig = "e".repeat(128),
            search = SearchFields(name = "member"),
        )

    /** Reputation lands asynchronously like anything else; wait for the lens to see it. */
    private suspend fun awaitInfluence(
        index: VespaEventIndex,
        weighted: EventQuery,
    ) {
        repeat(120) {
            val seen = index.searchRanked(weighted.copy(observer = OBSERVER, minRank = 0.0))
            if (seen.size == MEMBERS.size) return
            delay(500)
        }
    }

    private suspend fun awaitCorpus(
        index: VespaEventIndex,
        expected: Int,
    ) {
        repeat(120) {
            if (index.count(EventQuery(kinds = listOf(0))) >= expected) return
            delay(500)
        }
        error("corpus never became searchable ($expected docs)")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        val FULL = "1".padStart(64, 'a')
        val NONE = "5".padStart(64, 'a')
        val OUTSIDER = "9".padStart(64, 'a')

        /** The reader whose trust must NOT decide where a member sits. */
        val OBSERVER = "7".padStart(64, 'b')

        /**
         * The confidences a real Trusted List carries — quartz's 0..100, and
         * deliberately including 0 and values the OLD quantizer would have
         * collapsed (12 rounded to zero; 87 and 74 shared one bucket).
         */
        val MEMBERS =
            listOf(
                FULL to 100,
                "2".padStart(64, 'a') to 87,
                "3".padStart(64, 'a') to 74,
                "4".padStart(64, 'a') to 12,
                NONE to 0,
            )

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
