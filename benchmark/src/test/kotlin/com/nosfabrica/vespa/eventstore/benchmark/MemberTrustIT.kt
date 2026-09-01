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
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.math.pow
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * §13.1 OF `event.sd`: WHOSE TRUST PLACES A MEMBER — the precedence chain, on a
 * real Vespa.
 *
 * A Trusted List's member is not a text match; a score has to be synthesized
 * for it, and until §13.1 that score carried the trust of the KEY THAT SIGNED
 * THE LIST. A NIP-85 provider is a key the reader appointed in their own 10040
 * and nobody follows — measured on staging: rank 0 for a reader who has not
 * enrolled it, 26 for one who has — so every member of every list it signs
 * inherited that as its magnitude, whatever the publisher said about them.
 *
 * The write path never worked that way: `TrustRecompute` stores an enrolled
 * provider's asserted rank AS the observer's trust in that subject, at face
 * value, with the provider's own rank nowhere in the equation. §13.1 is
 * ranking catching up, most specific answer first:
 *
 *   (a) the LIST's score for this member — `rawScore`, per document;
 *   (b) the MEMBER's own rank under this observer, when the list scored nobody;
 *   (c) neither, and only then the signer, through the pointer floor.
 *
 * Each rung is pinned below, and so is the property that motivates all three:
 * under (a) and (b) the signer's own trust — the one number `pointer_rel`
 * carries — cannot move a member at all.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class MemberTrustIT {
    @Test
    fun `a member is placed by what was said about it, not by who signed the list`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the member-trust IT")

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
                        // Two members the LIST disagrees about (87 vs 12) whose
                        // OWN ranks run the other way (3 vs 99), so no rung can
                        // be mistaken for another: whichever number is placing
                        // them, the order says which.
                        index.putAll(listOf(profile(SURE), profile(DOUBTED), profile(UNRANKED)))
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(
                                listOf(
                                    ReputationDoc(SURE, influenceScores = mapOf(OBSERVER to 3)),
                                    ReputationDoc(DOUBTED, influenceScores = mapOf(OBSERVER to 99)),
                                    // UNRANKED gets no reputation document at all.
                                ),
                            )
                        }
                        awaitCorpus(index, 3)

                        // ---- (a) THE LIST'S OWN SCORE ----
                        val listed = lookup(scored = true, pointerText = POINTER_TEXT, pointerRel = POINTER_REL)
                        val byKey = index.searchRanked(listed).associate { it.hit.pubkey to (it.score ?: 0.0) }

                        assertTrue(
                            byKey.getValue(SURE) > byKey.getValue(DOUBTED),
                            "the list's own score decides, over the member's rank pulling the other way: $byKey",
                        )
                        // ...and it decides ARITHMETICALLY, not merely in order:
                        // the band the pointer's words earned, times the trust
                        // curve over the number the list carried.
                        assertEquals(POINTER_TEXT * wot(87.0), byKey.getValue(SURE), byKey.getValue(SURE) * 0.001, "(a) places at pointer_text x wot(87)")
                        assertEquals(POINTER_TEXT * wot(12.0), byKey.getValue(DOUBTED), byKey.getValue(DOUBTED) * 0.001, "(a) places at pointer_text x wot(12)")

                        // THE SIGNER CANNOT MOVE THEM. `pointer_rel` is the only
                        // number here carrying the trust of the key that signed
                        // the list; a tenfold change in it leaves a scored
                        // member exactly where it was. This is the whole point
                        // of §13.1 in one assertion.
                        val richerSigner = index.searchRanked(listed.copy(rankFeatures = listed.rankFeatures + mapOf(EventYql.F_POINTER_REL to POINTER_REL * 10)))
                        assertEquals(
                            byKey,
                            richerSigner.associate { it.hit.pubkey to (it.score ?: 0.0) },
                            "a scored member does not move when the signer's own relevance does",
                        )

                        // ---- (b) THE MEMBER'S OWN RANK, when the list scored nobody ----
                        val unscored = lookup(scored = false, pointerText = POINTER_TEXT, pointerRel = POINTER_REL)
                        val byRank = index.searchRanked(unscored).associate { it.hit.pubkey to (it.score ?: 0.0) }
                        assertTrue(
                            byRank.getValue(DOUBTED) > byRank.getValue(SURE),
                            "with nothing said per member, each member's own rank orders them — 99 over 3: $byRank",
                        )
                        assertEquals(POINTER_TEXT * wot(99.0), byRank.getValue(DOUBTED), byRank.getValue(DOUBTED) * 0.001, "(b) places at pointer_text x wot(rank)")

                        // ---- (c) NEITHER: the signer is all anyone said ----
                        // UNRANKED has no list score and no reputation row, so
                        // it falls to the floor — which is a share of the
                        // pointer's own relevance, and therefore DOES move with
                        // the signer's trust.
                        assertTrue(
                            byRank.getValue(UNRANKED) < byRank.getValue(SURE),
                            "a member nobody scored sits under one somebody did: $byRank",
                        )
                        val richerUnscored =
                            index
                                .searchRanked(unscored.copy(rankFeatures = unscored.rankFeatures + mapOf(EventYql.F_POINTER_REL to POINTER_REL * 10)))
                                .associate { it.hit.pubkey to (it.score ?: 0.0) }
                        assertTrue(
                            richerUnscored.getValue(UNRANKED) > byRank.getValue(UNRANKED) * 5,
                            "…and it is the one case where the signer still places a member: $richerUnscored",
                        )
                        assertEquals(
                            byRank.getValue(DOUBTED),
                            richerUnscored.getValue(DOUBTED),
                            byRank.getValue(DOUBTED) * 0.001,
                            "while the member the reader's own service ranked stays put",
                        )

                        // ---- INERT WITHOUT A TEXT BAND ----
                        // An engine whose profile reports no match-features sends
                        // no pointer_text, and every member must place exactly as
                        // it did before §13.1 — the floor, ordered by confidence.
                        // The in-memory reference is one such engine, which is
                        // why its own model of this profile stays correct.
                        val noText = index.searchRanked(lookup(scored = true, pointerText = 0.0, pointerRel = POINTER_REL))
                        val floored = noText.associate { it.hit.pubkey to (it.score ?: 0.0) }
                        assertEquals(POINTER_REL, floored.getValue(SURE), POINTER_REL * 0.15, "no text band: the pointer floor answers, as before")
                        assertTrue(
                            floored.getValue(SURE) > floored.getValue(DOUBTED),
                            "…still ordered by the publisher's confidence inside the span: $floored",
                        )
                    }
                }
            }
    }

    /**
     * THE REPORTED SHAPE, END TO END: a real kind-30392 Trusted List, a real
     * kind-10040 enrolling its signer, real 30382 cards, and the People tab's
     * own filter — `kinds:[0]` plus the observer lens.
     *
     * The test above drives the `spliced_member` profile with the numbers a
     * list SUPPLIES, which is the only way to isolate a rung; it never
     * materializes a list. So nothing until now proved the chain that produces
     * those numbers: title -> `search_primary` -> the companion query finds the
     * list -> the gate admits its signer -> `p` tags become weighted keys ->
     * members come back ranked. Every link is somewhere else in the suite, and
     * a chain is not its links.
     *
     * It is also the kind the REPORT was about. The 10 961-event staging slice
     * replayed for the ladder work carries 125 people lists and no 30392 at
     * all, so the corpus measurement could not exercise this path and the
     * store-level coverage of it is `SearchExpansionTest`, which runs against
     * an in-memory engine that does not rank.
     *
     * The discriminating fixture: the list is SURE about a member the observer's
     * own service ranks low (87 over a rank of 20) and DOUBTS one it ranks high
     * (12 over 99). Rung (a) puts them one way, rung (b) the other, and the
     * page says which is deciding.
     */
    @Test
    fun `a trusted list's members reach the People tab in the order the list scored them`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the end-to-end member IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                VespaEventStore.open(url = queryUrl, autoDeploy = true, configUrl = configUrl).use { store ->
                    runBlocking {
                        // Everything a relay would have stored, stored the way a
                        // relay stores it — through the write path, so the
                        // store's own TrustProjection builds the tensors from
                        // the cards rather than a test writing reputation rows.
                        store.batchInsert(
                            listOf(
                                treasureMap(),
                                card(SURE, 20),
                                card(DOUBTED, 99),
                                card(SERVICE, 40),
                                trustedList(),
                                namedProfile(SURE, "Ada Lovelace"),
                                namedProfile(DOUBTED, "Grace Hopper"),
                            ),
                        )
                        store.awaitTrustProjection()
                        awaitStore(store, 7)

                        // The People tab: kinds:[0], lensed. Neither profile
                        // carries a word of the query — they can only arrive
                        // through the list that named them.
                        val page = store.query<Event>(Filter(kinds = listOf(0), search = "Verified Human observer:$OBSERVER", limit = 20))
                        assertEquals(
                            listOf(SURE, DOUBTED),
                            page.map { it.pubKey },
                            "the list's own scores order the page (87 over 12) against the members' own ranks (20 under 99)",
                        )

                        // THE GATE, on the same corpus: a reader who never named
                        // this service unpacks nothing it signed, and neither
                        // does an anonymous read.
                        assertEquals(
                            emptyList(),
                            store.query<Event>(Filter(kinds = listOf(0), search = "Verified Human observer:$STRANGER", limit = 20)).map { it.pubKey },
                            "a stranger's 10040 named nobody, so this list is a stranger's computation to them",
                        )
                        assertEquals(
                            emptyList(),
                            store.query<Event>(Filter(kinds = listOf(0), search = "Verified Human include:spam", limit = 20)).map { it.pubKey },
                            "…and an anonymous read expands no declaration at all",
                        )
                    }
                }
            }
    }

    // ------------------------------------------------------------------

    /**
     * One member lookup: the weighted key set the store sends per pointer, with
     * the two pointer numbers under test. [scored] false is a list that named
     * its members and said nothing about them — bare `p` tags — which is what
     * turns off `doc_conf` and reaches rung (b).
     */
    private fun lookup(
        scored: Boolean,
        pointerText: Double,
        pointerRel: Double,
    ) = EventQuery(
        kinds = listOf(0),
        authorWeights = if (scored) mapOf(SURE to 87, DOUBTED to 12, UNRANKED to 50) else mapOf(SURE to 0, DOUBTED to 0, UNRANKED to 0),
        observer = OBSERVER,
        // The floor the read carries. 0, not the store's default 2: these
        // fixtures include an author with no reputation row at all, and the
        // gate would drop it before the placement under test could be read.
        minRank = 0.0,
        ranking = EventYql.RANK_SPLICED_MEMBER,
        rankFeatures =
            buildMap {
                if (scored) put(EventYql.F_DOC_CONF, 1.0)
                put(EventYql.F_POINTER_REL, pointerRel)
                put(EventYql.F_POINTER_TEXT, pointerText)
                put(EventYql.F_SUBJECT_FLOOR_SPAN, 0.1769)
            },
    )

    private fun profile(key: String) =
        EventDoc(
            id = key,
            pubkey = key,
            createdAt = 1_780_000_000L,
            kind = 0,
            tags = emptyList(),
            content = """{"name":"member"}""",
            sig = "e".repeat(128),
            search = SearchFields(name = "member"),
        )

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        val OBSERVER = "0b5e".repeat(16)
        val SURE = "11".repeat(32)
        val DOUBTED = "22".repeat(32)
        val UNRANKED = "33".repeat(32)

        /** The band a pointer earned with its words, and the finished relevance the signer's trust multiplied it into. */
        const val POINTER_TEXT = 130_000.0
        const val POINTER_REL = 260_000.0

        /** `event.sd`'s wot_of() at the schema's shipped defaults, with the floor these lookups send. */
        fun wot(rank: Double) = 1.0 + rank.pow(2.7)

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)

        /** The provider the reader enrolled, and a reader who enrolled nobody. */
        val SERVICE = "5e".repeat(32)
        val STRANGER = "57".repeat(32)

        var nextId = 0

        fun id() = (++nextId).toString(16).padStart(64, 'a')

        fun event(
            kind: Int,
            pubkey: String,
            tags: String,
            content: String = "",
        ): Event =
            Event.fromJson(
                """{"id":"${id()}","pubkey":"$pubkey","created_at":1780000000,"kind":$kind,"tags":$tags,"content":"$content","sig":"${"e".repeat(128)}"}""",
            )

        /**
         * The reader's kind-10040, in BOTH shapes it has to carry: the NIP-85
         * `30382:rank` entry that lets the projection count this service's
         * cards, and the bare-kind `30392` entry that lets the search unpack
         * its lists (Delegations' KDoc for why one does not imply the other).
         */
        fun treasureMap() =
            event(
                10040,
                OBSERVER,
                """[["30382:rank","$SERVICE","wss://scores.test/"],["30392","$SERVICE","wss://scores.test/"]]""",
            )

        /** A 30382 the enrolled service signs about one subject: the observer's trust in that person. */
        fun card(
            subject: String,
            rank: Int,
        ) = event(30382, SERVICE, """[["d","$subject"],["rank","$rank"]]""")

        /** The list from the report, titled exactly what is searched for, disagreeing with the cards about both members. */
        fun trustedList() =
            event(
                30392,
                SERVICE,
                """[["d","roster"],["title","Verified Human"],["p","$SURE","","87"],["p","$DOUBTED","","12"]]""",
            )

        /** A profile carrying no word of the query — it can only reach the page by being named. */
        fun namedProfile(
            key: String,
            name: String,
        ) = event(0, key, "[]", """{\"name\":\"$name\"}""")

        suspend fun awaitStore(
            store: VespaEventStore,
            expected: Int,
        ) {
            repeat(120) {
                if (store.count(Filter()) >= expected) return
                delay(500)
            }
            error("corpus never became searchable ($expected docs)")
        }

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
