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
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationDoc
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
    // Docs 1/2/14 get DISTINCT authors so the sort:followers case can hang a
    // different verified-follower count on each via the reputation parent;
    // docs 6/7/8 likewise, so the include:spam case can trust exactly one of
    // the three Vitor namesakes; docs 2/15/19/20 likewise, so the
    // trust-crossing case can bound the crossing from both sides.
    private val corpus =
        listOf(
            profile(1, name = "Ode", pubkey = pk(1)),
            profile(2, name = "ODELL", pubkey = pk(2)),
            profile(3, name = "Odessa"),
            profile(4, name = "model"),
            profile(5, name = "code"),
            profile(6, name = "VitorPamplona", pubkey = pk(6)),
            profile(7, name = "Vitor Pamplona", pubkey = pk(7)),
            profile(8, name = "Vitor-Pamplona", pubkey = pk(8)),
            profile(9, name = "BitcoinMemeTreasury"),
            profile(10, name = "CoffeeLover"),
            profile(11, name = "CITADELDISPATCH"),
            profile(12, name = "José"),
            profile(13, name = "中村太郎"),
            profile(14, name = "Ode Fan Club", pubkey = pk(14)),
            profile(15, name = "podcaster", about = "a show hosted by ODELL every week", pubkey = pk(15)),
            profile(16, name = "coffee"),
            // A note whose hashtags ride search_secondary — the weak tier's prefix reach.
            doc(17, kind = 1, search = SearchFields(primary = "morning thoughts", secondary = "#bitcoin #nostr")),
            // Matches only ONE word of "Vitor Pamplona" — the multi-word AND must keep it out.
            profile(18, name = "Vitor"),
            // A barely-trusted account NAMED odell — the trust-crossing case's foil.
            profile(19, name = "ODELL mirror", pubkey = pk(19)),
            // A well-but-not-top-trusted bio mention — must NOT cross above the foil.
            profile(20, name = "fan zine", about = "all things ODELL, unofficial", pubkey = pk(20)),
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
                        // "jose" -> "José" is an EXACT hit, not near: Vespa's
                        // linguistic pipeline folds diacritics on index fields,
                        // so the exact clause matches the folded token and the
                        // near tier (gated behind exact) never fires. The near
                        // attributes still carry the fold for schemas/fields
                        // without that pipeline (raw-byte attribute matching).
                        expect("jose", has = "José", tier = "name")
                        expect("vitorp", has = "Vitor Pamplona", tier = "near")
                        expect("vitorp", has = "Vitor-Pamplona", tier = "near")

                        // --- multi-word AND: every word must be present on the doc ---
                        expect("Vitor Pamplona", has = "Vitor Pamplona", tier = "name")
                        expect("Vitor Pamplona", has = "VitorPamplona")
                        expect("Vitor Pamplona", has = "Vitor-Pamplona")
                        absent("Vitor Pamplona", "Vitor")
                        // A typo'd word still counts as present through its fuzzy matcher.
                        expect("Vitor Pamplna", has = "Vitor Pamplona")
                        absent("Vitor Pamplna", "Vitor")

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

                        // --- exactness: the whole-field match wins its band. Under the
                        // single-phase `text` profile this follows from the fieldLength
                        // division in name_text(); the point of asserting it is the
                        // ordering contract, not the mechanism. ---
                        val ode = search("Ode").map { nameOf(it.doc) }
                        assertTrue(
                            ode.indexOf("Ode") < ode.indexOf("Ode Fan Club"),
                            "exact whole-name must beat the longer name in the same band: $ode",
                        )

                        // --- the default profile + its SECOND PHASE actually execute ---
                        // `search` is the only profile whose second phase computes
                        // the fieldMatch precision features at query time; a broken
                        // feature reference would surface HERE, not at deploy. No
                        // observer is passed, so trust is a constant multiplier
                        // and the tier ladder alone must produce this order.
                        val ranked = searchWith("Ode", EventYql.RANK_SEARCH)
                        val rnames = ranked.map { nameOf(it.doc) }
                        assertEquals("Ode", rnames.first(), "exact match on top under the default profile: $rnames")
                        assertTrue("ODELL" in rnames, "the near hit must survive text_score_cutoff: $rnames")
                        assertEquals("near", ranked.first { nameOf(it.doc) == "ODELL" }.tier)
                        assertTrue(
                            rnames.indexOf("Ode") < rnames.indexOf("Ode Fan Club"),
                            "second-phase exactness keeps the whole-field match on top: $rnames",
                        )

                        // --- sort:followers — verified-follower order within tiers ---
                        // Fed through the real reputation parent: the fan club
                        // out-follows the exact "Ode"; ODELL out-follows everyone
                        // but arrives through the NEAR tier — the count orders
                        // hits WITHIN a tier and never lifts one across tiers.
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(
                                listOf(
                                    ReputationDoc(pk(1), followerCounts = mapOf(OBSERVER to 10.0)),
                                    // ODELL also carries an influence score for the
                                    // trust-crossing case below (sort_followers ignores it:
                                    // its within-tier key is the follower count).
                                    ReputationDoc(
                                        pk(2),
                                        followerCounts = mapOf(OBSERVER to 250_000.0),
                                        influenceScores = mapOf(OBSERVER to 93),
                                    ),
                                    ReputationDoc(pk(14), followerCounts = mapOf(OBSERVER to 5_000.0)),
                                    // The include:spam case: the observer's provider ranks
                                    // ONE of the three Vitor namesakes; the other two stay
                                    // unranked (user_score 0).
                                    ReputationDoc(pk(7), influenceScores = mapOf(OBSERVER to 60)),
                                    // The trust-crossing case: a near-top-trust bio mention
                                    // and a well-trusted one vs a barely-trusted name squatter.
                                    ReputationDoc(pk(15), influenceScores = mapOf(OBSERVER to 97)),
                                    ReputationDoc(pk(19), influenceScores = mapOf(OBSERVER to 13)),
                                    ReputationDoc(pk(20), influenceScores = mapOf(OBSERVER to 77)),
                                ),
                            )
                        }
                        val followers = searchWith("Ode", EventYql.RANK_FOLLOWERS, observer = OBSERVER)
                        val fnames = followers.map { nameOf(it.doc) }
                        assertTrue(
                            fnames.indexOf("Ode Fan Club") < fnames.indexOf("Ode"),
                            "within the token tier the larger verified-follower count wins: $fnames",
                        )
                        assertTrue(
                            fnames.indexOf("Ode") < fnames.indexOf("ODELL"),
                            "a near hit never outranks a token hit, whatever its follower count: $fnames",
                        )
                        assertEquals("near", followers.first { nameOf(it.doc) == "ODELL" }.tier)

                        // --- include:spam: the GATE goes, the trust ORDER stays ---
                        // The store maps include:spam on a search to min_rank=0.0
                        // rather than omitting the feature: wot_mult() in the
                        // default profile anchors its log boost at query(min_rank),
                        // and the schema's fail-open default (-1e9) clamps the
                        // multiplier to the same constant for every author — the
                        // observer's own trusted namesake then sorts under text
                        // noise (the reported "many Vitors above the real one").
                        // Anchored at 0 the curve keeps its full span: trust
                        // orders the namesakes, nothing is dropped.
                        val spamOk =
                            indexRef.searchScored(
                                EventQuery(search = "vitor", observer = OBSERVER, minRank = 0.0),
                            )
                        val vnames = spamOk.map { nameOf(it.doc) }
                        assertEquals(
                            "Vitor Pamplona",
                            vnames.first(),
                            "the observer-trusted namesake must rank FIRST under include:spam: $vnames",
                        )
                        assertTrue(
                            "VitorPamplona" in vnames && "Vitor-Pamplona" in vnames,
                            "include:spam keeps the unranked namesakes in the result: $vnames",
                        )

                        // --- overwhelming trust crosses the tier ladder — bounded from BOTH sides ---
                        // The 2026-08-02 "odell" report: under the observer's lens
                        // CITADEL DISPATCH (bio "hosted by ODELL", trust 97) sat
                        // #9, below trust-13 accounts NAMED odell — the log trust
                        // curve (span ~5.6×) could never cross the ~236× bio→name
                        // band ratio. wot_mult()'s power curve (w_wot_pow 2.7)
                        // crosses on a ~7.6× trust-delta advantage, a window the
                        // report pins from both sides: the 97-trust bio mention
                        // (delta ratio 95/11 ≈ 8.6) must beat the 13-trust name
                        // squatter, the 77-trust bio mention (75/11 ≈ 6.8) must
                        // NOT, and the 93-trust exact name stays on top of
                        // everything (97-vs-93 is nowhere near a crossing).
                        val lensed =
                            indexRef.searchScored(
                                EventQuery(search = "odell", observer = OBSERVER, minRank = 2.0),
                            )
                        val lnames = lensed.map { nameOf(it.doc) }
                        assertEquals(
                            "ODELL",
                            lnames.first(),
                            "the exact name at comparable trust must stay on top: $lnames",
                        )
                        assertTrue(
                            lnames.indexOf("podcaster") < lnames.indexOf("ODELL mirror"),
                            "a 97-trust bio mention must cross above a 13-trust name match: $lnames",
                        )
                        assertTrue(
                            lnames.indexOf("ODELL mirror") < lnames.indexOf("fan zine"),
                            "a 77-trust bio mention must NOT cross above a 13-trust name match: $lnames",
                        )
                        assertEquals("affiliation", lensed.first { nameOf(it.doc) == "podcaster" }.tier)

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

    private suspend fun search(text: String) = searchWith(text, EventYql.RANK_TEXT)

    private suspend fun searchWith(
        text: String,
        ranking: String,
        observer: String? = null,
    ) = indexRef.searchScored(EventQuery(search = text, ranking = ranking, observer = observer))

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
        pubkey: String = "a1".repeat(32),
    ) = doc(n, kind = 0, search = SearchFields(name = name, about = about), pubkey = pubkey)

    private fun doc(
        n: Int,
        kind: Int,
        search: SearchFields,
        pubkey: String = "a1".repeat(32),
    ) = EventDoc(
        id = id(n),
        pubkey = pubkey,
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

        /** The ranking lens for the sort:followers case — cells are keyed by observer. */
        val OBSERVER = "c".repeat(64)

        /** A doc-n author pubkey, distinct from ids (b-padded vs 0-padded). */
        fun pk(n: Int) = n.toString(16).padStart(64, 'b')

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
