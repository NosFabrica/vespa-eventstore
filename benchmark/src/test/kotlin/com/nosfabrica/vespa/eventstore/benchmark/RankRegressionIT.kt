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
            // --- the 2026-08-05 "amethyst" report: whole-field exactness inside
            // the token band. Field-for-field the four docs from the reported
            // export, and they share ONE author (pk(21)) on purpose: wot_mult()
            // is then identical across them and the order is text ALONE. 21/24
            // are the exact hits (the app handler's profile name and the
            // application's primary field ARE the query); 22/23 merely contain
            // it, and repeat it across secondary and body — the shape that won.
            doc(
                21,
                kind = 31990,
                pubkey = pk(21),
                search = SearchFields(name = "Amethyst", about = "Nostr client for Android", website = "https://amethyst.social/"),
            ),
            doc(
                22,
                kind = 41,
                pubkey = pk(21),
                search =
                    SearchFields(
                        primary = "Amethyst Devs",
                        secondary = "Channel for coders and designers to discuss Amethyst development.",
                    ),
            ),
            doc(
                23,
                kind = 30023,
                pubkey = pk(21),
                search =
                    SearchFields(
                        primary = "Schedule posts for later in Amethyst",
                        secondary = "amethyst grownostr",
                        text =
                            "There's no magic server in the cloud holding your post. Your phone publishes it, and then your " +
                                "#amethyst, on your phone, wakes up and fires it off to the relays. Turn on always-on " +
                                "notifications: this keeps a small background service alive so #amethyst can wake up on " +
                                "schedule and post even while the app is closed. #amethyst checks for due posts on a " +
                                "15-minute cycle. The next time you open #amethyst with a working internet connection, any " +
                                "overdue scheduled posts get published.",
                    ),
            ),
            doc(
                24,
                kind = 32267,
                pubkey = pk(21),
                search =
                    SearchFields(
                        primary = "Amethyst",
                        secondary = "The all-in-one Nostr client social-network nostr",
                        text = "A privacy-focused Nostr client for Android. Built-in TOR support, the most configurable relay system.",
                    ),
            ),
            // --- the 2026-08-05 "Avi Burra" / "Jon Gordon" reports: a perfect
            // match losing to a long title by a ONE- OR TWO-POINT trust gap.
            // Unlike the amethyst pair these have DIFFERENT authors, near the
            // top of the scale where wot_mult() is steepest — 98-vs-100 is
            // +5.7% and 96-vs-97 is +2.9%, both larger than any within-band
            // text signal short of a rung. Fields are the reported ones.
            doc(
                25,
                kind = 0,
                pubkey = pk(25),
                search =
                    SearchFields(
                        name = "AviBurra",
                        displayName = "Avi Burra",
                        about = "Chronicler of the Sovereign Age",
                        nip05 = "avi@nip21.media",
                        lud16 = "avi@primal.net",
                        website = "casanostra.ink",
                    ),
            ),
            doc(
                26,
                kind = 30023,
                pubkey = pk(26),
                search =
                    SearchFields(
                        primary = "Do You Want a Seat at the Table? Join Avi Burra's Journey to Paraguay!",
                        text =
                            "Avi was born in India and moved to New York a few months after 9/11. Avi wanted to create " +
                                "something that welcomed people in without turning it into a lecture. Avi left his fiat job " +
                                "on July 4th. For Avi, Finding Home is a way to show lives full of creativity and purpose. " +
                                "Avi is currently at 82% of his goal, and Avi will share meals with shop owners in Paraguay.",
                    ),
            ),
            doc(
                27,
                kind = 0,
                pubkey = pk(27),
                search =
                    SearchFields(
                        name = "thebitcoinyogi",
                        displayName = "Jon Gordon",
                        about = "Bringing bitcoin and nostr to healthcare and Chicago",
                        nip05 = "jon@soundhsa.com",
                        lud16 = "thebitcoinyogi@primal.net",
                        website = "https://www.soundhsa.com",
                    ),
            ),
            doc(
                28,
                kind = 34235,
                pubkey = pk(28),
                search =
                    SearchFields(
                        primary = "Take a Look Into the Future of Healthcare, It's Purple and Orange | Jon Gordon",
                        secondary = "Where to find Jon Gordon: Satoshi Health Advisors, NosFabrica",
                        text = "Where to find Jon Gordon: Satoshi Health Advisors and NosFabrica. You can hear this episode on Fountain.",
                    ),
            ),
            // --- the 2026-08-05 "Primal" report: the SINGLE-word shape, and the
            // starkest evidence of what the token band degenerates to. The
            // reported 40 hits came back in EXACT author-trust order —
            // 99,99,99,99,98,97,97,97,97,97,97,96,… — with the account whose
            // name IS "primal" at #6 behind four trust-99 articles. Every hit
            // scores the same w_name_tier and no within-band text signal is
            // worth a single trust point, so the sort is trust and nothing
            // else. Single-word queries also make queryCompleteness a constant
            // 1.0, leaving fieldCompleteness to carry perfect_match() alone.
            doc(
                29,
                kind = 0,
                pubkey = pk(29),
                search =
                    SearchFields(
                        name = "primal",
                        displayName = "primal",
                        about = "The official Primal account",
                        nip05 = "primal@primal.net",
                        lud16 = "primal@primal.net",
                    ),
            ),
            doc(
                30,
                kind = 30023,
                pubkey = pk(30),
                search =
                    SearchFields(
                        primary = "Introducing Primal for iOS",
                        text =
                            "What happens when you integrate a Nostr client with a bitcoin lightning wallet? They both get " +
                                "massively better. Primal for iOS is now available on the App Store.",
                    ),
            ),
            // Same author as 29, primary field ALSO exactly the query: a follow
            // set titled "Primal". It is a genuine perfect match, so the rung
            // lifts it too — the profile stays ahead only on its identity
            // credit (nip05/lud16 primal@…, ~50×bm25 against the set's zero;
            // the gram terms that could offset it are capped at gram_cap).
            // Deliberately here to keep that margin under test. What is NOT
            // asserted is where the SET lands relative to the trust-99
            // article: the rung puts it above, and whether a kind-30000 set
            // belongs in the name tier at all is an extractor question
            // (upstream, in Quartz's SearchFieldExtractor), not a ranking one.
            doc(31, kind = 30000, pubkey = pk(29), search = SearchFields(primary = "Primal")),
            // --- the 2026-08-05 "Jack" report: an AMBIGUOUS query. The four
            // cases above each have one obviously-right answer; "Jack" has
            // three accounts literally named jack and a dozen real people
            // named Jack Something, so the rung cannot pick a winner — it can
            // only put the whole-field matches above the fragments and let
            // trust order what remains. Two shapes the others never reach:
            //   * SEVERAL simultaneous perfect matches (32, 34) — the rung
            //     saturates and trust is the tie-break, which is correct.
            //   * a perfect match that must overturn a HIGHER-trust partial
            //     (34 at trust 97 over 33 at trust 100). The previous reports
            //     only ever asked the rung to beat +1 or +2 points; this asks
            //     for 3, and bounds how much it may buy.
            // 33 also pins a sibling-field split: its `name` (jacksweeney) is
            // reachable only through the NEAR tier, its display_name matches
            // exactly, so the doc must arrive in the token band.
            doc(
                32,
                kind = 0,
                pubkey = pk(32),
                search = SearchFields(name = "Jack", displayName = "jack (n/acc)", nip05 = "jack@chakany.systems"),
            ),
            doc(
                33,
                kind = 0,
                pubkey = pk(33),
                search = SearchFields(name = "jacksweeney", displayName = "jack sweeney", nip05 = "jacksweeney@nostrplebs.com"),
            ),
            doc(34, kind = 0, pubkey = pk(34), search = SearchFields(name = "jack", nip05 = "jack@primal.net")),
            // --- B1/B2 probes (2026-08-05 audit), ONE author so trust cancels:
            // 40 spells the query, 42 reverses it, 41 concatenates it. Before
            // the audit 40 and 42 were bit-identical (text_score() never
            // carried the w_proximity term relevance() has, so the default
            // profile was blind to word order) and 40 read perfect_match 0.667
            // rather than 1.0 (fieldMatch's queryCompleteness divides by the
            // SYNTHETIC terms FuzzyWordGroup adds, which a normally-spelled
            // doc can never match).
            doc(40, kind = 0, pubkey = pk(40), search = SearchFields(name = "Jon Gordon", about = "bitcoin")),
            doc(41, kind = 0, pubkey = pk(40), search = SearchFields(name = "JonGordon", about = "bitcoin")),
            doc(42, kind = 0, pubkey = pk(40), search = SearchFields(name = "Gordon Jon", about = "bitcoin")),
            // 43 is the MIXED-FIELD trap: its `name` reverses the query (best
            // coverage, worst order) while its display_name holds the query in
            // order inside a longer string (worst coverage, best order). Taking
            // each factor's max independently across fields scores it a perfect
            // 1.0 and floats it above 40, which is exactly what the first cut of
            // order_factor() did — measured, it ranked FIRST. Every factor must
            // come from the SAME field.
            doc(43, kind = 0, pubkey = pk(40), search = SearchFields(name = "Gordon Jon", displayName = "Jon Gordon Fan Club", about = "bitcoin")),
            // --- the 2026-08-05 "divórcio e desabafa" report: a hit whose
            // ONLY match is the BODY. The reported kind 1 has no NIP-14
            // subject, so its subject/title column is empty and everything it
            // says lives in search_text; it came back for "vitor" (through
            // the #VItor-roque hashtag, the weak tier) and NOT for three
            // words that are its content verbatim. search_text had no rung —
            // query(w_content) × bm25, a couple of points — so text_score()
            // sat under text_score_cutoff and `search` dropped the hit
            // outright. Fields are the reported event's, extracted the way
            // SearchExtractors does it (t-tags -> secondary, content -> text).
            doc(
                44,
                kind = 1,
                pubkey = pk(44),
                search =
                    SearchFields(
                        secondary = "Esportes VItor roque",
                        text =
                            "Esposa de Vitor Roque pede divórcio e desabafa sobre crise. A influencer digital Dayana Lins " +
                                "chamou atenção nas redes sociais ao compartilhar aspectos íntimos de seu relacionamento com " +
                                "o jogador de futebol Vitor Roque.",
                    ),
            ),
            // --- the secondary tier's OTHER rungless routes (2026-08-05 audit).
            // The weak tier read only search_secondary_tokens, a NearText
            // attribute, so an exact match on the search_secondary INDEX
            // field that the attribute cannot mirror arrived with no rung and
            // died at text_score_cutoff exactly as the body did. Two of the
            // three routes are decided by THIS repo's constants, so they are
            // pinnable; the third (stemming — search_secondary is stemmed,
            // the attribute is raw bytes, so "bitcoiners" reaches the index
            // and not the prefix column) depends on the engine's stemmer and
            // is left to the comment in event.sd.
            //   * "ai" is under FuzzyWordGroup.MIN_PREFIX_LEN, so NO prefix
            //     clause is emitted at all, and under MIN_AND_GRAMS_TEXT, so
            //     no gram clause either — the exact clause is the only route.
            //   * "quilombola" sits past NearText.MAX_ELEMENTS (48) in the
            //     token array below (50 fillers precede it), so the attribute
            //     does not carry it while the index field carries every word.
            doc(
                45,
                kind = 1,
                pubkey = pk(45),
                search =
                    SearchFields(
                        secondary = (1..50).joinToString(" ") { "fill%02d".format(it) } + " ai quilombola",
                        text = "a body that says none of the words above",
                    ),
            ),
            // The CJK-body pin (see the known-limitation block in runCase):
            // prose in a script the default linguistics cannot segment. Its
            // author is trusted, so only tokenization can explain a miss.
            doc(46, kind = 1, pubkey = pk(44), search = SearchFields(text = "中村太郎は東京で新しい会社を設立しました。")),
            doc(
                35,
                kind = 30023,
                pubkey = pk(35),
                search =
                    SearchFields(
                        primary = "Dr. Jack Kruse on Artificial Light, Sunlight, and Health: Podcast Summary",
                        text = "Dr. Jack Kruse on why artificial light is the problem and sunlight is the fix.",
                    ),
            ),
        ) + FLOOR_CASES.map { doc(it.n, kind = it.kind, pubkey = pk(60), search = it.search) }

    @Test
    fun `prefix, typo, infix, folded and CJK shapes land in the right tier and order`() {
        val external = System.getenv("VESPA_IT_URL")
        assumeTrue(external != null || dockerAvailable(), "no VESPA_IT_URL and Docker not available — skipping the rank regression IT")
        if (external != null) {
            // An ALREADY-RUNNING Vespa (`docker run … vespaengine/vespa`, a
            // port-forward), query port and config port derived from it. CI
            // keeps the testcontainers path; this exists because a sandbox
            // where testcontainers' strategy selection fails — even with a
            // reachable daemon — otherwise cannot run this test AT ALL, and
            // the alternative people fall back to (driving Vespa by hand and
            // eyeballing the numbers) does not execute the assertions. It let
            // a wrong assertion reach CI on 2026-08-05.
            runCase(external.trimEnd('/'), external.trimEnd('/').replace(":8080", ":19071"))
            return
        }

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                runCase(
                    "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}",
                    "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}",
                )
            }
    }

    private fun runCase(
        queryUrl: String,
        configUrl: String,
    ) {
        run {
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
                    // NOT a test of the second-phase exactness rule, despite
                    // the shape: both docs are single-field profiles, so the
                    // fieldLength division in name_text() already separates
                    // them and this passes with w_exactness_pop=0. The rule
                    // itself is pinned by the "amethyst" case below, where the
                    // tail runs the other way. Kept as the ordering contract.
                    assertTrue(
                        rnames.indexOf("Ode") < rnames.indexOf("Ode Fan Club"),
                        "the whole-field match stays on top under the default profile: $rnames",
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
                                // The "amethyst" case: ONE author for all four
                                // docs, at the top of the scale — wot_mult()
                                // ≈ 237000, the worst case for a boost that
                                // rides the second phase un-multiplied.
                                ReputationDoc(pk(21), influenceScores = mapOf(OBSERVER to 100)),
                                // The "Avi Burra" / "Jon Gordon" cases: the
                                // reported trust pairs, verbatim. Both put the
                                // partial match ONE or TWO points ahead — the
                                // margin wot_mult() turns into +5.7% / +2.9%.
                                ReputationDoc(pk(25), influenceScores = mapOf(OBSERVER to 98)),
                                ReputationDoc(pk(26), influenceScores = mapOf(OBSERVER to 100)),
                                ReputationDoc(pk(27), influenceScores = mapOf(OBSERVER to 96)),
                                ReputationDoc(pk(28), influenceScores = mapOf(OBSERVER to 97)),
                                // The "Primal" case: the reported pair, two points apart.
                                ReputationDoc(pk(29), influenceScores = mapOf(OBSERVER to 97)),
                                ReputationDoc(pk(30), influenceScores = mapOf(OBSERVER to 99)),
                                // The "Jack" case: 34 must overturn 33 on
                                // exactness despite giving up 3 points.
                                ReputationDoc(pk(32), influenceScores = mapOf(OBSERVER to 100)),
                                ReputationDoc(pk(33), influenceScores = mapOf(OBSERVER to 100)),
                                ReputationDoc(pk(34), influenceScores = mapOf(OBSERVER to 97)),
                                ReputationDoc(pk(35), influenceScores = mapOf(OBSERVER to 98)),
                                ReputationDoc(pk(40), influenceScores = mapOf(OBSERVER to 50)),
                                // The body-match case: the reported author's score, 8 —
                                // low enough that only a real rung can lift the hit over
                                // text_score_cutoff, and it must clear the store's own
                                // min_rank (2) as it did in the report.
                                ReputationDoc(pk(44), influenceScores = mapOf(OBSERVER to 8)),
                                // The secondary-rung case: same low score, same reason.
                                ReputationDoc(pk(45), influenceScores = mapOf(OBSERVER to 8)),
                                // The recall-floor matrix: ONE author for all ten rows, so
                                // the matrix measures the ladder and never trust.
                                ReputationDoc(pk(60), influenceScores = mapOf(OBSERVER to 50)),
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

                    // --- whole-field exactness inside the TOKEN band (2026-08-05 "amethyst") ---
                    // Reported: searching "amethyst" put the app handler
                    // NAMED Amethyst at #4 and the application whose primary
                    // field is Amethyst at #6, under a channel ("Amethyst
                    // Devs") and two articles ("Schedule posts for later in
                    // Amethyst") — the FIRST TWO by the same author, so trust
                    // explained nothing. Inside the token band every hit
                    // scores the same w_name_tier; what then ordered them was
                    // the additive tail, where tier_text() sums UNCAPPED
                    // secondary + body bm25 a one-word field has no columns to
                    // collect, against the single-digit bm25/fieldLength^1.5
                    // that is the only term favouring the exact hit.
                    // exactness() exists to overrule exactly this, but at 40
                    // points added AFTER the ×wot_mult first phase it was
                    // worth 40/237000 text points here — see the second phase
                    // in `search`. Same query the report ran: the store's own
                    // min_rank (DEFAULT_MIN_RANK) and no explicit profile, so
                    // this also pins EventYql's choice of `search`.
                    val amethyst =
                        indexRef.searchScored(
                            EventQuery(search = "amethyst", observer = OBSERVER, minRank = 2.0),
                        )
                    val anames = amethyst.map { nameOf(it.doc) }
                    val aorder = amethyst.map { it.doc.id }
                    assertTrue(
                        aorder.indexOf(id(21)) < aorder.indexOf(id(22)) && aorder.indexOf(id(21)) < aorder.indexOf(id(23)),
                        "the profile name that IS the query must beat the names that merely contain it: $anames",
                    )
                    assertTrue(
                        aorder.indexOf(id(24)) < aorder.indexOf(id(22)) && aorder.indexOf(id(24)) < aorder.indexOf(id(23)),
                        "the primary field that IS the query must beat the titles that merely contain it: $anames",
                    )
                    // The tail it has to overrule is at its largest on the
                    // article: title + hashtags + a body repeating the term.
                    assertTrue(
                        aorder.indexOf(id(22)) < aorder.indexOf(id(23)),
                        "two extra columns must not outrank a one-word primary field: $anames",
                    )
                    // All four arrive through the SAME band — the fix reorders
                    // within it and must not manufacture a new tier.
                    assertTrue(
                        amethyst.filter { it.doc.id in setOf(id(21), id(22), id(23), id(24)) }.all { it.tier == "name" },
                        "every amethyst doc must arrive through the token band: ${amethyst.map { it.doc.id to it.tier }}",
                    )

                    // --- a perfect match outranks a better-trusted partial (2026-08-05) ---
                    // Both reports are the same shape as the amethyst case
                    // with the one control removed: DIFFERENT authors, a
                    // one- or two-point trust gap the wrong way. That gap
                    // is not small where wot_mult() is steep — d(score)/
                    // score = w_wot_pow × Δtrust/(trust − min_rank), so
                    // ~2.8% per point up here, more than the whole
                    // words+exactness tie-breaker can offer. The perfect
                    // match has to be a RUNG (w_perfect_pop) to survive it.
                    // Trust still crosses the rung on a real advantage —
                    // the odell bounds above pin that from both sides, and
                    // they are what caps w_perfect_pop.
                    for (
                    (query, perfect, partial) in
                    listOf(
                        Triple("Avi Burra", id(25), id(26)),
                        Triple("Jon Gordon", id(27), id(28)),
                        // Single word: queryCompleteness is a constant 1.0,
                        // so fieldCompleteness carries the rung alone.
                        Triple("primal", id(29), id(30)),
                        Triple("jack", id(32), id(35)),
                    )
                    ) {
                        val hits = indexRef.searchScored(EventQuery(search = query, observer = OBSERVER, minRank = 2.0))
                        val labels = hits.map { nameOf(it.doc) }
                        val ids = hits.map { it.doc.id }
                        assertEquals(
                            perfect,
                            ids.firstOrNull(),
                            "\"$query\" is the whole primary field of $perfect — it must rank first: $labels",
                        )
                        assertTrue(
                            ids.indexOf(perfect) < ids.indexOf(partial),
                            "a better-trusted partial title must not outrank the perfect match for \"$query\": $labels",
                        )
                    }

                    // --- how much trust a whole-field match may overturn ---
                    // The "Jack" report's ambiguous shape. A perfect match
                    // at trust 97 must beat a HALF match at trust 100 (the
                    // rung is (130200+109000)/(130200+56000) = 1.285, so it
                    // survives a trust-delta ratio up to 1.285^(1/2.7) =
                    // 1.096 — and up here the whole 0..100 scale cannot
                    // supply that, which is why a top-trust perfect match
                    // is effectively unbeatable by a fragment while a
                    // trust-20 one is not). The bound BELOW is the same
                    // ladder the odell case pins, from the other side.
                    val jack = indexRef.searchScored(EventQuery(search = "jack", observer = OBSERVER, minRank = 2.0))
                    val jorder = jack.map { it.doc.id }
                    val jlabels = jack.map { nameOf(it.doc) }
                    assertTrue(
                        jorder.indexOf(id(34)) < jorder.indexOf(id(33)),
                        "a trust-97 whole-field match must overturn a trust-100 half match: $jlabels",
                    )
                    assertTrue(
                        jorder.indexOf(id(33)) < jorder.indexOf(id(35)),
                        "a half match still beats a fragment of a long title at comparable trust: $jlabels",
                    )
                    // The exact hit on display_name decides the band even
                    // though `name` (jacksweeney) is only a NEAR hit.
                    assertEquals("name", jack.first { it.doc.id == id(33) }.tier)

                    // --- word ORDER and the whole-field rung (2026-08-05 audit) ---
                    // Same author, so this is text alone. 40 spells the
                    // query and must beat 42, which reverses it: order_factor
                    // halves the rung for a wrong-order field, gated on
                    // n_words > 1 so the single-word odell ceiling above is
                    // untouched by construction. 41 (the concatenation) is a
                    // KNOWN 0.5 — it matches only the joined synthetic term,
                    // indistinguishable from a doc named "Jon" inside a rank
                    // expression.
                    val jg = indexRef.searchScored(EventQuery(search = "Jon Gordon", observer = OBSERVER, minRank = 2.0))
                    val jgOrder = jg.map { it.doc.id }
                    assertTrue(
                        jgOrder.indexOf(id(40)) < jgOrder.indexOf(id(42)),
                        "the field that spells the query must beat the one that reverses it: ${jg.map { nameOf(it.doc) }}",
                    )
                    assertTrue(
                        id(41) in jgOrder,
                        "the concatenation must still recall: ${jg.map { nameOf(it.doc) }}",
                    )
                    assertTrue(
                        jgOrder.indexOf(id(40)) < jgOrder.indexOf(id(43)),
                        "coverage and order must come from the SAME field: ${jg.map { nameOf(it.doc) }}",
                    )

                    // A quoted PHRASE is one query item spanning several
                    // field tokens, so the rung's coverage halves must be
                    // item-granular (n_words counts phrases) and
                    // token-granular (fieldCompleteness) respectively. With
                    // matchCount/fieldLength and no n_words this read 0.5 on
                    // a field the phrase covers completely — the most
                    // explicit exactness a user can ask for, paid half.
                    val phrase = indexRef.searchScored(EventQuery(phrases = listOf("Jon Gordon"), observer = OBSERVER, minRank = 2.0))
                    val phraseOrder = phrase.map { it.doc.id }
                    // 27 and 40 are BOTH whole-field phrase matches; 27 wins on
                    // trust (96 vs 50), which is the ladder working as designed.
                    // The coverage claim therefore has to be made where trust is
                    // CONTROLLED — 40 vs 43, one author — because that is the
                    // only comparison the phrase fix actually moves. Asserting
                    // 40 first instead was the 2026-08-05 CI failure: it had
                    // been checked against an isolated four-doc corpus in which
                    // 27 did not exist.
                    assertEquals(
                        id(27),
                        phraseOrder.firstOrNull(),
                        "the best-trusted whole-field phrase match must rank first: ${phrase.map { nameOf(it.doc) }}",
                    )
                    assertTrue(
                        phraseOrder.indexOf(id(40)) < phraseOrder.indexOf(id(43)),
                        "at equal trust, the field the phrase covers WHOLLY beats the one it covers partly: ${phrase.map { nameOf(it.doc) }}",
                    )

                    // --- the rungs the 2026-08-05 audit added, in ONE picture ---
                    //   "divórcio e desabafa"  three words that are doc 44's
                    //     content verbatim, under the same lens that found it
                    //     by hashtag. Before tier_body_match() search_text
                    //     earned only query(w_content) × bm25 — low tens
                    //     against text_score_cutoff's 100 — so `search` scored
                    //     the hit 0.0 and rank-score-drop-limit removed it.
                    //   "vitor"  the SAME doc, which must still arrive WEAK
                    //     through its hashtag: the body rung must not promote
                    //     a body past the tier above it.
                    //   "ai" / "quilombola"  exact matches on search_secondary
                    //     that its NearText attribute cannot mirror (under the
                    //     prefix floor; past the element cap).
                    // Asserted under the DEFAULT profile with an observer —
                    // the only profile the cutoff lives in, which is why this
                    // read as an indexing bug and was not one.
                    //
                    // Collected into one comparison rather than asserted one
                    // by one: a per-case assertion stops at the first mismatch
                    // and hides the rest, and these run only in CI against a
                    // real Vespa, so each round trip has to be worth a full
                    // answer.
                    // ONE comparison for the audit's rungs AND the recall-floor
                    // matrix (FLOOR_CASES). They were two assertEquals calls
                    // for exactly one CI run, and the first one throwing meant
                    // the matrix never executed — so a six-minute round trip
                    // against the only engine that can answer these bought
                    // half a picture. Every row now reports, pass or fail.
                    //
                    // The last three rows are a DISCRIMINATOR ladder, kept
                    // permanently because each rung is a real recall contract.
                    // The reported query is "divórcio e desabafa"; index-side
                    // diacritic folding is already proven above ("jose" ->
                    // "José"), so reading the three together says which layer
                    // broke without another round trip:
                    //   ASCII, 2 words     fails -> the body rung is dead
                    //   accented, 2 words  fails alone -> the QUERY side does
                    //                      not fold what the index folded
                    //   accented, 3 words  fails alone -> the 1-character word
                    //                      is unmatchable, and an unmatchable
                    //                      word in FuzzyWordGroup's conjunction
                    //                      takes the whole query down with it
                    //                      (the rule EventYql already applies
                    //                      to letter-less words like "⚡").
                    val ladder =
                        FLOOR_CASES.map { Triple(it.token, id(it.n), it.tier) } +
                            listOf(
                                Triple("vitor", id(44), "weak"),
                                Triple("ai", id(45), "weak"),
                                Triple("quilombola", id(45), "weak"),
                                // Doc 44's body is PORTUGUESE. Until the text
                                // fields were set `stemming: none`, "futebol"
                                // was the ONLY word here an English-stemmed
                                // query could reach — Vespa stems a document
                                // with its DETECTED language and a query as
                                // English, so the two only ever met on a word
                                // with no suffix to strip. These rows are the
                                // regression for that: the reported query, its
                                // words one at a time, ASCII and accented.
                                Triple("futebol", id(44), "body"),
                                Triple("esposa", id(44), "body"),
                                Triple("crise", id(44), "body"),
                                Triple("influencer", id(44), "body"),
                                Triple("desabafa", id(44), "body"),
                                Triple("divorcio", id(44), "body"),
                                Triple("divórcio", id(44), "body"),
                                Triple("atenção", id(44), "body"),
                                Triple("divorcio desabafa", id(44), "body"),
                                Triple("divórcio desabafa", id(44), "body"),
                                Triple("divórcio e desabafa", id(44), "body"),
                            )
                    assertEquals(
                        ladder.associate { (query, _, tier) -> query to tier },
                        ladder.associate { (query, docId, _) -> query to tierOfHit(query, docId) },
                        "every searchable column must be reachable under the observer lens, through its own tier",
                    )

                    // --- KNOWN LIMITATION, PINNED AS A BUG: CJK body text is
                    // still unsearchable. This block asserts what the engine
                    // DOES, not what it should do. Fix the cause and it fails —
                    // that is the point, and its message says to delete it.
                    //
                    // The Portuguese half of this problem is fixed (the rows in
                    // the ladder above): the text fields now carry
                    // `stemming: none`, so a document's DETECTED language can no
                    // longer stem its index terms away from an English-stemmed
                    // query. Tokenization is a different layer and still
                    // language-driven, and the default linguistics does not
                    // SEGMENT CJK at all — a Japanese sentence is one token, so
                    // no query reaches inside it.
                    //
                    // Measured on a live Vespa, before and after the stemming
                    // change, identical both times: 中村太郎 0, 東京 0, 会社 0.
                    // Fixing it needs a CJK-capable linguistics component
                    // (Vespa's OpenNLP bundle or similar), which is a
                    // deployment/dependency decision, not a schema one.
                    //
                    // Profile NAMES in CJK are unaffected and keep working —
                    // "中村" -> "中村太郎" passes above — because they ride
                    // NearText's raw-byte attributes, not Vespa's tokenizer.
                    val cjk = listOf("中村太郎", "東京", "会社")
                    assertEquals(
                        cjk.associateWith { "MISSING" },
                        cjk.associate { it to tierOfHit(it, id(46)) },
                        "CJK body recall is BROKEN and pinned here; if this fails, someone fixed it — delete this block",
                    )

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

    /**
     * The tier [docId] arrives through for [query] under the observer lens —
     * the DEFAULT profile, the only one carrying text_score_cutoff. "MISSING"
     * rather than null when the doc does not come back at all, so a recall
     * failure and a mis-tiered hit read the same way in one map comparison
     * instead of one masking the other.
     */
    private suspend fun tierOfHit(
        query: String,
        docId: String,
    ): String =
        indexRef
            .searchScored(EventQuery(search = query, observer = OBSERVER, minRank = DEFAULT_IT_MIN_RANK))
            .firstOrNull { it.doc.id == docId }
            ?.tier ?: "MISSING"

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

    /**
     * The doc's label, resolved by id from the seeded corpus (summaries don't
     * carry search fields): the profile name, else the generic tier's primary
     * field, so a failure message reads the same for both groups.
     */
    private fun nameOf(doc: EventDoc): String = corpus.first { it.id == doc.id }.search.let { it.name ?: it.primary } ?: doc.id

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

    /** One recall-floor row: a nonsense token living in exactly one column, and the tier it must arrive through. */
    private data class FloorCase(
        val n: Int,
        val kind: Int,
        val token: String,
        val tier: String,
        val search: SearchFields,
    )

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** The store's own floor (FilterMapping.DEFAULT_MIN_RANK), so these queries are shaped like the reported ones. */
        const val DEFAULT_IT_MIN_RANK = 2.0

        /**
         * THE RECALL-FLOOR MATRIX: one doc per searchable column, each
         * carrying a nonsense token in THAT COLUMN ALONE. Asserted under the
         * observer-lensed `search` profile — the only one with
         * text_score_cutoff, and therefore the only one where a column
         * without a rung is not ranked low but DELETED.
         *
         * This is the standing answer to the 2026-08-05 audit. Three columns
         * had been silently unfindable under a lens (search_text and
         * search_location outright; search_secondary whenever its NearText
         * attribute could not mirror a word the index field held), and the
         * reason no gate saw it is that the corpus above grew case-by-case
         * from reported bugs — every one of which was about NAMES. Nothing
         * ever asked the plain question "is each column reachable at all".
         *
         * The tiers are asserted, not just presence, so this doubles as an
         * executable picture of the ladder. Two rows record known state
         * rather than desired state: nip05/lud16 report "name" because
         * matchCount on either flips has_token_match (see identity_text() in
         * event.sd) — when identity gets its own rung those two become
         * "identity", and this matrix is where that lands.
         *
         * LIMIT, deliberate: the tokens are rare, so this proves each column
         * is REACHABLE, not that it survives at low IDF. The identity cliff
         * (a domain token common enough that query(w_identity) × bm25 falls
         * under the cutoff) needs a corpus skewed on purpose and is covered
         * by floored_text_score() instead — hence the "floor" tier, which
         * nothing here should currently return.
         */
        val FLOOR_CASES =
            listOf(
                FloorCase(60, 0, "zoxlarn", "name", SearchFields(name = "zoxlarn")),
                FloorCase(61, 0, "quibrith", "name", SearchFields(displayName = "quibrith")),
                FloorCase(62, 0, "vandreth", "affiliation", SearchFields(about = "vandreth")),
                FloorCase(63, 0, "kelmoraq", "name", SearchFields(nip05 = "kelmoraq@thrandil.invalid")),
                FloorCase(64, 0, "brivvoth", "name", SearchFields(lud16 = "brivvoth@thrandil.invalid")),
                FloorCase(65, 0, "plentharn", "affiliation", SearchFields(website = "https://plentharn.invalid")),
                FloorCase(66, 1, "xanthorel", "name", SearchFields(primary = "xanthorel")),
                FloorCase(67, 1, "grintavos", "weak", SearchFields(secondary = "grintavos")),
                FloorCase(68, 1, "murbaneth", "body", SearchFields(text = "murbaneth")),
                FloorCase(69, 1, "delvarion", "body", SearchFields(location = "delvarion")),
            )

        /** The ranking lens for the sort:followers case — cells are keyed by observer. */
        val OBSERVER = "c".repeat(64)

        /** A doc-n author pubkey, distinct from ids (b-padded vs 0-padded). */
        fun pk(n: Int) = n.toString(16).padStart(64, 'b')

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
