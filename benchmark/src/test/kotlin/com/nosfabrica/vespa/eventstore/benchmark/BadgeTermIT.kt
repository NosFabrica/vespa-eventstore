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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
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
import kotlin.math.abs
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A NIP-30 BADGE IS ONE TERM, on a real Vespa — the engine half of
 * [com.nosfabrica.vespa.eventstore.mapping.Shortcodes], which no unit test can
 * reach.
 *
 * The report (staging, 2026-09-01): six of the top six hits for a bare
 * `verified` search were bridged Mastodon accounts named `… :verified:`, one of
 * them outranking a Trusted List titled exactly the searched phrase. The
 * picture was a name token, so it stood on the 130 000 name rung.
 *
 * Both reference engines answer "found" here for the wrong reason and so can
 * pin neither half of the fix: `InMemoryEventIndex` matches by naive substring,
 * which reaches `xemojiverified` from inside; `MockVespaEngine` parses the word
 * group without executing a matcher. What only Vespa can show is that the
 * word's ONE remaining route into the badge term — the AND-of-trigrams net over
 * `search_secondary_gram` — is gram-only, scores under
 * `query(text_score_cutoff)` (its ceiling is `w_secondary` x `gram_cap` = 32
 * against a cutoff of 100) and is therefore DELETED by the profile's
 * `rank-score-drop-limit`, rather than merely ranked low.
 *
 * Three properties, all of them the reason the fix is a tokenization rather
 * than the deletion it replaced:
 *
 *  - the WORD no longer finds the badge (the regression);
 *  - the BADGE still finds the badge, `:verified:` typed as a query — including
 *    an account whose whole name is one, which a deletion made unfindable;
 *  - wearing a badge costs the NAME nothing: `DotardTed :verified:` scores what
 *    a plain `DotardTed` scores for a search of that name, which is what moving
 *    the term off the name field and onto the secondary tier buys.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class BadgeTermIT {
    @Test
    fun `a badge is searchable as a badge, never as the word inside it`() {
        val external = System.getenv("VESPA_IT_URL")
        assumeTrue(external != null || dockerAvailable(), "no VESPA_IT_URL and Docker not available — skipping the badge-term IT")
        if (external != null) {
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
        val store = VespaEventStore.open(url = queryUrl, autoDeploy = true, configUrl = configUrl)
        store.use {
            runBlocking {
                store.batchInsert(CORPUS + fillers())
                // Every fixture author ranked by the observer, equally: the
                // lens has to be ON (it selects the trust-multiplied `search`
                // profile — EventYql.profileOf), and equal scores keep the
                // trust factor out of every comparison below.
                VespaReputationIndex(queryUrl).use { reputation ->
                    reputation.putAll(CORPUS.map { ReputationDoc(it.pubKey, influenceScores = mapOf(OBSERVER to 50)) })
                }
                awaitCorpus(store, CORPUS.size + FILLER_COUNT)

                // ---- the regression: the word must not reach the picture ----

                assertEquals(
                    setOf(SAYS_IT),
                    search(store, "verified").toSet(),
                    "the WORD must find only the account that says it",
                )
                assertEquals(
                    setOf(SAYS_IT),
                    search(store, "verified human").toSet(),
                    "…and the reported two-word query the same way",
                )

                // ---- the badge is still findable, as itself ----

                assertEquals(
                    setOf(BADGED, BADGE_ONLY),
                    search(store, ":verified:").toSet(),
                    "the badge finds every account wearing it, the name-is-a-badge account included",
                )
                assertEquals(
                    setOf(BADGED),
                    search(store, "dotardted :verified:").toSet(),
                    "name and badge together narrow to the one account that has both",
                )
                assertEquals(
                    emptySet(),
                    search(store, ":unworn:").toSet(),
                    "a badge nobody wears matches nothing — it is not the word either",
                )

                // ---- the pure-text ladder, where the gram net is not deleted ----
                //
                // With NO observer the query runs the `text` profile, which has
                // no rank-score-drop-limit: the AND-gram net over
                // search_secondary_gram reaches "verified" INSIDE the badge
                // term and those hits survive as noise. Bounded noise, and the
                // point of the ladder — the term still buys no rung, so the
                // account that says the word leads and the badges sit under it
                // instead of on the 130 000 name rung above it.
                val pureText = store.query<Event>(Filter(search = "verified", limit = 40)).map { it.id }
                assertEquals(SAYS_IT, pureText.first(), "untrusted and un-lensed, the word still leads with the word: $pureText")

                // ---- wearing a badge costs the name nothing ----

                val byName = store.eventIndex.searchRanked(EventQuery(search = "dotardted", limit = 10, minRank = 0.0))
                assertEquals(
                    setOf(BADGED, PLAIN_TWIN),
                    byName.map { it.hit.id }.toSet(),
                    "both DotardTeds answer to the name",
                )
                val badged = byName.first { it.hit.id == BADGED }.score!!
                val plain = byName.first { it.hit.id == PLAIN_TWIN }.score!!
                assertTrue(
                    abs(badged - plain) <= 0.005 * plain,
                    "the badge must not dilute the name it decorates: $badged vs $plain",
                )
            }
        }
    }

    // ------------------------------------------------------------------

    /**
     * Through the store's NIP-50 path, so BOTH rewrites run — the feed's on the
     * way in and the query's here. Under the observer lens, which is what a
     * relay always sends and what selects the trust-multiplied `search`
     * profile: it is the profile whose rank-score-drop-limit DELETES a
     * gram-only hit, and therefore the one the recall claims here are about.
     */
    private suspend fun search(
        store: VespaEventStore,
        terms: String,
    ): List<String> = store.query<Event>(Filter(search = "$terms observer:$OBSERVER", limit = 40)).map { it.id }

    /**
     * Unrelated profiles so bm25's corpus statistics resemble a relay rather
     * than a four-doc toy (the regime text_score_cutoff's comments warn is
     * unrepresentative). None contains any word of the queries.
     */
    private fun fillers(): List<Event> =
        (0 until FILLER_COUNT).map { n ->
            val id = (n + 16).toString(16).padStart(64, '0')
            val pubkey = (n + 1).toString(16).padStart(64, 'e')
            Event.fromJson(
                """{"id":"$id","pubkey":"$pubkey","created_at":${1_700_000_000L + n},"kind":0,"tags":[],""" +
                    """"content":"{\"name\":\"member$n\",\"about\":\"just another account $n\"}","sig":"${id + id}"}""",
            )
        }

    private suspend fun awaitCorpus(
        store: VespaEventStore,
        expected: Int,
    ) {
        repeat(120) {
            if (store.count(Filter(kinds = listOf(0))) >= expected) return
            delay(500)
        }
        error("corpus never became searchable ($expected docs)")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val FILLER_COUNT = 120

        /** The canonical staging observer (SearchPrefixLadderIT's, and rank_cases.json's) — a public key, no secret needed. */
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)

        /** The reported shape: a bridged account whose display name wears a declared badge. */
        val BADGED = "1".repeat(64)

        /** The account a deletion would have erased: its whole name is the badge. */
        val BADGE_ONLY = "2".repeat(64)

        /** The account that actually says the word — what a `verified` search is asking for. */
        val SAYS_IT = "3".repeat(64)

        /** Same name, no badge: the control for the dilution assertion. */
        val PLAIN_TWIN = "4".repeat(64)

        /** Same instant for every fixture — recency_mult() multiplies the score, and the twins must differ in nothing but the badge. */
        const val AT = 1_800_000_000L

        val CORPUS =
            listOf(
                profile(BADGED, "a".repeat(64), """{\"name\":\"DotardTed :verified:\"}""", badge = "verified"),
                profile(BADGE_ONLY, "b".repeat(64), """{\"name\":\":verified:\",\"about\":\"a bridged account\"}""", badge = "verified"),
                profile(SAYS_IT, "c".repeat(64), """{\"name\":\"Carol\",\"about\":\"verified human, on the record\"}"""),
                profile(PLAIN_TWIN, "d".repeat(64), """{\"name\":\"DotardTed\"}"""),
            )

        /** A kind 0, optionally carrying the NIP-30 declaration that makes its `:code:` a picture. */
        fun profile(
            id: String,
            pubkey: String,
            content: String,
            badge: String? = null,
        ): Event {
            val tags = badge?.let { """[["emoji","$it","https://static/$it.png"]]""" } ?: "[]"
            return Event.fromJson(
                """{"id":"$id","pubkey":"$pubkey","created_at":$AT,"kind":0,"tags":$tags,"content":"$content","sig":"${id + id}"}""",
            )
        }
    }
}
