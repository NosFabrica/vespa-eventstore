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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertEquals

/**
 * THE AS-YOU-TYPE regression, from a live report (2026-08-02): a verbatim
 * search-over-trust export of what `{"search":"Vitor Pamplona","limit":40}`
 * returns for the observer Vitor — ten kind-0 profiles reached through every
 * match route the ranker knows (name/display_name for the namesakes,
 * nip05/lud16 for `amethyst@vitorpamplona.com`-style identities, bios holding
 * `github.com/vitorpamplona/amethyst`). The reported bug is the LADDER on the
 * way there:
 *
 *     "Vitor P"       -> nothing
 *     "Vitor Pa"      -> nothing
 *     "Vitor Pam"     -> nothing
 *     "Vitor Pamp"    -> only 3
 *     "Vitor Pamplon" -> only 4
 *     "Vitor Pamplona"-> all 10
 *
 * Two distinct failures compose it: a trailing word under the prefix floor
 * ("P", "Pa") leaves its AND'd word group with only unsatisfiable exact
 * clauses, and the only rescue — the joined-variant prefix — could not reach
 * the docs whose match lives in nip05/lud16/website/about, because those
 * fields had no prefix-reachable sibling. So recall COLLAPSED while typing and
 * snapped back only at the exact final keystroke.
 *
 * The contract pinned here: every prefix of the typed name from "Vitor P" on
 * recalls THE SAME ten documents the finished query returns, with the
 * observer's own top-trust profile ranked first the whole way. The corpus is
 * the export verbatim (real events, real trust scores from the report), fed
 * through the store's real kind-0 extraction, padded with unrelated profiles
 * so corpus statistics (bm25 IDF) resemble a real relay rather than a
 * ten-doc toy.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class SearchPrefixLadderIT {
    @Test
    fun `every prefix from Vitor P on returns what the full query returns`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the prefix ladder IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"

                val store = VespaEventStore.open(url = queryUrl, autoDeploy = true, configUrl = configUrl)
                store.use {
                    runBlocking {
                        val export = loadExport()
                        store.batchInsert(export + fillers())
                        // The report's AUTHOR SCORES, verbatim (npub -> hex),
                        // keyed by the observer: Vitor ranking as himself.
                        VespaReputationIndex(queryUrl).use { reputation ->
                            reputation.putAll(
                                SCORES.map { (pubkey, score) ->
                                    ReputationDoc(pubkey, influenceScores = mapOf(OBSERVER to score))
                                },
                            )
                        }
                        awaitCorpus(store, export.size + FILLER_COUNT)

                        val fullQuery = labels(search(store, FULL_QUERY))
                        assertEquals(
                            LABELS.values.toSet(),
                            fullQuery.toSet(),
                            "\"$FULL_QUERY\" must return the export's ten events: $fullQuery",
                        )

                        // The ladder: from "Vitor P" on, every keystroke keeps
                        // the full result set — typing must never DROP a doc the
                        // finished query returns.
                        for (prefix in PREFIXES) {
                            val hits = labels(search(store, prefix))
                            assertEquals(
                                LABELS.values.toSet(),
                                hits.toSet(),
                                "\"$prefix\" must already return everything \"$FULL_QUERY\" returns: $hits",
                            )
                            assertEquals(
                                LABELS.getValue(OWN_PROFILE),
                                hits.first(),
                                "\"$prefix\" must rank the observer's own top-trust profile first: $hits",
                            )
                        }
                    }
                }
            }
    }

    // ------------------------------------------------------------------

    /** The export's query, through the store's NIP-50 path with the observer lens (the report's "ranking as Vitor"). */
    private suspend fun search(
        store: VespaEventStore,
        terms: String,
    ): List<Event> = store.query(Filter(search = "$terms observer:$OBSERVER", limit = 40))

    /** Hits -> the report's display names, so assertion failures read like the export. */
    private fun labels(hits: List<Event>): List<String> = hits.map { LABELS[it.id] ?: it.id.take(8) }

    private fun loadExport(): List<Event> =
        Json
            .parseToJsonElement(javaClass.getResource("/search_vitor_pamplona_export.json")!!.readText())
            .jsonArray
            .map { Event.fromJson(it.toString()) }

    /**
     * Unrelated kind-0 profiles so bm25's corpus statistics behave like a real
     * relay's: with only the ten export docs, "vitorpamplona" appears in most
     * identity fields and IDF collapses toward zero — exactly the regime the
     * schema's text_score_cutoff comments warn is unrepresentative. None of
     * these contain any substring of the query, so an exact-set assertion also
     * proves the ladder doesn't over-recall.
     */
    private fun fillers(): List<Event> =
        (0 until FILLER_COUNT).map { n ->
            val id = (n + 1).toString(16).padStart(64, '0')
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

        const val FULL_QUERY = "Vitor Pamplona"

        /** Every keystroke of the second word from the report, "Vitor P" onward. */
        val PREFIXES = (1 until "Pamplona".length).map { "Vitor " + "Pamplona".take(it) }

        /** The observer AND the top-scored author: Vitor ranking as himself (npub1gcxzte5… from the report). */
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

        /** The event the ladder must keep on top — the observer's own profile. */
        const val OWN_PROFILE = "b48187a7aa20d1607f8767c46f8656ad01d18d66a91ebb33efda712860be91d5"

        /** The export's ten result events, by id, labeled with their display names + match route. */
        val LABELS =
            mapOf(
                OWN_PROFILE to "Vitor Pamplona (name)",
                "9822d6cda8978779925dc074dd48e93e8b4499ed7ae48a50718f926ef3572fcb" to "VitorPamplona's Brainstorm Assistant (name)",
                "3d3dd4239b0ce42ab8fd70d191066ef7b1be87847f2f269ac89713b632e0fbea" to "Vitor Pamplona namesake (name)",
                "90ef4f3d994a22b1199ded3479acf6491781bfb0ea712c204e98ba66db0efa26" to "Amethyst (nip05+lud16 @vitorpamplona.com)",
                "3a7d52ee6d8316dd8d46b999524afebcdcd9619980412ab20b9d70e041769a60" to "Dr Martha Liz (nip05 @vitorpamplona.com)",
                "17a3a90efa8358a7c7a0fcb69b91616b3b4107ece24e4ed9942a93f69e1bb6ea" to "Dr. Edo Paz (lud16 vitorpamplona@getalby.com)",
                "17f37e03f2ecc4aed95df4628b9b54ac6e8b14bff13ebf7caefd1e06c834a7a3" to "Amethyst project (about github.com/vitorpamplona)",
                "399e7f558553085d3cc98ad96f989fbd1f8b9c33253435b33b37431d3f0e5d1d" to "Release notes RSS (about+nip05 github.com/vitorpamplona)",
                "647a9ee7633152b676387d12abb5f18d5f2e16f14afc44fa5904c0cf6b874ba5" to "davotoula (about github.com/vitorpamplona)",
                "6dbb8cd133d8e9a90c1cea62c27677534c2f14d63331063233bd50525761fad0" to "Measure of a Mountain (lud16 vitor@vitorpamplona.com)",
            )

        /** AUTHOR SCORES UNDER THIS LENS from the export, npubs decoded to hex. */
        val SCORES =
            mapOf(
                "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c" to 100,
                "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377" to 26,
                "3cddee78ad379131cf633de94f6ca325d91d73e0d8f79fb90d7849487d3e5ac5" to 9,
                "aa9047325603dacd4f8142093567973566de3b1e20a89557b728c3be4c6a844b" to 97,
                "e7764a227c12ac1ef2db79ae180392c90903b2cec1e37f5c1a4afed38117185e" to 45,
                "f8ff11c7a7d3478355d3b4d174e5a473797a906ea4aa61aa9b6bc0652c1ea17a" to 48,
                "98b5514d1806cec91e771b6d89052e9a878e809099b3839d604e3e1a75230a3f" to 47,
                "05acfe7f6f21994ed3f58d7b4820f865482f60166c8cbd18b875b8aa436f6688" to 5,
                "1aa25ae8479f23895bf8fab5c94d9619f2fe3d71cd3d9031e4f49e0fa87c8967" to 14,
                "6c7bc8d6dbc6d9b06c509475cfa96ae5635e2fba26daaae8d47ecbbcbcd0bff4" to 3,
            )

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
