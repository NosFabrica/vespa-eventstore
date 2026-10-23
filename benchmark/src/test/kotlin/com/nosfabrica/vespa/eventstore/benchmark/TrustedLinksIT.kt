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
import com.nosfabrica.vespa.eventstore.engine.LinkTarget
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.search.LinkRerank
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import com.vitorpamplona.quartz.utils.EventFactory
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
 * TRUSTED LINKS ON A REAL VESPA — what only the engine can answer about the
 * `sort:links` rerank (store/search/LinkRerank.kt, engine/LinkScore.kt):
 *
 *  - `EventIndex.scoreLinks`' three reads DEPLOY and parse: the people grouping
 *    (key -> author -> kind, `filter(regex())`, the `link_trust` profile), the
 *    zap receipts read as documents, and the zappers' cells off the
 *    `reputation` schema's `trust_cell` profile;
 *  - THE GATE REACHES THE GROUPING at `hits=0` — below-floor and unranked
 *    authors are not people at all (a `totalCount` at `hits=0` skips the
 *    ranking pass on Vespa 8.731, see VespaEventIndex.count, so this is pinned,
 *    not assumed);
 *  - the per-person policy, number for number against the hand-worked sums in
 *    the comments: repeats halve, the author is no one, a person is their trust
 *    times their capped links, a zap belongs to its `P` zapper at the ZAPPER's
 *    trust, an untrusted zapper or an untrusted LNURL signer counts nothing, a
 *    receipt without `P` counts once at its signer's trust, a report is −1;
 *  - end to end through `open()`: a search page reorders by those scores — an
 *    under-ranked note people engaged with overtakes one with twenty untrusted
 *    reactions and a trusted report, even at `limit 1` (the widened recall is
 *    what lets it in) — and `sort:links:0` is the engine's order.
 *
 * Tagged `integration`; run with `-Pintegration` where Docker is available.
 */
@Tag("integration")
class TrustedLinksIT {
    @Test
    fun `links are scored per person through the lens, zaps by their zapper, and a page reorders by them`() {
        assumeTrue(dockerAvailable(), "Docker not available - skipping the trusted links IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                SchemaDeployer(configUrl).deployIfAbsent(queryUrl)
                VespaEventStore.open(url = queryUrl, autoDeploy = false, configUrl = configUrl).use { store ->
                    runBlocking {
                        val now = System.currentTimeMillis() / 1000
                        store.batchInsert(
                            listOf(
                                TrustProviderListEvent(hexId(), OBSERVER, now - 5_000, arrayOf(arrayOf("30382:rank", PROVIDER, "wss://scores.test/")), "", ""),
                                card(AUTHOR, 50, now),
                                card(TRUSTED, 50, now),
                                card(HIGH, 100, now),
                                card(LOW, 1, now),
                                card(ZAP_SERVICE, 80, now),
                                card(BAD_SERVICE, 1, now),
                                card(ZAPPER, 50, now),
                            ),
                        )
                        val dusk = event(AUTHOR, 1, "zebra crossing at dusk", now - 3_600)
                        val dawn = event(AUTHOR, 1, "zebra crossing at dawn", now - 30 * 86_400)
                        val article = event(AUTHOR, 30023, "a zebra guide", now - 7_200, arrayOf("d", "zebra-guide"))
                        store.batchInsert(listOf(dusk, dawn, article))
                        store.awaitTrustProjection()

                        val search = "zebra crossing observer:$OBSERVER"
                        val base = store.query<Event>(Filter(kinds = listOf(1), search = search, limit = 2)).map { it.id }
                        assertEquals(2, base.size, "both notes recall through the lens: $base")
                        val (top, under) = base
                        val address = "30023:$AUTHOR:zebra-guide"

                        fun zap(
                            signer: String,
                            target: String,
                            zapper: String?,
                        ) = event(signer, 9735, "", now - 40, arrayOf("e", target), *listOfNotNull(zapper?.let { arrayOf("P", it) }).toTypedArray())
                        store.batchInsert(
                            // under:
                            List(5) { event(TRUSTED, 7, "+", now - 60 + it, arrayOf("e", under)) } + // 1.9375 ...
                                List(2) { event(TRUSTED, 1, "agreed $it", now - 50 + it, arrayOf("e", under)) } + // ... + 1.5 = 3.4375 x 0.5 = 1.71875
                                event(HIGH, 7, "+", now - 50, arrayOf("e", under)) + // 1 x 1.0
                                List(3) { event(AUTHOR, 1, "thanks $it", now - 45 + it, arrayOf("e", under)) } + // the author: no one
                                List(3) { zap(ZAP_SERVICE, under, ZAPPER) } + // the zapper's: 1.75 x 0.5 = 0.875
                                zap(ZAP_SERVICE, under, null) + // no P: 1 x the signer's 0.8
                                zap(ZAP_SERVICE, under, UNRANKED) + // a zapper the lens does not rank: nothing
                                zap(BAD_SERVICE, under, ZAPPER) + // an untrusted signer: nothing
                                zap(ZAP_SERVICE, under, AUTHOR) + // a self-zap: no one
                                // top:
                                List(20) { event(LOW, 7, "+", now - 60 + it, arrayOf("e", top)) } + // below the floor: nothing
                                event(UNRANKED, 7, "+", now - 30, arrayOf("e", top)) + // unranked: nothing
                                event(TRUSTED, 1984, "", now - 5, arrayOf("e", top, "spam")) + // a report: -1 x 0.5
                                event(HIGH, 10000, "", now - 5, arrayOf("e", top)) + // a mute list naming it: nothing
                                // the article, by its address:
                                event(TRUSTED, 1, "good read", now - 10, arrayOf("a", address)) + // 1 x 0.5
                                event(LOW, 1, "meh", now - 10, arrayOf("a", address)) +
                                // ONE like naming it by id AND address (NIP-25): in both key groups, counted once — 1 x 1.0
                                event(HIGH, 7, "+", now - 10, arrayOf("e", article.id), arrayOf("a", address)) +
                                // a quote-reply naming it by `e` AND `q`: one more kind, one link — 1 x 1.0
                                event(HIGH, 1, "worth reading", now - 8, arrayOf("e", article.id), arrayOf("q", article.id)),
                        )

                        val targets = listOf(LinkTarget(top, AUTHOR), LinkTarget(under, AUTHOR), LinkTarget(article.id, AUTHOR, address))
                        val lensed = EventQuery(observer = OBSERVER, rankKey = PROVIDER, minRank = 2.0, ranking = EventYql.RANK_RECENCY_GATED)
                        val scored = store.engine.scoreLinks(targets, lensed, LinkRerank.DEFAULT_WEIGHTS)
                        assertEquals(1.71875 + 1.0 + 0.875 + 0.8, scored.getValue(under).score, 1e-9, "under: ${scored[under]}")
                        assertEquals(4, scored.getValue(under).people, "trusted, high, the zapper, and one unnamed zap")
                        assertEquals(mapOf(7 to 6, 1 to 2, 9735 to 4), scored.getValue(under).byKind)
                        assertEquals(-0.5, scored.getValue(top).score, 1e-9, "top's twenty-one reactions are below the floor; its report is all that counts")
                        assertEquals(0.5 + 1.0 + 1.0, scored.getValue(article.id).score, 1e-9, "linked by address, a two-key like and a quote-reply once each: ${scored[article.id]}")

                        // No lens: everyone fully trusted, nothing gated — but the
                        // author, the decay and the per-zapper attribution all still hold.
                        val open = store.engine.scoreLinks(targets, EventQuery(), LinkRerank.DEFAULT_WEIGHTS)
                        // under: trusted 3.4375 + high 1 + the zapper's four receipts (3 + the bad
                        // signer's) 1.875 + no-P 1 + the unranked zapper 1.
                        assertEquals(3.4375 + 1.0 + 1.875 + 1.0 + 1.0, open.getValue(under).score, 1e-9)
                        // top: low's twenty (2 − 2^-19) + unranked 1 − the report 1.
                        assertEquals(2.0 - Math.pow(2.0, -19.0), open.getValue(top).score, 1e-9)

                        assertEquals(base, store.query<Event>(Filter(kinds = listOf(1), search = search, limit = 2)).map { it.id }, "no token, the engine's order")
                        assertEquals(listOf(under, top), store.query<Event>(Filter(kinds = listOf(1), search = "$search sort:links", limit = 2)).map { it.id })
                        assertEquals(listOf(under), store.query<Event>(Filter(kinds = listOf(1), search = "$search sort:links", limit = 1)).map { it.id }, "the widened recall lifts a hit the limit alone would have cut")
                        assertEquals(base, store.query<Event>(Filter(kinds = listOf(1), search = "$search sort:links:0", limit = 2)).map { it.id }, "strength 0 is the engine's order")
                    }
                }
            }
    }

    private fun card(
        subject: String,
        rank: Int,
        now: Long,
    ) = UserAssertionEvent(hexId(), PROVIDER, now - 4_000, arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString())), "", "")

    private fun event(
        author: String,
        kind: Int,
        content: String,
        at: Long,
        vararg tags: Array<String>,
    ): Event = EventFactory.create(hexId(), author, at, kind, arrayOf(*tags), content, "e".repeat(128))

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        val OBSERVER = "c".repeat(64)
        val PROVIDER = "5e".repeat(32)
        val AUTHOR = "a0".repeat(32)
        val TRUSTED = "b1".repeat(32)
        val LOW = "b2".repeat(32)
        val UNRANKED = "b3".repeat(32)
        val HIGH = "b4".repeat(32)
        val ZAP_SERVICE = "d1".repeat(32)
        val BAD_SERVICE = "d2".repeat(32)
        val ZAPPER = "f1".repeat(32)

        private var seq = 0

        fun hexId() = (++seq).toString(16).padStart(64, 'a')

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
