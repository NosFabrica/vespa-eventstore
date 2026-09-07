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
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
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
import kotlin.test.assertTrue

/**
 * THE `idtimeauthor` SUMMARY EXISTS AND PROJECTS WHAT IT CLAIMS.
 *
 * `EventIndex.newestPerAuthor` reads (id, created_at, pubkey) through a
 * document-summary declared in `event.sd`. Only a real Vespa can hold that up:
 * a summary class that the schema does not declare is a 400 at query time, and
 * a class that omits a field returns it EMPTY rather than failing — the second
 * being the quieter of the two, since the client drops a hit with a blank
 * pubkey and the answer comes back simply missing that author.
 *
 * `MockVespaEngine` can prove neither. It serves whatever fields it holds
 * regardless of the summary asked for, so the unit test beside this one pins
 * the summary NAME on the wire and stops exactly where the schema begins.
 *
 * The corpus is deliberately shaped so the two ways to be wrong are separable:
 * one author whose newest version is not its first-written, and two authors
 * tied on `created_at` so the NIP-01 tiebreak (lowest id) has something to
 * decide. A projection that lost `pubkey` fails the key set; one that lost the
 * ordering fails the winners.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class NewestPerAuthorIT {
    @Test
    fun `the newest version per author comes back off attributes alone`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the newest-per-author IT")

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
                        // ALICE: three versions, and the newest is neither the
                        // first nor the last written — so "first hit wins" and
                        // "last hit wins" both fail.
                        val aliceOld = note(1, ALICE, at = 100)
                        val aliceNew = note(2, ALICE, at = 300)
                        val aliceMid = note(3, ALICE, at = 200)
                        // BOB: a tie on created_at. NIP-01 gives it to the
                        // LOWEST id, which is note 4.
                        val bobLow = note(4, BOB, at = 500)
                        val bobHigh = note(5, BOB, at = 500)
                        // CAROL is asked about but holds nothing.
                        val corpus = listOf(aliceOld, aliceNew, aliceMid, bobLow, bobHigh)
                        index.putAll(corpus)
                        awaitCorpus(index, corpus.size)

                        val newest = index.newestPerAuthor(EventQuery(kinds = listOf(1), authors = listOf(ALICE, BOB, CAROL)))

                        // The key set proves `pubkey` really came back: a
                        // summary missing it yields blank keys, which the
                        // client drops — an EMPTY map, not a wrong one.
                        assertEquals(setOf(ALICE, BOB), newest.keys, "one entry per author that holds an event, and pubkey must survive the projection")
                        assertEquals(aliceNew.id, newest[ALICE]?.id, "the newest created_at wins, whenever it was written")
                        assertEquals(300L, newest[ALICE]?.createdAt, "and its stamp comes back with it")
                        assertEquals(bobLow.id, newest[BOB]?.id, "a tie goes to the LOWEST id, as putIfNewer would resolve it")
                        assertEquals(500L, newest[BOB]?.createdAt)

                        // An author set nobody matches is an empty map, not the
                        // whole corpus — the failure mode of a dropped filter.
                        assertEquals(emptyMap(), index.newestPerAuthor(EventQuery(kinds = listOf(1), authors = listOf(CAROL))), "no matches means no entries")
                    }
                }
            }
    }

    /**
     * THE SAME PROJECTION, ON REAL EVENTS OF THE KIND IT ACTUALLY PROBES.
     *
     * The case above is synthetic and kind 1, because it needs two versions
     * per author and a controlled tie — a replaceable kind's own supersession
     * would remove both out from under it. The cost of that control is that it
     * proves nothing about the documents this read exists to AVOID reading:
     * real profiles, with real content, under a real store's write path.
     *
     * So this half feeds the staging export — ten kind-0 profiles from ten
     * distinct pubkeys, captured off search-staging — through
     * `VespaEventStore.batchInsert`, and asks the front door's own handle. It
     * covers what the synthetic case cannot: kind 0 is a REPLACEABLE kind, so
     * these docs are address-keyed and the id attribute is what carries the
     * event id; `EngineReads` is exercised against a deployed schema for the
     * first time; and the pubkeys are 64-hex keys that came off the wire, not
     * `"11".repeat(32)`.
     *
     * The assertion that matters is the ROUND TRIP: every author in the export
     * answers, and each with the id of the event that author actually
     * published. A projection that dropped `pubkey` yields an empty map; one
     * that returned the docid rather than the id attribute yields ids that
     * match no event in the file.
     */
    @Test
    fun `real captured profiles round-trip through the front door's handle`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the newest-per-author export IT")

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
                        val export = loadExport()
                        assertTrue(export.all { it.kind == 0 } && export.size >= 10, "the fixture is the kind-0 capture this case is written around")
                        store.batchInsert(export)

                        val authors = export.map { it.pubKey }.distinct()
                        val published = export.associate { it.pubKey to it.id }
                        awaitProfiles(store, authors, export.size)

                        val newest = store.engine.newestPerAuthor(EventQuery(kinds = listOf(0), authors = authors))

                        assertEquals(authors.toSet(), newest.keys, "every captured author must come back, so pubkey survived the projection")
                        assertEquals(published, newest.mapValues { (_, held) -> held.id }, "and each author's id must be the one they published")
                    }
                }
            }
    }

    /** The staging capture: real kind-0 profiles, one per author. */
    private fun loadExport(): List<Event> =
        Json
            .parseToJsonElement(javaClass.getResource("/search_vitor_pamplona_export.json")!!.readText())
            .jsonArray
            .map { Event.fromJson(it.toString()) }

    private suspend fun awaitProfiles(
        store: VespaEventStore,
        authors: List<String>,
        expected: Int,
    ) {
        repeat(120) {
            if (store.engine.count(EventQuery(kinds = listOf(0), authors = authors)) >= expected) return
            delay(500)
        }
        error("the captured profiles never became searchable ($expected docs)")
    }

    /** A kind-1 note by [pubkey] stamped [at]; ids ascend with [n], which is what the tiebreak turns on. */
    private fun note(
        n: Int,
        pubkey: String,
        at: Long,
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = at,
        kind = 1,
        tags = emptyList(),
        content = "note $n",
        sig = "e".repeat(128),
        search = SearchFields(text = "note"),
    )

    private suspend fun awaitCorpus(
        index: VespaEventIndex,
        expected: Int,
    ) {
        repeat(120) {
            if (index.count(EventQuery(kinds = listOf(1))) >= expected) return
            delay(500)
        }
        error("corpus never became searchable ($expected docs)")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        val ALICE = "11".repeat(32)
        val BOB = "22".repeat(32)
        val CAROL = "33".repeat(32)

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
