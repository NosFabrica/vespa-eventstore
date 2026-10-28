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

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.security.MessageDigest
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * A `created_at` SECOND TOO DENSE FOR THE ID WALK'S CURSOR IS SPLIT BY ID
 * PREFIX, ON A REAL VESPA.
 *
 * NIP-85 providers sign a whole run on one second — staging held 49 seconds
 * of 80k to 220k cards each in one month — and the walk used to hand such a
 * second to a document-API scan that read the whole corpus: 22 to 51 minutes
 * a time on 516M documents (NosFabrica/vespa-eventstore#185). It now reads
 * the second in id-prefix buckets through the attribute index.
 *
 * What only the engine can say, and this pins: that
 * `id contains ({prefix:true}"…")` matches by PREFIX on an attribute declared
 * `match { cased }` with `dictionary { hash }` — a dictionary that cannot seek
 * a prefix — so the sixteen buckets sum to the second exactly, and a walk
 * through them serves the scan's id set with no scan at all — and a LIMITED
 * walk stopping inside the second keeps its lowest ids. The mock parses
 * the clause back into the in-memory spec, so it would agree with any
 * grammar EventYql emitted.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class DenseSecondWalkIT {
    @Test
    fun `a dense second is walked whole through id-prefix buckets, with no scan`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the dense-second walk IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}").deployIfAbsent(queryUrl)
                runBlocking {
                    val corpus = corpus()
                    VespaEventIndex(queryUrl).use { index ->
                        corpus.chunked(500).forEach { index.putAll(it) }
                        awaitCorpus(index, corpus.size)

                        // The issue's own check: the sixteen one-digit buckets
                        // sum to the second, and each holds only its prefix.
                        val second = EventQuery(kinds = listOf(KIND), authors = listOf(PROVIDER), since = DENSE_AT, until = DENSE_AT)
                        assertEquals(TIED, index.count(second), "the dense second")
                        var summed = 0
                        for (digit in "0123456789abcdef") {
                            val bucket = index.search(second.copy(idPrefix = "$digit"))
                            assertEquals(true, bucket.all { it.id.startsWith(digit) }, "bucket $digit holds only its prefix")
                            summed += bucket.size
                        }
                        assertEquals(TIED, summed, "the sixteen prefix buckets sum to the second EXACTLY")
                    }

                    // Two cursor sizes: one split (buckets of ~190 against a cap
                    // of 401), and two levels (the same buckets against 81).
                    for (pageSize in listOf(100, 20)) {
                        IngestStats.reset()
                        VespaEventIndex(queryUrl, idPageSize = pageSize).use { walker ->
                            val got = ArrayList<DocRef>()
                            walker.visitIds(EventQuery(kinds = listOf(KIND), authors = listOf(PROVIDER))) { page ->
                                got += page
                                true
                            }
                            assertEquals(corpus.size, got.size, "pageSize=$pageSize: every id once")
                            assertEquals(corpus.map { it.id }.toSet(), got.map { it.id }.toSet(), "pageSize=$pageSize: exactly the corpus")
                            val stages = IngestStats.snapshot()
                            assertNotNull(stages["walk.ties.dense"], "pageSize=$pageSize: the second was too dense for the cursor")
                            assertNotNull(stages["walk.ties.prefix"], "pageSize=$pageSize: and was read by prefix")
                            assertNull(stages["walk.scan"], "pageSize=$pageSize: with no scan")
                        }
                    }

                    // A LIMITED walk stopping inside the second keeps NIP-01's
                    // members — its lowest ids — not the ones the engine's
                    // single-key sort reached first. Each path: one overfetched
                    // page whose tie read is over its cap (limit 200 inside
                    // 250-id pages, cap 1,001), and the budgeted cursor at one
                    // split (cap 401) and at two (cap 81).
                    val nip01 = corpus.sortedWith(compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id })
                    for ((pageSize, limit) in listOf(250 to 200, 100 to 1_000, 20 to 1_000)) {
                        IngestStats.reset()
                        VespaEventIndex(queryUrl, idPageSize = pageSize).use { walker ->
                            val limited = ArrayList<DocRef>()
                            walker.visitIds(EventQuery(kinds = listOf(KIND), authors = listOf(PROVIDER), limit = limit)) { page ->
                                limited += page
                                true
                            }
                            assertEquals(
                                nip01.take(limit).map { it.id }.toSet(),
                                limited.map { it.id }.toSet(),
                                "pageSize=$pageSize limit=$limit: the newest, lowest-id first",
                            )
                            assertNotNull(IngestStats.snapshot()["walk.ties.prefix"], "pageSize=$pageSize limit=$limit: the second was split")
                        }
                    }
                    IngestStats.reset()
                }
            }
    }

    /**
     * One provider's run: [TIED] cards on [DENSE_AT], with [AROUND] more on the
     * seconds either side so the walk must hand back to the cursor. Ids are
     * hashes, as a real event's are — the split relies on them spreading over
     * the hex digits — and each card a distinct `d`, as a provider's are.
     */
    private fun corpus(): List<EventDoc> =
        (0 until TIED + 2 * AROUND).map { n ->
            val at =
                when {
                    n < TIED -> DENSE_AT
                    n < TIED + AROUND -> DENSE_AT + 1 + (n - TIED)
                    else -> DENSE_AT - 1 - (n - TIED - AROUND)
                }
            val subject = n.toString(16).padStart(64, '5')
            EventDoc(
                id = sha256("card $n"),
                pubkey = PROVIDER,
                createdAt = at,
                kind = KIND,
                tags = listOf(listOf("d", subject), listOf("rank", "${n % 100}")),
                content = "",
                sig = "e".repeat(128),
            )
        }

    private fun sha256(s: String): String = MessageDigest.getInstance("SHA-256").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }

    private suspend fun awaitCorpus(
        index: VespaEventIndex,
        expected: Int,
    ) {
        repeat(120) {
            if (index.count(EventQuery(kinds = listOf(KIND))) >= expected) return
            delay(500)
        }
        error("corpus never became searchable ($expected docs)")
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val KIND = 30382
        const val PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"
        const val DENSE_AT = 1_793_074_186L
        const val TIED = 3_000
        const val AROUND = 150

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
