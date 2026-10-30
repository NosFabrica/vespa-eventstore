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
package com.nosfabrica.vespa.eventstore

import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.client.FeedCompression
import com.nosfabrica.vespa.eventstore.engine.client.RecencyStrategy
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [EngineTuning.fromEnv] is the STRICT reading the store's own lenient one never
 * was: a value it cannot parse stops the boot, every bad one named at once, and
 * unset or blank means "the default" — the contract vespa-relay re-implemented
 * per variable before the store had it.
 */
class EngineTuningTest {
    private val mock = MockVespaEngine()

    @AfterTest
    fun stopMock() = mock.stop()

    @Test
    fun `every variable parses into its field`() {
        val t =
            EngineTuning.fromEnv(
                mapOf(
                    "VESPA_FEED_BATCH" to "0",
                    "VESPA_FEED_CONNECTIONS" to "16",
                    "VESPA_FEED_COMPRESSION" to "NONE",
                    "VESPA_FEED_STREAMS" to "64",
                    "VESPA_FEED_INFLIGHT_FACTOR" to "2",
                    "VESPA_VISIT_SLICES" to "12",
                    "VESPA_VISIT_CONCURRENCY" to "100",
                    "VESPA_VISIT_STREAM" to "off",
                    "VESPA_QUERY_PLANNER" to "no",
                    "VESPA_RECENCY_STRATEGY" to " Match_Phase ",
                    "VESPA_ID_FIRST" to "0",
                    "VESPA_RECENCY_MEMORY" to "false",
                    "VESPA_BODY_EDGE" to "yes",
                    "VESPA_ADDRESS_KEYED" to "1",
                    "VESPA_UNBOUNDED_HITS" to "400",
                    "GUARD_OWNERS_DISABLE" to "true",
                    "VESPA_QUERY_FANOUT" to "8",
                    "VESPA_PUT_FANOUT" to "16",
                    "VESPA_MATCH_PHASE_MAX_HITS" to "10000",
                    "VESPA_DEDUP_CHUNK" to "2000",
                    "VESPA_TRUST_WALK_PAGE" to "100",
                    "VESPA_TRUST_GATE_SLICE" to "50",
                    "VESPA_ACCESS_LOG" to "Disabled",
                ),
            )
        assertEquals(0, t.feedBatch)
        assertEquals(16, t.feedConnections)
        assertEquals(FeedCompression.NONE, t.feedCompression)
        assertEquals(64, t.feedStreams)
        assertEquals(2, t.feedInflightFactor)
        assertEquals(12, t.visitSlices)
        assertEquals(100, t.visitConcurrency)
        assertEquals(false, t.visitStreaming)
        assertEquals(false, t.queryPlanning)
        assertEquals(RecencyStrategy.MATCH_PHASE, t.recencyStrategy)
        assertEquals(false, t.idFirst)
        assertEquals(false, t.recencyMemory)
        assertEquals(true, t.bodyEdgeFeed)
        assertEquals(true, t.addressKeyed)
        assertEquals(400, t.unboundedHits)
        assertEquals(true, t.guardOwnersDisabled)
        assertEquals(8, t.queryFanout)
        assertEquals(16, t.putFanout)
        assertEquals(10_000, t.matchPhaseMaxHits)
        assertEquals(2_000, t.dedupChunk)
        assertEquals(100, t.trustWalkPage)
        assertEquals(50, t.trustGateSlice)
        assertEquals("disabled", t.accessLog)
    }

    @Test
    fun `unset and blank are the default, so an empty environment names nothing`() {
        val t = EngineTuning.fromEnv(mapOf("VESPA_FEED_BATCH" to "   ", "VESPA_DEDUP_CHUNK" to ""))
        assertNull(t.feedBatch)
        assertNull(t.dedupChunk)
        assertEquals("EngineTuning()", EngineTuning.fromEnv(emptyMap()).toString())
    }

    /** The lenient reads these replace: `off` turned batching off, `5OO` kept 500, `5000` was split again. */
    @Test
    fun `garbage stops the boot, every bad variable named at once`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                EngineTuning.fromEnv(
                    mapOf(
                        "VESPA_FEED_BATCH" to "off",
                        "VESPA_DEDUP_CHUNK" to "5OO",
                        "VESPA_VISIT_CONCURRENCY" to "101",
                        "VESPA_FEED_COMPRESSION" to "zstd",
                        "VESPA_ID_FIRST" to "maybe",
                        "VESPA_RECENCY_STRATEGY" to "fastest",
                        "VESPA_QUERY_FANOUT" to "0",
                        "VESPA_ACCESS_LOG" to "json",
                    ),
                )
            }
        val m = e.message!!
        for (key in listOf("VESPA_FEED_BATCH='off'", "VESPA_DEDUP_CHUNK='5OO'", "VESPA_VISIT_CONCURRENCY='101'", "VESPA_FEED_COMPRESSION='zstd'", "VESPA_ID_FIRST='maybe'", "VESPA_RECENCY_STRATEGY='fastest'", "VESPA_QUERY_FANOUT='0'", "VESPA_ACCESS_LOG='json'")) {
            assertTrue(key in m, "$key in: $m")
        }
        assertTrue("8 bad setting(s)" in m, m)
        assertFailsWith<IllegalArgumentException>("past the feed chunk a batch is split again") { EngineTuning.fromEnv(mapOf("VESPA_FEED_BATCH" to "4097")) }
        assertEquals(EngineTuning.MAX_FEED_BATCH, EngineTuning.fromEnv(mapOf("VESPA_FEED_BATCH" to "4096")).feedBatch)
    }

    @Test
    fun `the ranges hold for values built in code too`() {
        assertFailsWith<IllegalArgumentException> { EngineTuning(feedBatch = -1) }
        assertFailsWith<IllegalArgumentException> { EngineTuning(dedupChunk = 0) }
        assertFailsWith<IllegalArgumentException> { EngineTuning(accessLog = "json") }
    }

    @Test
    fun `the constants consumers mirrored are the ones the code runs on`() {
        assertEquals(100, EngineTuning.DEFAULT_FEED_BATCH)
        assertEquals(4_096, EngineTuning.MAX_FEED_BATCH)
        assertEquals(500, EngineTuning.DEFAULT_DEDUP_CHUNK)
        assertEquals(EngineTuning.VARIABLES.size, EngineTuning.VARIABLES.toSet().size, "each variable once")
        assertEquals(23, EngineTuning.VARIABLES.size)
    }

    /** A per-store setting reaches the store it opens: feedBatch 0 leaves the batch path unbuilt. */
    @Test
    fun `open applies the per-store settings`() {
        VespaEventStore.open(url = mock.url, autoDeploy = false).use { default ->
            assertTrue(" batch " in default.feedStatus(), "the default batches: ${default.feedStatus()}")
        }
        VespaEventStore.open(url = mock.url, autoDeploy = false, tuning = EngineTuning(feedBatch = 0)).use { unbatched ->
            assertFalse("batch" in unbatched.feedStatus(), "feedBatch = 0 writes one per request: ${unbatched.feedStatus()}")
        }
    }

    @Test
    fun `a config url is the query host on 19071`() {
        assertEquals("http://vespa.internal:19071", VespaEventStore.deriveConfigUrl("http://vespa.internal:8080"))
        assertEquals("https://10.0.0.5:19071", VespaEventStore.deriveConfigUrl("https://10.0.0.5"))
    }
}
