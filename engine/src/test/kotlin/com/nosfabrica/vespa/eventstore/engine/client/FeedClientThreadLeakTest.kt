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
package com.nosfabrica.vespa.eventstore.engine.client

import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.OperationParameters
import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * #199: while Vespa refused writes, a relay gained Jetty feed-client threads for good. The
 * client's default circuit breaker half-opens after 10 s without a success, and each half-open
 * replaces the client's whole Jetty cluster; a replaced cluster with nothing in flight is never
 * closed, not even by `close()`. One refused write and the 10 s grace are enough to leak one.
 *
 * Slow on purpose (~11 s): the grace being defended against is the library's default.
 */
class FeedClientThreadLeakTest {
    private val mock = MockVespaEngine()

    @AfterTest
    fun stop() = mock.stop()

    /** Live threads of the feed client's own Jetty pools (`QueuedThreadPool.setName("feedclient")`). */
    private fun feedThreads(): Set<Thread> = Thread.getAllStackTraces().keys.filterTo(HashSet()) { it.isAlive && it.name.startsWith("feedclient-") }

    @Test
    fun aFeedBlockLeavesNoFeedClientThreadsBehindAfterClose() {
        // Vespa's answer to a write during a feed block: 507, which the client counts as a failure
        // and does not retry.
        mock.updateReply = 507 to
            """{"message":"ReturnCode(NO_SPACE, External feed is blocked due to resource exhaustion: memory on node 0 (0.900 > 0.800))"}"""
        val before = feedThreads()
        val feed = VespaFeed(listOf("http://127.0.0.1:${mock.port}"))
        val refused =
            runCatching {
                feed.client.update(DocumentId.of("reputation", "reputation", "x"), """{"fields":{}}""", OperationParameters.empty()).get()
            }
        assertTrue(refused.isFailure, "the blocked write is refused")
        assertEquals(1, mock.updateRequests, "and refused once: a 507 is not retried")
        // Past the default breaker's 10 s grace: with it, the strategy has swapped clusters by now.
        Thread.sleep(10_500)
        feed.close()

        // close() stops its pool, whose threads exit within moments; poll rather than race them.
        val deadline = System.nanoTime() + 5_000_000_000
        var left = feedThreads() - before
        while (left.isNotEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(50)
            left = feedThreads() - before
        }
        assertEquals(emptyList(), left.map { it.name }.sorted(), "feed-client threads outlived close()")
    }
}
