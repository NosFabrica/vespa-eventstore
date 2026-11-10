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

import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Each index builds its own OkHttp client ([VespaHttp]): a dispatcher whose threads are not
 * daemons, and a connection pool keeping every HTTP/2 connection (and its reader thread) for five
 * minutes. Closing the index used to close only its feed client, so a store opened and closed in
 * a loop — a test suite, a benchmark sweep — kept every one of them.
 */
class OkHttpThreadLeakTest {
    private val mock = MockVespaEngine()

    @AfterTest
    fun stop() = mock.stop()

    /**
     * Live OkHttp threads this client can own: its dispatcher's, and an HTTP/2 reader (named for
     * its connection while it runs). Not the `OkHttp TaskRunner` threads idle between tasks:
     * that runner is one per process, shared by every client, and idles out on its own.
     */
    private fun okHttpThreads(): Set<Thread> = Thread.getAllStackTraces().keys.filterTo(HashSet()) { it.isAlive && it.name.startsWith("OkHttp") && it.name != "OkHttp TaskRunner" }

    @Test
    fun closingTheIndexesLeavesNoOkHttpThreadsBehind() {
        val before = okHttpThreads()
        val events = VespaEventIndex(mock.url, eagerFeed = false)
        val reputations = VespaReputationIndex(mock.url, eagerFeed = false)
        // One read each: a dispatcher thread and an HTTP/2 connection per client. The mock does
        // not model reputation documents, so that read is answered 400: a round trip all the same.
        runBlocking {
            assertNull(events.get("a".repeat(64)))
            assertTrue(
                runCatching { reputations.get("b".repeat(64)) }
                    .exceptionOrNull()
                    ?.message
                    .orEmpty()
                    .contains("400"),
            )
        }
        assertTrue((okHttpThreads() - before).isNotEmpty(), "the reads started OkHttp threads")
        events.close()
        reputations.close()

        // Shut-down executors and closed sockets let their threads go within moments; poll rather than race them.
        val deadline = System.nanoTime() + 5_000_000_000
        var left = okHttpThreads() - before
        while (left.isNotEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(50)
            left = okHttpThreads() - before
        }
        assertEquals(emptyList(), left.map { "${it.name} (daemon=${it.isDaemon})" }.sorted(), "OkHttp threads outlived close()")
    }
}
