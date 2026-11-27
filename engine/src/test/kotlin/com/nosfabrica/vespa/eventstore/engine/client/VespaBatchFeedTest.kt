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
import com.nosfabrica.vespa.eventstore.engine.WriterLockHeld
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.time.Duration
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * What a write under the store's writer lock ([WriterLockHeld]) may wait for, engine-side: one
 * ask of a refusing engine instead of [VespaHttp.QUERY_RETRIES] back-offs, a batch the handler
 * never answers handed back within [VespaBatchFeed]'s call bound, and the feed client's re-put
 * of what it handed back bounded by [VespaEventIndex.LOCKED_FALLBACK_TIMEOUT] instead of
 * [VespaEventIndex.FEED_TIMEOUT]. The store-level twin, with a second writer queued behind the
 * first, is `WriterLockRetryHoldTest`.
 */
class VespaBatchFeedTest {
    private val mock = MockVespaEngine()
    private var seq = 0

    @AfterTest
    fun tearDown() {
        mock.stop()
    }

    private fun doc() =
        EventDoc(
            id = (++seq).toString(16).padStart(64, '0'),
            pubkey = "a1".repeat(32),
            createdAt = 1_000L + seq,
            kind = 1,
            tags = emptyList(),
            content = "hello",
            sig = "e".repeat(128),
            owner = "a1".repeat(32),
            search = SearchFields.NONE,
        )

    private val body = { d: EventDoc -> d.feedJson(includeNear = true, includeEdge = false, includeStem = true) }

    @Test
    fun `under a writer lock a refused request is asked once, unlocked it is retried`() =
        runBlocking<Unit> {
            val asked = AtomicInteger()
            mock.answerWhere = { _, path, _ -> if (path.startsWith("/document/v1/")) 503.also { asked.incrementAndGet() } else null }
            val http = VespaHttp()
            try {
                val url = "${mock.url}/document/v1/event/event/docid/${"1".repeat(64)}"
                val started = System.nanoTime()
                val locked = withTimeout(20_000) { withContext(WriterLockHeld) { http.get(url) } }
                val lockedMs = (System.nanoTime() - started) / 1_000_000
                assertEquals(503, locked.statusCode(), "surfaced as the last retry's answer is")
                assertEquals(1, asked.get(), "one ask under the lock, no back-off ($lockedMs ms)")
                // Under the first back-off (500 ms), however loaded the box.
                assertTrue(lockedMs < 500, "no back-off paid under the lock: $lockedMs ms")

                asked.set(0)
                assertEquals(503, withTimeout(20_000) { http.get(url) }.statusCode())
                assertEquals(1 + VespaHttp.QUERY_RETRIES, asked.get(), "an unlocked read keeps its retries")
            } finally {
                http.close()
            }
        }

    @Test
    fun `a batch the handler never answers is handed back within the call bound, naming its timeout`() =
        runBlocking<Unit> {
            val queries = Collections.synchronizedList(ArrayList<String?>())
            mock.answerWhere = { method, path, query ->
                if (method == "POST" && path == VespaBatchFeed.PATH) queries += query
                null
            }
            val gate = CountDownLatch(1)
            mock.holdWrites = gate
            val http = VespaHttp()
            try {
                val batches = VespaBatchFeed(http, { mock.url }, batchSize = 100, concurrency = 1, timeout = Duration.ofMillis(200), callBound = Duration.ofMillis(CALL_BOUND_MS))
                val docs = List(3) { doc() }
                val started = System.nanoTime()
                val handedBack = withTimeout(20_000) { withContext(WriterLockHeld) { batches.putAll(docs, { it.id }, body) } }
                val ms = (System.nanoTime() - started) / 1_000_000
                println("[VespaBatchFeedTest] held batch handed back after $ms ms (call bound $CALL_BOUND_MS ms); queries $queries")
                assertEquals(docs.toSet(), handedBack.toSet(), "an unanswered batch is handed back whole, for the feed client")
                assertEquals(listOf<String?>("timeout=0.2s"), queries.toList(), "one try under the lock, its timeout named as the handler parses it")
                // The bound plus a round trip; generous against a loaded box, and far under what an
                // unbounded wait would take (the gate is never opened before this returns).
                assertTrue(ms < CALL_BOUND_MS + 2_000, "the held batch was given up on after the call bound: $ms ms")
            } finally {
                gate.countDown()
                http.close()
            }
        }

    @Test
    fun `under a writer lock the feed client's re-put of a refused batch fails within the locked deadline`() =
        runBlocking<Unit> {
            // The batch POST is refused (answerWhere runs before the hold); every document-API put
            // behind it is held, an engine that stopped acking. Unlocked, that put waits FEED_TIMEOUT.
            val index = VespaEventIndex(mock.url)
            val gate = CountDownLatch(1)
            try {
                // Warm-up, unheld and unrefused: the feed client's handshake and connections.
                index.putAll(listOf(doc(), doc()))
                mock.answerWhere = { method, path, _ -> if (method == "POST" && path == VespaBatchFeed.PATH) 503 else null }
                mock.holdWrites = gate
                val started = System.nanoTime()
                val failure = withTimeout(60_000) { runCatching { withContext(WriterLockHeld) { index.putAll(listOf(doc(), doc())) } }.exceptionOrNull() }
                val ms = (System.nanoTime() - started) / 1_000_000
                println("[VespaBatchFeedTest] locked re-put failed after $ms ms: $failure")
                assertTrue(failure != null, "an unacked re-put under the lock fails the commit (the caller re-offers) rather than reporting it stored")
                val bound = VespaEventIndex.LOCKED_FALLBACK_TIMEOUT.toMillis()
                assertTrue(ms < bound + 5_000, "the re-put gave up near the locked deadline ($bound ms), not FEED_TIMEOUT: $ms ms; $failure")
            } finally {
                gate.countDown()
                index.close()
            }
        }

    private companion object {
        const val CALL_BOUND_MS = 500L
    }
}
