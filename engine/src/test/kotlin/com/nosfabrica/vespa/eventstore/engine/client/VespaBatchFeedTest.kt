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

    private fun doc(content: String = "hello") =
        EventDoc(
            id = (++seq).toString(16).padStart(64, '0'),
            pubkey = "a1".repeat(32),
            createdAt = 1_000L + seq,
            kind = 1,
            tags = emptyList(),
            content = content,
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

    @Test
    fun `under a writer lock a 429 is asked again briefly, then surfaced`() =
        runBlocking<Unit> {
            // Two 429s, then served: pushback that clears inside the locked schedule.
            val asked = AtomicInteger()
            val pushbacks = AtomicInteger(2)
            mock.answerWhere = { _, path, _ ->
                if (path.startsWith("/document/v1/")) {
                    asked.incrementAndGet()
                    if (pushbacks.getAndDecrement() > 0) 429 else null
                } else {
                    null
                }
            }
            val http = VespaHttp()
            try {
                val url = "${mock.url}/document/v1/event/event/docid/${"1".repeat(64)}"
                var started = System.nanoTime()
                val cleared = withTimeout(20_000) { withContext(WriterLockHeld) { http.get(url) } }
                var ms = (System.nanoTime() - started) / 1_000_000
                println("[VespaBatchFeedTest] two 429s under the lock: answered ${cleared.statusCode()} after ${asked.get()} asks, $ms ms")
                assertEquals(404, cleared.statusCode(), "pushback is asked again under the lock, and the read is served (an absent document)")
                assertEquals(3, asked.get(), "two 429s and the ask that was served")
                assertTrue(ms < 1_000, "the locked pauses stay short (50 + 100 ms here): $ms ms")

                // Pushback that does not clear: surfaced after the locked schedule, not the unlocked one.
                asked.set(0)
                pushbacks.set(Int.MAX_VALUE)
                started = System.nanoTime()
                val refused = withTimeout(20_000) { withContext(WriterLockHeld) { http.get(url) } }
                ms = (System.nanoTime() - started) / 1_000_000
                println("[VespaBatchFeedTest] 429 throughout under the lock: ${asked.get()} asks, $ms ms")
                assertEquals(429, refused.statusCode(), "surfaced as the last ask's answer")
                assertEquals(1 + VespaHttp.LOCKED_PUSHBACK_DELAYS_MS.size, asked.get(), "the locked schedule's asks, no more")
                assertTrue(ms < 1_000, "given up on well under a second, not after the unlocked 3 s of back-off: $ms ms")
            } finally {
                http.close()
            }
        }

    @Test
    fun `a batch path this call parks leaves the locked re-put its FEED_TIMEOUT`() =
        runBlocking<Unit> {
            // No handler deployed: the batch POST is answered 404 and parks the batch path, so the
            // feed client is the write path itself. Every document-API put is held for longer than
            // the locked fallback's deadline and well inside FEED_TIMEOUT: an engine slow to ack,
            // which the feed client's own 30 s waits out, as it does with batching off.
            val index = VespaEventIndex(mock.url)
            val gate = CountDownLatch(1)
            val release = Thread { Thread.sleep(HELD_PAST_LOCKED_MS).also { gate.countDown() } }
            try {
                index.putAll(listOf(doc(), doc()))
                mock.answerWhere = { method, path, _ -> if (method == "POST" && path == VespaBatchFeed.PATH) 404 else null }
                mock.holdWrites = gate
                val docs = listOf(doc(), doc())
                val started = System.nanoTime()
                release.start()
                val failure = withTimeout(60_000) { runCatching { withContext(WriterLockHeld) { index.putAll(docs) } }.exceptionOrNull() }
                val ms = (System.nanoTime() - started) / 1_000_000
                println("[VespaBatchFeedTest] parked by its own 404, the locked re-put took $ms ms (puts held $HELD_PAST_LOCKED_MS ms): $failure")
                assertTrue(index.feedBatchParked, "fixture: the call's 404 parked the batch path")
                assertEquals(null, failure, "a feed client that is the write path keeps FEED_TIMEOUT under the lock too: $ms ms")
                docs.forEach { assertTrue(index.get(it.id) != null, "stored: ${it.id}") }
            } finally {
                gate.countDown()
                release.join()
                index.close()
            }
        }

    @Test
    fun `a large locked hand-back is not failed by an engine queueing it at a healthy rate`() =
        runBlocking<Unit> {
            // The throttle window started at the chunk (64 x the floor of 64): every put of the
            // hand-back is handed to the feed client, and its deadline started, at once. The
            // engine writes one put per DOCUMENT_PUT_MICROS, ~650 a second, half the 1,306-1,322
            // events/s measured end to end on one shared 4-core node: nothing fails there, it
            // only queues. The batch POST is refused, so the whole commit is handed back.
            val index = VespaEventIndex(mock.url, tuning = ClientTuning(feedInflightFactor = 64))
            try {
                index.putAll(listOf(doc(), doc()))
                mock.answerWhere = { method, path, _ -> if (method == "POST" && path == VespaBatchFeed.PATH) 503 else null }
                mock.documentPutMicros = DOCUMENT_PUT_MICROS
                val docs = List(VespaEventIndex.FEED_CHUNK) { doc() }
                val started = System.nanoTime()
                val failure = withTimeout(120_000) { runCatching { withContext(WriterLockHeld) { index.putAll(docs) } }.exceptionOrNull() }
                val ms = (System.nanoTime() - started) / 1_000_000
                println("[VespaBatchFeedTest] ${docs.size}-document locked hand-back at ~${1_000_000 / DOCUMENT_PUT_MICROS} puts/s took $ms ms: $failure")
                assertEquals(null, failure, "a healthy engine's queue is not a failure of the commit: $ms ms; locked deadline for the chunk ${VespaEventIndex.lockedFallbackTimeout(docs.size)}")
                assertTrue(ms < VespaEventIndex.lockedFallbackTimeout(docs.size).toMillis() + 5_000, "and the lock stays bounded by that deadline: $ms ms")
            } finally {
                mock.documentPutMicros = 0
                index.close()
            }
        }

    @Test
    fun `a batch sent without a writer lock names no timeout and is waited for`() =
        runBlocking<Unit> {
            val queries = Collections.synchronizedList(ArrayList<String?>())
            mock.answerWhere = { method, path, query ->
                if (method == "POST" && path == VespaBatchFeed.PATH) queries += query
                null
            }
            val gate = CountDownLatch(1)
            mock.holdWrites = gate
            val release = Thread { Thread.sleep(UNLOCKED_HOLD_MS).also { gate.countDown() } }
            val http = VespaHttp()
            try {
                // The same bounds the locked test passes: an unlocked batch must not see them.
                val batches = VespaBatchFeed(http, { mock.url }, batchSize = 100, concurrency = 1, timeout = Duration.ofMillis(200), callBound = Duration.ofMillis(CALL_BOUND_MS))
                val docs = List(3) { doc() }
                val started = System.nanoTime()
                release.start()
                val handedBack = withTimeout(30_000) { batches.putAll(docs, { it.id }, body) }
                val ms = (System.nanoTime() - started) / 1_000_000
                println("[VespaBatchFeedTest] unlocked held batch answered after $ms ms (held $UNLOCKED_HOLD_MS ms, call bound $CALL_BOUND_MS ms); queries $queries")
                assertEquals(listOf<String?>(null), queries.toList(), "one POST, naming no timeout: the handler's default, and no client bound to re-POST it at")
                assertEquals(emptyList(), handedBack, "the batch was waited for and stored, not handed back to be re-put")
            } finally {
                gate.countDown()
                release.join()
                http.close()
            }
        }

    @Test
    fun `a locked batch's call bound grants its body's upload and parse`() =
        runBlocking<Unit> {
            // The handler starts its deadline only after reading and parsing the body; the mock
            // spends BODY_MS_PER_MB per MB of body before it answers. The batch below is ~1 MB,
            // so it answers after ~1 s, past the bare call bound and inside the bound with the
            // body's share (at the test's 0.5 MB/s, ~2 s more).
            mock.batchBodyMsPerMb = BODY_MS_PER_MB
            val posts = AtomicInteger()
            mock.answerWhere = { method, path, _ ->
                if (method == "POST" && path == VespaBatchFeed.PATH) posts.incrementAndGet()
                null
            }
            val http = VespaHttp()
            try {
                val batches =
                    VespaBatchFeed(
                        http,
                        { mock.url },
                        batchSize = 100,
                        concurrency = 1,
                        timeout = Duration.ofMillis(200),
                        callBound = Duration.ofMillis(CALL_BOUND_MS),
                        bodyBytesPerSecond = 500_000,
                    )
                val docs = List(10) { doc("x".repeat(100_000)) }
                val started = System.nanoTime()
                val handedBack = withTimeout(30_000) { withContext(WriterLockHeld) { batches.putAll(docs, { it.id }, body) } }
                val ms = (System.nanoTime() - started) / 1_000_000
                println("[VespaBatchFeedTest] ~1 MB locked batch answered after $ms ms (bare call bound $CALL_BOUND_MS ms); POSTs ${posts.get()}")
                assertEquals(1, posts.get(), "fixture: one POST")
                assertEquals(emptyList(), handedBack, "a batch the handler answered inside its own deadline is not given up on and handed back: $ms ms")
            } finally {
                mock.batchBodyMsPerMb = 0
                http.close()
            }
        }

    private companion object {
        const val CALL_BOUND_MS = 500L

        /** Past the locked fallback's base deadline (5 s) by enough to tell them apart on a loaded box. */
        val HELD_PAST_LOCKED_MS = VespaEventIndex.LOCKED_FALLBACK_TIMEOUT.toMillis() + 2_000

        /** Past the test's call bound (500 ms) by a margin, under any back-off retry's timing. */
        const val UNLOCKED_HOLD_MS = 1_500L

        /** One put per 1.5 ms: ~650 puts/s, so a full chunk takes ~6.1 s to drain. */
        const val DOCUMENT_PUT_MICROS = 1_500L

        const val BODY_MS_PER_MB = 1_000L
    }
}
