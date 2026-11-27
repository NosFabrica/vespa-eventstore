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
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A bulk `batchInsert` must not hold the store's writer lock (`WriteLocks.writes`, stage
 * `lock.ingest.hold`) across `VespaHttp.send()`'s retry back-off (0.5 + 1 + 1.5 s for
 * `QUERY_RETRIES = 3`) or across an unbounded wait on the batch-feed POST, or one slow or failing
 * engine answer stalls every other writer. The fix: `send()` asks once under `WriterLockHeld`, and
 * the batch-feed POST names a `timeout` (`VespaBatchFeed.TIMEOUT`) and bounds its own wait for the
 * answer (`VespaBatchFeed.CALL_BOUND`). Both tests failed before it (a second writer waited ~3 s
 * behind the first's 503 back-off; the batch-feed POST named no timeout).
 */
class WriterLockRetryHoldTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)
    private val store = NostrSemanticsStore(index)
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)
    private var seq = 0

    @AfterTest
    fun tearDown() {
        mock.stop()
        index.close()
    }

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun note(author: String) = Event(id(), author, 1_700_000_000L + seq, 1, emptyArray(), "note $seq", "")

    private fun isFeedPost(
        method: String,
        path: String,
    ) = method == "POST" && path == FEED_PATH

    /**
     * Writer A's batch-feed POST is answered 503 four times (the first try and `send()`'s three
     * retries); writer B, an unrelated author's batch, starts the moment A's first 503 is served,
     * i.e. while A holds the lock in its commit. B must not wait out A's retry back-off.
     *
     * BOUND: [UNBLOCKED_BOUND_MS] = 1,000 ms. A mock round trip is milliseconds and B's own commit
     * is two puts, so a writer that does not hold the lock across back-off answers B in well under
     * that, even on a loaded CI box (generous against jitter); the current code's back-off alone is
     * 3,000 ms, and any bound under 1,500 ms (the first two pauses) still tells the two apart.
     */
    @Test
    fun `a second writer does not wait out the first writer's 503 retry back-off`() =
        runBlocking<Unit> {
            // Warm-up: connections, JIT, the feed client's handshake — so B's time is the wait alone.
            store.batchInsert(listOf(note(alice), note(alice))).forEach { assertEquals(IEventStore.InsertOutcome.Accepted, it) }

            val feedPosts = AtomicInteger()
            val refusedAt = Collections.synchronizedList(ArrayList<Long>())
            val firstRefused = CompletableDeferred<Unit>()
            mock.answerWhere = { method, path, _ ->
                if (isFeedPost(method, path) && feedPosts.incrementAndGet() <= 1 + RETRIES) {
                    refusedAt += System.nanoTime()
                    firstRefused.complete(Unit)
                    503
                } else {
                    null
                }
            }

            val aStart = System.nanoTime()
            val a = async(Dispatchers.IO) { store.batchInsert(listOf(note(alice), note(alice))) }
            withTimeout(10_000) { firstRefused.await() }
            // A is inside its commit now: the batch-feed POST is sent only under the writer lock.
            val bStart = System.nanoTime()
            val b = async(Dispatchers.IO) { store.batchInsert(listOf(note(bob), note(bob))) }
            val bOut = withTimeout(60_000) { b.await() }
            val bWaitMs = (System.nanoTime() - bStart) / 1_000_000
            val aOut = withTimeout(60_000) { a.await() }
            val aMs = (System.nanoTime() - aStart) / 1_000_000
            val gaps = refusedAt.zipWithNext { x, y -> (y - x) / 1_000_000 }

            val measured = "B waited $bWaitMs ms; A took $aMs ms; A's feed POST answered 503 ${refusedAt.size}x, gaps between tries $gaps ms; feed POSTs total ${feedPosts.get()}"
            println("[WriterLockRetryHoldTest] $measured")
            assertTrue(aOut.all { it == IEventStore.InsertOutcome.Accepted }, "fixture: A's documents re-put through the feed client: $aOut")
            assertTrue(bOut.all { it == IEventStore.InsertOutcome.Accepted }, "fixture: B stored: $bOut")
            assertTrue(
                bWaitMs < UNBLOCKED_BOUND_MS,
                "a writer must not hold the writer lock across retry back-off: $measured (bound $UNBLOCKED_BOUND_MS ms)",
            )
        }

    /**
     * Writer A's batch-feed POST is HELD (the engine has not answered); writer B starts once it
     * arrives and the test releases A's POST after [HOLD_MS]. Measures how long B waits (the lock
     * is held for as long as the HTTP client waits: no read/call timeout on that client), then
     * asserts the correct shape: the feed POST bounds the engine's wait itself, by naming a
     * `timeout` the way the lone direct put does (`?timeout=30s`). Without one, BatchFeedHandler
     * waits its DEFAULT_TIMEOUT (30 s, +5 s slack) before a 504 that `send()` then retries.
     */
    @Test
    fun `a held batch-feed POST bounds its wait with a timeout parameter`() =
        runBlocking<Unit> {
            store.batchInsert(listOf(note(alice), note(alice))).forEach { assertEquals(IEventStore.InsertOutcome.Accepted, it) }

            val feedQueries = Collections.synchronizedList(ArrayList<String?>())
            mock.answerWhere = { method, path, query ->
                if (isFeedPost(method, path)) feedQueries += query
                null
            }
            val gate = CountDownLatch(1)
            mock.holdWrites = gate

            val a = async(Dispatchers.IO) { store.batchInsert(listOf(note(alice), note(alice))) }
            withTimeout(10_000) { while (mock.heldWrites < 1) delay(5) }
            val bStart = System.nanoTime()
            val b = async(Dispatchers.IO) { store.batchInsert(listOf(note(bob), note(bob))) }
            delay(HOLD_MS)
            val bDoneWhileHeld = b.isCompleted
            gate.countDown()
            val bOut = withTimeout(60_000) { b.await() }
            val bWaitMs = (System.nanoTime() - bStart) / 1_000_000
            val aOut = withTimeout(60_000) { a.await() }

            val measured = "B done while A's POST was held: $bDoneWhileHeld; B waited $bWaitMs ms for a POST held $HOLD_MS ms; feed POST query strings: $feedQueries"
            println("[WriterLockRetryHoldTest] $measured")
            assertTrue(aOut.all { it == IEventStore.InsertOutcome.Accepted } && bOut.all { it == IEventStore.InsertOutcome.Accepted }, "fixture: both stored: $aOut $bOut")
            assertTrue(
                feedQueries.isNotEmpty() && feedQueries.all { it.orEmpty().split('&').any { p -> p.startsWith("timeout=") } },
                "the batch-feed POST must name a timeout, so an unanswered batch cannot hold the writer lock for the handler's 30 s default per try: $measured",
            )
            // Read as BatchFeedHandler.timeout reads it (seconds, an optional `s`): a value it could
            // not parse would leave it on its 30 s default, the very wait the parameter exists to cut.
            val named =
                feedQueries.map { q ->
                    q
                        .orEmpty()
                        .split('&')
                        .first { it.startsWith("timeout=") }
                        .removePrefix("timeout=")
                }
            val seconds = named.map { it.replace("s", "").toDoubleOrNull() }
            assertTrue(
                seconds.all { it != null && it > 0 && it < HANDLER_DEFAULT_TIMEOUT_S },
                "the named timeout must parse as the handler reads it and be under its ${HANDLER_DEFAULT_TIMEOUT_S}s default: $named",
            )
        }

    private companion object {
        const val FEED_PATH = "/eventstore/v1/feed"

        /** `VespaHttp.QUERY_RETRIES` (internal to the engine module). */
        const val RETRIES = 3

        const val UNBLOCKED_BOUND_MS = 1_000L

        const val HOLD_MS = 3_000L

        /** `BatchFeedHandler.DEFAULT_TIMEOUT`, what a batch that names no timeout waits. */
        const val HANDLER_DEFAULT_TIMEOUT_S = 30.0
    }
}
