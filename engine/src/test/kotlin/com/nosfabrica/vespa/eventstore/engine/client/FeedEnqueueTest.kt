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
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Handing an operation to the feed client blocks the handing thread once the client's throttle
 * window is full (`HttpRequestStrategy.acquireSlot` sleeps until a slot frees). A write must not
 * do that on its caller's dispatcher, and a cancelled write must stop handing operations over.
 *
 * One feed connection makes the window 2 (2 x connections x endpoints), so [OPS] removes overrun
 * it tenfold, and the mock holds every write until the test opens [gate]: the window stays full
 * for exactly as long as each assertion needs, with no timing to guess.
 */
class FeedEnqueueTest {
    private val gate = CountDownLatch(1)
    private val mock = MockVespaEngine().apply { holdWrites = gate }
    private val tuning = ClientTuning(feedConnections = 1, feedInflightFactor = 1)

    // Batching off: every write here goes through the feed client, the path under test.
    private val index = VespaEventIndex(mock.url, tuning = tuning, feedBatch = 0)
    private val reputations = VespaReputationIndex(mock.url, tuning = tuning, feedBatch = 0)
    private val ids = List(OPS) { "%064x".format(it) }

    @AfterTest
    fun stop() {
        gate.countDown()
        index.close()
        reputations.close()
        mock.stop()
    }

    /** Until the mock holds a full window of writes: the writer is now waiting for a slot. */
    private fun awaitFullWindow() {
        val deadline = System.nanoTime() + 10_000_000_000
        while (mock.heldWrites < WINDOW && System.nanoTime() < deadline) Thread.sleep(10)
        assertEquals(WINDOW, mock.heldWrites, "the mock holds a full throttle window of writes")
    }

    /** [write] launched on a one-thread dispatcher, and another coroutine there run while [write] waits for a slot. */
    private fun assertCallerStaysFree(
        write: suspend () -> Unit,
        sent: () -> Int,
    ) {
        val caller = Executors.newSingleThreadExecutor { Thread(it, "feed-caller") }.asCoroutineDispatcher()
        try {
            runBlocking {
                val job = launch(caller) { write() }
                awaitFullWindow()
                // The caller's only thread: blocked in the client's slot wait, this never runs.
                val other = async(caller) { "ran" }
                val ran = withTimeoutOrNull(5_000) { other.await() }
                // Before asserting: runBlocking waits for [job], which a held window would keep forever.
                gate.countDown()
                assertEquals("ran", ran, "another coroutine on the writer's dispatcher ran while the write waited for a slot")
                withTimeout(30_000) { job.join() }
                assertEquals(OPS, sent(), "and the write still sent every operation")
            }
        } finally {
            gate.countDown()
            caller.close()
        }
    }

    @Test
    fun anEventWriteWaitingForTheThrottleWindowLeavesItsCallersThreadFree() = assertCallerStaysFree({ index.removeAll(ids) }, { mock.documentRemoves })

    @Test
    fun aReputationWriteWaitingForTheThrottleWindowLeavesItsCallersThreadFree() {
        mock.updateReply = 200 to """{"id":"x"}"""
        val service = ServiceKey("5e".repeat(32))
        assertCallerStaysFree({ reputations.updateCells(ids.map { ReputationCells(it, service, influence = 1, followers = null) }) }, { mock.updateRequests })
    }

    @Test
    fun aCancelledWriteStopsHandingOperationsToTheClient() {
        runBlocking {
            val write = launch(Dispatchers.Default) { index.removeAll(ids) }
            awaitFullWindow()
            write.cancel()
            gate.countDown()
            withTimeout(30_000) { write.join() }
            // Graceful: waits for whatever was handed over to finish.
            index.close()
            // The window, plus at most the one remove whose slot the write was waiting for.
            val sent = mock.documentRemoves
            assertTrue(sent <= WINDOW + 1, "a cancelled write handed over $sent of $OPS removes")
        }
    }

    private companion object {
        const val OPS = 20

        /** The throttle window at one connection: `StaticThrottler.minInflight`, 2 x 1 x 1. */
        const val WINDOW = 2
    }
}
