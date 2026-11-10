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
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.CountDownLatch
import kotlin.coroutines.Continuation
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The shape staging actually serves on `:8080/metrics/v2/values` — two content
 * nodes, memory and disk each. Trimmed to the fields the panel reads, so a
 * Vespa upgrade that moves them fails here rather than blanking the panel.
 */
class EngineResourcesTest {
    private val mock = MockVespaEngine()

    /** The probe as a page reaches it: through the index, on the index's own client. */
    private val index = VespaEventIndex(mock.url, eagerFeed = false)

    @AfterTest
    fun stop() {
        index.close()
        mock.stop()
    }

    private val realShape =
        """
        {"nodes":[
          {"hostname":"vespa-relay-vespa-0.vespa-relay-vespa.ns.svc.cluster.local","services":[
            {"name":"vespa.searchnode","metrics":[
              {"values":{"content.proton.resource_usage.memory.average":0.811,
                         "content.proton.resource_usage.disk.average":0.435,
                         "content.proton.resource_usage.feeding_blocked.last":0.0}}]}]},
          {"hostname":"vespa-relay-vespa-1.vespa-relay-vespa.ns.svc.cluster.local","services":[
            {"name":"vespa.searchnode","metrics":[
              {"values":{"content.proton.resource_usage.memory.average":0.722,
                         "content.proton.resource_usage.disk.average":0.422,
                         "content.proton.resource_usage.feeding_blocked.last":0.0}}]}]}
        ]}
        """.trimIndent()

    @Test
    fun `both nodes are read, and the peak is what decides whether the cluster feeds`() {
        val usage = EngineResources.parse(realShape, nowMillis = 1_000)
        assertEquals(2, usage.nodes.size)
        assertEquals(listOf("vespa-relay-vespa-0", "vespa-relay-vespa-1"), usage.nodes.map { it.host }, "hostnames are shortened to the pod name")
        assertEquals(0.811, usage.peakMemory, 1e-9, "the WORST node, not an average — that is the one that blocks feed")
        assertEquals(0.435, usage.peakDisk, 1e-9)
        assertTrue(!usage.anyFeedBlocked)
    }

    /**
     * THE TEST THAT WAS MISSING. The first cut of this shipped a parser and
     * its tests, and nothing constructed the class in production — the feature
     * was dead code and every test passed, because they all called the parser
     * seam directly. This one walks the surface a caller actually uses.
     */
    @Test
    fun `the index exposes headroom rather than orphaning the probe`() {
        // A MEMBER, looked up on the class: an extension of the same name would compile to a
        // static elsewhere and fail this. Java reflection because kotlin-reflect left the
        // classpath with Quartz's jackson-module-kotlin (28bf170f92); a suspend fun is a
        // JVM method taking the Continuation.
        assertNotNull(VespaEventIndex::class.java.getMethod("engineHeadroom", Continuation::class.java), "the probe must be reachable from the index, not dead code beside it")
    }

    /** The state the panel exists to make visible: feed blocked on ONE node blocks the cluster. */
    @Test
    fun `a single blocked node reports the cluster as blocked`() {
        val blocked = realShape.replace("\"content.proton.resource_usage.feeding_blocked.last\":0.0", "\"content.proton.resource_usage.feeding_blocked.last\":1.0")
        assertTrue(EngineResources.parse(blocked, nowMillis = 1).anyFeedBlocked)
    }

    /**
     * Every caller that finds the reading stale while a refresh is in flight awaits THAT
     * refresh: twenty page renders at once are one request, not twenty. Each caller is started
     * undispatched, so all twenty have asked before any answer can arrive.
     */
    @Test
    fun `concurrent stale reads share one metrics request`() {
        val readings =
            runBlocking {
                List(20) { async(start = CoroutineStart.UNDISPATCHED) { index.engineHeadroom() } }.awaitAll()
            }
        assertEquals(1, mock.metricsRequests, "one request for twenty concurrent stale reads")
        val first = assertNotNull(readings.first(), "the shared reading")
        readings.forEach { assertSame(first, it, "every caller gets the one reading") }
        assertEquals(0.5, first.peakMemory, 1e-9)
    }

    /**
     * The caller that happened to start a refresh is a page render like any other, and may be
     * abandoned: its cancellation must end only its own wait, not the refresh another caller is
     * awaiting, and must not reach that caller as an exception.
     */
    @Test
    fun `a waiter still gets the reading when the caller that started the refresh is cancelled`() {
        val hold = CountDownLatch(1)
        mock.holdMetrics = hold
        runBlocking {
            val starter = launch(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) { index.engineHeadroom() }
            val waiter = async(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) { index.engineHeadroom() }
            // Until the request is held at the mock; a second would arrive within this, were there one.
            val deadline = System.nanoTime() + 1_000_000_000
            while (mock.metricsRequests < 2 && System.nanoTime() < deadline) Thread.sleep(10)
            starter.cancelAndJoin()
            hold.countDown()
            assertNotNull(waiter.await(), "the waiter got the reading")
            assertTrue(starter.isCancelled)
        }
        assertEquals(1, mock.metricsRequests, "and it was the one refresh both asked for")
    }

    /**
     * A metrics endpoint that takes the request and never answers. The query client has no
     * deadline on an answer, so without one of its own the probe held its caller (a page
     * render) for as long as the engine stayed silent.
     */
    @Test
    fun `a metrics endpoint that never answers costs a caller the deadline, not forever`() {
        mock.holdMetrics = CountDownLatch(1)
        val started = System.nanoTime()
        val reading =
            runBlocking {
                withContext(Dispatchers.Default) { withTimeoutOrNull(3 * EngineResources.DEADLINE_MILLIS) { index.engineHeadroom() } }
            }
        val tookMillis = (System.nanoTime() - started) / 1_000_000
        assertNull(reading, "no reading yet, so nothing to show")
        assertTrue(tookMillis < 2 * EngineResources.DEADLINE_MILLIS, "the probe returned after $tookMillis ms")
        assertEquals(1, mock.metricsRequests)
    }
}
