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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.ingest.GuardOwners
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The store's background workers retry forever and keep their state safe when
 * they fail, so a permanently broken one has no symptom of its own: ranking
 * quietly stops following trust writes, or the guard cache quietly stops
 * honouring the staleness bound [WriterTopology.SHARED] advertises. These pin
 * that the failures are COUNTED rather than swallowed.
 */
class BackgroundFailuresTest {
    @BeforeTest
    fun clean() = BackgroundFailures.reset()

    @Test
    fun `a healthy process reports an empty line, so a status display can splice it in unconditionally`() {
        assertEquals("", BackgroundFailures.statusLine())
    }

    @Test
    fun `a recovered worker keeps its count but clears the consecutive run`() {
        BackgroundFailures.record(BackgroundFailures.TRUST_DRAIN, IOException("vespa unreachable"))
        BackgroundFailures.record(BackgroundFailures.TRUST_DRAIN, IOException("vespa unreachable"))
        assertEquals(2, BackgroundFailures.consecutiveFailures(BackgroundFailures.TRUST_DRAIN))
        assertTrue(
            BackgroundFailures.statusLine().contains("2 consecutive"),
            "a stuck worker must be distinguishable from one that failed twice in a month",
        )

        BackgroundFailures.succeeded(BackgroundFailures.TRUST_DRAIN)
        assertEquals(0, BackgroundFailures.consecutiveFailures(BackgroundFailures.TRUST_DRAIN))
        val line = BackgroundFailures.statusLine()
        assertTrue(line.contains("trust.drain 2 fail"), "the cumulative count survives recovery: $line")
        assertFalse(line.contains("consecutive"), "but the process is no longer stuck: $line")
    }

    /**
     * The wiring, not just the counter: a refresher whose corpus scan keeps
     * failing must show up. Only the initial load succeeds here, which is
     * exactly the shape that used to be invisible — the guard sets stay usable
     * (never wrong in the forbidden direction), so nothing else reports it.
     */
    @Test
    fun `the guard refresher records its failures instead of swallowing them`() =
        runBlocking {
            val scans = AtomicInteger()
            val index =
                object : StubIndex() {
                    override suspend fun scanAuthors(query: EventQuery): Set<String> {
                        // The load runs one scan per guard kind; everything after
                        // that is a refresh, and every refresh fails.
                        if (scans.incrementAndGet() > 2) throw IllegalStateException("engine down")
                        return emptySet()
                    }
                }
            val guards = GuardOwners(index, WriterTopology.SHARED, refreshMillis = 50)
            try {
                // Triggers the load, which starts the refresher.
                guards.mightBeDeleted("a".repeat(64))
                val deadline = System.currentTimeMillis() + 10_000
                while (BackgroundFailures.consecutiveFailures(BackgroundFailures.GUARD_REFRESH) == 0L &&
                    System.currentTimeMillis() < deadline
                ) {
                    delay(25)
                }
                assertTrue(
                    BackgroundFailures.consecutiveFailures(BackgroundFailures.GUARD_REFRESH) > 0,
                    "a refresher failing every cycle must be visible, not silent",
                )
                assertTrue(
                    BackgroundFailures.statusLine().contains("engine down"),
                    "the line must name the cause: ${BackgroundFailures.statusLine()}",
                )
            } finally {
                guards.close()
            }
        }

    /** Enough [EventIndex] to build a [GuardOwners]; the scan is what each test overrides. */
    private open class StubIndex : EventIndex {
        override suspend fun get(id: String): EventDoc? = null

        override suspend fun search(query: EventQuery): List<EventDoc> = emptyList()

        override suspend fun count(query: EventQuery): Int = 0

        override suspend fun put(doc: EventDoc) {}

        override suspend fun remove(id: String) {}

        override fun close() {}
    }
}
