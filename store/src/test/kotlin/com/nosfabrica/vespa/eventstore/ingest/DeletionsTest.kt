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
package com.nosfabrica.vespa.eventstore.ingest

import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.client.RecencyStrategy
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The NIP-09 guard probe is ONE engine query per target style, whatever the
 * recency strategy — it is an existence check on the insert hot path, and a
 * time window can only add a round trip to it.
 *
 * The case that regressed: a BACKFILLED event (older than the speculative
 * strategy's first window). Its probe's `since` is the event's own
 * created_at, so the first window was tighter, came back empty, and the real
 * probe followed — two queries instead of one, per probe, per insert, under
 * the default SHARED_STRICT topology. The probe now opts out of planning.
 */
class DeletionsTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE)
    private val deletions = Deletions(index, relay = null, sweepPage = 100)
    private val alice = "a1".repeat(32)
    private val now = System.currentTimeMillis() / 1000
    private var seq = 0

    @AfterTest
    fun stop() {
        index.close()
        mock.stop()
    }

    private fun id() = (++seq).toString(16).padStart(64, '0')

    @Test
    fun `a backfilled event's guard probe is one query`() =
        runBlocking {
            val old = Event(id(), alice, now - 30 * 86_400L, 1, emptyArray(), "a month old", "")
            val before = mock.searchRequests.size
            assertFalse(deletions.isDeleted(old))
            val sent = mock.searchRequests.drop(before).map { it.getValue("yql") }
            assertEquals(1, sent.size, "one probe, never a window first: $sent")
            assertTrue(sent.none { it.contains("created_at >= ${now - 3_600}") }, "no speculative window: $sent")
        }

    @Test
    fun `the probe still finds the tombstone`() =
        runBlocking {
            val old = Event(id(), alice, now - 30 * 86_400L, 1, emptyArray(), "deleted later", "")
            val tombstone = DeletionRequestEvent(id(), alice, now - 29 * 86_400L, arrayOf(arrayOf("e", old.id)), "", "")
            index.put(tombstone.toDoc())
            assertTrue(deletions.isDeleted(old), "a same-owner kind 5 newer than the event blocks it")
        }
}
