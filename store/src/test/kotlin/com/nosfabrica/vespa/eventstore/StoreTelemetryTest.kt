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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * [VespaEventStore.telemetry] carries every section a pulse page reads — the
 * store's own ledger AND the process-wide registries a consumer used to name
 * one by one — from a single call.
 */
class StoreTelemetryTest {
    private val mock = MockVespaEngine()

    @AfterTest
    fun stopMock() = mock.stop()

    @Test
    fun `one call carries the ledger, the process-wide registries and the feed line`() =
        runBlocking {
            mock.inner.put(
                EventDoc(id = "1".repeat(64), pubkey = "a".repeat(64), createdAt = 1_700_000_000, kind = 1, tags = emptyList(), content = "a note", sig = "e".repeat(128), search = SearchFields(text = "a note")),
            )
            VespaEventStore.open(url = mock.url, autoDeploy = false, readOnly = true).use { store ->
                store.query<Event>(Filter(kinds = listOf(1)))
                IngestStats.add("telemetry.test", 1_000)

                val before = System.currentTimeMillis()
                val t = store.telemetry()
                assertTrue(t.atMillis >= before, "stamped when read")
                assertTrue(t.metrics.ports.any { it.activity == Activity.Query }, "the ledger's port calls: ${t.metrics.ports.map { it.activity }}")
                assertTrue("telemetry.test" in t.stages, "the process-wide ingest stages ride along: ${t.stages.keys}")
                assertEquals(store.feedStatus(), t.feed, "the feed line")
                assertEquals(store.backgroundStatus(), t.background, "the background line")
                assertTrue(t.held.isEmpty(), "no writer lock is held between calls")
            }
        }
}
