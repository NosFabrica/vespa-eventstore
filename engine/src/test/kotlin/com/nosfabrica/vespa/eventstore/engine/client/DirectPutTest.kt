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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * A lone put skips the feed client (and its dispatcher's millisecond sleep) only when nothing the
 * feed client holds could be overtaken by it: VespaEventIndex.putDirect.
 */
class DirectPutTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)
    private var seq = 0

    @AfterTest
    fun stop() {
        mock.holdWrites?.countDown()
        index.close()
        mock.stop()
    }

    private fun doc(
        kind: Int = 1,
        search: SearchFields = SearchFields.NONE,
    ) = EventDoc(
        id = (++seq).toString(16).padStart(64, '0'),
        pubkey = "a1".repeat(32),
        createdAt = 1_000L + seq,
        kind = kind,
        tags = emptyList(),
        content = "hello",
        sig = "e".repeat(128),
        owner = "a1".repeat(32),
        search = search,
    )

    @Test
    fun `a lone put with nothing in flight goes straight to the engine`() =
        runBlocking<Unit> {
            val d = doc()
            index.put(d)
            assertEquals(1, index.directPuts)
            assertNotNull(index.get(d.id), "and it is stored")
        }

    @Test
    fun `a put waits its turn behind a write the feed client still holds`() =
        runBlocking<Unit> {
            val gate = CountDownLatch(1)
            mock.holdWrites = gate
            val d = doc()
            // A remove of the very document, held unanswered inside the feed client.
            val remove = async(Dispatchers.Default) { index.remove(d.id) }
            val deadline = System.nanoTime() + 10_000_000_000
            while (mock.heldWrites < 1 && System.nanoTime() < deadline) Thread.sleep(10)
            assertEquals(1, mock.heldWrites, "the remove is held in flight")

            val put = async(Dispatchers.Default) { index.put(d) }
            while (mock.heldWrites < 2 && System.nanoTime() < deadline) Thread.sleep(10)
            gate.countDown()
            withTimeout(30_000) {
                remove.await()
                put.await()
            }
            assertEquals(0, index.directPuts, "the put went behind the remove, through the feed client")
            assertNotNull(index.get(d.id), "so the put, sent second, is what stands")
        }

    @Test
    fun `a put the engine refuses goes the feed client's way`() =
        runBlocking<Unit> {
            // The feed client's schema net drops a column the engine does not know and feeds again.
            mock.rejectNearFields = true
            val d = doc(kind = 0, search = SearchFields(name = "odell"))
            index.put(d)
            assertEquals(0, index.directPuts)
            assertNotNull(index.get(d.id))
        }
}
