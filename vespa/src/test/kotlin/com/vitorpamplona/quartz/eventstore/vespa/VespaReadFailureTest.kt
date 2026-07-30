/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.vespa

import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A read whose response body dies mid-stream must FAIL, never hang.
 *
 * The hazard is specific to OkHttp's async contract. `AsyncCall.run` sets its
 * `signalledCallback` flag before invoking `onResponse`, so an IOException thrown
 * inside that callback — which is where the body is read, to keep the read
 * non-blocking — is only logged at INFO ("Callback failure for call to …") and is
 * never handed to `onFailure`. A callback that resumes its continuation only on the
 * success path therefore leaves the caller suspended forever, with nothing but an
 * INFO line to show for it. Against the single-writer store, one such hang stalls
 * every write queued behind it.
 *
 * The `withTimeout` in these tests is the assertion: pre-fix they never return.
 */
class VespaReadFailureTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)

    @AfterTest
    fun tearDown() {
        index.close()
        mock.stop()
    }

    private fun doc(id: String) =
        EventDoc(
            id = id,
            pubkey = "a1".repeat(32),
            createdAt = 1_700_000_000L,
            kind = 1,
            tags = emptyList(),
            content = "hello",
            sig = "b2".repeat(32),
        )

    @Test
    fun `a body that aborts mid-stream surfaces as an exception instead of hanging`() =
        runBlocking {
            mock.abortBodiesMidStream = 99 // every attempt, so retries can't rescue it

            // 20s is far above the ~3s the bounded retry backoff needs, and far below
            // the forever a lost continuation takes.
            val failure =
                withTimeout(20_000) {
                    runCatching { index.search(EventQuery(limit = 10)) }.exceptionOrNull()
                }

            if (failure == null) fail("expected the aborted read to fail, but it returned normally")
            assertTrue(
                failure is Exception,
                "expected a transport failure, got ${failure::class.simpleName}: ${failure.message}",
            )
        }

    @Test
    fun `a read recovers when a later attempt returns a whole body`() =
        runBlocking {
            index.put(doc("1".repeat(64)))

            // Abort fewer times than the retry budget: the call must ride it out.
            mock.abortBodiesMidStream = 2

            val hits = withTimeout(20_000) { index.search(EventQuery(limit = 10)) }
            assertEquals(1, hits.size, "the retry should have recovered the read")
            assertEquals("1".repeat(64), hits.single().id)
            assertEquals(0, mock.abortBodiesMidStream, "both aborts should have been consumed")
        }
}
