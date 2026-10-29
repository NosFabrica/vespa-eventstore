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
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.IOException
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A response body that stops mid-stream on a LIVE connection must end in an
 * IOException, while the wait for an answer stays unbounded.
 *
 * Unlike [VespaReadFailureTest]'s abort, nothing fails here: the mock sends
 * headers and part of a gzipped body, then holds the stream open, and the
 * HTTP/2 connection keeps answering PINGs. Before the body deadline that read
 * waited forever (NosFabrica/vespa-relay#289: 4 h 40 min, until the caller was
 * restarted). The `withTimeout` is the assertion: pre-fix the first test never
 * returns.
 */
class VespaHttpBodyStallTest {
    private val mock = MockVespaEngine()
    private val http = VespaHttp(bodyIdleTimeout = Duration.ofSeconds(1))

    private val docUrl get() = "${mock.url}/document/v1/event/event/docid/${"1".repeat(64)}"

    @AfterTest
    fun tearDown() {
        mock.stop()
    }

    @Test
    fun `a body that stalls mid-stream fails once the retries are spent instead of hanging`(): Unit =
        runBlocking {
            mock.stallBodiesMidStream = 99 // every attempt, so retries can't rescue it

            // Four 1 s stalls and ~3 s of retry backoff, far below 20 s.
            val failure = withTimeout(20_000) { runCatching { http.get(docUrl) }.exceptionOrNull() }

            if (failure == null) fail("expected the stalled read to fail, but it returned normally")
            assertTrue(failure is IOException, "expected a transport failure, got ${failure::class.simpleName}: ${failure.message}")
            assertEquals(99 - (VespaHttp.QUERY_RETRIES + 1), mock.stallBodiesMidStream, "one stall per attempt")
        }

    @Test
    fun `a read recovers when a later attempt returns a whole body`(): Unit =
        runBlocking {
            mock.stallBodiesMidStream = 1

            val resp = withTimeout(20_000) { http.get(docUrl) }

            assertTrue(resp.statusCode() in 200..499, "the retry should have reached the mock's own answer, got ${resp.statusCode()}")
            assertEquals(0, mock.stallBodiesMidStream, "the stall should have been consumed")
        }

    @Test
    fun `the wait for an answer is not bounded by the body deadline`(): Unit =
        runBlocking {
            // An engine still matching sends no headers at all: three body windows of nothing.
            mock.delayAnswersMs = 3_000

            val resp = withTimeout(20_000) { http.get(docUrl) }

            assertTrue(resp.statusCode() in 200..499, "a slow answer must still be read, got ${resp.statusCode()}")
        }
}
