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
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Every read asks Vespa for an UNCOMPRESSED answer.
 *
 * Without an `Accept-Encoding` of its own, OkHttp sends `gzip` on every request and inflates the
 * answer transparently, and Vespa compresses whenever it is asked. On staging (2026-11-13) that
 * gzip was the top frame on the container's four rendering threads, and small reads queued behind
 * it. Nothing the client decodes shows which encoding it asked for, so the mock records the header.
 */
class VespaHttpCompressionTest {
    private val mock = MockVespaEngine()
    private val http = VespaHttp()

    @AfterTest
    fun tearDown() {
        http.close()
        mock.stop()
    }

    @Test
    fun `a search asks for an uncompressed answer`(): Unit =
        runBlocking {
            http.postJson("${mock.url}/search/", """{"yql":"select * from event where true","hits":1}""")

            assertEquals(listOf("/search/" to "identity"), mock.acceptEncodings.toList())
        }

    @Test
    fun `a document get and a visit page ask for an uncompressed answer`(): Unit =
        runBlocking {
            val path = "/document/v1/event/event/docid"
            http.get("${mock.url}$path/${"1".repeat(64)}")
            http.getVisit("${mock.url}$path?wantedDocumentCount=1")

            assertEquals(listOf("identity", "identity"), mock.acceptEncodings.map { it.second })
        }
}
