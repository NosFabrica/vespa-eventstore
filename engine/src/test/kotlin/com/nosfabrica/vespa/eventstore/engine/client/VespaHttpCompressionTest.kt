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
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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

    /**
     * The header is set where each request is BUILT, no longer by an interceptor that rebuilt
     * every request: the search url parsed once, and the streamed visit, whose request its
     * caller builds, ask for `identity` all the same.
     */
    @Test
    fun `a pre-parsed search url and a streamed visit ask for an uncompressed answer`(): Unit =
        runBlocking {
            http.post("${mock.url}/search/".toHttpUrl(), """{"yql":"select * from event where true","hits":1}""".toRequestBody())
            http
                .newVisitCall(
                    http
                        .request()
                        .url("${mock.url}/document/v1/event/event/docid?wantedDocumentCount=1")
                        .get()
                        .build(),
                ).execute()
                .close()

            assertEquals(listOf("identity", "identity"), mock.acceptEncodings.map { it.second })
        }

    /** Both decode paths — the string lexer under the threshold, the stream above it — read the same answer. */
    @Test
    fun `a small and a large answer decode alike`() {
        val json = Json { ignoreUnknownKeys = true }

        fun answer(hits: Int) = """{"root":{"children":[${(1..hits).joinToString(",") { """{"fields":{"id":"$it","note":"caf\u00e9 \ud83d\ude00 ${"x".repeat(200)}"}}""" }}]}}"""
        // The large one sized past this JVM's ceiling, which follows its heap region size.
        for (hits in listOf(3, (VespaHttp.STRING_DECODE_MAX_BYTES / 200).toInt() + 100)) {
            val text = answer(hits)
            val resp = VespaHttp.HttpResp(200, Buffer().writeUtf8(text))
            assertTrue((text.length > VespaHttp.STRING_DECODE_MAX_BYTES) == (hits > 3), "the two sizes straddle the threshold")
            assertEquals(json.parseToJsonElement(text), resp.decode(json, JsonElement.serializer()), "$hits hits")
            assertEquals(text, resp.body(), "the text stays readable after a decode")
        }
    }

    /** The string path's ceiling follows the G1 region: humongous starts at half a region, and a String may be twice its bytes. */
    @Test
    fun `the string-decode ceiling follows the heap region size`() {
        assertEquals(192L * 1024, VespaHttp.stringDecodeMax(1L shl 20), "1 MB regions: the measured 192 KB")
        assertEquals(768L * 1024, VespaHttp.stringDecodeMax(4L shl 20))
        assertEquals(VespaHttp.DEFAULT_STRING_DECODE_MAX_BYTES, VespaHttp.stringDecodeMax(null), "a JVM that will not say keeps the 1 MB answer")
        assertTrue(VespaHttp.STRING_DECODE_MAX_BYTES >= VespaHttp.DEFAULT_STRING_DECODE_MAX_BYTES, "and this JVM's is derived, never below it")
    }
}
