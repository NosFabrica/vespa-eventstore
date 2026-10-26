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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okio.Buffer
import kotlin.test.Test
import kotlin.test.assertEquals

/** The sink writer escapes as kotlinx does, and the batch body it builds cannot be broken out of. */
class JsonSinkTest {
    private val hostile =
        listOf("", "plain", "\"quoted\"", "back\\slash", "line\nbreak\r\t", "\u0000\u0001\u001f", "café 你好 🚀", "a\"b\\c\"d")

    @Test
    fun `a string is written as kotlinx writes it`() {
        for (s in hostile) assertEquals(JsonPrimitive(s).toString(), Buffer().also { it.writeJsonString(s) }.readUtf8(), s)
    }

    @Test
    fun `a batch id carrying JSON stays one id`() {
        val injected = "30000:${"a".repeat(64)}:x\",\"fields\":{}},{\"remove\":\"id:event:event::0:${"b".repeat(64)}:"
        val ids = listOf("c".repeat(64), injected)
        val bodies = ids.map { """{"fields":{"id":"x"}}""" }
        val sink = Buffer()
        VespaBatchFeed.operations(ids, bodies).writeTo(sink)
        val ops = Json.parseToJsonElement(sink.readUtf8()).jsonArray
        assertEquals(2, ops.size, "no operation was smuggled in")
        assertEquals(
            ids.map { "id:event:event::$it" },
            ops.map {
                it.jsonObject
                    .getValue("put")
                    .jsonPrimitive.content
            },
        )
        assertEquals(listOf(setOf("put", "fields")), ops.map { it.jsonObject.keys }.distinct())
    }

    @Test
    fun `the batch size reads blank as the default and a word as off`() {
        assertEquals(VespaEventIndex.DEFAULT_FEED_BATCH, VespaEventIndex.feedBatchOf(null))
        assertEquals(VespaEventIndex.DEFAULT_FEED_BATCH, VespaEventIndex.feedBatchOf(" "))
        assertEquals(0, VespaEventIndex.feedBatchOf("0"))
        assertEquals(0, VespaEventIndex.feedBatchOf("off"))
        assertEquals(250, VespaEventIndex.feedBatchOf("250"))
    }
}
