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
package com.nosfabrica.vespa.eventstore.engine.doc

import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * [appendJsonString] and [EventDoc.feedJson] write the bytes kotlinx would: a feed body
 * or a stored `tags` string that differed would change what is served back.
 */
class JsonTextTest {
    private fun kotlinx(value: String) = JsonPrimitive(value).toString()

    private fun ours(value: String) = StringBuilder().appendJsonString(value).toString()

    @Test
    fun `every UTF-16 unit is escaped as kotlinx escapes it`() {
        for (c in 0..0xFFFF) {
            val s = "a" + c.toChar() + "b"
            assertEquals(kotlinx(s), ours(s), "U+${c.toString(16)}")
        }
    }

    @Test
    fun `runs, surrogate pairs and edges match`() {
        val random = Random(7)
        val alphabet = "ab\"\\\n\r\t\b\u000c\u0000\u001f/ é漢🫥 \u007f"
        val samples =
            listOf("", "\"", "\\", "\u0000", "🫥🫥", "plain", "\"\"\\\\") +
                (0 until 500).map { (0 until random.nextInt(0, 40)).map { alphabet[random.nextInt(alphabet.length)] }.joinToString("") }
        for (s in samples) assertEquals(kotlinx(s), ours(s), s)
    }

    private val hostile = "q\"uote\\ back\nline\u0000nul 🫥 漢字   /slash"

    private val kind3 =
        EventDoc(
            id = "a".repeat(64),
            pubkey = "b".repeat(64),
            createdAt = 1_700_000_000L,
            kind = 3,
            tags =
                (0 until 300).map { listOf("p", it.toString(16).padStart(64, '0'), "wss://relay.example/$it", if (it % 7 == 0) hostile else "") } +
                    listOf(listOf("alt"), emptyList(), listOf("t", hostile), listOf("expiration", "1900000000")),
            content = """{"wss://relay.example":{"read":true,"write":true}}""",
            sig = "e".repeat(128),
        )

    private fun spec(
        doc: EventDoc,
        near: Boolean,
        edge: Boolean,
    ) = buildJsonObject { put("fields", doc.indexFields(includeNear = near, includeEdge = edge)) }.toString()

    @Test
    fun `the feed body is the indexFields object, byte for byte`() {
        val profile =
            kind3.copy(
                kind = 0,
                tags = listOf(listOf("t", hostile)),
                content = """{"name":"Ann \"the\" Smith"}""",
                search = SearchFields(name = "Ann \"the\" Smith", displayName = hostile, about = "tarantellas every night", nip05 = "ann@example.com", text = hostile),
            )
        for (doc in listOf(kind3, profile, kind3.copy(tags = emptyList(), content = ""))) {
            for (near in listOf(true, false)) {
                for (edge in listOf(true, false)) {
                    assertEquals(spec(doc, near, edge), doc.feedJson(includeNear = near, includeEdge = edge), "kind ${doc.kind} near=$near edge=$edge")
                }
            }
        }
    }

    @Test
    fun `the stored tags string is kotlinx's`() {
        assertEquals(
            kotlinx.serialization.json
                .JsonArray(kind3.tags.map { tag -> kotlinx.serialization.json.JsonArray(tag.map(::JsonPrimitive)) })
                .toString(),
            kind3.tagsJson(),
        )
    }
}
