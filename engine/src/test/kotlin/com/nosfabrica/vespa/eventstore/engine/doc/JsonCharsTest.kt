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

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * [JsonChars] and [EventDoc.feedJson] write the bytes kotlinx would: a feed body
 * or a stored `tags` string that differed would change what is served back.
 */
class JsonCharsTest {
    private fun kotlinx(value: String) = JsonPrimitive(value).toString()

    private fun ours(value: String) = JsonChars(0).string(value).toString()

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

    private fun kotlinxTags(tags: List<List<String>>) = JsonArray(tags.map { tag -> JsonArray(tag.map(::JsonPrimitive)) }).toString()

    /**
     * The old tree, with its `tags` string built by kotlinx too: indexFields() now takes it from
     * tagsJson(), and a reference that shares the code under test cannot catch that code.
     */
    private fun spec(
        doc: EventDoc,
        near: Boolean,
        edge: Boolean,
    ): String {
        val fields = doc.indexFields(includeNear = near, includeEdge = edge)
        return buildJsonObject { put("fields", JsonObject(fields + ("tags" to JsonPrimitive(kotlinxTags(doc.tags))))) }.toString()
    }

    @Test
    fun `the feed body is the indexFields object, byte for byte`() {
        val profile =
            kind3.copy(
                kind = 0,
                tags = listOf(listOf("t", hostile)),
                content = """{"name":"Ann \"the\" Smith"}""",
                search =
                    SearchFields(
                        name = "Ann \"the\" Smith",
                        displayName = hostile,
                        about = "tarantellas every night",
                        nip05 = "ann@example.com",
                        lud16 = "ann@$hostile",
                        website = "https://example.com/$hostile",
                        primary = hostile,
                        secondary = "\u2028\u2029\ud83d lone",
                        text = hostile,
                        location = hostile,
                    ),
            )
        for (doc in listOf(kind3, profile, kind3.copy(tags = emptyList(), content = ""))) {
            for (near in listOf(true, false)) {
                for (edge in listOf(true, false)) {
                    assertEquals(spec(doc, near, edge), doc.feedJson(includeNear = near, includeEdge = edge), "kind ${doc.kind} near=$near edge=$edge")
                }
            }
        }
    }

    /** Every UTF-16 unit as a tag value: 256 tags of 256 values, each unit between two letters. */
    private val everyUnit = kind3.copy(tags = (0..0xFFFF).chunked(256).map { chunk -> chunk.map { "a" + it.toChar() + "b" } })

    @Test
    fun `the stored tags string is kotlinx's`() {
        assertEquals(kotlinxTags(kind3.tags), kind3.tagsJson())
        assertEquals(kotlinxTags(everyUnit.tags), everyUnit.tagsJson())
    }

    @Test
    fun `every UTF-16 unit in a tag value survives both escapes of the feed body`() {
        // The tags field is JSON inside a JSON string: each unit goes through the TWICE table.
        assertEquals(spec(everyUnit, near = false, edge = false), everyUnit.feedJson(includeNear = false, includeEdge = false))
    }
}
