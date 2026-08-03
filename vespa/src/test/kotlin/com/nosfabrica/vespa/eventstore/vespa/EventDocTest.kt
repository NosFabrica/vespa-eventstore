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
package com.nosfabrica.vespa.eventstore.vespa
import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc
import com.nosfabrica.vespa.eventstore.vespa.doc.SearchFields
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class EventDocTest {
    // Lossiness traps on purpose: a 3-element tag (relay hint), a non-single-letter
    // tag, a valueless tag, and content with quotes/newlines/emoji/backslash.
    private val doc =
        EventDoc(
            id = "a".repeat(64),
            pubkey = "b".repeat(64),
            createdAt = 1_700_000_000L,
            kind = 30382,
            tags =
                listOf(
                    listOf("d", "c".repeat(64)),
                    listOf("e", "f".repeat(64), "wss://relay.example.com", "root"),
                    listOf("rank", "87"),
                    listOf("alt"),
                    listOf("P", "d".repeat(64)),
                ),
            content = "line one\n\"quoted\" \\backslash\\ emoji 🫥 tab\there",
            sig = "e".repeat(128),
        )

    @Test
    fun `tagIndex keeps only single-letter tags with a value`() {
        assertEquals(listOf("d:${"c".repeat(64)}", "e:${"f".repeat(64)}", "P:${"d".repeat(64)}"), doc.tagIndex())
    }

    @Test
    fun `event json round-trips losslessly`() {
        assertEquals(doc, EventDoc.fromEventJson(doc.toEventJson()))
    }

    @Test
    fun `summary fields round-trip losslessly`() {
        assertEquals(doc, EventDoc.fromSummary(doc.indexFields()))
    }

    @Test
    fun `summary parses an unsigned rumor whose empty sig Vespa omitted`() {
        // A rumor (NIP-59 inner event / draft) is stored with sig == "". Real Vespa
        // drops empty-string fields from query summaries, so the hit comes back with
        // no `sig` key. fromSummary must default it to "" rather than throw and make
        // the event vanish from search. (Simulate the omission by removing the key.)
        val rumor = doc.copy(sig = "")
        val served = JsonObject(rumor.indexFields().filterKeys { it != "sig" })
        val back = EventDoc.fromSummary(served)
        assertEquals("", back.sig)
        assertEquals(rumor, back)
    }

    @Test
    fun `derived owner and search text round-trip too`() {
        val derived = doc.copy(owner = "9".repeat(64), search = SearchFields(name = "alice", primary = "a title", text = "the body"))
        assertEquals(derived, EventDoc.fromSummary(derived.indexFields()))
        // Defaults: owner falls back to the author, search to unsearchable.
        assertEquals(doc.pubkey, doc.owner)
        assertEquals(SearchFields.NONE, doc.search)
    }

    @Test
    fun `reconstructed event carries the exact nip01 fields`() {
        val o = Json.parseToJsonElement(doc.toEventJson()).jsonObject
        assertEquals(doc.id, o.getValue("id").jsonPrimitive.content)
        assertEquals(doc.pubkey, o.getValue("pubkey").jsonPrimitive.content)
        assertEquals(
            doc.createdAt,
            o
                .getValue("created_at")
                .jsonPrimitive.content
                .toLong(),
        )
        assertEquals(
            doc.kind,
            o
                .getValue("kind")
                .jsonPrimitive.content
                .toInt(),
        )
        assertEquals(doc.content, o.getValue("content").jsonPrimitive.content)
        assertEquals(doc.sig, o.getValue("sig").jsonPrimitive.content)
        // The 3-element tag survives whole — tag_index would have dropped the relay hint.
        val eTag =
            o
                .getValue("tags")
                .jsonArray[1]
                .jsonArray
                .map { it.jsonPrimitive.content }
        assertEquals(listOf("e", "f".repeat(64), "wss://relay.example.com", "root"), eTag)
    }

    @Test
    fun `index fields store tags as exact json text and tag_index as the derived view`() {
        val fields = doc.indexFields()
        val storedTags = Json.parseToJsonElement(fields.getValue("tags").jsonPrimitive.content)
        assertEquals(doc.tags, storedTags.jsonArray.map { tag -> tag.jsonArray.map { it.jsonPrimitive.content } })
        assertEquals(doc.tagIndex(), fields.getValue("tag_index").jsonArray.map { it.jsonPrimitive.content })
    }

    @Test
    fun `parses a raw nostr event`() {
        val raw =
            """{"id":"${"1".repeat(64)}","pubkey":"${"2".repeat(64)}","created_at":1700000001,""" +
                """"kind":0,"tags":[["p","${"3".repeat(64)}"]],"content":"{\"name\":\"vitor\"}","sig":"${"4".repeat(128)}"}"""
        val parsed = EventDoc.fromEventJson(raw)
        assertEquals(0, parsed.kind)
        assertEquals(1_700_000_001L, parsed.createdAt)
        assertEquals("""{"name":"vitor"}""", parsed.content)
        assertEquals(listOf(listOf("p", "3".repeat(64))), parsed.tags)
        // ...and what we would serve back is the same event.
        assertEquals(parsed, EventDoc.fromEventJson(parsed.toEventJson()))
    }

    @Test
    fun `malformed event json throws`() {
        assertFailsWith<Exception> { EventDoc.fromEventJson("""{"id":"x"}""") }
    }

    private fun tagged(tags: List<List<String>>) = doc.copy(tags = tags)

    @Test
    fun `dTagOrEmpty is the first d value, empty when absent or valueless`() {
        assertEquals("subject", tagged(listOf(listOf("d", "subject"))).dTagOrEmpty())
        // First d wins; a later d is ignored (firstTagValue semantics).
        assertEquals("first", tagged(listOf(listOf("d", "first"), listOf("d", "second"))).dTagOrEmpty())
        // Extra values are kept as-is; only the first value is the key.
        assertEquals("k", tagged(listOf(listOf("d", "k", "ignored"))).dTagOrEmpty())
        // Missing == empty: no d tag, or a valueless d, both read as "".
        assertEquals("", tagged(listOf(listOf("e", "x".repeat(64)))).dTagOrEmpty())
        assertEquals("", tagged(listOf(listOf("d"))).dTagOrEmpty())
    }

    @Test
    fun `expiresAt is the first parseable expiration, null otherwise`() {
        assertEquals(200L, tagged(listOf(listOf("expiration", "200"))).expiresAt())
        // First expiration tag wins.
        assertEquals(200L, tagged(listOf(listOf("expiration", "200"), listOf("expiration", "300"))).expiresAt())
        // No expiration tag -> never expires.
        assertEquals(null, tagged(listOf(listOf("d", "k"))).expiresAt())
        // Valueless or unparseable expiration contributes nothing (scan continues).
        assertEquals(null, tagged(listOf(listOf("expiration"))).expiresAt())
        assertEquals(300L, tagged(listOf(listOf("expiration", "later"), listOf("expiration", "300"))).expiresAt())
    }
}
