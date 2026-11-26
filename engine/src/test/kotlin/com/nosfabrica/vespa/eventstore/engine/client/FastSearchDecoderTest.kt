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

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromStream
import java.io.ByteArrayInputStream
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * [FastSearchDecoder] answers what kotlinx answers, or nothing. Every response here is decoded
 * both ways, and through both of the hand decoder's entries: [FastSearchDecoder.decode] on the
 * text, held to kotlinx's `decodeFromString`, and [FastSearchDecoder.decodeStream] on its UTF-8
 * bytes, held to kotlinx's `decodeFromStream` (the two fallbacks `VespaHttp.decodeSearch` uses).
 * Where the hand decoder returns a value it must be kotlinx's, field for field; where kotlinx
 * throws, the hand decoder must not return one.
 */
class FastSearchDecoderTest {
    /** Every field the callers read, rendered so two envelopes compare as text. */
    private fun SearchEnvelope.canon(): String =
        buildString {
            append("timing=").append(timing?.let { "${it.querytime}/${it.summaryfetchtime}/${it.searchtime}" })
            append(" total=").append(root.fields.totalCount)
            with(root.coverage) { append(" cov=$full/$coverage/$nodes/$documents/$degraded") }
            append(" errors=").append(root.errors.map { "${it.code}|${it.summary}|${it.source}|${it.message}" })
            root.children.forEach { h ->
                append("\n hit rel=").append(h.relevance)
                h.fields?.let { f ->
                    append(" id=${f.id} pk=${f.pubkey} at=${f.createdAt} k=${f.kind} sig=${f.sig} owner=${f.owner}")
                    append(" tags=").append(f.tags.codes()).append(" content=").append(f.content.codes())
                    append(" rest=${f.tagIndex}/${f.name}/${f.matchfeatures}")
                } ?: append(" fields=null")
            }
        }

    /** A string with its UTF-16 units spelled out, so a lone surrogate or a control character shows. */
    private fun String.codes(): String = map { if (it in ' '..'~') it.toString() else "\\u%04x".format(it.code) }.joinToString("")

    private companion object {
        val PRETTY = Json { prettyPrint = true }
    }

    private fun kotlinx(text: String): SearchEnvelope = VESPA_JSON.decodeFromString(SearchEnvelope.serializer(), text)

    @OptIn(ExperimentalSerializationApi::class)
    private fun kotlinxStream(bytes: ByteArray): SearchEnvelope = VESPA_JSON.decodeFromStream(SearchEnvelope.serializer(), ByteArrayInputStream(bytes))

    private fun split(bytes: ByteArray): SearchEnvelope? = FastSearchDecoder.decodeStream(ByteArrayInputStream(bytes))

    /** Both entries read [text], and each answers what its kotlinx fallback answers. */
    private fun assertSame(text: String) {
        val fast = assertNotNull(FastSearchDecoder.decode(text), "the hand decoder reads this shape:\n$text")
        assertEquals(kotlinx(text).canon(), fast.canon(), text)
        // The bytes a large payload arrives as (a lone surrogate written as `?`, as the wire carries it).
        val bytes = text.toByteArray(Charsets.UTF_8)
        val split = assertNotNull(split(bytes), "the hand decoder reads this shape a hit at a time too:\n$text")
        assertEquals(kotlinxStream(bytes).canon(), split.canon(), text)
    }

    // ---- generated responses ---------------------------------------------------

    private val pieces =
        listOf(
            "a",
            "Z",
            "nostr",
            " ",
            "\"",
            "\\",
            "/",
            "\n",
            "\r",
            "\t",
            "\b",
            "\u000C",
            "\u0000",
            "\u001F",
            "\u007F",
            "\u00E9",
            "\u4E16\u754C",
            "\uD83D\uDE00",
            "\uD83D",
            "\uDE00",
            "\u2028",
            "\u200B",
            "\uFEFF",
            "[\"p\",\"x\"]",
            "}",
            "{",
            "]",
            ",",
        )

    private fun Random.text(max: Int): String = buildString { repeat(nextInt(max + 1)) { append(pieces[nextInt(pieces.size)]) } }

    private fun Random.hex(n: Int): String = buildString { repeat(n) { append("0123456789abcdef"[nextInt(16)]) } }

    private fun Random.tagsJson(): String {
        val tags = List(nextInt(5)) { JsonArray(List(1 + nextInt(3)) { JsonPrimitive(if (nextBoolean()) hex(64) else text(6)) }) }
        return JsonArray(tags).toString()
    }

    private fun Random.summary(): JsonObject =
        JsonObject(
            buildMap {
                if (nextInt(10) > 0) put("sddocname", JsonPrimitive("event"))
                put("id", JsonPrimitive(hex(64)))
                if (nextInt(10) > 0) put("pubkey", JsonPrimitive(hex(64)))
                put("created_at", JsonPrimitive(nextLong(0, 2_000_000_000)))
                if (nextInt(10) > 0) put("kind", JsonPrimitive(nextInt(0, 40_000)))
                if (nextInt(10) > 0) put("tags", JsonPrimitive(tagsJson()))
                if (nextInt(10) > 0) put("content", JsonPrimitive(text(40)))
                if (nextInt(10) > 0) put("sig", JsonPrimitive(hex(128)))
                when (nextInt(3)) {
                    0 -> put("owner", JsonPrimitive(hex(64)))
                    1 -> put("owner", JsonNull)
                }
                // The idtimetag summary's array, and a ranked search's features.
                when (nextInt(5)) {
                    0 -> put("tag_index", JsonArray(List(nextInt(4)) { JsonPrimitive("t:" + text(6)) }))
                    1 -> put("tag_index", JsonNull)
                }
                when (nextInt(6)) {
                    0 -> put("matchfeatures", JsonObject(mapOf("freshness" to JsonPrimitive(nextDouble()), "itemRawScore(mtch_exact)" to JsonPrimitive(0.0), "n" to JsonPrimitive(nextInt()))))
                    1 -> put("matchfeatures", JsonNull)
                }
            },
        )

    private fun Random.hit(): JsonObject =
        JsonObject(
            buildMap {
                put("id", JsonPrimitive("index:content/0/${hex(24)}"))
                put("relevance", JsonPrimitive(listOf(0.0, 1.0, nextDouble(), -nextDouble() * 1e6, 1.5e-7)[nextInt(5)]))
                put("source", JsonPrimitive("content"))
                put("fields", if (nextInt(20) == 0) JsonNull else summary())
            },
        )

    private fun Random.envelope(): JsonObject {
        val root =
            buildMap<String, JsonElement> {
                put("id", JsonPrimitive("toplevel"))
                put("relevance", JsonPrimitive(1.0))
                put("fields", JsonObject(mapOf("totalCount" to JsonPrimitive(nextInt(0, 1_000_000)))))
                put(
                    "coverage",
                    JsonObject(
                        mapOf(
                            "coverage" to JsonPrimitive(nextInt(0, 101)),
                            "documents" to JsonPrimitive(nextLong(0, 1L shl 40)),
                            "full" to JsonPrimitive(nextBoolean()),
                            "nodes" to JsonPrimitive(nextInt(1, 9)),
                            "results" to JsonPrimitive(1),
                            "resultsFull" to JsonPrimitive(1),
                        ),
                    ),
                )
                if (nextInt(4) > 0) put("children", JsonArray(List(nextInt(12)) { hit() }))
            }
        return JsonObject(
            buildMap {
                if (nextInt(3) == 0) {
                    put("timing", JsonObject(mapOf("querytime" to JsonPrimitive(nextDouble()), "summaryfetchtime" to JsonPrimitive(0.001), "searchtime" to JsonPrimitive(nextDouble()))))
                }
                put("root", JsonObject(root))
            },
        )
    }

    /** kotlinx's own rendering, then the same with the spacing and escapes another writer might use. */
    private fun Random.renderings(e: JsonObject): List<String> {
        val compact = e.toString()
        val spaced = PRETTY.encodeToString(JsonObject.serializer(), e)
        // `\/` and `\u` escapes for characters kotlinx writes plainly: the same strings.
        val escaped = compact.replace("/", "\\/").replace("\u00E9", "\\u00e9").replace("\u4E16", "\\u4E16")
        return listOf(compact, spaced, escaped)
    }

    @Test
    fun `generated responses decode as kotlinx decodes them`() {
        val rnd = Random(2026_11_26)
        var read = 0
        repeat(4_000) {
            for (text in rnd.renderings(rnd.envelope())) {
                assertSame(text)
                read++
            }
        }
        assertEquals(12_000, read)
    }

    @Test
    fun `a response cut short is never answered where kotlinx refuses it`() {
        val rnd = Random(7)
        repeat(300) {
            val text = rnd.envelope().toString()
            val cut = text.substring(0, rnd.nextInt(text.length))
            val fast = FastSearchDecoder.decode(cut)
            val slow = runCatching { kotlinx(cut) }
            if (fast != null) assertEquals(slow.getOrThrow().canon(), fast.canon(), "a value from a cut response must be kotlinx's: $cut")
            val bytes = cut.toByteArray(Charsets.UTF_8)
            val split = split(bytes)
            if (split != null) assertEquals(runCatching { kotlinxStream(bytes) }.getOrThrow().canon(), split.canon(), "a value from a cut payload must be kotlinx's: $cut")
        }
    }

    /** The response a recency read got back from Vespa 8.788 (nip01 summary), as sent. */
    @Test
    fun `a real nip01 page decodes as kotlinx decodes it`() {
        val real =
            """{"root":{"id":"toplevel","relevance":1.0,"fields":{"totalCount":101089},"coverage":{"coverage":100,"documents":101089,"full":true,"nodes":1,"results":1,"resultsFull":1},""" +
                """"children":[{"id":"index:content/0/cf27e785964a4a14dd33aef5","relevance":0.0,"source":"content","fields":{"sddocname":"event",""" +
                """"id":"93adc4ba84f6ea361ba0c763e591771d82431224c5409cc27c0b7571ac1cde75","pubkey":"5311998b7ebe577013b9a96bcb46191085c21bdf1e0a5b098005736bcc1ac1bb",""" +
                """"created_at":1795661594,"kind":1,"tags":"[[\"p\",\"5311998b7ebe577013b9a96bcb46191085c21bdf1e0a5b098005736bcc1ac1bb\"],[\"t\",\"nostr-bench-\"]]",""" +
                """"content":"This is a message from nostr-bench client","sig":"f260b39c55ccd359758f1f751a7b79d257972fd9bbdf8c318a7ff37f3175486f66e28936a48d66560b8b08bc2ba59f9f5439c5a6a94a4bbfb8c5bd4442d59ebf",""" +
                """"owner":"5311998b7ebe577013b9a96bcb46191085c21bdf1e0a5b098005736bcc1ac1bb"}}]}}"""
        assertSame(real)
        // The idtime summary: ids and times only.
        assertSame("""{"root":{"fields":{"totalCount":2},"coverage":{"coverage":100,"documents":5,"full":true,"nodes":1},"children":[{"relevance":0.0,"fields":{"id":"ab","created_at":5}},{"fields":{"id":"cd","created_at":4}}]}}""")
        // A key twice: the last wins, in kotlinx and here.
        assertSame("""{"root":{"children":[{"fields":{"id":"ab"}}],"children":[{"fields":{"id":"cd"}},{"fields":{"id":"ef"}}]}}""")
        // No hits at all, and no children key.
        assertSame("""{"root":{"id":"toplevel","relevance":1.0,"fields":{"totalCount":0},"coverage":{"coverage":100,"documents":0,"full":true,"nodes":1,"results":1,"resultsFull":1}}}""")
    }

    /**
     * ANSWERS AS VESPA WRITES THEM, captured from vespaengine/vespa 8.763 (`vespa-answers/`): notes with
     * emoji (which Vespa writes as escaped surrogate pairs, `\uD83D\uDE00`), CJK and Hebrew (written raw),
     * a ZWJ sequence, a BOM, DEL, quotes, backslashes, newlines, tabs, a kind 0's JSON content and tags
     * holding all of it; the `nip01`, `idtimetag` (`tag_index`) summaries, and ranked searches whose hits
     * carry `matchfeatures` (`text_relevance`, `search`).
     */
    @Test
    fun `answers captured from Vespa decode as kotlinx decodes them`() {
        for (name in listOf("nip01", "idtimetag", "rank-text_relevance", "rank-search")) {
            val text = assertNotNull(javaClass.getResource("/vespa-answers/$name.json"), "capture $name").readText(Charsets.UTF_8)
            assertSame(text)
            assertTrue(kotlinx(text).root.children.isNotEmpty(), "$name holds hits")
        }
    }

    /** A page past `STRING_DECODE_MAX_BYTES` (~1.4 MB here): [FastSearchDecoder.decodeStream] reads it a hit at a time, as kotlinx's stream does. */
    @Test
    fun `a page too large for one String decodes a hit at a time`() {
        val rnd = Random(11)
        val hits = JsonArray(List(3_000) { rnd.hit() })
        val big = JsonObject(mapOf("root" to JsonObject(mapOf("fields" to JsonObject(mapOf("totalCount" to JsonPrimitive(3_000))), "children" to hits)))).toString()
        assertTrue(big.length > 1_000_000, "a large page: ${big.length}")
        assertSame(big)
        assertEquals(3_000, split(big.toByteArray(Charsets.UTF_8))!!.root.children.size)
    }

    /** Each of these is a shape kotlinx reads differently, or decides alone: the hand decoder hands it over. */
    @Test
    fun `what the hand decoder does not read it leaves to kotlinx`() {
        val hit = """{"relevance":0.0,"fields":{"id":"ab","created_at":5}}"""

        fun env(
            root: String = "",
            hitJson: String = hit,
        ) = """{"root":{"fields":{"totalCount":1},"coverage":{"coverage":100,"documents":5,"full":true,"nodes":1}$root,"children":[$hitJson]}}"""
        val handedOver =
            mapOf(
                "a degradation report" to """{"root":{"coverage":{"coverage":50,"degraded":{"match-phase":true,"timeout":false}},"children":[]}}""",
                "errors" to env(""","errors":[{"code":12,"summary":"Timed out","message":"m"}]"""),
                "a summary field past those it reads" to env(hitJson = """{"fields":{"id":"ab","documentid":"id:event:event::ab"}}"""),
                "a null inside tag_index" to env(hitJson = """{"fields":{"id":"ab","tag_index":["t:x",null]}}"""),
                "match-features that are not an object" to env(hitJson = """{"fields":{"id":"ab","matchfeatures":[1]}}"""),
                "a hit that is not an object" to """{"root":{"children":[1]}}""",
                "an escaped key" to env(hitJson = """{"fields":{"\u0069d":"ab"}}"""),
                "an escaped key outside the summary" to """{"ro\u006ft":{"children":[]}}""",
                "a fraction where an integer goes" to env(hitJson = """{"fields":{"id":"ab","created_at":5.0}}"""),
                "a number too long to read safely" to env(hitJson = """{"fields":{"id":"ab","created_at":12345678901234567890}}"""),
                "a null where a string goes" to env(hitJson = """{"fields":{"id":null}}"""),
                "a non-ASCII digit in a \\u escape" to env(hitJson = "{\"fields\":{\"id\":\"\\u00\u0661\u0669\"}}"),
                "NaN" to env(hitJson = """{"relevance":NaN,"fields":{"id":"ab"}}"""),
                "a trailing value" to env() + " {}",
            )
        for ((what, text) in handedOver) {
            assertNull(FastSearchDecoder.decode(text), "$what goes to kotlinx: $text")
            assertNull(split(text.toByteArray(Charsets.UTF_8)), "$what goes to kotlinx a hit at a time too: $text")
        }
        assertNull(split(byteArrayOf('{'.code.toByte(), '"'.code.toByte(), 0xC3.toByte(), '"'.code.toByte(), ':'.code.toByte(), '1'.code.toByte(), '}'.code.toByte())), "malformed UTF-8 is kotlinx's to replace")
    }

    /** kotlinx keeps a raw control character inside a string (it is not JSON, but it reads it); so does the hand decoder. */
    @Test
    fun `a raw control character is kept, as kotlinx keeps it`() {
        for (raw in listOf("\u0000", "\u0001", "\n", "\u001F")) assertSame("{\"root\":{\"children\":[{\"fields\":{\"id\":\"a${raw}b\"}}]}}")
    }

    @Test
    fun `escapes resolve to the characters kotlinx resolves`() {
        val strings = listOf("plain", "quote\" back\\ slash/", "\b\u000C\n\r\t", "\u0000\u001F", "caf\u00E9", "\uD83D\uDE00", "lone \uD83D high", "lone \uDE00 low")
        for (s in strings) {
            val body = JsonPrimitive(s).toString()
            // kotlinx's own escaping, and every character as a \u escape.
            val unicode = "\"" + s.map { "\\u%04x".format(it.code) }.joinToString("") + "\""
            for (literal in listOf(body, unicode)) {
                val text = """{"root":{"children":[{"fields":{"id":"x","content":$literal}}]}}"""
                assertSame(text)
                assertTrue(
                    FastSearchDecoder
                        .decode(text)!!
                        .root.children
                        .single()
                        .fields!!
                        .content == s,
                    "$literal reads as the string it spells",
                )
            }
        }
        if (FastSearchDecoder.decode("""{"root":{"children":[{"fields":{"id":"\q"}}]}}""") != null) fail("an escape JSON does not define is kotlinx's to refuse")
    }
}
