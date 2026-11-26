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

import kotlinx.serialization.json.JsonObject
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction

/**
 * A SEARCH RESPONSE DECODED BY HAND, for the shapes the store's reads get back: the root's count
 * and coverage, and hits whose summary holds an event's fields (`nip01`, `idtime`, `dedup`), its
 * `tag_index` (`idtimetag`) or a ranked search's `matchfeatures`. Decoding that through kotlinx
 * was a third of a relay's CPU under REQ load: its generic lexer reads every key into a String and
 * looks it up in the descriptor's map, and every hit has a dozen keys. Here a key is read once and
 * matched on its length first, and a value with no escape is one `substring`.
 *
 * NOT A SECOND JSON PARSER. Anything outside those shapes returns null and the caller decodes the
 * same text with kotlinx, so a response this has not seen is decoded exactly as before: a
 * degradation report, errors, any summary field past those, an escape or number form it does not
 * take, a null where one is not allowed. [decode] reads the same text kotlinx reads (the payload
 * decoded to UTF-16 once by okio), so a malformed byte is replaced the same way; [decodeStream],
 * for a payload too large for one String, decodes UTF-8 strictly and hands over anything
 * malformed. `matchfeatures` is the one value kotlinx still reads, as the JSON slice it is, so it
 * comes out the [JsonObject] kotlinx would build. `FastSearchDecoderTest` holds both entries to
 * kotlinx's answer on generated responses and on responses captured from Vespa.
 */
internal object FastSearchDecoder {
    /** [text] as kotlinx would decode it into a [SearchEnvelope], or null when it is not a shape this reads. */
    fun decode(text: String): SearchEnvelope? =
        try {
            Reader(text).envelope()
        } catch (_: NotThisShape) {
            null
        }

    /**
     * A payload too large to decode as one String (a G1 humongous allocation, `VespaHttp`'s
     * `STRING_DECODE_MAX_BYTES`), read a hit at a time: the bytes are split at the root's
     * `children` array, each hit decoded on its own, and everything around the array decoded as
     * the small envelope it is. No String larger than one hit is built. Null when the payload is
     * not a shape this reads, or not valid UTF-8; the caller then decodes it with kotlinx.
     */
    fun decodeStream(payload: InputStream): SearchEnvelope? =
        try {
            Splitter().split(payload)
        } catch (_: NotThisShape) {
            null
        }

    /** The one way out of [Reader] and [Splitter]: kotlinx decodes the payload instead. Stackless, so a fallback costs no trace. */
    private object NotThisShape : RuntimeException(null, null, false, false) {
        private fun readResolve(): Any = NotThisShape
    }

    private class Reader(
        private val s: String,
    ) {
        private var i = 0

        /** The key [nextKey] read last, as a region of [s]: compared by length first, then by its characters. */
        private var keyStart = 0
        private var keyLength = 0

        fun envelope(): SearchEnvelope {
            var root = SearchRoot()
            var timing: VespaTiming? = null
            obj {
                when {
                    keyIs("root") -> root = root()
                    keyIs("timing") -> timing = if (isNull()) null else timing()
                    else -> skipValue()
                }
            }
            end()
            return SearchEnvelope(root, timing)
        }

        /** One hit and nothing after it: [Splitter]'s unit. */
        fun hitOnly(): SearchHit = hit().also { end() }

        private fun end() {
            ws()
            if (i != s.length) throw NotThisShape
        }

        private fun root(): SearchRoot {
            var children: List<SearchHit> = emptyList()
            var coverage = SearchCoverage()
            var fields = SearchRootFields()
            obj {
                when {
                    keyIs("children") -> children = hits()

                    keyIs("coverage") -> coverage = coverage()

                    keyIs("fields") -> fields = rootFields()

                    // Errors refuse the answer; that path is kotlinx's, which builds them as before.
                    keyIs("errors") -> emptyArrayOrBail()

                    else -> skipValue()
                }
            }
            return SearchRoot(children, coverage, fields)
        }

        private fun coverage(): SearchCoverage {
            var full = true
            var coverage = 100
            var nodes = 1
            var documents = 0L
            obj {
                when {
                    keyIs("full") -> full = bool()

                    keyIs("coverage") -> coverage = int()

                    keyIs("nodes") -> nodes = int()

                    keyIs("documents") -> documents = long()

                    // A degraded answer is the one worth decoding exactly as before.
                    keyIs("degraded") -> throw NotThisShape

                    else -> skipValue()
                }
            }
            return SearchCoverage(full = full, coverage = coverage, nodes = nodes, documents = documents)
        }

        private fun rootFields(): SearchRootFields {
            var totalCount = 0
            obj {
                when {
                    keyIs("totalCount") -> totalCount = int()
                    else -> skipValue()
                }
            }
            return SearchRootFields(totalCount)
        }

        private fun timing(): VespaTiming {
            var query = 0.0
            var summary = 0.0
            var search = 0.0
            obj {
                when {
                    keyIs("querytime") -> query = double()
                    keyIs("summaryfetchtime") -> summary = double()
                    keyIs("searchtime") -> search = double()
                    else -> skipValue()
                }
            }
            return VespaTiming(query, summary, search)
        }

        private fun hits(): List<SearchHit> {
            val out = ArrayList<SearchHit>()
            array { out += hit() }
            return out
        }

        private fun hit(): SearchHit {
            var fields: VespaSummary? = null
            var relevance = 0.0
            obj {
                when {
                    keyIs("fields") -> fields = if (isNull()) null else summary()
                    keyIs("relevance") -> relevance = double()
                    else -> skipValue()
                }
            }
            return SearchHit(fields, relevance)
        }

        /** An event's fields, its `tag_index` and a ranked hit's `matchfeatures`: any other summary field is a shape kotlinx decodes. */
        private fun summary(): VespaSummary {
            var id = ""
            var pubkey = ""
            var createdAt = 0L
            var kind = 0
            var tags = "[]"
            var content = ""
            var sig = ""
            var owner: String? = null
            var tagIndex: List<String>? = null
            var matchfeatures: JsonObject? = null
            obj {
                // In the order Vespa writes them, so the common key matches first.
                when {
                    // Vespa's own, on every hit; VespaSummary has no such field.
                    keyIs("sddocname") -> skipValue()

                    keyIs("id") -> id = string()

                    keyIs("pubkey") -> pubkey = string()

                    keyIs("created_at") -> createdAt = long()

                    keyIs("kind") -> kind = int()

                    keyIs("tags") -> tags = string()

                    keyIs("content") -> content = string()

                    keyIs("sig") -> sig = string()

                    keyIs("owner") -> owner = if (isNull()) null else string()

                    keyIs("tag_index") -> tagIndex = if (isNull()) null else strings()

                    keyIs("matchfeatures") -> matchfeatures = if (isNull()) null else jsonObject()

                    else -> throw NotThisShape
                }
            }
            return VespaSummary(
                id = id,
                pubkey = pubkey,
                createdAt = createdAt,
                kind = kind,
                tags = tags,
                content = content,
                sig = sig,
                owner = owner,
                tagIndex = tagIndex,
                matchfeatures = matchfeatures,
            )
        }

        // ---- structure -------------------------------------------------------

        /**
         * An object: [member] runs once per key, with the key already read ([nextKey]) and [i] on
         * its value, which it must consume. Duplicate keys are read in order, so the last wins, as
         * in kotlinx.
         */
        private inline fun obj(member: () -> Unit) {
            ws()
            expect('{')
            ws()
            if (peek() == '}') {
                i++
                return
            }
            while (true) {
                ws()
                nextKey()
                member()
                ws()
                when (next()) {
                    ',' -> continue
                    '}' -> return
                    else -> throw NotThisShape
                }
            }
        }

        /** An array: [element] runs once per element, with [i] on it. */
        private inline fun array(element: () -> Unit) {
            ws()
            expect('[')
            ws()
            if (peek() == ']') {
                i++
                return
            }
            while (true) {
                ws()
                element()
                ws()
                when (next()) {
                    ',' -> continue
                    ']' -> return
                    else -> throw NotThisShape
                }
            }
        }

        /**
         * The key at [i], read once: its region is [keyStart] and [keyLength], and [i] is left on
         * its value, past the colon. A key holding an escape is handed over: `"id"` is `id`
         * to kotlinx, and a region compared as written would miss it.
         */
        private fun nextKey() {
            expect('"')
            val quote = s.indexOf('"', i)
            if (quote < 0 || nextSlash(i) in i until quote) throw NotThisShape
            keyStart = i
            keyLength = quote - i
            i = quote + 1
            ws()
            expect(':')
            ws()
        }

        /** Whether the key [nextKey] read is exactly [name]; the length is compared first, which turns most candidates away at once. */
        private fun keyIs(name: String): Boolean = keyLength == name.length && s.regionMatches(keyStart, name, 0, keyLength)

        private fun emptyArrayOrBail() {
            expect('[')
            ws()
            expect(']')
        }

        private fun skipValue() {
            when (peek()) {
                '"' -> {
                    skipString()
                }

                '{' -> {
                    obj { skipValue() }
                }

                '[' -> {
                    array { skipValue() }
                }

                else -> {
                    // A number or a literal: up to where the structure resumes.
                    val start = i
                    while (i < s.length && s[i] !in STOPS) i++
                    if (i == start) throw NotThisShape
                }
            }
        }

        // ---- values ----------------------------------------------------------

        private fun isNull(): Boolean {
            if (!s.startsWith("null", i)) return false
            i += 4
            return true
        }

        private fun bool(): Boolean =
            when {
                s.startsWith("true", i) -> {
                    i += 4
                    true
                }

                s.startsWith("false", i) -> {
                    i += 5
                    false
                }

                else -> {
                    throw NotThisShape
                }
            }

        /** A plain integer: digits with an optional minus. A fraction or exponent is kotlinx's to refuse or read. */
        private fun long(): Long {
            var j = i
            val negative = j < s.length && s[j] == '-'
            if (negative) j++
            val digits = j
            var v = 0L
            while (j < s.length) {
                val c = s[j]
                if (c !in '0'..'9') break
                // 18 digits cannot overflow; longer ones are rare enough to hand over.
                if (j - digits >= 18) throw NotThisShape
                v = v * 10 + (c - '0')
                j++
            }
            if (j == digits || (j < s.length && s[j] !in STOPS)) throw NotThisShape
            i = j
            return if (negative) -v else v
        }

        private fun int(): Int {
            val v = long()
            if (v < Int.MIN_VALUE || v > Int.MAX_VALUE) throw NotThisShape
            return v.toInt()
        }

        private fun double(): Double {
            // An unranked read's relevance, on every hit of it: no token to cut and parse.
            if (s.startsWith("0.0", i) && stopsAt(i + 3)) {
                i += 3
                return 0.0
            }
            if (s.startsWith("1.0", i) && stopsAt(i + 3)) {
                i += 3
                return 1.0
            }
            val start = i
            while (i < s.length && s[i] !in STOPS) i++
            val token = s.substring(start, i)
            // JSON numbers only: no NaN or Infinity, which kotlinx refuses by default.
            if (token.isEmpty() || token.any { it !in NUMBER_CHARS }) throw NotThisShape
            return token.toDoubleOrNull() ?: throw NotThisShape
        }

        private fun stopsAt(j: Int): Boolean = j >= s.length || s[j] in STOPS

        private fun strings(): List<String> {
            val out = ArrayList<String>()
            array {
                if (peek() != '"') throw NotThisShape
                out += string()
            }
            return out
        }

        /** The JSON object at [i], read by kotlinx from its own slice of the text: the same [JsonObject] kotlinx builds when it decodes the whole answer. */
        private fun jsonObject(): JsonObject {
            if (peek() != '{') throw NotThisShape
            val start = i
            skipValue()
            return VESPA_JSON.parseToJsonElement(s.substring(start, i)) as? JsonObject ?: throw NotThisShape
        }

        /**
         * Where the next backslash at or past the read position is: -1 when the text holds no more,
         * -2 before the first look. The position only moves forward, so remembering it keeps every
         * search for one linear over the whole text, where a fresh [String.indexOf] from each
         * string's start would scan to the end of a text with none in it, once per string.
         */
        private var slashAt = -2

        private fun nextSlash(from: Int): Int {
            if (slashAt != -1 && slashAt < from) slashAt = s.indexOf('\\', from)
            return slashAt
        }

        /**
         * A string value: one substring when it holds no escape, the escapes resolved when it does.
         * Both ends are found with [String.indexOf], the JDK's vectorized search: this read is most of
         * a hit's characters (an id, a pubkey, a sig, the content). A raw control character is kept,
         * as kotlinx keeps it.
         */
        private fun string(): String {
            expect('"')
            val start = i
            val quote = s.indexOf('"', start)
            if (quote < 0) throw NotThisShape
            if (nextSlash(start) in start until quote) return escaped(start)
            i = quote + 1
            return s.substring(start, quote)
        }

        /**
         * The string that starts at [start] and holds an escape: the runs between escapes copied whole,
         * into a builder sized to the string's escaped length, which its unescaped length never exceeds,
         * so it never grows. (Started at 64 it grew two to four times on a typical `tags` value.)
         */
        private fun escaped(start: Int): String {
            // Measured first, then read: the measuring walks [slashAt] to the string's end, and the
            // read below starts over from [start], so the walk is undone or the read would find no
            // escape left and return the string still escaped.
            val atStart = slashAt
            val out = StringBuilder(closingQuote(start) - start)
            slashAt = atStart
            var run = start
            while (true) {
                val quote = s.indexOf('"', run)
                if (quote < 0) throw NotThisShape
                val slash = nextSlash(run)
                if (slash !in 0 until quote) {
                    out.append(s, run, quote)
                    i = quote + 1
                    return out.toString()
                }
                out.append(s, run, slash)
                i = slash + 1
                if (i >= s.length) throw NotThisShape
                when (s[i++]) {
                    '"' -> out.append('"')
                    '\\' -> out.append('\\')
                    '/' -> out.append('/')
                    'b' -> out.append('\b')
                    'f' -> out.append('\u000C')
                    'n' -> out.append('\n')
                    'r' -> out.append('\r')
                    't' -> out.append('\t')
                    'u' -> out.append(hex4())
                    else -> throw NotThisShape
                }
                run = i
            }
        }

        /** The four hex digits of a `\u` escape, as the UTF-16 unit they spell. ASCII hex only: Character.digit takes other scripts' digits too. */
        private fun hex4(): Char {
            if (i + 4 > s.length) throw NotThisShape
            var code = 0
            repeat(4) {
                val h =
                    when (val d = s[i++]) {
                        in '0'..'9' -> d - '0'
                        in 'a'..'f' -> d - 'a' + 10
                        in 'A'..'F' -> d - 'A' + 10
                        else -> throw NotThisShape
                    }
                code = code * 16 + h
            }
            return code.toChar()
        }

        /**
         * The index of the quote that closes the string whose content starts at [start]: past every
         * escape, the character each escapes included. Moves nothing but [slashAt], to the string's
         * end; a caller that reads the string again from [start] restores it ([escaped]).
         */
        private fun closingQuote(start: Int): Int {
            var run = start
            while (true) {
                val quote = s.indexOf('"', run)
                if (quote < 0) throw NotThisShape
                val slash = nextSlash(run)
                if (slash !in 0 until quote) return quote
                // Past the escape and the character it escapes, which may be the quote just found.
                run = slash + 2
                if (run > s.length) throw NotThisShape
            }
        }

        private fun skipString() {
            expect('"')
            i = closingQuote(i) + 1
        }

        // ---- characters ------------------------------------------------------

        private fun ws() {
            while (i < s.length) {
                val c = s[i]
                if (c != ' ' && c != '\n' && c != '\r' && c != '\t') return
                i++
            }
        }

        private fun peek(): Char = if (i < s.length) s[i] else throw NotThisShape

        private fun next(): Char = if (i < s.length) s[i++] else throw NotThisShape

        private fun expect(c: Char) {
            if (i >= s.length || s[i] != c) throw NotThisShape
            i++
        }
    }

    /**
     * [decodeStream]'s byte-level pass. It tracks only what splitting needs: the nesting depth, whether
     * it is inside a string (a UTF-8 multibyte sequence never holds a `"`, `\`, bracket or brace byte,
     * so bytes can be read as structure without decoding), and the key last read at the two outer
     * levels, which is how the root's `children` array is told from a `children` key anywhere else.
     * Everything outside that array is copied to a small skeleton with the array emptied; each element
     * of it is decoded by [Reader.hitOnly] as soon as it closes.
     */
    private class Splitter {
        private val skeleton = ByteArrayOutputStream(1024)
        private val hit = ByteArrayOutputStream(16 * 1024)
        private val hits = ArrayList<SearchHit>()

        /** Strict UTF-8: a malformed byte is kotlinx's to replace, not this pass's to guess. */
        private val utf8 =
            Charsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)

        private var depth = 0
        private var inString = false
        private var escaped = false

        /** The bytes of the string being read, kept only at the two outer levels, where a key can be `root` or `children`. */
        private val token = ByteArrayOutputStream(32)

        /** The key last completed at depth 1 (the envelope's) and depth 2 (the root's). */
        private var outerKey: String? = null
        private var rootKey: String? = null
        private var lastString: String? = null

        /** 0 outside the root's children; else the depth of the array, whose elements are hits. */
        private var childrenDepth = 0

        /** Set once the root's children have been read: a second array under that key replaces them, as kotlinx keeps the last. */
        private var sawChildren = false

        fun split(payload: InputStream): SearchEnvelope {
            val buf = ByteArray(64 * 1024)
            while (true) {
                val n = payload.read(buf)
                if (n < 0) break
                for (k in 0 until n) step(buf[k])
            }
            if (depth != 0 || inString) throw NotThisShape
            val envelope = Reader(text(skeleton)).envelope()
            val root = envelope.root
            return SearchEnvelope(SearchRoot(hits, root.coverage, root.fields, root.errors), envelope.timing)
        }

        private fun step(b: Byte) {
            val c = b.toInt()
            if (childrenDepth != 0 && depth > childrenDepth) {
                hit.write(c)
                track(c)
                if (depth == childrenDepth) {
                    // A hit just closed: decode it on its own and start the next.
                    hits += Reader(text(hit)).hitOnly()
                    hit.reset()
                }
                return
            }
            if (childrenDepth != 0 && depth == childrenDepth) {
                // Between hits: commas and whitespace are not the skeleton's; a bracket ends the array.
                when (c) {
                    '{'.code -> {
                        hit.write(c)
                        track(c)
                    }

                    ']'.code -> {
                        skeleton.write(c)
                        track(c)
                        childrenDepth = 0
                    }

                    ','.code, ' '.code, '\n'.code, '\r'.code, '\t'.code -> {}

                    // Only objects are hits; anything else is a shape kotlinx reads.
                    else -> {
                        throw NotThisShape
                    }
                }
                return
            }
            skeleton.write(c)
            val before = depth
            track(c)
            if (c == '['.code && before == 2 && outerKey == "root" && rootKey == "children") {
                if (sawChildren) hits.clear()
                sawChildren = true
                childrenDepth = depth
            }
        }

        /** Depth, string and key bookkeeping for one structural byte. */
        private fun track(c: Int) {
            if (inString) {
                when {
                    escaped -> {
                        escaped = false
                        if (depth <= 2) token.write(c)
                    }

                    c == '\\'.code -> {
                        escaped = true
                        // An escaped key at the outer levels is never `root` or `children` as written: kotlinx's call.
                        if (depth <= 2) token.write(c)
                    }

                    c == '"'.code -> {
                        inString = false
                        if (depth <= 2) lastString = token.toString(Charsets.UTF_8)
                    }

                    else -> {
                        if (depth <= 2) token.write(c)
                    }
                }
                return
            }
            when (c) {
                '"'.code -> {
                    inString = true
                    token.reset()
                }

                ':'.code -> {
                    when (depth) {
                        1 -> outerKey = lastString
                        2 -> rootKey = lastString
                    }
                }

                '{'.code, '['.code -> {
                    depth++
                }

                '}'.code, ']'.code -> {
                    depth--
                    if (depth < 0) throw NotThisShape
                }
            }
        }

        private fun text(bytes: ByteArrayOutputStream): String =
            try {
                utf8.reset()
                utf8.decode(ByteBuffer.wrap(bytes.toByteArray(), 0, bytes.size())).toString()
            } catch (_: CharacterCodingException) {
                throw NotThisShape
            }
    }

    /** Where a number or literal ends. */
    private val STOPS = charArrayOf(',', '}', ']', ' ', '\n', '\r', '\t')

    private const val NUMBER_CHARS = "0123456789+-.eE"
}
