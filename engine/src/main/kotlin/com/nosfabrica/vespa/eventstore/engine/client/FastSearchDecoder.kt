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

/**
 * A SEARCH RESPONSE DECODED BY HAND, for the shape a recency read gets back nearly every time:
 * the root's count and coverage, and hits whose summary holds an event's fields (`nip01`,
 * `idtime`). Decoding that through kotlinx was a third of a relay's CPU under REQ load: its
 * generic lexer reads every key into a String and looks it up in the descriptor's map, and every
 * hit has a dozen keys. Here a key is matched where it lies, without a String, and a value with no
 * escape is one `substring`.
 *
 * NOT A SECOND JSON PARSER. Anything outside that shape returns null and the caller decodes the
 * same text with kotlinx, so a response this has not seen is decoded exactly as before: a
 * degradation report, errors, match-features or any summary field past an event's own, an escape
 * or number form it does not take, a null where one is not allowed. It reads the same text
 * kotlinx reads (the payload decoded to UTF-16 once by okio), so a malformed byte is replaced the
 * same way. `FastSearchDecoderTest` holds it to kotlinx's answer on generated and real responses.
 */
internal object FastSearchDecoder {
    /** [text] as kotlinx would decode it into a [SearchEnvelope], or null when it is not the shape this reads. */
    fun decode(text: String): SearchEnvelope? =
        try {
            Reader(text).envelope()
        } catch (_: NotThisShape) {
            null
        }

    /** The one way out of [Reader]: kotlinx decodes the text instead. Stackless, so a fallback costs no trace. */
    private object NotThisShape : RuntimeException(null, null, false, false) {
        private fun readResolve(): Any = NotThisShape
    }

    private class Reader(
        private val s: String,
    ) {
        private var i = 0

        fun envelope(): SearchEnvelope {
            var root = SearchRoot()
            var timing: VespaTiming? = null
            obj {
                when {
                    key("root") -> root = root()
                    key("timing") -> timing = if (isNull()) null else timing()
                    else -> skipKeyAndValue()
                }
            }
            ws()
            if (i != s.length) throw NotThisShape
            return SearchEnvelope(root, timing)
        }

        private fun root(): SearchRoot {
            var children: List<SearchHit> = emptyList()
            var coverage = SearchCoverage()
            var fields = SearchRootFields()
            obj {
                when {
                    key("children") -> children = hits()

                    key("coverage") -> coverage = coverage()

                    key("fields") -> fields = rootFields()

                    // Errors refuse the answer; that path is kotlinx's, which builds them as before.
                    key("errors") -> emptyArrayOrBail()

                    else -> skipKeyAndValue()
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
                    key("full") -> full = bool()

                    key("coverage") -> coverage = int()

                    key("nodes") -> nodes = int()

                    key("documents") -> documents = long()

                    // A degraded answer is the one worth decoding exactly as before.
                    key("degraded") -> throw NotThisShape

                    else -> skipKeyAndValue()
                }
            }
            return SearchCoverage(full = full, coverage = coverage, nodes = nodes, documents = documents)
        }

        private fun rootFields(): SearchRootFields {
            var totalCount = 0
            obj {
                when {
                    key("totalCount") -> totalCount = int()
                    else -> skipKeyAndValue()
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
                    key("querytime") -> query = double()
                    key("summaryfetchtime") -> summary = double()
                    key("searchtime") -> search = double()
                    else -> skipKeyAndValue()
                }
            }
            return VespaTiming(query, summary, search)
        }

        private fun hits(): List<SearchHit> {
            expect('[')
            ws()
            if (peek() == ']') {
                i++
                return emptyList()
            }
            val out = ArrayList<SearchHit>()
            while (true) {
                ws()
                out += hit()
                ws()
                when (next()) {
                    ',' -> continue
                    ']' -> return out
                    else -> throw NotThisShape
                }
            }
        }

        private fun hit(): SearchHit {
            var fields: VespaSummary? = null
            var relevance = 0.0
            obj {
                when {
                    key("fields") -> fields = if (isNull()) null else summary()
                    key("relevance") -> relevance = double()
                    else -> skipKeyAndValue()
                }
            }
            return SearchHit(fields, relevance)
        }

        /** An event's fields and nothing else: any other summary field is a shape kotlinx decodes. */
        private fun summary(): VespaSummary {
            var id = ""
            var pubkey = ""
            var createdAt = 0L
            var kind = 0
            var tags = "[]"
            var content = ""
            var sig = ""
            var owner: String? = null
            obj {
                when {
                    key("id") -> id = string()

                    key("pubkey") -> pubkey = string()

                    key("created_at") -> createdAt = long()

                    key("kind") -> kind = int()

                    key("tags") -> tags = string()

                    key("content") -> content = string()

                    key("sig") -> sig = string()

                    key("owner") -> owner = if (isNull()) null else string()

                    // Vespa's own, on every hit; VespaSummary has no such field.
                    key("sddocname") -> skipValue()

                    else -> throw NotThisShape
                }
            }
            return VespaSummary(id = id, pubkey = pubkey, createdAt = createdAt, kind = kind, tags = tags, content = content, sig = sig, owner = owner)
        }

        // ---- structure -------------------------------------------------------

        /**
         * An object: [member] runs once per key, positioned AT the key's opening quote. It must
         * consume the key, the colon and the value, as [key] followed by a value read, or as
         * [skipKeyAndValue]. Duplicate keys are read in order, so the last wins, as in kotlinx.
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
                if (peek() != '"') throw NotThisShape
                member()
                ws()
                when (next()) {
                    ',' -> continue
                    '}' -> return
                    else -> throw NotThisShape
                }
            }
        }

        /**
         * Whether the key at [i] is exactly [name], written with no escape. If so it is consumed,
         * with the colon after it, leaving [i] on the value; if not, nothing is consumed.
         */
        private fun key(name: String): Boolean {
            val end = i + 1 + name.length
            if (end >= s.length || s[end] != '"' || !s.regionMatches(i + 1, name, 0, name.length)) return false
            i = end + 1
            ws()
            expect(':')
            ws()
            return true
        }

        private fun skipKeyAndValue() {
            // An escaped key could spell one this reads ("\u0069d" is `id`), which kotlinx would match.
            if (string() !== lastPlain) throw NotThisShape
            ws()
            expect(':')
            ws()
            skipValue()
        }

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
                    i++
                    ws()
                    if (peek() == '}') {
                        i++
                        return
                    }
                    while (true) {
                        ws()
                        skipString()
                        ws()
                        expect(':')
                        ws()
                        skipValue()
                        ws()
                        when (next()) {
                            ',' -> continue
                            '}' -> return
                            else -> throw NotThisShape
                        }
                    }
                }

                '[' -> {
                    i++
                    ws()
                    if (peek() == ']') {
                        i++
                        return
                    }
                    while (true) {
                        ws()
                        skipValue()
                        ws()
                        when (next()) {
                            ',' -> continue
                            ']' -> return
                            else -> throw NotThisShape
                        }
                    }
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
            val start = i
            while (i < s.length && s[i] !in STOPS) i++
            val token = s.substring(start, i)
            // JSON numbers only: no NaN or Infinity, which kotlinx refuses by default.
            if (token.isEmpty() || token.any { it !in NUMBER_CHARS }) throw NotThisShape
            return token.toDoubleOrNull() ?: throw NotThisShape
        }

        /** The last string [string] read with no escape in it, so a caller can tell that one apart. */
        private var lastPlain: String? = null

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
            val slash = nextSlash(start)
            if (slash in 0 until quote) return escaped(start)
            val out = s.substring(start, quote)
            i = quote + 1
            lastPlain = out
            return out
        }

        /** The string that starts at [start] and holds an escape: the runs between escapes copied whole. */
        private fun escaped(start: Int): String {
            val out = StringBuilder(64)
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

        /** The four hex digits of a `\\u` escape, as the UTF-16 unit they spell. ASCII hex only: Character.digit takes other scripts' digits too. */
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

        private fun skipString() {
            expect('"')
            var run = i
            while (true) {
                val quote = s.indexOf('"', run)
                if (quote < 0) throw NotThisShape
                val slash = nextSlash(run)
                if (slash !in 0 until quote) {
                    i = quote + 1
                    return
                }
                // Past the escape and the character it escapes, which may be the quote just found.
                run = slash + 2
                if (run > s.length) throw NotThisShape
            }
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

    /** Where a number or literal ends. */
    private val STOPS = charArrayOf(',', '}', ']', ' ', '\n', '\r', '\t')

    private const val NUMBER_CHARS = "0123456789+-.eE"
}
