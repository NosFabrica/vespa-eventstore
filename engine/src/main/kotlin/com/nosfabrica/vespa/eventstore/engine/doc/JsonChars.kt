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

/** Escapes for the chars below [ESCAPES]'s size that need one; null means "as is". */
private val ESCAPES: Array<String?> =
    arrayOfNulls<String>('\\'.code + 1).apply {
        for (c in 0 until 0x20) this[c] = "\\u" + c.toString(16).padStart(4, '0')
        this['"'.code] = "\\\""
        this['\\'.code] = "\\\\"
        this['\t'.code] = "\\t"
        this['\b'.code] = "\\b"
        this['\n'.code] = "\\n"
        this['\r'.code] = "\\r"
        this[0x0c] = "\\f"
    }

/** [c]'s JSON escape, or null when it stands as itself: the one table every writer here uses. */
internal fun jsonEscapeOf(c: Char): String? = if (c.code < ESCAPES.size) ESCAPES[c.code] else null

/**
 * Each char's [ESCAPES] form, escaped again: what a char of a JSON string
 * becomes once that JSON text is itself a string value. `"` is `\"` inside
 * the tags array and `\\\"` once the array is the `tags` field.
 */
private val TWICE: Array<String?> =
    arrayOfNulls<String>(ESCAPES.size).apply {
        for (c in indices) {
            val once = ESCAPES[c] ?: continue
            this[c] = JsonChars(once.length + 2).string(once).toString().let { it.substring(1, it.length - 1) }
        }
    }

/**
 * JSON text written into one char array, for the feed body and the stored `tags` string.
 *
 * Strings are escaped EXACTLY as kotlinx.serialization's `JsonPrimitive.toString()`
 * escapes them: `"`, `\` and the C0 controls, nothing else (`/`, DEL, U+2028 and every
 * non-ASCII char go through as they are). Byte-identity is the contract, not a nicety —
 * the `tags` string this writes is stored and later spliced verbatim into served events
 * (`toRawEvent`), and `JsonCharsTest` checks every UTF-16 unit against kotlinx so a corpus
 * fed before and after this writer stores the same bytes.
 *
 * WHY A CHAR ARRAY, not a `StringBuilder`: a value is copied in with one bulk
 * `getChars` (an intrinsic, widened from Latin-1 in vector strides) and then scanned IN
 * the array for a char that needs escaping, which almost no value has: a pubkey, an id, a
 * relay URL. Only a value that has one is rewritten char by char. The builder paid
 * `charAt` per char to scan, and its `append(CharSequence, start, end)` checked capacity
 * and coder on every run. Measured on 20,000 of relay.ditto.pub's newest kind 3s (avg
 * 9.6 KB, a 19 KB body), one core to itself: 122-131 µs per feed body through the
 * builder, 37-49 here, the same bytes. See benchmark/README.md, "The feed body, written in one pass".
 *
 * WHY IT EXISTS AT ALL: building a `JsonObject` tree and calling `toString()` turns every
 * tag value into its own `JsonPrimitive`, stringifies each one through a fresh builder,
 * then escapes the whole `tags` array a SECOND time as a string field, and a kind 3
 * carries hundreds of tags (320-360 µs per body).
 */
internal class JsonChars(
    capacity: Int,
) {
    private var buf = CharArray(capacity)
    private var size = 0

    private fun ensure(extra: Int) {
        if (size + extra > buf.size) buf = buf.copyOf(maxOf(buf.size * 2, size + extra))
    }

    /** [c] as it is: structure, never a value. */
    fun raw(c: Char): JsonChars {
        ensure(1)
        buf[size++] = c
        return this
    }

    /** [s] as it is: structure or a number, never a value. */
    fun raw(s: String): JsonChars {
        ensure(s.length)
        s.toCharArray(buf, size, 0, s.length)
        size += s.length
        return this
    }

    fun raw(n: Long): JsonChars = raw(n.toString())

    /** [value] as a quoted JSON string. */
    fun string(value: String): JsonChars = raw('"').escaped(value, ESCAPES).raw('"')

    /** `"name":`, the key half of an object member, preceded by a comma unless it is the [first]. */
    fun key(
        name: String,
        first: Boolean = false,
    ): JsonChars {
        if (!first) raw(',')
        return string(name).raw(':')
    }

    /** `["a","b"]`: a JSON array of strings. */
    fun strings(values: List<String>): JsonChars {
        raw('[')
        for (i in values.indices) {
            if (i > 0) raw(',')
            string(values[i])
        }
        return raw(']')
    }

    /** The JSON text of a tag array, `[["p","…"],…]`. */
    fun tags(tags: List<List<String>>): JsonChars {
        raw('[')
        for (i in tags.indices) {
            if (i > 0) raw(',')
            strings(tags[i])
        }
        return raw(']')
    }

    /**
     * [tags] as a JSON string value whose content is the JSON text of the tag array:
     * `"[[\"p\",\"…\"]]"`, the same text `string(tagsJson)` writes, in one pass over the
     * values instead of two.
     */
    fun tagsString(tags: List<List<String>>): JsonChars {
        raw("\"[")
        for (t in tags.indices) {
            if (t > 0) raw(',')
            raw('[')
            val tag = tags[t]
            for (v in tag.indices) {
                if (v > 0) raw(',')
                raw("\\\"").escaped(tag[v], TWICE).raw("\\\"")
            }
            raw(']')
        }
        return raw("]\"")
    }

    /**
     * `"[prefix]:[value]"` without building the joined string: escaping is per char, so
     * the escape of a concatenation is the concatenation of the escapes.
     */
    fun pair(
        prefix: String,
        value: String,
    ): JsonChars =
        raw('"')
            .escaped(prefix, ESCAPES)
            .raw(':')
            .escaped(value, ESCAPES)
            .raw('"')

    /** [value] with each char that needs one replaced by its [table] escape. */
    private fun escaped(
        value: String,
        table: Array<String?>,
    ): JsonChars {
        val length = value.length
        ensure(length)
        val start = size
        value.toCharArray(buf, start, 0, length)
        val chars = buf
        val end = start + length
        var i = start
        // Exactly the chars [ESCAPES] (and so [TWICE]) has an entry for.
        while (i < end) {
            val c = chars[i].code
            if (c < 0x20 || c == '"'.code || c == '\\'.code) break
            i++
        }
        size = i
        for (k in i - start until length) {
            val c = value[k]
            val escape = if (c.code < table.size) table[c.code] else null
            if (escape != null) raw(escape) else raw(c)
        }
        return this
    }

    override fun toString(): String = String(buf, 0, size)
}
