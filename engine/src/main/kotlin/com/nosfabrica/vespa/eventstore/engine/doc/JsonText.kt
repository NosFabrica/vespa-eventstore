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

/**
 * [value] as a quoted JSON string, written straight into the builder and escaped EXACTLY as
 * kotlinx.serialization's `JsonPrimitive.toString()` escapes it: `"`, `\` and
 * the C0 controls, nothing else (`/`, DEL, U+2028 and every non-ASCII char go
 * through as they are). Byte-identity is the contract, not a nicety — the
 * `tags` string this writes is stored and later spliced verbatim into served
 * events (`toRawEvent`), and `JsonTextTest` checks every UTF-16 unit against
 * kotlinx so a corpus fed before and after this writer stores the same bytes.
 *
 * WHY IT EXISTS: building a `JsonObject` tree and calling `toString()` turns
 * every tag value into its own `JsonPrimitive`, stringifies each one through a
 * fresh builder, then escapes the whole `tags` array a SECOND time as a string
 * field, and a kind 3 carries hundreds of tags. Measured on relay.ditto.pub's
 * kind 3s (2026-10-16): 340 µs per feed body through the tree, 190 µs through
 * [EventDoc.feedJson], about a ninth of what a relay spends per published
 * event. See benchmark/README.md, "The feed body, written in one pass".
 */
internal fun StringBuilder.appendJsonString(value: String): StringBuilder {
    append('"')
    var run = 0
    for (i in value.indices) {
        val c = value[i].code
        if (c < ESCAPES.size) {
            val escape = ESCAPES[c] ?: continue
            append(value, run, i)
            append(escape)
            run = i + 1
        }
    }
    append(value, run, value.length)
    return append('"')
}

/**
 * Each char's [ESCAPES] form, escaped again: what a char of a JSON string
 * becomes once that JSON text is itself a string value. `"` is `\"` inside
 * the tags array and `\\\"` once the array is the `tags` field.
 */
private val TWICE: Array<String?> =
    arrayOfNulls<String>(ESCAPES.size).apply {
        for (c in indices) {
            val once = ESCAPES[c] ?: continue
            this[c] = StringBuilder().appendJsonString(once).let { it.substring(1, it.length - 1) }
        }
    }

/**
 * [tags] as a JSON string value whose content is the JSON text of the tag
 * array: `"[[\"p\",\"…\"]]"`, the same text `appendJsonString(tagsJson)`
 * writes, in one pass over the values instead of two.
 */
internal fun StringBuilder.appendJsonTagsString(tags: List<List<String>>): StringBuilder {
    append("\"[")
    for (t in tags.indices) {
        if (t > 0) append(',')
        append('[')
        val tag = tags[t]
        for (v in tag.indices) {
            if (v > 0) append(',')
            append("\\\"")
            val value = tag[v]
            var run = 0
            for (i in value.indices) {
                val c = value[i].code
                if (c < TWICE.size) {
                    val escape = TWICE[c] ?: continue
                    append(value, run, i)
                    append(escape)
                    run = i + 1
                }
            }
            append(value, run, value.length)
            append("\\\"")
        }
        append(']')
    }
    return append("]\"")
}

/** `[ "a","b" ]` without the spaces: a JSON array of strings. */
internal fun StringBuilder.appendJsonStrings(values: List<String>): StringBuilder {
    append('[')
    for (i in values.indices) {
        if (i > 0) append(',')
        appendJsonString(values[i])
    }
    return append(']')
}

/** `"name":` — the key half of an object member, preceded by a comma unless it is the [first]. */
internal fun StringBuilder.appendKey(
    name: String,
    first: Boolean = false,
): StringBuilder {
    if (!first) append(',')
    return appendJsonString(name).append(':')
}
