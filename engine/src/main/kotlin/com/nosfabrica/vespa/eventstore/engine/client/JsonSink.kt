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

import com.nosfabrica.vespa.eventstore.engine.doc.jsonEscapeOf
import okio.BufferedSink

/**
 * [value] as a quoted JSON string, written into the sink as it goes: the request bodies that
 * carry a long yql or a batch never exist as one array. Escapes as [com.nosfabrica.vespa.eventstore.engine.doc.JsonChars] does.
 */
internal fun BufferedSink.writeJsonString(value: String): BufferedSink = writeByte('"'.code).writeJsonEscaped(value).writeByte('"'.code)

/** [value]'s JSON-escaped characters without the quotes, in runs between the chars that need one. */
internal fun BufferedSink.writeJsonEscaped(value: String): BufferedSink {
    var run = 0
    for (i in value.indices) {
        val escape = jsonEscapeOf(value[i]) ?: continue
        if (i > run) writeUtf8(value, run, i)
        writeUtf8(escape)
        run = i + 1
    }
    if (run < value.length) writeUtf8(value, run, value.length)
    return this
}
