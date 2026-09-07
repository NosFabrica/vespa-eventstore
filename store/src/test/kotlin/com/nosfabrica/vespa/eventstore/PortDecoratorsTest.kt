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
package com.nosfabrica.vespa.eventstore

import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * EVERY DECORATOR OVERRIDES EVERY PORT MEMBER — the rule that has no compiler
 * behind it.
 *
 * `EventIndex` gives several members a default body, and each default is a
 * CORRECT-BUT-SLOW answer for an engine that cannot do better: `countByAuthor`
 * counts a materialized search, `distinctTagIndexValues` says "no such
 * aggregate here" by returning null. That is right for the in-memory
 * reference and wrong for a decorator, because a decorator wrapping the real
 * client does not merely fail to decorate an inherited default — it ANSWERS
 * with it, and the engine's fast path is never reached. Nothing warns: the
 * class still compiles, every test that uses a bare index still passes, and
 * the only symptom is that production got slower.
 *
 * That is exactly how `distinctTagIndexValues` shipped dead in the stack
 * `VespaEventStore.open()` assembles — the port grew a member, both decorators
 * kept compiling, and the ~1s grouping quietly stayed a ~157s walk.
 *
 * Read off the SOURCE rather than by reflection: an override that merely calls
 * `super` would satisfy reflection and reintroduce the bug.
 */
class PortDecoratorsTest {
    /**
     * The decorators of [com.nosfabrica.vespa.eventstore.engine.EventIndex] —
     * classes that wrap another index and must pass every member through. A new
     * one belongs here; the cost of forgetting is silent.
     */
    private val decorators =
        listOf(
            "store/src/main/kotlin/com/nosfabrica/vespa/eventstore/trust/TrustProjection.kt",
            "engine/src/main/kotlin/com/nosfabrica/vespa/eventstore/engine/metrics/MeteredEventIndex.kt",
            // Not published, and it still counts: this one's whole job is
            // reporting what the store asks of the engine, so a member it
            // inherits is a measurement of the PORT's default round trips
            // reported as the client's.
            "benchmark/src/main/kotlin/com/nosfabrica/vespa/eventstore/benchmark/harness/CountingEventIndex.kt",
        )

    private val port = "engine/src/main/kotlin/com/nosfabrica/vespa/eventstore/engine/EventIndex.kt"

    @Test
    fun `every decorator overrides every member of the index port`() {
        val members = Regex("""^\s{4}(?:suspend )?fun (\w+)""", RegexOption.MULTILINE).findAll(read(port)).map { it.groupValues[1] }.toSet()
        assertTrue(members.size > 10, "the port parse found only $members — the regex, not the port, is what changed")

        for (path in decorators) {
            val overridden = Regex("""override (?:suspend )?fun (\w+)""").findAll(read(path)).map { it.groupValues[1] }.toSet()
            val missing = (members - overridden).sorted()
            assertTrue(
                missing.isEmpty(),
                "${path.substringAfterLast('/')} inherits ${missing.size} port member(s) instead of forwarding them: $missing",
            )
        }
    }

    /** Tests run with the module directory as the working dir; the sources sit one level up from `:store`. */
    private fun read(path: String): String =
        File(path).takeIf { it.isFile }?.readText()
            ?: File("..", path).takeIf { it.isFile }?.readText()
            ?: error("cannot find $path from ${File(".").absolutePath}")
}
