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
package com.vitorpamplona.quartz.eventstore.store.mapping

import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.nip01Core.core.Event
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The [toEvent] fast path must be INDISTINGUISHABLE from the canonical
 * `Event.fromJson(toEventJson())` it replaces on the query hot path: for every
 * kind the factory-built event has the SAME runtime subclass AND serializes to
 * the SAME JSON as fromJson would. If a Quartz upgrade ever changed the by-kind
 * dispatch, this fails loudly rather than letting the store hand back a
 * mis-typed event.
 */
class EventDocConversionTest {
    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun doc(
        kind: Int,
        tags: List<List<String>> = listOf(listOf("e", "a".repeat(64)), listOf("p", "b".repeat(64)), listOf("alt", "note")),
        content: String = "hello \"world\"\nwith\ttabs and emoji 🤙 and a \\backslash",
    ) = EventDoc(
        id = id(),
        pubkey = "c".repeat(64),
        createdAt = 1_700_000_000L,
        kind = kind,
        tags = tags,
        content = content,
        sig = "d".repeat(128),
    )

    // A broad spread: the corpus kinds, plus follow list / repost / relay list /
    // gift wrap / zap receipt / addressable app data, plus an UNKNOWN kind to
    // exercise the base-Event fallback. Every one must match fromJson.
    private val kinds = listOf(0, 1, 3, 5, 6, 7, 10002, 30023, 1059, 9735, 30078, 987_654)

    @Test
    fun `every kind matches fromJson type and serialization exactly`() {
        for (kind in kinds) {
            val d = doc(kind)
            val direct = d.toEvent()
            val canonical = Event.fromJson(d.toEventJson())
            assertEquals(canonical::class, direct::class, "kind $kind: subclass diverged from fromJson")
            assertEquals(d.toEventJson(), direct.toJson(), "kind $kind: factory serialization diverged from the canonical form")
            assertEquals(canonical.toJson(), direct.toJson(), "kind $kind: factory JSON diverged from fromJson's")
        }
    }

    @Test
    fun `empty tags and empty content reconstruct correctly`() {
        for (kind in kinds) {
            val d = doc(kind, tags = emptyList(), content = "")
            assertEquals(Event.fromJson(d.toEventJson()).toJson(), d.toEvent().toJson(), "kind $kind empty")
        }
    }
}
