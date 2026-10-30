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
package com.nosfabrica.vespa.eventstore.mapping

import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.addressTagOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

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

    /**
     * The ingest guards key an EVENT by quartz's `addressTagOrNull()`, the
     * engine's supersession keys its stored DOC by [EventDoc.addressOrNull]:
     * the two must name the same coordinate, or a version the guard admitted
     * would supersede under another key. Covers the shapes where a rule could
     * differ: a replaceable carrying a stray `d` (the fixed empty d wins), an
     * addressable with no `d` (empty), with an empty first `d` before a valued
     * one (the first `d` counts), and a regular kind (no address).
     */
    @Test
    fun `an event and its stored doc name the same address`() {
        val shapes =
            listOf(
                doc(0, tags = listOf(listOf("d", "stray"))),
                doc(10002, tags = emptyList()),
                doc(30023, tags = listOf(listOf("d", "slug"))),
                doc(30023, tags = emptyList()),
                doc(30023, tags = listOf(listOf("d", ""), listOf("d", "second"))),
                doc(30382, tags = listOf(listOf("d", "e".repeat(64)))),
            )
        for (d in shapes) assertEquals(d.addressOrNull(), d.toEvent().addressTagOrNull(), "kind ${d.kind} tags ${d.tags}")
        assertEquals("0:${"c".repeat(64)}:", doc(0, tags = listOf(listOf("d", "stray"))).toEvent().addressTagOrNull())
        assertNull(doc(1).toEvent().addressTagOrNull())
        assertNull(doc(1).addressOrNull())
    }
}
