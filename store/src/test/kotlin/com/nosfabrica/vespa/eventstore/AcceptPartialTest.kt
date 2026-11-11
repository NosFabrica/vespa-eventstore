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

import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * One of the cluster's content nodes is offline, and every engine response
 * says so: below 100%, `non-ideal-state`. A client's REQ or NIP-50 search
 * installed [AcceptPartial] and is served from the documents the cluster
 * could still search; every other read refuses as it always has, because the
 * store cannot know whether its caller decides something from the answer.
 */
class AcceptPartialTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)
    private val store = NostrSemanticsStore(index)

    @AfterTest
    fun tearDown() {
        index.close()
        mock.stop()
    }

    private val note =
        EventDoc(
            id = "1".repeat(64),
            pubkey = "a".repeat(64),
            createdAt = 1_700_000_000,
            kind = 1,
            tags = emptyList(),
            content = "hello partial world",
            sig = "e".repeat(128),
            search = SearchFields(text = "hello partial world"),
        )

    @Test
    fun `a client read under AcceptPartial is served while a node is missing`() =
        runBlocking {
            mock.inner.put(note)
            mock.nonIdealStateCoverage = true

            withContext(AcceptPartial) {
                assertEquals(listOf(note.id), store.query<Event>(Filter(kinds = listOf(1), limit = 10)).map { it.id }, "a feed REQ")
                assertEquals(listOf(note.id), store.query<Event>(Filter(kinds = listOf(1), search = "hello")).map { it.id }, "a NIP-50 search")
                val raw = ArrayList<String>()
                store.rawQuery(listOf(Filter(kinds = listOf(1), limit = 10))) { raw += it.id }
                assertEquals(listOf(note.id), raw, "the raw path a relay serves REQs from")
            }
        }

    @Test
    fun `without AcceptPartial the same read refuses`() =
        runBlocking {
            mock.inner.put(note)
            mock.nonIdealStateCoverage = true

            val refused = runCatching { store.query<Event>(Filter(kinds = listOf(1), limit = 10)) }.exceptionOrNull()
            assertTrue(refused?.message.orEmpty().contains("PARTIAL"), "a consumer's own read must not act on part of the corpus: $refused")
        }

    /** A NIP-45 count over part of the corpus is a wrong number, not a short one: the marker does not reach it. */
    @Test
    fun `a count refuses even under AcceptPartial`() =
        runBlocking {
            mock.inner.put(note)
            mock.nonIdealStateCoverage = true

            assertTrue(
                withContext(AcceptPartial) { runCatching { store.count(Filter(kinds = listOf(1))) } }.isFailure,
                "the count must refuse",
            )
        }
}
