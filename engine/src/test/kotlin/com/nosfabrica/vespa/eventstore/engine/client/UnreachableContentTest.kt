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

import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A container whose content node is down still answers: HTTP 200, nothing
 * found, a coverage block calling itself full at 100% over zero nodes, and an
 * `errors` entry naming the backend. Read as an answer, that is "the corpus is
 * empty" — a NIP-45 count of 0, a relay-discovery read with no relays, a dedup
 * probe that finds nothing. Every funnel refuses it instead, as the partial
 * answer it is, and serves again once the node is back.
 */
class UnreachableContentTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)

    @AfterTest
    fun tearDown() {
        index.close()
        mock.stop()
    }

    private val doc =
        EventDoc(
            id = "1".repeat(64),
            pubkey = "a1".repeat(32),
            createdAt = 1_700_000_000L,
            kind = 30166,
            tags = listOf(listOf("d", "wss://relay.example/")),
            content = "",
            sig = "b2".repeat(32),
        )

    /** The failure each read must end in: refused as partial, never an empty answer. */
    private fun assertRefused(
        what: String,
        read: suspend () -> Any?,
    ) {
        val outcome = runCatching { runBlocking { read() } }
        val failure = outcome.exceptionOrNull()
        assertTrue(failure is PartialAnswer, "$what answered ${outcome.getOrNull()} with no content node reachable, instead of refusing: $failure")
        assertTrue("Backend communication error" in failure.message.orEmpty(), "$what's refusal names Vespa's own error: ${failure.message}")
    }

    @Test
    fun `a NIP-45 count refuses rather than answering zero`() {
        runBlocking { index.put(doc) }
        mock.contentUnreachable = true
        assertRefused("count") { index.count(EventQuery(kinds = listOf(30166))) }
    }

    @Test
    fun `a distinct tag read refuses rather than answering no values`() {
        runBlocking { index.put(doc) }
        mock.contentUnreachable = true
        assertRefused("distinctTagIndexValues") { index.distinctTagIndexValues(EventQuery(kinds = listOf(30166)), "d") }
    }

    @Test
    fun `an author grouping refuses rather than answering no authors`() {
        runBlocking { index.put(doc) }
        mock.contentUnreachable = true
        assertRefused("countByAuthor") { index.countByAuthor(EventQuery(kinds = listOf(30166))) }
    }

    @Test
    fun `a recall refuses rather than answering no events`() {
        runBlocking { index.put(doc) }
        mock.contentUnreachable = true
        assertRefused("search") { index.search(EventQuery(kinds = listOf(30166))) }
        assertRefused("rawSearch") { index.rawSearch(EventQuery(kinds = listOf(30166))) }
    }

    @Test
    fun `every read serves again once the node is back`() =
        runBlocking {
            index.put(doc)
            mock.contentUnreachable = true
            assertRefused("count") { index.count(EventQuery(kinds = listOf(30166))) }
            mock.contentUnreachable = false

            assertEquals(1, index.count(EventQuery(kinds = listOf(30166))))
            assertEquals(1, index.search(EventQuery(kinds = listOf(30166))).size)
        }
}
