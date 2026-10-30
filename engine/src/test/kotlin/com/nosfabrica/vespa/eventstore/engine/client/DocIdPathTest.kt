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
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

/**
 * A docid is one path segment of a document-API url, and nothing a signer wrote
 * may climb out of it. Quartz's `ETag.parseId` only checks a value's length, so
 * a kind 5 can name `../../../../../search/?ranking=pwned&…` (64 characters) as
 * its target, and `Deletions` hands every target to [VespaEventIndex.get].
 */
class DocIdPathTest {
    private val mock = MockVespaEngine()
    private val events = VespaEventIndex(mock.url)
    private val reputations = VespaReputationIndex(mock.url)

    @AfterTest
    fun tearDown() {
        events.close()
        reputations.close()
        mock.stop()
    }

    private val climb = "../../../../../search/?ranking=pwned&yql=select+*+from+event&p=x"

    @Test
    fun `an event get never leaves its docid segment`() {
        assertEquals(64, climb.length, "the shape ETag.parseId accepts")
        assertNull(runBlocking { events.get(climb) })
        assertFalse("pwned" in mock.searchRankings, "rankings: ${mock.searchRankings}")
    }

    @Test
    fun `a reputation get never leaves its docid segment`() {
        assertNull(runBlocking { reputations.get(climb) })
        assertFalse("pwned" in mock.searchRankings, "rankings: ${mock.searchRankings}")
    }

    @Test
    fun `a dot segment is not a docid`() {
        assertNull(runBlocking { reputations.get("..") })
        assertNull(runBlocking { reputations.get(".") })
    }

    @Test
    fun `a docid segment is allow-listed, not encoded`() {
        assertEquals("ab".repeat(32), docIdSegment("ab".repeat(32)))
        assertEquals("reputation-keyed-by-service", docIdSegment("reputation-keyed-by-service"))
        assertEquals("30023:${"cd".repeat(32)}:x.y_z", docIdSegment("30023:${"cd".repeat(32)}:x.y_z"))
        listOf("", ".", "..", "a/b", "a?b", "a#b", "a%2Fb", "a b", "a\\b", "caf\u00e9").forEach {
            assertNull(docIdSegment(it), "'$it'")
        }
    }
}
