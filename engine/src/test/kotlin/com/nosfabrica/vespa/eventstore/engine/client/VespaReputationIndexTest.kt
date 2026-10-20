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
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * What a retraction or a cell removal may swallow. Both can meet a parent that does not exist,
 * which is the intended no-op, and nothing else: a failure that merely MENTIONS "404" or "not
 * found" is a failure. Driven through the real feed client, so the exception under test is the
 * one it builds, not a message typed here.
 */
class VespaReputationIndexTest {
    private val mock = MockVespaEngine()
    private val reputations = VespaReputationIndex(mock.url)

    @AfterTest
    fun tearDown() {
        reputations.close()
        mock.stop()
    }

    private val subject = "b".repeat(64)
    private val service = ServiceKey("5e".repeat(32))

    private fun retract() = runBlocking { reputations.updateCells(listOf(ReputationCells(subject, service, influence = null, followers = null, dropInfluence = true, dropFollowers = true))) }

    private fun removeCell() = runBlocking { reputations.removeCells(listOf(CellRemoval(subject, service, influence = true, followers = false))) }

    @Test
    fun `a missing parent is a no-op whether Vespa answers 200 or a missing-document 404`() {
        // Vespa 8.763: 200 for an update to a document that does not exist (measured).
        mock.updateReply = 200 to """{"pathId":"/document/v1/reputation/reputation/docid/$subject","id":"id:reputation:reputation::$subject"}"""
        retract()
        removeCell()
        // A Vespa that answered 404 instead would send the same body; it stays a no-op.
        mock.updateReply = 404 to """{"pathId":"/document/v1/reputation/reputation/docid/$subject","id":"id:reputation:reputation::$subject"}"""
        retract()
        removeCell()
        assertEquals(4, mock.updateRequests)
    }

    @Test
    fun `any other failure surfaces, however it is worded`() {
        val failures =
            listOf(
                // A container serving no document API: the 404 that is not about the document.
                404 to """{"error-code":"NOT_FOUND","message":"Document API is not configured in this cluster"}""",
                // Words the old test matched on, on failures that are not a missing document.
                400 to """{"pathId":"/document/v1/reputation/reputation/docid/x","message":"Field 'influence_scores' not found in document type 'reputation'"}""",
                400 to """{"pathId":"/document/v1/reputation/reputation/docid/x","message":"tensor address 404 is out of range"}""",
                404 to "<html><body>404 Not Found</body></html>",
            )
        for ((status, body) in failures) {
            mock.updateReply = status to body
            assertFailsWith<Exception>("a retraction must not swallow $status $body") { retract() }
            assertFailsWith<Exception>("a cell removal must not swallow $status $body") { removeCell() }
        }
    }

    @Test
    fun `the 404 test reads the feed client's own message`() {
        val absent = """{"pathId":"/document/v1/reputation/reputation/docid/x","id":"id:reputation:reputation::x"}"""
        // What the feed client builds, document id first (FeedException's own prefix).
        assertTrue(RuntimeException("(id:reputation:reputation::x) Status 404 executing 'PUT /document/v1/reputation/reputation/docid/x': $absent").isAbsentDocumentFailure())
        assertTrue(RuntimeException("Status 404 executing 'PUT /document/v1/reputation/reputation/docid/x': $absent").isAbsentDocumentFailure(), "and without it")
        assertFalse(RuntimeException("Status 400 executing 'PUT /document/v1/reputation/reputation/docid/x': $absent").isAbsentDocumentFailure(), "a 400 is never absence")
        assertFalse(RuntimeException("Status 404 executing 'PUT /x': Document API is not configured in this cluster").isAbsentDocumentFailure())
        assertFalse(RuntimeException("Connection refused").isAbsentDocumentFailure())
        assertFalse(RuntimeException().isAbsentDocumentFailure())
    }
}
