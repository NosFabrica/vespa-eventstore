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
package com.nosfabrica.vespa.eventstore.engine

import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A query with no `limit` asks for the whole match set, and the store's contract is
 * that it gets it — however long that takes. The engine, left alone, does not
 * honour that: Vespa DEGRADES instead of failing. A query it gives up on comes back
 * HTTP 200 with however many hits it had at that moment and `coverage.full: false`,
 * which at the call site is indistinguishable from a filter that genuinely matched
 * that few.
 *
 * The deployed query profile makes that vanishingly rare (no 500 ms deadline, soft
 * timeout off), but "rare" is not a contract — node coverage and match-phase can
 * degrade a query too. So every search response is checked, and a partial one is
 * refused. On a read path a silent partial under-delivers; on a write path it is
 * worse, because dedup and the NIP-09/62 guards decide by "did the query find it"
 * and a partial answer resurrects a deleted event.
 */
class VespaCoverageTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)

    @AfterTest
    fun tearDown() {
        index.close()
        mock.stop()
    }

    private fun doc(id: String) =
        EventDoc(
            id = id,
            pubkey = "a1".repeat(32),
            createdAt = 1_700_000_000L,
            kind = 1,
            tags = emptyList(),
            content = "hello",
            sig = "b2".repeat(32),
        )

    @Test
    fun `a complete response is returned normally`() =
        runBlocking {
            index.put(doc("1".repeat(64)))

            val hits = index.search(EventQuery(limit = 10))
            assertEquals(1, hits.size, "full coverage must not be mistaken for degradation")
        }

    @Test
    fun `a soft-timeout partial is refused, not returned as a short result`() =
        runBlocking {
            index.put(doc("1".repeat(64)))
            mock.degradeCoverage = "timeout"

            val failure = runCatching { index.search(EventQuery()) }.exceptionOrNull()

            if (failure == null) fail("a partially-searched corpus was returned as if it were the whole answer")
            assertTrue(
                failure.message?.contains("PARTIAL") == true,
                "the failure must name the cause, got: ${failure.message}",
            )
        }

    @Test
    fun `the raw recall path refuses a partial too`() =
        runBlocking {
            index.put(doc("1".repeat(64)))
            mock.degradeCoverage = "match-phase"

            assertTrue(
                runCatching { index.rawSearch(EventQuery()) }.isFailure,
                "rawSearch bypasses the EventDoc model, not the coverage check",
            )
        }

    /** Counts degrade the same way, and a short count is a wrong number, not a small one. */
    @Test
    fun `the count path refuses a partial too`() =
        runBlocking {
            index.put(doc("1".repeat(64)))
            mock.degradeCoverage = "timeout"

            assertTrue(
                runCatching { index.count(EventQuery()) }.isFailure,
                "an aggregation over part of the corpus is not the aggregation that was asked for",
            )
        }

    /**
     * The ONE accepted degradation: a limit'd unranked query rides the
     * `recency` match-phase profile, which ASKS the engine to cut the match
     * phase — that cut arriving as match-phase-degraded coverage is the
     * optimization working, not a partial answer.
     */
    @Test
    fun `match-phase degradation is accepted for the limit'd recency shape`() =
        runBlocking {
            // The client overfetches limit + TIE_SLACK (65 here), so "full
            // degraded page" means the OVERFETCHED page: seed past it, with
            // unique timestamps so no boundary tie demotes the acceptance.
            repeat(70) { i ->
                index.put(doc(i.toString(16).padStart(64, '0')).copy(createdAt = 1_700_000_000L + i))
            }
            mock.degradeCoverage = "match-phase"

            // A FULL degraded page is provably the exact top-limit and is
            // served as-is (a SHORT one is rerun exact — separate test).
            val hits = index.search(EventQuery(limit = 1))
            assertEquals(1, hits.size, "the opted-in match-phase cut must be served, not refused")
            assertEquals(1_700_000_069L, hits.single().createdAt, "and it must be the newest doc")
        }

    /**
     * The shape that names NO reason, and the one that took the relay's feeds
     * down: `full: false` at a rounded 100% with no `degraded` block.
     *
     * `full` is `docs == active`, an exact equality; the percentage rounds
     * `docs / targetActive`; and Vespa emits `degraded` only when its own
     * `isDegraded()` holds, which at a rounded 100% and no flag set it does
     * not. A node a hair short of its target hits all three at once — and this
     * guard, keyed on `full` alone, refused it with "vespa searched only 100%
     * of the corpus (degraded: unspecified)". Every read on that relay, for as
     * long as the node stayed short. Nothing there is actionable, so it is
     * served.
     *
     * Both funnels, because they differ on [allowMatchPhase] and the bug was in
     * neither branch of that: the limit'd shape is the empty-search feed
     * (`recency`, match-phase allowed) and the unlimited one is plain recall.
     */
    @Test
    fun `a not-full response the engine itself calls undegraded is served`() =
        runBlocking {
            index.put(doc("1".repeat(64)))
            mock.roundedCompleteCoverage = true

            assertEquals(1, index.search(EventQuery(limit = 10)).size, "the empty-search feed shape must be served")
            assertEquals(1, index.search(EventQuery()).size, "and so must plain unlimited recall")
        }

    /** The grouping funnel checks coverage separately (queryRoot), so it needs its own proof. */
    @Test
    fun `the count path serves an undegraded not-full response too`() =
        runBlocking {
            index.put(doc("1".repeat(64)))
            mock.roundedCompleteCoverage = true

            assertEquals(1, index.count(EventQuery()), "a count must not refuse what the recall path serves")
        }

    /**
     * The mirror shape, and the one keying on `full` used to serve at ANY
     * percentage: `docs == active`, so the engine says `full: true`, but both
     * below `targetActive` — documents the ideal state expects that are not
     * active anywhere yet, which Vespa names `non-ideal-state`. Nothing can
     * return those documents, so this is a partial answer whatever `full` says,
     * and the guard that refuses partial answers has to refuse it.
     *
     * Both funnels: the limit'd feed shape (match-phase allowed, and this flag
     * is not match-phase) and the count path, which checks coverage separately.
     */
    @Test
    fun `a full response the engine names non-ideal-state on is refused`() =
        runBlocking {
            index.put(doc("1".repeat(64)))
            mock.nonIdealStateCoverage = true

            val failure = runCatching { index.search(EventQuery(limit = 10)) }.exceptionOrNull()
            if (failure == null) fail("a response the engine calls degraded was served because it also called itself full")
            assertTrue(
                failure.message?.contains("full: true") == true,
                "the message must name the contradiction it refused on, got: ${failure.message}",
            )
            assertTrue(
                runCatching { index.count(EventQuery()) }.isFailure,
                "a count over a corpus missing documents is not the count that was asked for",
            )
        }

    /**
     * The relaxation is for the shape that names NO reason. A node that is both
     * a hair short AND degraded for a stated reason is still a partial answer —
     * the rounded-100 carve-out must not swallow the flag underneath it.
     */
    @Test
    fun `a named reason is refused even on the rounded-complete shape`() =
        runBlocking {
            index.put(doc("1".repeat(64)))
            mock.roundedCompleteCoverage = true
            mock.degradeCoverage = "timeout"

            assertTrue(
                runCatching { index.search(EventQuery()) }.isFailure,
                "a stated degradation must outrank the not-full-but-undegraded carve-out",
            )
        }

    /** The carve-out is match-phase ONLY — a timeout on the same limit'd shape is still a partial answer. */
    @Test
    fun `a timeout partial is refused even on the limit'd recency shape`() =
        runBlocking {
            index.put(doc("1".repeat(64)))
            mock.degradeCoverage = "timeout"

            assertTrue(
                runCatching { index.search(EventQuery(limit = 10)) }.isFailure,
                "timeout degradation must not slip through the match-phase carve-out",
            )
        }
}
