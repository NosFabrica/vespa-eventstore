/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.vespa
import com.vitorpamplona.quartz.eventstore.vespa.client.DocRef
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.SearchFields
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [VespaEventIndex] against [MockVespaEngine]: every operation goes over real
 * HTTP (h2c feed writes, HTTP/1.1 reads), the mock parses the YQL back into an
 * [EventQuery], and results must agree with a directly-driven
 * [InMemoryEventIndex] — builder, wire format, and matching semantics all
 * checked in one loop.
 */
class VespaEventIndexTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)
    private val reference = InMemoryEventIndex()

    @AfterTest
    fun tearDown() {
        index.close()
        mock.stop()
    }

    private var seq = 0

    @Test
    fun `the feed client starts whatever number of endpoints the cluster names`() {
        // The client sizes ONE Jetty pool at max(min(cores,64),8) + connections *
        // endpoints, and Jetty refuses to start it unless that exceeds what the
        // HTTP client leases. A connection budget applied per endpoint instead of
        // per cluster therefore fails at some endpoint count — on a 12-core host,
        // at two, with "Insufficient configured threads: required=76 < max=76".
        // Naming a cluster's containers must not be what breaks the writer.
        listOf(1, 2, 3, 5, 8, 16).forEach { n ->
            VespaEventIndex(endpoints = List(n) { mock.url }).close()
        }
    }

    @Test
    fun `multi-endpoint client round-robins reads and feeds every endpoint`() =
        runBlocking {
            // Two endpoint entries pointing at the same engine: every request must
            // land somewhere valid whichever slot the round-robin picks, and the
            // feed client must accept the multi-URI form.
            val multi = VespaEventIndex(endpoints = listOf(mock.url, mock.url))
            try {
                val d = doc(kind = 1, content = "multi endpoint")
                multi.put(d)
                assertEquals(d.id, multi.get(d.id)?.id)
                assertEquals(d.id, multi.get(d.id)?.id, "second get takes the other endpoint slot")
                assertEquals(listOf(d.id), multi.search(EventQuery(ids = listOf(d.id))).map { it.id })
                multi.remove(d.id)
                assertNull(multi.get(d.id))
            } finally {
                multi.close()
            }
        }

    private fun doc(
        kind: Int = 1,
        pubkey: String = "a1".repeat(32),
        at: Long = (1000 + seq).toLong(),
        tags: List<List<String>> = emptyList(),
        content: String = "",
        owner: String = pubkey,
        search: SearchFields = SearchFields.NONE,
    ) = EventDoc(
        id = (++seq).toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = at,
        kind = kind,
        tags = tags,
        content = content,
        sig = "e".repeat(128),
        owner = owner,
        search = search,
    )

    private fun seed(vararg docs: EventDoc) =
        runBlocking {
            for (d in docs) {
                index.put(d)
                reference.put(d)
            }
        }

    /** The wire answer must equal the in-memory spec's answer, in order. */
    private fun check(query: EventQuery) =
        runBlocking {
            assertEquals(reference.search(query).map { it.id }, index.search(query).map { it.id }, "query: $query")
        }

    @Test
    fun `put get remove round-trip over the wire`() =
        runBlocking {
            val d =
                doc(
                    kind = 30382,
                    tags = listOf(listOf("d", "b2".repeat(32)), listOf("e", "f".repeat(64), "wss://relay.example.com", "root")),
                    content = "line\n\"quoted\" 🫥",
                    search = SearchFields(name = "findable", primary = "also findable"),
                )
            index.put(d)
            assertEquals(d, index.get(d.id))
            index.remove(d.id)
            assertNull(index.get(d.id))
            assertNull(index.get("0".repeat(64)))
        }

    @Test
    fun `search agrees with the in-memory spec across the filter surface`() {
        val bob = "b2".repeat(32)
        seed(
            doc(kind = 0, search = SearchFields(name = "vitor", about = "pamplona dev")),
            doc(kind = 1, tags = listOf(listOf("p", bob)), content = "hi bob"),
            doc(kind = 1, pubkey = bob, at = 5000),
            doc(kind = 30382, pubkey = bob, tags = listOf(listOf("d", "x"), listOf("t", "nostr"), listOf("t", "search"))),
            doc(kind = 1, owner = bob, tags = listOf(listOf("expiration", "2000"))),
            // Escaping round-trip: the tag value must survive YQL quoting + parsing.
            doc(kind = 1, tags = listOf(listOf("t", "quo\"te\\and\nnewline"))),
        )
        check(EventQuery())
        check(EventQuery(kinds = listOf(1)))
        check(EventQuery(authors = listOf(bob)))
        check(EventQuery(owners = listOf(bob)))
        check(EventQuery(tags = mapOf("p" to listOf(bob))))
        check(EventQuery(tags = mapOf("t" to listOf("nostr", "missing"))))
        check(EventQuery(tagsAll = mapOf("t" to listOf("nostr", "search"))))
        check(EventQuery(tags = mapOf("t" to listOf("quo\"te\\and\nnewline"))))
        check(EventQuery(since = 1002, until = 1005))
        check(EventQuery(kinds = listOf(1), limit = 2))
        check(EventQuery(notExpiredAt = 3000))
        check(EventQuery(expiresBefore = 3000))
        check(EventQuery(search = "vitor"))
        check(EventQuery(kinds = listOf(0, 1), tags = mapOf("p" to listOf(bob)), until = 9000))
        check(EventQuery(notKinds = listOf(0, 30382)))
        check(EventQuery(notKinds = listOf(0, 30382), authors = listOf(bob)))
        check(EventQuery(search = "vitor", notSearch = listOf("pamplona")))
        check(EventQuery(notSearch = listOf("vitor")))
    }

    @Test
    fun `notSearch drops exact word hits and nothing looser`() {
        val vitor = doc(kind = 0, search = SearchFields(name = "vitor", about = "pamplona dev"))
        val model = doc(kind = 0, search = SearchFields(name = "vitor", about = "model builder"))
        val jose = doc(kind = 0, search = SearchFields(name = "José"))
        val unsearchable = doc(kind = 1, content = "pamplona pamplona", search = SearchFields.NONE)
        seed(vitor, model, jose, unsearchable)
        runBlocking {
            // The exclusion drops the exact word, wherever it sits on the doc.
            assertEquals(listOf(model.id), index.search(EventQuery(search = "vitor", notSearch = listOf("pamplona"))).map { it.id })
            // Exact-only: "-ode" is not a token of "model builder", so the
            // substring reach the POSITIVE side has must not exclude here.
            assertEquals(listOf(model.id), index.search(EventQuery(search = "model", notSearch = listOf("ode"))).map { it.id })
            // Folded like the index: an unaccented exclusion reaches the accented name.
            assertTrue(index.search(EventQuery(kinds = listOf(0), notSearch = listOf("jose"))).none { it.id == jose.id })
            // Exclusion-only = plain recall minus the word, and docs invisible
            // to search (no search fields) contain no word — never excluded.
            assertEquals(
                listOf(unsearchable.id, jose.id, model.id).sorted(),
                index.search(EventQuery(notSearch = listOf("pamplona"))).map { it.id }.sorted(),
            )
        }
    }

    /**
     * Pure-id recall takes the document-API direct-get fast path (up to one get
     * wave); a larger id set falls back to a single `id in (…)` search. Both must
     * agree with the in-memory spec — same set, same newest-first order, same
     * NIP-40 expiry filtering, same limit.
     */
    @Test
    fun `pure-id lookups match the spec on the get path and the search fallback`() {
        val docs = (1..40).map { doc(kind = 1, at = (1000 + it).toLong()) }
        // An expiring note to exercise the get path's NIP-40 guard.
        val expiring = doc(kind = 1, tags = listOf(listOf("expiration", "2000")))
        seed(*docs.toTypedArray(), expiring)

        check(EventQuery(ids = listOf(docs[0].id))) // single id — get path
        check(EventQuery(ids = docs.take(5).map { it.id })) // a handful — get path
        check(EventQuery(ids = docs.take(10).map { it.id })) // newest-first across a set
        check(EventQuery(ids = docs.map { it.id })) // 40 ids > one wave — search fallback
        check(EventQuery(ids = listOf("f".repeat(64)))) // absent id — empty
        check(EventQuery(ids = listOf(docs[3].id, "f".repeat(64), docs[7].id))) // present + absent
        check(EventQuery(ids = listOf(expiring.id), notExpiredAt = 1000)) // not yet expired — kept
        check(EventQuery(ids = listOf(expiring.id), notExpiredAt = 3000)) // expired — dropped
        check(EventQuery(ids = docs.take(10).map { it.id }, limit = 3)) // limit after ordering
    }

    /**
     * The bulk-dedup existence check: `select id` under the attribute-only
     * `dedup` summary class, resolved from id-only hits. Membership must agree
     * exactly with the in-memory spec's default (which rides search) — present
     * and absent ids, case normalization, invalid entries — because a wrong
     * member here is a wrong write upstream, not a small answer.
     */
    @Test
    fun `existingIds answers exact membership through the dedup summary class`() =
        runBlocking {
            val docs = (1..40).map { doc(kind = 1, at = (1000 + it).toLong()) }
            seed(*docs.toTypedArray())
            val mixed = docs.take(10).map { it.id } + "f".repeat(64)
            assertEquals(reference.existingIds(mixed), index.existingIds(mixed))
            assertEquals(docs.take(10).map { it.id }.toSet(), index.existingIds(mixed))
            // Uppercase hex must match the lowercase-held id, exactly like search.
            assertEquals(setOf(docs[0].id), index.existingIds(listOf(docs[0].id.uppercase())))
            // No valid 64-hex left = unsatisfiable; answered locally, never on the wire.
            assertEquals(emptySet(), index.existingIds(listOf("nope", "")))
            // The empty list must be empty on the SPEC too: EventQuery treats
            // empty ids as "no constraint", so an unguarded default would answer
            // membership-of-nothing with the whole corpus — the wrong direction.
            assertEquals(emptySet(), index.existingIds(emptyList()))
            assertEquals(emptySet(), reference.existingIds(emptyList()))
        }

    /**
     * A serving schema that predates the `dedup` document-summary answers 400
     * naming the class — the client must demote the existence check to the
     * full-summary search, serve the exact answer, and remember, never fail
     * the ingest batch riding on it.
     */
    @Test
    fun `a schema without the dedup summary demotes existence checks to full summaries`() =
        runBlocking {
            val docs = (1..8).map { doc(kind = 1) }
            seed(*docs.toTypedArray())
            mock.rejectDedupSummary = true
            val fresh = VespaEventIndex(mock.url)
            try {
                val ids = docs.map { it.id } + "f".repeat(64)
                val expected = reference.existingIds(ids)
                assertEquals(expected, fresh.existingIds(ids), "first check (flips the flag)")
                assertEquals(expected, fresh.existingIds(ids), "second check (already demoted)")
            } finally {
                mock.rejectDedupSummary = false
                fresh.close()
            }
        }

    /** `notKinds` excludes the plumbing kinds; the count is the full content match set. */
    @Test
    fun `count honors notKinds exclusion`() =
        runBlocking {
            seed(
                doc(kind = 0),
                doc(kind = 30382),
                doc(kind = 1),
                doc(kind = 1),
                doc(kind = 30023),
            )
            assertEquals(3, index.count(EventQuery(notKinds = listOf(0, 30382))))
        }

    /** Distinct-author grouping: the number of unique pubkeys among the matches, over the wire, agreeing with the spec. */
    @Test
    fun `countDistinctAuthors counts unique pubkeys`() =
        runBlocking {
            val alice = "a1".repeat(32)
            val bob = "b2".repeat(32)
            val carol = "c3".repeat(32)
            seed(
                doc(kind = 1, pubkey = alice),
                doc(kind = 1, pubkey = alice),
                doc(kind = 1, pubkey = bob),
                doc(kind = 30023, pubkey = carol),
                doc(kind = 0, pubkey = carol), // plumbing: excluded, so carol only counts via her 30023
                doc(kind = 30382, pubkey = "d4".repeat(32)), // plumbing-only author: excluded
            )
            val content = EventQuery(notKinds = listOf(0, 30382))
            assertEquals(3, index.countDistinctAuthors(content))
            assertEquals(reference.countDistinctAuthors(content), index.countDistinctAuthors(content))
        }

    /** The per-kind histogram: one entry per kind with its doc count, over the wire, agreeing with the spec. */
    @Test
    fun `countByKind histograms the corpus by kind`() =
        runBlocking {
            seed(
                doc(kind = 1),
                doc(kind = 1),
                doc(kind = 1),
                doc(kind = 0),
                doc(kind = 30023),
                doc(kind = 30023),
            )
            val all = EventQuery()
            assertEquals(mapOf(1 to 3, 0 to 1, 30023 to 2), index.countByKind(all))
            assertEquals(reference.countByKind(all), index.countByKind(all))
            // Honors the same filters as the other queries.
            assertEquals(mapOf(1 to 3, 30023 to 2), index.countByKind(EventQuery(notKinds = listOf(0))))
        }

    /**
     * `limit` is the ONLY thing that bounds a result: absent, every match comes
     * back. The client picks no page size on the caller's behalf.
     */
    @Test
    fun `a query without a limit returns every match`() =
        runBlocking {
            seed(*(1..5).map { doc(kind = 9) }.toTypedArray())
            val q = EventQuery(kinds = listOf(9))

            assertEquals(5, index.search(q).size, "no limit: every match")
            assertEquals(5, index.rawSearch(q).size, "the raw path agrees")
            assertEquals(4, index.search(q.copy(limit = 4)).size, "an explicit limit is honored")
        }

    @Test
    fun `count returns the full match set past the hits page`() =
        runBlocking {
            seed(*(1..7).map { doc(kind = 7) }.toTypedArray())
            assertEquals(7, index.count(EventQuery(kinds = listOf(7))))
            // A limit'd search returns the page, the count stays total.
            assertEquals(3, index.search(EventQuery(kinds = listOf(7), limit = 3)).size)
        }

    @Test
    fun `match-nothing queries never reach the wire`() =
        runBlocking {
            seed(doc())
            assertEquals(emptyList(), index.search(EventQuery(authors = listOf("not-hex"))))
            assertEquals(0, index.count(EventQuery(limit = 0)))
        }

    /**
     * A trust-ranked match-all (`sort:rank` — ranking set, no search terms) is
     * ordered by SCORE, not recency: the planner must never window it, or every
     * hit older than the probe window silently disappears from "who does my
     * observer rank highest".
     */
    @Test
    fun `recency planner never windows a ranked query`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            // Dense enough that the one-hour probe rung would "prove" a window.
            seed(*(1..30).map { doc(kind = 1, at = now - it) }.toTypedArray())
            val ancient = doc(kind = 1, at = 1000) // the hit a window would drop
            seed(ancient)
            val planned = VespaEventIndex(mock.url)
            val unplanned = VespaEventIndex(mock.url, queryPlanning = false)
            try {
                val q = EventQuery(kinds = listOf(1), ranking = EventYql.RANK_DESC, limit = 40)
                assertEquals(
                    unplanned.search(q).map { it.id }.toSet(),
                    planned.search(q).map { it.id }.toSet(),
                    "a ranked query must not be recency-windowed",
                )
                assertTrue(planned.search(q).any { it.id == ancient.id }, "ranked recall must include hits older than the probe window")
            } finally {
                planned.close()
                unplanned.close()
            }
        }

    /**
     * The recency planner must be INVISIBLE in results: a dense live-shaped
     * corpus (events within the last hour) takes the windowed path, a sparse or
     * ancient corpus falls through to the unbounded query — both must return
     * exactly what a planner-off index returns, top-of-corpus order included.
     * Run against a schema WITHOUT the match-phase profile, because that is
     * when the planner owns small limits (with the profile serving, it stands
     * down for them — see the fallback test for that division).
     */
    @Test
    fun `recency planner returns exactly the unplanned results`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            // Dense: 30 docs inside the planner's first (one-hour) rung.
            seed(*(1..30).map { doc(kind = 1, at = now - it) }.toTypedArray())
            // Ancient: outside every rung — only the fall-through can see it.
            seed(doc(kind = 7, at = 1000))
            mock.rejectRecencyProfile = true
            val planned = VespaEventIndex(mock.url)
            val unplanned = VespaEventIndex(mock.url, queryPlanning = false)
            try {
                // First query flips each client's profile-missing flag (400 ->
                // demote); from then on the planner owns the small limits.
                planned.search(EventQuery(kinds = listOf(1), limit = 1))
                unplanned.search(EventQuery(kinds = listOf(1), limit = 1))
                for (q in listOf(
                    EventQuery(kinds = listOf(1), limit = 10), // dense -> windowed
                    EventQuery(kinds = listOf(1), limit = 30), // exactly the window's population
                    EventQuery(kinds = listOf(7), limit = 5), // sparse+ancient -> fall-through, still found
                    EventQuery(limit = 500), // limit past the corpus -> everything
                )) {
                    assertEquals(unplanned.search(q).map { it.id }, planned.search(q).map { it.id }, "planned vs unplanned: $q")
                }
                // The dense query must actually return the newest docs, not a hole.
                assertEquals(now - 1, planned.search(EventQuery(kinds = listOf(1), limit = 1)).single().createdAt)
            } finally {
                mock.rejectRecencyProfile = false
                planned.close()
                unplanned.close()
            }
        }

    /**
     * Vespa documents that a match-phase-limited query can return FEWER hits
     * than requested on an unevenly distributed corpus, with no automatic
     * re-run. A short match-phase page silently served would under-deliver a
     * REQ — the client must rerun it unranked and serve the exact answer.
     */
    @Test
    fun `match-phase under-delivery is rerun exact, not served short`() =
        runBlocking {
            seed(*(1..8).map { doc(kind = 1) }.toTypedArray())
            mock.matchPhaseUnderdeliver = 2 // recency answers 2 hits, degraded
            try {
                val hits = index.search(EventQuery(kinds = listOf(1), limit = 6))
                assertEquals(
                    reference.search(EventQuery(kinds = listOf(1), limit = 6)).map { it.id },
                    hits.map { it.id },
                    "a short degraded page must be rerun exact, not served",
                )
            } finally {
                mock.matchPhaseUnderdeliver = 0
            }
        }

    /**
     * max-hits is PER CONTENT NODE: on a multi-node cluster each node cuts at
     * its own threshold, so even a FULL degraded page can silently omit
     * mid-page docs. Full-page acceptance is single-node only — a multi-node
     * degraded page must be rerun exact regardless of fill.
     */
    @Test
    fun `a full match-phase page from a multi-node cluster is rerun exact`() =
        runBlocking {
            seed(*(1..8).map { doc(kind = 1) }.toTypedArray())
            mock.matchPhaseUnderdeliver = 6 // page LOOKS full (== limit)...
            mock.matchPhaseNodes = 2 // ...but two nodes cut independently
            try {
                val hits = index.search(EventQuery(kinds = listOf(1), limit = 6))
                assertEquals(
                    reference.search(EventQuery(kinds = listOf(1), limit = 6)).map { it.id },
                    hits.map { it.id },
                    "a multi-node degraded page proves nothing — must be rerun exact",
                )
            } finally {
                mock.matchPhaseUnderdeliver = 0
                mock.matchPhaseNodes = 1
            }
        }

    /**
     * The observer gate's hot path ("kind 1, limit 50, authenticated") rides
     * the recency_gated match-phase profile, and its under-delivery must be
     * rerun on recency_gated_exact — never served short. The mock's matcher
     * doesn't emulate the trust gate (that is Vespa's job), so what this pins
     * is the plumbing: the fast profile is sent, the degraded short page
     * triggers exactly one exact rerun, and the served page is complete.
     */
    @Test
    fun `gated match-phase under-delivery is rerun on the exact gated profile`() =
        runBlocking {
            seed(*(1..8).map { doc(kind = 1) }.toTypedArray())
            mock.matchPhaseUnderdeliver = 2 // recency_gated answers 2 hits, degraded
            try {
                val q = EventQuery(kinds = listOf(1), limit = 6, ranking = EventYql.RANK_RECENCY_GATED, minRank = 2.0, observer = "ab".repeat(32))
                assertEquals(
                    reference.search(EventQuery(kinds = listOf(1), limit = 6)).map { it.id },
                    index.search(q).map { it.id },
                    "a short degraded gated page must be rerun on the exact gated profile, not served",
                )
            } finally {
                mock.matchPhaseUnderdeliver = 0
            }
        }

    /**
     * A serving schema that predates the observer gate answers 400 to both
     * gated profiles — the client must demote the query to plain ranking-free
     * recall (FAIL-OPEN, the pre-gate behavior, recency profile and planner
     * included), serve the REQ, and remember, never fail the caller.
     */
    @Test
    fun `a missing gated profile demotes to plain recall instead of failing`() =
        runBlocking {
            seed(*(1..5).map { doc(kind = 1) }.toTypedArray())
            mock.rejectGatedProfile = true
            val fresh = VespaEventIndex(mock.url)
            try {
                val q = EventQuery(kinds = listOf(1), limit = 3, ranking = EventYql.RANK_RECENCY_GATED, minRank = 2.0, observer = "ab".repeat(32))
                val expected = reference.search(EventQuery(kinds = listOf(1), limit = 3)).map { it.id }
                assertEquals(expected, fresh.search(q).map { it.id }, "first query (flips the flag)")
                assertEquals(expected, fresh.search(q).map { it.id }, "second query (already demoted)")
            } finally {
                mock.rejectGatedProfile = false
                fresh.close()
            }
        }

    /**
     * The engine sorts by created_at ONLY (the id tiebreak cost UCA collation
     * over the whole match set); the client restores exact
     * `created_at desc, id asc` order — INCLUDING boundary-tie membership
     * under a limit — from the overfetched page. The mock's scrambled tie
     * order is the adversarial engine: reversed ids within each timestamp, cut
     * arbitrarily at the limit.
     */
    @Test
    fun `boundary ties under a limit resolve to exact id-asc membership`() =
        runBlocking {
            // 10 docs on one timestamp (the boundary group), newer singles above.
            seed(*(1..10).map { doc(kind = 1, at = 7_000) }.toTypedArray())
            seed(*(1..3).map { doc(kind = 1, at = 8_000L + it) }.toTypedArray())
            mock.scrambleTieOrder = true
            try {
                // limit=7 cuts INSIDE the tie group: 3 newer + 4 of the 10 tied.
                val q = EventQuery(kinds = listOf(1), limit = 7)
                assertEquals(
                    reference.search(q).map { it.id },
                    index.search(q).map { it.id },
                    "the 4 tied survivors must be the LOWEST ids, in asc order",
                )
            } finally {
                mock.scrambleTieOrder = false
            }
        }

    /** A boundary tie group WIDER than the overfetch slack takes the [t,t] follow-up query and is still exact. */
    @Test
    fun `a tie group wider than the slack resolves through the tie-window query`() =
        runBlocking {
            val wide = 84 // TIE_SLACK (64) + 20 — must exceed the overfetch slack
            seed(*(1..wide).map { doc(kind = 1, at = 7_000) }.toTypedArray())
            seed(doc(kind = 1, at = 9_000))
            mock.scrambleTieOrder = true
            try {
                val q = EventQuery(kinds = listOf(1), limit = 5)
                assertEquals(
                    reference.search(q).map { it.id },
                    index.search(q).map { it.id },
                    "beyond-slack ties must be fetched via the [t,t] window and tiebroken exactly",
                )
            } finally {
                mock.scrambleTieOrder = false
            }
        }

    /** Deep-past until anchors (old-history pagination) skip the recency profile — the planner windows them instead. */
    @Test
    fun `deep-past until skips the recency profile`() {
        val recent = System.currentTimeMillis() / 1000 - 60
        val ancient = System.currentTimeMillis() / 1000 - 90 * 86_400L
        assertEquals(true, EventYql.usesRecencyProfile(EventQuery(kinds = listOf(1), limit = 10)))
        assertEquals(true, EventYql.usesRecencyProfile(EventQuery(kinds = listOf(1), limit = 10, until = recent)))
        assertEquals(false, EventYql.usesRecencyProfile(EventQuery(kinds = listOf(1), limit = 10, until = ancient)))
    }

    /**
     * A serving schema that predates the `recency` profile answers 400 to it —
     * the client must demote that query to unranked, serve the REQ, and
     * remember (no second 400), never fail the caller.
     */
    @Test
    fun `a missing recency profile demotes to unranked instead of failing`() =
        runBlocking {
            seed(*(1..5).map { doc(kind = 1) }.toTypedArray())
            mock.rejectRecencyProfile = true
            val fresh = VespaEventIndex(mock.url)
            try {
                val expected = reference.search(EventQuery(kinds = listOf(1), limit = 3)).map { it.id }
                assertEquals(expected, fresh.search(EventQuery(kinds = listOf(1), limit = 3)).map { it.id }, "first query (flips the flag)")
                assertEquals(expected, fresh.search(EventQuery(kinds = listOf(1), limit = 3)).map { it.id }, "second query (already demoted)")
            } finally {
                mock.rejectRecencyProfile = false
                fresh.close()
            }
        }

    /**
     * A serving schema that predates the near attribute fields (name_parts/…)
     * answers 400 to ANY search query naming them — the client must demote to
     * exact + gram matching, serve the REQ, and remember, never fail the
     * caller. Recall through the mock is the in-memory reference's either way;
     * what this pins is the retry-and-remember, not the narrower matching.
     */
    @Test
    fun `a schema without the near fields demotes to exact matching instead of failing`() =
        runBlocking {
            seed(doc(kind = 0, search = SearchFields(name = "odell")))
            mock.rejectNearFields = true
            val fresh = VespaEventIndex(mock.url)
            try {
                val expected = reference.search(EventQuery(search = "odell")).map { it.id }
                assertEquals(expected, fresh.search(EventQuery(search = "odell")).map { it.id }, "first query (flips the flag)")
                assertEquals(expected, fresh.search(EventQuery(search = "odell")).map { it.id }, "second query (already demoted)")
                assertEquals(1, fresh.count(EventQuery(search = "odell")), "the grouping paths ride the same net")
            } finally {
                mock.rejectNearFields = false
                fresh.close()
            }
        }

    /** The visit walk: the complete match set, across slices AND continuation pages. */
    @Test
    fun `visitIds streams every match through sliced continuation walks`() =
        runBlocking {
            val bob = "b2".repeat(32)
            // 100 docs across 8 slices (pinned — the default derives from host
            // cores): by pigeonhole some slice holds more than one mock bucket
            // (streamed) or page (paged), so the walk MUST cross continuation
            // boundaries — and the union across slices must still be complete.
            seed(*(1..100).map { doc(kind = 30382, pubkey = bob) }.toTypedArray())
            seed(doc(kind = 1, pubkey = bob), doc(kind = 30382)) // outside the selection
            val sliced = VespaEventIndex(mock.url, visitSlices = 8)
            try {
                val pages = ArrayList<List<DocRef>>()
                sliced.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob))) {
                    pages += it
                    true
                }
                assertEquals(true, pages.size > 1, "expected a multi-page walk, got ${pages.size} page(s)")
                val expected = reference.search(EventQuery(kinds = listOf(30382), authors = listOf(bob))).map { DocRef(it.id, it.createdAt) }
                assertEquals(expected.sortedBy { it.id }, pages.flatten().sortedBy { it.id })
            } finally {
                sliced.close()
            }
        }

    /**
     * A broken streamed visit must resume from the last per-bucket continuation
     * token and still deliver EXACTLY the match set: the cut bucket re-streams
     * in full on resume, so its pre-cut (uncertified) docs must not have been
     * delivered — a duplicate here would corrupt a negentropy snapshot.
     */
    @Test
    fun `streamed visit resumes a broken stream exactly once`() =
        runBlocking {
            seed(*(1..23).map { doc(kind = 30382) }.toTypedArray())
            // One slice so the one-shot cut deterministically hits the walk.
            val single = VespaEventIndex(mock.url, visitSlices = 1)
            try {
                mock.cutStreamedVisitAfterDocs = 12 // mid-bucket (buckets of 5), after two certified tokens
                val got = ArrayList<DocRef>()
                single.visitIds(EventQuery(kinds = listOf(30382))) {
                    got += it
                    true
                }
                val expected = reference.search(EventQuery(kinds = listOf(30382))).map { DocRef(it.id, it.createdAt) }
                assertEquals(expected.sortedBy { it.id }, got.sortedBy { it.id }, "resume must lose nothing and deliver nothing twice")
            } finally {
                single.close()
            }
        }

    /** A server that answers a streamed visit in plain JSON (an older Vespa) still gets the complete set via the paged fallback. */
    @Test
    fun `streamed visit falls back to paging when the server ignores streaming`() =
        runBlocking {
            seed(*(1..30).map { doc(kind = 30382) }.toTypedArray())
            mock.ignoreStreamedVisits = true
            try {
                val got = ArrayList<DocRef>()
                index.visitIds(EventQuery(kinds = listOf(30382))) {
                    got += it
                    true
                }
                val expected = reference.search(EventQuery(kinds = listOf(30382))).map { DocRef(it.id, it.createdAt) }
                assertEquals(expected.sortedBy { it.id }, got.sortedBy { it.id })
            } finally {
                mock.ignoreStreamedVisits = false
            }
        }

    /** VESPA_VISIT_STREAM=0 (visitStreaming=false) walks the paged path directly and is still complete. */
    @Test
    fun `paged visit configuration streams every match too`() =
        runBlocking {
            seed(*(1..30).map { doc(kind = 30382) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, visitStreaming = false)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382))) {
                    got += it
                    true
                }
                val expected = reference.search(EventQuery(kinds = listOf(30382))).map { DocRef(it.id, it.createdAt) }
                assertEquals(expected.sortedBy { it.id }, got.sortedBy { it.id })
            } finally {
                paged.close()
            }
        }

    /** onPage returning false stops the sliced walk early instead of scanning the whole corpus. */
    @Test
    fun `visitIds stops when the page callback declines to continue`() =
        runBlocking {
            seed(*(1..100).map { doc(kind = 30382) }.toTypedArray())
            val got = ArrayList<DocRef>()
            index.visitIds(EventQuery(kinds = listOf(30382))) {
                got += it
                false // first page is enough — a capped snapshot stopping early
            }
            // Exactly the one page the callback accepted; the cancelled slices
            // must not deliver more after the stop.
            assertEquals(true, got.isNotEmpty() && got.size < 100, "expected a partial walk, got ${got.size} of 100")
        }

    /** The projection's rebuild walk: d tags stream out with the ids. */
    @Test
    fun `visitIds projects d tags when asked`() =
        runBlocking {
            seed(*(1..15).map { doc(kind = 30382, tags = listOf(listOf("d", "s$it".repeat(1)))) }.toTypedArray())
            val got = ArrayList<DocRef>()
            index.visitIds(EventQuery(kinds = listOf(30382)), withDTag = true) {
                got += it
                true
            }
            assertEquals((1..15).map { "s$it" }.toSet(), got.mapNotNull { it.dTag }.toSet())
        }

    /** A selection-inexpressible query (tags) still walks correctly via the search fallback. */
    @Test
    fun `visitIds falls back to search for tag queries`() =
        runBlocking {
            seed(
                doc(kind = 30382, tags = listOf(listOf("d", "x"))),
                doc(kind = 30382, tags = listOf(listOf("d", "y"))),
            )
            val got = ArrayList<DocRef>()
            index.visitIds(EventQuery(kinds = listOf(30382), tags = mapOf("d" to listOf("x")))) {
                got += it
                true
            }
            val expected = reference.search(EventQuery(kinds = listOf(30382), tags = mapOf("d" to listOf("x")))).map { DocRef(it.id, it.createdAt) }
            assertEquals(expected, got)
        }
}
