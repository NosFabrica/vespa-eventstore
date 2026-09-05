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
import com.nosfabrica.vespa.eventstore.engine.client.TrustDescent
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
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

    /** [seed] for the thousands-of-docs shapes: one pipelined feed instead of a put per doc. */
    private fun seedBulk(docs: List<EventDoc>) =
        runBlocking {
            index.putAll(docs)
            docs.forEach { reference.put(it) }
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
        check(EventQuery(search = "vitor", notSearch = listOf("pamplona")))
        check(EventQuery(notSearch = listOf("vitor")))
        check(EventQuery(phrases = listOf("pamplona dev")))
        check(EventQuery(search = "vitor", phrases = listOf("pamplona dev"), notSearch = listOf("nothing")))
    }

    @Test
    fun `quoted phrases require exact adjacency in order`() {
        val pamplona = doc(kind = 0, search = SearchFields(name = "vitor", about = "pamplona dev"))
        val scattered = doc(kind = 0, search = SearchFields(name = "vitor", about = "dev of pamplona"))
        seed(pamplona, scattered)
        runBlocking {
            // Adjacent and in order: "pamplona dev" matches only the first doc.
            assertEquals(listOf(pamplona.id), index.search(EventQuery(phrases = listOf("pamplona dev"))).map { it.id })
            // Order matters — the reversed phrase matches only the reversed text… almost:
            // "dev of pamplona" has "of" between, so neither doc has "dev pamplona".
            assertEquals(emptyList(), index.search(EventQuery(phrases = listOf("dev pamplona"))).map { it.id })
            // A quoted single word is the fuzzy opt-out: exact token or nothing.
            assertEquals(2, index.search(EventQuery(phrases = listOf("vitor"))).size)
            assertEquals(0, index.search(EventQuery(phrases = listOf("vito"))).size, "no prefix reach inside quotes")
        }
    }

    @Test
    fun `an id lookup carrying a text constraint must not take the doc-API fast path`() {
        // getByIds never sees the search fields, so ids + phrases/notSearch
        // must route through the search path or the constraint is dropped.
        val d = doc(kind = 0, search = SearchFields(name = "vitor", about = "pamplona dev"))
        seed(d)
        runBlocking {
            assertEquals(listOf(d.id), index.search(EventQuery(ids = listOf(d.id))).map { it.id })
            assertEquals(0, index.search(EventQuery(ids = listOf(d.id), notSearch = listOf("pamplona"))).size)
            assertEquals(0, index.search(EventQuery(ids = listOf(d.id), phrases = listOf("something else"))).size)
            assertEquals(listOf(d.id), index.search(EventQuery(ids = listOf(d.id), phrases = listOf("pamplona dev"))).map { it.id })
        }
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
            // Folded like the index: an unaccented exclusion reaches the accented
            // name. Pinned as the full surviving SET — a none{} here would also
            // pass on an exclusion that wrongly dropped everything.
            assertEquals(
                setOf(vitor.id, model.id),
                index.search(EventQuery(kinds = listOf(0), notSearch = listOf("jose"))).map { it.id }.toSet(),
            )
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
        // A WEIGHTED key set is a recall constraint (a dotProduct over `pubkey`)
        // that a document-API get cannot see, so an id set carrying one must
        // leave the fast path. These ids all belong to one author; naming a
        // different one must answer EMPTY, which is what the reference does and
        // what the get path — returning the ids and forgetting the constraint —
        // did not.
        check(EventQuery(ids = docs.take(5).map { it.id }, authorWeights = mapOf("b2".repeat(32) to 80)))
        check(EventQuery(ids = docs.take(5).map { it.id }, authorWeights = mapOf("a1".repeat(32) to 80)))
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

    /**
     * The author VALUES AND their doc counts from ONE grouping — what the
     * orphan-score sweep builds its candidate list and its dry-run report from.
     * A truncated or value-less answer would leave orphans behind, so this pins
     * the leaf `group:pubkey:…` parse over the wire.
     */
    @Test
    fun `countByAuthor returns every author with its doc count`() =
        runBlocking {
            val alice = "a1".repeat(32)
            val bob = "b2".repeat(32)
            val carol = "c3".repeat(32)
            seed(
                doc(kind = 30382, pubkey = alice),
                doc(kind = 30382, pubkey = alice),
                doc(kind = 30382, pubkey = alice),
                doc(kind = 30382, pubkey = bob),
                doc(kind = 30382, pubkey = carol),
                doc(kind = 1, pubkey = "d4".repeat(32)), // another kind: not an author of these
            )
            val cards = EventQuery(kinds = listOf(30382))
            assertEquals(mapOf(alice to 3, bob to 1, carol to 1), index.countByAuthor(cards))
            assertEquals(reference.countByAuthor(cards), index.countByAuthor(cards))
        }

    /**
     * A LIMIT IS A PROMISE: "a query with a limit gets exactly that". Two shapes
     * used to break it, and both are pinned here over the wire.
     *
     * 1. The tie-resolution overfetch ([TIE_SLACK]) was added BEFORE the rank
     *    profile was chosen, so a limit within EventYql's match-phase band came
     *    out the other side on the unranked profile — where the client refuses a
     *    match-phase-degraded page instead of reconciling it. The real ceiling
     *    was `band - TIE_SLACK`, and crossing it turned a served page into an
     *    error. Measured against production: 1936 served, 1937 refused.
     * 2. Past the band there was no shape left that could serve the limit at
     *    all; it is paged now, and the caller still gets exactly what it asked.
     */
    @Test
    fun `a limit is served whole, on the profile its own size selects`() =
        runBlocking {
            val band = EventYql.MATCH_PHASE_BAND
            seedBulk(List(band + 40) { doc(kind = 1, at = 5_000L + it) })

            // At the very top of the band the overfetch must not demote it.
            mock.searchRankings.clear()
            assertEquals(band, index.search(EventQuery(kinds = listOf(1), limit = band)).size, "the band's own limit is served whole")
            assertEquals(
                listOf(EventYql.RANK_RECENCY),
                mock.searchRankings.distinct(),
                "a limit inside the band rides the match-phase profile, overfetch and all",
            )

            // One past the band: paged, still exact, still newest-first.
            mock.searchRankings.clear()
            val over = index.search(EventQuery(kinds = listOf(1), limit = band + 20))
            assertEquals(band + 20, over.size, "a limit past the band is paged, never truncated")
            assertEquals(over.map { it.id }, over.sortedWith(EventDoc.NEWEST_FIRST).map { it.id }, "still created_at desc, id asc")
            assertEquals(over.map { it.id }.distinct().size, over.size, "paging must not repeat a document")
            assertTrue(mock.searchRankings.size > 1, "it took more than one round trip: ${mock.searchRankings}")
            assertTrue(
                mock.searchRankings.none { it == EventYql.RANK_TEXT || it == EventYql.RANK_SEARCH },
                "plain recall never lands on a relevance profile: ${mock.searchRankings.distinct()}",
            )

            // A caller's "everything, but bounded" spelling. The overfetch used
            // to make `limit + TIE_SLACK` OVERFLOW to a negative, which
            // EventYql.build reads as its matches-nothing sentinel — so the
            // largest limit expressible returned NOTHING. Paging never overfetches
            // past the band, so the sentinel is unreachable from a positive limit.
            assertEquals(
                band + 40,
                index.search(EventQuery(kinds = listOf(1), limit = Int.MAX_VALUE)).size,
                "an enormous limit returns the corpus, not an empty page",
            )
        }

    /**
     * The corpus shape that makes paging hard, and the one that reaches these
     * limits in practice: a trust provider bulk-publishes its whole score set on
     * ONE `created_at`. A cursor that stepped to `boundary` rather than taking
     * the tie group whole would re-read the same page forever.
     */
    @Test
    fun `paging survives a corpus that shares a single timestamp`() =
        runBlocking {
            val band = EventYql.MATCH_PHASE_BAND
            seedBulk(List(band + 30) { doc(kind = 1, at = 9_000L) })
            val hits = index.search(EventQuery(kinds = listOf(1), limit = band + 25))
            assertEquals(band + 25, hits.size, "one timestamp still pages to the asked-for count")
            assertEquals(hits.map { it.id }.distinct().size, hits.size, "and never repeats a document")
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
     * THE DESCENT, rung by rung, through the mock: a ranked search under an
     * observer asks the first rung, reads its K-th score, asks the rung that
     * score proves, and serves it — the same page the undescended query
     * serves, since every author here is above the floor. A page whose first
     * rung holds too few hits goes straight to the floor rung. Off, nothing
     * carries a rung; an ungated read never does.
     */
    @Test
    fun `a ranked search descends the trust rungs and serves the same page`() =
        runBlocking {
            val top = "f1".repeat(32)
            val mid = "f2".repeat(32)
            val low = "f3".repeat(32)
            mock.authorMaxRank[top] = 95
            mock.authorMaxRank[mid] = 40
            mock.authorMaxRank[low] = 5
            seed(
                doc(kind = 1, pubkey = top, search = SearchFields(text = "alpha one")),
                doc(kind = 1, pubkey = top, search = SearchFields(text = "alpha two")),
                doc(kind = 1, pubkey = mid, search = SearchFields(text = "alpha three")),
                doc(kind = 1, pubkey = low, search = SearchFields(text = "alpha four")),
                doc(kind = 1, pubkey = low, search = SearchFields(text = "alpha five")),
            )
            // Scores that mean what the profile's mean: a body hit times the author's trust curve.
            mock.relevanceOf = { d -> 550.0 * TrustDescent.wotMult((mock.authorMaxRank[d.pubkey] ?: 0).toDouble(), 2.0) }
            val q = EventQuery(kinds = listOf(1), search = "alpha", observer = "ab".repeat(32), minRank = 2.0, limit = 2)

            fun rungs(from: Int) =
                mock.searchRequests.drop(from).map { r ->
                    Regex("author_max_rank >= (\\d+)")
                        .find(r.getValue("yql"))
                        ?.groupValues
                        ?.get(1)
                        ?.toInt()
                }
            try {
                val exact = index.search(q).map { it.id }
                assertEquals(2, exact.size, "the page")
                assertEquals(listOf<Int?>(null), rungs(0), "off: the query carries no rung")

                index.trustDescent = true
                val at = mock.searchRequests.size
                assertEquals(exact, index.search(q).map { it.id }, "the descended page is the page")
                val proven = TrustDescent.provenRung(550.0 * TrustDescent.wotMult(95.0, 2.0), 2.0, 1)
                assertEquals(listOf<Int?>(TrustDescent.FIRST_RUNG, proven), rungs(at), "the first rung, then the rung its page proved")
                assertTrue(proven in 3 until TrustDescent.FIRST_RUNG, "a body hit by a top author proves a rung between the floor and the first, got $proven")

                // Too few hits at the first rung: the floor rung, which is exact by the gate.
                val short = q.copy(limit = 4)
                val whole = index.search(short.copy(observer = null, minRank = null)).map { it.id }
                val at2 = mock.searchRequests.size
                assertEquals(whole, index.search(short).map { it.id }, "the floor rung serves the whole page")
                assertEquals(listOf<Int?>(TrustDescent.FIRST_RUNG, 2), rungs(at2).take(2), "the first rung came up short, so the floor")

                // include:spam: no gate, no rung.
                val at3 = mock.searchRequests.size
                index.search(q.copy(minRank = 0.0))
                assertEquals(listOf<Int?>(null), rungs(at3))
            } finally {
                index.trustDescent = false
                mock.relevanceOf = null
                mock.authorMaxRank.clear()
            }
        }

    /**
     * THE RUNG WITH NOTHING UNDER IT. `max_rank` read 0 on every document on
     * staging (2026-09-04: a schema flip dropped the field and the redeploy
     * brought it back empty), so every rung — the floor rung included, which
     * is exact only while the scalar is true — matched nobody, and every
     * ranked search answered EMPTY. A floor page that comes up short is the
     * one shape that cannot tell "few trusted hits" from "the scalar is
     * wrong", so it is not served: the exact query runs, and the page is the
     * page whatever the field says.
     */
    @Test
    fun `a floor rung that comes up short is not served, the exact page is`() =
        runBlocking {
            val top = "f1".repeat(32)
            val low = "f3".repeat(32)
            // No entry in authorMaxRank: every author reads 0, as on staging.
            seed(
                doc(kind = 1, pubkey = top, search = SearchFields(text = "beta one")),
                doc(kind = 1, pubkey = top, search = SearchFields(text = "beta two")),
                doc(kind = 1, pubkey = low, search = SearchFields(text = "beta three")),
            )
            val q = EventQuery(kinds = listOf(1), search = "beta", observer = "ab".repeat(32), minRank = 2.0, limit = 2)
            try {
                val exact = index.search(q).map { it.id }
                assertEquals(2, exact.size, "the page, off")
                index.trustDescent = true
                val at = mock.searchRequests.size
                assertEquals(exact, index.search(q).map { it.id }, "the page, on, with nothing to descend on")
                val rungs =
                    mock.searchRequests.drop(at).map { r ->
                        Regex("author_max_rank >= (\\d+)")
                            .find(r.getValue("yql"))
                            ?.groupValues
                            ?.get(1)
                            ?.toInt()
                    }
                assertEquals(listOf<Int?>(TrustDescent.FIRST_RUNG, 2, null), rungs, "the first rung, the floor rung (short), then the exact query")
            } finally {
                index.trustDescent = false
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
     * A serving schema that predates the near attribute fields (name_near/…)
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

    /**
     * The WRITE half of the same net, and the one that actually stops a
     * deployment: a schema predating the near columns refuses the whole
     * DOCUMENT (`Field 'name_near' is not defined in document type 'event'`),
     * so before this net existed, upgrading the library against an
     * already-serving cluster made every insert of a searchable event throw —
     * `deployIfAbsent` deliberately does not redeploy over a live application,
     * so the jar and the schema part company and ingest stops dead.
     * Reproduced against a real Vespa; this is the pin.
     *
     * The demoted document lands WITHOUT its prefix/fuzzy columns — fail open,
     * exactly like the read side — which is the state reindexFullTextSearch
     * already exists to repair.
     */
    @Test
    fun `a schema without the near fields still accepts writes, minus those columns`() =
        runBlocking {
            mock.rejectNearFields = true
            val fresh = VespaEventIndex(mock.url)
            try {
                val searchable = doc(kind = 0, search = SearchFields(name = "odell"))
                fresh.put(searchable) // would throw before the write-side net
                assertEquals(searchable.id, fresh.get(searchable.id)?.id, "the document landed")
                // …and the batch path, which is what a mirror actually uses.
                val batch = (1..3).map { doc(kind = 0, search = SearchFields(name = "pamplona$it")) }
                fresh.putAll(batch)
                assertEquals(batch.map { it.id }.toSet(), batch.mapNotNull { fresh.get(it.id)?.id }.toSet(), "the batch landed")
                // That these landed AT ALL is the proof the retry dropped the
                // columns: the mock refuses any document still carrying one.
                // Not asserted from the read side — the mock rebuilds a
                // summary by re-deriving indexFields() from the stored
                // SearchFields, so it always reports the columns back whatever
                // was fed. What is stored on a real engine is covered by the
                // migration walk in docs/attribute-memory.md.
            } finally {
                mock.rejectNearFields = false
                fresh.close()
            }
        }

    /**
     * The snapshot walk: every match, across CURSOR pages.
     *
     * visitIds no longer rides the document-API visit — it pages the attribute
     * index on a created_at cursor — so this asserts the property that
     * actually matters (the union across pages is the exact match set) rather
     * than the slicing of a mechanism it no longer uses. The visit's own slice
     * and resume guarantees are still covered by the tags/authors walks.
     */
    @Test
    fun `visitIds streams every match across cursor pages`() =
        runBlocking {
            val bob = "b2".repeat(32)
            seed(*(1..100).map { doc(kind = 30382, pubkey = bob) }.toTypedArray())
            seed(doc(kind = 1, pubkey = bob), doc(kind = 30382)) // outside the selection
            val paged = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val pages = ArrayList<List<DocRef>>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob))) {
                    pages += it
                    true
                }
                assertEquals(true, pages.size > 1, "expected a multi-page walk, got ${pages.size} page(s)")
                val expected = reference.search(EventQuery(kinds = listOf(30382), authors = listOf(bob))).map { DocRef(it.id, it.createdAt) }
                assertEquals(expected.sortedBy { it.id }, pages.flatten().sortedBy { it.id })
                assertEquals(expected.size, pages.flatten().distinctBy { it.id }.size, "pages must not overlap")
            } finally {
                paged.close()
            }
        }

    /**
     * A LIMITED plain walk is the newest N ids, paged on the cursor with a
     * budget — never handed to search(), which fetched a full summary per id
     * to answer an id question (a relay stamps `limit: 100000` on every COUNT
     * filter, and a two-filter COUNT over 51k events took 40 s that way).
     */
    @Test
    fun `visitIds with a limit pages the cursor for the newest N and stops`() =
        runBlocking {
            val bob = "b3".repeat(32)
            seed(*(1..100).map { doc(kind = 30382, pubkey = bob) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val q = EventQuery(kinds = listOf(30382), authors = listOf(bob), limit = 25)
                val got = ArrayList<DocRef>()
                var pages = 0
                paged.visitIds(q) {
                    got += it
                    pages++
                    true
                }
                val expected = reference.search(q).map { DocRef(it.id, it.createdAt) }
                assertEquals(25, got.size, "exactly the limit")
                assertEquals(expected.sortedBy { it.id }, got.sortedBy { it.id }, "the newest 25, as search() would page them")
                assertEquals(true, pages <= 3, "a 25-id budget on 10-id pages stops after three pages, not the whole corpus: $pages")
            } finally {
                paged.close()
            }
        }

    /**
     * A tie group WIDER than the page is the case that silently loses data.
     *
     * Every doc shares one created_at, so a `created_at <= T` cursor re-reads
     * the same page forever and a `< T` cursor drops the rest of the group.
     * Both were observed against a live corpus, where one second held 41,329
     * events by one author — the naive drain returned a clean-looking 54.78%
     * of the set with no error. The walk must return the group EXACTLY once,
     * whole, and then terminate.
     */
    @Test
    fun `visitIds returns a tie group wider than the page exactly once`() =
        runBlocking {
            val bob = "b3".repeat(32)
            seed(*(1..200).map { doc(kind = 30382, pubkey = bob, at = 5_000L) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 100)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob))) {
                    got += it
                    true
                }
                assertEquals(200, got.size, "the whole tie group, and no duplicates")
                assertEquals(0, mock.visitRequests, "a tie group inside the density bound stays on the cursor")
                assertEquals(200, got.distinctBy { it.id }.size)
            } finally {
                paged.close()
            }
        }

    /** A tie group straddling the page boundary: partly in the page, the rest behind it. */
    @Test
    fun `visitIds completes a tie group that straddles the page boundary`() =
        runBlocking {
            val bob = "b4".repeat(32)
            // 30 newer singletons, then 40 sharing one second — the boundary at
            // page size 10 lands inside neither cleanly on every page.
            seed(*(1..30).map { doc(kind = 30382, pubkey = bob, at = (9_000 + it).toLong()) }.toTypedArray())
            seed(*(1..25).map { doc(kind = 30382, pubkey = bob, at = 8_000L) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob))) {
                    got += it
                    true
                }
                assertEquals(55, got.distinctBy { it.id }.size, "every doc exactly once across the boundary")
                assertEquals(25, got.count { it.createdAt == 8_000L }, "the whole tied second")
            } finally {
                paged.close()
            }
        }

    /**
     * A tie-DENSE walk falls back to the scan, and is still complete.
     *
     * 90 docs share one second against a page of 10, so every boundary would
     * cost an unbounded window query — the shape the cursor loses on (measured
     * unkeyed 30382: scan 4,046 ids/s against cursor 480). The routing must
     * notice and step aside, and stepping aside must not cost a single doc.
     */
    @Test
    fun `a tie-dense walk falls back to the scan and stays complete`() =
        runBlocking {
            val bob = "b7".repeat(32)
            seed(*(1..500).map { doc(kind = 30382, pubkey = bob, at = 3_000L) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 100)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob))) {
                    got += it
                    true
                }
                assertEquals(500, got.distinctBy { it.id }.size, "the scan fallback must lose nothing")
                assertTrue(mock.visitRequests > 0, "a tie-dense walk must actually fall back to the scan")
            } finally {
                paged.close()
            }
        }

    /** `since` bounds the walk: the cursor must stop at it, not run to the epoch. */
    @Test
    fun `visitIds honours since`() =
        runBlocking {
            val bob = "b5".repeat(32)
            seed(*(1..40).map { doc(kind = 30382, pubkey = bob, at = (7_000 + it).toLong()) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob), since = 7_021)) {
                    got += it
                    true
                }
                assertEquals(20, got.distinctBy { it.id }.size)
                assertEquals(true, got.all { it.createdAt >= 7_021 })
            } finally {
                paged.close()
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

    /**
     * storedNearFields is decode metadata with a strict provenance rule: the
     * document-API reads (get, the `[document]` visit) see the complete stored
     * doc and stamp exactly the near arrays it holds, while search summaries
     * never carry the near fields (no `| summary` in the schema) and must
     * claim NO evidence — stamping there would mark every hit as predating
     * the near tier and turn the full-text reindex into a corpus rewrite.
     */
    @Test
    fun `document-api reads stamp stored near state and search claims no evidence`() =
        runBlocking {
            val d = doc(kind = 0, search = SearchFields(name = "ODELL", about = "freedom"))
            index.put(d)
            val expected = d.search.nearFieldsWritten()

            assertEquals(expected, index.get(d.id)?.storedNearFields)
            val visited = index.visitDocsPage(EventQuery(kinds = listOf(0)), resumeFrom = null, maxDocs = 10).docs
            assertEquals(listOf(expected), visited.map { it.storedNearFields })

            assertEquals(listOf<Map<String, List<String>>?>(null), index.search(EventQuery(kinds = listOf(0))).map { it.storedNearFields })
        }

    /** onPage returning false stops the cursor walk early instead of paging the whole match set. */
    @Test
    fun `visitIds stops when the page callback declines to continue`() =
        runBlocking {
            val bob = "b6".repeat(32)
            // Keyed on an author so this drives the CURSOR path — an unkeyed
            // query routes to the scan, whose early stop is a different loop.
            seed(*(1..100).map { doc(kind = 30382, pubkey = bob) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob))) {
                    got += it
                    false // first page is enough — a capped snapshot stopping early
                }
                assertEquals(true, got.isNotEmpty() && got.size < 100, "expected a partial walk, got ${got.size} of 100")
            } finally {
                paged.close()
            }
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

    /**
     * The tags projection keeps FULL fidelity — multi-character names,
     * every position, several same-name tags per doc — which is exactly what
     * `tag_index` (single-letter, first-value) loses; a distinct-tag-value
     * caller reads markers and non-first positions off these.
     */
    @Test
    fun `visitTags streams every match's exact tags across the sliced walk`() =
        runBlocking {
            // 30 docs so the walk must cross page/bucket boundaries (page cap 7).
            seed(
                *(1..30)
                    .map {
                        doc(
                            kind = 10002,
                            tags =
                                listOf(
                                    listOf("r", "wss://relay$it.example/", if (it % 2 == 0) "write" else "read"),
                                    listOf("30382:rank", "provider$it", "wss://prov$it.example/"),
                                ),
                        )
                    }.toTypedArray(),
            )
            seed(doc(kind = 1, tags = listOf(listOf("r", "wss://other.example/")))) // outside the selection
            val got = ArrayList<List<List<String>>>()
            index.visitTags(EventQuery(kinds = listOf(10002))) {
                got += it
                true
            }
            val expected = reference.search(EventQuery(kinds = listOf(10002))).map { it.tags }
            assertEquals(expected.map { it.toString() }.sorted(), got.map { it.toString() }.sorted())
        }

    /** A selection-inexpressible query keeps the same tags through the search fallback. */
    @Test
    fun `visitTags falls back to search for tag queries`() =
        runBlocking {
            seed(
                doc(kind = 10002, tags = listOf(listOf("d", "x"), listOf("r", "wss://a.example/", "write"))),
                doc(kind = 10002, tags = listOf(listOf("d", "y"), listOf("r", "wss://b.example/"))),
            )
            val q = EventQuery(kinds = listOf(10002), tags = mapOf("d" to listOf("x")))
            val got = ArrayList<List<List<String>>>()
            index.visitTags(q) {
                got += it
                true
            }
            assertEquals(reference.search(q).map { it.tags }, got)
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
