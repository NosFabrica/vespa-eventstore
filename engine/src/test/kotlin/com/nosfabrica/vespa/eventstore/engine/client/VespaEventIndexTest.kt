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
import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.MockVespaEngine
import com.nosfabrica.vespa.eventstore.engine.WriterLockHeld
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.security.MessageDigest
import kotlin.random.Random
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
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

    /**
     * The queries that RESOLVE a page, without the id-first hydration (the one
     * `id in (…)` lookup that reads the documents afterwards — see
     * VespaEventIndex.hydrate). The strategy tests count attempts; hydration is
     * pinned by its own tests.
     */
    private fun List<Map<String, String>>.pageQueries() = filter { !it.getValue("yql").contains(" where id in (") }

    /** Whether [yql] asks for exactly [hits] — `limit 20` must not match `limit 200`. */
    private fun asksFor(
        yql: String,
        hits: Int,
    ) = Regex("""\blimit $hits\b""").containsMatchIn(yql)

    /** The hit count the LAST limit'd page query since [before] asked for: the read's overfetch (a `[t,t]` read has no limit). */
    private fun lastFetch(before: Int): Int =
        mock.searchRequests
            .drop(before)
            .pageQueries()
            .mapNotNull { q ->
                Regex("""\blimit (\d+)\b""")
                    .findAll(q.getValue("yql"))
                    .lastOrNull()
                    ?.groupValues
                    ?.get(1)
                    ?.toInt()
            }.last()

    /** The `[t, t]` tie-window queries since [before]: the extra query a cut boundary group pays. */
    private fun tieQueries(
        before: Int,
        t: Long,
    ) = mock.searchRequests
        .drop(before)
        .pageQueries()
        .filter { it.getValue("yql").let { y -> y.contains("created_at >= $t") && y.contains("created_at <= $t") } }

    /**
     * Calls booked under each `recency.speculative*` stage, keyed by the suffix.
     * [IngestStats] is process-global, so tests compare a before/after pair.
     */
    private fun speculativeCalls(): Map<String, Long> =
        IngestStats
            .snapshot()
            .filterKeys { it.startsWith(VespaEventIndex.SPECULATIVE_STAGE) }
            .mapKeys { it.key.removePrefix(VespaEventIndex.SPECULATIVE_STAGE) }
            .mapValues { it.value.calls }

    private fun Map<String, Long>.minus(before: Map<String, Long>): Map<String, Long> = mapValues { (k, v) -> v - (before[k] ?: 0L) }.filterValues { it != 0L }

    private companion object {
        val BUSY = "b1".repeat(32)
        val WEEKLY = "b2".repeat(32)
        val QUIET = "b3".repeat(32)
    }

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

    @Test
    fun `split feed and query containers each get only their own role's requests`() =
        runBlocking {
            // Two containers over one content cluster: one declares only <document-api/>, the other
            // only <search/>, and each answers the other's paths 404. Every request must land on
            // the role that serves it, and the contract must hold across the split: an acked put
            // through the feed side is visible to the next search through the query side.
            val content = InMemoryEventIndex()
            val feedSide = MockVespaEngine(content, servesSearch = false)
            val querySide = MockVespaEngine(content, servesDocumentApi = false)
            val split =
                VespaEventIndex(
                    "http://127.0.0.1:1",
                    feedEndpoints = listOf(feedSide.url),
                    queryEndpoints = listOf(querySide.url),
                    feedBatch = 10,
                )
            try {
                val one = doc(kind = 1, content = "split one")
                split.put(one)
                val many = List(25) { doc(kind = 1, content = "split batch $it") }
                split.putAll(many)
                assertEquals(3, feedSide.batchRequests, "the batch handler is on the feed side")
                assertEquals(one.id, split.get(one.id)?.id, "get by id is a document-API read")
                assertEquals(listOf(one.id), split.search(EventQuery(ids = listOf(one.id))).map { it.id })
                assertEquals(26, split.count(EventQuery(kinds = listOf(1))))
                var visited = 0
                var resume: String? = null
                do {
                    val page = split.visitDocsPage(EventQuery(kinds = listOf(1)), resume, maxDocs = 100)
                    visited += page.docs.size
                    resume = page.continuation
                } while (resume != null)
                assertEquals(26, visited)
                assertTrue(feedSide.visitRequests > 0, "a visit is a document-API read")
                split.remove(one.id)
                assertNull(split.get(one.id))

                assertEquals(0, feedSide.wrongRoleRequests, "no query reached the feed container")
                assertEquals(0, querySide.wrongRoleRequests, "no document-API request reached the query container")
                assertTrue(feedSide.searchRequests.isEmpty())
                assertTrue(querySide.searchRequests.isNotEmpty())
            } finally {
                split.close()
                feedSide.stop()
                querySide.stop()
            }
        }

    @Test
    fun `a get that reaches a container without the document API throws instead of reading absent`() =
        runBlocking {
            // A query container in feedEndpoints, the misconfiguration a deployment walks into by
            // keeping `endpoints = every container` after splitting its clusters. Its 404 is not a
            // missing document, and a get that read it as one would let a kind 5 skip the targets it
            // could not "find". The handshake passes (one endpoint answers it), so only the get can
            // refuse.
            val content = InMemoryEventIndex()
            val feedSide = MockVespaEngine(content, servesSearch = false)
            val querySide = MockVespaEngine(content, servesDocumentApi = false)
            val mixed = VespaEventIndex("http://127.0.0.1:1", feedEndpoints = listOf(feedSide.url, querySide.url), queryEndpoints = listOf(querySide.url))
            try {
                val missing = "f".repeat(64)
                val answers = List(4) { runCatching { mixed.get(missing) } }
                assertEquals(2, answers.count { it.isSuccess && it.getOrNull() == null }, "the feed container's 404 is a missing document")
                val refused = answers.mapNotNull { it.exceptionOrNull() }
                assertEquals(2, refused.size, "the query container's 404 is not")
                assertTrue(refused.all { "not configured" in it.message.orEmpty() }, refused.toString())
                assertEquals(2, querySide.wrongRoleRequests)
            } finally {
                mixed.close()
                feedSide.stop()
                querySide.stop()
            }
        }

    @Test
    fun `only a document API body reads as absent`() {
        assertTrue(isAbsentDocument("""{"pathId":"/document/v1/event/event/docid/x","id":"id:event:event::x"}"""))
        assertFalse(isAbsentDocument("""{"error-code":"NOT_FOUND","message":"Document API is not configured in this cluster"}"""))
        assertFalse(isAbsentDocument("<html><body>404 Not Found</body></html>"), "a proxy's page")
        assertFalse(isAbsentDocument(""))
    }

    /**
     * The reindex's re-put on the wire: one test-and-set put per doc, never
     * created, so a doc removed since it was read is not brought back. The
     * in-memory reference answers the same.
     */
    @Test
    fun `putAllIfStored writes only docs still stored, as the reference does`() =
        runBlocking {
            val kept = doc(content = "kept")
            val gone = doc(content = "gone")
            seed(kept, gone)
            index.remove(gone.id)
            reference.remove(gone.id)
            val page = listOf(kept, gone).map { it.copy(search = SearchFields(text = "rederived")) }
            val notHex = kept.copy(id = "not-an-event-id")

            val batches = index.feedBatchesSent
            assertEquals(setOf(kept.id), index.putAllIfStored(page + notHex))
            assertEquals(batches + 1, index.feedBatchesSent, "the conditional puts rode the batch handler")
            assertEquals(setOf(kept.id), reference.putAllIfStored(page))
            assertEquals("rederived", index.get(kept.id)?.search?.text, "the kept doc carries the new column")
            assertEquals(null, index.get(gone.id), "the removed doc stays removed")
            assertTrue(index.putAllIfStored(emptyList()).isEmpty())

            // The per-document path, with batching off: one test-and-set put each, same answer.
            VespaEventIndex(mock.url, feedBatch = 1).use { single ->
                val again = page.map { it.copy(search = SearchFields(text = "single")) }
                assertEquals(setOf(kept.id), single.putAllIfStored(again))
                assertEquals(0L, single.feedBatchesSent)
                assertEquals("single", index.get(kept.id)?.search?.text)
                assertEquals(null, index.get(gone.id))
            }
        }

    /**
     * Visits go over HTTP/1.1 on their own connections; queries stay on the
     * shared HTTP/2 connection. Measured against a real Vespa 8.781.11: a visit
     * page multiplexed on the store's busy h2c connection sometimes stopped
     * mid-body (headers and the first bytes sent, then nothing until the client's
     * 120 s read timeout), in 4 of 15 reindex walks. The same visits over
     * HTTP/1.1 stalled in 0 of 30 walks, and 0 of 2,400+ pages from a separate
     * HTTP/1.1 client (benchmark/README.md, `reindexLockProbe`). Every visit
     * transport is covered: a reindex page, the paged walk, the streamed slices.
     */
    @Test
    fun `visits ride HTTP 1 1 and queries stay on HTTP 2`() =
        runBlocking {
            seed(doc(), doc(), doc())
            mock.protocols.clear()
            index.visitDocsPage(EventQuery(kinds = listOf(1)), null, 10)
            index.visitTags(EventQuery(kinds = listOf(1))) { true }
            index.scanAuthors(EventQuery(kinds = listOf(1)))
            VespaEventIndex(mock.url, visitStreaming = true, visitSlices = 2).use { streamed ->
                streamed.scanAuthors(EventQuery(kinds = listOf(1)))
            }
            index.search(EventQuery(kinds = listOf(1), limit = 10))

            val visits = mock.protocols.filter { (path, _) -> path == "/document/v1/event/event/docid" }
            assertTrue(visits.size >= 5, "every visit transport ran: $visits")
            assertEquals(setOf("HTTP/1.1"), visits.map { it.second }.toSet(), "visits")
            assertEquals(
                setOf("HTTP/2.0"),
                mock.protocols
                    .filter { it.first == "/search/" }
                    .map { it.second }
                    .toSet(),
                "queries",
            )
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

    /**
     * [doc] with a HASH for an id, as a real event's is. [doc]'s ids count up
     * from `000…1`, so they share every leading digit — the right shape for most
     * tests, and the wrong one for an id-prefix split, which relies on ids
     * spreading evenly over the hex digits.
     */
    private fun hashedDoc(
        kind: Int,
        pubkey: String,
        at: Long,
    ): EventDoc {
        val d = doc(kind = kind, pubkey = pubkey, at = at)
        val id = MessageDigest.getInstance("SHA-256").digest(d.id.toByteArray()).joinToString("") { "%02x".format(it) }
        return d.copy(id = id)
    }

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
        // The id prefix: one id, all of them, none — [doc]'s ids count up from `000…1`.
        check(EventQuery(idPrefix = "0".repeat(63) + "3"))
        check(EventQuery(idPrefix = "0".repeat(62)))
        check(EventQuery(kinds = listOf(1), idPrefix = "1"))
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
            val atBand = mock.searchRequests.size
            assertEquals(band, index.search(EventQuery(kinds = listOf(1), limit = band)).size, "the band's own limit is served whole")
            assertEquals(
                listOf(EventYql.RANK_RECENCY),
                mock.searchRequests
                    .drop(atBand)
                    .pageQueries()
                    .map { it["ranking"] }
                    .distinct(),
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
     * A corpus with every density a [RecencyStrategy] branches on: [BUSY]
     * posting every second of the last half-minute (the first speculative
     * window fills), [WEEKLY] posting every 8 hours for a week (it widens),
     * [QUIET] posting only in 1970 (the narrow-read rule gives up at once),
     * plus kind-7 noise interleaved with the busy author.
     */
    private fun seedStrategyCorpus(now: Long) {
        seed(*(1..30).map { doc(kind = 1, pubkey = BUSY, at = now - it) }.toTypedArray())
        seed(*(1..20).map { doc(kind = 1, pubkey = WEEKLY, at = now - it * 8 * 3_600L) }.toTypedArray())
        seed(*(1..5).map { doc(kind = 1, pubkey = QUIET, at = 1_000L + it) }.toTypedArray())
        seed(*(1..10).map { doc(kind = 7, pubkey = BUSY, at = now - it * 2) }.toTypedArray())
    }

    /**
     * THE STRATEGIES ARE INVISIBLE IN RESULTS. Whatever a [RecencyStrategy]
     * does to the engine's workload, the page it serves is the page every other
     * strategy serves — ids AND order — across the shapes that exercise each
     * branch: a window that fills first try, one that widens, a narrow read that
     * falls back, a deep `until`, a caller `since` tighter than any window, a
     * limit past the corpus, and the gated profile (whose gate the mock does not
     * apply — that half is RecencyStrategyIT's, against a real Vespa).
     */
    @Test
    fun `every recency strategy serves the same page`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val clients = RecencyStrategy.entries.associateWith { VespaEventIndex(mock.url, recencyStrategy = it) }
            try {
                val shapes =
                    listOf(
                        EventQuery(kinds = listOf(1), limit = 10),
                        EventQuery(kinds = listOf(1), limit = 40),
                        EventQuery(kinds = listOf(1, 7), limit = 35),
                        EventQuery(authors = listOf(WEEKLY), limit = 5),
                        EventQuery(authors = listOf(QUIET), limit = 3),
                        EventQuery(kinds = listOf(1), until = now - 2 * 86_400L, limit = 5),
                        EventQuery(kinds = listOf(1), since = now - 10, limit = 50),
                        EventQuery(limit = 500),
                        EventQuery(kinds = listOf(1), limit = 10, ranking = EventYql.RANK_RECENCY_GATED, observer = BUSY, rankKey = BUSY, minRank = 2.0),
                    ).map { it.copy(nowSecs = now) }
                for (q in shapes) {
                    val pages = clients.mapValues { (_, c) -> c.search(q).map { it.id } }
                    val expected = pages.getValue(RecencyStrategy.FULL_SCAN)
                    assertTrue(expected.isNotEmpty(), "the shape must match something: $q")
                    pages.forEach { (s, ids) -> assertEquals(expected, ids, "$s vs FULL_SCAN: $q") }
                }
            } finally {
                clients.values.forEach { it.close() }
            }
        }

    /** The busy feed is the case the speculative window exists for: ONE engine query, and it is the windowed one. */
    @Test
    fun `speculative serves a busy feed from its first window`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                val before = mock.searchRequests.size
                val booked = speculativeCalls()
                val page = speculative.search(EventQuery(kinds = listOf(1), limit = 10, nowSecs = now))
                val sent = mock.searchRequests.drop(before).pageQueries()
                assertEquals(mapOf("" to 1L, ".attempt" to 1L, ".first" to 1L), speculativeCalls().minus(booked), "one read, one attempt, proven first")
                assertEquals((1..10).map { now - it }, page.map { it.createdAt })
                assertEquals(1, sent.size, "a full first window is the whole answer: ${sent.map { it["yql"] }}")
                assertTrue(sent.single().getValue("yql").contains("created_at >= ${now - RecencyPlanner.FIRST_WINDOW}"), "the one query is the windowed one")
            }
        }

    /**
     * A far-future `until` (a client sentinel) must not strand the windows over
     * empty time: the anchor is capped at the request clock, so the busy feed
     * still resolves in its first window — and a note dated past the clock
     * (the live corpus holds some from 2100) is still inside it and still first.
     */
    @Test
    fun `speculative anchors a future until at the request clock`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val future = doc(kind = 1, pubkey = BUSY, at = now + 5 * 365 * 86_400L)
            seed(future)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                val before = mock.searchRequests.size
                val page = speculative.search(EventQuery(kinds = listOf(1), until = now + 100L * 365 * 86_400, limit = 10, nowSecs = now))
                assertEquals(
                    1,
                    mock.searchRequests
                        .drop(before)
                        .pageQueries()
                        .size,
                    "one windowed query, anchored at now",
                )
                assertEquals(future.id, page.first().id, "a future-dated note is inside the window and newest")
                assertEquals(listOf(future.createdAt) + (1..9).map { now - it }, page.map { it.createdAt })
            }
        }

    /**
     * A GATED read past the match-phase band is windowed too. Plain reads past
     * the band are paged band by band (each page windowed); a gated one has no
     * pager and used to run the full-scan gated profile over the whole match
     * set — ~5 s on a 44M-doc corpus at the relay's max_limit. Here the busy
     * author fills a 3,000-limit page nowhere near, so the read must still be
     * windowed first and fall back — never go straight to the unbounded scan.
     */
    @Test
    fun `speculative windows a gated read past the band`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                val q = EventQuery(kinds = listOf(1), limit = EventYql.MATCH_PHASE_BAND + 1_000, ranking = EventYql.RANK_RECENCY_GATED, observer = BUSY, rankKey = BUSY, minRank = 2.0, nowSecs = now)
                val before = mock.searchRequests.size
                val page = speculative.search(q)
                val yqls = mock.searchRequests.drop(before).map { it.getValue("yql") }
                assertEquals(55, page.size, "the whole (ungated, on the mock) kind-1 corpus")
                assertTrue(yqls.first().contains("created_at >= "), "the first query is a window: $yqls")
            }
        }

    /**
     * THE NARROW-READ RULE: a read with nothing in the first window has no rate
     * to widen by, so it projects past [RecencyPlanner.NARROW_HORIZON] and runs
     * unwindowed immediately — one small attempt over the shipped cost, not a
     * ladder of them.
     */
    @Test
    fun `speculative gives up on a narrow read after one attempt`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                val before = mock.searchRequests.size
                val booked = speculativeCalls()
                val page = speculative.search(EventQuery(authors = listOf(QUIET), limit = 3, nowSecs = now))
                assertEquals(mapOf("" to 1L, ".attempt" to 1L, ".fallback.narrow" to 1L), speculativeCalls().minus(booked), "one attempt, then the narrow-read fallback")
                val sent =
                    mock.searchRequests
                        .drop(before)
                        .pageQueries()
                        .map { it.getValue("yql") }
                assertEquals(listOf(1_005L, 1_004L, 1_003L), page.map { it.createdAt })
                assertEquals(2, sent.size, "one windowed attempt, then the unwindowed read: $sent")
                assertTrue(sent[0].contains("created_at >= "), "the attempt is windowed")
                assertTrue(!sent[1].contains("created_at >= "), "the fallback is not")
            }
        }

    /**
     * A FEED BLOCK IS SAID AT ONCE. A cluster past its disk or memory limit refuses every write, and the
     * batch handler lists each one INSUFFICIENT_STORAGE: the write throws [EngineRefusesWrites] straight
     * away, without re-putting each document through the feed client, whose retries held a refused
     * batch of 100 for 5 s on a real Vespa. A lone put is answered at once as well; a write only the
     * feed client makes still fails, as [EngineRefusesWrites].
     */
    @Test
    fun `a feed block fails a batch write at once without re-putting it`() =
        runBlocking {
            mock.feedBlocked = true
            VespaEventIndex(mock.url).use { index ->
                val docs = (1..50).map { doc(kind = 1, at = 1_000L + it) }
                val before = mock.documentPutAttempts.get()
                val started = System.nanoTime()
                assertFailsWith<EngineRefusesWrites> { index.putAll(docs) }
                assertEquals(before, mock.documentPutAttempts.get(), "nothing re-put through the feed client")
                assertTrue(System.nanoTime() - started < 5_000_000_000L, "answered at once, not after the feed client's retries")
                // A lone document takes the batch path too, so it is answered as fast and in the same words.
                val lone = System.nanoTime()
                assertFailsWith<EngineRefusesWrites> { index.putAll(listOf(doc(kind = 1, at = 2_000L))) }
                assertEquals(before, mock.documentPutAttempts.get(), "a lone document is not re-put through the feed client either")
                assertTrue(System.nanoTime() - lone < 5_000_000_000L, "a lone document answered at once")
                // A lone put with nothing in flight goes straight to /document/v1 and is answered at once too.
                val put = System.nanoTime()
                assertFailsWith<EngineRefusesWrites> { index.put(doc(kind = 1, at = 3_000L)) }
                assertTrue(System.nanoTime() - put < 5_000_000_000L, "a lone put answered at once")
                assertEquals(0L, index.directPuts, "and not counted as stored")
            }
            // A write only the feed client makes (an address-keyed conditional put) still waits out its
            // retries, but its failure names the engine.
            VespaEventIndex(mock.url, addressKeyed = true).use { index ->
                val refused = assertFailsWith<EngineRefusesWrites> { index.putIfNewer(doc(kind = 0, at = 3_000L)) }
                assertTrue(refused.cause?.message?.contains("External feed is blocked") == true, refused.cause?.message)
            }
        }

    /**
     * AN ENGINE THAT REFUSES `select` COSTS ONE REQUEST. A text-free read goes as Vespa's JSON
     * `select` first; refused with a 400 (this mock, like an engine predating `select.fields`),
     * the same query is asked as YQL at once, served, and every later read goes as YQL while
     * `select` is parked (SchemaFallbacksTest holds the park's end). With
     * `VESPA_SELECT_QUERIES` off nothing tries `select` at all.
     */
    @Test
    fun `a refused select query falls back to yql and parks select`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val q = EventQuery(kinds = listOf(1), authors = listOf(BUSY), limit = 5, nowSecs = now)
            val refusedBefore = IngestStats.snapshot()[VespaEventIndex.SELECT_REFUSED_STAGE]?.calls ?: 0L
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { index ->
                val first = index.search(q).map { it.createdAt }
                assertEquals((1..5).map { now - it }, first, "the YQL answer, served on the first read")
                assertEquals(1, mock.selectRequests.size, "select tried once")
                assertTrue(mock.selectRequests.single().contains("\"select\":{\"where\":"), mock.selectRequests.single())
                assertEquals(first, index.search(q.copy(limit = 5)).map { it.createdAt })
                index.search(EventQuery(kinds = listOf(7), limit = 3, nowSecs = now))
                assertEquals(1, mock.selectRequests.size, "every later read goes as YQL")
            }
            assertEquals(refusedBefore + 1, IngestStats.snapshot()[VespaEventIndex.SELECT_REFUSED_STAGE]?.calls ?: 0L)
            mock.selectRequests.clear()
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE, selectQueries = false).use { index ->
                index.search(q)
                assertEquals(0, mock.selectRequests.size, "switched off: YQL only")
            }
        }

    /**
     * THE PRIOR PAYS ON THE FIRST READ. Every value of a `#p` feed is a new exact
     * shape, so the window memory never answers its first read: each cold one
     * spends an attempt, then the fallback. Once [NarrowPrior.MIN_SAMPLES] of
     * them have ended narrow, a value no read has named yet goes straight to the
     * unwindowed read — one query — and serves the same page FULL_SCAN serves.
     */
    @Test
    fun `cold reads of a narrow family take one query once the family has voted`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val quietMention = doc(kind = 1, pubkey = QUIET, at = 1_500L, tags = listOf(listOf("p", "c0".repeat(32))))
            seed(quietMention)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.FULL_SCAN).use { reference ->
                    fun mentions(key: String) = EventQuery(kinds = listOf(1), tags = mapOf("p" to listOf(key)), limit = 10, nowSecs = now)

                    suspend fun sent(q: EventQuery): Int {
                        val before = mock.searchRequests.size
                        assertEquals(reference.search(q).map { it.id }, speculative.search(q).map { it.id }, "same page as FULL_SCAN: $q")
                        return mock.searchRequests
                            .drop(before)
                            .pageQueries()
                            .count()
                    }
                    // `sent` also counts the FULL_SCAN reference's one query.
                    repeat(NarrowPrior.MIN_SAMPLES) { i ->
                        assertEquals(1 + 2, sent(mentions(i.toString(16).padStart(64, 'd'))), "cold, before the family has voted: an attempt, then the fallback")
                    }
                    val booked = speculativeCalls()
                    assertEquals(1 + 1, sent(mentions("e0".repeat(32))), "a value no read named: the unwindowed read alone")
                    assertEquals(1 + 1, sent(mentions("c0".repeat(32))), "and one that matches, served whole")
                    assertEquals(mapOf("" to 2L, ".prior.narrow" to 2L), speculativeCalls().minus(booked))
                }
            }
        }

    /**
     * A WRONG VOTE COSTS ONE READ. A busy value in a family that voted narrow is
     * sent unwindowed once (the shipped match-phase cost); its full page names the
     * window that would have held it, so its exact shape remembers that window and
     * its next read is the windowed one, proven first.
     */
    @Test
    fun `a busy value the prior sent unwindowed teaches its shape a window`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            val hot = "c1".repeat(32)
            seed(*(1..30).map { doc(kind = 1, pubkey = BUSY, at = now - it, tags = listOf(listOf("p", hot))) }.toTypedArray())
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                fun mentions(key: String) = EventQuery(kinds = listOf(1), tags = mapOf("p" to listOf(key)), limit = 10, nowSecs = now)
                repeat(NarrowPrior.MIN_SAMPLES) { i -> speculative.search(mentions(i.toString(16).padStart(64, 'd'))) }

                var booked = speculativeCalls()
                var before = mock.searchRequests.size
                val first = speculative.search(mentions(hot))
                assertEquals((1..10).map { now - it }, first.map { it.createdAt })
                assertEquals(mapOf("" to 1L, ".prior.narrow" to 1L, ".prior.busy" to 1L), speculativeCalls().minus(booked))
                assertEquals(
                    1,
                    mock.searchRequests
                        .drop(before)
                        .pageQueries()
                        .size,
                    "sent unwindowed once",
                )

                booked = speculativeCalls()
                before = mock.searchRequests.size
                val again = speculative.search(mentions(hot))
                assertEquals(first.map { it.id }, again.map { it.id })
                assertEquals(mapOf("" to 1L, ".recalled.window" to 1L, ".attempt" to 1L, ".first" to 1L), speculativeCalls().minus(booked))
                val sent = mock.searchRequests.drop(before).pageQueries()
                assertEquals(1, sent.size)
                assertTrue(sent.single().getValue("yql").contains("created_at >= "), "the remembered window: ${sent.single()["yql"]}")
            }
        }

    /**
     * THE MEMORY PAYS ON THE SECOND READ. A narrow shape's first read spends one
     * attempt before falling back; the second skips straight to the unwindowed
     * read — one query, the shipped cost. A shape that needed a second, wider
     * window starts the next read there and proves out in one. Same pages
     * throughout: the memory moves cost, never answers.
     */
    @Test
    fun `speculative remembers where a shape found its page`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                suspend fun read(q: EventQuery): Pair<List<Long>, Int> {
                    val before = mock.searchRequests.size
                    val page = speculative.search(q).map { it.createdAt }
                    return page to
                        mock.searchRequests
                            .drop(before)
                            .pageQueries()
                            .size
                }

                val narrow = EventQuery(authors = listOf(QUIET), limit = 3, nowSecs = now)
                val (narrowFirst, narrowFirstSent) = read(narrow)
                var booked = speculativeCalls()
                val (narrowAgain, narrowAgainSent) = read(narrow)
                assertEquals(narrowFirst, narrowAgain)
                assertEquals(2, narrowFirstSent, "cold: one attempt, then the fallback")
                assertEquals(1, narrowAgainSent, "remembered narrow: the unwindowed read alone")
                assertEquals(mapOf("" to 1L, ".recalled.narrow" to 1L), speculativeCalls().minus(booked))

                // 30 busy notes in the hour, one weekly note exactly 8h back: a
                // limit of 31 misses in the first window and proves in the second.
                val widening = EventQuery(kinds = listOf(1), limit = 31, nowSecs = now)
                booked = speculativeCalls()
                val (wideFirst, wideFirstSent) = read(widening)
                assertEquals(1L, speculativeCalls().minus(booked)[".widened"], "cold: proven by the second window")
                assertEquals(2, wideFirstSent)
                booked = speculativeCalls()
                val (wideAgain, wideAgainSent) = read(widening)
                assertEquals(wideFirst, wideAgain)
                assertEquals(1, wideAgainSent, "remembered window: proven by the first attempt")
                assertEquals(mapOf("" to 1L, ".attempt" to 1L, ".recalled.window" to 1L, ".first" to 1L), speculativeCalls().minus(booked))
            }
        }

    /**
     * A page served off PART of the corpus (a node down, `acceptsPartial`) is thinner than the
     * shape's real rate, so the memory learns nothing from it: the same narrow shape that is
     * remembered after one healthy read is still cold after one served partial, and after the
     * outage too.
     */
    @Test
    fun `speculative learns nothing from a page served partial`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                suspend fun sent(q: EventQuery): Int {
                    val before = mock.searchRequests.size
                    speculative.search(q)
                    return mock.searchRequests
                        .drop(before)
                        .pageQueries()
                        .size
                }

                val narrow = EventQuery(authors = listOf(QUIET), limit = 3, nowSecs = now, acceptsPartial = true)
                mock.nonIdealStateCoverage = true
                assertEquals(2, sent(narrow), "cold: one attempt, then the fallback")
                assertEquals(2, sent(narrow), "a page served partial was not remembered as narrow")
                mock.nonIdealStateCoverage = false
                assertEquals(2, sent(narrow), "nor is the shape remembered once the node is back")
                assertEquals(1, sent(narrow), "a healthy read is remembered as before")
            }
        }

    /**
     * PAGE REUSE, widening: a short first window is the newest slice of the
     * answer, so the second attempt asks only for what is strictly older than
     * that window, with only the remaining limit — never re-fetching the 30
     * notes the first attempt delivered. Same page as the full scan.
     */
    @Test
    fun `a widened attempt fetches only what the short one did not`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = false).use { speculative ->
                val q = EventQuery(kinds = listOf(1), limit = 31, nowSecs = now)
                val before = mock.searchRequests.size
                val page = speculative.search(q)
                val yqls =
                    mock.searchRequests
                        .drop(before)
                        .pageQueries()
                        .map { it.getValue("yql") }
                assertEquals(2, yqls.size, "a short window, then one widened attempt: $yqls")
                val boundary = now - RecencyPlanner.FIRST_WINDOW
                assertTrue(yqls[1].contains("created_at <= ${boundary - 1}"), "the widened attempt stops below the first window: ${yqls[1]}")
                assertTrue(asksFor(yqls[1], 1 + VespaEventIndex.TIE_SLACK), "and asks for the ONE missing note (plus the flat tie slack, memory off): ${yqls[1]}")
                assertEquals((1..30).map { now - it } + (now - 8 * 3_600L), page.map { it.createdAt })
                VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.FULL_SCAN).use { full ->
                    assertEquals(full.search(q).map { it.id }, page.map { it.id }, "the same page the full scan serves")
                }
            }
        }

    /**
     * PAGE REUSE, fallback: a first window holding ONE note projects no rate and
     * falls back — and the fallback asks only for what is older than that
     * window, with the remaining limit, keeping the note it already has.
     */
    @Test
    fun `a fallback keeps what the short attempt found`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val recent = doc(kind = 1, pubkey = "b4".repeat(32), at = now - 10)
            seed(recent)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = false).use { speculative ->
                val q = EventQuery(authors = listOf(QUIET, recent.pubkey), limit = 3, nowSecs = now)
                val before = mock.searchRequests.size
                val page = speculative.search(q)
                val yqls =
                    mock.searchRequests
                        .drop(before)
                        .pageQueries()
                        .map { it.getValue("yql") }
                assertEquals(2, yqls.size, "one windowed attempt, then the fallback: $yqls")
                val boundary = now - RecencyPlanner.FIRST_WINDOW
                assertTrue(yqls[1].contains("created_at <= ${boundary - 1}") && !yqls[1].contains("created_at >= "), "the fallback is unwindowed below the boundary: ${yqls[1]}")
                assertTrue(asksFor(yqls[1], 2 + VespaEventIndex.TIE_SLACK), "for the two notes still missing: ${yqls[1]}")
                assertEquals(listOf(now - 10, 1_005L, 1_004L), page.map { it.createdAt })
            }
        }

    /**
     * Memory off (`VESPA_RECENCY_MEMORY=0`): every read starts cold, which is
     * what the probe compares against.
     */
    @Test
    fun `speculative without memory starts every read cold`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = false).use { speculative ->
                val narrow = EventQuery(authors = listOf(QUIET), limit = 3, nowSecs = now)
                repeat(2) {
                    val before = mock.searchRequests.size
                    speculative.search(narrow)
                    assertEquals(
                        2,
                        mock.searchRequests
                            .drop(before)
                            .pageQueries()
                            .size,
                        "no memory: an attempt and the fallback, every time",
                    )
                }
            }
        }

    /**
     * ID-FIRST: the page is resolved off the attribute-only `idtime` summary —
     * overfetch included — and the documents are read ONCE, for exactly the
     * page's ids. Same page as reading documents all the way.
     */
    @Test
    fun `id-first reads documents only for the page`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val q = EventQuery(kinds = listOf(1), limit = 10, nowSecs = now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = false, idFirstMaxDirectHits = 0).use { idFirst ->
                val before = mock.searchRequests.size
                val page = idFirst.search(q)
                val sent = mock.searchRequests.drop(before)
                val resolve = sent.filter { !it.getValue("yql").contains(" where id in (") }
                val hydrate = sent.filter { it.getValue("yql").contains(" where id in (") }
                assertTrue(resolve.isNotEmpty() && resolve.all { it["presentation.summary"] == "idtime" && it.getValue("yql").startsWith("select id, created_at from ") }, "every page query is ids-only: $resolve")
                assertEquals(1, hydrate.size, "the documents are read once: $hydrate")
                assertEquals("10", hydrate.single()["hits"], "for exactly the page")
                assertEquals((1..10).map { now - it }, page.map { it.createdAt })
                VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = false, idFirst = false).use { documents ->
                    assertEquals(documents.search(q).map { it.id }, page.map { it.id }, "the page reading documents all the way serves")
                    assertEquals(page.map { it.content }, documents.search(q).map { it.content }, "and the same documents, whole")
                }
            }
        }

    /**
     * A PAGE PAST ONE HYDRATION CHUNK READS ITS CHUNKS AT ONCE. A 5,000-limit
     * REQ is three `id in (…)` lookups of [VespaEventIndex.HYDRATE_CHUNK], and
     * they were issued back to back — three document-store round trips where
     * one would do. Overlap is read off the mock's in-flight gauge, not a
     * clock, and the page must stay the page: every document, in order.
     *
     * Verified to fail with the serial loop restored: peak 1.
     */
    @Test
    fun `a hydration of several chunks fetches them concurrently, in page order`() =
        runBlocking {
            val chunks = 3
            val n = VespaEventIndex.HYDRATE_CHUNK * (chunks - 1) + 100
            val now = System.currentTimeMillis() / 1000
            seedBulk((1..n).map { doc(kind = 1, at = now - it) })
            val q = EventQuery(kinds = listOf(1), limit = n, nowSecs = now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE, recencyMemory = false).use { idFirst ->
                mock.holdIdLookupsMs = 500
                mock.idLookupsInFlightPeak = 0
                val before = mock.searchRequests.size
                val page =
                    try {
                        idFirst.search(q)
                    } finally {
                        mock.holdIdLookupsMs = 0
                    }
                val hydrate = mock.searchRequests.drop(before).filter { it.getValue("yql").contains(" where id in (") }
                assertEquals(chunks, hydrate.size, "one lookup per chunk: ${hydrate.map { it["hits"] }}")
                assertEquals(chunks, mock.idLookupsInFlightPeak, "every chunk in flight at once")
                assertEquals(reference.search(q).map { it.id }, page.map { it.id }, "and the page is the page, in order")
            }
        }

    /** Id-first moves reads, never answers: every strategy, every branch, the same page with it on and off. */
    @Test
    fun `id-first and documents-first serve the same pages`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val shapes =
                listOf(
                    EventQuery(kinds = listOf(1), limit = 10),
                    EventQuery(kinds = listOf(1), limit = 31),
                    EventQuery(kinds = listOf(1, 7), limit = 35),
                    EventQuery(authors = listOf(QUIET), limit = 3),
                    EventQuery(kinds = listOf(1), until = now - 2 * 86_400L, limit = 5),
                    EventQuery(limit = 500),
                    EventQuery(kinds = listOf(1), limit = 10, ranking = EventYql.RANK_RECENCY_GATED, observer = BUSY, rankKey = BUSY, minRank = 2.0),
                ).map { it.copy(nowSecs = now) }
            for (strategy in RecencyStrategy.entries) {
                VespaEventIndex(mock.url, recencyStrategy = strategy, idFirst = true).use { on ->
                    VespaEventIndex(mock.url, recencyStrategy = strategy, idFirst = false).use { off ->
                        for (q in shapes) assertEquals(off.search(q).map { it.id }, on.search(q).map { it.id }, "$strategy: $q")
                    }
                }
            }
        }

    /**
     * A BULK TIE GROUP — a trust provider publishes hundreds of thousands of
     * cards on one second — resolves off ids in EITHER mode: the tie window reads
     * the group's ids, not its documents. Id-first reads only the page's
     * documents; a direct read (the default) reads its page query's documents and
     * at most the page's worth more, never the group.
     */
    @Test
    fun `a bulk tie group resolves from ids`() =
        runBlocking {
            val second = 1_700_000_000L
            seedBulk(List(300) { doc(kind = 30382, pubkey = WEEKLY, at = second) })
            val q = EventQuery(kinds = listOf(30382), limit = 5)

            fun List<Map<String, String>>.tieWindows() = filter { it.getValue("yql").contains("created_at >= $second") && it.getValue("yql").contains("created_at <= $second") }
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.MATCH_PHASE, idFirstMaxDirectHits = 0).use { idFirst ->
                val before = mock.searchRequests.size
                val page = idFirst.search(q)
                val sent = mock.searchRequests.drop(before)
                val resolve = sent.filter { !it.getValue("yql").contains(" where id in (") }
                assertTrue(resolve.tieWindows().isNotEmpty(), "the tie window ran: $resolve")
                assertTrue(resolve.all { it["presentation.summary"] == "idtime" }, "and ids-only, like the page query: $resolve")
                assertEquals("5", sent.single { it.getValue("yql").contains(" where id in (") }["hits"], "five documents read, not three hundred")
                assertEquals(page.map { it.id }.sorted(), page.map { it.id }, "the lowest ids win the tie (NIP-01 order)")
                assertEquals(5, page.size)
                VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.MATCH_PHASE).use { direct ->
                    val at = mock.searchRequests.size
                    val same = direct.search(q)
                    val directSent = mock.searchRequests.drop(at)
                    val pageQueries = directSent.pageQueries()
                    assertFalse(pageQueries.first().isIdTime(), "the default build asks its page query for documents: $directSent")
                    assertTrue(pageQueries.tieWindows().isNotEmpty() && pageQueries.tieWindows().all { it.isIdTime() }, "and reads the group as ids: $directSent")
                    assertTrue(directSent.filter { it.isHydrate() }.all { it.getValue("hits").toInt() <= 5 }, "at most the page's documents more: $directSent")
                    assertEquals(page.map { it.id }, same.map { it.id }, "the same page")
                    assertEquals(page.map { it.content }, same.map { it.content }, "whole")
                }
            }
        }

    /**
     * A FULL READ IS SENT ON ITS SUMMARY CLASS. Naming the event's fields in YQL cost the query
     * container a field list to parse and a filter that decodes every field of every hit; the
     * read goes out as `select *` on `nip01` instead, and the page is the same, whole.
     */
    @Test
    fun `a full read is sent as select star on the nip01 summary`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val q = EventQuery(kinds = listOf(1), authors = listOf(BUSY), limit = 10, nowSecs = now)
            val before = mock.searchRequests.size
            val page = index.search(q)
            val sent = mock.searchRequests.drop(before)
            assertTrue(sent.isNotEmpty() && sent.all { it.getValue("yql").startsWith("select * from event where ") && it["presentation.summary"] == "nip01" }, "$sent")
            assertEquals(reference.search(q).map { it.id }, page.map { it.id })
            assertEquals(reference.search(q).map { it.sig to it.tags }, page.map { it.sig to it.tags }, "whole events")
        }

    /** A serving schema without `nip01` costs one refused read: it is sent again naming its fields, as every read is after. */
    @Test
    fun `a schema without the nip01 summary falls back to naming the fields`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            mock.rejectEventSummary = true
            try {
                VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.MATCH_PHASE).use { old ->
                    val q = EventQuery(kinds = listOf(1), authors = listOf(BUSY), limit = 10, nowSecs = now)
                    var before = mock.searchRequests.size
                    assertEquals(reference.search(q).map { it.id }, old.search(q).map { it.id }, "served")
                    var sent = mock.searchRequests.drop(before)
                    assertEquals(1, sent.count { it["presentation.summary"] == "nip01" }, "one refused read: $sent")
                    assertTrue(sent.last().getValue("yql").startsWith("select ${EventYql.SUMMARY_FIELDS} from "), "then the fields, named: $sent")
                    before = mock.searchRequests.size
                    old.search(q)
                    sent = mock.searchRequests.drop(before)
                    assertTrue(sent.none { it["presentation.summary"] == "nip01" }, "and never asked for nip01 again: $sent")
                }
            } finally {
                mock.rejectEventSummary = false
            }
        }

    /** A serving schema without `idtime` is remembered, and every read goes back to documents-first. */
    @Test
    fun `id-first falls back on a schema without the idtime summary`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            mock.rejectIdTimeSummary = true
            try {
                VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.MATCH_PHASE).use { index ->
                    val q = EventQuery(kinds = listOf(1), limit = 10, nowSecs = now)
                    assertEquals((1..10).map { now - it }, index.search(q).map { it.createdAt }, "served, documents-first")
                    val before = mock.searchRequests.size
                    index.search(q)
                    assertTrue(mock.searchRequests.drop(before).none { it["presentation.summary"] == "idtime" }, "and never asked for idtime again")
                }
            } finally {
                mock.rejectIdTimeSummary = false
            }
        }

    /**
     * A LEGACY SCHEMA MUST NOT LOSE THE WINDOW. Against a schema without the
     * `recency` profile, the profile net demotes a plain read to
     * [EventYql.RANK_UNRANKED] — an explicit ranking, which [isWindowable]
     * reads as an opt-out — AFTER planning. The shipped strategy windows the
     * bare scan before that demotion (count probes); the speculative one must
     * not hand it past the demotion unplanned, or every small-limit feed on
     * such a cluster becomes the full unranked sort the degrader cuts.
     */
    @Test
    fun `speculative keeps a window against a schema without the recency profile`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            mock.rejectRecencyProfile = true
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.SPECULATIVE).use { speculative ->
                try {
                    val q = EventQuery(kinds = listOf(1), limit = 10, nowSecs = now)
                    speculative.search(q) // flips the profile-missing flag
                    val before = mock.searchRequests.size
                    val page = speculative.search(q)
                    val yqls = mock.searchRequests.drop(before).map { it.getValue("yql") }
                    assertEquals((1..10).map { now - it }, page.map { it.createdAt })
                    assertTrue(yqls.any { it.contains("created_at >= ") }, "the read must still run inside a window: $yqls")
                } finally {
                    mock.rejectRecencyProfile = false
                }
            }
        }

    /**
     * A RANKED COUNT ASKS FOR ONE HIT. With `hits=0` Vespa skips first-phase
     * ranking, so `rank-score-drop-limit` never removes a below-floor author and
     * `totalCount` is the UNGATED match count (measured on Vespa 8.731 — see
     * [VespaEventIndex.count]). One hit is what forces the gate to run.
     * Plain counts stay on the hit-less grouping.
     */
    @Test
    fun `a ranked count asks the engine for one hit`() =
        runBlocking {
            seed(doc())
            val before = mock.searchRequests.size
            index.count(EventQuery(kinds = listOf(1), ranking = EventYql.RANK_RECENCY_GATED, observer = BUSY, rankKey = BUSY, minRank = 2.0))
            val gated = mock.searchRequests.drop(before).single()
            assertEquals(EventYql.RANK_RECENCY_GATED_EXACT, gated["ranking"])
            assertEquals("1", gated["hits"], "a gated count must run the ranking pass its gate lives in")

            val plainAt = mock.searchRequests.size
            index.count(EventQuery(kinds = listOf(1)))
            assertEquals("0", mock.searchRequests.drop(plainAt).single()["hits"], "an ungated count stays hit-less")
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
            val wide = VespaEventIndex.TIE_SLACK + 20 // past even a cold shape's (widest) slack
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

    /**
     * THE SLACK IS THE SHAPE'S. A shape seen first overfetches the flat [VespaEventIndex.TIE_SLACK];
     * one whose pages keep showing no boundary ties (one author, a note a second at most) narrows
     * toward [VespaEventIndex.MIN_TIE_SLACK]. Then the same shape meets a dense boundary second —
     * thirty notes in one second on an engine that returns ties in any order: the narrow slack
     * cuts it, the `[t,t]` query reads it whole and the page is still exact, and the shape goes
     * straight back to the wide slack, so its NEXT read pays no tie query. The busy-feed case
     * 10fbce5's page-width slack lost: there the cut was the common case at 20-50 events/s.
     */
    @Test
    fun `a sparse shape narrows its tie slack and a dense boundary widens it back`() =
        runBlocking {
            val author = "d7".repeat(32)
            seed(*(1..40).map { doc(kind = 1, pubkey = author, at = 5_000L + it * 10) }.toTypedArray())
            val q = EventQuery(kinds = listOf(1), authors = listOf(author), limit = 10)
            val fetches =
                (1..30).map {
                    val before = mock.searchRequests.size
                    assertEquals(reference.search(q).map { it.id }, index.search(q).map { it.id })
                    lastFetch(before)
                }
            // Its first read's short attempt measured the shape thin, so even the first fallback
            // starts at the least slack instead of the wide one (TieSlackMemory.measuredSlack).
            assertEquals(10 + VespaEventIndex.MIN_TIE_SLACK, fetches.first(), "a shape measured thin starts narrow: $fetches")
            assertTrue(fetches.zipWithNext().all { (a, b) -> b <= a }, "and never widens while no tie shows: $fetches")
            assertEquals(10 + VespaEventIndex.MIN_TIE_SLACK, fetches.last(), "at the least slack: $fetches")

            val dense = 6_000L
            seed(*(1..30).map { doc(kind = 1, pubkey = author, at = dense) }.toTypedArray())
            mock.scrambleTieOrder = true
            try {
                val cutAt = mock.searchRequests.size
                assertEquals(reference.search(q).map { it.id }, index.search(q).map { it.id }, "a cut boundary group is still the exact page (lowest ids)")
                assertEquals(1, tieQueries(cutAt, dense).size, "the narrow slack cut the thirty-note second, which paid its [t,t] query")
                val nextAt = mock.searchRequests.size
                assertEquals(reference.search(q).map { it.id }, index.search(q).map { it.id })
                assertEquals(10 + VespaEventIndex.TIE_SLACK, lastFetch(nextAt), "the cut put the shape back on the wide slack")
                assertEquals(emptyList(), tieQueries(nextAt, dense), "so its second read holds the group in one query")
            } finally {
                mock.scrambleTieOrder = false
            }
        }

    /**
     * A CUT LEARNS THE GROUP IT CUT. The `[T,T]` read counts the boundary group exactly, so a
     * shape whose boundary second holds more than the wide slack learns that width (with the
     * usual headroom, up to [TieSlackMemory.MAX_LEARNED_SLACK]) instead of going back to 64 and
     * paying the extra query on every read.
     */
    @Test
    fun `a cut boundary group wider than the wide slack is learned, not paid again`() =
        runBlocking {
            val author = "d9".repeat(32)
            seed(*(1..150).map { doc(kind = 1, pubkey = author, at = 9_000L) }.toTypedArray())
            val q = EventQuery(kinds = listOf(1), authors = listOf(author), limit = 10)
            mock.scrambleTieOrder = true
            try {
                assertEquals(reference.search(q).map { it.id }, index.search(q).map { it.id })
                val nextAt = mock.searchRequests.size
                assertEquals(reference.search(q).map { it.id }, index.search(q).map { it.id })
                assertEquals(10 + TieSlackMemory.MAX_LEARNED_SLACK, lastFetch(nextAt), "140 past the page: the learned cap")
                assertEquals(emptyList(), tieQueries(nextAt, 9_000L), "so the next read holds the group in one query")
            } finally {
                mock.scrambleTieOrder = false
            }
        }

    /**
     * A page served off PART of the corpus teaches the tie slack nothing, as it teaches the
     * window memory nothing: its boundary second is not the shape's.
     */
    @Test
    fun `a degraded page teaches its shape no tie slack`() =
        runBlocking {
            val author = "da".repeat(32)
            seed(*(1..30).map { doc(kind = 1, pubkey = author, at = 9_500L) }.toTypedArray())
            val q = EventQuery(kinds = listOf(1), authors = listOf(author), limit = 10, acceptsPartial = true)
            mock.scrambleTieOrder = true
            mock.nonIdealStateCoverage = true
            try {
                index.search(q)
                mock.nonIdealStateCoverage = false
                val at = mock.searchRequests.size
                assertEquals(reference.search(q).map { it.id }, index.search(q).map { it.id })
                assertEquals(10 + VespaEventIndex.MIN_TIE_SLACK, lastFetch(at), "the cut a degraded page saw was not learned")
            } finally {
                mock.scrambleTieOrder = false
                mock.nonIdealStateCoverage = false
            }
        }

    /** `VESPA_RECENCY_MEMORY=0` turns the learning off with the windows: every read keeps the flat slack. */
    @Test
    fun `with recency memory off every read overfetches the flat tie slack`() =
        runBlocking {
            val author = "d8".repeat(32)
            seed(*(1..40).map { doc(kind = 1, pubkey = author, at = 5_000L + it * 10) }.toTypedArray())
            VespaEventIndex(mock.url, recencyMemory = false).use { cold ->
                val q = EventQuery(kinds = listOf(1), authors = listOf(author), limit = 10)
                repeat(12) {
                    val before = mock.searchRequests.size
                    assertEquals(reference.search(q).map { it.id }, cold.search(q).map { it.id })
                    assertEquals(10 + VespaEventIndex.TIE_SLACK, lastFetch(before), "read ${it + 1}")
                }
            }
        }

    /**
     * A SUPERSESSION READS NO DOCUMENT. The decision needs the replaced versions' id,
     * created_at and d alone, and the default read them as full documents — a stored kind 3
     * is 50-500 KB of tags, read under the store's writer lock to be thrown away. It walks
     * the attribute-only summaries now (`idtime`; `idtimetag` for an addressable's d), and
     * still sweeps every older version, the d-matched one only.
     */
    @Test
    fun `a supersession reads its address off the attribute-only summaries`() =
        runBlocking {
            val follows = (1..300).map { listOf("p", it.toString(16).padStart(64, 'f')) }
            val v1 = doc(kind = 3, at = 1_000, tags = follows)
            val stale = doc(kind = 3, at = 900, tags = follows)
            seed(v1, stale)
            val post = doc(kind = 30_023, at = 1_000, tags = listOf(listOf("d", "a-post")))
            val other = doc(kind = 30_023, at = 1_000, tags = listOf(listOf("d", "other")))
            seed(post, other)
            val before = mock.searchRequests.size
            val v2 = doc(kind = 3, at = 2_000, tags = follows)
            val post2 = doc(kind = 30_023, at = 2_000, tags = listOf(listOf("d", "a-post")))
            assertTrue(index.putIfNewer(v2))
            assertTrue(index.putIfNewer(post2))
            assertFalse(index.putIfNewer(doc(kind = 3, at = 1_500, tags = follows)), "an older version still loses")
            val reads = mock.searchRequests.drop(before)
            assertTrue(reads.isNotEmpty(), "the versions were read")
            assertTrue(reads.all { it["presentation.summary"] in setOf("idtime", "idtimetag") }, "no read fetched a document: ${reads.map { it["presentation.summary"] to it["yql"] }}")
            assertEquals(listOf(v2.id), index.search(EventQuery(kinds = listOf(3))).map { it.id }, "every older version swept, the stale one too")
            assertEquals(setOf(post2.id, other.id), index.search(EventQuery(kinds = listOf(30_023))).map { it.id }.toSet(), "only the d-matched address superseded")
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
     * The same write net for the PROTOTYPE edge column (#161), which is its own
     * schema generation: a schema without `search_text_edge` must keep taking
     * body documents, and keep the near tier it does have.
     */
    @Test
    fun `a schema without the edge column still accepts body writes, minus that column`() =
        runBlocking {
            mock.rejectEdgeField = true
            val fresh = VespaEventIndex(mock.url, bodyEdgeFeed = true)
            try {
                val body = doc(kind = 1, search = SearchFields(text = "tarantellas every night"))
                fresh.put(body)
                assertEquals(body.id, fresh.get(body.id)?.id, "the document landed")
                val batch = (1..3).map { doc(kind = 1, search = SearchFields(text = "tarantella $it")) }
                fresh.putAll(batch)
                assertEquals(batch.map { it.id }.toSet(), batch.mapNotNull { fresh.get(it.id)?.id }.toSet(), "the batch landed")
            } finally {
                mock.rejectEdgeField = false
                fresh.close()
            }
        }

    @Test
    fun `a schema without the stemmed columns still accepts and finds a Portuguese body`() =
        runBlocking {
            mock.rejectStemFields = true
            val fresh = VespaEventIndex(mock.url)
            try {
                val body = doc(kind = 1, search = SearchFields(text = "Não perca as notícias de hoje, elas são muito importantes para nós"))
                fresh.put(body)
                assertEquals(body.id, fresh.get(body.id)?.id, "the document landed without its stemmed column")
                // A `language:pt` search against the same schema demotes rather than fails.
                assertEquals(listOf(body.id), fresh.search(EventQuery(kinds = listOf(1), search = "notícias", stemLanguage = "pt")).map { it.id })
            } finally {
                mock.rejectStemFields = false
                fresh.close()
            }
        }

    @Test
    fun `batched puts go through the batch handler, 100 to a request`() =
        runBlocking {
            val batched = VespaEventIndex(mock.url, feedBatch = 100)
            try {
                val docs = List(250) { doc(content = "batched $it") }
                batched.putAll(docs)
                assertEquals(3, mock.batchRequests, "250 documents are three requests")
                for (d in docs) assertEquals(d.id, batched.get(d.id)?.id)
                // A lone document is batched too: the handler answers a feed block at once, the feed client after its retries.
                batched.putAll(listOf(doc()))
                assertEquals(4, mock.batchRequests)
            } finally {
                batched.close()
            }
        }

    @Test
    fun `an application package without the batch handler falls back to the document API, and says so`() =
        runBlocking {
            mock.batchHandlerAbsent = true
            val batched = VespaEventIndex(mock.url, feedBatch = 100)
            try {
                assertTrue(batched.feedStatus().endsWith(" batch 0 sent"), batched.feedStatus())
                val docs = List(150) { doc() }
                batched.putAll(docs)
                for (d in docs) assertEquals(d.id, batched.get(d.id)?.id, "every document landed through /document/v1")
                assertEquals(false, batched.batchFeed?.available, "a 404 parks the batch path")
                assertTrue(batched.feedBatchParked)
                assertTrue(Regex(""" batch PARKED \(404 x\d\), retry in \d+s$""").containsMatchIn(batched.feedStatus()), batched.feedStatus())
                // The put's two batches run concurrently, so both may meet the 404 before it parks.
                val asked = batched.batchFeed!!.notFound
                batched.putAll(List(150) { doc() })
                assertEquals(0, mock.batchRequests, "parked: the next put does not try the handler")
                assertEquals(asked, batched.batchFeed?.notFound, "parked: no 404 asked again")
            } finally {
                mock.batchHandlerAbsent = false
                batched.close()
            }
        }

    @Test
    fun `a parked batch path tries the handler again once the interval is over`() =
        runBlocking {
            // A handler that arrives with a deploy, or a container back from a restart before its
            // handlers bound: the 404 parks the path, it does not end it.
            // Negative, as System.nanoTime() may be: a park must never be measured against a fixed origin.
            var now = -1_000_000L
            val batches = VespaBatchFeed(VespaHttp(), { mock.url }, batchSize = 100, concurrency = 1, retryAfterMs = 60_000, clock = { now })
            val body = { d: EventDoc -> d.feedJson(includeNear = true, includeEdge = false, includeStem = true) }
            assertTrue(batches.available, "nothing parked yet, whatever the clock reads")
            assertEquals("batch 0 sent", batches.statusLine())
            mock.batchHandlerAbsent = true
            val first = List(150) { doc() }
            assertEquals(first.toSet(), batches.putAll(first, { it.id }, body).toSet(), "every document handed back to the feed client")
            assertFalse(batches.available)
            assertEquals("batch PARKED (404 x1), retry in 60s", batches.statusLine())

            mock.batchHandlerAbsent = false
            now += 59_999
            val second = List(150) { doc() }
            assertEquals(second.toSet(), batches.putAll(second, { it.id }, body).toSet(), "still parked: not tried")
            assertEquals(0, mock.batchRequests)

            now += 1
            assertTrue(batches.available)
            assertEquals(emptyList(), batches.putAll(List(150) { doc() }, { it.id }, body), "the handler took them all")
            assertEquals(2, mock.batchRequests)
            assertEquals("batch 2 sent", batches.statusLine())
        }

    @Test
    fun `a batch the schema refuses lands through the document API, and later batches leave the column out`() =
        runBlocking {
            mock.rejectStemFields = true
            val batched = VespaEventIndex(mock.url, feedBatch = 100)
            try {
                val portuguese = { doc(kind = 1, search = SearchFields(text = "Não perca as notícias de hoje, elas são muito importantes para nós")) }
                val first = List(5) { portuguese() }
                batched.putAll(first)
                for (d in first) assertEquals(d.id, batched.get(d.id)?.id)
                assertEquals(0, mock.batchRequests, "the refused batch was not stored by the handler")
                // The feed client's refusal flipped the stem flag, so the next batch is built without it.
                val second = List(5) { portuguese() }
                batched.putAll(second)
                assertEquals(1, mock.batchRequests)
                for (d in second) assertEquals(d.id, batched.get(d.id)?.id)
            } finally {
                mock.rejectStemFields = false
                batched.close()
            }
        }

    /** A schema missing BOTH fed columns names one per refusal: two demotions, then the write lands. */
    @Test
    fun `a schema missing the near tier and the edge column demotes twice on write`() =
        runBlocking {
            mock.rejectNearFields = true
            mock.rejectEdgeField = true
            val fresh = VespaEventIndex(mock.url, bodyEdgeFeed = true)
            try {
                val both = doc(kind = 30023, search = SearchFields(primary = "Tarantella", text = "tarantellas every night"))
                fresh.put(both)
                assertEquals(both.id, fresh.get(both.id)?.id)
                val conditional = doc(kind = 30023, search = SearchFields(primary = "Second", text = "another body"))
                fresh.putAll(listOf(conditional))
                assertEquals(conditional.id, fresh.get(conditional.id)?.id)
            } finally {
                mock.rejectNearFields = false
                mock.rejectEdgeField = false
                fresh.close()
            }
        }

    /**
     * A PARTIAL ANSWER IS ABOUT THE MOMENT, NOT THE DATA, and must not buy a
     * whole-corpus read. Measured against one predicate on staging: 7.1s of
     * visiting found 4 of 8,765 matches that one indexed query answered
     * completely in 6ms. Trading a fifth of a second of index work for ~18
     * minutes of scanning is a bargain the walk should almost never take.
     */
    @Test
    fun `a transient partial is retried on the index, never handed to the scan`() =
        runBlocking {
            IngestStats.reset()
            val ann = "a7".repeat(32)
            seed(*(1..40).map { doc(kind = 30382, pubkey = ann, at = 1_700_000_000L + it) }.toTypedArray())
            mock.visitRequests = 0
            // One refusal, then the cluster is itself again.
            mock.degradeNextSearches = 1

            val idx = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                idx.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(ann))) {
                    got += it
                    true
                }

                assertEquals(40, got.distinctBy { it.id }.size, "a retried walk must still deliver every id")
                assertEquals(0, mock.visitRequests, "a transient partial must NOT reach the scan")
                assertNotNull(IngestStats.snapshot()["walk.refused.$nonIdealState"], "the refusal must be booked, by flag")
            } finally {
                idx.close()
                IngestStats.reset()
            }
        }

    /**
     * A SCHEMA WITHOUT THE ATTRIBUTE-ONLY ID SUMMARIES STILL SUPERSEDES. The
     * read path demotes a missing `idtime` to the full summary, but the id
     * walk — which the write path now reads its supersession versions through
     * (the default putIfNewer, the bulk version read) — had no such net: every
     * replaceable insert against such a schema threw the engine's 400.
     */
    @Test
    fun `supersession and the id walk survive a schema without the id summaries`() =
        runBlocking {
            mock.rejectIdTimeSummary = true
            val pk = "c5".repeat(32)
            val old = doc(kind = 30023, pubkey = pk, at = 1_700_000_000L, tags = listOf(listOf("d", "post")))
            index.put(old)
            val newer = doc(kind = 30023, pubkey = pk, at = 1_700_000_100L, tags = listOf(listOf("d", "post")))

            assertTrue(index.putIfNewer(newer), "the newer version wins")

            val walked = ArrayList<DocRef>()
            index.visitIds(EventQuery(kinds = listOf(30023), authors = listOf(pk)), withDTag = true) { page ->
                walked += page
                true
            }
            assertEquals(listOf(DocRef(newer.id, newer.createdAt, "post")), walked, "one version left, its d read off the default summary")
        }

    /**
     * THE VERSION CENSUS IS ONE GROUPING: each author's winner (newest second, lowest id) and
     * how many versions it holds, as the in-memory reference reduces them — one version, a
     * drifted pair, a tie at the newest second, and an author with nothing stored.
     */
    @Test
    fun `the version census answers winner and count in one grouping`() =
        runBlocking {
            val one = "e1".repeat(32)
            val drifted = "e2".repeat(32)
            val tied = "e3".repeat(32)
            val none = "e4".repeat(32)
            seed(
                doc(kind = 0, pubkey = one, at = 100),
                doc(kind = 0, pubkey = drifted, at = 100),
                doc(kind = 0, pubkey = drifted, at = 200),
                doc(kind = 0, pubkey = tied, at = 300),
                doc(kind = 0, pubkey = tied, at = 300),
                doc(kind = 0, pubkey = tied, at = 50),
            )
            val authors = listOf(one, drifted, tied, none)
            val before = mock.searchRequests.size

            val census = index.versionCensus(0, authors)

            assertEquals(1, mock.searchRequests.size - before, "one grouping for the chunk")
            assertEquals(reference.versionCensus(0, authors), census)
            assertEquals(listOf(1, 2, 3), listOf(one, drifted, tied).map { census.getValue(it).versions })
            assertEquals(reference.newestVersions(0, authors), index.newestVersions(0, authors), "and the newest-version grouping agrees with it")
        }

    /**
     * AN ADDRESS-KEYED FALLBACK SUPERSESSION KEEPS ITS WINNER. A doc whose id
     * cannot go into the engine's test-and-set condition (not 64 hex) takes
     * the port's read-then-supersede instead, and that put the winner at the
     * address's docid and then removed the replaced version BY its docid —
     * the same address — deleting the winner it had just written.
     */
    @Test
    fun `an address-keyed putIfNewer that falls back to reading keeps its winner`() =
        runBlocking {
            // An engine that stores a replaceable AT its address, as Vespa does under address keying.
            val keyed = MockVespaEngine(InMemoryEventIndex(supersedesViaPut = true))
            val idx = VespaEventIndex(keyed.url, addressKeyed = true)
            try {
                val pk = "c4".repeat(32)
                val old = doc(kind = 0, pubkey = pk, at = 1_700_000_000L)
                idx.put(old)
                val winner = old.copy(id = "not-hex-${old.id}", createdAt = 1_700_000_100L)

                assertTrue(idx.putIfNewer(winner), "the newer version wins")

                assertEquals(listOf(winner.id), idx.search(EventQuery(kinds = listOf(0), authors = listOf(pk))).map { it.id }, "the address holds the winner")
            } finally {
                idx.close()
                keyed.stop()
            }
        }

    /**
     * ADDRESS-KEYED REMOVES RESOLVE THEIR DOCIDS IN ONE QUERY. A replaceable
     * lives at its address, so removing by event id has to read the document
     * to learn where it is — and that read was one `id in ("<id>")` search PER
     * ID, which the expiry sweep paid inside its writer-lock hold (50 expired
     * notes: 57 searches, 50 of them single-id). One `id in (…)` per chunk
     * resolves the whole batch, and each docid still comes off its document.
     */
    @Test
    fun `an address-keyed removeAll resolves its docids in one query per chunk`() =
        runBlocking {
            val idx = VespaEventIndex(mock.url, addressKeyed = true)
            try {
                val notes = (1..50).map { doc(kind = 1, at = 1_700_000_000L + it) }
                val profile = doc(kind = 0, pubkey = "b2".repeat(32), at = 1_700_000_100L)
                seedBulk(notes + profile)
                val before = mock.searchRequests.size

                idx.removeAll((notes + profile).map { it.id })

                assertEquals(1, mock.searchRequests.size - before, "one resolve for 51 ids")
                assertEquals(emptySet(), idx.existingIds(notes.map { it.id }), "every note removed")
                assertTrue("0:${"b2".repeat(32)}:" in mock.removedDocIds, "the replaceable removed at its ADDRESS docid: ${mock.removedDocIds.takeLast(3)}")
            } finally {
                idx.close()
            }
        }

    /**
     * A `complete` WALK still walks. A walk's page is limited by design, while
     * real Vespa's `totalCount` is the whole match set (staging: 2,809,568 on
     * a limit-2,000 page) — so the "every match served" half of `complete`'s
     * check refused every page of any walk over more than one page, and the
     * expiry sweep, which asked for it, never drained. On a walk `complete`
     * means what it can: the engine searched every document (`full`).
     */
    @Test
    fun `a complete walk over many pages delivers every id`() =
        runBlocking {
            val ann = "a7".repeat(32)
            seedBulk((1..300).map { doc(kind = 1, pubkey = ann, at = 1_700_000_000L + it) })

            val idx = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                idx.visitIds(EventQuery(authors = listOf(ann), complete = true)) {
                    got += it
                    true
                }
                assertEquals(300, got.distinctBy { it.id }.size, "every id, across ~30 pages")
            } finally {
                idx.close()
            }
        }

    /** ...while a `complete` walk over a cluster that searched only part of the corpus is still refused. */
    @Test
    fun `a complete walk refuses an answer that is not full`() =
        runBlocking {
            val ann = "a8".repeat(32)
            seedBulk((1..30).map { doc(kind = 1, pubkey = ann, at = 1_700_000_000L + it) })
            // `full: false` at a percentage that rounds to 100, no reason named:
            // the read path serves it; a `complete` read must not.
            mock.roundedCompleteCoverage = true

            val idx = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val refused = runCatching { idx.visitIds(EventQuery(authors = listOf(ann), complete = true)) { true } }.exceptionOrNull()
                assertTrue(refused is IllegalArgumentException && "full: false" in refused.message.orEmpty(), "refused as not full: $refused")
            } finally {
                mock.roundedCompleteCoverage = false
                idx.close()
            }
        }

    /**
     * A REFUSAL THAT DOES NOT CLEAR THROWS — it must not buy the corpus.
     *
     * Falling back to a whole-corpus document-API read was the shape of this
     * code for a long time, and it is backwards: ~17.9 minutes of reading every
     * document, issued against a cluster that has just said it is unwell. What
     * this walk can actually be refused for is a node redistributing or still
     * opening its buckets, and every caller already owns the retry loop that
     * belongs there — the drain leaves a failed unit queued and backs off 5 s,
     * a NIP-77 peer re-syncs, a COUNT's client re-asks.
     *
     * So: ask across the backoff, then refuse in ~4.2 s and let them wait for a
     * healthy engine.
     */
    @Test
    fun `a refusal that never clears throws, and never buys the corpus`() =
        runBlocking {
            IngestStats.reset()
            val ivy = "c8".repeat(32)
            seedBulk((1..40).map { doc(kind = 30382, pubkey = ivy, at = 1_700_000_000L + it) })
            mock.visitRequests = 0
            mock.degradeCoverage = nonIdealState

            val idx = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                // try/catch rather than assertFailsWith: the call is suspend
                // and the assertion helper's block is not.
                val thrown =
                    try {
                        idx.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(ivy))) { true }
                        null
                    } catch (refused: PartialAnswer) {
                        refused
                    }
                assertNotNull(thrown, "a walk that cannot be served must say so, not read the corpus")
                val stages = IngestStats.snapshot()
                assertEquals(0, mock.visitRequests, "the scan is for tie-dense DATA, never for a sick cluster")
                assertEquals(null, stages["walk.scan"], "and it must not be entered at all")
                assertEquals(
                    VespaEventIndex.RETRY_PAUSES_MILLIS.size.toLong(),
                    stages["walk.refused.$nonIdealState"]?.calls,
                    "every ask in the backoff is spent before giving up, and booked by flag: " +
                        "${stages.keys.filter { it.startsWith("walk.") }}",
                )
            } finally {
                mock.degradeCoverage = null
                idx.close()
                IngestStats.reset()
            }
        }

    /**
     * UNDER A WRITER LOCK THE WALK ASKS ONCE. The backoff above is the remedy
     * for a reader; on the write path (supersession reads its versions
     * through this walk) it held every other writer for ~4.2 s per page and
     * per tie read, where those commits used to fail in one round trip and
     * their caller retries anyway.
     */
    @Test
    fun `a refused walk under a writer lock gives up after one ask`() =
        runBlocking {
            IngestStats.reset()
            val ivy = "c9".repeat(32)
            seedBulk((1..5).map { doc(kind = 30382, pubkey = ivy, at = 1_700_000_000L + it) })
            mock.degradeCoverage = nonIdealState
            try {
                val thrown =
                    try {
                        withContext(WriterLockHeld) { index.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(ivy))) { true } }
                        null
                    } catch (refused: PartialAnswer) {
                        refused
                    }
                assertNotNull(thrown, "still refused")
                val stages = IngestStats.snapshot()
                assertEquals(1L, stages["walk.refused.$nonIdealState"]?.calls, "one ask")
                assertEquals(null, stages["walk.retry.wait"], "and no wait")
            } finally {
                mock.degradeCoverage = null
                IngestStats.reset()
            }
        }

    /**
     * THE EXPIRY SWEEP'S WALK, CUT: its scan fallback must be a scan. A
     * selection could not express `expiresBefore`, so a sweep walk that spent
     * its cut budget fell to the UNSELECTABLE path — one unlimited search for
     * every due document, whole, in one list (an OOM on a large backlog, a
     * refusal under a hits cap). `event.expires_at<N` is a selection clause
     * like the not-expired guard's, so it streams.
     */
    @Test
    fun `a cut expiry walk falls to a streamed scan, never the unbounded search`() =
        runBlocking {
            IngestStats.reset()
            val kim = "e7".repeat(32)
            val due = (1..30).map { doc(kind = 1, pubkey = kim, at = 1_700_000_000L + it, tags = listOf(listOf("expiration", "1700000500"))) }
            val later = (1..10).map { doc(kind = 1, pubkey = kim, at = 1_700_000_100L + it, tags = listOf(listOf("expiration", "1800000000"))) }
            val never = (1..10).map { doc(kind = 1, pubkey = kim, at = 1_700_000_200L + it) }
            seedBulk(due + later + never)
            mock.visitRequests = 0
            mock.degradeCoverage = PartialAnswer.MATCH_PHASE

            val idx = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                idx.visitIds(EventQuery(kinds = listOf(1), expiresBefore = 1_700_000_501L)) {
                    got += it
                    true
                }

                assertEquals(due.map { it.id }.toSet(), got.mapTo(HashSet()) { it.id }, "exactly the due documents")
                val stages = IngestStats.snapshot()
                assertEquals(null, stages["walk.scan.unselectable"], "never the unbounded search; stages: ${stages.keys.sorted()}")
                assertEquals(true, mock.visitRequests > 0, "a streamed document-API scan")
            } finally {
                mock.degradeCoverage = null
                idx.close()
                IngestStats.reset()
            }
        }

    /**
     * THE OTHER HALF OF THAT CONTRACT, and the one staging proved was missing.
     *
     * `match-phase` is not a cluster having a moment — it is the ENGINE saying
     * it truncated this match set, and it will say the same thing in four
     * seconds. Measured on staging after the retry-and-throw shipped: an
     * unbounded `unranked` id walk came back at 1% COVERAGE over 2,251,965 of
     * 362M documents with `match-phase: true`, at limit 2,064 and 20,064 alike,
     * while the same shape WINDOWED answered 943,949 matches at 100%. The
     * assumption that this walk cannot be refused for size holds only for a
     * bounded one.
     *
     * So a cut takes the scan — the document-API visit has no match phase
     * because it is not a search — and it does not spend the backoff first.
     * Seven of 146 visits aborted in the three minutes before this was fixed.
     */
    @Test
    fun `a cut that narrowing cannot clear falls to the scan, and never waits`() =
        runBlocking {
            IngestStats.reset()
            val jay = "d9".repeat(32)
            seedBulk((1..40).map { doc(kind = 30382, pubkey = jay, at = 1_700_000_000L + it) })
            mock.visitRequests = 0
            // Cut WHATEVER is asked, however narrow — the one shape narrowing
            // cannot fix, and the reason the halving carries a budget at all.
            mock.degradeCoverage = PartialAnswer.MATCH_PHASE

            val idx = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                idx.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(jay))) {
                    got += it
                    true
                }

                assertEquals(40, got.distinctBy { it.id }.size, "the scan must finish what no window could")
                val stages = IngestStats.snapshot()
                assertEquals(true, mock.visitRequests > 0, "an unclearable cut is exactly what the scan is for")
                assertNotNull(stages["walk.cut.budget"], "and it must arrive by spending the budget, not by recursing forever")
                assertEquals(null, stages["walk.retry.wait"], "nothing sleeps: waiting cannot clear a cut")
                // The BOUND, and the reason it counts splits rather than depth:
                // a depth of six allows 63 splits and 64 leaves, and every leaf
                // that still refuses scans. Counting splits caps the scans at
                // MAX_CUT_NARROWINGS + 1.
                val splits = stages["walk.cut.narrowed"]?.calls ?: 0
                assertEquals(
                    true,
                    splits <= VespaEventIndex.MAX_CUT_NARROWINGS.toLong(),
                    "the fan-out is bounded by splits, not depth; got $splits narrowing(s)",
                )
                // `walk.scan`, NOT `mock.visitRequests`: one scan issues many
                // HTTP requests — a slice each, then continuations — so the
                // request count answers a different question than "how many
                // scans", and answers it in the hundreds.
                val scans = stages["walk.scan"]?.calls ?: 0
                assertEquals(
                    true,
                    scans <= VespaEventIndex.MAX_CUT_NARROWINGS + 1L,
                    "and so at most ${VespaEventIndex.MAX_CUT_NARROWINGS + 1} scans; got $scans",
                )
            } finally {
                mock.degradeCoverage = null
                idx.close()
                IngestStats.reset()
            }
        }

    /**
     * THE WALK CARRIES ITS OWN SORTING POLICY, so the engine has no cut to
     * refuse and the ladder above never starts.
     *
     * Staging, 2026-09-11 to 09-14: every unranked page sorted by
     * `created_at` was cut by Vespa's sorting degrader on a big enough match
     * set, the client refused it, halving cleared most and the rest read the
     * document store — 15 to 19 scans an hour at 17.9 minutes each, which was
     * the proton load on both content nodes. The fix is one parameter on the
     * walk's query (EventYql.SORT_DEGRADING); the mock's [MockVespaEngine.degradeSortedUnranked]
     * cuts exactly the way real Vespa did unless that parameter arrives.
     *
     * Verified to fail with the parameter reverted: the walk then refuses,
     * narrows, spends the budget and scans (`visitRequests` > 0, `walk.scan`
     * booked) — the shape the previous test pins on purpose.
     */
    @Test
    fun `the id walk opts out of the sorting degrader, so no page is cut, narrowed or scanned`() =
        runBlocking {
            IngestStats.reset()
            val kim = "e1".repeat(32)
            seedBulk((1..40).map { doc(kind = 30382, pubkey = kim, at = 1_700_000_000L + it) })
            mock.visitRequests = 0
            mock.degradeSortedUnranked = true

            val idx = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                idx.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(kim))) {
                    got += it
                    true
                }

                assertEquals(40, got.distinctBy { it.id }.size, "every id, through the cursor alone")
                val stages = IngestStats.snapshot()
                assertEquals(0, mock.visitRequests, "nothing read the document store")
                assertEquals(null, stages["walk.scan"], "no scan")
                assertEquals(null, stages["walk.cut.narrowed"], "no narrowing")
                assertEquals(null, stages["walk.cut.budget"], "no budget spent")
                assertEquals(null, stages["walk.refused.${PartialAnswer.MATCH_PHASE}"], "and no refusal to narrow from")
                // ONE page: a cursor page fetches idPageSize + TIE_SLACK, and
                // 40 ids fit inside that, so the whole walk is a single read.
                val pages = assertNotNull(stages["walk.ids.page"], "the cursor did the work: ${stages.keys.sorted()}")
                assertEquals(1L, pages.calls, "forty ids fit one cursor page (idPageSize + TIE_SLACK), so the walk is exactly one read")
            } finally {
                mock.degradeSortedUnranked = false
                idx.close()
                IngestStats.reset()
            }
        }

    /** Vespa's own name for a cluster still settling — the wire value, which is what the client keys on. */
    private val nonIdealState = "non-ideal-state"

    /**
     * AN UNBOUNDED WALK MEANS UNBOUNDED, and this corpus holds events dated
     * past the clock — staging carries notes stamped in the year 2100.
     *
     * The bisection that used to live here lost exactly those: it filled an
     * absent `until` in with the wall clock to compute a midpoint and then
     * handed that number to the newer half as a bound, so a 2100 event matched
     * the caller's query and neither half. The bisection is gone, but the
     * property it broke is worth a test of its own — on the NIP-77 path this id
     * set is what a peer reconciles against, and a silently dropped id is a
     * peer that offers it forever.
     */
    @Test
    fun `an unbounded walk serves ids dated past the clock`() =
        runBlocking {
            val eve = "d7".repeat(32)
            val now = System.currentTimeMillis() / 1000
            val past = (1..40).map { doc(kind = 30382, pubkey = eve, at = now - 100_000 + it) }
            val future = doc(kind = 30382, pubkey = eve, at = 4_102_444_800L) // 2100-01-01
            seedBulk(past + future)

            val idx = VespaEventIndex(mock.url, idPageSize = 10)
            try {
                val got = ArrayList<DocRef>()
                // NO since and NO until — the shape `snapshotIdsForNegentropy`
                // hands down for an uncapped catch-up.
                idx.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(eve))) {
                    got += it
                    true
                }
                assertTrue(got.any { it.id == future.id }, "the event dated past the clock is in the window and must be served")
                assertEquals(41, got.distinctBy { it.id }.size, "every id, exactly once")
            } finally {
                idx.close()
            }
        }

    /**
     * THE SCAN MUST BOOK ITS OWN TIME. It is the most expensive walk here and
     * was the only branch with no stage: a document-API visit is not a query,
     * so it books no engine time against any caller on the pulse either. A walk
     * could read every document in the corpus and look, on every instrument
     * this process has, like a walk doing nothing — which is how ~7M documents
     * matched per second went unattributed on staging while two fixes aimed at
     * the cursor path changed no CPU at all.
     *
     * Reached here the one way a DENSE SECOND still can: its id-prefix split
     * spending the budget. These ids are NOT hashes — [doc] counts them up from
     * `000…1` — so the whole second sits in bucket `0` at every depth, the
     * shape a depth bound would let run sixty-four levels down. The budget is
     * reads, shared: sixteen levels of sixteen, then what is left goes to the
     * scan, which must be ONE scan of the second, filtered to what the split
     * did not serve.
     *
     * Pinned so the timer cannot be dropped the way it was never added.
     */
    @Test
    fun `what the prefix split cannot reach is one scan, booked as walk-scan`() =
        runBlocking {
            IngestStats.reset()
            val ida = "d4".repeat(32)
            seedBulk((1..200).map { doc(kind = 30382, pubkey = ida, at = 1_700_000_000L) })
            val scanning = VespaEventIndex(mock.url, idPageSize = 1)
            try {
                val got = ArrayList<DocRef>()
                scanning.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(ida))) { page ->
                    got += page
                    true
                }
                assertEquals(200, got.distinctBy { it.id }.size, "the scan must still deliver every match")
                assertEquals(200, got.size, "and each exactly once: a bucket read AND scanned would arrive twice")

                val stages = IngestStats.snapshot()
                val scan = assertNotNull(stages["walk.scan"], "the scan booked no stage — it is invisible again; stages seen: ${stages.keys.sorted()}")
                assertEquals(1L, scan.calls, "whatever the budget could not split shares ONE scan of the second")
                assertNotNull(stages["walk.scan.page"], "the scan's pages must be counted too")
                assertNotNull(stages["walk.ties.dense"], "a group too wide for the cursor must say so")
                assertNotNull(stages["walk.ties.prefix.budget"], "and the scan must arrive by spending the split's budget")
                assertEquals(
                    VespaEventIndex.MAX_TIE_PREFIX_QUERIES.toLong(),
                    stages["walk.ties.prefix"]?.calls,
                    "the budget is READS across the whole split, not a depth",
                )
                // ONE cursor page, and no more. The routing is discovered, not
                // predicted, so the walk necessarily reads the page that shows
                // it the boundary — and then resolves the second rather than
                // paging on. That one page IS the cost the probe used to be.
                assertEquals(1L, stages["walk.ids.page"]?.calls, "discovery costs exactly one cursor page")
                assertNotNull(stages["walk.ids.tiegroup"], "and the bounded boundary read that measured the group")
            } finally {
                scanning.close()
                IngestStats.reset()
            }
        }

    /**
     * WORDS TYPED WITH AN IME SPACE ARE TWO WORDS, on the wire and in the spec
     * alike. `ビットコイン　価格` (U+3000 between them, what a Japanese IME types)
     * was ONE word to Java's ASCII `\\s`: the reference then required the
     * literal U+3000-joined run and the engine was sent one term holding both,
     * so a note carrying both words apart was missed by each. Verified to fail
     * with WHITESPACE back on `\\s+`.
     */
    @Test
    fun `a search split by an ideographic space requires each word, not the run`() =
        runBlocking {
            val both = doc(kind = 1, content = "価格が上がった。ビットコインは強い", search = SearchFields(text = "価格が上がった。ビットコインは強い"))
            val one = doc(kind = 1, content = "ビットコインの話", search = SearchFields(text = "ビットコインの話"))
            seed(both, one)
            val q = EventQuery(kinds = listOf(1), search = "ビットコイン\u3000価格")
            assertEquals(listOf(both.id), reference.search(q).map { it.id }, "the spec: both words, anywhere")
            check(q)
        }

    /**
     * A DEPLOYMENT THAT CAPS `maxHits` STILL WALKS, and walks exactly. The
     * cursor's page asked for `idPageSize + TIE_SLACK` hits (20,064 by default)
     * whatever VESPA_UNBOUNDED_HITS said, and the limited walk's one-page read
     * `limit + TIE_SLACK`, while the tie-group read beside them was already
     * clamped — so under a cap below the overfetch every page was refused 400
     * and nothing fell back. The cap here sits below the page size itself, and
     * ties straddle the shrunken page boundaries in a scrambled engine order,
     * so a clamp that broke the boundary resolution would lose or repeat ids.
     *
     * Verified to fail with the clamps reverted: the first page is refused
     * ("124 hits requested, configured limit: 50.").
     */
    @Test
    fun `a walk under a deployed hits cap pages inside it and stays exact`() =
        runBlocking {
            val ivy = "d9".repeat(32)
            // Seconds of 1..13 events each, and one 30-wide second, so ties land
            // on boundaries of every page size the clamp can produce.
            val docs = (0 until 240).map { doc(kind = 1, pubkey = ivy, at = 1_700_000_000L + (it * it) % 97) } + (1..30).map { doc(kind = 1, pubkey = ivy, at = 1_700_000_050L) }
            seedBulk(docs)
            mock.refuseHitsAbove = 50
            mock.scrambleTieOrder = true
            val capped = VespaEventIndex(mock.url, tuning = ClientTuning(unboundedHits = 50), idPageSize = 60)
            try {
                val q = EventQuery(kinds = listOf(1), authors = listOf(ivy))
                val all = ArrayList<DocRef>()
                capped.visitIds(q) {
                    all += it
                    true
                }
                assertEquals(docs.size, all.size, "every id, each exactly once")
                assertEquals(docs.map { it.id }.toSet(), all.map { it.id }.toSet())
                // A limit inside the cap takes the one-page read; one past the cap
                // but inside a page must not ask for itself in one query either.
                for (limit in listOf(20, 55, 130)) {
                    val got = ArrayList<DocRef>()
                    capped.visitIds(q.copy(limit = limit)) {
                        got += it
                        true
                    }
                    assertEquals(
                        reference.search(q.copy(limit = limit)).map { it.id }.sorted(),
                        got.map { it.id }.sorted(),
                        "limit $limit: the newest N with NIP-01's lowest-id tiebreak",
                    )
                }
                assertTrue(mock.searchRequests.all { (it["hits"]?.toInt() ?: 0) <= 50 }, "no request may ask past the deployed cap")
            } finally {
                mock.refuseHitsAbove = null
                mock.scrambleTieOrder = false
                capped.close()
            }
        }

    /**
     * THE REQ PATH UNDER THE SAME CAP. A recency read asked for `limit +
     * TIE_SLACK` hits however close the limit sat to a deployed `maxHits`, so a
     * REQ the cap admits (limit 48 under a cap of 48) was refused 400 for this
     * client's own overfetch. The slack now stops at the cap, and the page must
     * stay NIP-01's: ties straddle every boundary below, served in a scrambled
     * engine order, so a clamp that trusted a slack-less answer would keep the
     * engine's arbitrary members instead of the lowest ids. Both the
     * speculative default and the shipped match-phase plan are read.
     *
     * Verified to fail with the clamp reverted: "76 hits requested, configured
     * limit: 48." on the first limit; with only the `>=` reverted to `>`, the
     * limit AT the cap serves the engine's arbitrary boundary members.
     */
    @Test
    fun `a recency read under a deployed hits cap stays inside it and stays exact`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            val uma = "da".repeat(32)
            // Five events a second, so every limit below ends inside a tie group.
            val docs = (0 until 150).map { doc(kind = 1, pubkey = uma, at = now - 10 - it / 5) }
            seedBulk(docs)
            mock.refuseHitsAbove = 48
            mock.scrambleTieOrder = true
            try {
                for (strategy in listOf(RecencyStrategy.SPECULATIVE, RecencyStrategy.MATCH_PHASE)) {
                    VespaEventIndex(mock.url, tuning = ClientTuning(unboundedHits = 48), recencyStrategy = strategy, recencyMemory = false).use { capped ->
                        for (limit in listOf(12, 29, 43, 48)) {
                            val q = EventQuery(kinds = listOf(1), authors = listOf(uma), limit = limit, nowSecs = now)
                            assertEquals(reference.search(q).map { it.id }, capped.search(q).map { it.id }, "$strategy, limit $limit: NIP-01's page, lowest ids at the boundary")
                        }
                    }
                }
                assertTrue(mock.searchRequests.all { (it["hits"]?.toInt() ?: 0) <= 48 }, "no request may ask past the deployed cap")
            } finally {
                mock.refuseHitsAbove = null
                mock.scrambleTieOrder = false
            }
        }

    /** A walk the observer gates: the lens [EventYql.gateFloor] reads, on the recency-gated profile a lensed termless read takes. */
    private fun gated(q: EventQuery) = q.copy(observer = "0b".repeat(32), rankKey = "0c".repeat(32), minRank = 10.0, ranking = EventYql.RANK_RECENCY_GATED)

    /**
     * A GATED WALK'S LAST RESORT GATES TOO. The document-API scan is not a
     * search, so it has no rank profile and no drop-limit: a selection built
     * from a lensed query streamed every author the lens drops. The walk reached
     * it on exactly the shape a provider's cards have — one dense second, its
     * id-prefix split out of budget — so a NIP-77 snapshot declared through an
     * observer handed over the below-floor authors in that second.
     *
     * Verified to fail with EventSelection's gate check removed: the scan
     * serves the below-floor author's 20 ids alongside the trusted 200.
     */
    @Test
    fun `a gated walk that runs out of prefix budget never scans the gate away`() =
        runBlocking {
            IngestStats.reset()
            val trusted = "d5".repeat(32)
            val spam = "d6".repeat(32)
            seedBulk((1..200).map { doc(kind = 30382, pubkey = trusted, at = 1_700_000_000L) } + (1..20).map { doc(kind = 30382, pubkey = spam, at = 1_700_000_000L) })
            mock.belowFloorAuthors = setOf(spam)
            mock.visitRequests = 0
            val walking = VespaEventIndex(mock.url, idPageSize = 1)
            try {
                val got = ArrayList<DocRef>()
                walking.visitIds(gated(EventQuery(kinds = listOf(30382), authors = listOf(trusted, spam)))) { page ->
                    got += page
                    true
                }
                val stages = IngestStats.snapshot()
                assertNotNull(stages["walk.ties.prefix.budget"], "the walk must reach the last resort this pins; stages seen: ${stages.keys.sorted()}")
                val spamIds = reference.search(EventQuery(authors = listOf(spam))).map { it.id }.toSet()
                assertEquals(emptyList(), got.filter { it.id in spamIds }.map { it.id }, "a below-floor author's ids left a gated walk")
                assertEquals(200, got.distinctBy { it.id }.size, "and every trusted id still arrives")
                assertEquals(200, got.size, "each exactly once")
                assertEquals(0, mock.visitRequests, "a selection cannot carry the gate, so a gated walk never visits")
            } finally {
                mock.belowFloorAuthors = emptySet()
                walking.close()
                IngestStats.reset()
            }
        }

    /** The same gate on the other selection-backed reads that a search would gate: the tags projection and the reindex page. */
    @Test
    fun `the selection-backed projections of a gated query keep the gate`() =
        runBlocking {
            val trusted = "d7".repeat(32)
            val spam = "d8".repeat(32)
            seed(doc(kind = 10002, pubkey = trusted, tags = listOf(listOf("r", "wss://kept.example/"))), doc(kind = 10002, pubkey = spam, tags = listOf(listOf("r", "wss://gated.example/"))))
            mock.belowFloorAuthors = setOf(spam)
            mock.visitRequests = 0
            try {
                val q = gated(EventQuery(kinds = listOf(10002)))
                val tags = ArrayList<List<List<String>>>()
                index.visitTags(q) {
                    tags += it
                    true
                }
                assertEquals(listOf(listOf(listOf("r", "wss://kept.example/"))), tags, "visitTags")
                assertEquals(listOf(trusted), index.visitDocsPage(q, null, 10).docs.map { it.pubkey }, "visitDocsPage")
                assertEquals(0, mock.visitRequests, "neither may take the ungated visit")
            } finally {
                mock.belowFloorAuthors = emptySet()
            }
        }

    /**
     * THE PAGE SIZE IS THE WALK'S COST, INVERTED. The engine matches every
     * document in the cursor's remaining range per page and the limit does not
     * touch that, so the pages a walk issues — not the ids it returns — are
     * what it is billed for. Pinned because the relationship is the only reason
     * `PAGE_IDS` is the number it is: on staging this walk was 79.3% of all
     * engine time, and lowering the constant would put it back.
     */
    @Test
    fun `a wider page walks the same ids in proportionally fewer requests`() =
        runBlocking {
            val zoe = "e5".repeat(32)
            seed(*(1..200).map { doc(kind = 30382, pubkey = zoe) }.toTypedArray())
            val q = EventQuery(kinds = listOf(30382), authors = listOf(zoe))

            fun pagesAt(size: Int): Int =
                runBlocking {
                    VespaEventIndex(mock.url, idPageSize = size).use { idx ->
                        var pages = 0
                        val ids = HashSet<String>()
                        idx.visitIds(q) { page ->
                            pages++
                            ids += page.map { it.id }
                            true
                        }
                        assertEquals(200, ids.size, "every id must arrive whatever the page size")
                        pages
                    }
                }

            val narrow = pagesAt(20)
            val wide = pagesAt(100)
            // Not the exact 5x the page sizes suggest: TIE_SLACK's fixed overfetch is wide next
            // to a 20-id page, so the walk's last page carries the whole remainder. The direction
            // and the order of magnitude are the property; the constant is not.
            assertEquals(true, narrow > wide, "a 20-id page must take more requests than a 100-id page, got $narrow and $wide")
            assertEquals(
                true,
                narrow >= wide * 3,
                "five times the page must be within reach of a fifth of the requests — " +
                    "got $narrow at 20/page and $wide at 100/page",
            )
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

    /**
     * A LIMIT THAT CUTS A TIE GROUP TAKES ITS LOWEST IDS — NIP-01's
     * `created_at desc, id asc`, the page a read serves. The walk orders by
     * created_at alone, so the members of a group straddling the Nth position
     * were whichever the engine sorted first, and a NIP-77 snapshot of a
     * limit'd filter disagreed with its own REQ at exactly that seam. Both
     * limit'd paths: one overfetched page, and the budgeted cursor.
     */
    @Test
    fun `a limit'd walk cutting a tie group keeps the group's lowest ids`() =
        runBlocking {
            val bob = "b5".repeat(32)
            seed(*(1..8).map { doc(kind = 30382, pubkey = bob, at = (7_000 + it).toLong()) }.toTypedArray())
            seed(*(1..30).map { doc(kind = 30382, pubkey = bob, at = 6_000L) }.toTypedArray())
            val all = reference.search(EventQuery(kinds = listOf(30382), authors = listOf(bob))).map { DocRef(it.id, it.createdAt) }
            val nip01 = all.sortedWith(compareByDescending<DocRef> { it.createdAt }.thenBy { it.id })
            // Real Vespa's single-key sort picks tie MEMBERSHIP arbitrarily; the
            // mock's own order would hide the bug, so flip ids within each tie.
            mock.scrambleTieOrder = true
            try {
                limitCases(bob, nip01)
            } finally {
                mock.scrambleTieOrder = false
            }
        }

    /**
     * THE SAME PROMISE PAST THE DENSE CAP. A limited walk used to read a tied
     * second to `idPageSize * TIE_DENSE_FACTOR` and keep the ENGINE's members
     * of it, which on real Vespa are whichever its single-key sort reached
     * first — so a NIP-77 snapshot of a limit'd filter over a provider's run
     * disagreed with its own REQ. The id-prefix split hands a second out
     * lowest ids first, so a budget that stops inside it keeps exactly
     * NIP-01's members. Both limited paths: one overfetched page (limit 15
     * inside 20-id pages) and the budgeted cursor (limit 40 past 10-id pages),
     * each against a 300-id second over its cap.
     */
    @Test
    fun `a limit'd walk cutting a dense second keeps its lowest ids`() =
        runBlocking {
            val bob = "b6".repeat(32)
            seed(*(1..8).map { doc(kind = 30382, pubkey = bob, at = (7_000 + it).toLong()) }.toTypedArray())
            seedBulk((1..300).map { hashedDoc(kind = 30382, pubkey = bob, at = 6_000L) })
            val all = reference.search(EventQuery(kinds = listOf(30382), authors = listOf(bob))).map { DocRef(it.id, it.createdAt) }
            val nip01 = all.sortedWith(compareByDescending<DocRef> { it.createdAt }.thenBy { it.id })
            mock.scrambleTieOrder = true
            try {
                for ((pageSize, limit) in listOf(20 to 15, 10 to 40)) {
                    VespaEventIndex(mock.url, idPageSize = pageSize).use { paged ->
                        val got = ArrayList<DocRef>()
                        paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob), limit = limit)) {
                            got += it
                            true
                        }
                        assertEquals(nip01.take(limit).map { it.id }.toSet(), got.map { it.id }.toSet(), "limit $limit on $pageSize-id pages: the newest, lowest-id first")
                    }
                }
            } finally {
                mock.scrambleTieOrder = false
            }
        }

    private suspend fun limitCases(
        bob: String,
        nip01: List<DocRef>,
    ) {
        for ((pageSize, limit) in listOf(100 to 20, 10 to 25)) {
            val paged = VespaEventIndex(mock.url, idPageSize = pageSize)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob), limit = limit)) {
                    got += it
                    true
                }
                assertEquals(nip01.take(limit).map { it.id }.toSet(), got.map { it.id }.toSet(), "limit $limit on $pageSize-id pages: the newest, lowest-id first")
            } finally {
                paged.close()
            }
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

    // ---- the cursor's slices (#209) ------------------------------------------

    /** What one walk returned, and what its `select id, created_at` queries cost the mock engine. */
    private class Walked(
        val ids: List<DocRef>,
        /** Per query, in order: documents matched (before the limit) and hits served. */
        val costs: List<Pair<Int, Int>>,
    ) {
        val matched: Long get() = costs.sumOf { it.first.toLong() }
        val served: Long get() = costs.sumOf { it.second.toLong() }
        val ratio: Double get() = matched.toDouble() / maxOf(1L, served)
    }

    private suspend fun walked(
        idx: VespaEventIndex,
        q: EventQuery,
    ): Walked {
        val before = mock.matchCounts.size
        val ids = ArrayList<DocRef>()
        idx.visitIds(q) { page ->
            ids += page
            true
        }
        val costs =
            synchronized(mock.matchCounts) { mock.matchCounts.drop(before) }
                .filter { (yql, _, _) -> yql.startsWith("select id, created_at") }
                .map { (_, matched, served) -> matched to served }
        return Walked(ids, costs)
    }

    /** NIP-01's order over everything [q] matches: `created_at desc, id asc`, the order a walk hands ids out in. */
    private fun nip01(q: EventQuery): List<String> =
        runBlocking { reference.search(q.copy(limit = null)) }
            .sortedWith(compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id })
            .map { it.id }

    /**
     * The bar every slice shape is held to: the walk serves EXACTLY the ids NIP-01 orders, in
     * that order, open and closed at both ends and under budgets of a page and several; and an
     * unlimited walk spends at most [maxRatio] documents matched per id served.
     */
    private suspend fun assertWalksExactly(
        name: String,
        q: EventQuery,
        pageSize: Int,
        maxRatio: Double,
    ): Walked {
        val expected = nip01(q)
        check(expected.size > 3 * pageSize) { "$name: a multi-page window needs more than three pages of ids" }
        return VespaEventIndex(mock.url, idPageSize = pageSize).use { idx ->
            val open = walked(idx, q)
            assertEquals(expected, open.ids.map { it.id }, "$name: the same ids in NIP-01 order")
            assertTrue(
                open.ratio <= maxRatio,
                "$name: matched ÷ served must stay near one page per query, got ${"%.2f".format(open.ratio)} " +
                    "over ${open.costs.size} queries: ${open.costs}",
            )
            // The window the NIP-77 mirror sends, `kinds,since,until`, closed at both ends.
            val first = expected.size / 7
            val last = expected.size - expected.size / 5
            val docs = runBlocking { reference.search(q.copy(limit = null)) }.associateBy { it.id }
            val until = docs.getValue(expected[first]).createdAt
            val since = docs.getValue(expected[last]).createdAt
            val closed = q.copy(since = since, until = until)
            assertEquals(nip01(closed), walked(idx, closed).ids.map { it.id }, "$name: a window closed at both ends")
            // Budgets inside one page, a few pages, and most of the window.
            for (limit in listOf(pageSize + 7, 3 * pageSize + 1, expected.size - 3)) {
                assertEquals(expected.take(limit), walked(idx, q.copy(limit = limit)).ids.map { it.id }, "$name: the newest $limit, lowest ids first")
            }
            open
        }
    }

    /**
     * EACH PAGE MATCHES ABOUT A PAGE, NOT THE REST OF THE WINDOW (#209).
     *
     * A page is `order by created_at desc limit N`, so the engine matches and sorts everything
     * the query's range admits. A cursor that lowered only `until` made page k of an N-page walk
     * match the N - k + 1 pages left: ~N(N+1)/2 pages matched to serve N. On staging that was
     * 15.8 documents matched per id served, and 91.5% of the sync's engine and summary time
     * (2026-11-17, `engineByCaller`, Snapshot / unranked / `kinds,since,until`).
     *
     * Fifty pages of one id a second, measured here: the open cursor matched 25,380 documents
     * over 47 queries to serve these 1,000 ids (6.44 per hit served — the mock counts the
     * 64-row tie overfetch as served, as Vespa does). With slices: 1,980 over 60 queries (1.86).
     * The first page stays open and still matches the window once; every later query matched
     * about one page.
     *
     * Verified to fail with the slices reverted (the cursor's `since` back at the window's).
     */
    @Test
    fun `a multi-page walk matches about a page per query, not the rest of the window`() =
        runBlocking {
            val ann = "c1".repeat(32)
            seedBulk((0 until 1_000).map { doc(kind = 30382, pubkey = ann, at = 1_700_000_000L + it) })
            val q = EventQuery(kinds = listOf(30382), authors = listOf(ann))
            val walk = assertWalksExactly("uniform", q, pageSize = 20, maxRatio = 2.5)
            val fetch = 20 + VespaEventIndex.TIE_SLACK
            val widest = walk.costs.drop(1).maxOf { it.first }
            assertTrue(widest <= 2 * fetch, "after the first page, no query may match more than two pages' fetch; the widest matched $widest")
        }

    /**
     * BURSTS AND SILENCES. A slice is sized from the density the page before it saw, so the
     * shapes that test the estimate are the ones whose density changes: bursts of a few seconds
     * between gaps of an hour to two days, 400 ids on 76 seconds (3 to 6 a second) whose ties
     * straddle slice and page edges in a scrambled engine order, and a window with three years of nothing in the middle.
     * An empty slice must not end the walk — only a short one that reached the window's bottom
     * does — and a run of them must be crossed in a few queries, not one per page width.
     *
     * Measured here, matched per hit served with slices: bursty 1.99, ties 1.69, empty years
     * 2.07 (in 50 queries for ~33 pages). Verified to fail with the slices reverted: the first
     * shape, bursty, then matches 8.44 per hit served.
     */
    @Test
    fun `bursts, ties and empty years walk the same ids in order, a page per query`() =
        runBlocking {
            val rng = Random(209)
            val bursty = "c2".repeat(32)
            var at = 1_700_000_000L
            val bursts =
                (1..40).flatMap {
                    at -= rng.nextLong(3_600, 172_800)
                    val start = at
                    (1..rng.nextInt(5, 60)).map { doc(kind = 30382, pubkey = bursty, at = start - rng.nextLong(0, 4)) }
                }
            val tied = "c3".repeat(32)
            val ties = (0 until 400).map { doc(kind = 30382, pubkey = tied, at = 1_700_000_000L - (it * it) % 151) }
            val gapped = "c4".repeat(32)
            val years = 3 * 365 * 86_400L
            val gaps =
                (0 until 300).map { doc(kind = 30382, pubkey = gapped, at = 1_700_000_000L - it) } +
                    (0 until 300).map { doc(kind = 30382, pubkey = gapped, at = 1_700_000_000L - years - it) } +
                    (0 until 50).map { doc(kind = 30382, pubkey = gapped, at = 1_700_000_000L - 2 * years - 7 * it) }
            seedBulk(bursts + ties + gaps)
            mock.scrambleTieOrder = true
            try {
                assertWalksExactly("bursty", EventQuery(kinds = listOf(30382), authors = listOf(bursty)), pageSize = 20, maxRatio = 3.5)
                assertWalksExactly("ties", EventQuery(kinds = listOf(30382), authors = listOf(tied)), pageSize = 20, maxRatio = 3.5)
                val gapWalk = assertWalksExactly("empty years", EventQuery(kinds = listOf(30382), authors = listOf(gapped)), pageSize = 20, maxRatio = 3.5)
                // 650 ids are ~33 pages; the silences may add a handful of empty slices each.
                assertTrue(gapWalk.costs.size <= 60, "two empty years and an open bottom must cost a few queries each, got ${gapWalk.costs.size}")
            } finally {
                mock.scrambleTieOrder = false
            }
        }

    /**
     * A DENSE SECOND INSIDE A SLICE, AND A WINDOW THAT IS ONE SECOND. A second wider than
     * `idPageSize * TIE_DENSE_FACTOR` is read by id prefix (#185); slicing must hand it to that
     * split exactly as the open cursor did, wherever a slice edge lands next to it. A window of a
     * single second is the narrowest a slice can be, and must not loop or step past itself.
     *
     * Verified to fail with the slices reverted: 5.21 matched per hit served (1.82 with them).
     */
    @Test
    fun `dense seconds and a one-second window walk exactly through the slices`() =
        runBlocking {
            val dee = "c5".repeat(32)
            val singles = (0 until 600).map { doc(kind = 30382, pubkey = dee, at = 1_700_000_000L - 3 * it) }
            // Twenty-id pages carry a tie of up to 80; these are 120 and 150, one mid-window and
            // one on the second right below a single, where a slice edge is likely to sit.
            val dense = (1..120).map { hashedDoc(kind = 30382, pubkey = dee, at = 1_699_999_400L) } + (1..150).map { hashedDoc(kind = 30382, pubkey = dee, at = 1_699_998_799L) }
            seedBulk(singles + dense)
            IngestStats.reset()
            try {
                val q = EventQuery(kinds = listOf(30382), authors = listOf(dee))
                assertWalksExactly("dense", q, pageSize = 20, maxRatio = 3.5)
                assertNotNull(IngestStats.snapshot()["walk.ties.dense"], "the dense seconds must have gone through the id-prefix split")
                VespaEventIndex(mock.url, idPageSize = 20).use { idx ->
                    for (second in listOf(1_699_999_400L, 1_699_998_799L, 1_700_000_000L, 1_700_000_001L)) {
                        val one = q.copy(since = second, until = second)
                        assertEquals(nip01(one), walked(idx, one).ids.map { it.id }, "the one-second window at $second")
                    }
                }
            } finally {
                IngestStats.reset()
            }
        }

    /**
     * A CUT INSIDE A SLICE HALVES THE SLICE, not the window. The first page asked for the window
     * and was answered; the cut lands on a later, bounded page, so what was too much to ask is
     * that slice. Its newer half re-enters the cursor, its older half takes everything below,
     * and one narrowing is spent from the shared budget. No scan: halving cleared it.
     *
     * Verified to fail with the slices reverted: no query of the walk is a slice to cut.
     */
    @Test
    fun `a match-phase cut inside a slice halves the slice and keeps every id`() =
        runBlocking {
            val cy = "c6".repeat(32)
            seedBulk((0 until 1_000).map { doc(kind = 30382, pubkey = cy, at = 1_700_000_000L - it) })
            val q = EventQuery(kinds = listOf(30382), authors = listOf(cy))
            // The window is open at both ends, so the first query bounded on both sides and wider
            // than one second (a tie read is one second) is a slice. Cut it, once.
            val range = Regex("created_at >= (-?\\d+) and created_at <= (-?\\d+)")
            var cuts = 0
            mock.cutWhere = { yql ->
                val bounds = range.find(yql)?.groupValues
                (cuts == 0 && bounds != null && bounds[1] != bounds[2]).also { if (it) cuts++ }
            }
            IngestStats.reset()
            try {
                VespaEventIndex(mock.url, idPageSize = 20).use { idx ->
                    val walk = walked(idx, q)
                    assertEquals(1, cuts, "the cut must have landed on a slice: ${walk.costs}")
                    // The older half starts on a slice at the density the cut was asked at. Asked
                    // open, it matched the whole rest of the window again (verified: 900+ here).
                    val fetch = 20 + VespaEventIndex.TIE_SLACK
                    val widest = walk.costs.drop(1).maxOf { it.first }
                    assertTrue(widest <= 2 * fetch, "no query after the first may match the rest of the window; the widest matched $widest: ${walk.costs}")
                    assertEquals(nip01(q), walk.ids.map { it.id }, "every id, in order, across the halves")
                }
                val stages = IngestStats.snapshot()
                assertEquals(1L, stages["walk.cut.narrowed"]?.calls, "one halving, from the shared budget")
                assertNull(stages["walk.scan"], "halving cleared it: no scan")
                assertEquals(0, mock.visitRequests)
            } finally {
                mock.cutWhere = null
                IngestStats.reset()
            }
        }

    /**
     * A CLUSTER STILL SETTLING, MID-WALK. A `non-ideal-state` refusal on a slice is the same
     * moment it was on an open page: asked again across the backoff, never narrowed, never
     * scanned — and the walk then carries on from the slice it was refused on.
     *
     * Verified to fail with the slices reverted: the refused query is open at the bottom.
     */
    @Test
    fun `a non-ideal-state refusal on a slice is retried, and the walk carries on`() =
        runBlocking {
            val di = "c7".repeat(32)
            seedBulk((0 until 300).map { doc(kind = 30382, pubkey = di, at = 1_700_000_000L - it) })
            val q = EventQuery(kinds = listOf(30382), authors = listOf(di))
            IngestStats.reset()
            try {
                VespaEventIndex(mock.url, idPageSize = 20).use { idx ->
                    val got = ArrayList<DocRef>()
                    var pages = 0
                    val before = mock.searchRequests.size
                    idx.visitIds(q) { page ->
                        got += page
                        // After the first (open) page, refuse the next answer once: a slice.
                        if (++pages == 1) mock.degradeNextSearches = 1
                        true
                    }
                    assertEquals(nip01(q), got.map { it.id })
                    // The query refused and the one asked again after it: the same slice, bounded below.
                    val (refused, retried) =
                        mock.searchRequests
                            .drop(before)
                            .drop(1)
                            .take(2)
                            .map { it.getValue("yql") }
                    assertEquals(refused, retried, "the retry asks the same slice again")
                    assertTrue("created_at >= " in refused, "the refusal must have landed on a slice: $refused")
                }
                val stages = IngestStats.snapshot()
                assertEquals(1L, stages["walk.refused.$nonIdealState"]?.calls, "the refusal is booked once, then the ask after it answers")
                assertNull(stages["walk.cut.narrowed"], "a cluster state is waited out, not narrowed")
                assertEquals(0, mock.visitRequests)
            } finally {
                IngestStats.reset()
            }
        }

    /**
     * A GATED WALK GATES ON EVERY SLICE. The lens rides the query, and a slice is the same query
     * with a different `since`: each page must still be asked on the gated exact profile, and a
     * below-floor author's ids must still never leave.
     *
     * Verified to fail with the slices reverted: the pages after the first are not slices.
     */
    @Test
    fun `a gated walk keeps its gate on every slice`() =
        runBlocking {
            val trusted = "c8".repeat(32)
            val spam = "c9".repeat(32)
            seedBulk(
                (0 until 300).map { doc(kind = 30382, pubkey = trusted, at = 1_700_000_000L - 2 * it) } +
                    (0 until 300).map { doc(kind = 30382, pubkey = spam, at = 1_700_000_000L - 2 * it - 1) },
            )
            mock.belowFloorAuthors = setOf(spam)
            try {
                VespaEventIndex(mock.url, idPageSize = 20).use { idx ->
                    val before = mock.searchRequests.size
                    val walk = walked(idx, gated(EventQuery(kinds = listOf(30382), authors = listOf(trusted, spam))))
                    assertEquals(nip01(EventQuery(kinds = listOf(30382), authors = listOf(trusted))), walk.ids.map { it.id }, "exactly the trusted author's ids, in order")
                    val asked = mock.searchRequests.drop(before)
                    val rankings = asked.map { it["ranking"] }
                    assertTrue(rankings.size > 3, "a multi-slice walk: $rankings")
                    // Between the open first page and the last — which a window with no `since` asks
                    // open at the bottom again, once a slice would reach the epoch — every page is a slice.
                    assertTrue(
                        asked.drop(1).dropLast(1).all { "created_at >= " in it.getValue("yql") },
                        "every page after the first is a slice: ${asked.map { it.getValue("yql").substringAfter(" where ") }}",
                    )
                    assertEquals(setOf(EventYql.RANK_RECENCY_GATED_EXACT), rankings.toSet(), "every slice on the gated exact profile")
                    // The engine MATCHES the gated-out author too and drops it in the rank phase, so
                    // the first page's cost is both authors' 600, not the 300 it serves from.
                    assertEquals(600, walk.costs.first().first, "matched counts what the predicate matched, before the gate")
                }
            } finally {
                mock.belowFloorAuthors = emptySet()
            }
        }

    /**
     * A WINDOW WITH NO `since` HAS NO BOTTOM, and a cut must not invent one. A cut's halving
     * needs a number for the window's floor and takes the epoch; the older half then carried that
     * 0 as its `since`, and every event dated before 1970 — this corpus is dirty enough to hold
     * them — left the NIP-77 snapshot silently. The older half keeps the window's own `since`.
     *
     * Verified to fail with the older half bounded at `floor`: the pre-1970 ids are missing.
     */
    @Test
    fun `a cut on a walk with no since keeps events dated before 1970`() =
        runBlocking {
            val old = "d1".repeat(32)
            val recent = (0 until 100).map { doc(kind = 30382, pubkey = old, at = 1_700_000_000L - it) }
            val prehistoric = (1..5).map { doc(kind = 30382, pubkey = old, at = -1_000L * it) }
            seedBulk(recent + prehistoric)
            var cuts = 0
            // The first query, open at both ends: cut it once.
            mock.cutWhere = { yql -> (cuts == 0 && yql.startsWith("select id, created_at") && "created_at" !in yql.substringAfter(" where ").substringBefore(" order by")).also { if (it) cuts++ } }
            try {
                VespaEventIndex(mock.url, idPageSize = 20).use { idx ->
                    val q = EventQuery(kinds = listOf(30382), authors = listOf(old))
                    val got = walked(idx, q).ids.map { it.id }
                    assertEquals(1, cuts, "the first page must have been cut")
                    assertTrue(prehistoric.all { it.id in got }, "the events dated before 1970 must survive the halving")
                    assertEquals(nip01(q), got, "every id, in order")
                }
            } finally {
                mock.cutWhere = null
            }
        }

    /**
     * A TIED SECOND IS A POINT, NOT A RATE. A page whose older half was one tied second used to
     * size the next slice at two seconds, and the walk then widened through near-empty slices to
     * get back to the data's real density — after every dense second of a provider's run. The
     * estimate now reads the rows above the boundary second.
     *
     * Verified to fail with the estimate back on the page's older half: empty slices follow the
     * dense second.
     */
    @Test
    fun `a dense second does not shrink the slice after it`() =
        runBlocking {
            val ty = "d2".repeat(32)
            // One id every 10 s, so a 20-id page spans ~200 s, and a 100-id second 5 s below the
            // newest: the first page is that one id and then the second, which fills its older half.
            val singles = (0 until 300).map { doc(kind = 30382, pubkey = ty, at = 1_700_000_000L - 10 * it) }
            val dense = (1..100).map { hashedDoc(kind = 30382, pubkey = ty, at = 1_700_000_000L - 5) }
            seedBulk(singles + dense)
            val q = EventQuery(kinds = listOf(30382), authors = listOf(ty), since = 1_700_000_000L - 2_990)
            IngestStats.reset()
            try {
                VespaEventIndex(mock.url, idPageSize = 20).use { idx ->
                    assertEquals(nip01(q), walked(idx, q).ids.map { it.id })
                }
                val stages = IngestStats.snapshot()
                assertNotNull(stages["walk.ties.dense"], "the second must have been dense enough to split")
                assertNull(stages["walk.slice.empty"], "no slice may come back empty: the data never stops, so an empty one was sized off the tie")
            } finally {
                IngestStats.reset()
            }
        }

    /**
     * THE NEXT WALK OF A SHAPE STARTS ON A SLICE. A walk's first page has no density to size a
     * slice from, so asked open it matched the whole window — after slicing, about half of what a
     * walk still matched. The mirror walks the same filters over and over, so the width a page
     * of a shape took last time sizes the next walk's first slice. Switched off with the recency
     * memory, which is how the cold case is pinned beside it.
     *
     * Verified to fail with the recall removed: the second walk's first query matches the window.
     */
    @Test
    fun `a walk of a shape walked before starts on a slice`() =
        runBlocking {
            val sam = "d3".repeat(32)
            seedBulk((0 until 600).map { doc(kind = 30382, pubkey = sam, at = 1_700_000_000L - it) })
            val q = EventQuery(kinds = listOf(30382), authors = listOf(sam), since = 1_700_000_000L - 599, until = 1_700_000_000L)
            val fetch = 20 + VespaEventIndex.TIE_SLACK
            VespaEventIndex(mock.url, idPageSize = 20).use { idx ->
                val cold = walked(idx, q)
                assertEquals(600, cold.costs.first().first, "the first walk of a shape asks its first page open")
                val warm = walked(idx, q)
                assertEquals(nip01(q), warm.ids.map { it.id }, "the same ids, in order")
                assertTrue(warm.costs.first().first <= 2 * fetch, "the next walk's first query is a slice: ${warm.costs.first()}")
                assertTrue(warm.ratio < cold.ratio, "and the walk costs less: ${warm.ratio} against ${cold.ratio}")
            }
            VespaEventIndex(mock.url, idPageSize = 20, recencyMemory = false).use { idx ->
                walked(idx, q)
                assertEquals(600, walked(idx, q).costs.first().first, "without the memory every walk starts open")
            }
        }

    /**
     * A SLICE IS BOOKED WHERE ITS PAGE WAS. A slice adds a `since` the caller never sent, and
     * booked as itself it moved a walk's pages from its own `engineByCaller` row (`…,until`)
     * into `…,since,until` — the row the before/after comparison of #209 reads.
     *
     * Verified to fail with the shape override removed: `kinds,authors,since,until` appears.
     */
    @Test
    fun `slices are booked under the shape of the walk's own pages`() =
        runBlocking {
            val val0 = "d4".repeat(32)
            seedBulk((0 until 400).map { doc(kind = 30382, pubkey = val0, at = 1_700_000_000L - it) })
            val ledger = CostLedger()
            VespaEventIndex(mock.url, idPageSize = 20, ledger = ledger).use { idx ->
                withActivity(Activity.Snapshot) {
                    idx.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(val0), until = 1_700_000_000L)) { true }
                }
            }
            val rows = ledger.snapshot().engineByCaller.filter { it.activity == Activity.Snapshot }
            assertTrue(rows.sumOf { it.queries } > 3, "a multi-slice walk: $rows")
            assertEquals(setOf("kinds,authors,until"), rows.map { it.shape }.toSet(), "every page in the walk's own row")
        }

    /**
     * THE SAME SHAPE, LIMITED: the newest N, from the id walk — not from
     * `search`.
     *
     * A limit is an ORDER ("the newest N"), so the unordered scan cannot serve
     * it and the walk has to step somewhere else. Stepping to the port's
     * default is what it did first, and that default is `search(query)`: a
     * full document summary per id — content, sig, tags — materialized to
     * produce an id list, on whatever profile the recency planner picks, whose
     * match phase may cap the total and drop hits silently. A relay stamps
     * `limit: 100000` on exactly this shape for a COUNT.
     *
     * `buildIdTime` is the walk that is both ordered and unranked, and it
     * honours the limit itself. Asserted on the WIRE, because the two are
     * indistinguishable in the ids they return here.
     */
    @Test
    fun `a limited tie-dense walk asks for ids, not for documents`() =
        runBlocking {
            val bob = "b8".repeat(32)
            seed(*(1..500).map { doc(kind = 30382, pubkey = bob, at = 4_000L) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 100)
            try {
                val at = mock.searchRequests.size
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob), limit = 50)) {
                    got += it
                    true
                }
                assertEquals(50, got.size, "a limited walk serves exactly the budget")
                // THE LAST REQUEST IS THE ONE THAT SERVED THE PAGE — which,
                // since a limit inside one page skips the routing probe, is
                // also the only one. The probe is itself an id-time read, so
                // "an id-time request happened" proves nothing, and asserting
                // that was how the first version of this test passed against
                // both branches.
                val after = mock.searchRequests.drop(at)
                assertEquals(
                    EventYql.SUMMARY_IDTIME,
                    after.last()["presentation.summary"],
                    "the page must come from the id-only walk, not a document search: ${after.map { it["presentation.summary"] ?: "<full>" }}",
                )
            } finally {
                paged.close()
            }
        }

    /**
     * A LIMIT THAT FITS IN A PAGE IS ONE QUERY, FOR THE LIMIT — not a page,
     * and not a probe first.
     *
     * The cursor fetches `idPageSize + TIE_SLACK` rows whatever the caller
     * asked for, and #134 made that ten times bigger: a `limit: 500` walk
     * serialized 20,064 id rows to serve 500, having first paid a 2,000-row
     * routing probe to decide it. Neither buys anything on this shape — a walk
     * that stops inside its first page never crosses a PAGE boundary, and the
     * one tie group it can cut — at the Nth position — is resolved from the tie
     * slack it overfetches, with a `[t, t]` read only when that group runs past
     * the slack.
     *
     * Asserted on the WIRE, on the number of requests AND the hits each asked
     * for: the ids returned are identical either way, which is exactly why this
     * went unnoticed.
     */
    @Test
    fun `a limit inside one page is a single request for exactly the limit`() =
        runBlocking {
            val hal = "c4".repeat(32)
            seed(*(1..300).map { doc(kind = 30382, pubkey = hal) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 100)
            try {
                val q = EventQuery(kinds = listOf(30382), authors = listOf(hal), limit = 40)
                val at = mock.searchRequests.size
                val got = ArrayList<DocRef>()
                paged.visitIds(q) {
                    got += it
                    true
                }
                val after = mock.searchRequests.drop(at)
                assertEquals(40, got.size, "still exactly the newest N")
                assertEquals(reference.search(q).map { it.id }.sorted(), got.map { it.id }.sorted(), "and the same N search() serves")
                assertEquals(1, after.size, "one round trip, not a probe plus a page: ${after.map { it["hits"] }}")
                // The limit plus the tie slack, never a whole page: the slack is
                // how the walk sees whether a tie group straddles the Nth
                // position, which it must resolve by id to serve the page a read
                // serves (see "a limit'd walk cutting a tie group keeps the
                // group's lowest ids"). Attribute-only ids, 64 of them.
                assertEquals((40 + VespaEventIndex.TIE_SLACK).toString(), after.single()["hits"], "it must ask for the LIMIT and its tie slack, not for a whole page")
            } finally {
                paged.close()
            }
        }

    /** Past a page, the budget still pages the cursor — the shape `PAGE_IDS` was measured on. */
    @Test
    fun `a limit past one page still pages the cursor`() =
        runBlocking {
            val iris = "c5".repeat(32)
            seed(*(1..300).map { doc(kind = 30382, pubkey = iris) }.toTypedArray())
            val paged = VespaEventIndex(mock.url, idPageSize = 20)
            try {
                val q = EventQuery(kinds = listOf(30382), authors = listOf(iris), limit = 50)
                val at = mock.searchRequests.size
                val got = ArrayList<DocRef>()
                paged.visitIds(q) {
                    got += it
                    true
                }
                assertEquals(50, got.size, "exactly the limit")
                assertEquals(reference.search(q).map { it.id }.sorted(), got.map { it.id }.sorted(), "the newest 50, in any order")
                assertTrue(mock.searchRequests.size - at > 1, "a limit over the page must still take the probe-then-page path")
            } finally {
                paged.close()
            }
        }

    /**
     * A TIE-DENSE SECOND IS SPLIT BY ID PREFIX, and never buys the corpus.
     *
     * 500 docs share one second against a page of 100, past the
     * `idPageSize * TIE_DENSE_FACTOR` the cursor can carry. This went to the
     * document-API scan, and on staging's 516M documents that scan took 22 to
     * 51 minutes for a provider's one-second run of ~220k cards (2026-10-28) —
     * while sixteen id-prefix reads of the same second, through the attribute
     * index, summed to it exactly in about a second.
     *
     * Verified to fail with the split reverted: the walk scans
     * (`visitRequests` > 0, `walk.scan` booked).
     */
    @Test
    fun `a tie-dense second splits by id prefix, stays complete, and never scans`() =
        runBlocking {
            IngestStats.reset()
            val bob = "b7".repeat(32)
            seed(*(1..500).map { hashedDoc(kind = 30382, pubkey = bob, at = 3_000L) }.toTypedArray())
            val before = mock.visitRequests
            val paged = VespaEventIndex(mock.url, idPageSize = 100)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob))) {
                    got += it
                    true
                }
                assertEquals(500, got.distinctBy { it.id }.size, "the split must lose nothing")
                assertEquals(500, got.size, "and serve each id once")
                val stages = IngestStats.snapshot()
                assertNotNull(stages["walk.ties.dense"], "the second really was too dense for the cursor")
                assertEquals(0, mock.visitRequests - before, "a dense second must not buy a document-API read of the corpus")
                assertEquals(null, stages["walk.scan"], "no scan")
                // ~31 ids a bucket against a cap of 401: one split, sixteen reads.
                assertEquals(16L, stages["walk.ties.prefix"]?.calls, "one split of the second, a read per hex digit")
            } finally {
                paged.close()
                IngestStats.reset()
            }
        }

    /**
     * A BUCKET STILL TOO WIDE SPLITS AGAIN, on the next hex digit — the case a
     * second of millions reaches, here at a bucket cap of 13. The ids must all
     * arrive once and in id order within the second (depth-first, hex order),
     * with no scan.
     */
    @Test
    fun `a prefix bucket too wide for the cap splits on the next digit`() =
        runBlocking {
            IngestStats.reset()
            val cy = "c1".repeat(32)
            seedBulk((1..400).map { hashedDoc(kind = 30382, pubkey = cy, at = 4_000L) })
            val before = mock.visitRequests
            // The DEFAULT budget: every one of the sixteen buckets is over the
            // cap, and two full levels is exactly what it buys.
            val paged = VespaEventIndex(mock.url, idPageSize = 3)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(cy))) {
                    got += it
                    true
                }
                assertEquals(400, got.size, "every id")
                assertEquals(400, got.distinctBy { it.id }.size, "each once")
                val stages = IngestStats.snapshot()
                assertEquals(17L, stages["walk.ties.prefix.split"]?.calls, "~25 ids a bucket against 13: the root, then every bucket under it")
                assertEquals(null, stages["walk.ties.prefix.budget"], "two full levels fit the default budget")
                assertEquals(0, mock.visitRequests - before, "no scan")
                assertEquals(got.map { it.id }.sorted(), got.map { it.id }, "depth-first in hex order serves the second lowest id first")
            } finally {
                paged.close()
                IngestStats.reset()
            }
        }

    /**
     * THE SECOND IS READ ALONE, and the cursor resumes strictly below it: what
     * is newer goes out before the split, what is older after it, and nothing
     * on either side arrives twice or goes missing. The scan this replaced was
     * bounded `until = boundary` and finished the whole rest of the range; the
     * split covers ONE second, so the walk below it is the cursor's again.
     */
    @Test
    fun `a dense second between newer and older pages hands the walk back to the cursor`() =
        runBlocking {
            IngestStats.reset()
            val dee = "c2".repeat(32)
            val tied = 5_000L
            val corpus =
                (1..30).map { doc(kind = 30382, pubkey = dee, at = tied + it) } +
                    (1..300).map { hashedDoc(kind = 30382, pubkey = dee, at = tied) } +
                    (1..45).map { doc(kind = 30382, pubkey = dee, at = tied - it) }
            seedBulk(corpus)
            val before = mock.visitRequests
            val paged = VespaEventIndex(mock.url, idPageSize = 20)
            try {
                val got = ArrayList<DocRef>()
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(dee))) {
                    got += it
                    true
                }
                assertEquals(corpus.map { it.id }.sorted(), got.map { it.id }.sorted(), "exactly the corpus, each once")
                val firstTied = got.indexOfFirst { it.createdAt == tied }
                val lastTied = got.indexOfLast { it.createdAt == tied }
                assertTrue(got.subList(0, firstTied).all { it.createdAt > tied }, "everything newer goes out before the second")
                assertTrue(got.subList(firstTied, lastTied + 1).all { it.createdAt == tied }, "the second goes out in one run")
                assertTrue(got.subList(lastTied + 1, got.size).all { it.createdAt < tied }, "and the cursor resumes below it")
                assertNotNull(IngestStats.snapshot()["walk.ties.dense"], "the second was dense")
                assertEquals(0, mock.visitRequests - before, "no scan")
            } finally {
                paged.close()
                IngestStats.reset()
            }
        }

    /**
     * A WALK STOPPED INSIDE THE SECOND STAYS STOPPED: the split must not read
     * the remaining buckets, nor the cursor page below them, once the caller
     * has said it is done.
     */
    @Test
    fun `a walk stopped inside a dense second reads no further`() =
        runBlocking {
            IngestStats.reset()
            val eli = "c4".repeat(32)
            seedBulk(
                (1..300).map { hashedDoc(kind = 30382, pubkey = eli, at = 6_000L) } +
                    (1..30).map { doc(kind = 30382, pubkey = eli, at = 6_000L - it) },
            )
            val paged = VespaEventIndex(mock.url, idPageSize = 20)
            try {
                var pages = 0
                paged.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(eli))) {
                    pages++
                    false
                }
                assertEquals(1, pages, "the first bucket said stop")
                val stages = IngestStats.snapshot()
                assertEquals(1L, stages["walk.ties.prefix"]?.calls, "no bucket after the stop is read")
                assertEquals(1L, stages["walk.ids.page"]?.calls, "and the cursor does not page on below the second")
            } finally {
                paged.close()
                IngestStats.reset()
            }
        }

    /**
     * A TIE GROUP THE CURSOR CAN CARRY IS CARRIED — no scan, no probe.
     *
     * The other half of `a tie-dense walk falls back to the scan`: the routing
     * is not a prediction any more, it is what the boundary read actually
     * returns. Asking for one row past what the cursor can carry turns the
     * guess into a measurement, and costs the same query the walk was going to
     * issue anyway.
     *
     * Sized so the boundary lands INSIDE the tied second and the group is under
     * the cap: 20-id pages overfetch to 84, so 15 newer docs put the 20th hit
     * inside a 70-doc group — wide enough to fill the page, well under the
     * `idPageSize * TIE_DENSE_FACTOR` the cursor can still carry.
     */
    @Test
    fun `a tie group the cursor can carry is carried whole, without a scan`() =
        runBlocking {
            IngestStats.reset()
            val ivy = "c3".repeat(32)
            val tied = 6_000L
            seedBulk(
                (1..15).map { doc(kind = 30382, pubkey = ivy, at = tied + it) } +
                    (1..70).map { doc(kind = 30382, pubkey = ivy, at = tied) },
            )
            val before = mock.visitRequests
            val idx = VespaEventIndex(mock.url, idPageSize = 20)
            try {
                val got = ArrayList<DocRef>()
                idx.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(ivy))) {
                    got += it
                    true
                }
                assertEquals(85, got.distinctBy { it.id }.size, "the whole group, and everything newer, exactly once")
                assertNotNull(IngestStats.snapshot()["walk.ids.tiegroup"], "the boundary group must actually have been read")
                assertEquals(
                    0,
                    mock.visitRequests - before,
                    "a group the cursor can carry must not buy a document-API read of the corpus",
                )
            } finally {
                idx.close()
                IngestStats.reset()
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

    /**
     * Two author shards: each cluster applies a group LIMIT on its own, so a merged answer would
     * hold up to twice the groups asked for. Refused before anything is sent; the `max(expr)`
     * AGGREGATOR is not a limit and goes through.
     */
    @Test
    fun `a sharded index refuses a grouping limit and passes the max aggregator`() =
        runBlocking {
            VespaEventIndex(mock.url, tuning = ClientTuning(authorShards = 2)).use { sharded ->
                val e =
                    kotlin.test.assertFailsWith<IllegalArgumentException> {
                        sharded.group(EventQuery(kinds = listOf(1)), "all(group(pubkey) max(10) each(output(count())))")
                    }
                assertTrue("max(n)" in e.message.orEmpty(), e.message)
                sharded.group(EventQuery(kinds = listOf(1)), "all(group(pubkey) each(output(count(), max(created_at))))")
                // A limit inside one author's groups is exact across clusters: the first cut refused any max(n).
                sharded.group(EventQuery(kinds = listOf(1)), "all(group(pubkey) each(all(group(created_at) max(1) order(-max(created_at)) each(output(count())))))")
            }
            index.group(EventQuery(kinds = listOf(1)), "all(group(pubkey) max(10) each(output(count())))")
            Unit
        }

    /**
     * A bucket move's cut-over: an unscoped read meets a moved document in its old and new
     * cluster, and Vespa cuts the page at `hits` before the client sees the repeats. The page must
     * still hold `limit` distinct documents, newest first, rather than a short one a reader would
     * take for the end.
     */
    @Test
    fun `a sharded page that loses repeats is asked again until it is full`() =
        runBlocking {
            val docs = (0 until 10).map { doc(at = 2_000L - it) }
            seedBulk(docs)
            mock.repeatFirstHits = 3
            VespaEventIndex(mock.url, tuning = ClientTuning(authorShards = 2)).use { sharded ->
                assertEquals(docs.take(5).map { it.id }, sharded.search(EventQuery(kinds = listOf(1), limit = 5)).map { it.id })
                assertEquals(docs.map { it.id }, sharded.search(EventQuery(kinds = listOf(1), limit = 50)).map { it.id }, "a page the corpus cannot fill ends where the corpus does")
            }
            assertEquals(8, index.search(EventQuery(kinds = listOf(1), limit = 8)).size, "one cluster never deduplicates: it cannot hold a document twice")
        }

    /** The refetch never asks past the hit ceiling an operator capping maxHits sets: a 400 would replace a short page. */
    @Test
    fun `a sharded refetch stops at the hit ceiling`() =
        runBlocking {
            seedBulk((0 until 10).map { doc(at = 2_000L - it) })
            mock.repeatFirstHits = 3
            mock.refuseHitsAbove = 6
            VespaEventIndex(mock.url, tuning = ClientTuning(authorShards = 2, unboundedHits = 6)).use { sharded ->
                val page = sharded.search(EventQuery(kinds = listOf(1), limit = 5)).map { it.id }
                assertEquals(page.size, page.toSet().size, "no repeat served")
                assertTrue(page.size in 3..5, "as full as six hits allow: $page")
            }
        }

    @Test
    fun `the page size is rewritten in the yql, never inside a quoted term`() {
        assertEquals("select id from event where kind = 1 order by created_at desc limit 40", VespaEventIndex.withYqlLimit("select id from event where kind = 1 order by created_at desc limit 20", 40))
        assertEquals(
            """select * from event where text contains "limit 5" limit 80""",
            VespaEventIndex.withYqlLimit("""select * from event where text contains "limit 5" limit 20""", 80),
        )
        assertEquals(null, VespaEventIndex.withYqlLimit("""select * from event where text contains "limit 5"""", 80), "a page set by hits alone")
    }

    // ---- one query or id-first ------------------------------------------------

    private fun Map<String, String>.isHydrate() = getValue("yql").contains(" where id in (")

    private fun Map<String, String>.isIdTime() = this["presentation.summary"] == "idtime"

    /** Reads booked under each `recency.fetch.*` mode counter, keyed by the mode ([VespaEventIndex.FETCH_STAGE]). */
    private fun fetchModes(): Map<String, Long> =
        IngestStats
            .snapshot()
            .filterKeys { it.startsWith(VespaEventIndex.FETCH_STAGE + ".") }
            .mapKeys { it.key.removePrefix(VespaEventIndex.FETCH_STAGE + ".") }
            .mapValues { it.value.calls }

    /**
     * ONE QUERY FOR A SMALL PAGE. A limit-10 feed of one author on a cold shape plans 20 hits
     * (the page plus its page-width slack), under the default direct threshold: it is served by
     * ONE full-summary query — no `idtime` projection, no hydrate — where id-first spends two
     * round trips (~800 µs of container CPU each) on it. The same page as the id-first build.
     *
     * Verified to fail with the rule removed: two requests, the first `idtime`.
     */
    @Test
    fun `a small recency page is served in one full-summary query`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val q = EventQuery(kinds = listOf(1), authors = listOf(BUSY), limit = 10, nowSecs = now)
            VespaEventIndex(mock.url).use { direct ->
                val modes = fetchModes()
                val before = mock.searchRequests.size
                val page = direct.search(q)
                val sent = mock.searchRequests.drop(before)
                assertEquals(1, sent.size, "one query: $sent")
                assertFalse(sent.single().isIdTime() || sent.single().isHydrate(), "full summaries, no hydrate: $sent")
                assertTrue(asksFor(sent.single().getValue("yql"), 20), "for the page plus its slack: $sent")
                assertEquals(mapOf("direct" to 1L), fetchModes().minus(modes), "booked as a direct read")
                assertEquals(reference.search(q).map { it.id }, page.map { it.id })
                assertEquals((1..10).map { now - it }, page.map { it.createdAt })
                VespaEventIndex(mock.url, idFirstMaxDirectHits = 0).use { idFirst ->
                    val at = mock.searchRequests.size
                    val same = idFirst.search(q)
                    val idFirstSent = mock.searchRequests.drop(at)
                    assertEquals(2, idFirstSent.size, "the id-first build: the idtime query, then the hydrate: $idFirstSent")
                    assertTrue(idFirstSent[0].isIdTime() && idFirstSent[1].isHydrate(), "$idFirstSent")
                    assertEquals(same.map { it.id }, page.map { it.id }, "the id-first build serves the same page")
                    assertEquals(same.map { it.content }, page.map { it.content }, "and the same documents, whole")
                }
            }
        }

    /**
     * ONE QUERY UNLESS THE PAGE IS LARGE, AND A CROWDED SECOND COSTS IDS. A 70-hit page of
     * one feed is served in one query; a page planned past the cap is id-first; a feed whose
     * boundary second holds thirty notes is direct too, its cut group read as ids and only the
     * documents its page keeps read after, and the next read, at the slack the cut taught, is
     * id-first (it would be cut again either way); a global feed is direct from its first read.
     * Each serves the reference page.
     */
    @Test
    fun `a small page is one query and a crowded second costs ids`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val modes = fetchModes()

            val large = EventQuery(kinds = listOf(1, 7), authors = listOf(BUSY), limit = 35, nowSecs = now)
            var before = mock.searchRequests.size
            assertEquals(reference.search(large).map { it.id }, index.search(large).map { it.id })
            var sent = mock.searchRequests.drop(before)
            assertEquals(70, lastFetch(before), "the page plus its page-width slack")
            assertTrue(sent.none { it.isIdTime() || it.isHydrate() }, "a 70-hit page is documents, no hydrate: $sent")

            val oversized = EventQuery(kinds = listOf(1, 7), authors = listOf(BUSY), limit = ClientTuning.DEFAULT_ID_FIRST_MAX_DIRECT_HITS, nowSecs = now)
            before = mock.searchRequests.size
            assertEquals(reference.search(oversized).map { it.id }, index.search(oversized).map { it.id })
            sent = mock.searchRequests.drop(before)
            assertTrue(
                sent
                    .pageQueries()
                    .first()
                    .getValue("hits")
                    .toInt() > ClientTuning.DEFAULT_ID_FIRST_MAX_DIRECT_HITS,
                "a page at the cap plans past it with any slack: $sent",
            )
            assertTrue(sent.pageQueries().isNotEmpty() && sent.pageQueries().all { it.isIdTime() }, "so it resolves off ids: $sent")

            val author = "e2".repeat(32)
            val crowded = now - 100
            seed(*(1..30).map { doc(kind = 1, pubkey = author, at = crowded) }.toTypedArray())
            val q = EventQuery(kinds = listOf(1), authors = listOf(author), limit = 10, nowSecs = now)
            mock.scrambleTieOrder = true
            try {
                before = mock.searchRequests.size
                val page = index.search(q)
                assertEquals(reference.search(q).map { it.id }, page.map { it.id }, "the cut boundary is exact")
                assertTrue(page.all { it.sig.isNotEmpty() }, "and whole")
                sent = mock.searchRequests.drop(before)
                val ties = tieQueries(before, crowded)
                assertEquals(1, ties.size, "the 20-hit fetch cut the thirty-note second: $sent")
                assertTrue(ties.single().isIdTime(), "the group is read as ids: $sent")
                assertFalse(sent.first().isIdTime(), "the page query asked for documents: $sent")
                assertTrue(sent.filter { it.isHydrate() }.all { it.getValue("hits").toInt() <= 10 }, "then at most the page's documents: $sent")

                before = mock.searchRequests.size
                assertEquals(reference.search(q).map { it.id }, index.search(q).map { it.id })
                sent = mock.searchRequests.drop(before)
                assertEquals(10 + VespaEventIndex.TIE_SLACK, lastFetch(before), "the cut taught the shape the wide slack")
                assertTrue(sent.pageQueries().all { it.isIdTime() }, "a shape cut before is id-first: its page query reads ids: $sent")
                assertEquals("10", sent.single { it.isHydrate() }["hits"], "and exactly the page's documents")
            } finally {
                mock.scrambleTieOrder = false
            }

            val global = EventQuery(kinds = listOf(1), limit = 10, nowSecs = now)
            before = mock.searchRequests.size
            assertEquals(reference.search(global).map { it.id }, index.search(global).map { it.id })
            sent = mock.searchRequests.drop(before)
            assertFalse(sent.pageQueries().first().isIdTime(), "a global feed asks for documents too: $sent")

            assertEquals(mapOf("direct" to 3L, "idfirst" to 2L), fetchModes().minus(modes))
        }

    /**
     * NO DOCUMENT QUERY PAST THE CAP. A read planned at exactly the cap goes direct, but its
     * band pages each plan a band plus slack: every one of those asks for ids, and the page
     * is read whole after — the cap bounds every documents query of a direct read, not only its
     * first.
     */
    @Test
    fun `a direct read asks for ids past the cap on every query`() =
        runBlocking {
            val band = EventYql.MATCH_PHASE_BAND
            val now = System.currentTimeMillis() / 1000
            seedBulk((1..band + 50).map { doc(kind = 1, at = now - it) })
            val q = EventQuery(kinds = listOf(1), limit = band + 1, nowSecs = now)
            VespaEventIndex(mock.url, recencyStrategy = RecencyStrategy.MATCH_PHASE, idFirstMaxDirectHits = band + 1).use { direct ->
                val modes = fetchModes()
                val before = mock.searchRequests.size
                val page = direct.search(q)
                val sent = mock.searchRequests.drop(before)
                assertEquals(mapOf("direct" to 1L), fetchModes().minus(modes), "planned at the cap: direct")
                val documents = sent.pageQueries().filter { !it.isIdTime() }
                assertTrue(documents.all { it.getValue("hits").toInt() <= band + 1 }, "no documents query past the cap: $sent")
                assertTrue(sent.pageQueries().any { it.isIdTime() }, "the band page past it asked for ids: $sent")
                assertEquals((1..band + 1).map { now - it }, page.map { it.createdAt })
                assertTrue(page.all { it.sig.isNotEmpty() }, "read whole")
            }
        }

    /**
     * A SPARSE SHAPE STAYS DIRECT AS IT NARROWS. A limit-20 feed of one sparse author starts at
     * its page-width slack (40 planned hits) and narrows as its pages show no boundary ties; it
     * is never dense, so every read is one query, whatever slack it plans.
     */
    @Test
    fun `a sparse shape is one query on every read`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            val author = "e4".repeat(32)
            seed(*(1..40).map { doc(kind = 1, pubkey = author, at = now - it * 10L) }.toTypedArray())
            val q = EventQuery(kinds = listOf(1), authors = listOf(author), limit = 20, nowSecs = now)
            val reads =
                (1..20).map {
                    val before = mock.searchRequests.size
                    assertEquals(reference.search(q).map { it.id }, index.search(q).map { it.id })
                    val sent = mock.searchRequests.drop(before)
                    assertEquals(1, sent.size, "one query per read: $sent")
                    lastFetch(before) to sent.any { it.isHydrate() || it.isIdTime() }
                }
            assertEquals(40, reads.first().first, "cold: the page-width slack")
            assertTrue(reads.last().first < reads.first().first, "the slack narrowed: $reads")
            assertTrue(reads.none { it.second }, "never id-first: $reads")
        }

    /**
     * THE TWO OFF SWITCHES. A threshold of 0 is id-first as it was before the rule (the small page
     * of the test above: an `idtime` query and a hydrate); `VESPA_ID_FIRST=0` reads documents all
     * the way whatever the threshold, and books no mode (it never chose one).
     */
    @Test
    fun `threshold zero keeps id-first and id-first off reads documents`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val q = EventQuery(kinds = listOf(1), authors = listOf(BUSY), limit = 10, nowSecs = now)
            val expected = reference.search(q).map { it.id }
            VespaEventIndex(mock.url, tuning = ClientTuning(idFirstMaxDirectHits = 0)).use { zero ->
                val modes = fetchModes()
                val before = mock.searchRequests.size
                assertEquals(expected, zero.search(q).map { it.id })
                val sent = mock.searchRequests.drop(before)
                assertEquals(2, sent.size, "$sent")
                assertTrue(sent[0].isIdTime() && sent[1].isHydrate(), "threshold 0: id-first, as before: $sent")
                assertEquals(mapOf("idfirst" to 1L), fetchModes().minus(modes))
            }
            VespaEventIndex(mock.url, idFirst = false, idFirstMaxDirectHits = Int.MAX_VALUE).use { off ->
                val modes = fetchModes()
                val before = mock.searchRequests.size
                assertEquals(expected, off.search(q).map { it.id })
                val sent = mock.searchRequests.drop(before)
                assertEquals(1, sent.size, "$sent")
                assertFalse(sent.single().isIdTime(), "id-first off: documents all the way: $sent")
                assertEquals(emptyMap(), fetchModes().minus(modes), "no mode was chosen")
                // A large page too: no threshold turns id-first back on.
                val at = mock.searchRequests.size
                off.search(q.copy(limit = 40))
                assertTrue(mock.searchRequests.drop(at).none { it.isIdTime() || it.isHydrate() })
            }
        }

    /** The builds a mode-parity check compares: the default rule, always id-first, always direct, id-first off. */
    private fun modeBuilds(strategy: RecencyStrategy = RecencyStrategy.SPECULATIVE): Map<String, VespaEventIndex> =
        mapOf(
            "default" to VespaEventIndex(mock.url, recencyStrategy = strategy),
            "id-first" to VespaEventIndex(mock.url, recencyStrategy = strategy, idFirstMaxDirectHits = 0),
            "direct" to VespaEventIndex(mock.url, recencyStrategy = strategy, idFirstMaxDirectHits = Int.MAX_VALUE),
            "documents" to VespaEventIndex(mock.url, recencyStrategy = strategy, idFirst = false),
        )

    /**
     * A TIE-HEAVY BOUNDARY, BOTH MODES. Forty notes on one second under three newer ones, a
     * limit of 7 that cuts inside them, on the mock's adversarial tie order: the default build
     * goes direct (7 + 8 planned hits) and its slack cuts the group, so the `[t,t]` read
     * resolves it as ids — and every build serves the reference page, lowest ids first, under every
     * strategy, read after read (the second read meets the slack the cut taught).
     */
    @Test
    fun `a tie-heavy boundary serves the same page in both modes`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            val author = "e3".repeat(32)
            seed(*(1..40).map { doc(kind = 1, pubkey = author, at = now - 50) }.toTypedArray())
            seed(*(1..3).map { doc(kind = 1, pubkey = author, at = now - it) }.toTypedArray())
            val shapes =
                listOf(
                    EventQuery(kinds = listOf(1), authors = listOf(author), limit = 7),
                    EventQuery(kinds = listOf(1), authors = listOf(author), limit = 3),
                    EventQuery(kinds = listOf(1), authors = listOf(author), limit = 20),
                    EventQuery(kinds = listOf(1), authors = listOf(author), until = now - 50, limit = 9),
                ).map { it.copy(nowSecs = now) }
            mock.scrambleTieOrder = true
            try {
                for (strategy in RecencyStrategy.entries) {
                    val builds = modeBuilds(strategy)
                    try {
                        for (q in shapes) {
                            val expected = reference.search(q).map { it.id }
                            repeat(2) { read ->
                                for ((name, build) in builds) {
                                    val before = mock.searchRequests.size
                                    assertEquals(expected, build.search(q).map { it.id }, "$strategy $name read $read: $q")
                                    // The shape's first read is cold (15 planned hits): the default goes direct,
                                    // its page query asks for documents and its cut tie group is read as ids.
                                    if (strategy == RecencyStrategy.SPECULATIVE && name == "default" && q === shapes.first() && read == 0) {
                                        val sent = mock.searchRequests.drop(before)
                                        assertFalse(sent.first().isIdTime() || sent.first().isHydrate(), "the default build asked for the page's documents: $sent")
                                        assertTrue(sent.filter { it.isHydrate() }.all { it.getValue("hits").toInt() <= 7 }, "and for no more than the page's worth after: $sent")
                                    }
                                }
                            }
                        }
                    } finally {
                        builds.values.forEach { it.close() }
                    }
                }
            } finally {
                mock.scrambleTieOrder = false
            }
        }

    /**
     * A DEGRADED FIRST ANSWER AND THE GATED PROFILE, BOTH MODES. A settling cluster refuses the
     * read's first query (`non-ideal-state`): a speculative attempt falls through to the
     * unwindowed read, a partial-accepting one is served; a gated read whose match phase
     * under-delivers is rerun on the exact gated profile. Whichever mode the read took, it took
     * it for all of its queries, and every build serves the same page.
     */
    @Test
    fun `a degraded first answer and the gated profile serve the same page in both modes`() =
        runBlocking {
            val now = System.currentTimeMillis() / 1000
            seedStrategyCorpus(now)
            val plain = EventQuery(kinds = listOf(1), authors = listOf(BUSY), limit = 10, nowSecs = now)
            val builds = modeBuilds()
            try {
                for (q in listOf(plain, plain.copy(acceptsPartial = true), plain.copy(authors = listOf(WEEKLY), limit = 6))) {
                    val expected = reference.search(q).map { it.id }
                    for ((name, build) in builds) {
                        mock.degradeNextSearches = 1
                        val before = mock.searchRequests.size
                        assertEquals(expected, build.search(q).map { it.id }, "$name, first answer degraded: $q")
                        val sent = mock.searchRequests.drop(before)
                        if (name == "direct" || name == "documents") assertTrue(sent.none { it.isIdTime() || it.isHydrate() }, "$name: one mode for every query: $sent")
                        if (name == "id-first") assertTrue(sent.pageQueries().all { it.isIdTime() } && sent.count { it.isHydrate() } == 1, "$name: one mode for every query: $sent")
                    }
                }
                val gated = plain.copy(ranking = EventYql.RANK_RECENCY_GATED, observer = BUSY, rankKey = BUSY, minRank = 2.0)
                val expected = reference.search(plain).map { it.id }
                mock.matchPhaseUnderdeliver = 2
                try {
                    for ((name, build) in builds) {
                        val before = mock.searchRequests.size
                        assertEquals(expected, build.search(gated).map { it.id }, "$name: gated, under-delivered")
                        val sent = mock.searchRequests.drop(before)
                        assertTrue(sent.any { it["ranking"] == EventYql.RANK_RECENCY_GATED_EXACT }, "$name: the short gated page was rerun on the exact gated profile: $sent")
                        if (name == "default" || name == "direct") assertTrue(sent.none { it.isIdTime() || it.isHydrate() }, "$name: the gated small page went direct, rerun included: $sent")
                        if (name == "id-first") assertTrue(sent.pageQueries().all { it.isIdTime() }, "$name: $sent")
                    }
                } finally {
                    mock.matchPhaseUnderdeliver = 0
                }
            } finally {
                mock.degradeNextSearches = 0
                builds.values.forEach { it.close() }
            }
        }
}
