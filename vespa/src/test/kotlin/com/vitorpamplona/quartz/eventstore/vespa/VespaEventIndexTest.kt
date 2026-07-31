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
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

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

    /** The visit walk: the complete match set, across MULTIPLE continuation pages. */
    @Test
    fun `visitIds streams every match through continuation tokens`() =
        runBlocking {
            val bob = "b2".repeat(32)
            seed(*(1..20).map { doc(kind = 30382, pubkey = bob) }.toTypedArray())
            seed(doc(kind = 1, pubkey = bob), doc(kind = 30382)) // outside the selection
            val pages = ArrayList<List<DocRef>>()
            index.visitIds(EventQuery(kinds = listOf(30382), authors = listOf(bob))) {
                pages += it
                true
            }
            // The mock caps pages far below the requested size, so a full walk
            // proves the client actually follows continuation tokens.
            assertEquals(true, pages.size > 1, "expected a multi-page walk, got ${pages.size} page(s)")
            val expected = reference.search(EventQuery(kinds = listOf(30382), authors = listOf(bob))).map { DocRef(it.id, it.createdAt) }
            assertEquals(expected.sortedBy { it.id }, pages.flatten().sortedBy { it.id })
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
