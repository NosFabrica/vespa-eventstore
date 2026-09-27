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

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.quartz.nipXXSql.FilterSql
import com.vitorpamplona.quartz.nipXXSql.SqlException
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * NQL (NIP-FF) over this store must answer exactly what Quartz's SQLite
 * store does for the same events — the relay-shaped queries sync and
 * monitor need, plus a random mix — while taking the engine's aggregate
 * paths where it can (checked through a spy on the index). The two sides run
 * different engines: this store through Quartz's interpreter over
 * [VespaSqlBackend], the reference compiled to SQLite.
 */
class SqlConformanceTest {
    private class Spy(
        val inner: EventIndex,
    ) : EventIndex by inner {
        var counts = 0
        var byAuthor = 0
        var distinctTags = 0
        var walks = 0
        var searches = 0
        var inDOrder = 0

        override suspend fun searchInDOrder(
            query: EventQuery,
            after: String,
            limit: Int,
        ) = inner.searchInDOrder(query, after, limit).also { inDOrder++ }

        override suspend fun search(query: EventQuery) = inner.search(query).also { searches++ }

        override suspend fun count(query: EventQuery) = inner.count(query).also { counts++ }

        override suspend fun countByAuthor(query: EventQuery) = inner.countByAuthor(query).also { byAuthor++ }

        override suspend fun distinctTagIndexValues(
            query: EventQuery,
            tagName: String,
        ) = inner.distinctTagIndexValues(query, tagName).also { distinctTags++ }

        override suspend fun visitIds(
            query: EventQuery,
            withDTag: Boolean,
            onPage: suspend (List<DocRef>) -> Boolean,
        ) = inner.visitIds(query, withDTag, onPage).also { walks++ }
    }

    private val spy = Spy(InMemoryEventIndex())
    private val vespa = NostrSemanticsStore(spy, relay = "wss://sot.test/".normalizeRelayUrl(), nowSecs = { 2_000_000_000L })
    private val reference = EventStore(dbName = null, relay = null)

    /** The same store reading addressable events in `d` order off `d_tag` (dOrderedReads). */
    private val dSpy = Spy(InMemoryEventIndex())
    private val vespaD = NostrSemanticsStore(dSpy, relay = "wss://sot.test/".normalizeRelayUrl(), nowSecs = { 2_000_000_000L }, dOrderedReads = true)

    private val authors = List(6) { "a$it".repeat(32) }
    private var seq = 0

    private fun event(
        author: String,
        createdAt: Long,
        kind: Int,
        tags: Array<Array<String>>,
        content: String = "",
    ): Event = EventFactory.create((++seq).toString(16).padStart(64, '0'), author, createdAt, kind, tags, content, "0".repeat(128))

    init {
        val r = Random(7)
        val words = listOf("nostr", "sql", "Nostr", "relay", "")
        val urls = List(8) { "wss://relay$it.test/" }
        val events = ArrayList<Event>()
        authors.forEachIndexed { i, a ->
            events += event(a, 1_000L + i, 0, emptyArray(), """{"name":"u$i"}""")
            events += event(a, 1_000L + i, 3, arrayOf(arrayOf("p", authors[(i + 1) % authors.size])))
            events +=
                event(
                    a,
                    1_000L + i,
                    10002,
                    Array(r.nextInt(1, 5)) {
                        when (r.nextInt(3)) {
                            0 -> arrayOf("r", urls[r.nextInt(urls.size)])
                            1 -> arrayOf("r", urls[r.nextInt(urls.size)], "write")
                            else -> arrayOf("r", urls[r.nextInt(urls.size)], "read")
                        }
                    },
                )
        }
        repeat(80) { i ->
            val tags = ArrayList<Array<String>>()
            repeat(r.nextInt(3)) { tags += arrayOf("t", words[r.nextInt(words.size)]) }
            if (r.nextBoolean()) tags += arrayOf("p", authors[r.nextInt(authors.size)])
            // Only a few distinct timestamps, so LIMIT boundaries land inside tie groups.
            events += event(authors[i % authors.size], 1_100L + r.nextInt(12), 1, tags.toTypedArray(), "note $i ${words[r.nextInt(words.size)]}")
        }
        urls.forEachIndexed { i, url ->
            val (label, ns) = if (i % 3 == 0) "dead" to "relay.fitness" else "alive" to (if (i % 2 == 0) "relay.fitness" else "other")
            events += event(authors[i % 2], 1_200L + i, 30166, arrayOf(arrayOf("d", url), arrayOf("l", label, ns, "1200", "e1")))
        }
        repeat(20) { i ->
            val target = events.first { it.kind == 1 && it.content.startsWith("note ${i * 3} ") }
            events += event(authors[(i + 2) % authors.size], 1_300L + i, 7, arrayOf(arrayOf("e", target.id), arrayOf("p", target.pubKey)), "+")
        }
        runBlocking {
            events.forEach {
                vespa.insert(it)
                reference.insert(it)
                vespaD.insert(it)
            }
        }
    }

    @AfterTest
    fun close() {
        reference.close()
        vespa.close()
        vespaD.close()
    }

    private fun run(
        store: IEventStore,
        query: String,
        params: List<Any?>,
    ): Pair<List<String>, List<String>> {
        val result = runBlocking { store.nql(query, params) }
        return result.columns.map { "${it.name}:${it.type}" } to result.rows.map { it.toString() }.sorted()
    }

    private fun assertSame(
        sql: String,
        vararg params: Any?,
    ) {
        val expected = run(reference, sql, params.toList())
        val actual = run(vespa, sql, params.toList())
        assertEquals(expected, actual, sql)
        assertTrue(expected.second.isNotEmpty(), "vacuous: $sql")
    }

    /**
     * NQL keyset pages by `events.d`, read off `d_tag` in byte order: every page, in
     * order, is the reference's, and the engine's d-ordered read is what served them.
     */
    @Test
    fun dOrderedPagesMatchTheReference() {
        // More cards than one read's batch (100), so the engine's order decides
        // which reach a page: mixed case, non-ASCII and emoji (byte order is not
        // UTF-16 or collation order), and two authors per `d` (ties at a boundary).
        val r = Random(11)
        val shapes = listOf("B", "b", "Z", "a", "\u00e9", "\uD83D\uDC9C", "\uFB01")
        runBlocking {
            repeat(250) { i ->
                val d = shapes[i % shapes.size] + (i / 2)
                val card = event(authors[i % 2], 3_000L + i, 30382, arrayOf(arrayOf("d", d), arrayOf("rank", r.nextInt(100).toString())))
                reference.insert(card)
                vespaD.insert(card)
            }
            // A full batch on one page, split by where code point and UTF-16 order
            // disagree: U+FB01 sorts before an emoji by code point, after it in UTF-16.
            repeat(150) { i ->
                val d = (if (i < 100) "\uFB01" else "\uD83D\uDC9C") + i.toString().padStart(3, '0')
                val card = event(authors[2], 4_000L + i, 30384, arrayOf(arrayOf("d", d)))
                reference.insert(card)
                vespaD.insert(card)
            }
        }
        val pages =
            listOf(
                "SELECT e.d AS target FROM events AS e WHERE e.kind = 30384 AND e.d > ? ORDER BY target LIMIT 100",
                "SELECT e.d AS target, e.id, CAST(r.t1 AS INTEGER) AS rank FROM events AS e JOIN tags AS r ON r.event_id = e.id AND r.t0 = 'rank' " +
                    "WHERE e.kind = 30382 AND e.d > ? ORDER BY target, e.id LIMIT 7",
                "SELECT e.d AS target FROM events AS e JOIN tags AS r ON r.event_id = e.id AND r.t0 = 'rank' " +
                    "WHERE e.kind = 30382 AND e.d > ? AND CAST(r.t1 AS INTEGER) > 60 ORDER BY target, e.id LIMIT 5",
                "SELECT e.d AS target, e.id FROM events AS e WHERE e.kind = 30166 AND e.d > ? ORDER BY target, e.id LIMIT 2",
                "SELECT e.d AS target, l.t1 AS label, l.t2 AS ns FROM events AS e JOIN tags AS l ON l.event_id = e.id AND l.t0 = 'l' " +
                    "WHERE e.kind = 30166 AND e.d > ? ORDER BY target LIMIT 3",
                "SELECT e.d AS target FROM events AS e JOIN tags AS l ON l.event_id = e.id AND l.t0 = 'l' " +
                    "WHERE e.kind = 30166 AND e.pubkey = ? AND e.d > ? AND l.t1 = 'dead' ORDER BY target LIMIT 1",
            )
        for (q in pages) {
            var after = ""
            var walked = 0
            while (true) {
                val params = if (q.contains("e.pubkey = ?")) listOf(authors[0], after) else listOf(after)
                val expected = runBlocking { reference.nql(q, params) }.rows
                assertEquals(expected, runBlocking { vespaD.nql(q, params) }.rows, "$q after '$after'")
                if (expected.isEmpty()) break
                walked += expected.size
                after = expected.last()[0] as String
            }
            assertTrue(walked > 0, "vacuous: $q")
        }
        assertTrue(dSpy.inDOrder > 0, "the d-ordered read should serve these pages")
    }

    @Test
    fun countIsOneEngineCount() {
        assertSame("SELECT count(*) AS n FROM events WHERE kind = 1")
        assertSame("SELECT count(*) AS n FROM events WHERE kind IN (1, 7) AND created_at BETWEEN 1105 AND 1310")
        assertEquals(2, spy.counts)
        assertEquals(0, spy.walks)
    }

    @Test
    fun idListingsAreOneWalkAndNoDocuments() {
        // The shape the relay's mirror reads NIP-77 snapshots through.
        val ids = FilterSql.ids(Filter(kinds = listOf(1, 7), authors = authors.take(3), since = 1_100))
        // Loading the corpus searches (replaceable checks); only what the query does counts.
        val searchesBefore = spy.searches
        val actual = run(vespa, ids.nql, ids.params)
        assertEquals(1, spy.walks)
        assertEquals(searchesBefore, spy.searches, "an id listing must not fetch documents")
        assertEquals(run(reference, ids.nql, ids.params), actual)
        assertTrue(actual.second.isNotEmpty(), "vacuous")
    }

    @Test
    fun countPerAuthorIsTheServerSideGrouping() {
        assertSame("SELECT pubkey, count(*) AS n FROM events WHERE kind = 1 GROUP BY pubkey ORDER BY n DESC, pubkey")
        assertEquals(1, spy.byAuthor)
        assertEquals(0, spy.walks)
    }

    @Test
    fun distinctRelayUrlsUseTheTagIndexGrouping() {
        assertSame("SELECT DISTINCT t1 FROM tags WHERE kind = 10002 AND t0 = 'r' AND t1 <> ''")
        assertEquals(1, spy.distinctTags)
    }

    @Test
    fun relayShapedQueriesMatchTheReference() {
        // Math functions over grouped values, computed by the interpreter over this store's rows.
        assertSame(
            "SELECT kind, round(sqrt(avg(created_at)) * 1000) / 1000 AS root, floor(log10(count(*) + 1)) AS digits, pow(2, kind % 5) AS p, " +
                "max(created_at) % 7 AS m, abs(min(created_at) - 1200) AS a, ceil(avg(length(content)) / 3.0) AS c " +
                "FROM events WHERE kind IN (1, 7) GROUP BY kind ORDER BY kind",
        )
        // R10: write relays only — the marker is tag position 2, which no engine index holds.
        assertSame("SELECT DISTINCT t1 FROM tags WHERE kind = 10002 AND t0 = 'r' AND (t2 IS NULL OR t2 = 'write')")
        // R2: newest version per author for one replaceable kind.
        assertSame("SELECT pubkey, max(created_at) AS newest FROM events WHERE kind = 0 AND pubkey IN (?, ?, ?) GROUP BY pubkey", authors[0], authors[2], authors[4])
        // R3: window counts per kind.
        assertSame("SELECT kind, count(*) AS n FROM events WHERE created_at BETWEEN 1100 AND 1400 AND kind IN (1, 7, 30166) GROUP BY kind")
        // R12: dead relays in one namespace (tag positions 1 and 2).
        assertSame("SELECT d.t1 FROM tags AS l JOIN tags AS d ON d.event_id = l.event_id AND d.t0 = 'd' WHERE l.kind = 30166 AND l.t0 = 'l' AND l.t1 = 'dead' AND l.t2 = 'relay.fitness'")
        // R14: latest verdict per url for one monitor, reading the epoch at position 4.
        assertSame(
            "SELECT d.t1, max(e.created_at) AS newest, max(l.t4) AS epoch FROM events AS e JOIN tags AS d ON d.event_id = e.id AND d.t0 = 'd' " +
                "JOIN tags AS l ON l.event_id = e.id AND l.t0 = 'l' WHERE e.kind = 30166 AND e.pubkey = ? GROUP BY d.t1",
            authors[0],
        )
        // R14 through NIP-FF's `events.d`, the addressable identifier: no join to the d tag.
        assertSame(
            "SELECT e.d, max(e.created_at) AS newest, max(l.t4) AS epoch FROM events AS e JOIN tags AS l ON l.event_id = e.id AND l.t0 = 'l' " +
                "WHERE e.kind = 30166 AND e.pubkey = ? GROUP BY e.d",
            authors[0],
        )
        // A keyset page by `d`, and `d = ?` (pushed down as `#d`), and `d` NULL outside addressable kinds.
        assertSame("SELECT d FROM events WHERE kind = 30166 AND pubkey = ? AND d > ? ORDER BY d LIMIT 2", authors[0], "")
        val someD = run(reference, "SELECT d FROM events WHERE kind = 30166 ORDER BY d LIMIT 1", emptyList()).second.single().removeSurrounding("[", "]")
        assertSame("SELECT kind, pubkey FROM events WHERE d = ?", someD)
        assertSame("SELECT kind, count(*) AS n FROM events WHERE kind IN (1, 30166) AND d IS NULL GROUP BY kind")
        // Reactions to one author's notes.
        assertSame("SELECT count(*) AS n FROM events AS r JOIN tags AS t ON t.event_id = r.id AND t.t0 = 'e' JOIN events AS n ON n.id = t.t1 WHERE r.kind = 7 AND n.kind = 1 AND n.pubkey = ?", authors[0])
        // Newest-first listing whose LIMIT lands inside a tie group.
        assertSame("SELECT id, created_at FROM events WHERE kind = 1 ORDER BY created_at DESC, id LIMIT 7")
        assertSame("SELECT id FROM events WHERE kind = 1 ORDER BY created_at DESC, id LIMIT 5 OFFSET 4")
        // Hashtag counts through a tag condition.
        assertSame("SELECT lower(t1) AS tag, count(*) AS n FROM tags WHERE t0 = 't' AND t1 IN ('nostr', 'Nostr') GROUP BY tag")
    }

    @Test
    fun randomQueriesMatchTheReference() {
        val r = Random(11)
        val kinds = listOf(0, 1, 3, 7, 10002, 30166)
        val shapes =
            listOf<() -> String>(
                { "SELECT kind, count(*) AS n, min(created_at) AS first, max(created_at) AS last FROM events WHERE kind IN (${kinds.random(r)}, ${kinds.random(r)}) GROUP BY kind" },
                { "SELECT pubkey, count(*) AS n FROM events WHERE kind = ${kinds.random(r)} GROUP BY pubkey" },
                { "SELECT count(*) AS n FROM events WHERE kind = ${kinds.random(r)} AND created_at >= ${1_000 + r.nextInt(400)}" },
                { "SELECT t1, count(*) AS n FROM tags WHERE kind = ${kinds.random(r)} AND t0 = '${listOf("p", "t", "r", "e").random(r)}' GROUP BY t1" },
                { "SELECT DISTINCT t1 FROM tags WHERE t0 = '${listOf("p", "t", "r").random(r)}' AND kind = ${kinds.random(r)} AND t1 <> ''" },
                { "SELECT id FROM events WHERE kind = 1 AND pubkey = '${authors.random(r)}' ORDER BY created_at DESC, id LIMIT ${r.nextInt(1, 6)}" },
                { "SELECT e.content FROM events AS e WHERE e.kind = 1 AND EXISTS (SELECT 1 AS x FROM tags AS t WHERE t.event_id = e.id AND t.t0 = 't' AND t.t1 = 'sql')" },
                { "SELECT count(*) AS n FROM tags WHERE t0 = 'p' AND t1 = '${authors.random(r)}'" },
            )
        repeat(200) {
            val sql = shapes.random(r)()
            assertEquals(run(reference, sql, emptyList()), run(vespa, sql, emptyList()), sql)
        }
    }

    @Test
    fun corpusWideScansAreRefused() {
        val e = assertFailsWith<SqlException> { run(vespa, "SELECT count(*) AS n FROM events WHERE content LIKE '%sql%'", emptyList()) }
        assertEquals(SqlException.UNSUPPORTED, e.prefix)
        // The same question, narrowed to a kind, is answered.
        assertSame("SELECT count(*) AS n FROM events WHERE kind = 1 AND content LIKE '%sql%'")
    }
}
