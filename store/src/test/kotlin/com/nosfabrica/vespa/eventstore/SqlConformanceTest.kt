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
 * SQL over this store must answer exactly what Quartz's SQLite store does
 * for the same events — the relay-shaped queries sync and monitor need,
 * plus a random mix — while taking the engine's aggregate paths where it
 * can (checked through a spy on the index).
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
            }
        }
    }

    @AfterTest
    fun close() {
        reference.close()
        vespa.close()
    }

    private fun run(
        store: IEventStore,
        sql: String,
        params: List<Any?>,
    ): Pair<List<String>, List<String>> {
        var columns = emptyList<String>()
        val rows = ArrayList<String>()
        runBlocking { store.sql(sql, params, onColumns = { columns = it }) { rows.add(it.toString()) } }
        return columns to rows.sorted()
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

    @Test
    fun countIsOneEngineCount() {
        assertSame("SELECT count(*) FROM events WHERE kind = 1")
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
        val actual = run(vespa, ids.sql, ids.params)
        assertEquals(1, spy.walks)
        assertEquals(searchesBefore, spy.searches, "an id listing must not fetch documents")
        assertEquals(run(reference, ids.sql, ids.params), actual)
        assertTrue(actual.second.isNotEmpty(), "vacuous")
    }

    @Test
    fun countPerAuthorIsTheServerSideGrouping() {
        assertSame("SELECT pubkey, count(*) FROM events WHERE kind = 1 GROUP BY pubkey ORDER BY 2 DESC, 1")
        assertEquals(1, spy.byAuthor)
        assertEquals(0, spy.walks)
    }

    @Test
    fun distinctRelayUrlsUseTheTagIndexGrouping() {
        assertSame("SELECT DISTINCT value FROM tags WHERE kind = 10002 AND name = 'r' AND value <> ''")
        assertEquals(1, spy.distinctTags)
    }

    @Test
    fun relayShapedQueriesMatchTheReference() {
        // Math functions over grouped values, computed on the pushdown's scratch rows.
        assertSame(
            "SELECT kind, round(sqrt(avg(created_at)), 3), floor(log10(count(*) + 1)), pow(2, kind % 5), mod(max(created_at), 7), " +
                "sign(min(created_at) - 1200), ceil(avg(length(content)) / 3.0) FROM events WHERE kind IN (1, 7) GROUP BY kind ORDER BY kind",
        )
        // R10: write relays only — the marker is tag position 2, which no engine index holds.
        assertSame("SELECT DISTINCT value FROM tags WHERE kind = 10002 AND name = 'r' AND (v2 IS NULL OR v2 = 'write')")
        // R2: newest version per author for one replaceable kind.
        assertSame("SELECT pubkey, max(created_at) FROM events WHERE kind = 0 AND pubkey IN (?, ?, ?) GROUP BY pubkey", authors[0], authors[2], authors[4])
        // R3: window counts per kind.
        assertSame("SELECT kind, count(*) FROM events WHERE created_at BETWEEN 1100 AND 1400 AND kind IN (1, 7, 30166) GROUP BY kind")
        // R12: dead relays in one namespace (tag positions 1 and 2).
        assertSame("SELECT d.value FROM tags l JOIN tags d ON d.event_id = l.event_id AND d.name = 'd' WHERE l.kind = 30166 AND l.name = 'l' AND l.value = 'dead' AND l.v2 = 'relay.fitness'")
        // R14: latest verdict per url for one monitor, reading the epoch at position 4.
        assertSame(
            "SELECT d.value, max(e.created_at), l.v4 FROM events e JOIN tags d ON d.event_id = e.id AND d.name = 'd' " +
                "JOIN tags l ON l.event_id = e.id AND l.name = 'l' WHERE e.kind = 30166 AND e.pubkey = ? GROUP BY d.value",
            authors[0],
        )
        // Reactions to one author's notes.
        assertSame("SELECT count(*) FROM events r JOIN tags t ON t.event_id = r.id AND t.name = 'e' JOIN events n ON n.id = t.value WHERE r.kind = 7 AND n.kind = 1 AND n.pubkey = ?", authors[0])
        // Newest-first listing whose LIMIT lands inside a tie group.
        assertSame("SELECT id, created_at FROM events WHERE kind = 1 ORDER BY created_at DESC, id LIMIT 7")
        assertSame("SELECT id FROM events WHERE kind = 1 ORDER BY created_at DESC, id LIMIT 5 OFFSET 4")
        // Hashtag counts through a tag condition.
        assertSame("SELECT lower(value), count(*) FROM tags WHERE name = 't' AND value IN ('nostr', 'Nostr') GROUP BY 1")
    }

    @Test
    fun randomQueriesMatchTheReference() {
        val r = Random(11)
        val kinds = listOf(0, 1, 3, 7, 10002, 30166)
        val shapes =
            listOf<() -> String>(
                { "SELECT kind, count(*), min(created_at), max(created_at) FROM events WHERE kind IN (${kinds.random(r)}, ${kinds.random(r)}) GROUP BY kind" },
                { "SELECT pubkey, count(*) FROM events WHERE kind = ${kinds.random(r)} GROUP BY pubkey" },
                { "SELECT count(*) FROM events WHERE kind = ${kinds.random(r)} AND created_at >= ${1_000 + r.nextInt(400)}" },
                { "SELECT value, count(*) FROM tags WHERE kind = ${kinds.random(r)} AND name = '${listOf("p", "t", "r", "e").random(r)}' GROUP BY value" },
                { "SELECT DISTINCT value FROM tags WHERE name = '${listOf("p", "t", "r").random(r)}' AND kind = ${kinds.random(r)} AND value <> ''" },
                { "SELECT id FROM events WHERE kind = 1 AND pubkey = '${authors.random(r)}' ORDER BY created_at DESC, id LIMIT ${r.nextInt(1, 6)}" },
                { "SELECT e.content FROM events e WHERE e.kind = 1 AND EXISTS (SELECT 1 FROM tags t WHERE t.event_id = e.id AND t.name = 't' AND t.value = 'sql')" },
                { "SELECT count(*) FROM tags WHERE name = 'p' AND value = '${authors.random(r)}'" },
            )
        repeat(200) {
            val sql = shapes.random(r)()
            assertEquals(run(reference, sql, emptyList()), run(vespa, sql, emptyList()), sql)
        }
    }

    @Test
    fun corpusWideScansAreRefused() {
        val e = assertFailsWith<SqlException> { run(vespa, "SELECT count(*) FROM events WHERE content LIKE '%sql%'", emptyList()) }
        assertEquals(SqlException.UNSUPPORTED, e.prefix)
        // The same question, narrowed to a kind, is answered.
        assertSame("SELECT count(*) FROM events WHERE kind = 1 AND content LIKE '%sql%'")
    }
}
