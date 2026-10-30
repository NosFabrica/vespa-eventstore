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
package com.nosfabrica.vespa.eventstore.benchmark

import com.nosfabrica.vespa.eventstore.EngineReads
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.PortCall
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.security.MessageDigest
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `EngineReads.group` against a REAL Vespa, with the pipelines vespa-relay's
 * `/stats.json` actually runs (its `StatsYql`, copied here verbatim as data):
 * totals, distinct counts, per-value histograms, min/max spans, calendar
 * buckets (`time.date`, a Monday-aligned week expression, a month index), a
 * two-level nested grouping, `tag_index` pairs, and the reputation count.
 * Every WHERE clause the relay wrote by hand is expressed as an [EventQuery]
 * here — `kind = K`, a `created_at` window, `created_at > now` as `since =
 * now + 1` — which is the claim this IT exists to check: the relay can move
 * onto this read without a statistic changing. The mock cannot run any of the
 * grouping language.
 */
@Tag("integration")
class EngineGroupIT {
    @Test
    fun `the relay's statistics pipelines answer through the facade`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the grouping IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                VespaEventStore.open(queryUrl, configUrl = configUrl).use { store ->
                    runBlocking {
                        val now = System.currentTimeMillis() / 1000
                        VespaEventIndex(queryUrl).use { it.putAll(corpus(now)) }
                        // Not zero on a fresh store: open() starts the trust-keying
                        // migration, whose completion marker is itself a reputation
                        // document (TrustKeyingMigration.MARKER_KEY) — so the relay's
                        // "scored pubkeys" reads one more than it means. Settled first.
                        store.awaitTrustKeying()
                        val reputationsBefore = singleCount(store.engine.group(TOTAL, source = EngineReads.Source.REPUTATION))!!
                        VespaReputationIndex(queryUrl).use { reps ->
                            reps.putAll(listOf("p1", "p2").map { ReputationDoc(hex(it), mapOf(ServiceKey(hex("svc")) to 50)) })
                        }
                        val g = store.engine
                        val upToNow = EventQuery(until = now)
                        val days60 = EventQuery(since = BASE, until = BASE + 60 * DAY)

                        assertEquals(6, singleCount(g.group(TOTAL)), "every event")
                        assertEquals(1, singleCount(g.group(TOTAL, EventQuery(since = now + 1))), "future-dated: created_at > now")
                        assertEquals(4, singleCount(g.group("all(group(pubkey) output(count()))")), "distinct pubkeys")
                        assertEquals(reputationsBefore + 2, singleCount(g.group(TOTAL, source = EngineReads.Source.REPUTATION)), "scored pubkeys")
                        assertEquals(
                            mapOf("1" to 4L, "7" to 1L, "10002" to 1L),
                            counts(g.group("all(group(kind) each(output(count())))")),
                            "events per kind",
                        )
                        val span = topGroups(g.group("all(group(kind) each(output(min(created_at), max(created_at))))", upToNow)).first { value(it) == "1" }
                        assertEquals(BASE + 3_600, agg(span, "min(created_at)"), "oldest kind 1 up to now")
                        assertEquals(BASE + DAY + 7_200, agg(span, "max(created_at)"), "newest kind 1 up to now: the future-dated one is cut")
                        assertEquals(
                            mapOf("2024-1-1" to 2L, "2024-1-2" to 1L),
                            counts(g.group("all(group(time.date(created_at)) each(output(count())))", days60.copy(kinds = listOf(1)))),
                            "kind 1 per UTC day, in Vespa's unpadded time.date form",
                        )
                        val weeks = topGroups(g.group("all(group((created_at + 259200) / 604800) each(all(group(pubkey) output(count()))))", days60))
                        // Jan 1 2024 is a Monday: A, B and C all sign in its week; B's kind 7 is in February.
                        assertEquals(listOf(3L, 1L), weeks.sortedBy { value(it)!!.toLong() }.map { distinctCount(it) }, "distinct authors per week")
                        assertEquals(
                            mapOf("1" to mapOf("2024-1-1" to 2L, "2024-1-2" to 1L), "7" to mapOf("2024-2-10" to 1L)),
                            topGroups(g.group("all(group(kind) each(all(group(time.date(created_at)) each(output(count())))))", days60.copy(kinds = listOf(1, 7))))
                                .associate { value(it)!! to counts(it.childList()) },
                            "two levels: kind, then day",
                        )
                        assertEquals(
                            mapOf("r:wss://a.example" to 1L, "r:wss://b.example" to 1L),
                            counts(g.group("all(group(tag_index) each(output(count())))", EventQuery(kinds = listOf(10002)))),
                            "tag pairs of the relay lists",
                        )
                        assertEquals(
                            mapOf("${2024 * 12 + 1}" to 4L, "${2024 * 12 + 2}" to 1L),
                            counts(g.group("all(group(time.year(created_at) * 12 + time.monthofyear(created_at)) each(output(count())))", days60)),
                            "the month index",
                        )
                        val booked = store.metrics().ports.filter { it.activity == Activity.Aggregate && it.call == PortCall.Group }
                        assertTrue(booked.sumOf { it.calls } >= 11, "every statistic is metered: $booked")
                    }
                }
            }
    }

    private fun corpus(now: Long): List<EventDoc> =
        listOf(
            doc("A", 1, BASE + 3_600),
            doc("A", 1, BASE + DAY + 7_200),
            doc("B", 1, BASE + 5 * 3_600),
            doc("B", 7, BASE + 40 * DAY),
            doc("C", 10002, BASE + 100, listOf(listOf("r", "wss://a.example"), listOf("r", "wss://b.example"))),
            doc("D", 1, now + 10 * DAY),
        )

    private fun doc(
        author: String,
        kind: Int,
        at: Long,
        tags: List<List<String>> = emptyList(),
    ) = EventDoc(id = hex("$author:$kind:$at"), pubkey = hex(author), createdAt = at, kind = kind, tags = tags, content = "", sig = "e".repeat(128), owner = hex(author))

    private fun hex(s: String): String =
        MessageDigest
            .getInstance("SHA-256")
            .digest(s.toByteArray())
            .joinToString("") { "%02x".format(it) }

    // ---- the relay's readers, reduced to what this IT asserts ---------------

    private fun singleCount(root: JsonElement): Long? =
        when (root) {
            is JsonObject -> agg(root, "count()") ?: root.childList().firstNotNullOfOrNull { singleCount(it) }
            is JsonArray -> root.firstNotNullOfOrNull { singleCount(it) }
            else -> null
        }

    private fun topGroups(root: JsonElement): List<JsonObject> = shallowest(listOf(root))

    private fun shallowest(from: List<JsonElement>): List<JsonObject> {
        var frontier = from
        repeat(10) {
            val lists = frontier.filterIsInstance<JsonObject>().filter { it.idOf().startsWith("grouplist:") }
            if (lists.isNotEmpty()) return lists.flatMap { l -> l.childList().filterIsInstance<JsonObject>().filter { it.idOf().startsWith("group:") } }
            frontier = frontier.flatMap { (it as? JsonObject)?.childList().orEmpty() }
        }
        return emptyList()
    }

    private fun counts(root: JsonElement): Map<String, Long> = topGroups(root).associate { value(it)!! to agg(it, "count()")!! }

    private fun counts(children: List<JsonElement>): Map<String, Long> = shallowest(children).associate { value(it)!! to agg(it, "count()")!! }

    /** The relay's `distinctCountOf`: Vespa collapses the inner list's count onto the outer group; the walk is the fallback. */
    private fun distinctCount(group: JsonObject): Long? = agg(group, "count()") ?: group.childList().firstNotNullOfOrNull { singleCount(it) }

    private fun value(group: JsonObject): String? = (group["value"] as? JsonPrimitive)?.content

    private fun agg(
        group: JsonObject,
        name: String,
    ): Long? =
        group["fields"]
            ?.jsonObject
            ?.get(name)
            ?.jsonPrimitive
            ?.longOrNull

    private fun JsonObject.idOf(): String = (this["id"] as? JsonPrimitive)?.content ?: ""

    private fun JsonObject.childList(): List<JsonElement> = (this["children"] as? JsonArray) ?: emptyList()

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val TOTAL = "all(output(count()))"
        const val DAY = 86_400L

        /** 2024-01-01T00:00:00Z, a Monday. */
        const val BASE = 1_704_067_200L

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
