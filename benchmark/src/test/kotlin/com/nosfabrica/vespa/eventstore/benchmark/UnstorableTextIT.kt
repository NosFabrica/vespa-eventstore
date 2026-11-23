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

import com.nosfabrica.vespa.eventstore.RejectedException
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Events whose text a Vespa string field refuses, stored and served EXACTLY by a
 * real engine: NIP-01's own `\b` and `\f` escapes (relay-tester's "Accepts NIP-01
 * JSON escape sequences"), the noncharacter U+FDD1 beside an astral character
 * (its "Accepts UTF-8 non-characters"), the C0 controls profiles carry, and an
 * astral noncharacter. The store refused all of these until `content_json`; the
 * mock cannot tell, because only a real engine validates a string field.
 *
 * Every read path a relay uses: the id lookup, a filtered recall, the raw recall
 * a relay splices onto the wire, and `#t` recall by the event's storable value.
 * Both write paths: the per-event insert and the bulk planner (16 or more).
 */
@Tag("integration")
class UnstorableTextIT {
    @Test
    fun `text the engine refuses raw is stored and served exactly`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the unstorable-text IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                SchemaDeployer(configUrl).deployIfAbsent(queryUrl)
                VespaEventStore.open(url = queryUrl, autoDeploy = false, configUrl = configUrl).use { front ->
                    runBlocking {
                        val single = TEXTS.mapIndexed { i, text -> note(i, text) }
                        single.forEach { front.insert(it) }
                        val bulk = (0 until 20).map { note(100 + it, TEXTS[it % TEXTS.size] + " #$it") }
                        val outcomes = front.batchInsert(bulk)
                        assertTrue(outcomes.all { it is IEventStore.InsertOutcome.Accepted }, "bulk path: $outcomes")
                        val all = single + bulk

                        // The id lookup.
                        val byId = front.query<Event>(Filter(ids = all.map { it.id })).associateBy { it.id }
                        for (e in all) assertEquals(e.toJson(), byId[e.id]?.toJson(), "by id: ${e.id}")

                        // A filtered recall, through the summary select list.
                        val recalled = front.query<Event>(Filter(authors = listOf(AUTHOR), kinds = listOf(1), limit = 500)).associateBy { it.id }
                        for (e in all) assertEquals(e.toJson(), recalled[e.id]?.toJson(), "recall: ${e.id}")

                        // The raw recall: content decoded, tags the stored JSON text, which parses back exact.
                        val raw = ArrayList<RawEvent>()
                        front.rawQuery(listOf(Filter(authors = listOf(AUTHOR), kinds = listOf(1), limit = 500))) { raw += it }
                        val rawById = raw.associateBy { it.id }
                        for (e in all) {
                            val r = rawById.getValue(e.id)
                            assertEquals(e.content, r.content, "raw content: ${e.id}")
                            val tags = Json.parseToJsonElement(r.jsonTags).jsonArray.map { row -> row.jsonArray.map { it.jsonPrimitive.content } }
                            assertEquals(e.tags.map { it.toList() }, tags, "raw tags: ${e.id}")
                        }

                        // `#t` finds each event by its storable value; the unstorable one matches nothing, and does not throw.
                        val byTag = front.query<Event>(Filter(tags = mapOf("t" to listOf("plain")), limit = 500)).map { it.id }.toSet()
                        assertEquals(all.map { it.id }.toSet(), byTag)
                        assertEquals(emptyList(), front.query<Event>(Filter(tags = mapOf("t" to listOf(TEXTS[1])), limit = 500)))

                        // An addressable whose `d` the engine cannot hold is still refused: its
                        // versions are found by #d, which tag_index cannot hold for that value.
                        val article = Event("%064x".format(999), AUTHOR, 1_700_000_999L, 30023, arrayOf(arrayOf("d", "a\u0001b")), "x", "")
                        val refused = runCatching { front.insert(article) }.exceptionOrNull()
                        assertTrue(refused is RejectedException, "expected a semantic rejection, got $refused")
                    }
                }
            }
    }

    private fun note(
        n: Int,
        text: String,
    ) = Event("%064x".format(n + 1), AUTHOR, 1_700_000_000L + n, 1, arrayOf(arrayOf("t", text), arrayOf("t", "plain")), text, "")

    companion object {
        private const val QUERY_PORT = 8080
        private const val CONFIG_PORT = 19071
        private val AUTHOR = "ab".repeat(32)

        /** Written as escapes, never raw: a raw control character in source is invisible. */
        private val TEXTS =
            listOf(
                "linebreak\ndoublequote\"backslash\\carraigereturn\rtab\tbackspace\bformfeed\u000cend",
                "\uFDD1\uDBFF\uDFF2",
                "\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007 \u000b \u000e \u000f \u0010\u0011\u0012\u0013\u0014\u0015\u0016 /",
                "astral noncharacter \uDBFF\uDFFF and an emoji \uD83D\uDE00",
            )

        private fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
