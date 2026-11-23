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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
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
 * Replaceable and addressable supersession against a REAL Vespa, through both write paths: a
 * batch past BULK_MIN (BulkRecordInsert, whose version read is the attribute-only
 * `newestVersions` / `countByAuthor` groupings for plain replaceables and `visitIds` for
 * addressables) and single inserts (the default `putIfNewer`, read through `visitIds`). The
 * mock cannot run those groupings, so this is the only test that serves them.
 *
 * Per author: a kind 0, a kind 3 and two kind 30023s (`d` = "a", "b") are stored, then a batch
 * supersedes every address and also offers versions OLDER than what is stored. Afterwards each
 * address holds exactly its newest version, nothing older came back, and the stale offers
 * were refused.
 */
@Tag("integration")
class BulkSupersessionIT {
    @Test
    fun `each address keeps exactly its newest version on both write paths`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the bulk supersession IT")

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
                        // The bulk path: every batch well past BULK_MIN.
                        pass(front.store, authors = (0 until 24).map { NostrSignerSync() }, single = false)
                        // The per-event path: the same writes one insert at a time.
                        pass(front.store, authors = (0 until 3).map { NostrSignerSync() }, single = true)
                    }
                }
            }
    }

    private suspend fun pass(
        store: NostrSemanticsStore,
        authors: List<NostrSignerSync>,
        single: Boolean,
    ) {
        val path = if (single) "per-event" else "bulk"

        fun versions(
            signer: NostrSignerSync,
            at: Long,
        ): List<Event> =
            listOf(
                signer.sign(at, 0, emptyArray(), "profile $at"),
                signer.sign(at, 3, arrayOf(arrayOf("p", "f".repeat(64))), ""),
                signer.sign(at, 30023, arrayOf(arrayOf("d", "a")), "article a $at"),
                signer.sign(at, 30023, arrayOf(arrayOf("d", "b")), "article b $at"),
            )
        val stored = authors.flatMap { versions(it, 1_700_000_000L) }
        val newer = authors.flatMap { versions(it, 1_700_000_500L) }
        val stale = authors.flatMap { versions(it, 1_699_999_000L) }

        write(store, stored, single).forEachIndexed { i, outcome ->
            assertTrue(outcome is IEventStore.InsertOutcome.Accepted, "$path: first versions are stored (#$i: $outcome)")
        }
        val outcomes = write(store, newer + stale, single)
        newer.indices.forEach { i ->
            assertTrue(outcomes[i] is IEventStore.InsertOutcome.Accepted, "$path: a newer version is stored (#$i: ${outcomes[i]})")
        }
        stale.indices.forEach { i ->
            val o = outcomes[newer.size + i]
            assertTrue(o is IEventStore.InsertOutcome.Rejected, "$path: a version older than the stored one is refused (#$i: $o)")
        }

        val expected = newer.map { it.id }.toSet()
        val pubkeys = authors.map { it.pubKey }
        var served: Set<String> = emptySet()
        // Searchable once the content node has applied the writes.
        repeat(60) {
            served = read(store, Filter(kinds = listOf(0, 3, 30023), authors = pubkeys))
            if (served == expected) return@repeat
            delay(500)
        }
        assertEquals(expected, served, "$path: every address serves exactly its newest version, and nothing older")
    }

    private suspend fun write(
        store: NostrSemanticsStore,
        events: List<Event>,
        single: Boolean,
    ): List<IEventStore.InsertOutcome> = if (single) events.map { store.batchInsert(listOf(it)).single() } else store.batchInsert(events)

    private suspend fun read(
        store: NostrSemanticsStore,
        filter: Filter,
    ): Set<String> {
        val ids = HashSet<String>()
        store.query<Event>(listOf(filter)) { ids += it.id }
        return ids
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
