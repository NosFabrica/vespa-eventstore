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

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
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
 * addressables; BulkMixedInsert for a batch carrying a kind 5) and single inserts (the default
 * `putIfNewer`, read through `visitIds`). The mock cannot run those groupings, so this is the
 * only test that serves them.
 *
 * Each path runs TWICE, with address keying pinned off and then on (`EngineTuning.addressKeyed`):
 * the keyed engine stores a replaceable AT its address, and a mixed batch that removed the
 * replaced version by that address deleted the winner it had just written.
 *
 * Per author, in separate batches: a kind 0, a kind 3 and three kind 30023s (`d` = "a", "b" and
 * the EMPTY d, stored with no d tag at all and superseded with `["d",""]`) are stored; then
 * newer versions of all of them; then STALE versions, in a batch of their own, which only the
 * stored-version read can refuse; then a mixed batch (a kind 5 beside a newer kind 0 and 30023
 * "a"); then a kind 0 at the SAME second with a lower id, which wins the NIP-01 tie. Id-keyed,
 * one more author holds two DRIFTED kind 3s written straight to the engine, so the version read
 * takes its full-read fallback and the supersession sweeps both.
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
                for (addressKeyed in listOf(false, true)) {
                    VespaEventStore.open(url = queryUrl, autoDeploy = false, configUrl = configUrl, tuning = EngineTuning(addressKeyed = addressKeyed)).use { front ->
                        runBlocking {
                            // The bulk path: every batch well past BULK_MIN.
                            pass(front.store, queryUrl, authors = (0 until 24).map { NostrSignerSync() }, single = false, addressKeyed = addressKeyed)
                            // The per-event path: the same writes one insert at a time.
                            pass(front.store, queryUrl, authors = (0 until 3).map { NostrSignerSync() }, single = true, addressKeyed = addressKeyed)
                        }
                    }
                }
            }
    }

    private suspend fun pass(
        store: NostrSemanticsStore,
        queryUrl: String,
        authors: List<NostrSignerSync>,
        single: Boolean,
        addressKeyed: Boolean,
    ) {
        val path = "${if (single) "per-event" else "bulk"}, addressKeyed=$addressKeyed"

        fun versions(
            signer: NostrSignerSync,
            at: Long,
            emptyD: Array<Array<String>>,
        ): List<Event> =
            listOf(
                signer.sign(at, 0, emptyArray(), "profile $at"),
                signer.sign(at, 3, arrayOf(arrayOf("p", "f".repeat(64))), ""),
                signer.sign(at, 30023, arrayOf(arrayOf("d", "a")), "article a $at"),
                signer.sign(at, 30023, arrayOf(arrayOf("d", "b")), "article b $at"),
                signer.sign(at, 30023, emptyD, "article with an empty d $at"),
            )
        val stored = authors.flatMap { versions(it, T0, emptyArray()) }
        val newer = authors.flatMap { versions(it, T0 + 500, arrayOf(arrayOf("d", ""))) }
        val stale = authors.flatMap { versions(it, T0 - 1_000, arrayOf(arrayOf("d", ""))) }

        // The mixed batch: a kind 5 naming nothing stored, beside a newer profile and article "a".
        val tieAt = T0 + 900
        val ties = authors.map { a -> listOf<Event>(a.sign(tieAt, 0, emptyArray(), "tie one"), a.sign(tieAt, 0, emptyArray(), "tie two")).sortedBy { it.id } }
        val newest = authors.map<NostrSignerSync, Event> { it.sign(T0 + 900, 30023, arrayOf(arrayOf("d", "a")), "article a, newest") }
        val mixed = authors.flatMap { listOf<Event>(it.sign(T0 + 900, 5, arrayOf(arrayOf("e", "0".repeat(64))), "")) } + ties.map { it[1] } + newest
        // The tie's LOWER id, offered after the higher one is stored: NIP-01 gives it the address.
        val tieWinners = ties.map { it[0] }

        // Id-keyed only: an author holding two versions of one address, as a failed sweep leaves it.
        val drifter = NostrSignerSync().takeUnless { addressKeyed }
        val drifted = drifter?.let { d -> listOf<Event>(d.sign(T0, 3, emptyArray(), ""), d.sign(T0 + 1, 3, emptyArray(), "")) }.orEmpty()
        if (drifted.isNotEmpty()) VespaEventIndex(queryUrl).use { raw -> raw.putAll(drifted.map { it.raw() }) }
        val drifterWinner: Event? = drifter?.sign(T0 + 2, 3, emptyArray(), "")

        expectAll(path, "the first versions are stored", write(store, stored, single)) { it is IEventStore.InsertOutcome.Accepted }
        expectAll(path, "a newer version is stored", write(store, newer + listOfNotNull(drifterWinner), single)) { it is IEventStore.InsertOutcome.Accepted }
        // Alone in their batch, so nothing in it is newer: only the stored-version read refuses them.
        expectAll(path, "a version older than the stored one is refused", write(store, stale, single)) { it is IEventStore.InsertOutcome.Rejected }
        expectAll(path, "the mixed batch is admitted", write(store, mixed, single)) { it is IEventStore.InsertOutcome.Accepted }
        expectAll(path, "the tie's lower id wins", write(store, tieWinners, single)) { it is IEventStore.InsertOutcome.Accepted }

        val expected =
            authors.indices
                .flatMap { i ->
                    val five = newer.subList(i * 5, i * 5 + 5)
                    // kind 0 -> the tie's lower id; 30023 "a" -> newest; kind 3, 30023 "b" and "" -> newer.
                    listOf(tieWinners[i].id, five[1].id, newest[i].id, five[3].id, five[4].id)
                }.toSet() + listOfNotNull(drifterWinner?.id)
        val pubkeys = authors.map { it.pubKey } + listOfNotNull(drifter?.pubKey)
        var served: Set<String> = emptySet()
        // Searchable once the content node has applied the writes.
        for (attempt in 0 until 60) {
            served = read(store, Filter(kinds = listOf(0, 3, 30023), authors = pubkeys))
            if (served == expected) break
            delay(500)
        }
        assertEquals(expected, served, "$path: every address serves exactly its newest version, and nothing older")
    }

    private fun expectAll(
        path: String,
        what: String,
        outcomes: List<IEventStore.InsertOutcome>,
        ok: (IEventStore.InsertOutcome) -> Boolean,
    ) = outcomes.forEachIndexed { i, o -> assertTrue(ok(o), "$path: $what (#$i: $o)") }

    /** [this] as the engine document the store would write — for the drifted versions written past the store. */
    private fun Event.raw() = EventDoc(id = id, pubkey = pubKey, createdAt = createdAt, kind = kind, tags = tags.map { it.toList() }, content = content, sig = sig)

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
        const val T0 = 1_700_000_000L

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
