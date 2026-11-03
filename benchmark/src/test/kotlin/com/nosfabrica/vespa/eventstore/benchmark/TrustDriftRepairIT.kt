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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.trust.TrustReconciler
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * THE 2026-11-03 DRIFT, ON A REAL VESPA, AND ITS REPAIR.
 *
 * search.brainstorm.world gated out an author whose card the observer's
 * provider had signed at rank 70: the card was stored, but the author's
 * reputation document carried no cell under that provider, so every read
 * through the lens dropped them (`filter:rank:gte:1` hid them, `include:spam`
 * showed them). This builds that state at a scale that crosses the confirm's
 * gate slice — every subject loses the observer's provider's cell — plus one
 * subject with no document at all and one orphan document, and holds
 * `verifyTrust` to it: the check finds exactly that drift and writes nothing,
 * the repair converges, and the gated read serves the author again.
 *
 * Tagged `integration`; run with `-Pintegration` where Docker is available.
 */
@Tag("integration")
class TrustDriftRepairIT {
    @Test
    fun `verify finds cells missing beside stored cards and repair restores the gated read`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the trust drift IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                SchemaDeployer(configUrl).deployIfAbsent(queryUrl)
                VespaReputationIndex(queryUrl).use { reputations ->
                    VespaEventStore.open(url = queryUrl, autoDeploy = false, configUrl = configUrl).use { store ->
                        runBlocking {
                            // P1 is the observer's provider, P2 another observer's,
                            // P3 nobody's: its cards are dead storage the derive skips.
                            store.batchInsert(listOf(list10040(OBSERVER, P1), list10040(OTHER_OBSERVER, P2)))
                            SUBJECTS.chunked(500).forEach { chunk ->
                                store.batchInsert(chunk.flatMap { s -> listOf(card(P1, s, 70), card(P2, s, 12), card(P3, s, 99)) })
                            }
                            store.batchInsert(listOf(note(MISSING), card(P1, MISSING, 40)) + SUBJECTS.take(3).map(::note))
                            store.awaitTrustProjection()
                            assertTrue(store.verifyTrust().isClean(), "a faithful projection to start from")
                            assertEquals(SUBJECTS.size, SUBJECTS.count { reputations.get(it)?.influenceScores?.get(ServiceKey(P1)) == 70 }, "every card landed its cell")
                            assertEquals(1, authorNotes(store, SUBJECTS[0]), "the lens serves the author")

                            // The drift: the observer's provider's cell gone beside the other one.
                            reputations.removeCells(SUBJECTS.map { CellRemoval(it, ServiceKey(P1), influence = true, followers = true) })
                            reputations.remove(MISSING)
                            reputations.put(ReputationDoc(ORPHAN, serviceCells(P1 to 50), emptyMap()))
                            assertEquals(serviceCells(P2 to 12), reputations.get(SUBJECTS[0])?.influenceScores)
                            assertEquals(0, authorNotes(store, SUBJECTS[0]), "the live symptom: gated out despite a rank-70 card")
                            assertEquals(1, authorNotes(store, SUBJECTS[0], "include:spam"), "and there without the gate")

                            val drifted = SUBJECTS.size + 2
                            IngestStats.reset()
                            var t0 = System.nanoTime()
                            val check = store.verifyTrust()
                            val checkMs = (System.nanoTime() - t0) / 1_000_000
                            assertEquals(drifted, check.driftCount, "every stripped subject, the missing one and the orphan")
                            assertEquals(SUBJECTS.size + 1, check.subjectsChecked)
                            assertEquals(0, authorNotes(store, SUBJECTS[0]), "a check writes nothing")
                            val kinds = check.drift.groupBy { TrustReconcilerKinds.of(it) }.mapValues { it.value.size }
                            println("trust-drift-it: check ${checkMs}ms, $drifted drifted, sample kinds $kinds; ${gateHolds()}")
                            IngestStats.reset()

                            // A writer behind the repair: a kind-30382 insert takes the trust
                            // gate, so its latency is what the repair's holds cost live ingest.
                            val waits = ArrayList<Long>()
                            var repairing = true
                            t0 = System.nanoTime()
                            val repair =
                                coroutineScope {
                                    val writer =
                                        launch(Dispatchers.IO) {
                                            var n = 0
                                            while (repairing) {
                                                val w0 = System.nanoTime()
                                                store.insert(card(P3, "e" + (++n).toString(16).padStart(63, '0'), 5))
                                                waits += (System.nanoTime() - w0) / 1_000_000
                                                delay(50)
                                            }
                                        }
                                    store.verifyTrust(repair = true).also {
                                        repairing = false
                                        writer.join()
                                    }
                                }
                            val repairMs = (System.nanoTime() - t0) / 1_000_000
                            assertEquals(drifted, repair.driftCount)
                            val sorted = waits.sorted()
                            println(
                                "trust-drift-it: repair ${repairMs}ms; ${gateHolds()}; card inserts during it: ${waits.size}, " +
                                    "median ${sorted.getOrElse(sorted.size / 2) { 0 }}ms, p95 ${sorted.getOrElse(sorted.size * 95 / 100) { 0 }}ms, max ${sorted.lastOrNull() ?: 0}ms",
                            )

                            assertTrue(store.verifyTrust().isClean(), "repair converged")
                            assertEquals(serviceCells(P1 to 70, P2 to 12), reputations.get(SUBJECTS[0])?.influenceScores, "the unnamed P3 never gets a cell")
                            assertEquals(serviceCells(P1 to 40), reputations.get(MISSING)?.influenceScores)
                            assertNull(reputations.get(ORPHAN))
                            assertEquals(1, authorNotes(store, SUBJECTS[0]), "the lens serves the author again")
                            assertEquals(1, authorNotes(store, MISSING), "and the one that had no document")
                        }
                    }
                }
            }
    }

    /** The writer lock's holds since the last reset: how many, and the longest — what an ingest behind it waits. */
    private fun gateHolds(): String =
        IngestStats
            .snapshot()
            .filterKeys { it.startsWith("lock.gate.hold") }
            .entries
            .joinToString { (k, v) -> "$k x${v.calls} max ${v.maxNanos / 1_000_000}ms total ${v.totalNanos / 1_000_000}ms" }
            .ifEmpty { "no gate holds booked" }

    private suspend fun authorNotes(
        store: VespaEventStore,
        author: String,
        lens: String = "observer:$OBSERVER",
    ): Int = store.query<Event>(Filter(kinds = listOf(1), authors = listOf(author), limit = 10, search = lens)).size

    /** The drift's kind in the words the relay's report uses. */
    private object TrustReconcilerKinds {
        fun of(d: TrustReconciler.TrustDrift) =
            when {
                d.actual == null || d.actual!!.isEmpty() -> "missing"
                d.expected == null -> "orphan"
                else -> "stale"
            }
    }

    private fun list10040(
        observer: String,
        service: String,
    ) = TrustProviderListEvent(hexId(), observer, 100, arrayOf(arrayOf("30382:rank", service, "wss://scores.test/"), arrayOf("30382:followers", service, "wss://scores.test/")), "", "")

    private fun card(
        service: String,
        subject: String,
        rank: Int,
    ) = UserAssertionEvent(hexId(), service, 1_000L, arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString()), arrayOf("followers", "12")), "", "")

    private fun note(author: String) = TextNoteEvent(hexId(), author, 1_700_000_000L, emptyArray(), "a note", "e".repeat(128))

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        val OBSERVER = "c".repeat(64)
        val OTHER_OBSERVER = "d".repeat(64)
        val P1 = "5e".repeat(32)
        val P2 = "6e".repeat(32)
        val P3 = "7e".repeat(32)

        /** More than two gate slices (500 each), so the confirm takes the lock in several holds. */
        val SUBJECTS = (1..1_200).map { "b" + it.toString(16).padStart(63, '0') }
        val MISSING = "f".repeat(64)
        val ORPHAN = "9".repeat(64)

        private var seq = 0

        // Prefixed, not padded with a hex digit: padStart(64, 'a') makes ids 10
        // and 170 the same string, and the store rightly drops the duplicate.
        fun hexId() = "a" + (++seq).toString(16).padStart(63, '0')

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
