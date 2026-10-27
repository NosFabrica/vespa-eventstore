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
package com.nosfabrica.vespa.eventstore.benchmark.probe

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/**
 * ONE ASSISTANT RUN, timed: what it costs the store to take a GrapeRank
 * publisher's whole run — [N] kind-30382 cards, every one by the SAME service,
 * published together — and then the next run, which supersedes every one of
 * them. That second run is the production steady state: brainstorm re-signs
 * ~150,000 cards every few minutes, and a store slower than the publisher
 * falls behind for good.
 *
 * The cards go through `batchInsert` at the relay's publish batch
 * ([BATCH], `RELAY_INGEST_BATCH`'s default 1000 — set it lower to model a
 * publisher whose in-flight window caps the batch) from [WORKERS] concurrent
 * callers (`RELAY_INGEST_CONCURRENCY`, default 2), exactly as the relay's
 * pipeline calls the store. One observer's 10040 names the service first, so
 * every card is projected into cells.
 *
 * Prints, per run: wall time, cards/s, the per-batch latency spread, and the
 * stage split — `proj.write` (the cell writes), `proj.marker` (the crash
 * ledger's write-ahead and cleanup), `write` / `remove` (the event feed),
 * `versions` (the supersession read) and the lock hold — then checks a sample
 * of subjects really carries the run's rank in the service's cell.
 *
 * Signatures are never verified by the store, so the cards carry a random id
 * and an empty signature.
 *
 *     VESPA_URL=http://localhost:8080 N=150000 BATCH=1000 WORKERS=2 \
 *       ./gradlew :benchmark:cardRunProbe
 */
object CardRunProbe {
    private val N = System.getenv("N")?.toIntOrNull() ?: 150_000
    private val BATCH = System.getenv("BATCH")?.toIntOrNull() ?: 1_000
    private val WORKERS = System.getenv("WORKERS")?.toIntOrNull() ?: 2
    private val RUNS = System.getenv("RUNS")?.toIntOrNull() ?: 2

    private val rnd = Random(System.nanoTime())

    private fun hex() = ByteArray(32).also { rnd.nextBytes(it) }.joinToString("") { "%02x".format(it) }

    private fun card(
        service: String,
        subject: String,
        at: Long,
        rank: Int,
    ): Event =
        UserAssertionEvent(
            hex(),
            service,
            at,
            arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString()), arrayOf("followers", (rank * 3).toString())),
            "",
            "",
        )

    private fun stage(name: String): IngestStats.Stage = IngestStats.snapshot()[name] ?: IngestStats.Stage(0, 0, 0)

    private val STAGES = listOf("lock.ingest.trust.wait", "lock.ingest.hold", "dedup", "versions", "supersede", "write", "remove", "proj.write", "proj.marker")

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val store = VespaEventStore.open(url)
            val service = hex()
            val observer = hex()
            val now = System.currentTimeMillis() / 1000
            println("cardRunProbe: N=$N batch=$BATCH workers=$WORKERS service=${service.take(12)} against $url")
            val list = TrustProviderListEvent(hex(), observer, now - 10_000, arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/"), arrayOf("30382:followers", service, "wss://scores.example.com/")), "", "")
            check(store.batchInsert(listOf(list)).single() is IEventStore.InsertOutcome.Accepted)
            store.awaitTrustProjection()
            val subjects = List(N) { hex() }

            for (run in 1..RUNS) {
                val at = now - 5_000 + run * 300L
                val rankOf = { i: Int -> (i * 7 + run * 13) % 100 }
                val cards = subjects.mapIndexed { i, s -> card(service, s, at, rankOf(i)) }
                val before = STAGES.associateWith { stage(it) }
                val batches = ConcurrentLinkedQueue(cards.chunked(BATCH))
                val latencies = ConcurrentLinkedQueue<Long>()
                val accepted = AtomicInteger()
                val t0 = System.nanoTime()
                coroutineScope {
                    List(WORKERS) {
                        async {
                            while (true) {
                                val b = batches.poll() ?: break
                                val b0 = System.nanoTime()
                                accepted.addAndGet(store.batchInsert(b).count { it is IEventStore.InsertOutcome.Accepted })
                                latencies += (System.nanoTime() - b0) / 1_000_000
                            }
                        }
                    }.awaitAll()
                }
                val ingestSecs = (System.nanoTime() - t0) / 1e9
                val s0 = System.nanoTime()
                store.awaitTrustProjection()
                val settleSecs = (System.nanoTime() - s0) / 1e9
                val sorted = latencies.sorted()
                println(
                    "run $run: ${cards.size} cards, ${accepted.get()} accepted, ingest %.1fs (%.0f cards/s), settle %.1fs; batch ms p50=%d p90=%d max=%d over %d batches".format(
                        ingestSecs,
                        cards.size / ingestSecs,
                        settleSecs,
                        sorted[sorted.size / 2],
                        sorted[sorted.size * 9 / 10],
                        sorted.last(),
                        sorted.size,
                    ),
                )
                for (name in STAGES) {
                    val a = stage(name)
                    val b = before.getValue(name)
                    val calls = a.calls - b.calls
                    if (calls == 0L && a.totalNanos == b.totalNanos) continue
                    println("  %-24s %8.1fs over %6d calls (%.1f per batch, mean %.0f ms)".format(name, (a.totalNanos - b.totalNanos) / 1e9, calls, calls.toDouble() / sorted.size, if (calls > 0) (a.totalNanos - b.totalNanos) / 1e6 / calls else 0.0))
                }
                // The cells must carry THIS run's rank: a fast path that lands
                // the wrong cell is not a speed-up.
                var wrong = 0
                val sample = subjects.indices.shuffled(rnd).take(200)
                VespaReputationIndex(url).use { reps ->
                    for (i in sample) {
                        val rep = reps.get(subjects[i])
                        if (rep?.influenceScores?.get(ServiceKey(service)) != rankOf(i)) wrong++
                    }
                }
                println("  cells checked: ${sample.size - wrong}/${sample.size} carry run $run's rank")
            }
            store.close()
        }
}
