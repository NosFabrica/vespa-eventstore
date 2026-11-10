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
import com.nosfabrica.vespa.eventstore.benchmark.harness.Backends
import com.nosfabrica.vespa.eventstore.benchmark.harness.SampledLatencies
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Issue #160: does a KIND-SCOPED full-text reindex stall the process's other
 * writes, and by how much?
 *
 * `reindexPage` used to read its visit page while holding `writes`. On a
 * sparse selection a page has to scan many documents to fill, so this
 * measures, on a real Vespa, (1) how long each scoped page takes and (2) the
 * latency of plain kind-1 `insert()`s issued from the SAME store while the walk
 * runs, against the same inserts with no walk. Numbers before and after the
 * fix: benchmark/README.md, "A scoped reindex holding the write lock".
 *
 * Corpus (idempotent, deterministic ids): PROBE_DOCS kind-1 filler with every
 * PROBE_EVERY-th document a kind-30023 instead, fed straight through the index
 * WITHOUT search columns, so the first walk finds every 30023 drifted and
 * re-puts it, the way a repair after an extractor change would.
 *
 * Env: VESPA_URL (default http://localhost:8080), PROBE_DOCS (default
 * 2_000_000), PROBE_EVERY (default 200, i.e. 0.5% sparse), PROBE_WALKS
 * (default 2: the first re-puts, later ones only visit), PROBE_WRITE_MS (gap
 * between probe inserts, default 25), PROBE_LOAD (`read` times lock-free store
 * reads instead of inserts).
 */
object ReindexLockProbe {
    private const val SPARSE_KIND = 30023

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val target = System.getenv("PROBE_DOCS")?.toIntOrNull() ?: 2_000_000
            val every = System.getenv("PROBE_EVERY")?.toIntOrNull() ?: 200
            val walks = System.getenv("PROBE_WALKS")?.toIntOrNull() ?: 2
            val gapMs = System.getenv("PROBE_WRITE_MS")?.toLongOrNull() ?: 25L
            // "read" swaps the probe inserts for lock-free store reads: the
            // control for a visit that overlaps this process's other traffic.
            val reads = System.getenv("PROBE_LOAD") == "read"
            println("reindex lock probe @ $url  docs=$target  sparse=1/$every (kind $SPARSE_KIND)")

            Backends.openVespa(url).use { store ->
                feedUpTo(url, target, every)
                val writeSeq = AtomicLong(System.nanoTime())

                val idle = SampledLatencies()
                var idleMax = 0L
                repeat(200) {
                    val t0 = System.nanoTime()
                    probeOp(store, reads, writeSeq.incrementAndGet())
                    val d = System.nanoTime() - t0
                    idle.record(d)
                    idleMax = maxOf(idleMax, d)
                    delay(gapMs)
                }
                println("${if (reads) "reads" else "inserts"}, no walk:      ${idle.summary()} max=${SampledLatencies.fmt(idleMax)}")

                repeat(walks) { walk ->
                    val during = SampledLatencies()
                    var duringMax = 0L
                    var writes = 0
                    val running = AtomicBoolean(true)
                    val writer =
                        launch {
                            while (isActive && running.get()) {
                                val t0 = System.nanoTime()
                                probeOp(store, reads, writeSeq.incrementAndGet())
                                val d = System.nanoTime() - t0
                                during.record(d)
                                duringMax = maxOf(duringMax, d)
                                writes++
                                delay(gapMs)
                            }
                        }
                    val pages = SampledLatencies()
                    var pageMax = 0L
                    var docs = 0
                    val walkNanos =
                        async {
                            val t0 = System.nanoTime()
                            var cursor: String? = null
                            do {
                                val p0 = System.nanoTime()
                                val progress = reindex(store, cursor)
                                val d = System.nanoTime() - p0
                                pages.record(d)
                                pageMax = maxOf(pageMax, d)
                                docs += progress.processedThisBatch
                                cursor = progress.cursor
                            } while (!progress.done)
                            System.nanoTime() - t0
                        }.await()
                    running.set(false)
                    writer.join()
                    println(
                        "walk ${walk + 1}: ${"%.1f".format(walkNanos / 1e9)}s, $docs docs of kind $SPARSE_KIND  " +
                            "pages ${pages.summary()} max=${SampledLatencies.fmt(pageMax)}",
                    )
                    println("  ${if (reads) "reads" else "inserts"} during walk: $writes  ${during.summary()} max=${SampledLatencies.fmt(duringMax)}")
                }
            }
        }

    private suspend fun probeOp(
        store: VespaEventStore,
        reads: Boolean,
        seq: Long,
    ) {
        if (reads) {
            store.query<Event>(Filter(kinds = listOf(1), authors = listOf((seq % 512).toString(16).padStart(64, 'a')), limit = 10))
        } else {
            store.insert(note(seq))
        }
    }

    private suspend fun reindex(
        store: VespaEventStore,
        cursor: String?,
    ) = store.reindexFullTextSearch(listOf(SPARSE_KIND), cursor, 1_000)

    private fun note(seq: Long): Event =
        Event(
            id = "f" + seq.toString(16).padStart(63, '0'),
            pubKey = "b".repeat(64),
            createdAt = 1_800_000_000L + (seq % 1_000_000),
            kind = 1,
            tags = emptyArray(),
            content = "probe write $seq",
            sig = "e".repeat(128),
        )

    private suspend fun feedUpTo(
        url: String,
        target: Int,
        every: Int,
    ) {
        val index = VespaEventIndex(url)
        try {
            val have = index.count(EventQuery(kinds = listOf(1))) + index.count(EventQuery(kinds = listOf(SPARSE_KIND)))
            println("corpus has $have docs; target $target")
            if (have >= target) return
            var fed = have.toInt()
            val started = System.nanoTime()
            while (fed < target) {
                val n = minOf(2_000, target - fed)
                val docs =
                    List(n) { i ->
                        val seq = fed + i
                        val sparse = seq % every == 0
                        val author = (seq % 512).toString(16).padStart(64, 'a')
                        EventDoc(
                            id = seq.toString(16).padStart(64, '0'),
                            pubkey = author,
                            createdAt = 1_700_000_000L + seq,
                            kind = if (sparse) SPARSE_KIND else 1,
                            tags = if (sparse) listOf(listOf("d", "article-$seq"), listOf("title", "Article number $seq")) else emptyList(),
                            content = if (sparse) "long form article body $seq about reindexing" else "filler note $seq",
                            sig = "e".repeat(128),
                        )
                    }
                index.putAll(docs)
                fed += n
                if (fed % 100_000 < 2_000) {
                    val rate = (fed - have) * 1e9 / (System.nanoTime() - started)
                    println("  fed $fed / $target (${String.format("%,.0f", rate)} docs/s)")
                }
            }
        } finally {
            index.close()
        }
    }
}
