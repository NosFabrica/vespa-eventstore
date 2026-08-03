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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicLong

/**
 * Mixed read/write load (`BENCH_MIXED=1`): a relay never serves queries from a
 * quiet store — REQs arrive WHILE syncs feed it. The query-only suite misses
 * that interference entirely (and the earlier throughput runs hinted it is
 * real: bulk-read throughput measured right after an ingest dropped ~30% while
 * proton flushed). This drives both sides of the store at once and reports how
 * much each loses to the other:
 *
 *  1. read-only baseline   — R readers, no writers
 *  2. write-only baseline  — W writers, no readers
 *  3. mixed                — both simultaneously
 *
 * Writers stream `batchInsert` chunks of fresh id-band-disjoint events (the
 * multi-relay sync shape — plan/guard reads overlap outside the store's writer
 * lock, commits serialize behind it, exactly as in production). Readers loop
 * the follow-feed (300 authors) and author-timeline shapes. Everything runs
 * through the real store stack against a live Vespa.
 *
 * Tunables: BENCH_MIXED_WRITERS (2), BENCH_MIXED_READERS (16),
 * BENCH_MIXED_MS (8000 per mode), BENCH_MIXED_BATCH (500).
 */
object MixedLoadBench {
    fun run(
        url: String,
        mainCorpus: List<Event>,
        seed: Long,
    ) = runBlocking {
        val writers = System.getenv("BENCH_MIXED_WRITERS")?.toIntOrNull() ?: 2
        val readers = System.getenv("BENCH_MIXED_READERS")?.toIntOrNull() ?: 16
        val durMs = System.getenv("BENCH_MIXED_MS")?.toLongOrNull() ?: 8_000L
        val batch = System.getenv("BENCH_MIXED_BATCH")?.toIntOrNull() ?: 500

        println("mixed-load @ $url  writers=$writers readers=$readers ${durMs}ms/mode batch=$batch")
        val w = BenchWorkload.from(mainCorpus)

        // Each (mode, writer) pair gets its own id-band corpus so every phase
        // inserts genuinely FRESH events (re-feeding a band already written by
        // the write-only baseline would turn the mixed phase into an all-
        // duplicates run — different work). Sized so a writer cannot run dry.
        val perWriter = ((durMs / 1000) * 4_000).toInt().coerceAtLeast(8_000)

        // BENCH_MIXED_BAND_SEED gives an A/B rerun FRESH write bands while the
        // reader workload (sampled from the main corpus, BENCH_SEED) stays
        // identical — re-feeding earlier bands would silently turn the write
        // phases into an all-duplicates run, a different workload.
        val bandSeed = System.getenv("BENCH_MIXED_BAND_SEED")?.toLongOrNull() ?: seed

        fun feedsFor(mode: Int) =
            (0 until writers).map { i ->
                NostrCorpus.generate(NostrCorpus.Config(size = perWriter, seed = bandSeed + 100 + mode * 16 + i, idBand = 0xB0L + mode * 16 + i))
            }

        VespaEventStore.open(url).use { store ->
            suspend fun drive(
                nWriters: Int,
                nReaders: Int,
                label: String,
                feeds: List<List<Event>> = emptyList(),
            ) {
                val written = AtomicLong()
                val queries = AtomicLong()
                val hits = AtomicLong()
                val queryLat = Latencies()
                val start = System.nanoTime()
                val deadline = start + durMs * 1_000_000
                withContext(Dispatchers.Default) {
                    repeat(nWriters) { wi ->
                        launch {
                            val chunks = feeds[wi].chunked(batch).iterator()
                            while (System.nanoTime() < deadline && chunks.hasNext()) {
                                val c = chunks.next()
                                runCatching { store.batchInsert(c) }
                                written.addAndGet(c.size.toLong())
                            }
                        }
                    }
                    repeat(nReaders) { ri ->
                        launch {
                            var i = ri
                            while (System.nanoTime() < deadline) {
                                val r =
                                    queryLat.timed {
                                        runCatching {
                                            if (i % 2 == 0) {
                                                store.query<Event>(Filter(authors = w.authorList(i, 300), kinds = listOf(1, 6, 7), limit = 500))
                                            } else {
                                                store.query<Event>(Filter(authors = listOf(w.author(i)), limit = 50))
                                            }
                                        }.getOrNull()
                                    }
                                queries.incrementAndGet()
                                hits.addAndGet((r?.size ?: 0).toLong())
                                i += nReaders
                            }
                        }
                    }
                }
                val secs = (System.nanoTime() - start) / 1e9
                println(
                    String.format(
                        "%-12s %8s ingest-ev/s %10s query-q/s %12s query-ev/s  %s",
                        label,
                        num(written.get() / secs),
                        num(queries.get() / secs),
                        num(hits.get() / secs),
                        queryLat.summary(),
                    ),
                )
                BenchResults.record(
                    "mixed $label",
                    queryLat,
                    "ingest_events_per_sec" to written.get() / secs,
                    "query_qps" to queries.get() / secs,
                    "query_events_per_sec" to hits.get() / secs,
                )
            }

            // Short warmup so JIT/connections settle before any baseline.
            drive(0, readers, "(warmup)")
            drive(0, readers, "read-only")
            drive(writers, 0, "write-only", feedsFor(0))
            drive(writers, readers, "mixed", feedsFor(1))
            // Reads again right after the mixed ingest: how long the flush
            // shadow lasts on an idle-again store.
            drive(0, readers, "post-ingest")
        }
    }

    private fun num(v: Double) = if (v >= 1000) String.format("%,d", v.toLong()) else String.format("%.1f", v)
}
