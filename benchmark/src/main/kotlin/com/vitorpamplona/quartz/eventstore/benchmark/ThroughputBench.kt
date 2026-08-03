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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.lang.management.ManagementFactory

/**
 * Concurrent, duration-based query throughput — the metric a relay actually
 * lives on. The latency suite issues one query at a time (throughput = 1/latency,
 * capped by a single round trip); this drives [concurrency] coroutines against the
 * store at once, which is how real load arrives and how Vespa is meant to be fed.
 * Reports events/sec and queries/sec at rising concurrency, plus GC time and
 * allocation for the run — the target is 50k events/sec after warmup, with GC
 * kept off the hot path.
 *
 * Measured AFTER a warmup pass so JIT is settled. GC and bytes-allocated come
 * from the JVM's own beans, so "how much garbage per event" is visible directly.
 */
object ThroughputBench {
    private data class Sample(
        val queries: Long,
        val events: Long,
        val wallNanos: Long,
        val gcCount: Long,
        val gcMillis: Long,
        val bytesAllocated: Long,
    )

    fun run(
        store: IEventStore,
        corpus: List<Event>,
        concurrencies: List<Int>,
        durationMs: Long,
    ) = runBlocking {
        val w = BenchWorkload.from(corpus)
        val shapes =
            listOf<Triple<String, Int, suspend (Int) -> Int>>(
                Triple("id-lookup(16)", 16) { i -> store.query<Event>(Filter(ids = w.idList(i, 16))).size },
                Triple("author-timeline", 50) { i -> store.query<Event>(Filter(authors = listOf(w.author(i)), limit = 50)).size },
                Triple("kind-scan(200)", 200) { store.query<Event>(Filter(kinds = listOf(1), limit = 200)).size },
                // List-shaped REQs under concurrency — the follow-feed is THE
                // relay workload (every connected client holds one open), and the
                // id-set fetch is the >ID_GET_FANOUT path (one search, not N gets).
                Triple("follow-feed(a300)", 500) { i -> store.query<Event>(Filter(authors = w.authorList(i, 300), kinds = listOf(1, 6, 7), limit = 500)).size },
                Triple("ids-set(100)", 100) { i -> store.query<Event>(Filter(ids = w.idList(i, 100))).size },
            )

        println("\n-- concurrent throughput (events/sec after warmup; target 50,000) --")
        println(String.format("%-16s %5s %13s %12s %10s %12s  %s", "query", "conc", "events/sec", "queries/sec", "GC ms", "bytes/event", "latency tail"))
        for ((name, _, op) in shapes) {
            // Warm up this shape (JIT + connection pool) before measuring.
            drive(concurrency = 8, durationMs = (durationMs / 2).coerceAtLeast(1_000), op = op)
            for (c in concurrencies) {
                val lat = Latencies()
                val s = drive(c, durationMs, op, lat)
                val secs = s.wallNanos / 1e9
                val eps = s.events / secs
                val qps = s.queries / secs
                val bytesPerEvent = if (s.events > 0) s.bytesAllocated / s.events else 0
                println(
                    String.format(
                        "%-16s %5d %13s %12s %10d %12d  %s",
                        name,
                        c,
                        num(eps),
                        num(qps),
                        s.gcMillis,
                        bytesPerEvent,
                        lat.summary(),
                    ),
                )
                BenchResults.record("$name@$c", lat, "events_per_sec" to eps, "qps" to qps, "bytes_per_event" to bytesPerEvent.toDouble())
            }
        }
    }

    /** Drive [op] from [concurrency] coroutines until [durationMs] elapses; account GC + allocation over the window. */
    private suspend fun drive(
        concurrency: Int,
        durationMs: Long,
        op: suspend (Int) -> Int,
        lat: Latencies? = null,
    ): Sample {
        val gcBeans = ManagementFactory.getGarbageCollectorMXBeans()

        fun gcCount() = gcBeans.sumOf { it.collectionCount.coerceAtLeast(0) }

        fun gcTime() = gcBeans.sumOf { it.collectionTime.coerceAtLeast(0) }
        val threadBean = ManagementFactory.getThreadMXBean() as? com.sun.management.ThreadMXBean

        val gc0 = gcCount()
        val gcT0 = gcTime()
        val alloc0 = allocatedBytes(threadBean)
        val start = System.nanoTime()
        val deadline = start + durationMs * 1_000_000
        val queries =
            java.util.concurrent.atomic
                .AtomicLong()
        val events =
            java.util.concurrent.atomic
                .AtomicLong()

        withContext(Dispatchers.Default) {
            repeat(concurrency) { worker ->
                launch {
                    var i = worker
                    var q = 0L
                    var e = 0L
                    while (System.nanoTime() < deadline) {
                        val t0 = System.nanoTime()
                        e += runCatching { op(i) }.getOrDefault(0)
                        lat?.record(System.nanoTime() - t0)
                        q++
                        i += concurrency
                    }
                    queries.addAndGet(q)
                    events.addAndGet(e)
                }
            }
        }
        val wall = System.nanoTime() - start
        return Sample(
            queries = queries.get(),
            events = events.get(),
            wallNanos = wall,
            gcCount = gcCount() - gc0,
            gcMillis = gcTime() - gcT0,
            bytesAllocated = (allocatedBytes(threadBean) - alloc0).coerceAtLeast(0),
        )
    }

    /** Total bytes allocated across all live threads (best-effort; 0 if the bean is unavailable). */
    private fun allocatedBytes(bean: com.sun.management.ThreadMXBean?): Long {
        bean ?: return 0
        val ids = ManagementFactory.getThreadMXBean().allThreadIds
        return bean.getThreadAllocatedBytes(ids).sumOf { it.coerceAtLeast(0) }
    }

    private fun num(v: Double) = if (v >= 1000) String.format("%,d", v.toLong()) else String.format("%.1f", v)
}
