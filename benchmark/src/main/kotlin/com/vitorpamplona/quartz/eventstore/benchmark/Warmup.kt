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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking

/**
 * Warm the client JVM and — for real Vespa — the engine's jdisc JIT before a
 * TIMED ingest, so reported rates are steady state, not JVM/JIT warmup.
 *
 * A freshly-deployed Vespa spends ~30s of its FIRST feed JIT-compiling the
 * Jetty/jdisc/docproc/messagebus pipeline to C2 (profiled: cumulative compile
 * time +33s in feed round 1, then ~0 by round 3), so a cold ingest reads ~1.5x
 * slower than warm (≈1,000 vs ≈1,500–1,800 EPS on the dev box). The query suites
 * already warm per shape (`repeat(warm){op}`); this covers the ingest side those
 * loops don't. Disable with `BENCH_WARMUP=0`.
 *
 * Feeds [n] id-disjoint throwaway events (band 0x9, so never colliding with a
 * seed-42/default-band corpus), then — when [cleanup] — deletes them so the
 * measured corpus stays clean for a store that will also be queried. Pass
 * `cleanup=false` for a throwaway store that is closed right after.
 */
object Warmup {
    val N = System.getenv("BENCH_WARMUP")?.toIntOrNull() ?: 15_000

    fun warm(
        store: IEventStore,
        n: Int = N,
        cleanup: Boolean = true,
    ) {
        if (n <= 0) return
        val junk = NostrCorpus.generate(NostrCorpus.Config(size = n, seed = 41, idBand = 0x9L))
        runBlocking {
            junk.chunked(1_000).forEach { store.batchInsert(it) }
            // Delete in small id chunks: one giant Filter(ids=…) makes the store search
            // + summary-fetch all N at once, which times out (504) on real Vespa. Small
            // chunks keep each delete's recall bounded.
            if (cleanup) junk.map { it.id }.chunked(200).forEach { store.delete(Filter(ids = it)) }
        }
    }

    /** Warm the JVM/engine via a throwaway store from [factory] (fed, not cleaned, then closed). */
    fun warmVia(
        n: Int = N,
        factory: () -> IEventStore,
    ) {
        if (n <= 0) return
        factory().use { warm(it, n, cleanup = false) }
    }
}
