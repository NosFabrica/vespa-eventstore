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

import kotlinx.coroutines.runBlocking
import kotlin.system.measureNanoTime

/**
 * The focused per-event insert probe (`BENCH_INSERT_PROBE=1`): fresh inserts of
 * an id-band-disjoint mini corpus onto whatever the live Vespa already holds,
 * then a re-insert of the same events (all duplicates — the relay rebroadcast
 * case). Prints events/sec + µs/event for both. Everything runs through the
 * real store stack (feed client, admission guards, conditional put), so the
 * numbers are the store's, not a synthetic client's.
 */
object InsertProbe {
    fun run(
        url: String,
        band: Long,
        size: Int,
        seed: Long,
    ) = runBlocking {
        println("insert probe @ $url  band=0x${band.toString(16)}  size=$size  seed=$seed")
        val events = NostrCorpus.generate(NostrCorpus.Config(size = size, seed = seed + 7, idBand = band))
        Backends.openVespa(url).use { store ->
            Warmup.warm(store) // steady-state, not JIT warmup — see Warmup
            var ok = 0
            val freshLat = Latencies()
            val freshNanos = measureNanoTime { for (e in events) if (freshLat.timed { runCatching { store.insert(e) } }.isSuccess) ok++ }
            report("fresh insert()", ok, size, freshNanos, freshLat)
            var dup = 0
            val dupLat = Latencies()
            val dupNanos = measureNanoTime { for (e in events) if (dupLat.timed { runCatching { store.insert(e) } }.isFailure) dup++ }
            report("dup re-insert()", dup, size, dupNanos, dupLat)
        }
    }

    private fun report(
        op: String,
        counted: Int,
        total: Int,
        nanos: Long,
        lat: Latencies,
    ) {
        val secs = nanos / 1e9
        println(String.format("%-16s %6d/%d %12.1f events/sec %10.1f µs/event  %s", op, counted, total, total / secs, nanos / 1000.0 / total, lat.summary()))
        BenchResults.record(op, lat, "events_per_sec" to total / secs)
    }
}
