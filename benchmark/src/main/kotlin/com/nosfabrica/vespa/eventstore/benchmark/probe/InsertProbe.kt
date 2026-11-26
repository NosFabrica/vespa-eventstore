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

import com.nosfabrica.vespa.eventstore.benchmark.harness.Backends
import com.nosfabrica.vespa.eventstore.benchmark.harness.BenchResults
import com.nosfabrica.vespa.eventstore.benchmark.harness.NostrCorpus
import com.nosfabrica.vespa.eventstore.benchmark.harness.SampledLatencies
import com.nosfabrica.vespa.eventstore.benchmark.harness.Warmup
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip02FollowList.ContactListEvent
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.system.measureNanoTime

/**
 * The focused per-event insert probe (`BENCH_INSERT_PROBE=1`): fresh inserts of
 * an id-band-disjoint mini corpus onto whatever the live Vespa already holds,
 * then a re-insert of the same events (all duplicates — the relay rebroadcast
 * case). Prints events/sec + µs/event for both. Everything runs through the
 * real store stack (feed client, admission guards, conditional put), so the
 * numbers are the store's, not a synthetic client's.
 *
 * Then the same two arms for `BENCH_PROBE_BIG` (40) LARGE documents: kind 3s of
 * `BENCH_PROBE_BIG_TAGS` (3000) `p` tags each, one per fresh author, which is
 * where a dup probe that reads the stored document pays for it (the generated
 * corpus's follow lists hold 5-45 tags). Each arm also prints the writer lock's
 * hold over it (`lock.ingest.hold`: total, and per insert), which is the
 * cost a slow probe passes on to every other writer.
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
        val bigCount = System.getenv("BENCH_PROBE_BIG")?.toIntOrNull() ?: 40
        val bigTags = System.getenv("BENCH_PROBE_BIG_TAGS")?.toIntOrNull() ?: 3_000
        val big = bigFollowLists(bigCount, bigTags, band, seed)
        Backends.openVespa(url).use { store ->
            Warmup.warm(store) // steady-state, not JIT warmup — see Warmup
            arms(store, events, "")
            if (big.isNotEmpty()) arms(store, big, " k3x$bigTags")
        }
    }

    /** The fresh arm, then the duplicate arm, over [events]. */
    private suspend fun arms(
        store: IEventStore,
        events: List<Event>,
        label: String,
    ) {
        var ok = 0
        val freshLat = SampledLatencies()
        val freshHold = hold()
        val freshNanos = measureNanoTime { for (e in events) if (freshLat.timed { runCatching { store.insert(e) } }.isSuccess) ok++ }
        report("fresh insert()$label", ok, events.size, freshNanos, freshLat, freshHold)
        var dup = 0
        val dupLat = SampledLatencies()
        val dupHold = hold()
        val dupNanos = measureNanoTime { for (e in events) if (dupLat.timed { runCatching { store.insert(e) } }.isFailure) dup++ }
        report("dup re-insert()$label", dup, events.size, dupNanos, dupLat, dupHold)
    }

    /**
     * [count] kind 3s of [tags] `p` tags, each by its own author drawn from [band]'s seed, so
     * each one is the only version of its address and its re-insert is a plain duplicate.
     */
    private fun bigFollowLists(
        count: Int,
        tags: Int,
        band: Long,
        seed: Long,
    ): List<Event> {
        val rnd = Random(seed * 31 + band)

        fun hex() = (0 until 32).joinToString("") { "%02x".format(rnd.nextInt(256)) }
        val now = System.currentTimeMillis() / 1000
        return List(count) { i ->
            val follows = Array(tags) { arrayOf("p", hex()) }
            Event(hex(), hex(), now - i, ContactListEvent.KIND, follows, "", "")
        }
    }

    /** The writer lock's cumulative hold so far: the baseline an arm's own hold is read against. */
    private fun hold(): IngestStats.Stage = IngestStats.snapshot()[HOLD] ?: IngestStats.Stage(0, 0, 0)

    private fun report(
        op: String,
        counted: Int,
        total: Int,
        nanos: Long,
        lat: SampledLatencies,
        holdBefore: IngestStats.Stage,
    ) {
        val secs = nanos / 1e9
        // Per insert, not per hold: the stage is booked by `add` (`IngestStats.holding`), which
        // counts no calls, and each insert() takes the writer lock once.
        val heldNanos = hold().totalNanos - holdBefore.totalNanos
        println(
            String.format(
                "%-24s %6d/%d %10.1f events/sec %10.1f µs/event  %s  $HOLD %.1f ms (%.0f µs/insert)",
                op,
                counted,
                total,
                total / secs,
                nanos / 1000.0 / total,
                lat.summary(),
                heldNanos / 1e6,
                heldNanos / 1000.0 / total,
            ),
        )
        BenchResults.record(op, lat, "events_per_sec" to total / secs, "hold_us_per_insert" to heldNanos / 1000.0 / total)
    }

    private const val HOLD = "lock.ingest.hold"
}
