/*
 * Copyright (c) 2026 Vitor Pamplona
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

import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.FeedClientBuilder
import ai.vespa.feed.client.OperationParameters
import ai.vespa.feed.client.Result
import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.time.Duration
import kotlin.random.Random
import kotlin.system.measureNanoTime

/**
 * `BENCH_CONDPUT=1`: does Vespa's server-side test-and-set beat our client-side
 * read-then-supersede for the replaceable/draft-churn flood (clients resending
 * old versions constantly)?
 *
 * Workload: [BENCH_CP_ADDRS] addressable draft addresses (kind 31234, one d-tag
 * each), a stream of [BENCH_CP_OPS] saves where [BENCH_CP_OLDPCT]% carry a
 * created_at BELOW the address's running head (stale resends that must be
 * rejected) and the rest advance it.
 *
 * A) read-then-supersede — the real store: feed the stream through
 *    NostrSemanticsStore.batchInsert (dedup + supersession searches + writes).
 * B) conditional put — address-keyed docid + `condition: event.created_at < X`
 *    with create-if-absent: one atomic write per event; the engine rejects the
 *    stale ones (conditionNotMet) with no client read.
 *
 * Both process the identical op stream (disjoint key spaces). Env: BENCH_CP_ADDRS
 * (5000), BENCH_CP_OPS (60000), BENCH_CP_OLDPCT (85), BENCH_BATCH (500).
 */
object CondPutProbe {
    private data class Op(
        val addr: Int,
        val createdAt: Long,
    )

    fun run(
        url: String,
        seed: Long,
    ) = runBlocking {
        val addrs = System.getenv("BENCH_CP_ADDRS")?.toIntOrNull() ?: 5_000
        val ops = System.getenv("BENCH_CP_OPS")?.toIntOrNull() ?: 60_000
        val oldPct = System.getenv("BENCH_CP_OLDPCT")?.toIntOrNull() ?: 85
        val batch = System.getenv("BENCH_BATCH")?.toIntOrNull() ?: 500

        // Build the churn stream: mostly stale resends, some advancing the head.
        val rnd = Random(seed)
        val heads = LongArray(addrs) { 1_700_000_000L }
        val stream =
            ArrayList<Op>(ops).apply {
                repeat(ops) {
                    val a = rnd.nextInt(addrs)
                    val stale = rnd.nextInt(100) < oldPct
                    val ts = if (stale) heads[a] - 1 - rnd.nextInt(5_000) else ++heads[a]
                    add(Op(a, ts))
                }
            }
        val staleCount =
            run {
                val h = LongArray(addrs) { Long.MIN_VALUE }
                stream.count { op -> (op.createdAt <= h[op.addr]).also { if (op.createdAt > h[op.addr]) h[op.addr] = op.createdAt } }
            }
        println("cond-put A/B: addrs=$addrs ops=$ops stale≈$staleCount (${staleCount * 100 / ops}%)  batch=$batch")

        // ---- B) conditional put, address-keyed (build first; it needs its own feed client) ----
        val feed =
            FeedClientBuilder
                .create(listOf(URI.create(url)))
                .setConnectionsPerEndpoint(32)
                .setMaxStreamPerConnection(128)
                .build()

        fun pk(a: Int) = "cd" + a.toString(16).padStart(62, '0')

        fun condFields(op: Op): String =
            """{"fields":{"id":"${op.createdAt.toString(16).padStart(64, '0')}","pubkey":"${pk(op.addr)}",""" +
                """"created_at":${op.createdAt},"kind":31234,"content":"draft","sig":"00","tags":"[]",""" +
                """"tag_index":["d:draft"],"owner":"${pk(op.addr)}","expires_at":9223372036854775807}}"""

        fun condParams(op: Op): OperationParameters =
            OperationParameters
                .empty()
                .createIfNonExistent(true)
                .testAndSetCondition("event.created_at < ${op.createdAt}")
                .timeout(Duration.ofSeconds(30))

        suspend fun runCond(sample: List<Op>): Pair<Long, Int> {
            var rejected = 0
            val nanos =
                measureNanoTime {
                    val futures =
                        sample.map { op ->
                            feed.put(DocumentId.of("event", "event", "cp-${op.addr}"), condFields(op), condParams(op))
                        }
                    futures.forEach { f ->
                        val r = f.get()
                        if (r.type() == Result.Type.conditionNotMet) rejected++
                    }
                }
            return nanos to rejected
        }

        // ---- A) read-then-supersede via the real store ----
        var counter = 0

        fun toEvent(op: Op): Event = Event((++counter).toString(16).padStart(64, '0'), pk(op.addr), op.createdAt, 31234, arrayOf(arrayOf("d", "draft")), "draft", "00")
        val storeEvents = stream.map { toEvent(it) }

        // Warm both paths on a slice, then time the full stream.
        val warm = stream.take(ops / 4)
        VespaEventStore.open(url).use { store ->
            runCond(warm)
            store.batchInsert(storeEvents.take(ops / 4))

            val aNanos = measureNanoTime { storeEvents.chunked(batch).forEach { store.batchInsert(it) } }
            val (bNanos, rejected) = runCond(stream)

            val aEps = ops / (aNanos / 1e9)
            val bEps = ops / (bNanos / 1e9)
            println(String.format("%-26s %10.0f events/sec  (%.1fs)", "A) read-then-supersede", aEps, aNanos / 1e9))
            println(String.format("%-26s %10.0f events/sec  (%.1fs)  rejected=%d server-side", "B) conditional put", bEps, bNanos / 1e9, rejected))
            println(String.format("B/A = %.2fx", bEps / aEps))
        }
        feed.close()
    }
}
