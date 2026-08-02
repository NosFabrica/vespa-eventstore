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

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import kotlinx.coroutines.runBlocking

/**
 * Load the deterministic [NostrCorpus] into a live store — the setup step the
 * workload benches assume ("the store is assumed already loaded"): their read
 * filters sample this corpus's authors/ids, so realistic hit rates require it
 * resident. Idempotent by construction (deterministic ids, the store dedups).
 *
 * Env: VESPA_URL (default http://localhost:8080), BENCH_SIZE (30000),
 * BENCH_SEED (42), BENCH_BATCH (500).
 */
object CorpusLoad {
    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val size = System.getenv("BENCH_SIZE")?.toIntOrNull() ?: 30_000
            val seed = System.getenv("BENCH_SEED")?.toLongOrNull() ?: 42L
            val batch = System.getenv("BENCH_BATCH")?.toIntOrNull() ?: 500
            println("loading NostrCorpus(size=$size, seed=$seed) into $url ...")
            val corpus = NostrCorpus.generate(NostrCorpus.Config(size = size, seed = seed))
            val store = VespaEventStore.open(url)
            try {
                corpus.chunked(batch).forEachIndexed { i, c ->
                    store.batchInsert(c)
                    if ((i + 1) % 20 == 0) println("  fed ${(i + 1) * batch} / $size")
                }
            } finally {
                store.close()
            }
            println("loaded $size events")
        }
}
