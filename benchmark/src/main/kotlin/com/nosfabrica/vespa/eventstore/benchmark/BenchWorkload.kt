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

import com.vitorpamplona.quartz.nip01Core.core.Event

/**
 * Sampled, deterministic query parameters drawn from the corpus — ONE workload
 * shared by the latency suite ([EventStoreBenchmark]), the real-Vespa runner
 * ([VespaRunner]) and the concurrent bench ([ThroughputBench]), so every suite
 * times the same shapes with the same inputs.
 *
 * Besides the single-value samplers, it serves the LIST-shaped parameters that
 * dominate real relay traffic and that single-value benchmarks miss entirely:
 * a follow-feed REQ carries the observer's whole contact list (hundreds of
 * authors), a thread fetch carries dozens of ids, a notification subscription
 * carries a wide `#p` value list. [authorList]/[idList]/[pList] return windows
 * over the sampled pools that ROTATE with the rep index, so successive reps
 * query different (but reproducible) value sets instead of re-hitting one
 * cache-hot list.
 */
class BenchWorkload private constructor(
    private val authors: List<String>,
    private val ids: List<String>,
    private val pValues: List<String>,
    private val terms: List<String>,
) {
    fun author(i: Int) = authors[Math.floorMod(i, authors.size)]

    fun id(i: Int) = ids[Math.floorMod(i, ids.size)]

    fun term(i: Int) = terms[Math.floorMod(i, terms.size)]

    /** [n] authors starting at a rotating offset — a follow-list of size n. */
    fun authorList(
        i: Int,
        n: Int,
    ) = window(authors, i, n)

    /** [n] event ids starting at a rotating offset — an id-set fetch. */
    fun idList(
        i: Int,
        n: Int,
    ) = window(ids, i, n)

    /** [n] `#p` values that actually occur in the corpus — a mentions/notification list. */
    fun pList(
        i: Int,
        n: Int,
    ) = window(pValues, i, n)

    private fun window(
        pool: List<String>,
        i: Int,
        n: Int,
    ): List<String> {
        val m = n.coerceAtMost(pool.size)
        val start = Math.floorMod(i * WINDOW_STRIDE, pool.size)
        return List(m) { pool[(start + it) % pool.size] }
    }

    companion object {
        /** Co-prime with the pool sizes in practice, so windows cycle through the whole pool. */
        private const val WINDOW_STRIDE = 131

        fun from(corpus: List<Event>): BenchWorkload {
            val authors =
                corpus
                    .map { it.pubKey }
                    .distinct()
                    .shuffled(kotlin.random.Random(1))
                    .take(1000)
            val ids = corpus.map { it.id }.shuffled(kotlin.random.Random(2)).take(2000)
            val pValues =
                corpus
                    .flatMap { e -> e.tags.filter { it.size >= 2 && it[0] == "p" }.map { it[1] } }
                    .distinct()
                    .shuffled(kotlin.random.Random(3))
                    .take(2000)
            return BenchWorkload(
                authors.ifEmpty { listOf("a".repeat(64)) },
                ids.ifEmpty { listOf("0".repeat(64)) },
                pValues.ifEmpty { listOf("a".repeat(64)) },
                "nostr bitcoin coffee freedom relay privacy vespa trust".split(" "),
            )
        }
    }
}
