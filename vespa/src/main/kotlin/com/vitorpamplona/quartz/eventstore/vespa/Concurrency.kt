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
package com.vitorpamplona.quartz.eventstore.vespa

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit

/**
 * How many engine queries a batch stage keeps in flight. Serialized round trips
 * starve a batch, but UNBOUNDED fan-out is worse: a dozen concurrent
 * multi-thousand-summary queries time out proton's summary stage (`504 Summary
 * data is incomplete`). This is measured, not hypothetical.
 */
const val QUERY_FANOUT = 4

/**
 * Fan-out for address-keyed conditional PUTs ([EventIndex.putIfNewer]) in the
 * bulk path. Unlike [QUERY_FANOUT], these are WRITES — they pipeline safely over
 * the feed client's HTTP/2 streams (no summary-stage 504), so this runs far
 * higher to keep the conditional puts in flight the way the raw feed does.
 * Overridable via VESPA_PUT_FANOUT for deployment tuning. 32 is the measured
 * sweet spot on the dev box: the draft-churn A/B climbs 4→16 (939→1157 EPS) then
 * plateaus, and 64 regresses slightly (scheduling overhead) — beyond ~16 the
 * store's per-batch dedup read, not the put concurrency, is the limiter.
 */
val PUT_FANOUT: Int = System.getenv("VESPA_PUT_FANOUT")?.toIntOrNull() ?: 32

/** Map [items] through [f] with at most [concurrency] in flight; results keep item order. */
suspend fun <T, R> List<T>.mapBounded(
    concurrency: Int,
    f: suspend (T) -> R,
): List<R> =
    coroutineScope {
        val gate = Semaphore(concurrency.coerceAtLeast(1))
        map { item -> async { gate.withPermit { f(item) } } }.awaitAll()
    }

/**
 * Like [mapBounded], but folds each result into [consume] as it arrives and
 * keeps none of them.
 *
 * For stages whose results are an intermediate: a fan-out that recalls tens of
 * docs per item holds `items × docs` alive when the results are collected, and
 * that product is what runs a heap out — not the item list the batch size was
 * chosen to bound. Consuming as they land keeps only what is in flight,
 * `concurrency × docs`.
 *
 * [consume] is serialized, so it may fold into an ordinary unsynchronized
 * accumulator. Results arrive in completion order, not list order.
 */
suspend fun <T, R> List<T>.forEachBounded(
    concurrency: Int,
    produce: suspend (T) -> R,
    consume: suspend (R) -> Unit,
) {
    val gate = Semaphore(concurrency.coerceAtLeast(1))
    val lock = Mutex()
    coroutineScope {
        forEach { item ->
            launch {
                gate.withPermit {
                    val result = produce(item)
                    lock.withLock { consume(result) }
                }
            }
        }
    }
}
