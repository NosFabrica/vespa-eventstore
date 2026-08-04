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
package com.nosfabrica.vespa.eventstore.engine

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit

/**
 * Engine queries a batch stage keeps in flight. UNBOUNDED fan-out times out
 * proton's summary stage (`504 Summary data is incomplete`) — measured, on
 * heavy bulk-stage queries; lighter per-filter REQ queries can raise it via
 * `VESPA_QUERY_FANOUT` for a deployment's own hardware.
 */
val QUERY_FANOUT: Int = System.getenv("VESPA_QUERY_FANOUT")?.toIntOrNull()?.coerceAtLeast(1) ?: 4

/**
 * Fan-out for address-keyed conditional PUTs ([EventIndex.putIfNewer]) in the
 * bulk path. WRITES pipeline safely over the feed client's HTTP/2 streams (no
 * summary-stage 504), so this runs far higher than [QUERY_FANOUT]. 32 is the
 * measured sweet spot: the draft-churn A/B climbs 4→16 (939→1157 EPS) then
 * plateaus, 64 regresses slightly — beyond ~16 the per-batch dedup read is
 * the limiter. Overridable via VESPA_PUT_FANOUT.
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
 * keeps none: collecting a fan-out that recalls tens of docs per item holds
 * `items × docs` alive — the product that runs a heap out — where consuming
 * keeps only `concurrency × docs`. [consume] is serialized (an unsynchronized
 * accumulator is fine); results arrive in completion order, not list order.
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
