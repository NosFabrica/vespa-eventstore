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
package com.nosfabrica.vespa.eventstore.engine.async

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay

/**
 * The pauses before each retry of a SUPERSESSION'S SWEEP — the removal of the
 * versions a winner replaced, after the winner landed ([retryingSweep]). Two
 * retries, 0.6 s at most: the sweep runs under the store's writer lock, so the
 * budget is sized for a transient feed failure (a connection reset, a node
 * restarting its bucket), not for an engine that is down.
 */
val SWEEP_RETRY_PAUSES_MILLIS: List<Long> = listOf(100, 500)

/**
 * Run [block] — a supersession's sweep — and retry it after each of [pauses]
 * when it throws anything but a cancellation, then throw the last failure.
 *
 * A sweep is an idempotent removal of documents the caller already decided
 * are stale, and it runs AFTER the winner was written: if it fails, the caller
 * (a batch) throws, its winner is re-offered as a DUPLICATE, and nothing would
 * ever look at the address again — two versions served until the next write to
 * it. A retry inside the commit is the cheap half of the cure; the store's
 * owed-sweep memory (`ingest/OwedSweeps`) is the other.
 */
suspend fun <T> retryingSweep(
    pauses: List<Long> = SWEEP_RETRY_PAUSES_MILLIS,
    block: suspend () -> T,
): T {
    var attempt = 0
    while (true) {
        try {
            return block()
        } catch (e: CancellationException) {
            throw e
        } catch (t: Throwable) {
            if (attempt >= pauses.size) throw t
            delay(pauses[attempt++])
        }
    }
}
