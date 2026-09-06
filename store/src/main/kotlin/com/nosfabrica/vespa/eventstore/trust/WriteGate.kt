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
package com.nosfabrica.vespa.eventstore.trust

/**
 * Exclusion around one short burst of projection writes.
 *
 * What is actually held is the implementation's business: the background
 * drainer and [TrustReconciler] pass the store's trust gate, so a long walk
 * shares it with ingest one slice at a time, while a caller that ALREADY holds
 * the writer lock passes [DIRECT] and the block runs where it stands.
 *
 * Taken per SLICE, never around a whole batch. Holding it for one
 * 20,000-subject batch measured 13 minutes on staging with every other writer
 * — ingest, the monitor's verdicts, the sweeps — queued behind it. Lower is
 * fairer and costs only the mutex round trip, microseconds against seconds of
 * work.
 */
internal fun interface WriteGate {
    suspend fun holding(block: suspend () -> Unit)

    companion object {
        /** For a caller already inside the writer lock: no second acquisition. */
        val DIRECT = WriteGate { it() }
    }
}
