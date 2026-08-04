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
package com.nosfabrica.vespa.eventstore

import com.vitorpamplona.quartz.nip01Core.store.RejectionReason

/** A SEMANTIC insert rejection (duplicate, replaced, or blocked). Transient engine failures are NOT this; they propagate. */
class RejectedException(
    message: String,
) : Exception(message)

/** The insert-rejection reasons — Quartz's shared vocabulary plus the one Vespa-specific reason. */
internal object Rejections {
    const val EXPIRED = RejectionReason.EXPIRED
    const val DUPLICATE = RejectionReason.DUPLICATE
    const val DELETED = RejectionReason.DELETED
    const val VANISHED = RejectionReason.VANISHED
    const val REPLACED = RejectionReason.REPLACED
    const val INSERT_FAILED = RejectionReason.INSERT_FAILED

    // One constant string, not one per field/code point: callers tally
    // rejections by reason, and a per-event reason fragments that tally.
    const val UNSTORABLE_TEXT = "blocked: text carries a code point the engine cannot store"
}
