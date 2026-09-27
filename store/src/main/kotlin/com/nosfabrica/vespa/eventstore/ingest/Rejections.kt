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
package com.nosfabrica.vespa.eventstore.ingest

import com.vitorpamplona.quartz.nip01Core.store.RejectionReason

/** The insert-rejection reasons — Quartz's shared vocabulary plus the one Vespa-specific reason. */
internal object Rejections {
    const val EXPIRED = RejectionReason.EXPIRED
    const val DUPLICATE = RejectionReason.DUPLICATE
    const val DELETED = RejectionReason.DELETED
    const val VANISHED = RejectionReason.VANISHED

    /**
     * A replaceable/addressable version that a stored one already beats (NIP-01:
     * newer `created_at`, or the same one and a lower id). `replaced:`, so a relay
     * answers `OK false` — DELIBERATELY not Quartz's `SUPERSEDED`.
     *
     * Quartz 28bf170f92 moved its SQLite store to `SUPERSEDED` ("duplicate: a newer
     * version …", STORE-W01/W02), which its `RelaySession` acks with `OK true`, to stop
     * MDK's `wn` retrying a same-second KeyPackage forever. This store does not follow:
     * NIP-01's third `OK` field is `true` when the event was ACCEPTED, and a stale
     * version is not written — nothing a later REQ could return. `OK true` would tell
     * the client its event is on this relay when it is not. `replaced:` is also what
     * strfry answers (`false, "replaced: have newer event"`), and "duplicate" would
     * misname it: this relay does not have THIS event, it has a newer one. A byte-for-
     * byte re-offer is still [DUPLICATE] (`OK true`), caught by the id check first.
     */
    const val REPLACED = RejectionReason.REPLACED
    const val INSERT_FAILED = RejectionReason.INSERT_FAILED

    // One constant string, not one per field/code point: callers tally
    // rejections by reason, and a per-event reason fragments that tally.
    const val UNSTORABLE_TEXT = "blocked: text carries a code point the engine cannot store"

    /** Every reason this store can produce — the CLOSED set the outcome tally is keyed by. */
    val ALL: List<String> = listOf(EXPIRED, DUPLICATE, DELETED, VANISHED, REPLACED, INSERT_FAILED, UNSTORABLE_TEXT)

    /**
     * The closed-set reason a rejection [message] belongs to.
     *
     * Reasons are PREFIXES with per-event detail after them ("duplicate: <id>"),
     * so the raw message is unbounded and tallying by it would key a counter by
     * event id — exactly the cardinality leak docs/telemetry.md §4 forbids.
     * This folds each message back onto the constant it started as; anything
     * unrecognised becomes [INSERT_FAILED] rather than a new key.
     */
    fun reasonOf(message: String?): String {
        if (message == null) return INSERT_FAILED
        return ALL.firstOrNull { message.startsWith(it) } ?: INSERT_FAILED
    }
}
