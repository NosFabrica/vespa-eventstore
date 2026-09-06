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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * WHICH DEGRADATION, ON WHICH QUERY SHAPE — the discriminator a degraded-read
 * counter does not carry.
 *
 * The cost ledger already counts degraded responses per rank profile, which
 * says a profile is being cut without saying WHY or how far. Vespa names four
 * reasons and they mean opposite things: `non-ideal-state` is a cluster still
 * settling and clears itself, `match-phase` is the engine truncating the match
 * set and does not. Twice this deployment took hours to tell those apart, once
 * concluding a performance problem that did not exist while the real answer —
 * `match-phase: true` at 54% — sat in a status line nothing printed.
 *
 * SHAPES, NEVER TERMS. The key is the rank profile plus which clause KINDS the
 * query carried, never their values: a yql carries what somebody searched for,
 * and this is read from places a search term must not reach. That is the same
 * rule the pulse's gate exists to enforce.
 *
 * Counters, cumulative since process start; two snapshots subtract.
 */
object DegradedReads {
    class Reading(
        val profile: String,
        /** The degradation flags Vespa actually SET, sorted — it lists the false ones too. */
        val flags: String,
        val shape: String,
        val refused: Boolean,
        val count: Long,
        val lastCoverage: Int,
        val lastDocuments: Long,
    )

    private class Tally(
        val profile: String,
        val flags: String,
        val shape: String,
        val refused: Boolean,
    ) {
        val count = AtomicLong()

        @Volatile var lastCoverage: Int = 0

        @Volatile var lastDocuments: Long = 0
    }

    private val tallies = ConcurrentHashMap<String, Tally>()

    fun record(
        profile: String,
        flags: Set<String>,
        shape: String,
        refused: Boolean,
        coverage: Int,
        documents: Long,
    ) {
        val flagText = flags.sorted().joinToString("+").ifEmpty { "none-named" }
        val key = "$profile|$flagText|$shape|$refused"
        val t = tallies.computeIfAbsent(key) { Tally(profile, flagText, shape, refused) }
        t.count.incrementAndGet()
        t.lastCoverage = coverage
        t.lastDocuments = documents
    }

    fun snapshot(): List<Reading> =
        tallies.values
            .map { Reading(it.profile, it.flags, it.shape, it.refused, it.count.get(), it.lastCoverage, it.lastDocuments) }
            .sortedByDescending { it.count }

    fun reset() = tallies.clear()

    /** Empty while every read is clean, so a status display can splice it in unconditionally. */
    fun line(): String {
        val rows = snapshot()
        if (rows.isEmpty()) return ""
        return "degraded-reads " +
            rows.take(TOP).joinToString("; ") { r ->
                "${r.profile}/${r.shape} ${r.flags} x${r.count}" +
                    (if (r.refused) " REFUSED" else " served") +
                    " (last ${r.lastCoverage}%, ${r.lastDocuments} docs)"
            }
    }

    private const val TOP = 6
}
