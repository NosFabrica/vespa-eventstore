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
package com.nosfabrica.vespa.eventstore.benchmark.harness

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.conformance.StoreConformance

/**
 * Correctness gate: this store and Quartz's SQLite store MUST answer every
 * NIP-01 filter identically — the same event ids, in the same newest-first
 * order, with the same NIP-45 counts. The battery, the comparison and the
 * report are quartz's [StoreConformance] (2.9.0), where every check names the
 * STORE-* rules it exercises, so a mismatch reads as "diverges from
 * STORE-D05" rather than as a bare failed filter. It replaced this module's
 * own `ParityCheck`, whose every filter quartz's battery holds (plus `#d`,
 * `tagsAll`, `since=until`, `limit=0` and multi-filter unions).
 *
 * What stays here is only the benchmark's plumbing: the printed block and the
 * [BenchResults] row. NIP-50 `search` is not in the battery: relevance is
 * engine-defined, so SQLite FTS5 and Vespa BM25 legitimately differ.
 */
object StoreParity {
    /**
     * Diff [candidate] against [reference], both already loaded with [corpus],
     * across [StoreConformance.battery] drawn from it; print the block and
     * record the row. Returns the report.
     */
    suspend fun run(
        corpus: List<Event>,
        referenceName: String,
        reference: IEventStore,
        candidateName: String,
        candidate: IEventStore,
    ): StoreConformance.Report {
        val report = StoreConformance.compare(reference, candidate, StoreConformance.battery(corpus))
        println("parity $candidateName vs $referenceName: ${report.summary()}")
        BenchResults.record("parity $candidateName vs $referenceName", "checks" to report.checks.toDouble(), "mismatches" to report.mismatches.size.toDouble())
        return report
    }
}
