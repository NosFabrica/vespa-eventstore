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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking

/**
 * Correctness gate: the Vespa store and Quartz's SQLite store MUST answer every
 * NIP-01 filter identically, and per spec. Both are loaded with the same corpus,
 * then a battery of filters runs against each and the result sets — and, for plain
 * filters, the newest-first ORDER (NIP-01: "events SHOULD be sorted by created_at
 * descending") — are compared. Counts are compared too. A mismatch means one
 * store disagrees with the other about NIP-01 recall, deletion/supersession
 * effects, or ordering — a correctness bug, and a hard failure of any
 * optimization.
 *
 * NIP-50 `search` is deliberately EXCLUDED from strict parity: the spec orders
 * search hits "by relevance", which is engine-defined, so SQLite FTS5 and Vespa
 * BM25 legitimately differ in ranking and typo-recall. Everything else is exact.
 *
 * The corpus advances created_at by a positive step per event, so every timestamp
 * is distinct — the newest-first order is a total order and the comparison is
 * unambiguous (no tie-break divergence to explain away).
 */
object ParityCheck {
    data class Spec(
        val name: String,
        val filter: Filter,
        /** Count-only checks skip the ordered-list comparison. */
        val countOnly: Boolean = false,
    )

    data class Result(
        val checks: Int,
        val mismatches: List<String>,
    ) {
        val ok get() = mismatches.isEmpty()
    }

    /**
     * Compare [candidate] against [reference] (both already loaded with the same
     * corpus) across the battery derived from [corpus]. Returns the mismatches.
     */
    fun run(
        corpus: List<Event>,
        referenceName: String,
        reference: IEventStore,
        candidateName: String,
        candidate: IEventStore,
    ): Result =
        runBlocking {
            val specs = battery(corpus)
            val mismatches = ArrayList<String>()
            for (spec in specs) {
                if (spec.countOnly) {
                    val rc = reference.count(spec.filter)
                    val cc = candidate.count(spec.filter)
                    if (rc != cc) mismatches += "count[${spec.name}]: $referenceName=$rc $candidateName=$cc"
                    continue
                }
                val r = reference.query<Event>(spec.filter).map { it.id }
                val c = candidate.query<Event>(spec.filter).map { it.id }
                if (r.toSet() != c.toSet()) {
                    val missing = (r.toSet() - c.toSet()).size
                    val extra = (c.toSet() - r.toSet()).size
                    mismatches += "set[${spec.name}]: sizes $referenceName=${r.size} $candidateName=${c.size} ($candidateName missing $missing, extra $extra)"
                } else if (r != c) {
                    val firstDiff = r.indices.firstOrNull { r[it] != c.getOrNull(it) } ?: -1
                    mismatches += "order[${spec.name}]: same set, different order (first diff at index $firstDiff of ${r.size})"
                }
            }
            Result(specs.size, mismatches)
        }

    /** Print a pass/fail block. Returns true if fully OK. */
    fun report(
        candidateName: String,
        referenceName: String,
        result: Result,
    ): Boolean {
        val status = if (result.ok) "PASS" else "FAIL"
        println("[$status] parity $candidateName vs $referenceName: ${result.checks - result.mismatches.size}/${result.checks} checks agree")
        BenchResults.record("parity $candidateName vs $referenceName", "checks" to result.checks.toDouble(), "mismatches" to result.mismatches.size.toDouble())
        result.mismatches.take(40).forEach { println("   ✗ $it") }
        if (result.mismatches.size > 40) println("   … ${result.mismatches.size - 40} more")
        return result.ok
    }

    /**
     * The NIP-01 filter battery, parameterized from the corpus so it hits real
     * authors/ids/tags/kinds and time ranges. Every filter here is plain recall
     * (no `search`), so results must be identical and newest-first on both stores.
     */
    private fun battery(corpus: List<Event>): List<Spec> {
        val authors = corpus.map { it.pubKey }.distinct().shuffled(kotlin.random.Random(7))
        val ids = corpus.map { it.id }.shuffled(kotlin.random.Random(8))
        val times = corpus.map { it.createdAt }.sorted()
        val tMid = times[times.size / 2]
        val tLo = times[times.size / 4]
        val tHi = times[times.size * 3 / 4]
        // Tag values that actually occur (for single-value and LIST tag filters).
        val pVals =
            corpus
                .flatMap { e -> e.tags.filter { it.size >= 2 && it[0] == "p" }.map { it[1] } }
                .distinct()
                .shuffled(kotlin.random.Random(9))
        val eVals =
            corpus
                .flatMap { e -> e.tags.filter { it.size >= 2 && it[0] == "e" }.map { it[1] } }
                .distinct()
                .shuffled(kotlin.random.Random(10))
        val pTag = pVals.firstOrNull()
        val eTag = eVals.firstOrNull()

        val specs = ArrayList<Spec>()
        // Kind scans (recall + newest-first order).
        for (k in listOf(0, 1, 3, 5, 6, 7, 10002, 30023)) {
            specs += Spec("kind=$k", Filter(kinds = listOf(k), limit = 500))
            specs += Spec("kind=$k limit10", Filter(kinds = listOf(k), limit = 10))
        }
        specs += Spec("kinds=[1,7]", Filter(kinds = listOf(1, 7), limit = 300))
        // Author timelines and author+kind.
        for (a in authors.take(15)) {
            specs += Spec("author", Filter(authors = listOf(a), limit = 100))
            specs += Spec("author+kind1", Filter(authors = listOf(a), kinds = listOf(1), limit = 50))
            // Replaceable: at most one kind-0 per author on BOTH stores.
            specs += Spec("author+kind0(replaceable)", Filter(authors = listOf(a), kinds = listOf(0)))
        }
        // Multi-author.
        specs += Spec("authors x5", Filter(authors = authors.take(5), limit = 200))
        // Id lookups (point + set).
        for (id in ids.take(30)) specs += Spec("id", Filter(ids = listOf(id)))
        specs += Spec("ids x25", Filter(ids = ids.take(25)))
        // Tag filters.
        if (pTag != null) specs += Spec("#p", Filter(kinds = listOf(1), tags = mapOf("p" to listOf(pTag)), limit = 100))
        if (eTag != null) specs += Spec("#e", Filter(tags = mapOf("e" to listOf(eTag)), limit = 100))
        // LIST-shaped filters — the shapes a client actually subscribes with
        // (follow-feed REQs with hundreds of authors, id-set fetches, big tag
        // value lists). These gate the store's list-query paths (the `in`
        // operator YQL, the id fan-out cutover) against SQLite recall.
        specs += Spec("follow-feed authors x300 kinds[1,6,7]", Filter(authors = authors.take(300), kinds = listOf(1, 6, 7), limit = 500))
        specs += Spec("contact-sync authors x100 kinds[0,3,10002]", Filter(authors = authors.take(100), kinds = listOf(0, 3, 10002), limit = 300))
        specs += Spec("ids x100", Filter(ids = ids.take(100)))
        if (pVals.size >= 2) specs += Spec("#p x${pVals.take(100).size}", Filter(tags = mapOf("p" to pVals.take(100)), limit = 300))
        if (eVals.size >= 2) specs += Spec("#e x${eVals.take(50).size}", Filter(tags = mapOf("e" to eVals.take(50)), limit = 200))
        specs += Spec("count follow-feed", Filter(authors = authors.take(300), kinds = listOf(1, 6, 7)), countOnly = true)
        // Case sensitivity: NIP-01 tag values compare by exact bytes. The
        // corpus's hashtags are Capitalized and never occur lowercased, so the
        // exact value must match and the lowercased form must match NOTHING —
        // on BOTH stores. (Vespa needed `match: cased` on tag_index for this;
        // its attributes match case-insensitively by default.)
        val tVal =
            corpus
                .flatMap { e -> e.tags.filter { it.size >= 2 && it[0] == "t" }.map { it[1] } }
                .firstOrNull { it != it.lowercase() }
        if (tVal != null) {
            specs += Spec("#t exact-case", Filter(tags = mapOf("t" to listOf(tVal)), limit = 100))
            specs += Spec("#t wrong-case matches nothing", Filter(tags = mapOf("t" to listOf(tVal.lowercase())), limit = 100))
        }
        // Time windows.
        specs += Spec("since", Filter(kinds = listOf(1), since = tMid, limit = 200))
        specs += Spec("until", Filter(kinds = listOf(1), until = tMid, limit = 200))
        specs += Spec("since+until window", Filter(since = tLo, until = tHi, limit = 300))
        specs += Spec("author since", Filter(authors = authors.take(3), since = tMid, limit = 100))
        // Counts (aggregate parity — validates deletion/supersession totals).
        specs += Spec("count all", Filter(), countOnly = true)
        for (k in listOf(0, 1, 3, 5, 6, 7, 10002, 30023)) specs += Spec("count kind=$k", Filter(kinds = listOf(k)), countOnly = true)
        for (a in authors.take(10)) specs += Spec("count author", Filter(authors = listOf(a)), countOnly = true)
        return specs
    }
}
