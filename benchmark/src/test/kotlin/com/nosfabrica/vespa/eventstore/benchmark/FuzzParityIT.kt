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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.benchmark.harness.Backends
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.conformance.ConformanceCorpus
import com.vitorpamplona.quartz.nip01Core.store.conformance.StoreConformance
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertTrue

/**
 * Random NIP-01 filters, diffed against Quartz's SQLite store on the same notes. Where
 * [VespaParityIT] runs quartz's fixed battery, this draws thousands of filters at random
 * (ids, authors, kinds, tags, windows on tie seconds, limits, unions) over a corpus with
 * dense tie bursts, and runs each one several times so the recency read path's learned
 * state (tie slack, family votes, one query or id-first) is exercised warm as well as cold.
 *
 * The reference is exact, not SQLite's own limited read: SQLite leaves same-second order
 * unspecified (STORE-F08), so each filter's whole SQLite match set is ordered newest first,
 * lowest id first within a second, and cut to its limit. SQLite's own COUNT is only reported
 * (`SQLITE-COUNT`), since before quartz 3bebf0e49 it dropped a `{}` branch from a union.
 *
 * Runs against a Vespa already up at FUZZ_VESPA_URL (empty, or holding only this corpus), and
 * skips without it. FUZZ_CHECKS, FUZZ_PASSES and FUZZ_SEED vary the battery; FUZZ_SKIP_LOAD
 * reuses a loaded store; VESPA_ID_FIRST_MAX_DIRECT_HITS=0 runs the id-first read path:
 * `FUZZ_VESPA_URL=http://localhost:8080 ./gradlew :benchmark:test -Pintegration --tests '*FuzzParityIT' --rerun`
 * (2026-11-25: 13,000 filters over three seeds, both read paths, twice each: no difference.)
 */
@Tag("integration")
class FuzzParityIT {
    @Test
    fun `random filters answer as SQLite does`() {
        val url = System.getenv("FUZZ_VESPA_URL")
        assumeTrue(!url.isNullOrBlank(), "FUZZ_VESPA_URL not set")
        val checks = (System.getenv("FUZZ_CHECKS") ?: "3000").toInt()
        val passes = (System.getenv("FUZZ_PASSES") ?: "3").toInt()
        val seed = (System.getenv("FUZZ_SEED") ?: "1").toLong()

        val corpus = corpus()
        val store = VespaEventStore.open(url = url, autoDeploy = true, configUrl = System.getenv("FUZZ_CONFIG_URL") ?: "http://localhost:19071")
        val sqlite = Backends.sqliteMemory()
        store.use {
            runBlocking {
                if (System.getenv("FUZZ_SKIP_LOAD") == null) StoreConformance.load(store, corpus, chunk = 1_000)
                StoreConformance.load(sqlite, corpus, chunk = 1_000)
                val battery = FuzzFilters(corpus, Random(seed)).checks(checks)
                // The exact NIP-01 answer: SQLite's whole match set per filter (its set semantics are
                // the reference), newest first and lowest id first within a second, cut to the
                // filter's limit, then the union in the same order. SQLite's own limited read leaves
                // same-second order unspecified (STORE-F08), so it can't be diffed byte for byte.
                // A count keeps only its size: a union holding `{}` is the whole corpus.
                val expected =
                    battery.associateWith { check ->
                        val ids =
                            check.filters
                                .flatMap { f -> sqlite.query<Event>(listOf(f.copy(limit = null))).sortedWith(NIP01_ORDER).take(f.limit ?: Int.MAX_VALUE) }
                                .distinctBy { it.id }
                                .sortedWith(NIP01_ORDER)
                                .map { it.id }
                        if (check.countOnly) ids.size else ids
                    }
                // Where SQLite's own NIP-45 count disagrees with the size of its own match set.
                var sqliteCountSelf = 0
                for (check in battery.filter { it.countOnly }) {
                    val own = sqlite.count(check.filters)
                    if (own != expected.getValue(check)) {
                        if (sqliteCountSelf++ < 10) println("SQLITE-COUNT own=$own set=${expected.getValue(check)} ${check.name.take(300)}")
                    }
                }
                println("SQLITE-COUNT $sqliteCountSelf of ${battery.count { it.countOnly }} counts disagree with SQLite's own match set")
                var failed = 0
                for (pass in 1..passes) {
                    val kinds = HashMap<String, Int>()
                    var shown = 0
                    for (check in battery) {
                        val problem =
                            if (check.countOnly) {
                                val want = expected.getValue(check)
                                val got = store.count(check.filters)
                                if (got != want) "count want=$want got=$got" else null
                            } else {
                                @Suppress("UNCHECKED_CAST")
                                val want = expected.getValue(check) as List<String>
                                val got = store.query<Event>(check.filters).map { it.id }
                                when {
                                    got == want -> null
                                    got.toSet() == want.toSet() -> "order: same ${want.size}, first difference at ${want.indices.first { want[it] != got[it] }}"
                                    else -> "set: want ${want.size} got ${got.size}; missing ${(want - got.toSet()).size} (${(want - got.toSet()).take(2)}), extra ${(got - want.toSet()).size} (${(got - want.toSet()).take(2)})"
                                }
                            }
                        if (problem != null) {
                            kinds.merge(problem.substringBefore(':').substringBefore(' '), 1, Int::plus)
                            if (shown++ < 40) println("FUZZ-MISMATCH pass=$pass $problem || ${check.name.take(400)}")
                        }
                    }
                    val bad = kinds.values.sum()
                    println("FUZZ pass $pass: ${battery.size - bad}/${battery.size} agree; $kinds")
                    failed += bad
                }
                sqlite.close()
                assertTrue(failed == 0, "$failed mismatches")
            }
        }
    }

    private companion object {
        val NIP01_ORDER: Comparator<Event> = compareByDescending<Event> { it.createdAt }.thenBy { it.id }
    }

    /** Quartz's conformance corpus, plus two id-disjoint ones squashed into tie bursts inside its span. */
    private fun corpus(): List<Event> {
        val base = ConformanceCorpus.generate(ConformanceCorpus.Config(size = 20_000, seed = 42))
        val start = base.first().createdAt
        val span = base.last().createdAt - start

        fun squash(
            size: Int,
            seed: Long,
            band: Long,
            factor: Long,
            at: Long,
        ) = ConformanceCorpus.generate(ConformanceCorpus.Config(size = size, seed = seed, idBand = band, startTime = 0)).map {
            EventFactory.create<Event>(it.id, it.pubKey, start + at + it.createdAt / factor, it.kind, it.tags, it.content, it.sig)
        }
        // ~10 events a second over ~1,000 seconds, and ~100 a second over ~50 seconds.
        return base + squash(10_000, 43, 0x2L, 40, span / 3) + squash(5_000, 44, 0x3L, 400, 2 * span / 3)
    }
}

/** Draws random filters from a corpus so most of them match something. */
class FuzzFilters(
    corpus: List<Event>,
    private val rnd: Random,
) {
    private val ids = corpus.map { it.id }.distinct()
    private val authors = corpus.map { it.pubKey }.distinct()
    private val times = corpus.map { it.createdAt }.sorted()
    private val kinds = listOf(0, 1, 3, 5, 6, 7, 10002, 30023)
    private val tagValues: Map<String, List<String>> =
        listOf("e", "p", "t", "d", "a").associateWith { name ->
            corpus.flatMap { e -> e.tags.filter { it.size > 1 && it[0] == name }.map { it[1] } }.distinct()
        }
    private val limits = listOf(0, 1, 2, 3, 5, 10, 20, 50, 100, 300, 500)

    fun checks(n: Int): List<StoreConformance.Check> =
        List(n) {
            val count = rnd.nextInt(100) < 15
            val filters = List(if (rnd.nextInt(100) < 75) 1 else rnd.nextInt(2, 4)) { filter(count) }
            StoreConformance.Check(filters.joinToString(" | ") { it.toJson() }, filters, listOf("fuzz"), countOnly = count)
        }

    private fun <T> some(
        from: List<T>,
        max: Int,
    ): List<T> = List(1 + rnd.nextInt(max)) { from[rnd.nextInt(from.size)] }.distinct()

    private fun filter(count: Boolean): Filter {
        var narrow = false
        val ids =
            if (rnd.nextInt(100) < 8) {
                narrow = true
                some(this.ids, 30) + if (rnd.nextBoolean()) listOf("f".repeat(64)) else emptyList()
            } else {
                null
            }
        val authors =
            if (rnd.nextInt(100) < 40) {
                narrow = true
                some(this.authors, listOf(1, 5, 300)[rnd.nextInt(3)])
            } else {
                null
            }
        val kinds = if (rnd.nextInt(100) < 60) some(this.kinds, 3) + if (rnd.nextInt(100) < 5) listOf(9999) else emptyList() else null
        val tags =
            if (rnd.nextInt(100) < 35) {
                narrow = true
                val name = listOf("e", "p", "t", "d", "a")[rnd.nextInt(5)]
                val values = tagValues.getValue(name)
                if (values.isEmpty()) null else mapOf(name to some(values, listOf(1, 3, 20)[rnd.nextInt(3)]).let { v -> if (name == "t" && rnd.nextInt(100) < 20) v.map { it.lowercase() } else v })
            } else {
                null
            }
        var since: Long? = null
        var until: Long? = null
        when (rnd.nextInt(10)) {
            0, 1 -> {
                until = times[rnd.nextInt(times.size)]
            }

            2 -> {
                since = times[rnd.nextInt(times.size)]
            }

            3 -> {
                val a = times[rnd.nextInt(times.size)]
                val b = times[rnd.nextInt(times.size)]
                since = minOf(a, b)
                until = maxOf(a, b)
            }

            4 -> {
                since = times[rnd.nextInt(times.size)]
                until = since
            }
        }
        val limit =
            when {
                count -> null
                narrow && rnd.nextInt(100) < 15 -> null
                else -> limits[rnd.nextInt(limits.size)]
            }
        return Filter(ids = ids, authors = authors, kinds = kinds, tags = tags, since = since, until = until, limit = limit)
    }
}
