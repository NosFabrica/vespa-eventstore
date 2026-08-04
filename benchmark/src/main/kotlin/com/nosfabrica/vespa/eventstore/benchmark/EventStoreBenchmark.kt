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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.system.measureNanoTime

/**
 * Head-to-head event-store benchmark: Quartz's SQLite [IEventStore] versus this
 * framework's Vespa-backed one, on an identical, reproducible corpus
 * ([NostrCorpus]). It measures the two things the ask named — insertion and query
 * speed — three ways:
 *
 *  1. INSERT throughput, per-event `insert()` and bulk `batchInsert()`.
 *  2. QUERY throughput, over the shapes a relay actually serves (author timeline,
 *     kind scan, tag mentions, id lookup, count, NIP-50 search).
 *  3. Store-layer I/O AMPLIFICATION — engine round-trips per event for the
 *     per-event vs the bulk path ([CountingEventIndex]). This is the number that
 *     drives optimizing THIS framework, and it is engine-independent.
 *
 * Runnable with no external services (SQLite + the in-memory Vespa reference).
 * Point `BENCH_VESPA_URL` at a running Vespa to fold in real engine numbers.
 *
 * Tunables (env): BENCH_SIZE (corpus events), BENCH_BATCH (batchInsert chunk),
 * BENCH_QUERIES (reps per query shape), BENCH_REF_SIZE (cap for the O(n) in-memory
 * reference so it doesn't run O(n^2)), BENCH_SEED.
 */
object EventStoreBenchmark {
    private fun env(
        key: String,
        default: Int,
    ) = System.getenv(key)?.toIntOrNull() ?: default

    @JvmStatic
    fun main(args: Array<String>) {
        val size = env("BENCH_SIZE", 30_000)
        val batch = env("BENCH_BATCH", 500)
        val queries = env("BENCH_QUERIES", 2_000)
        try {
            run(size, batch, queries)
        } finally {
            // Machine-readable results (BENCH_JSON=path): the regression baseline.
            BenchResults.writeIfRequested(
                mapOf(
                    "size" to size.toString(),
                    "batch" to batch.toString(),
                    "queries" to queries.toString(),
                    "seed" to (System.getenv("BENCH_SEED") ?: "42"),
                ),
            )
        }
    }

    private fun run(
        size: Int,
        batch: Int,
        queries: Int,
    ) {
        val refSize = env("BENCH_REF_SIZE", 8_000).coerceAtMost(size)
        val seed = System.getenv("BENCH_SEED")?.toLongOrNull() ?: 42L
        val vespaUrl = System.getenv("BENCH_VESPA_URL")

        // Iterate on the Vespa query path without re-running the SQLite/InMemory
        // sections each time (they don't change). Corpus + real Vespa only.
        val vespaOnly = System.getenv("BENCH_VESPA_ONLY") != null

        // Focused insert probe: fresh + duplicate per-event inserts against a live
        // Vespa, nothing else. For controlled A/Bs of the insert path (run two
        // code versions back to back against the SAME loaded corpus, giving each
        // its own BENCH_ID_BAND so "fresh" stays fresh).
        if (System.getenv("BENCH_INSERT_PROBE") != null) {
            requireNotNull(vespaUrl) { "BENCH_INSERT_PROBE needs BENCH_VESPA_URL" }
            InsertProbe.run(
                url = vespaUrl,
                band = System.getenv("BENCH_ID_BAND")?.toLongOrNull(16) ?: 0x6L,
                size = env("BENCH_PROBE_SIZE", 2_000),
                seed = seed,
            )
            return
        }

        // Ingest stage profile: run a bulk ingest against a live Vespa and print
        // the store's own per-stage wall-time split (dedup/guards/versions/write/
        // proj.*) plus the feed-client gauge — where each batchInsert millisecond
        // goes, before optimizing any of it.
        if (System.getenv("BENCH_INGEST_PROFILE") != null) {
            requireNotNull(vespaUrl) { "BENCH_INGEST_PROFILE needs BENCH_VESPA_URL" }
            val n = env("BENCH_PROFILE_SIZE", 20_000)
            val band = System.getenv("BENCH_ID_BAND")?.toLongOrNull(16) ?: 0xE0L
            val events = NostrCorpus.generate(NostrCorpus.Config(size = n, seed = seed + 31, idBand = band))
            com.nosfabrica.vespa.eventstore.VespaEventStore.open(vespaUrl).use { store ->
                com.nosfabrica.vespa.eventstore.engine.IngestStats
                    .statusLine() // reset the deltas
                val nanos =
                    kotlin.system.measureNanoTime {
                        runBlocking { events.chunked(batch).forEach { store.batchInsert(it) } }
                    }
                val secs = nanos / 1e9
                println(String.format("ingested %d events (batch %d) in %.1fs = %.0f events/sec", n, batch, secs, n / secs))
                println(
                    com.nosfabrica.vespa.eventstore.engine.IngestStats
                        .dump(),
                )
                println(store.feedStatus())
            }
            return
        }

        // Replaceable/draft-churn A/B: client read-then-supersede vs Vespa's
        // server-side test-and-set (address-keyed conditional put).
        if (System.getenv("BENCH_CONDPUT") != null) {
            requireNotNull(vespaUrl) { "BENCH_CONDPUT needs BENCH_VESPA_URL" }
            CondPutProbe.run(vespaUrl, seed)
            return
        }

        // Live end-to-end check of the address-keyed store path (run with
        // VESPA_ADDRESS_KEYED=1): newest-wins, dual id+address addressing, reject-stale.
        if (System.getenv("BENCH_ADDR_SMOKE") != null) {
            requireNotNull(vespaUrl) { "BENCH_ADDR_SMOKE needs BENCH_VESPA_URL" }
            AddrSmokeProbe.run(vespaUrl)
            return
        }

        // Ranking-change gate: order-fidelity + cost of a candidate rank profile
        // vs the tuned baseline, per term class (see RankQuality).
        if (System.getenv("BENCH_RANK_QUALITY") != null) {
            requireNotNull(vespaUrl) { "BENCH_RANK_QUALITY needs BENCH_VESPA_URL" }
            RankQuality.run(vespaUrl)
            return
        }

        // Throughput-vs-size curve: delta-ingest one prefix-stable corpus into
        // both engines and measure at each checkpoint (see ScaleCurve).
        if (System.getenv("BENCH_SCALE_CURVE") != null) {
            ScaleCurve.run(vespaUrl, seed)
            return
        }

        // Concurrent-writer ingest A/B: N publishers batchInsert at once, SQLite's
        // single writer vs the Vespa feed client's many-in-flight (see ConcurrentIngest).
        if (System.getenv("BENCH_CONCURRENT_INGEST") != null) {
            ConcurrentIngest.run(vespaUrl, seed)
            return
        }

        // Pure index-write probe: feed docs straight through VespaEventIndex against
        // whatever schema is deployed, to isolate index-build cost (see SchemaIngestProbe).
        if (System.getenv("BENCH_SCHEMA_INGEST") != null) {
            SchemaIngestProbe.main(emptyArray())
            return
        }

        // id-lookup fan-out sweep: N ids per REQ across the fast-path threshold (see IdLookupProbe).
        if (System.getenv("BENCH_ID_LOOKUP") != null) {
            IdLookupProbe.run(vespaUrl, seed)
            return
        }

        // Bulk-ingest sweep: chunk size x parallel streams against a live Vespa
        // (see IngestSweep) — which knob raises the throttle-floor-bound ingest.
        if (System.getenv("BENCH_INGEST_SWEEP") != null) {
            requireNotNull(vespaUrl) { "BENCH_INGEST_SWEEP needs BENCH_VESPA_URL" }
            IngestSweep.run(vespaUrl, seed)
            return
        }

        // Mixed read/write load against a live Vespa: read-only and write-only
        // baselines, then both at once (see MixedLoadBench). The main corpus is
        // generated only to SAMPLE the read workload — the store is assumed
        // already loaded with it (run the full benchmark first, or ingest).
        if (System.getenv("BENCH_MIXED") != null) {
            requireNotNull(vespaUrl) { "BENCH_MIXED needs BENCH_VESPA_URL" }
            MixedLoadBench.run(vespaUrl, NostrCorpus.generate(NostrCorpus.Config(size = size, seed = seed)), seed)
            return
        }

        println("Generating corpus: size=$size authors≈${size / 20} seed=$seed ...")
        val corpus = NostrCorpus.generate(NostrCorpus.Config(size = size, seed = seed))
        val refCorpus = corpus.take(refSize)
        printMix(corpus)

        // Query workload parameters sampled from the corpus (stable across backends).
        val workload = BenchWorkload.from(corpus)

        if (!vespaOnly) {
            println("\n" + "=".repeat(72))
            println("ROUND-TRIP AMPLIFICATION  (engine calls the store makes; engine-independent)")
            println("=".repeat(72))
            roundTripAnalysis(refCorpus, batch)

            println("\n" + "=".repeat(72))
            println("INSERT THROUGHPUT")
            println("=".repeat(72))
            Table.header()
            // SQLite: the real embedded-DB numbers, the honest comparison point.
            measureInserts("SQLite (memory)", corpus, batch) { Backends.sqliteMemory() }
            val diskFile = File.createTempFile("bench-events", ".db").also { it.delete() }
            try {
                measureInserts("SQLite (disk)", corpus, batch) { Backends.sqliteDisk(diskFile.path) }
            } finally {
                diskFile.delete()
                File(diskFile.path + "-wal").delete()
                File(diskFile.path + "-shm").delete()
            }
            // Vespa reference engine (O(n) scans) — labelled, capped to refSize.
            measureInserts("Vespa/InMemory ref* (n=$refSize)", refCorpus, batch) { Backends.vespaInMemory() }

            println("\n" + "=".repeat(72))
            println("QUERY THROUGHPUT  (store pre-loaded with the full corpus)")
            println("=".repeat(72))
            runQuerySuite("SQLite (memory)", corpus, workload, queries) { Backends.sqliteMemory() }
            runQuerySuite("Vespa/InMemory ref* (n=$refSize)", refCorpus, BenchWorkload.from(refCorpus), queries / 4) { Backends.vespaInMemory() }

            println("\n* Vespa/InMemory ref is the store over an O(n)-scan in-memory engine: a")
            println("  correctness reference, NOT a throughput proxy for real Vespa. Read its")
            println("  round-trip counts (above), not its wall-clock. Set BENCH_VESPA_URL for")
            println("  real engine numbers.")

            // Correctness gate: SQLite and the store must agree on every NIP-01 filter.
            println("\n" + "=".repeat(72))
            println("PARITY  (identical NIP-01 answers — correctness gate)")
            println("=".repeat(72))
            parity("Vespa/InMemory", Backends.vespaInMemory(), refCorpus)
        }

        // Real Vespa: a shared external store, so it gets its own no-reinsertion flow.
        if (vespaUrl != null) VespaRunner.run(vespaUrl, corpus, batch, queries, seed)
    }

    /** Load [corpus] into a fresh SQLite store and [candidate], then assert identical NIP-01 answers. */
    private fun parity(
        candidateName: String,
        candidate: IEventStore,
        corpus: List<Event>,
    ) = runBlocking {
        val sqlite = Backends.sqliteMemory()
        corpus.chunked(1_000).forEach {
            sqlite.batchInsert(it)
            candidate.batchInsert(it)
        }
        val result = ParityCheck.run(corpus, "SQLite", sqlite, candidateName, candidate)
        ParityCheck.report(candidateName, "SQLite", result)
        sqlite.close()
        candidate.close()
    }

    // ---- round-trip amplification ------------------------------------------

    private fun roundTripAnalysis(
        corpus: List<Event>,
        batch: Int,
    ) = runBlocking {
        val (perEventStore, perEventCounter) = Backends.countingVespa()
        for (e in corpus) runCatching { perEventStore.insert(e) }
        perEventStore.close()

        val (bulkStore, bulkCounter) = Backends.countingVespa()
        corpus.chunked(batch).forEach { bulkStore.batchInsert(it) }
        bulkStore.close()

        val n = corpus.size.toDouble()
        println("corpus for this section: ${corpus.size} events\n")
        println(String.format("%-14s %12s %12s %12s %12s %12s", "path", "reads", "writes", "total", "rt/event", "reads/event"))

        fun row(
            name: String,
            c: CountingEventIndex,
        ) {
            println(
                String.format(
                    "%-14s %12d %12d %12d %12.2f %12.2f",
                    name,
                    c.reads(),
                    c.writeCalls(),
                    c.total(),
                    c.total() / n,
                    c.reads() / n,
                ),
            )
            BenchResults.record("round-trips $name", "rt_per_event" to c.total() / n, "reads_per_event" to c.reads() / n)
        }
        row("insert()", perEventCounter)
        row("batchInsert()", bulkCounter)
        val ratio = perEventCounter.total().toDouble() / bulkCounter.total().coerceAtLeast(1)
        println(String.format("\nbulk path issues %.1fx fewer engine round-trips than per-event.", ratio))
        println("(reads = get+search+count; writes = put/putAll+remove/removeAll invocations)")
    }

    // ---- insert throughput -------------------------------------------------

    private fun measureInserts(
        name: String,
        corpus: List<Event>,
        batch: Int,
        factory: () -> IEventStore,
    ) = runBlocking {
        // Warm the JVM for this backend (throwaway store) so the timed inserts below
        // are steady state, not JIT warmup — see Warmup.
        Warmup.warmVia(factory = factory)
        // Per-event insert() measured AT CORPUS SCALE: preload everything but a
        // probe tail via the bulk path, then time per-event inserts on top —
        // the same shape as the real-Vespa probe. Per-event-inserting the FULL
        // corpus would measure the same thing and take hours at 1M (SQLite's
        // admission SELECTs degrade as the table grows — which this probe
        // captures, at the corpus's real size, in minutes). At corpus sizes up
        // to the probe cap the preload is empty and semantics are unchanged.
        val probe = corpus.takeLast(PER_EVENT_PROBE.coerceAtMost(corpus.size))
        val preload = corpus.dropLast(probe.size)
        val single = factory()
        runBlocking { preload.chunked(1_000).forEach { single.batchInsert(it) } }
        val singleNanos =
            measureNanoTime {
                runBlocking { for (e in probe) runCatching { single.insert(e) } }
            }
        single.close()

        // Bulk batchInsert() over the FULL corpus.
        val bulk = factory()
        val bulkNanos =
            measureNanoTime {
                runBlocking { corpus.chunked(batch).forEach { bulk.batchInsert(it) } }
            }
        bulk.close()

        val probeLabel = if (preload.isEmpty()) "insert()" else "insert() @${preload.size / 1_000}k preloaded"
        Table.row("$name  $probeLabel", probe.size, singleNanos)
        Table.row("$name  batchInsert($batch)", corpus.size, bulkNanos)
    }

    /** Per-event insert probe size — enough samples for a stable rate at any corpus size. */
    private const val PER_EVENT_PROBE = 30_000

    // ---- query throughput --------------------------------------------------

    private fun runQuerySuite(
        name: String,
        corpus: List<Event>,
        workload: BenchWorkload,
        reps: Int,
        factory: () -> IEventStore,
    ) = runBlocking {
        val store = factory()
        corpus.chunked(1_000).forEach { store.batchInsert(it) }
        println("\n-- $name (loaded ${corpus.size} events) --")
        BenchResults.section(name)
        Table.queryHeader()

        query("author-timeline", reps) { i -> store.query<Event>(Filter(authors = listOf(workload.author(i)), limit = 50)) }
        query("kind-scan(notes)", reps) { store.query<Event>(Filter(kinds = listOf(1), limit = 200)) }
        query("tag-mentions(p)", reps) { i -> store.query<Event>(Filter(kinds = listOf(1), tags = mapOf("p" to listOf(workload.author(i))), limit = 50)) }
        query("id-lookup(16)", reps) { i -> store.query<Event>(Filter(ids = workload.idList(i, 16))) }
        query("profile(kind0)", reps) { i -> store.query<Event>(Filter(kinds = listOf(0), authors = listOf(workload.author(i)), limit = 1)) }
        query("count(reactions)", reps) { store.count(Filter(kinds = listOf(7))) }
        query("nip50-search", reps) { i -> store.query<Event>(Filter(kinds = listOf(1), search = workload.term(i), limit = 50)) }

        // LIST-shaped REQs — what clients actually subscribe with: a follow-feed
        // (the observer's whole contact list as authors), an id-set fetch (a
        // thread), a wide #p list (notifications for everyone you follow), and a
        // contact-sync (profiles+contacts+relay lists for a list of authors).
        // Fewer reps: each does hundreds of times the work of a point lookup.
        val listReps = (reps / 4).coerceAtLeast(25)
        query("follow-feed(a300)", listReps) { i -> store.query<Event>(Filter(authors = workload.authorList(i, 300), kinds = listOf(1, 6, 7), limit = 500)) }
        query("ids-set(100)", listReps) { i -> store.query<Event>(Filter(ids = workload.idList(i, 100))) }
        query("tag-list(p100)", listReps) { i -> store.query<Event>(Filter(kinds = listOf(1, 7), tags = mapOf("p" to workload.pList(i, 100)), limit = 300)) }
        query("contact-sync(a100)", listReps) { i -> store.query<Event>(Filter(authors = workload.authorList(i, 100), kinds = listOf(0, 3, 10002), limit = 300)) }

        store.close()
    }

    private inline fun query(
        name: String,
        reps: Int,
        crossinline op: suspend (Int) -> Any?,
    ) = runBlocking {
        val warm = (reps / 10).coerceIn(1, 100)
        repeat(warm) { runCatching { op(it) } }
        var checksum = 0L
        val lat = Latencies()
        val nanos =
            measureNanoTime {
                runBlocking {
                    repeat(reps) { i ->
                        val r = lat.timed { runCatching { op(i) }.getOrNull() }
                        if (r is Collection<*>) checksum += r.size
                    }
                }
            }
        Table.queryRow(name, reps, nanos, checksum, lat)
    }

    // ---- helpers -----------------------------------------------------------

    private fun printMix(corpus: List<Event>) {
        val byKind = corpus.groupingBy { it.kind }.eachCount().toSortedMap()
        println("kind mix: " + byKind.entries.joinToString("  ") { "${it.key}:${it.value}" })
    }

    /** Small aligned table printer. */
    private object Table {
        fun header() = println(String.format("%-42s %12s %14s %12s", "operation", "events", "events/sec", "µs/event"))

        fun row(
            name: String,
            events: Int,
            nanos: Long,
        ) {
            val secs = nanos / 1e9
            val perSec = events / secs
            val microsEach = nanos / 1000.0 / events
            println(String.format("%-42s %12d %14s %12.2f", name, events, fmt(perSec), microsEach))
            BenchResults.record(name, "events_per_sec" to perSec)
        }

        fun queryHeader() = println(String.format("%-20s %10s %14s %12s %10s  %s", "query", "reps", "queries/sec", "µs/query", "Σresults", "latency tail"))

        fun queryRow(
            name: String,
            reps: Int,
            nanos: Long,
            checksum: Long,
            lat: Latencies,
        ) {
            val secs = nanos / 1e9
            println(String.format("%-20s %10d %14s %12.2f %10d  %s", name, reps, fmt(reps / secs), nanos / 1000.0 / reps, checksum, lat.summary()))
            BenchResults.record(name, lat, "qps" to reps / secs, "sum_results" to checksum.toDouble())
        }

        private fun fmt(v: Double) = if (v >= 1000) String.format("%,d", v.toLong()) else String.format("%.1f", v)
    }
}
