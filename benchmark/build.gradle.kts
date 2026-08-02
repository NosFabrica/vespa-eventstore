plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

// Internal tooling: a head-to-head benchmark of this framework's IEventStore
// against Quartz's SQLite one. NOT published (no vanniktech plugin) — it depends
// on the SQLite driver and a fixed corpus generator that have no place in the
// released artifacts.
dependencies {
    // :store brings the Vespa-backed IEventStore and, transitively (api), Quartz —
    // whose SQLite `EventStore` is the comparison target.
    implementation(project(":store"))
    // The bundled SQLite driver Quartz's EventStore opens (BundledSQLiteDriver).
    implementation(libs.androidx.sqlite.bundled)
    implementation(libs.kotlinx.coroutines)
    // The corpus generator builds canonical event JSON with the JsonElement tree API.
    implementation(libs.kotlinx.serialization.json)
    // CondPutProbe drives the feed client directly to A/B server-side test-and-set
    // (address-keyed conditional put) against the store's read-then-supersede.
    implementation(libs.vespa.feed.client)

    // VespaParityIT stands up a real Vespa in a container and runs the parity
    // battery against it — the CI correctness gate. Skips when Docker is absent.
    testImplementation(kotlin("test"))
    testImplementation(libs.testcontainers)
}

tasks.test {
    useJUnitPlatform {
        // The Vespa integration test (VespaParityIT) stands up a container and is
        // slow, so the default build stays unit-only. Run it with -Pintegration
        // (the CI 'integration' job does); it self-skips without a Docker daemon.
        if (!project.hasProperty("integration")) excludeTags("integration")
    }
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.EventStoreBenchmark")
}

// Search-ranking A/B harness (see RankAb.kt): runs the fixed rank_cases.json
// against a live Vespa with per-request query() knob overrides — no deploy, no
// reindex. `./gradlew :benchmark:rankAb --args="--vespa http://localhost:8080"`.
tasks.register<JavaExec>("rankAb") {
    group = "verification"
    description = "A/B Vespa ranking knobs against the fixed regression case set"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.RankAb")
    // JavaExec's default working dir is THIS subproject (benchmark/), where the
    // documented default --cases path 'benchmark/rank_cases.json' would resolve
    // to benchmark/benchmark/... and NoSuchFile. Run from the repo root so the
    // README's invocations work verbatim.
    workingDir = rootDir
}

// Large corpora need heap: a BENCH_SIZE=1M run holds the generated Event list
// (~2 GB) plus the SQLite stores' native memory. BENCH_HEAP sets -Xmx for the
// benchmark JVM (default 2g keeps the everyday 30k run lean).
tasks.named<JavaExec>("run") {
    maxHeapSize = System.getenv("BENCH_HEAP") ?: "2g"
}

tasks.register<JavaExec>("visitBench") {
    group = "verification"
    description = "A/B the full-corpus visit transports (paged serial / sliced / streamed) against a real Vespa"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.VisitBench")
    maxHeapSize = System.getenv("BENCH_HEAP") ?: "2g"
}

tasks.register<JavaExec>("queryBench") {
    group = "verification"
    description = "Latency + exact-correctness check of search/count shapes over the visitBench corpus"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.QueryBench")
    maxHeapSize = System.getenv("BENCH_HEAP") ?: "3g"
}

tasks.register<JavaExec>("searchBench") {
    group = "verification"
    description = "NIP-50 search latency: term shapes, trigram/fuzzy, filters, profiles, text vs text2 (self-feeding corpus)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.SearchBench")
    maxHeapSize = System.getenv("BENCH_HEAP") ?: "3g"
}

tasks.register<JavaExec>("corpusLoad") {
    group = "verification"
    description = "Load the deterministic NostrCorpus into a live store (setup for the workload benches)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.CorpusLoad")
    maxHeapSize = System.getenv("BENCH_HEAP") ?: "2g"
}

tasks.register<JavaExec>("multiFilterBench") {
    group = "verification"
    description = "Store-level multi-filter REQ latency vs the serialized single-filter sum (A/B VESPA_QUERY_FANOUT)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.MultiFilterBench")
    maxHeapSize = System.getenv("BENCH_HEAP") ?: "3g"
}

tasks.register<JavaExec>("dedupProbe") {
    group = "verification"
    description = "A/B the bulk-dedup existence-check variants (full summary / id-only / dedup class / grouping / doc-gets) against a loaded corpus"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.DedupProbe")
    maxHeapSize = System.getenv("BENCH_HEAP") ?: "2g"
}

tasks.register<JavaExec>("traceProbe") {
    group = "verification"
    description = "Dump Vespa query-execution plans (trace.explainLevel) for the named REQ shapes"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.vitorpamplona.quartz.eventstore.benchmark.TraceProbe")
    maxHeapSize = System.getenv("BENCH_HEAP") ?: "2g"
}
