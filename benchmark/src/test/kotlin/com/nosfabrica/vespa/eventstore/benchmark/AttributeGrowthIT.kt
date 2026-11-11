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

import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.SplittableRandom
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * WHERE PROTON'S GROWTH RESERVE SITS, AND WHETHER `allocation` IN services.xml
 * REACHES IT (#206).
 *
 * Staging showed attributes allocated ~20% ahead of what they use, resident and
 * counted by the feed block, and proposed lowering proton's
 * `documentdb[].allocation.growfactor` / `multivaluegrowfactor` (default 0.2)
 * through a `<config name="vespa.config.search.core.proton">` override. This
 * measures the two things that proposal rests on, on a throwaway container:
 *
 *  1. **Whether the override reaches the document db proton builds.** `documentdb`
 *     is an ARRAY in proton.def, and a services.xml override of an array is
 *     APPENDED to the entries the config model generates — it does not merge into
 *     them. Proton picks a document type's entry with the first match by
 *     `inputdoctypename` (`find_document_db_config_entry` in
 *     `documentdbconfigmanager.cpp`), which is the model's own, carrying the
 *     defaults. The test reads what the config server serves the search node and
 *     asserts which entry proton resolves.
 *  2. **Which component of each attribute holds the reserve**, from proton's
 *     per-attribute state API (enum-store values, dictionary, multi-value arrays,
 *     posting store, and the per-document vector as the remainder). That decides
 *     which knob could move it at all: `growfactor` sizes only the per-document
 *     vectors (the document vector of every attribute, the meta store's lid
 *     table), `multivaluegrowfactor` only the multi-value array store, while the
 *     enum stores (every `id`, `pubkey` and `tag_index` value) and every B-tree
 *     (dictionaries, posting lists) grow buffers by vespalib's own hard-coded
 *     0.2 (`ALLOC_GROW_FACTOR` in `unique_store_allocator.hpp` /
 *     `unique_store_string_allocator.cpp`, `DEFAULT_ALLOC_GROW_FACTOR` in
 *     `buffer_type.cpp`), and a `dictionary { hash }` shard DOUBLES when full
 *     (`sharded_hash_map.cpp`) — none of which any config reaches.
 *
 * Measured at 3M documents (docs/attribute-memory.md §7): the override run
 * allocates the same bytes as stock attribute by attribute, and most of the
 * reserve sits in the hard-coded rules (~60%).
 *
 * Alongside: proton's `RssAnon` (what the feed block divides by its limit),
 * sampled every [SAMPLE_MILLIS] so a growth step's transient shows as the peak
 * between checkpoints, and the feed rate per segment.
 *
 * A MEASUREMENT, not a CI gate: it feeds `-DitGrowDocs` documents per variant
 * and self-skips without it. `-DitGrowVariants` is a comma list of `stock`
 * (the package as built) and override factors (`0.05` appends an `event` entry
 * with growfactor = multivaluegrowfactor = 0.05); default `stock,0.05`.
 *
 * ```bash
 * ./gradlew :benchmark:test -Pintegration -DitGrowDocs=2000000 \
 *     --tests '*AttributeGrowthIT*'
 * ```
 *
 * The corpus is synthetic but shaped where it matters for memory: 64-hex ids
 * with no order (the `id` dictionary and enum store see random inserts, as in
 * production), a 20k-author pool, `e`/`p`/`t` tags drawn from the same id and
 * author spaces, ~4% replaceable kinds that overwrite their address (the dead
 * space production carries), and real search columns through
 * [SearchExtractors].
 */
@Tag("integration")
class AttributeGrowthIT {
    @Test
    fun `where the growth reserve sits, and whether an allocation override reaches proton`() {
        assumeTrue(dockerAvailable(), "Docker not available")
        val docs = System.getProperty("itGrowDocs")?.toIntOrNull()
        assumeTrue(docs != null && docs > 0, "set -DitGrowDocs=N (documents fed per variant) to run this measurement")
        val variants =
            (System.getProperty("itGrowVariants") ?: "stock,0.05")
                .split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .map { if (it == "stock") null else it.toDouble() }

        val runs = variants.map { measure(it, docs!!) }
        compare(runs)

        // CLAIM 2, measured rather than read off the config: an override run sizes
        // the per-document vectors exactly as stock does. `kind` is a single-value
        // attribute, so its unattributed remainder IS its document vector, the
        // structure `growfactor` sizes; its capacity follows the lid count alone,
        // which both runs reach identically. A 0.05 factor that took effect would
        // leave it at most ~1.05x its used bytes instead of stock's ~1.2x.
        val stock = runs.firstOrNull { it.override == null }
        if (stock != null) {
            for (run in runs.filter { it.override != null }) {
                assertEquals(
                    docVector(stock, "kind"),
                    docVector(run, "kind"),
                    "`kind`'s document vector differs from stock under ${run.label} — growfactor reached proton",
                )
            }
        }

        for (run in runs) {
            // CLAIM 1: whatever the package asked for, the entry proton resolves for
            // `event` is the first one served, and an override is never the first.
            val event = run.served.filter { it.docType == "event" }
            if (run.override == null) {
                assertEquals(1, event.size, "the stock package serves one `event` documentdb entry: ${run.served}")
            } else {
                assertTrue(event.size >= 2, "the override was expected APPENDED as a second `event` entry: ${run.served}")
                assertEquals(
                    run.override,
                    event.last().growfactor,
                    "the appended entry carries the override's growfactor: ${run.served}",
                )
            }
            assertEquals(
                DEFAULT_GROWFACTOR,
                event.first().growfactor,
                "proton reads the FIRST `event` entry; it no longer carries Vespa's default — the override reached it: ${run.served}",
            )
        }
    }

    private fun docVector(
        run: Run,
        attribute: String,
    ): Long =
        run.checkpoints
            .last()
            .attributes[attribute]
            ?.components
            ?.get(UNATTRIBUTED)
            ?.first ?: error("${run.label} recorded no `$attribute`")

    /** One variant's run: what was served, and the checkpoints taken while feeding. */
    private class Run(
        val label: String,
        val override: Double?,
        val served: List<DocDbEntry>,
        val checkpoints: List<Checkpoint>,
    )

    /** A `documentdb[]` entry as the config server serves it to the search node. */
    private data class DocDbEntry(
        val docType: String,
        val growfactor: Double,
        val multivaluegrowfactor: Double,
    )

    private class Checkpoint(
        val fed: Int,
        val ready: Long,
        val feedRate: Double,
        val rssAnon: Long,
        val rssAnonPeak: Long,
        val attributes: Map<String, Mem>,
    )

    /** Allocated / used bytes of one attribute, split by component. Bytes. */
    private class Mem(
        val allocated: Long,
        val used: Long,
        val dead: Long,
        val onHold: Long,
        val components: Map<String, Pair<Long, Long>>,
    )

    private fun measure(
        override: Double?,
        docs: Int,
    ): Run {
        val label = override?.let { "override $it" } ?: "stock"
        println("\n" + "=".repeat(78) + "\n$label: feeding $docs documents\n" + "=".repeat(78))
        return GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val deployer = SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}")
                deployer.deploy(packageWith(override))
                deployer.awaitServing(queryUrl)

                val searchNode = searchNodeConfigId(vespa)
                val served = servedDocumentDbs(vespa, searchNode)
                println("served documentdb entries (proton reads the first per type): $served")
                awaitOnline(vespa)
                val pid = protonPid(vespa)

                // RssAnon on its own thread: a growth step's transient (old and new
                // arrays both live until readers release the old) is shorter than
                // the gap between checkpoints, so only a continuous sample sees it.
                val windowPeak = AtomicLong()
                val running = AtomicBoolean(true)
                val sampler =
                    thread(name = "rss-sampler", isDaemon = true) {
                        while (running.get()) {
                            runCatching { rssAnon(vespa, pid) }.onSuccess { v -> windowPeak.accumulateAndGet(v, ::maxOf) }
                            Thread.sleep(SAMPLE_MILLIS)
                        }
                    }

                val checkpoints = ArrayList<Checkpoint>()
                val every = (docs / CHECKPOINTS).coerceAtLeast(CHUNK)
                VespaEventIndex(queryUrl).use { index ->
                    runBlocking {
                        var fed = 0
                        var segmentStart = System.nanoTime()
                        var segmentFed = 0
                        while (fed < docs) {
                            val wave = minOf(CHUNK * FEEDERS, docs - fed)
                            val base = fed
                            coroutineScope {
                                (0 until wave step CHUNK)
                                    .map { off ->
                                        async {
                                            index.putAll(List(minOf(CHUNK, wave - off)) { i -> doc(base + off + i) })
                                        }
                                    }.awaitAll()
                            }
                            fed += wave
                            segmentFed += wave
                            if (fed / every != (fed - wave) / every || fed == docs) {
                                val rate = segmentFed * 1e9 / (System.nanoTime() - segmentStart)
                                val now = rssAnon(vespa, pid)
                                val peak = maxOf(windowPeak.getAndSet(0), now)
                                val cp = Checkpoint(fed, readyDocs(vespa), rate, now, peak, settledAttributeMemory(vespa))
                                checkpoints += cp
                                printCheckpoint(cp)
                                segmentStart = System.nanoTime()
                                segmentFed = 0
                            }
                        }
                    }
                }
                running.set(false)
                sampler.join()
                printComponents(label, checkpoints.last())
                Run(label, override, served, checkpoints)
            }
    }

    // ---- the package ----------------------------------------------------------

    /**
     * The bundled package, with — for an override — the `<documentdb>` block an
     * operator following #206 would write, inside the content cluster's existing
     * proton `<config>`. `configid` is stated because without it the config
     * server cannot build the entry at all: it serves the search node an EMPTY
     * proton config (logged only at INFO), proton refuses it ("No document
     * databases in config") and stops taking config changes until the package is
     * fixed. That shape is not measured here, only warned about.
     */
    private fun packageWith(override: Double?): ByteArray {
        val zip = VespaApp.zipBytes(null)
        if (override == null) return zip
        val block =
            """
            <documentdb>
              <item>
                <inputdoctypename>event</inputdoctypename>
                <configid>content/search/cluster.content/event</configid>
                <allocation>
                  <growfactor>$override</growfactor>
                  <multivaluegrowfactor>$override</multivaluegrowfactor>
                </allocation>
              </item>
            </documentdb>
            """.trimIndent()
        return rewriteServices(zip) { services ->
            val anchor = "<numthreadspersearch>"
            require(anchor in services) { "services.xml no longer configures proton where this test inserts its override" }
            services.replaceFirst(anchor, "$block\n      $anchor")
        }
    }

    private fun rewriteServices(
        zip: ByteArray,
        edit: (String) -> String,
    ): ByteArray {
        val out = ByteArrayOutputStream()
        ZipOutputStream(out).use { zos ->
            ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
                generateSequence { zis.nextEntry }.forEach { entry ->
                    val bytes = zis.readBytes()
                    zos.putNextEntry(ZipEntry(entry.name))
                    zos.write(if (entry.name == "services.xml") edit(bytes.decodeToString()).encodeToByteArray() else bytes)
                    zos.closeEntry()
                }
            }
        }
        return out.toByteArray()
    }

    // ---- reading the node -----------------------------------------------------

    private fun searchNodeConfigId(vespa: GenericContainer<*>): String =
        CONFIG_ID.find(exec(vespa, "$VESPA_HOME/bin/vespa-sentinel-cmd list"))?.groupValues?.get(1)
            ?: error("the sentinel runs no searchnode")

    /** The `documentdb[]` entries the config server serves the search node, in the order proton walks them. */
    private fun servedDocumentDbs(
        vespa: GenericContainer<*>,
        configId: String,
    ): List<DocDbEntry> {
        val body =
            exec(
                vespa,
                "curl -sf localhost:$CONFIG_PORT/config/v2/tenant/default/application/default/vespa.config.search.core.proton/$configId",
            )
        val cfg = Json.parseToJsonElement(body).jsonObject
        val dbs = cfg["documentdb"]?.jsonArray ?: error("the served proton config holds no documentdb at all: ${body.take(400)}")
        return dbs.map { el ->
            val o = el.jsonObject
            val alloc = o.getValue("allocation").jsonObject
            DocDbEntry(
                o.getValue("inputdoctypename").jsonPrimitive.content,
                alloc
                    .getValue("growfactor")
                    .jsonPrimitive.content
                    .toDouble(),
                alloc
                    .getValue("multivaluegrowfactor")
                    .jsonPrimitive.content
                    .toDouble(),
            )
        }
    }

    private fun awaitOnline(vespa: GenericContainer<*>) {
        repeat(120) {
            val body = runCatching { exec(vespa, "curl -sf $STATE/documentdb/event") }.getOrNull()
            if (body != null && "\"ONLINE\"" in body) return
            Thread.sleep(1_000)
        }
        error("the event document db never came ONLINE")
    }

    /** The proton process, found through the sentinel (as `ContainerJvmIT` finds the container JVM). */
    private fun protonPid(vespa: GenericContainer<*>): String =
        SEARCHNODE_PID.find(exec(vespa, "$VESPA_HOME/bin/vespa-sentinel-cmd list"))?.groupValues?.get(1)
            ?: error("the sentinel runs no searchnode")

    /** Bytes. RssAnon is what the feed block's memory metric divides by its limit (#206, measured on staging). */
    private fun rssAnon(
        vespa: GenericContainer<*>,
        pid: String,
    ): Long =
        RSS_ANON
            .find(exec(vespa, "cat /proc/$pid/status"))
            ?.groupValues
            ?.get(1)
            ?.toLong()
            ?.times(1024)
            ?: error("no RssAnon for pid $pid")

    private fun readyDocs(vespa: GenericContainer<*>): Long =
        Json
            .parseToJsonElement(exec(vespa, "curl -sf $STATE/documentdb/event"))
            .jsonObject["documents"]
            ?.jsonObject
            ?.get("ready")
            ?.jsonPrimitive
            ?.long ?: -1

    /**
     * [attributeMemory] once it stops moving. An attribute's `status.memoryUsage`
     * total is a STATISTIC proton refreshes every few seconds, while the component
     * figures beside it are read live — so a snapshot taken mid-feed pairs a stale
     * total with fresh parts (the smoke run showed `id` at 8.1 MiB allocated with
     * 16.5 MiB of parts, a negative document vector). The feed is paused here, so
     * two reads [SETTLE_MILLIS] apart that agree are one consistent state.
     */
    private fun settledAttributeMemory(vespa: GenericContainer<*>): Map<String, Mem> {
        var last = attributeMemory(vespa)
        repeat(SETTLE_TRIES) {
            Thread.sleep(SETTLE_MILLIS)
            val next = attributeMemory(vespa)
            if (fingerprint(next) == fingerprint(last)) return next
            last = next
        }
        println("  (attribute statistics still moving after ${SETTLE_TRIES * SETTLE_MILLIS / 1000}s; reading the last)")
        return last
    }

    private fun fingerprint(m: Map<String, Mem>) = m.mapValues { (_, v) -> listOf(v.allocated, v.used) + v.components.values.flatMap { it.toList() } }

    /**
     * Every ready-sub-db attribute plus the document meta store, in ONE exec (one
     * curl per attribute, inside the container): reading them one exec at a time
     * took long enough that the feed moved between the first and the last.
     */
    private fun attributeMemory(vespa: GenericContainer<*>): Map<String, Mem> {
        val names =
            Json
                .parseToJsonElement(exec(vespa, "curl -sf $STATE/documentdb/event/subdb/ready/attribute"))
                .jsonObject.keys
                .sorted()
        val script =
            buildString {
                append("curl -sf $STATE/documentdb/event/subdb/ready/documentmetastore; echo;")
                for (n in names) append(" curl -sf $STATE/documentdb/event/subdb/ready/attribute/$n; echo;")
            }
        val lines = exec(vespa, script).lines().filter { it.isNotBlank() }
        require(lines.size == names.size + 1) { "expected ${names.size + 1} state documents, got ${lines.size}" }
        val out = LinkedHashMap<String, Mem>()
        mem(Json.parseToJsonElement(lines[0]).jsonObject)?.let { out[META_STORE] = it }
        names.forEachIndexed { i, n -> mem(Json.parseToJsonElement(lines[i + 1]).jsonObject)?.let { out[n] = it } }
        return out
    }

    /** null for an entry that reports no memory of its own (the state API lists some without a `status`). */
    private fun mem(state: JsonObject): Mem? {
        val total =
            state["status"]
                ?.jsonObject
                ?.get("memoryUsage")
                ?.jsonObject ?: return null
        val allocated = total.num("allocatedBytes")
        val used = total.num("usedBytes")
        val comps = LinkedHashMap<String, Pair<Long, Long>>()

        fun part(
            name: String,
            o: JsonObject?,
        ) {
            if (o != null) comps[name] = o.num("allocated") to o.num("used")
        }
        val enumStore = state["enumStore"]?.jsonObject
        part("enum values", enumStore?.get("valuesMemoryUsage")?.jsonObject)
        part("dictionary", enumStore?.get("dictionaryMemoryUsage")?.jsonObject)
        part("multi-value", state["multiValue"]?.jsonObject?.get("memoryUsage")?.jsonObject)
        part(
            "postings",
            state["posting_store"]
                ?.jsonObject
                ?.get("memory_usage")
                ?.jsonObject
                ?.get("total")
                ?.jsonObject,
        )
        part("change vector", state["changeVector"]?.jsonObject)
        // What the named parts do not account for. On a single-value attribute that
        // is the per-document vector (lid -> value / enum ref), and the meta store is
        // nothing BUT its lid table; on a multi-value or fast-search attribute the
        // named parts OVERLAP (measured: tag_index's parts sum to more than its
        // total), so the remainder there is not a component and can go negative.
        // Reported, never summed into the per-component table.
        val namedAllocated = comps.values.sumOf { it.first }
        val namedUsed = comps.values.sumOf { it.second }
        comps[UNATTRIBUTED] = (allocated - namedAllocated) to (used - namedUsed)
        return Mem(allocated, used, total.num("deadBytes"), total.num("onHoldBytes"), comps)
    }

    private fun JsonObject.num(key: String): Long = this[key]?.jsonPrimitive?.longOrNull ?: 0L

    private fun exec(
        vespa: GenericContainer<*>,
        command: String,
    ): String {
        val res = vespa.execInContainer("bash", "-c", command)
        require(res.exitCode == 0) { "`${command.take(120)}` failed (${res.exitCode}): ${res.stderr}" }
        return res.stdout.trim()
    }

    // ---- reporting ------------------------------------------------------------

    private fun printCheckpoint(cp: Checkpoint) {
        val alloc = cp.attributes.values.sumOf { it.allocated }
        val used = cp.attributes.values.sumOf { it.used }
        println(
            String.format(
                "fed %,10d  ready %,10d  %,7.0f docs/s  attrs alloc %8s used %8s reserve %5.1f%%  RssAnon %8s (peak %8s)",
                cp.fed,
                cp.ready,
                cp.feedRate,
                mib(alloc),
                mib(used),
                100.0 * (alloc - used) / alloc.coerceAtLeast(1),
                mib(cp.rssAnon),
                mib(cp.rssAnonPeak),
            ),
        )
    }

    private fun printComponents(
        label: String,
        cp: Checkpoint,
    ) {
        println("\n$label at ${String.format("%,d", cp.ready)} ready docs — allocated / used / reserve, by component (MiB):")
        val byComponent = LinkedHashMap<String, Pair<Long, Long>>()
        for ((name, m) in cp.attributes.entries.sortedByDescending { it.value.allocated }) {
            println(
                String.format(
                    "  %-26s %9s %9s %6.1f%%  dead %8s onHold %8s",
                    name,
                    mib(m.allocated),
                    mib(m.used),
                    100.0 * (m.allocated - m.used) / m.allocated.coerceAtLeast(1),
                    mib(m.dead),
                    mib(m.onHold),
                ),
            )
            for ((c, au) in m.components) {
                if (au.first == 0L && au.second == 0L) continue
                if (c == UNATTRIBUTED) {
                    println(String.format("      %-22s %9s %9s", c, mib(au.first), mib(au.second)))
                    continue
                }
                println(
                    String.format(
                        "      %-22s %9s %9s %6.1f%%",
                        c,
                        mib(au.first),
                        mib(au.second),
                        100.0 * (au.first - au.second) / au.first.coerceAtLeast(1),
                    ),
                )
                val prev = byComponent[c] ?: (0L to 0L)
                byComponent[c] = (prev.first + au.first) to (prev.second + au.second)
            }
        }
        val allocated = cp.attributes.values.sumOf { it.allocated }
        val used = cp.attributes.values.sumOf { it.used }
        println("\n  all attributes + meta store: allocated ${mib(allocated)} used ${mib(used)} reserve ${mib(allocated - used)}")
        println("  summed over attributes, by named component (the meta store and single-value document vectors are the unattributed rows above):")
        val total = byComponent.values.sumOf { it.first - it.second }.coerceAtLeast(1)
        for ((c, au) in byComponent.entries.sortedByDescending { it.value.first - it.value.second }) {
            println(
                String.format(
                    "  %-24s alloc %9s used %9s reserve %9s (%5.1f%% of the reserve)",
                    c,
                    mib(au.first),
                    mib(au.second),
                    mib(au.first - au.second),
                    100.0 * (au.first - au.second) / total,
                ),
            )
        }
    }

    private fun compare(runs: List<Run>) {
        if (runs.size < 2) return
        println("\n" + "=".repeat(78) + "\nfinal checkpoint, allocated MiB per attribute, by variant\n" + "=".repeat(78))
        println(String.format("  %-26s", "") + runs.joinToString("") { String.format("%16s", it.label) })
        val names =
            runs
                .first()
                .checkpoints
                .last()
                .attributes.keys
        for (n in names) {
            println(
                String.format("  %-26s", n) +
                    runs.joinToString("") {
                        String.format(
                            "%16s",
                            it.checkpoints
                                .last()
                                .attributes[n]
                                ?.let { m -> mib(m.allocated) } ?: "-",
                        )
                    },
            )
        }
        println(String.format("  %-26s", "RssAnon") + runs.joinToString("") { String.format("%16s", mib(it.checkpoints.last().rssAnon)) })
        println(String.format("  %-26s", "RssAnon peak (whole run)") + runs.joinToString("") { r -> String.format("%16s", mib(r.checkpoints.maxOf { it.rssAnonPeak })) })
    }

    private fun mib(bytes: Long) = String.format("%,.1f", bytes / 1_048_576.0)

    // ---- the corpus -----------------------------------------------------------

    /**
     * Document [seq], deterministic. Ids are 64 random hex characters, so the
     * `id` dictionary takes them in no order, as production does — a sequential
     * id would fill B-tree nodes left to right and understate their slack.
     */
    private fun doc(seq: Int): EventDoc {
        val rnd = SplittableRandom(seq * SEED_STRIDE + 1)
        val kind = pickKind(rnd.nextInt(100))
        val author = AUTHORS[rnd.nextInt(AUTHORS.size)]
        val createdAt = 1_700_000_000L + seq / 4

        fun earlierId() = idOf(if (seq == 0) 0 else rnd.nextInt(seq))

        fun someone() = AUTHORS[rnd.nextInt(AUTHORS.size)]
        val tags = ArrayList<List<String>>()
        val content: String
        when (kind) {
            1 -> {
                repeat(rnd.nextInt(3)) { tags += listOf("e", earlierId()) }
                repeat(rnd.nextInt(4)) { tags += listOf("p", someone()) }
                repeat(rnd.nextInt(3)) { tags += listOf("t", WORDS[rnd.nextInt(WORDS.size)]) }
                content = sentence(rnd, 4 + rnd.nextInt(40))
            }

            7 -> {
                tags += listOf("e", earlierId())
                tags += listOf("p", someone())
                content = "+"
            }

            6 -> {
                tags += listOf("e", earlierId())
                tags += listOf("p", someone())
                content = ""
            }

            9735 -> {
                tags += listOf("p", someone())
                tags += listOf("P", someone())
                tags += listOf("e", earlierId())
                tags += listOf("bolt11", "lnbc" + hex(rnd, 120))
                content = ""
            }

            0 -> {
                content = """{"name":"${WORDS[rnd.nextInt(WORDS.size)]}${rnd.nextInt(10_000)}","about":"${sentence(rnd, 8)}"}"""
            }

            3 -> {
                repeat(5 + rnd.nextInt(60)) { tags += listOf("p", someone()) }
                content = ""
            }

            10002 -> {
                tags += listOf("r", "wss://relay${rnd.nextInt(50)}.example.com")
                tags += listOf("r", "wss://nos.lol")
                content = ""
            }

            else -> {
                tags += listOf("d", "article-${rnd.nextInt(1_000_000)}")
                tags += listOf("title", sentence(rnd, 3 + rnd.nextInt(5)))
                repeat(rnd.nextInt(4)) { tags += listOf("t", WORDS[rnd.nextInt(WORDS.size)]) }
                content = sentence(rnd, 100 + rnd.nextInt(300))
            }
        }
        val id = idOf(seq)
        val search =
            SearchExtractors.extract(
                EventFactory.create<Event>(id, author, createdAt, kind, Array(tags.size) { tags[it].toTypedArray() }, content, SIG),
            )
        return EventDoc(id, author, createdAt, kind, tags, content, SIG, search = search)
    }

    private fun pickKind(p: Int): Int =
        when {
            p < 52 -> 1
            p < 77 -> 7
            p < 83 -> 6
            p < 89 -> 9735
            p < 91 -> 0
            p < 93 -> 3
            p < 95 -> 10002
            else -> 30023
        }

    private fun sentence(
        rnd: SplittableRandom,
        words: Int,
    ): String =
        (0 until words).joinToString(" ") {
            // A long tail of one-off tokens, so the text vocabulary keeps growing as real text does.
            if (rnd.nextInt(10) == 0) "w" + Integer.toString(rnd.nextInt(5_000_000), 36) else WORDS[rnd.nextInt(WORDS.size)]
        }

    private fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val VESPA_HOME = "/opt/vespa"

        /** The search node's state API, as `vespa-model-inspect` lists it on a single-node package. */
        const val STATE = "localhost:19113/state/v1/custom/component"

        const val DEFAULT_GROWFACTOR = 0.2
        const val META_STORE = "(document meta store)"
        const val UNATTRIBUTED = "unattributed (total-parts)"
        const val CHUNK = 2_000
        const val FEEDERS = 4
        const val CHECKPOINTS = 10
        const val SAMPLE_MILLIS = 500L
        const val SETTLE_MILLIS = 3_000L
        const val SETTLE_TRIES = 20
        val SIG = "e".repeat(128)

        /**
         * Seed multiplier for a document's own random stream. NOT 0x9E3779B97F4A7C15:
         * that is SplittableRandom's stream increment, so seeding document `seq` with
         * `seq * GAMMA` makes its stream document `seq - 1`'s shifted by one draw —
         * documents that draw a variable number of values before their id then draw
         * the SAME id, and overwrite one another (measured: ~60% of notes lost).
         */
        const val SEED_STRIDE = -0x40a7b892e31b1a47L

        /** `searchnode state=RUNNING mode=AUTO pid=453 exitstatus=0 id="content/search/cluster.content/0"` */
        val SEARCHNODE_PID = Regex("""^searchnode state=\S+\s+mode=\S+\s+pid=(\d+)""", RegexOption.MULTILINE)
        val CONFIG_ID = Regex("""^searchnode .*\bid="([^"]+)"""", RegexOption.MULTILINE)
        val RSS_ANON = Regex("""^RssAnon:\s+(\d+) kB""", RegexOption.MULTILINE)

        val AUTHORS: List<String> = SplittableRandom(7).let { r -> List(20_000) { hex(r, 64) } }

        val WORDS =
            (
                "bitcoin nostr relay zap lightning note freedom privacy protocol client key sign event post " +
                    "people world today good morning coffee music photo art code open source build ship test " +
                    "love time life work home game movie book read write learn think idea market price money " +
                    "node wallet channel payment fee block chain hash mining energy solar water food health run"
            ).split(" ")

        fun idOf(seq: Int): String = hex(SplittableRandom(seq * 0xC2B2AE3D27D4EB4FuL.toLong() + 3), 64)

        fun hex(
            rnd: SplittableRandom,
            chars: Int,
        ): String {
            val sb = StringBuilder(chars)
            while (sb.length < chars) {
                sb.append(
                    java.lang.Long
                        .toHexString(rnd.nextLong())
                        .padStart(16, '0'),
                )
            }
            return sb.substring(0, chars)
        }
    }
}
