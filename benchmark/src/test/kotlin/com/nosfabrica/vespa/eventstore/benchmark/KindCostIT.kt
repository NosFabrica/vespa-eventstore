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
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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
import java.io.File
import java.time.Duration
import java.util.SplittableRandom
import kotlin.test.assertTrue

/**
 * WHAT ONE EVENT COSTS, BY ITS SHAPE: attribute RAM and disk per document, for
 * the event shapes a mirror imports.
 *
 * `AttributeGrowthIT` sizes the reserve; this answers the question that comes
 * before it — whether WHICH events are imported moves the per-node budget, or
 * whether a document is a document. Each shape is fed into the same container
 * in turn, and its cost is the DELTA it adds, divided by its count:
 *
 *  - **attribute RAM**: the sum of `usedBytes` over every attribute plus the
 *    document meta store, from proton's state API. USED, not allocated:
 *    allocated moves in growth steps (the #206 sawtooth) that would land on
 *    whichever shape happened to cross one.
 *  - **document store** (disk; read through the page cache on every summary
 *    fill) and **text index** (disk, mmap'd — page cache, not RssAnon): bytes
 *    on disk after a forced flush, by `du`.
 *
 * Shapes later in the run share dictionary entries with earlier ones (the same
 * author pool, the same `e` targets), as a real corpus does, so a shape's delta
 * is its MARGINAL cost on a corpus already holding the others.
 *
 * A measurement, not a CI gate: self-skips without `-DitKindDocs` (documents
 * per shape).
 *
 * ```bash
 * ./gradlew :benchmark:test -Pintegration -DitKindDocs=100000 --tests '*KindCostIT*'
 * ```
 */
@Tag("integration")
class KindCostIT {
    @Test
    fun `attribute memory and disk per document, by event shape`() {
        assumeTrue(runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false), "Docker not available")
        val perShape = System.getProperty("itKindDocs")?.toIntOrNull()
        assumeTrue(perShape != null && perShape > 0, "set -DitKindDocs=N (documents per shape) to run this measurement")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val deployer = SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}")
                deployer.deploy(VespaApp.zipBytes(null))
                deployer.awaitServing(queryUrl)
                awaitOnline(vespa)

                val rows = ArrayList<Row>()
                VespaEventIndex(queryUrl).use { index ->
                    // A warm-up corpus first, so the first measured shape does not pay
                    // every structure's fixed minimum size on its own.
                    feed(index, Shape.NOTE, perShape!!, seqBase = 0)
                    var before = snapshot(vespa)
                    SHAPES.forEachIndexed { i, shape ->
                        val n = (perShape / shape.costDivisor).coerceAtLeast(1_000)
                        val kindBefore = runBlocking { index.count(EventQuery(kinds = listOf(shape.kind))) }
                        feed(index, shape, n, seqBase = (i + 1) * 10_000_000)
                        val after = snapshot(vespa)
                        val kindAfter = runBlocking { index.count(EventQuery(kinds = listOf(shape.kind))) }
                        println("  ${shape.label}: fed $n, kind ${shape.kind} count $kindBefore -> $kindAfter, ready +${after.ready - before.ready}")
                        rows += Row(shape, n, before, after)
                        before = after
                    }
                }
                report(rows)
                assertTrue(rows.all { it.ramPerDoc > 0 }, "a shape added no attribute memory — the readings are not moving")
                // Every shape mints a distinct document per seq; a shortfall means the
                // corpus collided with itself and every per-document figure is inflated.
                for (r in rows) {
                    assertTrue(r.stored >= r.fed * 0.99, "${r.shape.label}: fed ${r.fed}, stored ${r.stored} — the generator is minting duplicates")
                }

                val trust = trustPhase(vespa, queryUrl, "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}", perShape!!)
                writeResults(rows, trust)
            }
    }

    private enum class Shape(
        val label: String,
        val kind: Int,
        val costDivisor: Int = 1,
    ) {
        REACTION("kind 7 reaction (e, p)", 7),
        NOTE("kind 1 note (~40 words, 0-3 tags)", 1),
        REPOST("kind 6 repost (embedded event)", 6),
        ZAP("kind 9735 zap receipt", 9735),
        PROFILE("kind 0 profile", 0),
        CONTACTS("kind 3 contact list (150 p)", 3, costDivisor = 5),
        ASSERTION("kind 30382 trust assertion", 30382),
        GIFT_WRAP("kind 1059 gift wrap", 1059),
        ARTICLE("kind 30023 article (~400 words)", 30023, costDivisor = 5),
    }

    private class Snapshot(
        val ready: Long,
        val attrUsed: Map<String, Long>,
        val docStoreDisk: Long,
        val indexDisk: Long,
        /** Used bytes per STRUCTURE (`attribute :: component`), the unit a growth rule applies to. */
        val structUsed: Map<String, Long> = emptyMap(),
    )

    private inner class Row(
        val shape: Shape,
        val fed: Int,
        val before: Snapshot,
        val after: Snapshot,
    ) {
        /** Whether the store derives search columns for this shape at all (the NIP-50 path). */
        val searchable = doc(shape, 1).search != SearchFields.NONE

        val stored = (after.ready - before.ready).coerceAtLeast(1)
        val ramPerDoc = (after.attrUsed.values.sum() - before.attrUsed.values.sum()).toDouble() / stored
        val docStorePerDoc = (after.docStoreDisk - before.docStoreDisk).toDouble() / stored
        val indexPerDoc = (after.indexDisk - before.indexDisk).toDouble() / stored

        /** Used bytes per document this shape added to each structure. */
        fun perStructure(): Map<String, Double> =
            (after.structUsed.keys + before.structUsed.keys).associateWith {
                ((after.structUsed[it] ?: 0) - (before.structUsed[it] ?: 0)).toDouble() / stored
            }

        fun top(k: Int) =
            after.attrUsed.keys
                .map { it to ((after.attrUsed[it] ?: 0) - (before.attrUsed[it] ?: 0)).toDouble() / stored }
                .sortedByDescending { it.second }
                .take(k)
    }

    private fun feed(
        index: VespaEventIndex,
        shape: Shape,
        n: Int,
        seqBase: Int,
    ) = runBlocking {
        var fed = 0
        while (fed < n) {
            val wave = minOf(CHUNK * FEEDERS, n - fed)
            val base = seqBase + fed
            coroutineScope {
                (0 until wave step CHUNK)
                    .map { off -> async { index.putAll(List(minOf(CHUNK, wave - off)) { i -> doc(shape, base + off + i) }) } }
                    .awaitAll()
            }
            fed += wave
        }
    }

    // ---- reading the node -----------------------------------------------------

    /** Flush to disk, let the statistics settle, then read every counter. */
    private fun snapshot(
        vespa: GenericContainer<*>,
        docType: String = "event",
    ): Snapshot {
        exec(vespa, "$VESPA_HOME/bin/vespa-proton-cmd --local triggerFlush")
        repeat(120) {
            val idx = Json.parseToJsonElement(exec(vespa, "curl -sf ${ready(docType)}/index")).jsonObject
            val inMemory = idx["memoryIndexes"]?.jsonArray?.sumOf { it.jsonObject["docsInMemory"]?.jsonPrimitive?.longOrNull ?: 0L } ?: 0L
            if (inMemory == 0L) return@repeat
            Thread.sleep(1_000)
        }
        var last = attrState(vespa, docType)
        repeat(20) {
            Thread.sleep(3_000)
            val next = attrState(vespa, docType)
            if (next == last) return read(vespa, docType, next)
            last = next
        }
        return read(vespa, docType, last)
    }

    private fun read(
        vespa: GenericContainer<*>,
        docType: String,
        state: Map<String, JsonObject>,
    ): Snapshot {
        val attrs = state.mapNotNull { (n, o) -> used(o)?.let { n to it } }.toMap()
        val structs = HashMap<String, Long>()
        for ((n, o) in state) structures(o)?.forEach { (c, u) -> structs["$n :: $c"] = u }
        val ready =
            Json
                .parseToJsonElement(exec(vespa, "curl -sf $STATE/documentdb/$docType"))
                .jsonObject["documents"]!!
                .jsonObject["ready"]!!
                .jsonPrimitive.long
        return Snapshot(ready, attrs, du(vespa, docType, "summary"), du(vespa, docType, "index"), structs)
    }

    private fun du(
        vespa: GenericContainer<*>,
        docType: String,
        dir: String,
    ): Long = exec(vespa, "if [ -d $DOCUMENTS/$docType/0.ready/$dir ]; then du -sb $DOCUMENTS/$docType/0.ready/$dir | cut -f1; else echo 0; fi").toLong()

    private fun ready(docType: String) = "$STATE/documentdb/$docType/subdb/ready"

    private fun attrState(
        vespa: GenericContainer<*>,
        docType: String,
    ): Map<String, JsonObject> {
        val base = ready(docType)
        val names =
            Json
                .parseToJsonElement(exec(vespa, "curl -sf $base/attribute"))
                .jsonObject.keys
                .sorted()
        val script =
            buildString {
                append("curl -sf $base/documentmetastore; echo;")
                for (n in names) append(" curl -sf $base/attribute/$n; echo;")
            }
        val lines = exec(vespa, script).lines().filter { it.isNotBlank() }
        val out = LinkedHashMap<String, JsonObject>()
        (listOf(META_STORE) + names).zip(lines).forEach { (n, line) ->
            val o = Json.parseToJsonElement(line).jsonObject
            if (used(o) != null) out[n] = o
        }
        return out
    }

    /**
     * Used bytes by component, named as `AttributeGrowthIT` names them so the two
     * measurements join on the same keys: the enum store's values and dictionary,
     * the multi-value arrays, the posting store, and what the named parts leave
     * (the per-document vector on a single-value attribute; the whole lid table on
     * the meta store). Clamped at zero: on a multi-value attribute the named parts
     * overlap, and a negative remainder is not a structure.
     */
    private fun structures(state: JsonObject): Map<String, Long>? {
        val total = used(state) ?: return null
        val parts = LinkedHashMap<String, Long>()

        fun part(
            name: String,
            o: JsonObject?,
        ) {
            o
                ?.get("used")
                ?.jsonPrimitive
                ?.longOrNull
                ?.let { parts[name] = it }
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
        parts[UNATTRIBUTED] = (total - parts.values.sum()).coerceAtLeast(0)
        return parts
    }

    private fun used(state: JsonObject): Long? =
        state["status"]
            ?.jsonObject
            ?.get("memoryUsage")
            ?.jsonObject
            ?.get("usedBytes")
            ?.jsonPrimitive
            ?.longOrNull

    private fun awaitOnline(vespa: GenericContainer<*>) {
        repeat(120) {
            if (runCatching { exec(vespa, "curl -sf $STATE/documentdb/event") }.getOrNull()?.contains("\"ONLINE\"") == true) return
            Thread.sleep(1_000)
        }
        error("the event document db never came ONLINE")
    }

    private fun exec(
        vespa: GenericContainer<*>,
        command: String,
    ): String {
        val res = vespa.execInContainer("bash", "-c", command)
        require(res.exitCode == 0) { "`${command.take(120)}` failed (${res.exitCode}): ${res.stderr}" }
        return res.stdout.trim()
    }

    private fun report(rows: List<Row>) {
        println("\nper stored document, marginal (B = bytes):")
        println(String.format("  %-45s %8s %10s %12s %12s  %s", "shape", "stored", "attr RAM", "doc store", "text index", "largest attribute deltas"))
        for (r in rows) {
            println(
                String.format(
                    "  %-45s %,8d %8.0f B %10.0f B %10.0f B  %s",
                    r.shape.label + if (r.searchable) " [search]" else "",
                    r.stored,
                    r.ramPerDoc,
                    r.docStorePerDoc,
                    r.indexPerDoc,
                    r.top(3).joinToString(", ") { (n, b) -> "$n ${"%.0f".format(b)}" },
                ),
            )
        }
    }

    // ---- trust assertions through the store -----------------------------------------

    /**
     * Trust assertions through the real write path, where a card also writes a CELL
     * into its subject's `reputation` document — a `global` document type, so it is
     * held IN FULL on every content node rather than split across them like events.
     * The provider is enrolled by an observer's 10040 first (a card from a service no
     * list names projects nothing). Three passes: [n] cards about [n] new subjects (a
     * new reputation document each); a second enrolled provider's cards about the
     * SAME subjects (a cell on an existing document); then [DENSE_PROVIDERS] more
     * providers scoring the same [DENSE_SUBJECTS] subjects — a subject carrying ~50
     * cells, because on staging 1,550 enrolled services score largely the same
     * people, and a tensor's per-cell cost need not stay flat as it fills.
     */
    private fun trustPhase(
        vespa: GenericContainer<*>,
        queryUrl: String,
        configUrl: String,
        n: Int,
    ): List<TrustPass> {
        val results = ArrayList<TrustPass>()
        val subjects = SplittableRandom(99).let { r -> List(n) { hex(r, 64) } }
        val dense = SplittableRandom(98).let { r -> List(DENSE_PROVIDERS) { hex(r, 64) } }
        val passes =
            listOf(
                Triple("provider 1: new subjects (new reputation docs)", listOf(SERVICES[0]), subjects),
                Triple("provider 2: same subjects (2nd cell each)", listOf(SERVICES[1]), subjects),
                Triple("providers 3-${DENSE_PROVIDERS + 2}: ${DENSE_SUBJECTS / 1000}k subjects to ${DENSE_PROVIDERS + 2} cells", dense, subjects.take(DENSE_SUBJECTS)),
            )
        val providers = passes.flatMap { it.second }
        println("\ntrust assertions through the store (kind 30382, enrolled provider), per card:")
        println(String.format("  %-48s %10s %12s %16s %12s", "pass", "event RAM", "event disk", "reputation RAM", "rep. disk"))
        VespaEventStore
            .open(url = queryUrl, autoDeploy = false, configUrl = configUrl, deferTrustProjection = false)
            .use { store ->
                runBlocking {
                    store.insert(
                        TrustProviderListEvent(
                            hex(SplittableRandom(5), 64),
                            OBSERVER,
                            1_700_000_000L,
                            providers
                                .flatMap { p ->
                                    listOf(arrayOf("30382:rank", p, "wss://scores.example.com/"), arrayOf("30382:followers", p, "wss://scores.example.com/"))
                                }.toTypedArray(),
                            "",
                            "",
                        ),
                    )
                    for ((pass, spec) in passes.withIndex()) {
                        val (label, signers, about) = spec
                        val evBefore = snapshot(vespa)
                        val repBefore = snapshot(vespa, "reputation")
                        val rnd = SplittableRandom(1_000L + pass)
                        signers
                            .flatMap { provider -> about.map { provider to it } }
                            .map { (provider, about) ->
                                UserAssertionEvent(
                                    hex(rnd, 64),
                                    provider,
                                    1_700_000_000L,
                                    arrayOf(
                                        arrayOf("d", about),
                                        arrayOf("rank", rnd.nextInt(101).toString()),
                                        arrayOf("followers", rnd.nextInt(5_000).toString()),
                                    ),
                                    "",
                                    SIG,
                                )
                            }.chunked(1_000)
                            .forEach { store.batchInsert(it) }
                        store.awaitTrustProjection()
                        val evAfter = snapshot(vespa)
                        val repAfter = snapshot(vespa, "reputation")
                        val cards = (signers.size * about.size).toDouble()
                        results +=
                            TrustPass(
                                label,
                                cards.toLong(),
                                perCard(evBefore, evAfter, cards),
                                perCard(repBefore, repAfter, cards),
                            )
                        println(
                            String.format(
                                "  %-48s %8.0f B %10.0f B %14.0f B %10.0f B   (reputation docs %,d -> %,d)",
                                label,
                                (evAfter.attrUsed.values.sum() - evBefore.attrUsed.values.sum()) / cards,
                                ((evAfter.docStoreDisk + evAfter.indexDisk) - (evBefore.docStoreDisk + evBefore.indexDisk)) / cards,
                                (repAfter.attrUsed.values.sum() - repBefore.attrUsed.values.sum()) / cards,
                                ((repAfter.docStoreDisk + repAfter.indexDisk) - (repBefore.docStoreDisk + repBefore.indexDisk)) / cards,
                                repBefore.ready,
                                repAfter.ready,
                            ),
                        )
                    }
                }
            }
        return results
    }

    private class TrustPass(
        val label: String,
        val cards: Long,
        val event: Map<String, Double>,
        val reputation: Map<String, Double>,
    )

    private fun perCard(
        before: Snapshot,
        after: Snapshot,
        cards: Double,
    ): Map<String, Double> =
        (after.structUsed.keys + before.structUsed.keys).associateWith {
            ((after.structUsed[it] ?: 0) - (before.structUsed[it] ?: 0)) / cards
        }

    /**
     * The per-structure figures as JSON, for `benchmark/reserve_by_kind.py` to join
     * with `AttributeGrowthIT`'s per-structure reserve ratios and a corpus's kind
     * counts. Written under the module's build directory.
     */
    private fun writeResults(
        rows: List<Row>,
        trust: List<TrustPass>,
    ) {
        fun obj(m: Map<String, Double>) = JsonObject(m.filterValues { it != 0.0 }.mapValues { JsonPrimitive(it.value) })
        val json =
            JsonObject(
                mapOf(
                    "shapes" to
                        JsonObject(
                            rows.associate { r ->
                                r.shape.name to
                                    JsonObject(
                                        mapOf(
                                            "kind" to JsonPrimitive(r.shape.kind),
                                            "label" to JsonPrimitive(r.shape.label),
                                            "stored" to JsonPrimitive(r.stored),
                                            "searchable" to JsonPrimitive(r.searchable),
                                            "used_per_doc" to obj(r.perStructure()),
                                        ),
                                    )
                            },
                        ),
                    "trust" to
                        JsonArray(
                            trust.map { t ->
                                JsonObject(
                                    mapOf(
                                        "label" to JsonPrimitive(t.label),
                                        "cards" to JsonPrimitive(t.cards),
                                        "event_used_per_card" to obj(t.event),
                                        "reputation_used_per_card" to obj(t.reputation),
                                    ),
                                )
                            },
                        ),
                ),
            )
        val out = File("build/measurements/kind-cost.json")
        out.parentFile.mkdirs()
        out.writeText(json.toString())
        println("\nper-structure results: ${out.absolutePath}")
    }

    // ---- the shapes -------------------------------------------------------------

    private fun doc(
        shape: Shape,
        seq: Int,
    ): EventDoc {
        val rnd = SplittableRandom(seq * SEED_STRIDE + 11)
        val createdAt = 1_700_000_000L + seq % 50_000_000
        val pooled = AUTHORS[rnd.nextInt(AUTHORS.size)]
        val fresh = hex(rnd, 64)

        fun target() = hex(SplittableRandom((rnd.nextInt(5_000_000)).toLong() * 31 + 7), 64)

        fun someone() = AUTHORS[rnd.nextInt(AUTHORS.size)]
        val tags = ArrayList<List<String>>()
        var author = pooled
        val content: String
        when (shape) {
            Shape.REACTION -> {
                tags += listOf("e", target())
                tags += listOf("p", someone())
                content = "+"
            }

            Shape.NOTE -> {
                repeat(rnd.nextInt(2)) { tags += listOf("e", target()) }
                repeat(rnd.nextInt(3)) { tags += listOf("p", someone()) }
                repeat(rnd.nextInt(2)) { tags += listOf("t", WORDS[rnd.nextInt(WORDS.size)]) }
                content = sentence(rnd, 25 + rnd.nextInt(30))
            }

            Shape.REPOST -> {
                val e = target()
                val p = someone()
                tags += listOf("e", e)
                tags += listOf("p", p)
                content =
                    """{"id":"$e","pubkey":"$p","created_at":$createdAt,"kind":1,"tags":[],"content":"${sentence(rnd, 40)}","sig":"${hex(rnd, 128)}"}"""
            }

            Shape.ZAP -> {
                val p = someone()
                val zapper = someone()
                val e = target()
                tags += listOf("p", p)
                tags += listOf("P", zapper)
                tags += listOf("e", e)
                tags += listOf("bolt11", "lnbc21u1p" + hex(rnd, 380))
                tags += listOf("preimage", hex(rnd, 64))
                tags +=
                    listOf(
                        "description",
                        """{"kind":9734,"pubkey":"$zapper","content":"","tags":[["p","$p"],["e","$e"],["amount","21000"],""" +
                            """["relays","wss://relay.damus.io","wss://nos.lol"]],"created_at":$createdAt,"id":"${hex(rnd, 64)}","sig":"${hex(rnd, 128)}"}""",
                    )
                author = SERVICES[rnd.nextInt(SERVICES.size)]
                content = ""
            }

            Shape.PROFILE -> {
                author = fresh
                val name = WORDS[rnd.nextInt(WORDS.size)] + rnd.nextInt(100_000)
                content =
                    """{"name":"$name","display_name":"${name.replaceFirstChar { it.uppercase() }}","about":"${sentence(rnd, 15)}",""" +
                    """"picture":"https://example.com/${hex(rnd, 16)}.jpg","nip05":"$name@example.com","lud16":"$name@getalby.com"}"""
            }

            Shape.CONTACTS -> {
                author = fresh
                repeat(150) { tags += listOf("p", someone()) }
                content = ""
            }

            Shape.ASSERTION -> {
                author = SERVICES[0]
                tags += listOf("d", hex(rnd, 64))
                tags += listOf("rank", rnd.nextInt(101).toString())
                tags += listOf("followers", rnd.nextInt(5_000).toString())
                content = ""
            }

            Shape.GIFT_WRAP -> {
                author = fresh
                tags += listOf("p", someone())
                content = base64(rnd, 1_800)
            }

            Shape.ARTICLE -> {
                tags += listOf("d", "article-$seq")
                tags += listOf("title", sentence(rnd, 6))
                repeat(3) { tags += listOf("t", WORDS[rnd.nextInt(WORDS.size)]) }
                content = sentence(rnd, 400)
            }
        }
        val kind = shape.kind
        val id = hex(rnd, 64)
        val search =
            SearchExtractors.extract(EventFactory.create<Event>(id, author, createdAt, kind, Array(tags.size) { tags[it].toTypedArray() }, content, SIG))
        // A gift wrap's owner is its recipient (EventDocConversion.owner).
        val owner = if (kind == 1059) tags.first()[1] else author
        return EventDoc(id, author, createdAt, kind, tags, content, SIG, owner = owner, search = search)
    }

    private fun sentence(
        rnd: SplittableRandom,
        words: Int,
    ): String =
        (0 until words).joinToString(" ") {
            if (rnd.nextInt(10) == 0) "w" + Integer.toString(rnd.nextInt(5_000_000), 36) else WORDS[rnd.nextInt(WORDS.size)]
        }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val VESPA_HOME = "/opt/vespa"
        const val STATE = "localhost:19113/state/v1/custom/component"
        const val DOCUMENTS = "/opt/vespa/var/db/vespa/search/cluster.content/n0/documents"
        const val CHUNK = 2_000
        const val FEEDERS = 4
        val SIG = "e".repeat(128)

        /**
         * Seed multiplier for a document's own random stream. NOT 0x9E3779B97F4A7C15:
         * that is SplittableRandom's stream increment, so seeding document `seq` with
         * `seq * GAMMA` makes its stream document `seq - 1`'s shifted by one draw —
         * documents that draw a variable number of values before their id then draw
         * the SAME id, and overwrite one another (measured: ~60% of notes lost).
         */
        const val SEED_STRIDE = -0x40a7b892e31b1a47L

        val SHAPES = Shape.entries

        val AUTHORS: List<String> = SplittableRandom(7).let { r -> List(20_000) { hex(r, 64) } }
        val SERVICES: List<String> = SplittableRandom(8).let { r -> List(20) { hex(r, 64) } }
        val OBSERVER = "c".repeat(64)
        const val META_STORE = "(document meta store)"
        const val UNATTRIBUTED = "unattributed (total-parts)"
        const val DENSE_PROVIDERS = 48
        const val DENSE_SUBJECTS = 20_000

        val WORDS =
            (
                "bitcoin nostr relay zap lightning note freedom privacy protocol client key sign event post " +
                    "people world today good morning coffee music photo art code open source build ship test " +
                    "love time life work home game movie book read write learn think idea market price money " +
                    "node wallet channel payment fee block chain hash mining energy solar water food health run"
            ).split(" ")

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

        fun base64(
            rnd: SplittableRandom,
            chars: Int,
        ): String {
            val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            return String(CharArray(chars) { alphabet[rnd.nextInt(64)] })
        }
    }
}
