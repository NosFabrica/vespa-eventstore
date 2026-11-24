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
package com.nosfabrica.vespa.eventstore.benchmark.probe

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.SchemaDeploy
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.eventstore.engine.client.BucketMove
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.random.Random

/**
 * NEW USERS ARRIVING, ON REAL DATA, AND THE AUTHOR SPLIT THEY WILL FORCE
 * (`docs/growth-simulation.md`). The question it answers: when customers keep enrolling, each with
 * a personalized trust service and that service's ~150 k cards, what does each one cost the
 * nodes, and can the store be split from one content cluster into two in the middle of that growth
 * without losing, duplicating or mis-ranking anything, while the relay keeps serving?
 *
 * Against a FRESH Vespa, with a slice of the production relay (`./gradlew :benchmark:relaySlice`):
 *
 *  1. **Load** the slice through the real store (one cluster): its events, every kind 10040 it
 *     holds, and its template services' kind 30382 cards, so the trust projection builds the same
 *     reputation documents production would from them.
 *  2. **Grow**: users arrive one at a time. Each publishes a kind 10040 naming a NEW personalized
 *     service, and that service publishes a card for every subject a real service scored (cloned
 *     from the template, ranks jittered). Fed through the store, so the projection does what it
 *     does in production. After each arrival: the reputation document type's documents, memory
 *     and disk per search process, the time the arrival took, and the read latencies meanwhile.
 *  3. **Split**, after `SIM_USERS_BEFORE` arrivals: one cluster becomes two (`SIM_SPLIT_MAP`) with
 *     `BucketMove`, while `SIM_USERS_DURING` more users arrive and background traffic continues.
 *     Every store restarts on the overlap map, then on the final map, as a rolling restart would.
 *  4. **Grow again**: `SIM_USERS_AFTER` arrivals on the split store.
 *  5. **Verify**: `verifyTrust` (every reputation document against an exact derive from the stored
 *     cards), every event-kind NIP-01 read compared with an in-memory reference store fed the
 *     same events, and the relay's statistics pipelines (`vespa-relay` `StatsYql`) checked for
 *     consistency on the split store.
 *
 * Background traffic throughout: new notes by real authors (and some of their deletions) written to
 * both stores, and a reader issuing relay-shaped REQs, some through a real user's trust lens.
 *
 * Prints progress and a final `REPORT {json}`, and writes the same JSON to `SIM_REPORT`.
 * Env: BENCH_VESPA_URL, BENCH_CONFIG_URL, SIM_DIR (build/relay-slice), SIM_USERS_BEFORE (4),
 * SIM_USERS_DURING (1), SIM_USERS_AFTER (2), SIM_CARDS_PER_USER (150000), SIM_SPLIT_MAP
 * ("00-7f=0,80-ff=1"), SIM_NOTES_PER_SEC (20), SIM_REPORT (build/growth-sim/report.json).
 */
object GrowthSimulation {
    private val http: HttpClient = HttpClient.newHttpClient()

    private fun hex(seed: String): String = MessageDigest.getInstance("SHA-256").digest(seed.encodeToByteArray()).joinToString("") { "%02x".format(it) }

    /** A store event rebuilt through quartz's factory, so it carries its kind's class (the store dispatches on it). */
    private fun typed(e: Event): Event = EventFactory.create(e.id, e.pubKey, e.createdAt, e.kind, e.tags, e.content, e.sig)

    private fun read(file: File): List<Event> = file.readLines().filter { it.isNotBlank() }.map { typed(Event.fromJson(it)) }

    // ---- the run's state -------------------------------------------------------------

    private class Phase(
        val name: String,
    ) {
        val latencies = ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>>()
        val errors = ConcurrentHashMap<String, Int>()
    }

    private val phase = AtomicReference(Phase("load"))
    private val report = LinkedHashMap<String, Any?>()
    private val arrivals = ArrayList<Map<String, Any?>>()

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val configUrl = System.getenv("BENCH_CONFIG_URL") ?: "http://127.0.0.1:19071"
            val dir = File(System.getenv("SIM_DIR") ?: "build/relay-slice")
            val before = System.getenv("SIM_USERS_BEFORE")?.toInt() ?: 4
            val during = System.getenv("SIM_USERS_DURING")?.toInt() ?: 1
            val after = System.getenv("SIM_USERS_AFTER")?.toInt() ?: 2
            val cardsPerUser = System.getenv("SIM_CARDS_PER_USER")?.toInt() ?: 150_000
            val to = AuthorShards.BucketMap.parse(System.getenv("SIM_SPLIT_MAP") ?: "00-7f=0,80-ff=1")
            val notesPerSec = System.getenv("SIM_NOTES_PER_SEC")?.toInt() ?: 20
            val reportFile = File(System.getenv("SIM_REPORT") ?: "build/growth-sim/report.json")
            require(to.clusters == 2) { "the simulation splits one cluster into two" }

            // ---- the slice ----
            val events = read(File(dir, "events.jsonl"))
            val lists = read(File(dir, "lists.jsonl"))
            val templateFiles = dir.listFiles { f -> f.name.startsWith("cards-") }!!.sortedByDescending { it.length() }
            val templates = templateFiles.map(::read)
            val template = templates.first().take(cardsPerUser)
            println("slice: ${events.size} events, ${lists.size} lists, templates ${templates.map { it.size }}; each new user clones ${template.size} cards")
            report["slice"] = mapOf("events" to events.size, "lists" to lists.size, "templateCards" to templates.map { it.size }, "cardsPerUser" to template.size)

            // A real user whose lens reads go through the template service: the canonical provider's observer.
            val templateService = template.first().pubKey
            val realObserver = lists.firstOrNull { l -> l.tags.any { it.size >= 2 && it[0] == "30382:rank" && it[1] == templateService } }?.pubKey
            val authors = events.map { it.pubKey }.distinct()

            // ---- phase 1: load, one cluster ----
            val deployer = SchemaDeployer(configUrl)
            check(!deployer.isServing(url)) { "run against a fresh Vespa" }
            val reference = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            val store = AtomicReference(open(url, configUrl, AuthorShards.BucketMap.even(1), SchemaDeploy.IF_ABSENT))
            val t0 = System.nanoTime()
            events.chunked(1_000).forEach { chunk ->
                store.get().batchInsert(chunk)
                reference.batchInsert(chunk)
            }
            store.get().batchInsert(lists)
            templates.forEach { cards -> cards.chunked(1_000).forEach { store.get().batchInsert(it) } }
            store.get().awaitTrustProjection()
            val loadSeconds = secs(t0)
            val loadedTrust = settledTrust(url)
            report["load"] = mapOf("seconds" to loadSeconds, "trust" to loadedTrust)
            println("loaded in ${loadSeconds}s: $loadedTrust")

            // ---- background traffic ----
            val running = AtomicBoolean(true)
            val paused = AtomicBoolean(false)
            val gate = Mutex()
            val written = ConcurrentLinkedQueue<Event>()
            val traffic = CoroutineScope(Dispatchers.IO)
            val writer =
                traffic.launch {
                    val rnd = Random(5)
                    var n = 0
                    while (running.get()) {
                        if (!paused.get()) {
                            gate.withLock {
                                val now = System.currentTimeMillis() / 1000
                                val author = authors[rnd.nextInt(authors.size)]
                                val e =
                                    if (n % 10 == 9 && written.isNotEmpty()) {
                                        val victim = written.random(rnd)
                                        deletion(victim, now)
                                    } else {
                                        note(author, now, n, events[rnd.nextInt(events.size)].content.take(280))
                                    }
                                runCatching { store.get().batchInsert(listOf(e)) }.onFailure { phase.get().errors.merge("write", 1, Int::plus) }
                                reference.batchInsert(listOf(e))
                                if (e.kind == 1) written += e
                                n++
                            }
                        }
                        delay(1_000L / notesPerSec)
                    }
                }
            val reader =
                traffic.launch {
                    val rnd = Random(6)
                    val classes = listOf("global", "author", "hashtag", "lensed", "lensedSearch", "count")
                    var i = 0
                    while (running.get()) {
                        if (paused.get()) {
                            delay(50)
                            continue
                        }
                        val cls = classes[i++ % classes.size]
                        val lens = realObserver ?: authors.first()
                        val f =
                            when (cls) {
                                "global" -> Filter(kinds = listOf(1), limit = 50)
                                "author" -> Filter(kinds = listOf(1), authors = listOf(authors[rnd.nextInt(authors.size)]), limit = 20)
                                "hashtag" -> Filter(kinds = listOf(1), tags = mapOf("t" to listOf("nostr")), limit = 50)
                                "lensed" -> Filter(kinds = listOf(1), limit = 50, search = "observer:$lens")
                                "lensedSearch" -> Filter(kinds = listOf(1), limit = 20, search = "bitcoin observer:$lens")
                                else -> Filter(kinds = listOf(1), since = System.currentTimeMillis() / 1000 - 3_600)
                            }
                        val t = System.nanoTime()
                        runCatching { if (cls == "count") store.get().count(f) else store.get().query<Event>(f) }
                            .onSuccess { phase.get().latencies.getOrPut(cls) { ConcurrentLinkedQueue() } += System.nanoTime() - t }
                            .onFailure { phase.get().errors.merge(cls, 1, Int::plus) }
                    }
                }

            // ---- phase 2: grow on one cluster ----
            var user = 0
            repeat(before) { arrive(++user, template, store.get(), url, "one cluster") }
            report["statsBeforeSplit"] = statsConsistency(store.get())
            println("stats pipelines on one cluster: ${report["statsBeforeSplit"]}")

            // ---- phase 3: split, while users keep arriving ----
            val from = AuthorShards.BucketMap.even(1)
            val move = BucketMove(url, from, to)
            val steps = LinkedHashMap<String, Any?>()
            phase.set(Phase("split"))

            suspend fun restart(map: AuthorShards.BucketMap) {
                // A rolling restart: traffic pauses, the process reopens on the new map, traffic resumes.
                paused.set(true)
                gate.withLock {
                    val old = store.getAndSet(open(url, configUrl, map, SchemaDeploy.NEVER))
                    old.close()
                }
                paused.set(false)
            }
            var t = System.nanoTime()
            restart(move.overlap)
            steps["restartOnOverlap"] = secs(t)
            t = System.nanoTime()
            check(deployer.deployIfChanged(url, VespaApp.zipBytes(null, move.overlap))) { "the overlap package did not deploy" }
            steps["deployOverlap"] = secs(t)
            t = System.nanoTime()
            val arrivingDuring = traffic.launch { repeat(during) { arrive(++user, template, store.get(), url, "during the split") } }
            val copied = move.copy()
            steps["copy"] = mapOf("seconds" to secs(t), "events" to copied.events, "reputations" to copied.reputations)
            arrivingDuring.join()
            t = System.nanoTime()
            val repairs = ArrayList<String>()
            var diff = move.diff(repair = true)
            repairs += diff.toString()
            while (!diff.clean && repairs.size < 8) {
                diff = move.diff(repair = true)
                repairs += diff.toString()
            }
            check(diff.clean) { "the copy never verified: $repairs" }
            steps["verifyCopy"] = mapOf("seconds" to secs(t), "passes" to repairs)
            t = System.nanoTime()
            restart(to)
            steps["restartOnFinal"] = secs(t)
            t = System.nanoTime()
            val refused = runCatching { deployer.deployIfChanged(url, VespaApp.zipBytes(null, to)) }.exceptionOrNull()
            check(refused is IllegalStateException) { "the store's guard let a narrowing deploy through" }
            val zip = VespaApp.zipBytes(null, to)
            deployer.checkShardChange(url, zip, allowNarrowing = true)
            deployer.awaitConverged(SchemaDeployer.generationOf(deployer.deploy(zip)))
            steps["deployFinal"] = secs(t)
            t = System.nanoTime()
            val cleaned = move.cleanup()
            steps["cleanup"] = mapOf("seconds" to secs(t), "events" to cleaned.events, "reputations" to cleaned.reputations)
            steps["remaining"] = move.remaining().toString()
            move.close()
            steps["readsDuringSplit"] = latencies(phase.get())
            report["split"] = steps
            println("split: $steps")

            // ---- phase 4: grow on two clusters ----
            repeat(after) { arrive(++user, template, store.get(), url, "two clusters") }

            // ---- phase 5: verify ----
            running.set(false)
            writer.join()
            reader.join()
            val s = store.get()
            s.awaitTrustProjection()
            val verification = LinkedHashMap<String, Any?>()
            val audit = s.verifyTrust()
            verification["trust"] = mapOf("subjectsChecked" to audit.subjectsChecked, "parentsChecked" to audit.parentsChecked, "drift" to audit.driftCount)
            verification["events"] = compare(s, reference, authors, Random(11))
            verification["statsPipelines"] = statsConsistency(s)
            report["verify"] = verification
            report["arrivals"] = arrivals
            println("verify: $verification")
            s.close()

            // ---- after: does the source cluster give its memory back? ----
            // Removing documents frees their attribute memory only when the engine compacts its
            // local document id space, a background job. Sampled for up to SIM_SETTLE_MINUTES.
            val settle = ArrayList<Map<String, Any?>>()
            val settleStart = System.nanoTime()
            val minutes = System.getenv("SIM_SETTLE_MINUTES")?.toInt() ?: 10
            while (secs(settleStart) < minutes * 60) {
                settle += mapOf("seconds" to secs(settleStart), "trust" to trustMetrics(url))
                delay(30_000)
            }
            settle += mapOf("seconds" to secs(settleStart), "trust" to trustMetrics(url))
            report["afterSplitMemory"] = settle

            reportFile.parentFile.mkdirs()
            val json = value(report)
            reportFile.writeText(json)
            println("REPORT $json")
        }

    // ---- pieces ------------------------------------------------------------------------

    private suspend fun open(
        url: String,
        configUrl: String,
        map: AuthorShards.BucketMap,
        schema: SchemaDeploy,
    ): VespaEventStore =
        VespaEventStore.open(
            url = url,
            configUrl = configUrl,
            schema = schema,
            tuning = EngineTuning(authorBuckets = if (map.clusters == 1 && !map.moving) null else map.spec()),
        )

    /**
     * One new user: a kind 10040 naming a new personalized service, then that service's cards, one
     * per subject the template scored, ranks jittered by up to ±5. Measured from the 10040 to the
     * projection settling; telemetry read afterwards.
     */
    private suspend fun arrive(
        n: Int,
        template: List<Event>,
        store: VespaEventStore,
        url: String,
        topology: String,
    ) {
        val p = Phase("user $n")
        val previous = phase.getAndSet(p)
        val observer = hex("sim-observer-$n")
        val service = hex("sim-service-$n")
        val now = System.currentTimeMillis() / 1000
        val rnd = Random(n)
        val t0 = System.nanoTime()
        store.batchInsert(listOf(typedNew(observer, now, 10040, arrayOf(arrayOf("30382:rank", service, "wss://sim.invalid"), arrayOf("30382:followers", service, "wss://sim.invalid")), "")))
        template.chunked(1_000).forEach { chunk ->
            store.batchInsert(
                chunk.map { c ->
                    val tags =
                        c.tags
                            .map { tag ->
                                if (tag.size >= 2 && tag[0] == "rank") arrayOf("rank", ((tag[1].toIntOrNull() ?: 0) + rnd.nextInt(-5, 6)).coerceIn(0, 100).toString()) else tag
                            }.toTypedArray()
                    typedNew(service, now, 30382, tags, "")
                },
            )
        }
        store.awaitTrustProjection()
        val seconds = secs(t0)
        val trust = settledTrust(url)
        val row =
            linkedMapOf(
                "user" to n,
                "topology" to topology,
                "seconds" to seconds,
                "cardsPerSecond" to (template.size / seconds.coerceAtLeast(0.001)).toLong(),
                "trust" to trust,
                "reads" to latencies(p),
            )
        arrivals += row
        println("user $n ($topology): ${seconds}s, trust $trust")
        if (previous.name == "split") phase.set(previous)
    }

    private fun typedNew(
        pubkey: String,
        createdAt: Long,
        kind: Int,
        tags: Array<Array<String>>,
        content: String,
    ): Event {
        val id = hex("$pubkey|$createdAt|$kind|${tags.joinToString { it.joinToString(":") }}|$content")
        return EventFactory.create(id, pubkey, createdAt, kind, tags, content, "e".repeat(128))
    }

    private fun note(
        author: String,
        now: Long,
        n: Int,
        content: String,
    ): Event = typedNew(author, now, 1, if (n % 4 == 0) arrayOf(arrayOf("t", "nostr")) else emptyArray(), "$content #$n")

    private fun deletion(
        victim: Event,
        now: Long,
    ): Event = typedNew(victim.pubKey, now, 5, arrayOf(arrayOf("e", victim.id)), "")

    /**
     * [trustMetrics] once every search process's snapshot was TAKEN AFTER this call began: Vespa
     * publishes metrics in periodic snapshots, each service stamped with its `timestamp`, so a read
     * right after a write reports a snapshot from before it. (The first cut waited for two reads
     * to agree, and two reads of one stale snapshot agree: it reported 5,844 reputation documents
     * where a search counted 162,376.)
     */
    private suspend fun settledTrust(url: String): Map<String, Map<String, Long>> {
        val after = System.currentTimeMillis() / 1000
        repeat(40) {
            val stamps = searchnodeTimestamps(url)
            if (stamps.isNotEmpty() && stamps.all { it > after }) return trustMetrics(url)
            delay(5_000)
        }
        return trustMetrics(url)
    }

    private fun searchnodeTimestamps(url: String): List<Long> {
        val body = http.send(HttpRequest.newBuilder(URI.create("$url/metrics/v2/values")).GET().build(), HttpResponse.BodyHandlers.ofString()).body()
        return Json
            .parseToJsonElement(body)
            .jsonObject["nodes"]!!
            .jsonArray
            .flatMap { it.jsonObject["services"]!!.jsonArray }
            .filter { it.jsonObject["name"]?.jsonPrimitive?.content == "vespa.searchnode" }
            .mapNotNull { it.jsonObject["timestamp"]?.jsonPrimitive?.longOrNull }
    }

    /** The reputation type's documents, memory, disk and log PER SEARCH PROCESS (one per cluster on this host). */
    private fun trustMetrics(url: String): Map<String, Map<String, Long>> {
        val body = http.send(HttpRequest.newBuilder(URI.create("$url/metrics/v2/values")).GET().build(), HttpResponse.BodyHandlers.ofString()).body()
        val out = sortedMapOf<String, MutableMap<String, Long>>()
        for (node in Json.parseToJsonElement(body).jsonObject["nodes"]!!.jsonArray) {
            for (service in node.jsonObject["services"]!!.jsonArray) {
                for (metric in service.jsonObject["metrics"]!!.jsonArray) {
                    val dims = metric.jsonObject["dimensions"]?.jsonObject ?: continue
                    if (dims["documenttype"]?.jsonPrimitive?.content != "reputation") continue
                    val proc = dims["clustername"]?.jsonPrimitive?.content ?: dims["serviceId"]?.jsonPrimitive?.content ?: "?"
                    for ((name, v) in metric.jsonObject["values"]!!.jsonObject) {
                        val key =
                            when (name) {
                                "content.proton.documentdb.documents.total.last" -> "documents"
                                "content.proton.documentdb.documents.active.last" -> "active"
                                "content.proton.documentdb.memory_usage.allocated_bytes.last" -> "memory"
                                "content.proton.documentdb.disk_usage.last" -> "disk"
                                "content.proton.transactionlog.disk_usage.last" -> "tlog"
                                else -> null
                            } ?: continue
                        out.getOrPut(proc) { linkedMapOf() }[key] = v.jsonPrimitive.longOrNull ?: v.jsonPrimitive.content
                            .toDouble()
                            .toLong()
                    }
                }
            }
        }
        return out
    }

    private fun latencies(p: Phase): Map<String, Any> =
        p.latencies.entries.sortedBy { it.key }.associate { (cls, q) ->
            val v = q.sorted()
            cls to mapOf("n" to v.size, "p50" to v[v.size / 2] / 1e6, "p95" to v[(v.size * 95) / 100] / 1e6)
        } + (if (p.errors.isEmpty()) emptyMap() else mapOf("errors" to p.errors.toMap()))

    /**
     * Every event-kind read the reference can answer, on both stores: the reference holds only the
     * slice's events and the background traffic (no lists, no cards), so the filters name those
     * kinds. Each is a NIP-01 answer, exact on both sides: the ids must match.
     */
    private suspend fun compare(
        store: VespaEventStore,
        reference: NostrSemanticsStore,
        authors: List<String>,
        rnd: Random,
    ): Map<String, Any> {
        val kinds = listOf(1, 6, 7, 0, 3, 5)
        val filters = ArrayList<Pair<String, Filter>>()
        kinds.forEach { k -> filters += "kind $k" to Filter(kinds = listOf(k)) }
        repeat(20) { filters += "author" to Filter(kinds = kinds, authors = listOf(authors[rnd.nextInt(authors.size)])) }
        repeat(10) { filters += "300 authors" to Filter(kinds = kinds, authors = List(300) { authors[rnd.nextInt(authors.size)] }) }
        filters += "#t nostr" to Filter(kinds = listOf(1), tags = mapOf("t" to listOf("nostr")))
        filters += "newest 500" to Filter(kinds = listOf(1), limit = 500)
        var mismatches = 0
        val samples = ArrayList<String>()
        for ((name, f) in filters) {
            val a = store.query<Event>(f).map { it.id }.toSet()
            val b = reference.query<Event>(f).map { it.id }.toSet()
            if (a != b) {
                mismatches++
                if (samples.size < 5) samples += "$name: store ${a.size}, reference ${b.size}, only in store ${(a - b).take(2)}, only in reference ${(b - a).take(2)}"
            }
        }
        return mapOf("filters" to filters.size, "mismatches" to mismatches, "samples" to samples)
    }

    /**
     * vespa-relay's statistics pipelines (`StatsYql`) on the split store, checked against each
     * other: a `distinct` count must equal the number of groups the matching `countsBy` returns.
     * Per-author fields live in one cluster each, so their merge is exact; a field whose values
     * appear in several clusters is where a merged `distinct` can over-count.
     */
    private suspend fun statsConsistency(store: VespaEventStore): Map<String, Any> {
        val out = LinkedHashMap<String, Any>()
        for (field in listOf("kind", "pubkey")) {
            val groups = countGroups(store.engine.group("all(group($field) each(output(count())))"))
            val distinct = topCount(store.engine.group("all(group($field) output(count()))"))
            out[field] = mapOf("groups" to groups, "distinct" to distinct, "agree" to (groups.toLong() == distinct))
        }
        return out
    }

    private fun countGroups(root: JsonObject): Int {
        var n = 0

        fun walk(e: JsonObject) {
            if ((e["id"]?.jsonPrimitive?.content ?: "").let { it.startsWith("group:") && !it.startsWith("group:root") }) n++
            e["children"]?.jsonArray?.forEach { walk(it.jsonObject) }
        }
        walk(root)
        return n
    }

    /** The `count()` a grouplist-level output carries: the number of groups at that level. */
    private fun topCount(root: JsonObject): Long? {
        fun find(e: JsonObject): Long? {
            e["fields"]
                ?.jsonObject
                ?.get("count()")
                ?.jsonPrimitive
                ?.longOrNull
                ?.let { return it }
            return e["children"]?.jsonArray?.firstNotNullOfOrNull { find(it.jsonObject) }
        }
        return find(root)
    }

    private fun secs(t0: Long): Double = Math.round((System.nanoTime() - t0) / 1e8) / 10.0

    private fun value(v: Any?): String =
        when (v) {
            null -> "null"
            is String -> "\"" + v.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
            is Number, is Boolean -> v.toString()
            is Map<*, *> -> v.entries.joinToString(",", "{", "}") { (k, x) -> "\"$k\":${value(x)}" }
            is List<*> -> v.joinToString(",", "[", "]") { value(it) }
            else -> value(v.toString())
        }
}
