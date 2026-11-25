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

import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.eventstore.engine.client.BucketMove
import com.nosfabrica.vespa.eventstore.engine.client.ClientTuning
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.security.MessageDigest
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A BUCKET MOVE UNDER LOAD (`BucketMove`, `docs/author-shards.md` "Moving buckets"): two author
 * clusters become three, cluster 1 handing buckets c0–ff to the new cluster 2, while a writer keeps
 * adding events, deleting some, and changing reputation cells, including in the moving buckets.
 * An in-test oracle holds what must be served. At every phase reads must equal it exactly:
 * nothing missing, nothing twice.
 *
 *  - overlap: stores switch to the overlap map, then it deploys (the guard allows it: it only
 *    widens); the new cluster fills by copy + repair, and no read sees it;
 *  - cut-over: stores switch to the final map, the store's guard refuses the narrowing deploy,
 *    the move's verified deploy goes through, and cleanup empties the moved buckets' old owner;
 *  - after: every event and reputation document sits only in its new owner, every reputation
 *    document carries the cells written during the move, and the gate reads trust in the new
 *    cluster.
 */
@Tag("integration")
class AuthorShardsMoveIT {
    @Test
    fun `two clusters become three while writes continue, and reads stay exact`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the bucket move IT")
        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val url = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val deployer = SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}")
                runBlocking { move(url, deployer) }
            }
    }

    private suspend fun move(
        url: String,
        deployer: SchemaDeployer,
    ) {
        val from = AuthorShards.BucketMap.even(2)
        val to = AuthorShards.BucketMap.parse("00-7f=0,80-bf=1,c0-ff=2")
        deployer.deployIfAbsent(url, VespaApp.zipBytes(null, from))

        // ---- the corpus, under the two-cluster map ----
        val oracle = ConcurrentHashMap<String, EventDoc>()
        val cells = ConcurrentHashMap<String, Int>()
        VespaEventIndex(url, tuning = ClientTuning(authorBuckets = from.spec())).use { index ->
            VespaReputationIndex(url, tuning = ClientTuning(authorBuckets = from.spec())).use { reputation ->
                val notes = (0 until 1_200).map { note(it, AUTHORS[it % AUTHORS.size]) }
                index.putAll(notes)
                notes.forEach { oracle[it.id] = it }
                AUTHORS.forEachIndexed { i, a -> cells[a] = i % 100 }
                reputation.putAll(AUTHORS.map { a -> ReputationDoc(a, influenceScores = serviceCells(LENS to cells.getValue(a))) })
            }
        }
        val moving = AUTHORS.filter { to.shardOf(it) == 2 }
        assertTrue(moving.size > 20, "the fixture has authors in the moving buckets: ${moving.size}")

        BucketMove(url, from, to).use { mv ->
            // ---- overlap: stores first, then the package ----
            val overlapTuning = ClientTuning(authorBuckets = mv.overlap.spec())
            VespaEventIndex(url, tuning = overlapTuning).use { reader ->
                VespaReputationIndex(url, tuning = overlapTuning).use { writerRep ->
                    VespaEventIndex(url, tuning = overlapTuning).use { writer ->
                        assertTrue(deployer.deployIfChanged(url, VespaApp.zipBytes(null, mv.overlap)), "the overlap only widens: the guard lets it through")
                        assertEquals(AuthorShards.clusters(3), deployer.activeContentClusters())

                        // The event the test removes from the destination below: the writer leaves it alone, so the
                        // diff must find it missing whatever else the writer does.
                        val damaged = oracle.values.first { it.pubkey in moving }.id
                        val seq = AtomicInteger(10_000)
                        val writes = AtomicInteger()
                        // Stopped by this flag, never by cancel(): a cancellation can land after a write
                        // reached Vespa and before the oracle line under it, leaving the two apart.
                        val writing =
                            java.util.concurrent.atomic
                                .AtomicBoolean(true)
                        val load =
                            kotlinx.coroutines.CoroutineScope(Dispatchers.IO).launch {
                                val rnd = Random(9)
                                while (writing.get()) {
                                    val author = if (rnd.nextBoolean()) moving[rnd.nextInt(moving.size)] else AUTHORS[rnd.nextInt(AUTHORS.size)]
                                    when (rnd.nextInt(10)) {
                                        in 0..5 -> {
                                            note(seq.incrementAndGet(), author).also {
                                                inFlight.add(it.id)
                                                writer.put(it)
                                                oracle[it.id] = it
                                                inFlight.remove(it.id)
                                            }
                                        }

                                        in 6..7 -> {
                                            oracle.keys.filter { it != damaged }.randomOrNull(rnd)?.let { id ->
                                                inFlight.add(id)
                                                writer.remove(id)
                                                oracle.remove(id)
                                                inFlight.remove(id)
                                            }
                                        }

                                        else -> {
                                            val v = rnd.nextInt(101)
                                            writerRep.updateCells(listOf(ReputationCells(author, ServiceKey(LENS), influence = v, followers = null)))
                                            cells[author] = v
                                        }
                                    }
                                    writes.incrementAndGet()
                                    delay(5)
                                }
                            }

                        assertExact(reader, oracle, "overlap, before the copy: the pending cluster is read by no one")
                        val copied = mv.copy()
                        println("move: copied $copied, ${writes.get()} writes so far")
                        // The copy runs inside Vespa and OVERWRITES, so a racing write can leave the
                        // destination short, stale or holding a deleted document. The copy is too
                        // quick here for the writer to race it reliably, so the test does all three
                        // to the destination itself, and the first diff must find and repair each.
                        val destination = AuthorShards.clusterName(2)
                        val extraId = "f".repeat(63) + "0"
                        val stale = moving.first()
                        damage(url, destination, damaged, extraId, stale)
                        // The pending cluster now holds an event no one wrote: a read on the
                        // overlap map must not serve it, whether it goes as YQL (`from sources`) or
                        // as `select` (`model.sources`), which text-free reads like these do.
                        assertExact(reader, oracle, "overlap, the pending cluster holding a planted extra: read by no one")
                        var diff = mv.diff(repair = true)
                        println("move: first diff, after damaging the destination: $diff")
                        assertTrue(diff.missing >= 1 && diff.differing >= 1 && diff.extra >= 1, "the diff finds what the copy can leave wrong: $diff")
                        var passes = 1
                        while (!diff.clean && passes < 6) {
                            println("move: repair pass $passes fixed $diff")
                            diff = mv.diff(repair = true)
                            passes++
                        }
                        assertExact(reader, oracle, "overlap, after the copy: still no document twice")
                        assertTrue(docIn(url, destination, "event", damaged) != null, "the removed event was repaired into the destination")
                        assertEquals(null, docIn(url, destination, "event", extraId), "the document the source lacks was removed")
                        assertTrue(BOGUS_SERVICE !in docIn(url, destination, "reputation", stale).toString(), "the stale reputation document was rewritten from the source")

                        // ---- cut-over: stores first, then finish(): empty the sources, narrow, empty again ----
                        VespaEventIndex(url, tuning = ClientTuning(authorBuckets = to.spec())).use { after ->
                            assertExact(after, oracle, "stores on the final map before it deploys: duplicates dropped by id")
                            val refused = assertFailsWith<IllegalStateException> { deployer.deployIfChanged(url, VespaApp.zipBytes(null, to)) }
                            assertTrue("'content1' would drop 64 bucket(s)" in refused.message.orEmpty(), refused.message)
                            assertTrue(mv.diff(repair = true).let { it.clean || mv.diff(repair = true).clean }, "verified before narrowing")
                            val zip = VespaApp.zipBytes(null, to)
                            val removed =
                                mv.finish {
                                    // The sources are emptied while the overlap package still sends
                                    // them every write: whatever a read meets there is current, so
                                    // reads stay exact (no deleted event served again) up to the deploy.
                                    assertExact(after, oracle, "sources emptied, still receiving writes, before the narrowing deploy")
                                    assertTrue(runCatching { mv.diff(repair = true) }.exceptionOrNull() is IllegalStateException, "a diff now would take the moved documents for extras")
                                    deployer.checkShardChange(url, zip, allowNarrowing = true)
                                    deployer.awaitConverged(SchemaDeployer.generationOf(deployer.deploy(zip)))
                                }
                            println("move: finish removed $removed from the old owner")

                            writing.set(false)
                            load.join()
                            println("move: ${writes.get()} writes ran during the move")
                            awaitSettled(after, oracle)
                            assertEquals("events=0 reputations=0", mv.remaining().toString(), "the old owner keeps nothing of the moved buckets")
                            assertExact(after, oracle, "after the move")

                            // ---- placement and trust, under the final map ----
                            for (c in 0 until 3) {
                                val held = visitPubkeys(url, "event", AuthorShards.clusterName(c), "event:pubkey", "pubkey")
                                assertEquals(setOf(c), held.map { to.shardOf(it) }.toSet(), "events in ${AuthorShards.clusterName(c)}")
                                val reps = visitPubkeys(url, "reputation", AuthorShards.clusterName(c), "[id]", null)
                                assertEquals(setOf(c), reps.map { to.shardOf(it) }.toSet(), "reputation documents in ${AuthorShards.clusterName(c)}")
                            }
                            VespaReputationIndex(url, tuning = ClientTuning(authorBuckets = to.spec())).use { rep ->
                                for (a in moving) {
                                    assertEquals(cells.getValue(a), rep.get(a)?.influenceScores?.get(ServiceKey(LENS)), "$a's cell, written during the move")
                                }
                            }
                            val trusted = oracle.values.filter { (cells[it.pubkey] ?: 0) >= 50 }.sortedWith(compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id })
                            assertEquals(
                                trusted.map { it.id },
                                after.search(EventQuery(kinds = listOf(1), ranking = EventYql.RANK_RECENCY_GATED, minRank = 50.0, observer = LENS, rankKey = LENS, followersKey = LENS)).map { it.id },
                                "the gate reads the moved authors' trust in their new cluster",
                            )
                        }
                    }
                }
            }
        }
    }

    /**
     * What a write racing an overwriting copy can leave in [cluster], done on purpose with
     * `route=<cluster>` (document-API puts are POSTs; a PUT is an update): [missing] removed,
     * [extraId] put (an event by [missing]'s author under an id the source never held, as a delete
     * the copy undid would leave), and [stale]'s reputation document replaced by one holding a
     * cell the source never had.
     */
    private fun damage(
        url: String,
        cluster: String,
        missing: String,
        extraId: String,
        stale: String,
    ) {
        // A minimal event by the same author: events are compared by id, and the author places it.
        val pubkey = docIn(url, cluster, "event", missing)!!["fields"]!!.jsonObject["pubkey"]!!.jsonPrimitive.content
        send("POST", "$url/document/v1/event/event/docid/$extraId?route=$cluster", "{\"fields\":{\"id\":\"$extraId\",\"pubkey\":\"$pubkey\",\"kind\":1,\"created_at\":1700000000}}")
        send("DELETE", "$url/document/v1/event/event/docid/$missing?route=$cluster", null)
        send(
            "POST",
            "$url/document/v1/reputation/reputation/docid/$stale?route=$cluster",
            "{\"fields\":{\"pubkey\":\"$stale\",\"influence_scores\":{\"cells\":[{\"address\":{\"user\":\"$BOGUS_SERVICE\"},\"value\":7}]}}}",
        )
    }

    /** [id] of [type] as [cluster] holds it now, or null. */
    private fun docIn(
        url: String,
        cluster: String,
        type: String,
        id: String,
    ): kotlinx.serialization.json.JsonObject? {
        val r = HTTP.send(HttpRequest.newBuilder(URI.create("$url/document/v1/$type/$type/docid/$id?cluster=$cluster")).GET().build(), HttpResponse.BodyHandlers.ofString())
        if (r.statusCode() == 404) return null
        check(r.statusCode() == 200) { "get $id in $cluster: ${r.statusCode()} ${r.body().take(300)}" }
        return Json.parseToJsonElement(r.body()).jsonObject.takeIf { "fields" in it }
    }

    private fun send(
        method: String,
        url: String,
        body: String?,
    ) {
        val req =
            HttpRequest
                .newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .method(method, body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody())
                .build()
        val r = HTTP.send(req, HttpResponse.BodyHandlers.ofString())
        check(r.statusCode() == 200) { "$method $url: ${r.statusCode()} ${r.body().take(300)}" }
    }

    /** Reads through [index] equal [oracle] exactly: every id once, the count the same, an author-scoped read too. */
    private suspend fun assertExact(
        index: VespaEventIndex,
        oracle: Map<String, EventDoc>,
        phase: String,
    ) {
        // The oracle moves under the writer: read it, then the index, then it again, and judge the
        // index against what both oracle reads agree on (an id present in one only was written mid-read).
        // A write IN FLIGHT is judged by neither: Vespa can serve a put before the writer gets
        // its ack and records it, which read as a document "the oracle never held" (it flaked so,
        // once in a full-suite run, on an event written long after the copy and the diff). Each
        // in-flight snapshot is taken before its oracle snapshot: a write leaves the in-flight set
        // only after the oracle has it, so a write the read saw is in one of the two.
        val flyingBefore = inFlight.toSet()
        val before = oracle.keys.toSet()
        val served = allIds(index)
        val flyingAfter = inFlight.toSet()
        val after = oracle.keys.toSet()
        assertEquals(served.size, served.toSet().size, "$phase: a document served twice")
        val unsettled = flyingBefore union flyingAfter
        val stable = (before intersect after) - unsettled
        val missing = stable - served.toSet()
        val ghost = served.toSet() - (before union after) - unsettled
        assertTrue(missing.isEmpty(), "$phase: ${missing.size} live documents not served, e.g. ${missing.take(3)}")
        assertTrue(ghost.isEmpty(), "$phase: ${ghost.size} documents served that the oracle never held, e.g. ${ghost.take(3)}")
    }

    /**
     * Every event id served. Retried on a refused partial answer: a document deleted between Vespa
     * matching it and fetching its summary comes back as an "empty document summary" error, which
     * the store refuses (as it does on one cluster) and a client retries; the move has no part in it.
     */
    private suspend fun allIds(index: VespaEventIndex): List<String> {
        repeat(5) { attempt ->
            try {
                return index.search(EventQuery(kinds = listOf(1))).map { it.id }
            } catch (e: com.nosfabrica.vespa.eventstore.engine.client.PartialAnswer) {
                if (attempt == 4) throw e
                delay(200)
            }
        }
        error("unreachable")
    }

    private suspend fun awaitSettled(
        index: VespaEventIndex,
        oracle: Map<String, EventDoc>,
    ) {
        repeat(60) {
            if (allIds(index).toSet() == oracle.keys) return
            delay(500)
        }
    }

    private fun note(
        n: Int,
        author: String,
    ) = EventDoc(
        id = hex("move-event-$n"),
        pubkey = author,
        createdAt = 1_700_000_000L + n,
        kind = 1,
        tags = emptyList(),
        content = "note $n",
        sig = "e".repeat(128),
        search = SearchFields(text = "note $n"),
    )

    /** One cluster's documents, off the document API: [field] of each, or the id's specific part. */
    private fun visitPubkeys(
        url: String,
        type: String,
        cluster: String,
        fieldSet: String,
        field: String?,
    ): List<String> {
        val out = ArrayList<String>()
        var continuation: String? = null
        do {
            val u =
                "$url/document/v1/$type/$type/docid?cluster=$cluster&wantedDocumentCount=1024" +
                    "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}" + (continuation?.let { "&continuation=$it" } ?: "")
            val body = HTTP.send(HttpRequest.newBuilder(URI.create(u)).GET().build(), HttpResponse.BodyHandlers.ofString())
            check(body.statusCode() < 400) { "visit $cluster: ${body.statusCode()} ${body.body().take(300)}" }
            val env = Json.parseToJsonElement(body.body()).jsonObject
            env["documents"]?.jsonArray.orEmpty().forEach { doc ->
                val o = doc.jsonObject
                out += if (field == null) o["id"]!!.jsonPrimitive.content.substringAfter("::") else o["fields"]!!.jsonObject[field]!!.jsonPrimitive.content
            }
            continuation = env["continuation"]?.jsonPrimitive?.content
        } while (continuation != null)
        return out
    }

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        val HTTP: HttpClient = HttpClient.newHttpClient()

        /** Ids the test's writer is putting or removing right now, between its call and its oracle update ([assertExact]). */
        val inFlight: MutableSet<String> = ConcurrentHashMap.newKeySet()

        /** A service key no fixture or writer ever scores with. */
        val BOGUS_SERVICE = "ab".repeat(32)
        val LENS = "c".repeat(64)

        fun hex(seed: String): String = MessageDigest.getInstance("SHA-256").digest(seed.encodeToByteArray()).joinToString("") { "%02x".format(it) }

        /** Hash keys: their middle bytes are uniform, so every bucket run has authors. */
        val AUTHORS = List(300) { hex("move-author-$it") }

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
