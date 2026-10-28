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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import java.io.File

/**
 * THE DENSE-SECOND SNAPSHOT, against a captured provider run
 * (NosFabrica/vespa-eventstore#185).
 *
 * NIP-85 providers sign a whole run on one `created_at` second, and a NIP-77
 * snapshot over such a second used to send the id walk to a document-API scan
 * of the whole corpus. This runs `snapshotIdsForNegentropy` — the call a relay
 * makes for NEG-OPEN — over the three windows the issue named, against a store
 * the capture was fed into with `exportLoad`:
 *
 *  - the dense second, author-bound (the cursor's failing case);
 *  - the same second as a by-kind mirror window asks it (8 assertion kinds, no author);
 *  - the provider, all time (a retraction audit's ask);
 *  - the provider with `limit: 100000`, the limit a relay stamps on NIP-77 and
 *    COUNT filters — a limited walk must keep NIP-01's newest N
 *    (`created_at desc, id asc`), which inside a dense second is its LOWEST ids.
 *
 * For each it prints the wall time, whether the id set equals the capture's
 * (the oracle — the capture is itself a NIP-77 pull of the relay), and the
 * `walk.*` stages: `walk.ties.dense` with `walk.ties.prefix` and no
 * `walk.scan` is the fix working.
 *
 * The capture is the file `exportLoad` was fed (a JSON array) or JSON lines,
 * one event each. The densest second and its most frequent author are found in
 * it, so any provider's run works.
 *
 * THE ORACLE IS THE CAPTURE AS FED, so it holds only for a capture the store
 * accepted WHOLE — `exportLoad` prints "N read, N accepted". It does not
 * replay what the store enforces on the way in or out (a superseded card
 * version, a NIP-09 deletion, a NIP-40 expiry); a capture carrying any of
 * those reads as MISMATCH here while the store is right. A NIP-77 pull of one
 * provider's current cards carries none of them.
 *
 *     ./gradlew :benchmark:exportLoad --args="/path/run.json"
 *     ./gradlew :benchmark:denseSecondProbe --args="/path/run.json"
 */
object DenseSecondProbe {
    private val ASSERTION_KINDS = listOf(30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395)

    private class Card(
        val id: String,
        val pubkey: String,
        val kind: Int,
        val createdAt: Long,
    )

    private fun load(path: String): List<Card> {
        val text = File(path).readText()
        val objects =
            if (text.trimStart().startsWith("[")) {
                Json.parseToJsonElement(text).jsonArray.map { it.jsonObject }
            } else {
                text
                    .lineSequence()
                    .filter { it.isNotBlank() }
                    .map { Json.parseToJsonElement(it).jsonObject }
                    .toList()
            }
        return objects.map { it.card() }
    }

    private fun JsonObject.card() =
        Card(
            id = getValue("id").jsonPrimitive.content,
            pubkey = getValue("pubkey").jsonPrimitive.content,
            kind = getValue("kind").jsonPrimitive.int,
            createdAt = getValue("created_at").jsonPrimitive.long,
        )

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            require(args.size == 1) { "usage: denseSecondProbe <capture.json|capture.jsonl>" }
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val cards = load(args[0])
            require(cards.isNotEmpty()) { "${args[0]} holds no events" }
            val (second, tied) = cards.groupingBy { it.createdAt }.eachCount().maxBy { it.value }
            val author =
                cards
                    .filter { it.createdAt == second }
                    .groupingBy { it.pubkey }
                    .eachCount()
                    .maxBy { it.value }
                    .key
            println("denseSecondProbe: ${cards.size} cards in the capture; densest second $second holds $tied (author ${author.take(12)}...) against $url")

            val windows =
                listOf(
                    "the second, author-bound" to Filter(kinds = listOf(30382), authors = listOf(author), since = second, until = second),
                    "the second, 8 assertion kinds, no author" to Filter(kinds = ASSERTION_KINDS, since = second, until = second),
                    "the provider, all time" to Filter(kinds = listOf(30382), authors = listOf(author)),
                    "the provider, limit 100000" to Filter(kinds = listOf(30382), authors = listOf(author), limit = 100_000),
                )
            VespaEventStore.open(url).use { store ->
                for ((name, filter) in windows) {
                    val expected =
                        cards
                            .filter { c ->
                                c.kind in filter.kinds!! &&
                                    (filter.authors == null || c.pubkey in filter.authors!!) &&
                                    (filter.since == null || c.createdAt >= filter.since!!) &&
                                    (filter.until == null || c.createdAt <= filter.until!!)
                            }.sortedWith(compareByDescending<Card> { it.createdAt }.thenBy { it.id })
                            .let { matched -> filter.limit?.let { matched.take(it) } ?: matched }
                            .mapTo(HashSet()) { it.id }
                    IngestStats.reset()
                    val t0 = System.nanoTime()
                    val got = store.snapshotIdsForNegentropy(listOf(filter), null, null)
                    val ms = (System.nanoTime() - t0) / 1_000_000
                    val ids = got.mapTo(HashSet()) { it.id }
                    val verdict =
                        when {
                            ids.size != got.size -> "DUPLICATES (${got.size - ids.size})"
                            ids == expected -> "EXACT"
                            else -> "MISMATCH (missing ${(expected - ids).size}, extra ${(ids - expected).size})"
                        }
                    println("\n$name: ${got.size} ids of ${expected.size} expected in $ms ms: $verdict")
                    IngestStats
                        .snapshot()
                        .filterKeys { it.startsWith("walk.") }
                        .toSortedMap()
                        .forEach { (stage, s) -> println("  %-26s calls=%-5d mean=%6d ms  max=%6d ms".format(stage, s.calls, s.meanNanos / 1_000_000, s.maxNanos / 1_000_000)) }
                }
            }
        }
}
