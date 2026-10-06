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
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File

/**
 * A QUARTZ PIN BUMP, REPLAYED ON REAL DATA — the check that the bump's
 * `reindexFullTextSearch(kinds)` list is complete, which nothing else makes.
 *
 * The scoped repair is only as complete as its list, and the list is read off
 * upstream's diff: a kind it misses keeps its old search columns forever, with
 * no error anywhere — the store serves stale recall and every test stays green,
 * because tests derive and compare under ONE pin. Drift only exists BETWEEN two
 * pins, against documents fed under the first, so that is what this measures:
 * load a captured corpus at the old pin, switch the pin, and ask every stored
 * document whether the new pin would derive the same columns.
 *
 *     snapshot <out.jsonl> [--require-clean]
 *         walk every stored doc: its kind, the search columns the engine HOLDS,
 *         and the columns THIS build's SearchExtractors derive; print the stale
 *         count per kind. Under the new pin before the repair, a stale kind
 *         outside the list is the bug this exists to catch; after it,
 *         `--require-clean` fails the run on any stale doc.
 *     reindex <kinds.json>
 *         the scoped repair itself, through the store, timed.
 *     search <probes.jsonl> <out.jsonl>
 *         each probe's {kinds, search, limit} as a NIP-50 REQ through the whole
 *         store (`open()`, so the trust lens is live), ids appended.
 *
 * The procedure, the capture and the probe/grade halves are in
 * `benchmark/pin_replay.py`; the 68268da413 run is in benchmark/README.md,
 * "Replaying a pin bump". The engine walk is the reindex's own (visitDocsPage)
 * and the comparison is the reindex's own (`derived != doc.search`), so the
 * snapshot's stale count is exactly the set the repair will rewrite.
 */
object PinReplayProbe {
    private fun SearchFields.json() = JsonObject(fields().mapValues { JsonPrimitive(it.value) })

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            when (args[0]) {
                "snapshot" -> {
                    VespaEventIndex(url).use { idx ->
                        File(args[1]).bufferedWriter().use { out ->
                            var cursor: String? = null
                            var n = 0
                            val stale = HashMap<Int, Int>()
                            val total = HashMap<Int, Int>()
                            do {
                                val page = idx.visitDocsPage(EventQuery(), cursor, 1000)
                                for (doc in page.docs) {
                                    val derived = SearchExtractors.extract(EventFactory.create<Event>(doc.id, doc.pubkey, doc.createdAt, doc.kind, Array(doc.tags.size) { doc.tags[it].toTypedArray() }, doc.content, doc.sig))
                                    total.merge(doc.kind, 1, Int::plus)
                                    if (derived != doc.search) stale.merge(doc.kind, 1, Int::plus)
                                    val row =
                                        JsonObject(
                                            mapOf(
                                                "id" to JsonPrimitive(doc.id),
                                                "kind" to JsonPrimitive(doc.kind),
                                                "stored" to doc.search.json(),
                                                "derived" to derived.json(),
                                            ),
                                        )
                                    out.write(row.toString())
                                    out.newLine()
                                    n++
                                }
                                cursor = page.continuation
                            } while (cursor != null)
                            println("snapshot: $n docs, ${stale.values.sum()} stale under this pin")
                            stale.toSortedMap().forEach { (k, v) -> println("  kind $k: $v of ${total[k]} stale") }
                            check("--require-clean" !in args || stale.isEmpty()) { "${stale.values.sum()} docs stale under this pin" }
                        }
                    }
                }

                "reindex" -> {
                    val kinds = Json.parseToJsonElement(File(args[1]).readText()).jsonArray.map { it.jsonPrimitive.int }
                    VespaEventStore.open(url).use { store ->
                        val t0 = System.nanoTime()
                        store.store.reindexFullTextSearch(kinds)
                        println("reindexFullTextSearch(${kinds.size} kinds): ${(System.nanoTime() - t0) / 1_000_000} ms")
                    }
                }

                "search" -> {
                    VespaEventStore.open(url).use { store ->
                        File(args[2]).bufferedWriter().use { out ->
                            File(args[1]).readLines().filter { it.isNotBlank() }.forEach { line ->
                                val q = Json.parseToJsonElement(line).jsonObject
                                val kinds = q["kinds"]?.jsonArray?.map { it.jsonPrimitive.int }
                                val filter = Filter(kinds = kinds, search = q["search"]!!.jsonPrimitive.content, limit = q["limit"]?.jsonPrimitive?.int ?: 500)
                                val ids = store.query<Event>(listOf(filter)).map { JsonPrimitive(it.id) }
                                out.write(JsonObject(q + ("ids" to JsonArray(ids))).toString())
                                out.newLine()
                            }
                        }
                    }
                }
            }
        }
}
