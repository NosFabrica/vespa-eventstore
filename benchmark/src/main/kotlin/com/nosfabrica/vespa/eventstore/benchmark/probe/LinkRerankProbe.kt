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
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.search.LinkRerank
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path

/**
 * WHAT `sort:links` DOES TO A REAL SEARCH PAGE, and what it costs — the
 * end-to-end half of the trusted-links study (benchmark/README.md, "Trusted
 * links per search hit"), run through the store's real read path.
 *
 * For each term a `capture_links.py` capture searched, the same NIP-50 read is
 * served with and without the token: the latency of each (median of reps, after
 * a warm-up), how much of the page changed, and the reranked page's head with
 * where each hit sat before and the trusted links that moved it. Strengths are
 * swept with `sort:links:<x>`, so nothing is redeployed between them.
 *
 * Read-only against whatever store holds the capture (`exportLoad` it first).
 * The lens is the capture's observer, resolved by the store from its stored
 * kind 10040 exactly as a live read would.
 *
 *     VESPA_URL=http://localhost:8080 ./gradlew :benchmark:linkRerankProbe \
 *       --args="/tmp/links.json [limit=20] [reps=7] [strengths=0.25,0.5,1.0]"
 */
object LinkRerankProbe {
    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            require(args.isNotEmpty()) { "usage: linkRerankProbe <capture.json> [limit] [reps] [strengths]" }
            val meta = Json.parseToJsonElement(Files.readString(Path.of(args[0] + ".meta.json"))).jsonObject
            val limit = args.getOrNull(1)?.toInt() ?: 20
            val reps = args.getOrNull(2)?.toInt() ?: 7
            val strengths = args.getOrNull(3)?.split(',')?.map { it.toDouble() } ?: listOf(0.25, 0.5, 1.0)
            val observer = meta["observer"]!!.jsonPrimitive.content
            val provider = meta["provider"]!!.jsonPrimitive.content
            val terms = meta["pages"]!!.jsonObject.keys.filter { it != "popular" }
            val rerank = LinkRerank()
            // The same lens the store resolves, for READING BACK the counts that moved
            // each hit — the rerank itself resolves its own off the stored 10040.
            val linkers = EventQuery(observer = observer, rankKey = provider, minRank = 2.0, ranking = EventYql.RANK_RECENCY_GATED)

            VespaEventStore.open(System.getenv("VESPA_URL") ?: "http://localhost:8080").use { vs ->
                suspend fun page(search: String) = vs.store.query<Event>(Filter(kinds = listOf(1), search = search, limit = limit))

                suspend fun timed(search: String): Pair<List<Event>, Double> {
                    page(search) // warm
                    var last = emptyList<Event>()
                    val ms =
                        (1..reps).map {
                            val t0 = System.nanoTime()
                            last = page(search)
                            (System.nanoTime() - t0) / 1e6
                        }
                    return last to ms.sorted()[ms.size / 2]
                }

                val header = "%-10s %9s %9s".format("term", "base", "wide") + strengths.joinToString("") { " %14s".format("links:$it") }
                println("page = top $limit, kind 1, observer ${observer.take(8)} (median of $reps; 'wide' = the base read at the rerank window, no rerank)")
                println("each links column: latency | hits changed vs base")
                println(header)
                val pages = LinkedHashMap<String, Pair<List<Event>, List<Event>>>()
                val deltas = strengths.associateWith { ArrayList<Double>() }
                for (term in terms) {
                    val q = "$term observer:$observer"
                    val (base, baseMs) = timed(q)
                    val wideMs = timedWide(vs, q, rerank.window, reps)
                    val baseIds = base.mapTo(HashSet(), Event::id)
                    val cols =
                        strengths.map { s ->
                            val (linked, ms) = timed("$q sort:links:$s")
                            deltas[s]!!.add(ms - baseMs)
                            if (s == rerank.strength) pages[term] = base to linked
                            " %7.1fms |%3d".format(ms, linked.count { it.id !in baseIds })
                        }
                    println("%-10s %7.1fms %7.1fms".format(term, baseMs, wideMs) + cols.joinToString(""))
                }
                println("median added latency: " + strengths.joinToString("  ") { s -> "links:$s +%.1f ms".format(deltas[s]!!.sorted()[deltas[s]!!.size / 2]) })

                for ((term, pair) in pages) {
                    val (base, linked) = pair
                    val wide = vs.store.query<Event>(Filter(kinds = listOf(1), search = "$term observer:$observer", limit = rerank.window)).map(Event::id)
                    val counts = vs.engine.countLinks(linked.map { "e:${it.id}" }, linkers)
                    println("\n'$term' with sort:links (strength ${rerank.strength}): new pos <- engine pos, trusted links by kind")
                    linked.take(10).forEachIndexed { i, e ->
                        val was = wide.indexOf(e.id).let { if (it < 0) "?" else (it + 1).toString() }
                        val byKind = counts["e:${e.id}"].orEmpty()
                        val mix = byKind.entries.sortedByDescending { it.value }.joinToString(" ") { "${it.key}:${it.value}" }
                        val text = e.content.replace(Regex("\\s+"), " ").take(60)
                        println("%3d <- %-4s %5d  %-26s %s".format(i + 1, was, byKind.values.sum(), mix.take(26), text))
                    }
                }
            }
        }

    /** The base read widened to the rerank window, timed — what widening alone costs. */
    private suspend fun timedWide(
        vs: VespaEventStore,
        q: String,
        window: Int,
        reps: Int,
    ): Double {
        val ms =
            (1..reps).map {
                val t0 = System.nanoTime()
                vs.store.query<Event>(Filter(kinds = listOf(1), search = q, limit = window))
                (System.nanoTime() - t0) / 1e6
            }
        return ms.sorted()[ms.size / 2]
    }
}
