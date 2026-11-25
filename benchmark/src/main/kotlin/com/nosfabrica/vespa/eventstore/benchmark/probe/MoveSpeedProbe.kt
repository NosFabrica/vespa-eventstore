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

import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.nosfabrica.vespa.eventstore.engine.client.BucketMove
import com.nosfabrica.vespa.eventstore.engine.client.ClientTuning
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.security.MessageDigest
import kotlin.random.Random

/**
 * HOW FAST A BUCKET MOVE IS, with nothing else running: the steps of `BucketMove` timed on a fixed
 * corpus, so a change to the tool is judged against the same numbers. One cluster split into two
 * (`00-7f=0,80-ff=1`), half the corpus moving.
 *
 * Against a FRESH Vespa: loads [events] notes by [AUTHORS] hash-keyed authors and their reputation
 * documents (each subject scored by ~60% of 40 services) at the engine level, deploys the overlap,
 * then times `copy`, `diff` (one pass, which finds nothing to repair with no writes running),
 * the final deploy and `cleanup`. Two references beside them: `load`, the host's write rate through
 * the store's feed path (the ceiling a copy's puts meet), and `scan`, the moving buckets visited
 * with no writes (the read side alone). Rates are the moving documents (`scan`'s count) per second,
 * whatever each step reports touching: garbage collection may remove some before `cleanup` does.
 *
 * Prints `RESULT {json}`. Env: BENCH_VESPA_URL, BENCH_CONFIG_URL, BENCH_EVENTS (400000), BENCH_LABEL.
 */
object MoveSpeedProbe {
    private const val AUTHORS = 40_000

    private fun hex(seed: String): String = MessageDigest.getInstance("SHA-256").digest(seed.encodeToByteArray()).joinToString("") { "%02x".format(it) }

    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking {
            val url = System.getenv("BENCH_VESPA_URL") ?: "http://127.0.0.1:8080"
            val configUrl = System.getenv("BENCH_CONFIG_URL") ?: "http://127.0.0.1:19071"
            val events = System.getenv("BENCH_EVENTS")?.toInt() ?: 400_000
            val label = System.getenv("BENCH_LABEL") ?: "?"
            val deployer = SchemaDeployer(configUrl)
            check(!deployer.isServing(url)) { "run against a fresh Vespa" }
            val from = AuthorShards.BucketMap.even(1)
            val to = AuthorShards.BucketMap.parse("00-7f=0,80-ff=1")
            deployer.deployIfAbsent(url, VespaApp.zipBytes(null, from))

            val authors = List(AUTHORS) { hex("author-$it") }
            val loadStart = System.nanoTime()
            val services = List(40) { hex("service-$it") }
            VespaEventIndex(url, tuning = ClientTuning()).use { index ->
                val rnd = Random(42)
                (0 until events).chunked(2_000).forEach { chunk ->
                    index.putAll(
                        chunk.map { n ->
                            val text = "note $n " + List(8) { WORDS[rnd.nextInt(WORDS.size)] }.joinToString(" ")
                            EventDoc(hex("event-$n"), authors[rnd.nextInt(AUTHORS)], 1_700_000_000L + n, 1, listOf(listOf("t", WORDS[rnd.nextInt(WORDS.size)])), text, "e".repeat(128), search = SearchFields(text = text))
                        },
                    )
                }
                VespaReputationIndex(url).use { rep ->
                    authors.chunked(2_000).forEach { chunk ->
                        rep.putAll(
                            chunk.map { a ->
                                val r = Random(a.hashCode())
                                val mine = services.filter { r.nextInt(10) < 6 }
                                ReputationDoc(a, influenceScores = mine.associate { ServiceKey(it) to r.nextInt(101) }, followerCounts = mine.associate { ServiceKey(it) to r.nextInt(1, 5_000_000).toDouble() })
                            },
                        )
                    }
                }
                repeat(240) {
                    if (index.count(EventQuery()) >= events) return@repeat
                    delay(1_000)
                }
            }
            // What this host writes through the store's own feed path: the ceiling a copy's puts meet.
            val loadSecs = secs(loadStart)

            val steps = LinkedHashMap<String, String>()
            BucketMove(url, from, to).use { mv ->
                check(deployer.deployIfChanged(url, VespaApp.zipBytes(null, mv.overlap)))
                steps["load"] = "{\"seconds\":$loadSecs,\"docsPerSecond\":${((events + AUTHORS) / loadSecs).toLong()}}"
                // The moving buckets visited with no writes ([id] only): the read side's own speed.
                var t = System.nanoTime()
                val scanned = mv.remaining()
                val scanSecs = secs(t)
                steps["scan"] = "{\"seconds\":$scanSecs,\"docsPerSecond\":${((scanned.events + scanned.reputations) / scanSecs).toLong()}}"
                t = System.nanoTime()
                val copied = mv.copy()
                val copySecs = secs(t)
                steps["copy"] = "{\"seconds\":$copySecs,\"events\":${copied.events},\"reputations\":${copied.reputations},\"docsPerSecond\":${((scanned.events + scanned.reputations) / copySecs).toLong()}}"
                t = System.nanoTime()
                val diff = mv.diff(repair = true)
                steps["diff"] = "{\"seconds\":${secs(t)},\"result\":\"$diff\"}"
                val zip = VespaApp.zipBytes(null, to)
                t = System.nanoTime()
                var deployNanos = 0L
                val cleaned =
                    mv.finish {
                        val d = System.nanoTime()
                        deployer.checkShardChange(url, zip, allowNarrowing = true)
                        deployer.awaitConverged(SchemaDeployer.generationOf(deployer.deploy(zip)))
                        deployNanos = System.nanoTime() - d
                    }
                // finish() is two removals around the final deploy: the removals alone are timed.
                val cleanSecs = Math.round((System.nanoTime() - t - deployNanos) / 1e8) / 10.0
                steps["cleanup"] = "{\"seconds\":$cleanSecs,\"events\":${cleaned.events},\"reputations\":${cleaned.reputations},\"docsPerSecond\":${((scanned.events + scanned.reputations) / cleanSecs).toLong()}}"
                steps["remaining"] = "\"${mv.remaining()}\""
            }
            println("RESULT {\"label\":\"$label\",\"events\":$events,${steps.entries.joinToString(",") { "\"${it.key}\":${it.value}" }}}")
        }

    private val WORDS = "bitcoin nostr lightning pizza coffee music art zap relay garden ocean bike chess climate photo film code linux rust kotlin".split(" ")

    private fun secs(t0: Long): Double = Math.round((System.nanoTime() - t0) / 1e8) / 10.0
}
