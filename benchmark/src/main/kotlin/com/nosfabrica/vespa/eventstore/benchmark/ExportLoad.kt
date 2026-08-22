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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import java.nio.file.Files
import java.nio.file.Path

/**
 * Feed a CAPTURED corpus — a plain JSON array of events, which is all a capture
 * from a live relay needs to be — into a live store through the real write
 * path, then settle the trust projection.
 *
 * This is the missing setup half of [RankAb]: that harness A/Bs ranking knobs
 * against whatever a cluster already holds, and until now nothing in the repo
 * could put a real, messy, TIME-SPREAD corpus there. The generated
 * [NostrCorpus] cannot stand in for one — a ranking question about recency or
 * trust is a question about real timestamps and a real web of trust.
 *
 * Feed the NIP-85 events (the observer's kind 10040 and the provider's 30382s)
 * in the same run — they are events like any other, so the store's own
 * TrustProjection rebuilds the reputation tensors from them and the local
 * cluster ranks the way the source cluster does.
 *
 * Captures are made READ-ONLY, out of band (a NIP-01 REQ against a public
 * relay); nothing here talks to a relay, and no test may point at one. See
 * benchmark/README.md.
 *
 * Each file is parsed whole (a capture is a JSON array, so there is no
 * streaming entry point) — BENCH_HEAP raises the JVM's ceiling for a big one.
 * REJECTIONS ARE COUNTED AND PRINTED, not swallowed: a capture that lands
 * half-rejected (duplicates, supersessions, an expired NIP-40 window) would
 * otherwise leave a sweep measuring a corpus nobody meant to build.
 *
 *     VESPA_URL=http://localhost:8080 ./gradlew :benchmark:exportLoad \
 *       --args="/path/staging_events.json /path/staging_trust.json"
 *
 * VESPA_CONFIG_URL overrides the derived config endpoint (host:19071) when the
 * cluster's ports are mapped somewhere else.
 */
object ExportLoad {
    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            require(args.isNotEmpty()) { "usage: exportLoad <events.json> [more.json ...]" }
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val batch = System.getenv("BENCH_BATCH")?.toIntOrNull() ?: 500
            // VespaEventStore derives the config URL as host:19071, so a second
            // container mapped to another port needs to be told: without this,
            // open() deploys to whatever is on 19071 and then waits two minutes
            // for THIS url to start serving an application it never got.
            val configUrl = System.getenv("VESPA_CONFIG_URL")
            val store = if (configUrl != null) VespaEventStore.open(url = url, configUrl = configUrl) else VespaEventStore.open(url)
            store.use {
                for (path in args) {
                    val events =
                        Json
                            .parseToJsonElement(Files.readString(Path.of(path)))
                            .jsonArray
                            .map { Event.fromJson(it.toString()) }
                    val rejected = HashMap<String, Int>()
                    var accepted = 0
                    events.chunked(batch).forEach { chunk ->
                        // batchInsert, never a loop over insert(): the per-event
                        // path pays admission probes the batch path amortizes.
                        store.batchInsert(chunk).forEach { outcome ->
                            when (outcome) {
                                is IEventStore.InsertOutcome.Accepted -> {
                                    accepted++
                                }

                                // The reason's PREFIX is the vocabulary
                                // (duplicate:/replaced:/blocked:, Rejections.kt);
                                // the tail names the individual event.
                                is IEventStore.InsertOutcome.Rejected -> {
                                    rejected.merge(outcome.reason.substringBefore(':'), 1, Int::plus)
                                }

                                // A write that threw, not a rule that refused —
                                // the one outcome a loader must not shrug at.
                                is IEventStore.InsertOutcome.Failed -> {
                                    rejected.merge("FAILED", 1, Int::plus)
                                }
                            }
                        }
                    }
                    val why = rejected.entries.sortedByDescending { it.value }.joinToString { "${it.key} ${it.value}" }
                    println("${path.substringAfterLast('/')}: ${events.size} read, $accepted accepted${if (why.isEmpty()) "" else ", rejected: $why"}")
                }
                // Trust is settled by a background worker; a sweep that queries
                // before it drains ranks against half a web of trust.
                store.awaitTrustProjection()
                println("trust projection settled")
            }
        }
}
