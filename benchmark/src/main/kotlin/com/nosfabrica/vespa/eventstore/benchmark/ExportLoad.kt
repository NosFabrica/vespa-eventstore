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
 *     VESPA_URL=http://localhost:8080 ./gradlew :benchmark:exportLoad \
 *       --args="/path/staging_events.json /path/staging_trust.json"
 */
object ExportLoad {
    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            require(args.isNotEmpty()) { "usage: exportLoad <events.json> [more.json ...]" }
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val batch = System.getenv("BENCH_BATCH")?.toIntOrNull() ?: 500
            val store = VespaEventStore.open(url)
            store.use {
                for (path in args) {
                    val events =
                        Json
                            .parseToJsonElement(Files.readString(Path.of(path)))
                            .jsonArray
                            .map { Event.fromJson(it.toString()) }
                    var fed = 0
                    events.chunked(batch).forEach { chunk ->
                        // batchInsert, never a loop over insert(): the per-event
                        // path pays admission probes the batch path amortizes.
                        store.batchInsert(chunk)
                        fed += chunk.size
                    }
                    println("fed $fed events from $path")
                }
                // Trust is settled by a background worker; a sweep that queries
                // before it drains ranks against half a web of trust.
                store.awaitTrustProjection()
                println("trust projection settled")
            }
        }
}
