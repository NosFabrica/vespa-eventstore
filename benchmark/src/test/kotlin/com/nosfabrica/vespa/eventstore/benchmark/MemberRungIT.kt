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
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test

class MemberRungIT {
    private val vespa = System.getProperty("itVespa")
    private val corpusDir = System.getProperty("itCorpus")

    @Test
    fun rungs() {
        if (vespa == null || corpusDir == null) return println("RUNG skipped")
        // Loaded through the LOCAL store, so autoDeploy ships the schema in this
        // working tree — the one that has the member profiles. The relay's own
        // ITs would deploy the JitPack-pinned schema instead.
        val corpus = File(corpusDir!!, "corpus.jsonl").readLines().filter { it.isNotBlank() }.map { Event.fromJson(it) }
        VespaEventStore.open(vespa!!, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"), autoDeploy = true).use { store ->
            runBlocking { corpus.chunked(500).forEach { store.batchInsert(it) } }
        }
        Thread.sleep(5000)
        VespaEventIndex(vespa!!).use { index ->
            runBlocking {
                for (obs in listOf(null, System.getProperty("itObserver"))) {
                    val label = if (obs == null) "unlensed (text ladder)" else "lensed (search ladder)"
                    for (term in listOf("bitcoin", "nostr", "health", "human")) {
                        val q = EventQuery(search = term, includeSpam = true, observer = obs, limit = 30)
                        val hits = index.searchRanked(q).mapNotNull { it.score }
                        if (hits.isEmpty()) continue
                        val profile = EventYql.memberProfileOf(q)
                        println("RUNG [$label] term=$term hits=${hits.size} top=%.4g bottom=%.4g profile=$profile".format(hits.max(), hits.min()))
                        for (c in listOf(1.0, 0.95, 0.87, 0.75, 0.5, 0.1)) {
                            val mq =
                                q.copy(
                                    search = null,
                                    ranking = profile,
                                    rankFeatures = mapOf(EventYql.F_MEMBER_CONF to c),
                                    kinds = listOf(0),
                                    limit = 1,
                                )
                            val m = index.searchRanked(mq).firstOrNull()?.score ?: continue
                            println("    c=%.2f -> %.4g   below %d of %d hits".format(c, m, hits.count { it > m }, hits.size))
                        }
                    }
                }
            }
        }
    }
}
