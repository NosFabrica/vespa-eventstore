/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking

/**
 * `BENCH_ADDR_SMOKE=1` (run with `VESPA_ADDRESS_KEYED=1`): a LIVE end-to-end
 * check of the address-keyed store path the in-memory suite can't exercise —
 * per-event replaceable newest-wins, addressability by BOTH id and address, and
 * stale rejection, straight through NostrSemanticsStore against a real Vespa.
 */
object AddrSmokeProbe {
    fun run(url: String) =
        runBlocking {
            val pk = "ad".repeat(32)
            var fails = 0

            fun check(
                name: String,
                cond: Boolean,
            ) {
                println((if (cond) "  ok   " else "  FAIL ") + name)
                if (!cond) fails++
            }

            fun addr(
                id: String,
                at: Long,
                d: String,
            ) = Event(id.repeat(64).take(64), pk, at, 31234, arrayOf(arrayOf("d", d)), "draft", "00")

            VespaEventStore.open(url).use { store ->
                val v1 = addr("a1", 100, "smoke")
                val v2 = addr("a2", 300, "smoke")
                val vStale = addr("a3", 200, "smoke")
                val other = addr("b1", 100, "other")

                store.insert(v1)
                store.insert(v2) // newer -> supersedes v1
                store.insert(other)
                val staleRejected =
                    try {
                        store.insert(vStale)
                        false
                    } catch (e: Exception) {
                        true
                    }

                check("stale (created_at < head) rejected", staleRejected)
                // by ADDRESS: exactly the winner remains for (31234, pk, smoke)
                val byAddr = store.query<Event>(Filter(kinds = listOf(31234), authors = listOf(pk)))
                check("address query returns both live drafts", byAddr.map { it.id }.toSet() == setOf(v2.id, other.id))
                // by ID: the winner resolves, the superseded id does not
                check("get by winner id resolves", store.query<Event>(Filter(ids = listOf(v2.id))).map { it.id } == listOf(v2.id))
                check("get by superseded id is empty", store.query<Event>(Filter(ids = listOf(v1.id))).isEmpty())
                check("get by stale (never stored) id is empty", store.query<Event>(Filter(ids = listOf(vStale.id))).isEmpty())

                // cleanup
                runCatching { store.delete(Filter(ids = listOf(v2.id, other.id))) }
            }
            println(if (fails == 0) "ADDR SMOKE: PASS" else "ADDR SMOKE: $fails FAILURE(S)")
        }
}
