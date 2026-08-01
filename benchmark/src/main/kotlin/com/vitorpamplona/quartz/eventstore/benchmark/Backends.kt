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

import com.vitorpamplona.quartz.eventstore.store.NostrSemanticsStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore as SqliteEventStore

/**
 * Factories for the [IEventStore]s under test. Each returns a FRESH, empty store
 * so an insert benchmark starts from zero. The three that can run with no
 * external services (both SQLite modes and the in-memory Vespa reference) are the
 * default matrix; a real Vespa is opt-in via `BENCH_VESPA_URL` because it needs a
 * running cluster.
 */
object Backends {
    /** Quartz's SQLite store, entirely in memory (`dbName = null`) — no disk I/O, the fastest SQLite mode. */
    fun sqliteMemory(): IEventStore = SqliteEventStore(dbName = null)

    /** Quartz's SQLite store on a real file — pays WAL/fsync, the honest embedded-DB number. */
    fun sqliteDisk(path: String): IEventStore = SqliteEventStore(dbName = path)

    /**
     * This framework's store over the in-memory REFERENCE engine. NOTE: that
     * engine answers every filter with an O(n) linear scan, so its throughput is
     * NOT a proxy for Vespa — it is a labelled reference that isolates the store
     * layer's own logic. Use [countingVespa] to read the round-trip counts, which
     * ARE engine-independent.
     */
    fun vespaInMemory(): IEventStore = NostrSemanticsStore(InMemoryEventIndex(), relay = null)

    /** The reference store wrapped so every engine call is counted. */
    fun countingVespa(): Pair<IEventStore, CountingEventIndex> {
        val counting = CountingEventIndex(InMemoryEventIndex())
        return NostrSemanticsStore(counting, relay = null) to counting
    }
}
