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
package com.nosfabrica.vespa.eventstore.benchmark.harness

import com.vitorpamplona.quartz.nip01Core.store.conformance.ConformanceCorpus
import com.vitorpamplona.quartz.nip01Core.store.conformance.StoreConformance
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Quartz's conformance battery, with no Docker: the store's write path and the
 * in-memory reference ([Backends.vespaInMemory], the executable spec of the
 * engine's matching) against Quartz's SQLite store. `VespaParityIT` runs the
 * same battery against a real Vespa; this one catches a write-path divergence
 * (supersession, NIP-09 by id and by address, a deletion of a deletion, a
 * re-offered event) on every unit build, before an integration run would.
 */
class StoreParityTest {
    @Test
    fun `the in-memory store answers quartz's conformance battery exactly as SQLite`() =
        runBlocking {
            val corpus = ConformanceCorpus.generate(ConformanceCorpus.Config(size = 3_000, seed = 42))
            val sqlite = Backends.sqliteMemory()
            val store = Backends.vespaInMemory()
            StoreConformance.load(sqlite, corpus)
            StoreConformance.load(store, corpus)
            val report = StoreParity.run(corpus, "SQLite", sqlite, "in-memory", store)
            sqlite.close()
            store.close()
            assertTrue(report.ok, report.summary(limit = Int.MAX_VALUE))
            assertTrue(report.checks > 100, "the battery ran: ${report.checks} checks")
        }
}
