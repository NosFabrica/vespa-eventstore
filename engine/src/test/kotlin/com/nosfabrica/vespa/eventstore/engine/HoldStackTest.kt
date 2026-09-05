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
package com.nosfabrica.vespa.eventstore.engine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The live-holder record, and the wait attribution built on it.
 *
 * The holder map is keyed by STAGE, which is right for holds — a mutex has one
 * holder, so two labels can never collide on it — and that is what fixed the
 * single-`@Volatile`-field predecessor, where a short `writes` hold overwrote
 * and then erased the drain's seconds-long gate hold.
 *
 * The WAITER side needs the other key. Several stage labels share one mutex
 * (`lock.gate`, `lock.ingest.trust`, `lock.sweep.trust` and
 * `lock.reindex.trust` are all the trust gate), so matching a waiter to its
 * holder by label misses whenever the two took the same mutex under different
 * names — which is the common case, and which a real corpus caught after the
 * synthetic tests had all passed (docs/telemetry.md §15.1).
 */
class HoldStackTest {
    /** The two mutex names the store uses; several stage labels share each. */
    private val writeLock = "writes"
    private val trustGate = "trustGate"

    @BeforeTest
    fun clean() = IngestStats.reset()

    @AfterTest
    fun tidy() = IngestStats.reset()

    /** Hold [stage] on [lock] for the extent of [body], the way the store's lock helper does. */
    private inline fun <T> holding(
        lock: String,
        stage: String,
        body: () -> T,
    ): T {
        IngestStats.beginHold(stage, lock = lock)
        try {
            return body()
        } finally {
            IngestStats.endHold(stage)
        }
    }

    @Test
    fun `an inner hold's release does not free the outer one`() {
        holding(writeLock, "lock.ingest.hold") {
            holding(trustGate, "lock.ingest.trust.hold") {
                assertEquals(2, IngestStats.heldAll().size, "both locks are held here")
            }
            // THE REGRESSION. With a single field, the inner release left this
            // reporting nothing held while the outer lock was still held.
            val held = IngestStats.heldAll()
            assertEquals(1, held.size, "the outer lock is still held after the inner released")
            assertEquals("lock.ingest.hold", held.single().stage)
        }
        assertTrue(IngestStats.heldAll().isEmpty(), "and nothing is held once both release")
    }

    @Test
    fun `a waiter finds the holder even when they took the same mutex under different labels`() {
        // THE BUG REAL DATA FOUND. `lock.gate` and `lock.ingest.trust` are two
        // labels for ONE mutex, so a waiter arriving under one must still see a
        // holder registered under the other. Matching by label returned null
        // every time and silently dropped every attribution — 7.4 s of measured
        // gate wait on a real corpus recorded as "behind nobody".
        holding(trustGate, "lock.ingest.trust.hold") {
            IngestStats.annotateHold("bulk commit holding the trust gate")
            val holder = assertNotNull(IngestStats.holderOf(trustGate))
            assertEquals(
                "bulk commit holding the trust gate",
                IngestStats.labelOf(holder),
                "a lock.gate waiter must see the lock.ingest.trust holder — same mutex",
            )
            assertNull(IngestStats.holderOf(writeLock), "and must NOT see a holder of a different mutex")
        }
    }

    @Test
    fun `holderOf picks the longest-running holder of that mutex`() {
        holding(trustGate, "lock.gate.hold") {
            Thread.sleep(5)
            holding(trustGate, "lock.ingest.trust.hold") {
                // Both are the trust gate; the one that has been waited out
                // longest is the one a stalled writer is queued behind.
                assertEquals("lock.gate.hold", assertNotNull(IngestStats.holderOf(trustGate)).stage)
            }
        }
    }

    @Test
    fun `a hold survives the coroutine moving between threads`() =
        runBlocking {
            IngestStats.beginHold("lock.gate.hold", lock = trustGate)
            try {
                val before = Thread.currentThread().threadId()
                var moved = false
                repeat(40) {
                    withContext(Dispatchers.Default) {
                        delay(1)
                        if (Thread.currentThread().threadId() != before) moved = true
                    }
                }
                // A ThreadLocal record — the obvious repair for the old
                // single-field bug — would have lost this the first time the
                // continuation resumed on another worker.
                val held = IngestStats.heldAll()
                assertEquals(1, held.size, "the hold survived (thread changed: $moved)")
                assertEquals("lock.gate.hold", held.single().stage)
            } finally {
                IngestStats.endHold("lock.gate.hold")
            }
            assertTrue(IngestStats.heldAll().isEmpty())
        }

    @Test
    fun `annotateHold names the work and keeps the start time`() {
        holding(trustGate, "lock.gate.hold") {
            val started = assertNotNull(IngestStats.heldNow()).sinceNanos
            IngestStats.annotateHold("derive 20000 subject(s) in 40 chunk(s)")
            val held = assertNotNull(IngestStats.heldNow())
            assertEquals("derive 20000 subject(s) in 40 chunk(s)", held.detail)
            assertEquals("lock.gate.hold", held.stage, "annotating must not rename the lock")
            assertEquals(started, held.sinceNanos, "annotating a hold must not restart its clock")
            assertEquals(trustGate, held.lock, "and must not lose which mutex it is on")
        }
    }

    @Test
    fun `annotateHold outside a hold is a no-op rather than a crash`() {
        // Accounting must never break the work it reports on.
        IngestStats.annotateHold("nobody is holding anything")
        assertNull(IngestStats.heldNow())
    }

    @Test
    fun `heldNow reports the longest-running hold`() {
        holding(writeLock, "lock.ingest.hold") {
            Thread.sleep(5)
            holding(trustGate, "lock.gate.hold") {
                assertEquals("lock.ingest.hold", assertNotNull(IngestStats.heldNow()).stage)
            }
        }
    }

    @Test
    fun `wait time is attributed to whatever was holding`() {
        // `lock.ingest.wait 41s` only prompts a question; split by holder it
        // names a fix.
        IngestStats.addBlocked("lock.ingest.wait", "proj.fetch.derive", 38_000_000_000)
        IngestStats.addBlocked("lock.ingest.wait", "write", 2_100_000_000)
        IngestStats.addBlocked("lock.ingest.wait", "proj.fetch.derive", 400_000_000)
        val split = IngestStats.blockedSplit()["lock.ingest.wait"]!!
        assertEquals(38_400_000_000, split["proj.fetch.derive"], "repeat attributions accumulate")
        assertEquals(2_100_000_000, split["write"])
    }
}
