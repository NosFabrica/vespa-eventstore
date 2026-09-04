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
import kotlinx.coroutines.launch
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
 * The live-holder record under the two conditions that broke its predecessor.
 *
 * `IngestStats.held` was ONE `@Volatile` field, justified by "the store
 * serialises every write behind ONE mutex, so there is never more than one
 * holder". The trust-gate split made that false — `lockedForWrite` nests two
 * mutexes — so the inner begin overwrote the outer and the inner release
 * cleared it, reporting a still-held lock as free.
 *
 * The obvious repair, a `ThreadLocal` stack, is ALSO wrong here and that is the
 * second test below: this store is coroutines end to end, and a `suspend` body
 * may resume on a different dispatcher thread than it started on, so a push and
 * its pop can land on two different stacks.
 */
class HoldStackTest {
    @BeforeTest
    fun clean() = IngestStats.reset()

    @AfterTest
    fun tidy() = IngestStats.reset()

    @Test
    fun `an inner hold's release does not free the outer one`() =
        runBlocking {
            IngestStats.holding("lock.ingest.hold") {
                IngestStats.holding("lock.ingest.trust.hold") {
                    assertEquals(2, IngestStats.allHeld().size, "both locks are held here")
                }
                // THE REGRESSION. With a single field, the inner release left
                // this reporting nothing held while the outer lock was very
                // much still held.
                val held = IngestStats.allHeld()
                assertEquals(1, held.size, "the outer lock is still held after the inner released")
                assertEquals("lock.ingest.hold", held.single().stage)
            }
            assertTrue(IngestStats.allHeld().isEmpty(), "and nothing is held once both release")
        }

    @Test
    fun `a hold survives the coroutine moving between threads`() =
        runBlocking {
            IngestStats.holding("lock.gate.hold") {
                val before = Thread.currentThread().threadId()
                // Force real dispatch and a suspension; on Dispatchers.Default
                // the continuation is free to resume on another worker.
                var moved = false
                repeat(40) {
                    withContext(Dispatchers.Default) {
                        delay(1)
                        if (Thread.currentThread().threadId() != before) moved = true
                    }
                }
                // Whether or not the thread actually changed on this box, the
                // hold must be intact — a ThreadLocal stack would have lost it
                // the first time it did.
                val held = IngestStats.allHeld()
                assertEquals(1, held.size, "the hold survived (thread changed: $moved)")
                assertEquals("lock.gate.hold", held.single().stage)
            }
            assertTrue(IngestStats.allHeld().isEmpty())
        }

    @Test
    fun `annotateHold names the work, keeps the start time, and reaches across suspension`() =
        runBlocking {
            IngestStats.holding("lock.gate.hold") {
                val started = assertNotNull(IngestStats.heldNow()).sinceNanos
                withContext(Dispatchers.Default) {
                    delay(5)
                    IngestStats.annotateHold("derive 20000 subject(s) in 40 chunk(s)")
                }
                val held = assertNotNull(IngestStats.heldNow())
                assertEquals("derive 20000 subject(s) in 40 chunk(s)", held.detail)
                assertEquals("lock.gate.hold", held.stage, "annotating must not rename the lock")
                assertEquals(started, held.sinceNanos, "annotating a hold must not restart its clock")
                assertEquals(held.detail, held.label, "label prefers the specific detail")
            }
        }

    @Test
    fun `annotateHold outside a hold is a no-op rather than a crash`() =
        runBlocking {
            // Accounting must never break the work it reports on.
            IngestStats.annotateHold("nobody is holding anything")
            assertNull(IngestStats.heldNow())
        }

    @Test
    fun `holderOf identifies the holder of a SPECIFIC lock`() =
        runBlocking {
            IngestStats.holding("lock.ingest.hold") {
                IngestStats.annotateHold("bulk commit")
                IngestStats.holding("lock.gate.hold") {
                    IngestStats.annotateHold("max_rank raise")
                    // The causal edge is only useful if it names the holder of
                    // the lock the waiter actually wants, not whatever is
                    // running.
                    assertEquals("bulk commit", IngestStats.holderOf("lock.ingest.hold")?.label)
                    assertEquals("max_rank raise", IngestStats.holderOf("lock.gate.hold")?.label)
                    assertNull(IngestStats.holderOf("lock.nobody.hold"))
                }
            }
        }

    @Test
    fun `a hold is released even when the body throws`() =
        runBlocking {
            runCatching {
                IngestStats.holding("lock.ingest.hold") {
                    throw IllegalStateException("engine died mid-section")
                }
            }
            assertTrue(IngestStats.allHeld().isEmpty(), "a failed critical section must not leak a phantom holder forever")
        }

    @Test
    fun `heldNow reports the longest-running hold`() =
        runBlocking {
            IngestStats.holding("lock.ingest.hold") {
                delay(10)
                IngestStats.holding("lock.gate.hold") {
                    // The oldest hold is the one a stalled pipeline is waiting
                    // out, so that is the one a single-answer status shows.
                    assertEquals("lock.ingest.hold", assertNotNull(IngestStats.heldNow()).stage)
                }
            }
        }

    @Test
    fun `concurrent holders are each visible`() =
        runBlocking {
            val started = java.util.concurrent.CountDownLatch(3)
            val release = java.util.concurrent.CountDownLatch(1)
            kotlinx.coroutines.coroutineScope {
                val jobs =
                    (1..3).map { n ->
                        launch(Dispatchers.Default) {
                            IngestStats.holding("lock.worker$n.hold") {
                                started.countDown()
                                withContext(Dispatchers.IO) { release.await() }
                            }
                        }
                    }
                started.await()
                assertEquals(3, IngestStats.allHeld().size, "every open hold across the process is visible")
                release.countDown()
                jobs.forEach { it.join() }
            }
            assertTrue(IngestStats.allHeld().isEmpty())
        }

    @Test
    fun `wait time is attributed to whatever was holding`() {
        // The gap-4 edge, at the accounting level: `lock.ingest.wait 41s` only
        // prompts a question; split by holder it names a fix.
        IngestStats.addBlocked("lock.ingest.wait", "proj.fetch.derive", 38_000_000_000)
        IngestStats.addBlocked("lock.ingest.wait", "write", 2_100_000_000)
        IngestStats.addBlocked("lock.ingest.wait", "proj.fetch.derive", 400_000_000)
        val split = IngestStats.blockedSplit()["lock.ingest.wait"]!!
        assertEquals(38_400_000_000, split["proj.fetch.derive"], "repeat attributions accumulate")
        assertEquals(2_100_000_000, split["write"])
    }
}
