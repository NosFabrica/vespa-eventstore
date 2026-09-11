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
package com.nosfabrica.vespa.eventstore.engine.metrics

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * WHERE ONE CALL IS, not where all of them have been.
 *
 * The stage totals are sums: they say this process spent 22 core-hours scanning,
 * never which of the nineteen open walks is doing it. The hold registry cannot
 * answer it either — it is keyed by stage, one slot per name, so concurrent
 * walks in the same stage overwrite each other by design.
 *
 * Diagnosing that gap cost four passes over one code path, each one inferring a
 * cause from aggregates and getting a different answer.
 */
class CallStageTest {
    @Test
    fun `a call reports the stage it is in`() =
        runBlocking {
            val slot = IngestStats.CallStage()
            assertNull(slot.current(), "a call that has entered no stage is in none")

            kotlinx.coroutines.withContext(slot) {
                IngestStats.timed("walk.scan") {
                    val (stage, _) = assertNotNull(slot.current(), "inside a stage, the slot must name it")
                    assertEquals("walk.scan", stage)
                }
            }
            assertNull(slot.current(), "and is empty again once the stage returns")
        }

    @Test
    fun `nesting reports the innermost stage, then restores the outer`() =
        runBlocking {
            val slot = IngestStats.CallStage()
            kotlinx.coroutines.withContext(slot) {
                IngestStats.timed("walk.ids.page") {
                    assertEquals("walk.ids.page", slot.current()?.first)
                    IngestStats.timed("walk.ids.tiegroup") {
                        assertEquals("walk.ids.tiegroup", slot.current()?.first, "the innermost stage is the answer")
                    }
                    assertEquals("walk.ids.page", slot.current()?.first, "and the outer one comes back, not null")
                }
            }
        }

    /** The property the hold registry cannot offer: one slot per CALL, so concurrency does not collide. */
    @Test
    fun `concurrent calls in the same stage do not overwrite each other`() =
        runBlocking {
            val slots = List(8) { IngestStats.CallStage() }
            slots
                .mapIndexed { i, slot ->
                    async {
                        kotlinx.coroutines.withContext(slot) {
                            IngestStats.timed(if (i % 2 == 0) "walk.scan" else "walk.ids.page") {
                                delay(20)
                                slot.current()?.first
                            }
                        }
                    }
                }.awaitAll()
                .forEachIndexed { i, seen ->
                    assertEquals(if (i % 2 == 0) "walk.scan" else "walk.ids.page", seen, "call $i must report its OWN stage")
                }
        }

    @Test
    fun `an absent slot costs nothing and changes nothing`() =
        runBlocking {
            // No CallStage in context: `timed` still books its totals.
            IngestStats.reset()
            IngestStats.timed("walk.scan") { delay(1) }
            assertEquals(1L, IngestStats.snapshot()["walk.scan"]?.calls, "the totals are unaffected by the slot's absence")
            IngestStats.reset()
        }
}
