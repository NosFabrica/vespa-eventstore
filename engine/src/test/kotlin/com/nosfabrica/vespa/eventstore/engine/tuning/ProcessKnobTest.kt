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
package com.nosfabrica.vespa.eventstore.engine.tuning

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A process-wide setting is pinned ONCE: the same value again is fine (one
 * tuning opening several stores), a different one is refused rather than
 * silently re-tuning a store already running on the first.
 */
class ProcessKnobTest {
    // A variable no environment sets, so the unpinned value is the default.
    private fun knob() = ProcessKnob("VESPA_TEST_KNOB_THAT_IS_NEVER_SET", 7) { it.toIntOrNull() }

    @Test
    fun `unpinned it is the default, pinned it is the pin`() {
        val k = knob()
        assertEquals(7, k.value)
        k.pin(9)
        assertEquals(9, k.value)
        k.pin(9)
        assertEquals(9, k.value, "the same pin again is a no-op")
    }

    @Test
    fun `a second, different pin is refused and names the variable`() {
        val k = knob()
        k.pin(9)
        val e = assertFailsWith<IllegalStateException> { k.pin(10) }
        assertTrue("VESPA_TEST_KNOB_THAT_IS_NEVER_SET" in e.message!!, e.message)
        assertEquals(9, k.value, "the refused pin changed nothing")
    }
}
