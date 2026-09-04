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
package com.nosfabrica.vespa.eventstore

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** The operator's descent switch: unset is on, and only an explicit off is off. */
class TrustDescentSwitchTest {
    @Test
    fun `unset and anything but an explicit off leave the descent on`() {
        assertTrue(VespaEventStore.trustDescentFromEnv(null))
        assertTrue(VespaEventStore.trustDescentFromEnv(""))
        assertTrue(VespaEventStore.trustDescentFromEnv("on"))
        assertTrue(VespaEventStore.trustDescentFromEnv("true"))
        assertTrue(VespaEventStore.trustDescentFromEnv("yes please"), "an unrecognised word is not a switch-off")
    }

    @Test
    fun `off, false and 0 switch it off, whatever the case or spacing`() {
        assertFalse(VespaEventStore.trustDescentFromEnv("off"))
        assertFalse(VespaEventStore.trustDescentFromEnv(" OFF "))
        assertFalse(VespaEventStore.trustDescentFromEnv("False"))
        assertFalse(VespaEventStore.trustDescentFromEnv("0"))
    }
}
