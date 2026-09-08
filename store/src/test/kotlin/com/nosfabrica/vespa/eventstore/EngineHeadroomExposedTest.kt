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
import kotlin.test.assertTrue

/**
 * THE TEST THAT WAS MISSING FROM #132. That PR shipped the headroom parser and
 * its tests, and nothing constructed the class in production — the feature was
 * dead code and every test still passed, because they all drove the parser
 * seam directly. A page cannot render what the front door does not offer, so
 * this asserts the surface a caller actually reaches for.
 */
class EngineHeadroomExposedTest {
    @Test
    fun `the store's front door offers engine headroom`() {
        val exposed = VespaEventStore::class.members.map { it.name }
        assertTrue("engineHeadroom" in exposed, "no page can show what the store does not expose: $exposed")
    }
}
