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
package com.nosfabrica.vespa.eventstore.engine.client

import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * "The answer was partial" and "the query was wrong" are different facts, and
 * a caller that can walk the same set another way must be able to tell them
 * apart. Both were IllegalArgumentException, so a service walk with a
 * perfectly good document-API fallback died instead of taking it — and every
 * service large enough to trip proton's match-phase limiter went unprojected.
 */
class PartialAnswerTest {
    @Test
    fun `a partial answer is typed, so a caller with another way to read can take it`() {
        val cut = assertFailsWith<PartialAnswer> { throw PartialAnswer("vespa searched only 54% of the corpus") }
        assertTrue(cut is IllegalArgumentException, "still an IAE, so existing handlers keep working")
        assertTrue(cut.message!!.contains("54%"))
    }

    /**
     * The distinction the type carries: a caller catching PartialAnswer must
     * NOT swallow an ordinary argument error, which means something else.
     */
    @Test
    fun `an ordinary argument error is not a partial answer`() {
        assertFailsWith<IllegalArgumentException> {
            try {
                require(false) { "a real argument problem" }
            } catch (cut: PartialAnswer) {
                error("must not be caught as a partial answer")
            }
        }
    }
}
