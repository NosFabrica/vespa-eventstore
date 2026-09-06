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
package com.nosfabrica.vespa.eventstore.runtime

import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * A MESSAGE ALONE IS NOT A DIAGNOSIS. This deployment spent an afternoon on
 * `last: timeout` — which named neither the query that timed out nor the walk
 * it was under — because every caller recorded the message and dropped the
 * exception.
 */
class BackgroundFailuresWhereTest {
    @BeforeTest fun clean() = BackgroundFailures.reset()

    @AfterTest fun tidy() = BackgroundFailures.reset()

    private fun thrownHere(): Throwable =
        try {
            error("timeout")
        } catch (t: Throwable) {
            t
        }

    @Test
    fun `the status line names where in our code it threw, not only the message`() {
        BackgroundFailures.record("trust.drain", thrownHere())
        val line = BackgroundFailures.statusLine()
        assertTrue(line.contains("timeout"), "the message is still there: $line")
        assertTrue(line.contains("BackgroundFailuresWhereTest"), "and now so is the frame that threw it: $line")
    }

    /** The cause chain matters: a timeout wrapped three deep says more than its outermost class. */
    @Test
    fun `the cause chain is reported`() {
        val wrapped = IllegalStateException("drain failed", thrownHere())
        BackgroundFailures.record("trust.drain", wrapped)
        assertContains(BackgroundFailures.statusLine(), "IllegalStateException <- IllegalStateException")
    }

    /** A worker retrying forever must not flood the log with one trace. */
    @Test
    fun `only the first failure of a task prints its stack`() {
        BackgroundFailures.record("trust.drain", thrownHere())
        BackgroundFailures.record("trust.drain", thrownHere())
        BackgroundFailures.record("trust.drain", thrownHere())
        assertContains(BackgroundFailures.statusLine(), "trust.drain 3 fail")
    }

    /** Clean stays clean, so a status display can splice it in unconditionally. */
    @Test
    fun `a healthy process says nothing`() {
        assertTrue(BackgroundFailures.statusLine().isEmpty())
        BackgroundFailures.record("trust.drain", thrownHere())
        BackgroundFailures.succeeded("trust.drain")
        assertFalse(BackgroundFailures.statusLine().contains("consecutive"), "a recovered worker drops the consecutive detail")
    }
}
