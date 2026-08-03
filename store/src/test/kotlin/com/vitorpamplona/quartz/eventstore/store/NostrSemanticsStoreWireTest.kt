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
package com.vitorpamplona.quartz.eventstore.store

import com.vitorpamplona.quartz.eventstore.vespa.MockVespaEngine
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import kotlin.test.AfterTest

/**
 * The ENTIRE store semantics suite again, but over the wire: every operation
 * travels [VespaEventIndex] -> HTTP (h2c feed writes, plain reads) ->
 * [MockVespaEngine], whose YQL parser + in-memory evaluation must reproduce
 * exactly what the in-memory run produced.
 */
class NostrSemanticsStoreWireTest : NostrSemanticsStoreTest() {
    private val mock = MockVespaEngine()

    override fun newIndex(): EventIndex = VespaEventIndex(mock.url)

    @AfterTest
    fun stopMock() {
        index.close()
        mock.stop()
    }
}
