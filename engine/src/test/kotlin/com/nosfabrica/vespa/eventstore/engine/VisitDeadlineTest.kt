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

import com.nosfabrica.vespa.eventstore.engine.client.VespaHttp
import com.nosfabrica.vespa.eventstore.engine.client.VespaVisits
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertTrue

/**
 * THE DEADLINE ORDERING, which is the whole contract of a long visit.
 *
 * A sparse selection — one author's cards across a 196M-document corpus — can
 * honestly go a long time without emitting a matching document. If the SERVER
 * outlives the client's read deadline, the client kills the stream and the
 * retry re-issues the identical request, forever. Staging spent a night doing
 * exactly that: the streamed path was the one URL of three that omitted
 * `timeout`, and streaming is the path that runs.
 */
class VisitDeadlineTest {
    @Test
    fun `the server deadline sits under the client read deadline`() {
        assertTrue(
            VespaVisits.SERVER_TIMEOUT_SECONDS < VespaHttp.VISIT_READ_TIMEOUT_SECONDS,
            "the server must give up FIRST and return a continuation: " +
                "server=${VespaVisits.SERVER_TIMEOUT_SECONDS}s client=${VespaHttp.VISIT_READ_TIMEOUT_SECONDS}s",
        )
    }

    /**
     * Every visit URL comes from one builder now, so this covers all three
     * call sites at once — which is the point. Three hand-assembled URLs is
     * how one of them came to be missing the parameter.
     */
    @Test
    fun `every visit url carries the server deadline`() {
        val url = VespaVisits.visitUrl(endpoint = "http://vespa:8080", selection = "event.kind==30382", fieldSet = "event:id,created_at")
        assertContains(url, "timeout=${VespaVisits.SERVER_TIMEOUT_SECONDS}", message = "a visit without a server deadline outlives the client and retries forever: $url")
    }
}
