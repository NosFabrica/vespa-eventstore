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
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The shape staging actually serves on `:8080/metrics/v2/values` — two content
 * nodes, memory and disk each. Trimmed to the fields the panel reads, so a
 * Vespa upgrade that moves them fails here rather than blanking the panel.
 */
class EngineResourcesTest {
    private val realShape =
        """
        {"nodes":[
          {"hostname":"vespa-relay-vespa-0.vespa-relay-vespa.ns.svc.cluster.local","services":[
            {"name":"vespa.searchnode","metrics":[
              {"values":{"content.proton.resource_usage.memory.average":0.811,
                         "content.proton.resource_usage.disk.average":0.435,
                         "content.proton.resource_usage.feeding_blocked.last":0.0}}]}]},
          {"hostname":"vespa-relay-vespa-1.vespa-relay-vespa.ns.svc.cluster.local","services":[
            {"name":"vespa.searchnode","metrics":[
              {"values":{"content.proton.resource_usage.memory.average":0.722,
                         "content.proton.resource_usage.disk.average":0.422,
                         "content.proton.resource_usage.feeding_blocked.last":0.0}}]}]}
        ]}
        """.trimIndent()

    @Test
    fun `both nodes are read, and the peak is what decides whether the cluster feeds`() {
        val usage = EngineResources.parseForTest(realShape, atMillis = 1_000)
        assertEquals(2, usage.nodes.size)
        assertEquals(listOf("vespa-relay-vespa-0", "vespa-relay-vespa-1"), usage.nodes.map { it.host }, "hostnames are shortened to the pod name")
        assertEquals(0.811, usage.peakMemory, 1e-9, "the WORST node, not an average — that is the one that blocks feed")
        assertEquals(0.435, usage.peakDisk, 1e-9)
        assertTrue(!usage.anyFeedBlocked)
    }

    /**
     * THE TEST THAT WAS MISSING. The first cut of this shipped a parser and
     * its tests, and nothing constructed the class in production — the feature
     * was dead code and every test passed, because they all called the parser
     * seam directly. This one walks the surface a caller actually uses.
     */
    @Test
    fun `the index exposes headroom rather than orphaning the probe`() {
        val onIndex = VespaEventIndex::class.members.map { it.name }
        assertTrue("engineHeadroom" in onIndex, "the probe must be reachable from the index, not dead code beside it: $onIndex")
    }

    /** The state the panel exists to make visible: feed blocked on ONE node blocks the cluster. */
    @Test
    fun `a single blocked node reports the cluster as blocked`() {
        val blocked = realShape.replace("\"content.proton.resource_usage.feeding_blocked.last\":0.0", "\"content.proton.resource_usage.feeding_blocked.last\":1.0")
        assertTrue(EngineResources.parseForTest(blocked, atMillis = 1).anyFeedBlocked)
    }
}
