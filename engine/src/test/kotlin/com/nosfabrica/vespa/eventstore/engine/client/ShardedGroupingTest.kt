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

import com.nosfabrica.vespa.eventstore.engine.client.ShardedGrouping.Plan
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ShardedGroupingTest {
    @Test
    fun `counts of hits and author-keyed distinct counts merge as they are`() {
        for (p in listOf(
            "all(output(count()))",
            "all(group(kind) each(output(count())))",
            "all(group(pubkey) output(count()))",
            "all( group( pubkey )  output( count() ) )",
            "all(group(pubkey) each(group(kind) output(count())))",
            // Distinct authors per tag: each author lives in one cluster, so the per-cluster counts add.
            "all(group(tag_index) each(group(pubkey) output(count())))",
            "all(group(time.date(created_at)) each(output(count(), max(created_at))))",
            "all(group(kind) output(sum(created_at)) each(output(count())))",
        )) {
            assertEquals(Plan.AsIs, ShardedGrouping.plan(p), p)
        }
    }

    @Test
    fun `a distinct count over a non-author field is rewritten to count merged groups`() {
        assertEquals(Plan.Distinct("all(group(kind) output(count()) each(output(count())))"), ShardedGrouping.plan("all(group(kind) output(count()))"))
        assertEquals(Plan.Distinct("all(group(tag_index) output(count()) each(output(count())))"), ShardedGrouping.plan(" all( group(tag_index)  output( count() ) ) "))
    }

    @Test
    fun `any other group-level count over a non-author field is refused`() {
        for ((p, field) in listOf(
            "all(group(kind) output(count()) each(output(count())))" to "kind",
            "all(group(kind) each(group(tag_index) output(count())))" to "tag_index",
            "each(group(kind) output(count(), sum(created_at)))" to "kind",
            // A quoted ')' inside a filter does not end the scope early.
            "all(filter(regex(\"a)\", tag_index)) group(kind) output(count()))" to "kind",
        )) {
            val plan = assertIs<Plan.Refused>(ShardedGrouping.plan(p), p)
            assertTrue("`$field`" in plan.reason, "$p: ${plan.reason}")
        }
    }

    @Test
    fun `an each under the author partitions its groups, an all beside it does not`() {
        assertEquals(Plan.AsIs, ShardedGrouping.plan("all(group(pubkey) each(group(kind) output(count())))"))
        assertIs<Plan.Refused>(ShardedGrouping.plan("all(group(pubkey) all(group(kind) output(count())))"))
    }

    /** Two clusters' answers to the rewrite, merged: kinds 1 and 7 in both, 6 in one — three distinct, not five. */
    @Test
    fun `the merged groups replace the summed estimate and are dropped`() {
        val merged =
            GroupingResults.mergeRoots(
                Json
                    .parseToJsonElement(
                        """
                        {"id":"toplevel","relevance":1.0,"fields":{"totalCount":9},"children":[
                          {"id":"group:root:0","relevance":1.0,"fields":{"count()":2},"children":[
                            {"id":"grouplist:kind","label":"kind","relevance":1.0,"children":[
                              {"id":"group:long:1","relevance":1.0,"value":"1","fields":{"count()":3}},
                              {"id":"group:long:7","relevance":1.0,"value":"7","fields":{"count()":1}}]}]},
                          {"id":"group:root:0","relevance":1.0,"fields":{"count()":3},"children":[
                            {"id":"grouplist:kind","label":"kind","relevance":1.0,"children":[
                              {"id":"group:long:1","relevance":1.0,"value":"1","fields":{"count()":2}},
                              {"id":"group:long:6","relevance":1.0,"value":"6","fields":{"count()":2}},
                              {"id":"group:long:7","relevance":1.0,"value":"7","fields":{"count()":1}}]}]}]}
                        """.trimIndent(),
                    ).jsonObject,
            )
        val root = ShardedGrouping.exactDistinct(merged)
        val groupRoot = root["children"]!!.jsonArray.single().jsonObject
        assertEquals(3L, groupRoot["fields"]!!.jsonObject["count()"]!!.jsonPrimitive.long)
        assertEquals(null, groupRoot["children"], "the groups were not asked for: Vespa answers the original with a bare root")
        assertEquals(9L, root["fields"]!!.jsonObject["totalCount"]!!.jsonPrimitive.long)
    }
}
