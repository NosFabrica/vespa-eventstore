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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals

class GroupingResultsTest {
    /**
     * The two-level shape `EventYql.buildLinkCounts` asks for, as Vespa 8.763
     * answered it (trimmed): the kinds' leaf groups sit UNDER each key's group,
     * so a reader that collected every `group:` node — as [GroupingResults.groupCounts]
     * does — would mix "7" in among the keys.
     */
    @Test
    fun `key-then-kind groups read as key to kind to count`() {
        val root =
            Json
                .parseToJsonElement(
                    """
                    {"id":"toplevel","relevance":1.0,"fields":{"totalCount":5},"children":[
                      {"id":"group:root:0","relevance":1.0,"children":[
                        {"id":"grouplist:tag_index","label":"tag_index","children":[
                          {"id":"group:string:e:aa","relevance":1.0,"value":"e:aa","fields":{"count()":3},"children":[
                            {"id":"grouplist:kind","label":"kind","children":[
                              {"id":"group:long:7","relevance":1.0,"value":"7","fields":{"count()":2}},
                              {"id":"group:long:9735","relevance":1.0,"value":"9735","fields":{"count()":1}}]}]},
                          {"id":"group:string:a:30023:bb:d","relevance":1.0,"value":"a:30023:bb:d","fields":{"count()":2},"children":[
                            {"id":"grouplist:kind","label":"kind","children":[
                              {"id":"group:long:1","relevance":1.0,"value":"1","fields":{"count()":2}}]}]}]}]}]}
                    """.trimIndent(),
                ).jsonObject
        assertEquals(mapOf("e:aa" to mapOf(7 to 2, 9735 to 1), "a:30023:bb:d" to mapOf(1 to 2)), GroupingResults.keyKindCounts(root))
    }

    @Test
    fun `no groups is no links`() {
        val root = Json.parseToJsonElement("""{"id":"toplevel","fields":{"totalCount":0},"children":[{"id":"group:root:0"}]}""").jsonObject
        assertEquals(emptyMap(), GroupingResults.keyKindCounts(root))
    }
}
