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

import com.nosfabrica.vespa.eventstore.engine.DocRef
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class GroupingResultsTest {
    /**
     * The three-level shape `EventYql.buildLinkPeople` asks for, as Vespa 8.763
     * answered it (trimmed; `sum(relevance())` is the author's summed trust):
     * authors and kinds sit UNDER each key's group, so a reader that collected
     * every `group:` node — as [GroupingResults.groupCounts] does — would mix
     * pubkeys and "7" in among the keys.
     */
    @Test
    fun `key then person then kind groups read as nested maps`() {
        val root =
            Json
                .parseToJsonElement(
                    """
                    {"id":"toplevel","relevance":1.0,"fields":{"totalCount":5},"children":[
                      {"id":"group:root:0","relevance":1.0,"children":[
                        {"id":"grouplist:tag_index","label":"tag_index","children":[
                          {"id":"group:string:e:aa","relevance":1.0,"value":"e:aa","children":[
                            {"id":"grouplist:pubkey","label":"pubkey","children":[
                              {"id":"group:string:bob","relevance":90.0,"value":"bob","children":[
                                {"id":"grouplist:kind","label":"kind","children":[
                                  {"id":"group:long:7","relevance":90.0,"value":"7","fields":{"count()":2,"sum(relevance())":180.0}},
                                  {"id":"group:long:6","relevance":90.0,"value":"6","fields":{"count()":1,"sum(relevance())":90.0}}]}]},
                              {"id":"group:string:carol","relevance":3.0,"value":"carol","children":[
                                {"id":"grouplist:kind","label":"kind","children":[
                                  {"id":"group:long:1","relevance":3.0,"value":"1","fields":{"count()":1}}]}]}]}]}]}]}]}
                    """.trimIndent(),
                ).jsonObject
        assertEquals(
            mapOf("e:aa" to mapOf("bob" to mapOf(7 to (2 to 180.0), 6 to (1 to 90.0)), "carol" to mapOf(1 to (1 to null)))),
            GroupingResults.keyPersonKinds(root),
            "a sum the response does not carry (an unranked grouping) reads as null, not 0",
        )
    }

    @Test
    fun `no groups is no links`() {
        val root = Json.parseToJsonElement("""{"id":"toplevel","fields":{"totalCount":0},"children":[{"id":"group:root:0"}]}""").jsonObject
        assertEquals(emptyMap(), GroupingResults.keyPersonKinds(root))
    }

    /**
     * `EventYql.buildNewestVersions`' shape: author -> the newest second -> every
     * id at it. The grouping cannot order ids, so the reader breaks the tie to
     * the LOWEST id (NIP-01) — `bb…` here, although Vespa listed `cc…` first —
     * and re-takes the newest second as a max rather than trusting the response
     * to hold exactly the one `max(1)` asked for.
     */
    @Test
    fun `newest versions read the newest second and break its tie to the lowest id`() {
        val root =
            Json
                .parseToJsonElement(
                    """
                    {"id":"toplevel","relevance":1.0,"fields":{"totalCount":4},"children":[
                      {"id":"group:root:0","relevance":1.0,"children":[
                        {"id":"grouplist:pubkey","label":"pubkey","children":[
                          {"id":"group:string:alice","relevance":1.0,"value":"alice","children":[
                            {"id":"grouplist:created_at","label":"created_at","children":[
                              {"id":"group:long:200","relevance":1.0,"value":"200","children":[
                                {"id":"grouplist:id","label":"id","children":[
                                  {"id":"group:string:cc","relevance":1.0,"value":"cc","fields":{"count()":1}},
                                  {"id":"group:string:bb","relevance":1.0,"value":"bb","fields":{"count()":1}}]}]},
                              {"id":"group:long:100","relevance":1.0,"value":"100","children":[
                                {"id":"grouplist:id","label":"id","children":[
                                  {"id":"group:string:aa","relevance":1.0,"value":"aa","fields":{"count()":1}}]}]}]}]},
                          {"id":"group:string:bob","relevance":1.0,"value":"bob","children":[
                            {"id":"grouplist:created_at","label":"created_at","children":[
                              {"id":"group:long:50","relevance":1.0,"value":"50","children":[
                                {"id":"grouplist:id","label":"id","children":[
                                  {"id":"group:string:dd","relevance":1.0,"value":"dd","fields":{"count()":1}}]}]}]}]}]}]}]}
                    """.trimIndent(),
                ).jsonObject
        assertEquals(
            mapOf("alice" to DocRef("bb", 200), "bob" to DocRef("dd", 50)),
            GroupingResults.newestPerAuthor(root),
        )
    }

    @Test
    fun `no author groups is nothing to beat`() {
        val root = Json.parseToJsonElement("""{"id":"toplevel","fields":{"totalCount":0},"children":[{"id":"group:root:0"}]}""").jsonObject
        assertEquals(emptyMap(), GroupingResults.newestPerAuthor(root))
    }

    /**
     * Two content clusters (author sharding) answer one grouping with one `group:root` EACH —
     * Vespa 8 measured, the shape below trimmed from it. Without the merge [GroupingResults.firstCount]
     * read the first cluster's count alone and [GroupingResults.groupCounts] let the second
     * cluster's count for the same tag overwrite the first's.
     */
    @Test
    fun `per-cluster grouping roots merge into one, counts and sums added`() {
        val root =
            Json
                .parseToJsonElement(
                    """
                    {"id":"toplevel","relevance":1.0,"fields":{"totalCount":22},"children":[
                      {"id":"group:root:0","relevance":1.0,"fields":{"count()":11},"children":[
                        {"id":"grouplist:tag_index","label":"tag_index","children":[
                          {"id":"group:string:t:a","relevance":1.0,"value":"t:a","fields":{"count()":3,"max(created_at)":10,"sum(relevance())":1.5}},
                          {"id":"group:string:t:b","relevance":0.5,"value":"t:b","fields":{"count()":1,"max(created_at)":4,"sum(relevance())":2.0}}]}]},
                      {"id":"group:root:0","relevance":1.0,"fields":{"count()":11},"children":[
                        {"id":"grouplist:tag_index","label":"tag_index","children":[
                          {"id":"group:string:t:a","relevance":1.0,"value":"t:a","fields":{"count()":4,"max(created_at)":12,"sum(relevance())":0.5}},
                          {"id":"group:string:t:c","relevance":0.5,"value":"t:c","fields":{"count()":2,"max(created_at)":3}}]}]}]}
                    """.trimIndent(),
                ).jsonObject
        val merged = GroupingResults.mergeRoots(root)
        assertEquals(22, GroupingResults.firstCount(merged))
        assertEquals(mapOf("t:a" to 7, "t:b" to 1, "t:c" to 2), GroupingResults.groupCounts(merged).filterKeys { it.startsWith("t:") })
        val a = merged.toString().let { Json.parseToJsonElement(it) }
        val tagA =
            a.jsonObject["children"]!!
                .jsonArray
                .single()
                .jsonObject["children"]!!
                .jsonArray
                .single()
                .jsonObject["children"]!!
                .jsonArray
                .first()
                .jsonObject["fields"]!!
                .jsonObject
        assertEquals(12, tagA["max(created_at)"]!!.jsonPrimitive.int)
        assertEquals(2.0, tagA["sum(relevance())"]!!.jsonPrimitive.double)
    }

    /** The link read's three levels: a key both clusters hold, its people disjoint (an author lives in one). */
    @Test
    fun `a key both clusters hold keeps every cluster's people`() {
        fun cluster(person: String) =
            """{"id":"group:root:0","children":[{"id":"grouplist:tag_index","label":"tag_index","children":[
                 {"id":"group:string:e:aa","value":"e:aa","children":[{"id":"grouplist:pubkey","label":"pubkey","children":[
                   {"id":"group:string:$person","value":"$person","children":[{"id":"grouplist:kind","label":"kind","children":[
                     {"id":"group:long:7","value":"7","fields":{"count()":1,"sum(relevance())":9.0}}]}]}]}]}]}]}"""
        val root = Json.parseToJsonElement("""{"id":"toplevel","children":[${cluster("bob")},${cluster("carol")}]}""").jsonObject
        assertEquals(
            mapOf("e:aa" to mapOf("bob" to mapOf(7 to (1 to 9.0)), "carol" to mapOf(7 to (1 to 9.0)))),
            GroupingResults.keyPersonKinds(GroupingResults.mergeRoots(root)),
        )
    }

    @Test
    fun `one root is returned as it came`() {
        val root = Json.parseToJsonElement("""{"id":"toplevel","children":[{"id":"group:root:0","fields":{"count()":3}}]}""").jsonObject
        assertSame(root, GroupingResults.mergeRoots(root))
    }
}
