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

import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.sun.net.httpserver.HttpServer
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AuthorClustersTest {
    private val http = VespaHttp()

    /** Closed, or its connection pool's thread outlives the test (OkHttpThreadLeakTest checks the JVM). */
    @AfterTest
    fun close() = http.close()

    private fun clusters(shards: Int) = AuthorClusters(if (shards == 1) listOf(null) else AuthorShards.clusters(shards), http) { "http://unused" }

    private fun key(middle: String) = "0".repeat(32) + middle + "0".repeat(30)

    /** Vespa 8's answer to a visit naming a cluster it does not have, as measured. */
    @Test
    fun `the deployed clusters read off a no-such-cluster refusal`() {
        assertEquals(
            listOf("content1", "content"),
            AuthorClusters.deployedOf("""{"pathId":"/document/v1/event/event/docid","message":"Your Vespa deployment has no content cluster 'content2', only 'content1', 'content'"}"""),
        )
        assertNull(AuthorClusters.deployedOf("""{"message":"something else"}"""))
    }

    @Test
    fun `a scoped query needs only its authors' clusters`() {
        val two = clusters(2)
        assertEquals(listOf("content"), two.clustersFor(setOf(key("10"), key("7f"))))
        assertEquals(listOf("content1"), two.clustersFor(setOf(key("80"))))
        assertNull(two.clustersFor(setOf(key("10"), key("f0"))), "spanning every shard is every cluster")
        assertNull(two.clustersFor(null), "no scope can match anyone")
        assertNull(two.clustersFor(emptySet()), "an empty scope matches nothing anywhere")
        assertEquals(listOf("content1", "content3"), clusters(4).clustersFor(setOf(key("40"), key("c0"), key("ff"))))
        assertNull(clusters(1).clustersFor(setOf(key("10"))), "one cluster is never scoped")
    }

    @Test
    fun `the scope is the authors every match must have, as the clause spells them`() {
        val a = key("10")
        val b = key("f0")
        assertEquals(setOf(a), EventYql.authorScopeOf(EventQuery(authors = listOf(a.uppercase(), "not-hex"))))
        assertEquals(setOf(b), EventYql.authorScopeOf(EventQuery(authorWeights = mapOf(b to 3))))
        assertEquals(setOf(a), EventYql.authorScopeOf(EventQuery(authors = listOf(a, b), authorWeights = mapOf(a to 1))), "two AND-ed author clauses intersect")
        assertNull(EventYql.authorScopeOf(EventQuery(kinds = listOf(1))))
        assertEquals(setOf(a), EventYql.build(EventQuery(authors = listOf(a), kinds = listOf(1), limit = 5))?.authorScope)
    }

    @Test
    fun `a scoped query reads from the named clusters, the rest of its yql untouched`() {
        assertEquals(
            "select id, created_at from sources content, content1 where pubkey in (\"x\") and content contains \" from event where \"",
            VespaEventIndex.scopedYql("select id, created_at from event where pubkey in (\"x\") and content contains \" from event where \"", listOf("content", "content1")),
        )
        assertNull(VespaEventIndex.scopedYql("select pubkey from reputation where true", listOf("content")))
    }

    /**
     * The silent case the check exists for: a client configured for FEWER clusters than are
     * deployed. Its visits would name only its own and skip the rest without an error; the
     * probe of the cluster one past its last finds that the deployment has it (Vespa answers
     * documents, not a refusal) and fails instead.
     */
    @Test
    fun `verify fails a client that knows fewer clusters than are deployed, and passes an exact match`() {
        fun answering(
            status: Int,
            body: String,
        ): Pair<HttpServer, String> {
            val server =
                HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                    createContext("/") { ex ->
                        val bytes = body.toByteArray()
                        ex.sendResponseHeaders(status, bytes.size.toLong())
                        ex.responseBody.use { it.write(bytes) }
                    }
                    start()
                }
            return server to "http://127.0.0.1:${server.address.port}"
        }
        val (more, moreUrl) = answering(200, """{"documents":[{"id":"id:event:event::aa"}],"documentCount":1}""")
        val (exact, exactUrl) = answering(400, """{"message":"Your Vespa deployment has no content cluster 'content2', only 'content', 'content1'"}""")
        val (other, otherUrl) = answering(400, """{"message":"Your Vespa deployment has no content cluster 'content2', only 'content', 'shards'"}""")
        try {
            runBlocking {
                val e = assertFailsWith<IllegalStateException> { AuthorClusters(AuthorShards.clusters(2), http) { moreUrl }.verify() }
                assertTrue("also has 'content2'" in e.message.orEmpty(), e.message)
                AuthorClusters(AuthorShards.clusters(2), http) { exactUrl }.verify()
                assertFailsWith<IllegalStateException> { AuthorClusters(AuthorShards.clusters(2), http) { otherUrl }.verify() }
                AuthorClusters(listOf(null), http) { "http://127.0.0.1:1" }.verify() // one cluster: never asks
            }
        } finally {
            listOf(more, exact, other).forEach { it.stop(0) }
        }
    }
}
