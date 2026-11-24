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

import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.sun.net.httpserver.HttpServer
import java.io.ByteArrayInputStream
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipInputStream
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Each [SchemaDeploy] mode against a config server and container faked with the
 * JDK's HTTP server: the four answers vespa-relay used to assemble itself from
 * the engine's deployer (compare, deploy, converge, tolerate-or-not).
 */
class SchemaDeployTest {
    @Volatile private var active: Map<String, ByteArray> = files(VespaApp.zipBytes(null))

    @Volatile private var serving = true

    @Volatile private var deployStatus = 200

    @Volatile private var generation = 15L

    /** What the faked container's `/search/` count answers: the documents the deployment holds. */
    @Volatile private var stored = 0L

    private val deploys = AtomicInteger()

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                val path = ex.requestURI.path
                val (status, body) =
                    when {
                        path == "/ApplicationStatus" -> {
                            (if (serving) 200 else 404) to "{}".toByteArray()
                        }

                        path.endsWith("/prepareandactivate") -> {
                            deploys.incrementAndGet()
                            if (deployStatus == 200) {
                                generation += 1
                                active = files(ex.requestBody.readBytes())
                                serving = true
                                200 to """{"session-id":"$generation","activated":true}""".toByteArray()
                            } else {
                                deployStatus to "config server says no".toByteArray()
                            }
                        }

                        path == "/search/" -> {
                            200 to """{"root":{"id":"toplevel","fields":{"totalCount":$stored}}}""".toByteArray()
                        }

                        path.endsWith("/serviceconverge") -> {
                            200 to """{"currentGeneration":$generation,"wantedGeneration":$generation,"converged":true}""".toByteArray()
                        }

                        path.endsWith("/content/") -> {
                            200 to "[${active.keys.joinToString(",") { "\"$base$INSTANCE/content/$it\"" }}]".toByteArray()
                        }

                        path.contains("/content/") -> {
                            active[path.substringAfter("/content/")]?.let { 200 to it } ?: (404 to ByteArray(0))
                        }

                        else -> {
                            404 to ByteArray(0)
                        }
                    }
                ex.sendResponseHeaders(status, if (body.isEmpty()) -1 else body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            start()
        }

    private val base: String by lazy { "http://127.0.0.1:${server.address.port}" }

    @AfterTest
    fun stop() = server.stop(0)

    private fun deploy(
        mode: SchemaDeploy,
        tuning: EngineTuning = EngineTuning(),
    ) = VespaEventStore.deploySchema(base, configUrl = base, mode = mode, tuning = tuning, awaitTimeout = Duration.ofMillis(600))

    private fun changeActive() {
        active = active + ("schemas/other.sd" to "schema other { }".toByteArray())
    }

    @Test
    fun `if changed leaves an unchanged package alone and still owes the convergence read`() {
        val d = deploy(SchemaDeploy.IF_CHANGED)
        assertFalse(d.deployed)
        assertEquals(0, deploys.get())
        assertTrue(d.awaitConvergence(), "the services already run the wanted generation")
    }

    @Test
    fun `if changed deploys a difference and waits for every service`() {
        changeActive()
        val d = deploy(SchemaDeploy.IF_CHANGED)
        assertTrue(d.deployed)
        assertEquals(16L, d.generation)
        assertEquals(true, d.converged)
        assertEquals(1, deploys.get())
    }

    @Test
    fun `if absent leaves any serving package alone, and deploys onto an empty Vespa`() {
        changeActive()
        assertFalse(deploy(SchemaDeploy.IF_ABSENT).deployed, "a different package serving is someone else's upgrade to make")
        serving = false
        val d = deploy(SchemaDeploy.IF_ABSENT)
        assertTrue(d.deployed)
        assertNull(d.converged, "if-absent waits for the container, not for convergence, as it always did")
    }

    @Test
    fun `never touches nothing, await waits for someone else's deploy`() {
        serving = false
        assertFalse(deploy(SchemaDeploy.NEVER).deployed)
        assertEquals(0, deploys.get())
        val e = assertFailsWith<IllegalStateException> { deploy(SchemaDeploy.AWAIT) }
        assertTrue("never deploys" in e.message!!, e.message)
        serving = true
        assertFalse(deploy(SchemaDeploy.AWAIT).deployed)
        assertEquals(0, deploys.get(), "await never deploys")
    }

    /** The store does not decide whether a failed deploy is fatal; it says whether an old schema still serves. */
    @Test
    fun `a failed deploy says whether an application still serves`() {
        changeActive()
        deployStatus = 500
        val tolerable = assertFailsWith<SchemaDeployException> { deploy(SchemaDeploy.IF_CHANGED) }
        assertTrue(tolerable.serving, "the old schema serves: the caller may run on it")
        serving = false
        val fatal = assertFailsWith<SchemaDeployException> { deploy(SchemaDeploy.IF_ABSENT) }
        assertFalse(fatal.serving, "a fresh Vespa has nothing to run on")
    }

    private companion object {
        const val INSTANCE = "/application/v2/tenant/default/application/default/environment/prod/region/default/instance/default"

        fun files(zip: ByteArray): Map<String, ByteArray> =
            buildMap {
                ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
                    var e = zis.nextEntry
                    while (e != null) {
                        if (!e.isDirectory) put(e.name, zis.readBytes())
                        e = zis.nextEntry
                    }
                }
            }
    }

    /**
     * A new author-shard count over a deployment that holds documents is refused: each cluster's
     * selection is also what garbage collection keeps, so the deploy would delete every document
     * the new selections send elsewhere. On an empty deployment the change goes through.
     */
    @Test
    fun `a different author-shard count is refused over stored documents, and deployed over none`() {
        stored = 42
        val refused = assertFailsWith<SchemaDeployException> { deploy(SchemaDeploy.IF_CHANGED, EngineTuning(authorShards = 2)) }
        assertTrue("author-shard count" in refused.message.orEmpty(), refused.message)
        assertTrue(refused.serving, "the old package keeps serving")
        assertEquals(0, deploys.get(), "nothing was deployed")

        stored = 0
        assertTrue(deploy(SchemaDeploy.IF_CHANGED, EngineTuning(authorShards = 2)).deployed, "an empty deployment may change its shard count")
        assertFalse(deploy(SchemaDeploy.IF_CHANGED, EngineTuning(authorShards = 2)).deployed, "the same count again is unchanged")

        stored = 7
        assertFailsWith<SchemaDeployException>("and back to one cluster is the same move") { deploy(SchemaDeploy.IF_CHANGED) }
        changeActive()
        assertTrue(deploy(SchemaDeploy.IF_CHANGED, EngineTuning(authorShards = 2)).deployed, "a schema change at the same count deploys over data")
    }

    /**
     * A bucket move, as the guard sees it: starting to write the moving buckets to a new cluster
     * only WIDENS what clusters keep, so it deploys over data; the cut-over NARROWS the old owner,
     * so the store refuses it — only the move tool, after verifying the copy, may pass
     * `allowNarrowing`. A schema change at an unchanged map deploys as before.
     */
    @Test
    fun `a move's overlap deploys over data, its cut-over only when allowed`() {
        active = files(VespaApp.zipBytes(null, 2))
        stored = 1_000
        val overlap = EngineTuning(authorBuckets = "00-7f=0,80-bf=1,c0-ff=1+2")
        assertTrue(deploy(SchemaDeploy.IF_CHANGED, overlap).deployed, "receiving into a new cluster deletes nothing")
        val cut = EngineTuning(authorBuckets = "00-7f=0,80-bf=1,c0-ff=2")
        val refused = assertFailsWith<SchemaDeployException> { deploy(SchemaDeploy.IF_CHANGED, cut) }
        assertTrue("'content1' would drop 64 bucket(s)" in refused.message.orEmpty(), refused.message)
        val deployer =
            com.nosfabrica.vespa.eventstore.engine.app
                .SchemaDeployer(base)
        deployer.checkShardChange(base, VespaApp.zipBytes(null, cut.bucketMap()), allowNarrowing = true)
        changeActive()
        assertTrue(deploy(SchemaDeploy.IF_CHANGED, overlap).deployed, "a schema change at the same map deploys over data")
    }
}
