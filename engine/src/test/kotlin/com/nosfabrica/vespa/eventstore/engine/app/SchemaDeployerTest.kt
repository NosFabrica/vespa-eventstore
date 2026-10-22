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
package com.nosfabrica.vespa.eventstore.engine.app

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** [SchemaDeployer] against a config server and container faked with the JDK's own HTTP server. */
class SchemaDeployerTest {
    private val pkg = VespaApp.zipBytes(null)

    /** What the fake config server holds ACTIVE, path to bytes. */
    @Volatile private var active: Map<String, ByteArray> = SchemaDeployer.files(pkg)

    /** The host the fake config server names in its content listing; null names itself. */
    @Volatile private var listingHost: String? = null

    @Volatile private var current = 15L

    @Volatile private var wanted = 15L

    private val deploys = AtomicInteger()

    /** Generations the services advance by per converge poll after a deploy, so the wait is observable. */
    private val polls = AtomicInteger()

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                val path = ex.requestURI.path
                val (status, body) =
                    when {
                        path == "/ApplicationStatus" -> {
                            200 to "{}".toByteArray()
                        }

                        path.endsWith("/prepareandactivate") -> {
                            deploys.incrementAndGet()
                            wanted += 1
                            active = SchemaDeployer.files(ex.requestBody.readBytes())
                            200 to """{"message":"Session $wanted prepared and activated.","session-id":"$wanted","activated":true}""".toByteArray()
                        }

                        path.endsWith("/serviceconverge") -> {
                            // The services catch up a poll or two after the activation, as Vespa's do.
                            if (current < wanted && polls.incrementAndGet() % 2 == 0) current += 1
                            200 to """{"currentGeneration":$current,"wantedGeneration":$wanted,"converged":${current == wanted}}""".toByteArray()
                        }

                        path.endsWith("/content/") -> {
                            val host = listingHost ?: base
                            val listing = active.keys.map { "\"$host$INSTANCE/content/$it\"" } + "\"$host$INSTANCE/content/schemas/\""
                            200 to "[${listing.joinToString(",")}]".toByteArray()
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

    private val deployer = SchemaDeployer(base)

    @AfterTest
    fun stop() = server.stop(0)

    @Test
    fun anUnchangedPackageIsNotRedeployed() {
        assertTrue(deployer.serves(pkg))
        assertFalse(deployer.deployIfChanged(base, pkg), "the active package is this one")
        assertEquals(0, deploys.get())
    }

    @Test
    fun theListingIsReadByPathWhateverHostItNames() {
        // A config server behind a proxy, or reached by another name, lists urls on its own host.
        listingHost = "http://vespa-config.internal:19071"
        assertTrue(deployer.serves(pkg), "the same files, listed under another host")
        assertTrue(SchemaDeployer("$base/").serves(pkg), "a trailing slash on the config url")
    }

    @Test
    fun aChangedFileIsDeployedAndWaitedForUntilEveryServiceRunsIt() {
        active = active + ("schemas/reputation.sd" to "schema reputation { }".toByteArray())
        assertFalse(deployer.serves(pkg))
        assertTrue(deployer.deployIfChanged(base, pkg))
        assertEquals(1, deploys.get())
        assertEquals(wanted, current, "returned only once the services reached the deployed generation")
        assertTrue(deployer.serves(pkg))
    }

    @Test
    fun aMissingOrAnExtraFileIsAChange() {
        active = active - "schemas/reputation.sd"
        assertFalse(deployer.serves(pkg), "a file the package has and Vespa lacks")
        active = SchemaDeployer.files(pkg) + ("schemas/old.sd" to "schema old { }".toByteArray())
        assertFalse(deployer.serves(pkg), "a file Vespa still has that the package dropped")
    }

    @Test
    fun aConfigServerThatCannotAnswerMeansDeploy() {
        assertFalse(SchemaDeployer("http://127.0.0.1:1").serves(pkg), "doubt deploys: a needless deploy only costs time")
    }

    @Test
    fun theDeployedGenerationIsReadFromTheResponse() {
        assertEquals(16L, SchemaDeployer.generationOf("""{"session-id":"16","activated":true}"""))
        assertEquals(null, SchemaDeployer.generationOf("not json"))
    }

    @Test
    fun aGenerationThatNeverArrivesTimesOutWithoutThrowing() {
        wanted = 99
        assertFalse(deployer.awaitConverged(99, timeout = Duration.ofMillis(300), poll = Duration.ofMillis(50)))
    }

    private companion object {
        const val INSTANCE = "/application/v2/tenant/default/application/default/environment/prod/region/default/instance/default"
    }
}
