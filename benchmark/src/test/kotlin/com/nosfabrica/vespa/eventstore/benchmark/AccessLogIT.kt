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
package com.nosfabrica.vespa.eventstore.benchmark

import com.nosfabrica.vespa.eventstore.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.VespaApp
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `VESPA_ACCESS_LOG=disabled` against a REAL Vespa — the one thing no unit test
 * can reach, and the exact gap that let a broken version ship.
 *
 * THE HISTORY THIS EXISTS FOR: the first attempt declared `<accesslog
 * type="json" />` in services.xml and every IT failed, because Vespa demands a
 * `fileNamePattern` the moment an access log is configured at all. `./gradlew
 * build` was green throughout — it validates the package's SHAPE, and only a
 * config server validates its MEANING. The second attempt (a regex splice)
 * then broke on its own guard reading the prose in services.xml. Twice bitten,
 * and the ITs still could not have caught either: they all call
 * [VespaApp.zipBytes] with the variable unset, which early-returns the package
 * as built and never touches the rewrite.
 *
 * So this deploys the REWRITTEN package, which is what the other six do not:
 *
 *  - `zipBytes("disabled")` prepares and activates (no fileNamePattern demand),
 *    and the application actually SERVES afterwards — the schemas survived the
 *    zip round-trip and the injected element did not cost the container its
 *    `<search>` or `<document-api>`;
 *  - no access log is written for traffic the container answers;
 *  - and then, the POSITIVE CONTROL: redeploy the package as built onto the
 *    same container and the log appears. Without it, "no file found" would pass
 *    just as well if Vespa moved its log directory or stopped logging for some
 *    unrelated reason — the negative only means something next to a positive
 *    that fires under the same assumptions.
 *
 * One container, two deploys. Tagged `integration`, excluded from the default
 * `:benchmark:test`; run with `-Pintegration` where Docker is available. Skips
 * cleanly without a daemon.
 */
@Tag("integration")
class AccessLogIT {
    @Test
    fun `the disabled package deploys, serves, and stops the access log`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the access log IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val deployer = SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}")

                // 1. The rewritten package deploys. SchemaDeployer.deploy throws
                //    on a non-2xx, so the fileNamePattern failure would land here
                //    — as it did, on all six ITs, the first time around.
                deployer.deploy(VespaApp.zipBytes("disabled"))
                deployer.awaitServing(queryUrl)

                // 2. Serving at all proves the schemas came through the zip
                //    round-trip and the container kept its <search> handler: a
                //    query is answered, not just a health endpoint.
                assertEquals(200, query(queryUrl), "the disabled package must serve queries, not just activate")
                repeat(REQUESTS) { query(queryUrl) }

                assertEquals(
                    "",
                    accessLogFiles(vespa),
                    "VESPA_ACCESS_LOG=disabled must write no access log, but Vespa wrote one after $REQUESTS requests",
                )

                // 3. THE POSITIVE CONTROL. Same container, same requests, same
                //    place looked at — only the package differs. If this does
                //    not fire, the assertion above proved nothing.
                deployer.deploy(VespaApp.zipBytes(null))
                deployer.awaitServing(queryUrl)
                repeat(REQUESTS) { query(queryUrl) }

                assertTrue(
                    accessLogFiles(vespa).isNotBlank(),
                    "the package as built must still log — if it does not, the disabled assertion above is vacuous " +
                        "(Vespa's default moved, or the log is no longer under $LOG_DIR)",
                )
            }
    }

    /**
     * Every access-log file the container has written, newline-separated, or ""
     * when there are none. `find` rather than a fixed path: the file name
     * carries a rotation timestamp, and the point is whether ANY exists.
     */
    private fun accessLogFiles(vespa: GenericContainer<*>): String =
        vespa
            .execInContainer("bash", "-c", "find $LOG_DIR -name '*AccessLog*' 2>/dev/null | sort")
            .stdout
            .trim()

    /** One search, returning the status code. The query itself is irrelevant — it is traffic to be logged or not. */
    private fun query(queryUrl: String): Int =
        HttpClient
            .newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create("$queryUrl/search/?yql=select+*+from+event+where+true&hits=1")).GET().build(),
                HttpResponse.BodyHandlers.discarding(),
            ).statusCode()

    companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** Vespa's access logs live here; the container's own is `JsonAccessLog.default`. */
        const val LOG_DIR = "/opt/vespa/logs"

        /** Enough traffic that a working access log certainly has something to write. */
        const val REQUESTS = 10

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
