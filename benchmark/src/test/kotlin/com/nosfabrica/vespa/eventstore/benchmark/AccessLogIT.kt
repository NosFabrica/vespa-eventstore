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
 * BOTH CLAIMS ARE MADE ON A CLOCK, and that is not incidental. A container
 * answers `/ApplicationStatus` with 200 CONTINUOUSLY across a reconfiguration —
 * it never goes down — so `awaitServing` returns while the PREVIOUS package is
 * still the live one. The first CI run of this test failed exactly there:
 * traffic sent right after the redeploy landed on the disabled config and was
 * not logged, and the control read that as "logging is broken". So the positive
 * claim polls for the evidence itself, and the negative one drives traffic for
 * a settle window before concluding, because an impatient negative — "nothing
 * written" standing in for "nothing written YET" — is the one way this test
 * could go green while proving nothing at all.
 *
 * The same reconfiguration also DROPS traffic: the query container's endpoint
 * goes down and back up, so a request in flight is refused or cut off
 * mid-response. That is what failed the second CI run — an IOException out of
 * the polling loop, thrown by the very requests that were meant to poll through
 * the restart. A dropped request is evidence of nothing, so it is retried; what
 * is asserted instead is that each window landed traffic AT ALL, since a claim
 * about the access log over requests the container never answered is exactly
 * the vacuous kind this test is built to refuse.
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

                // Absence is asserted over the SAME window the control below is
                // given, and only after it. "Nothing was written" must not be
                // allowed to mean "nothing was written YET" — an impatient
                // negative is the one way this test could pass while proving
                // nothing at all.
                val disabled = driveFor(vespa, queryUrl, SETTLE)
                assertTrue(
                    disabled.served > 0,
                    "no request was answered during the $SETTLE window, so there was no traffic to log or not log — " +
                        "the absence assertion below would be vacuous",
                )
                assertEquals(
                    "",
                    disabled.logs,
                    "VESPA_ACCESS_LOG=disabled must write no access log, but Vespa wrote one",
                )

                // 3. THE POSITIVE CONTROL. Same container, same traffic, same
                //    place looked at — only the package differs. If this does
                //    not fire, the assertion above proved nothing.
                deployer.deploy(VespaApp.zipBytes(null))
                // NOT awaitServing: /ApplicationStatus answers 200 CONTINUOUSLY
                // across a config change, so it returns while the container is
                // still running the previous package — traffic sent then lands
                // on the disabled config and is not logged. That race is what
                // failed this test on its first CI run. Poll for the evidence
                // itself instead, which is the only thing that settles it.
                val control = awaitAccessLog(vespa, queryUrl)
                assertTrue(
                    control.logs.isNotBlank(),
                    "the package as built must still log, and did not within $LOG_TIMEOUT (${control.served} requests " +
                        "answered in that window) — if that is genuine rather than slow, the disabled assertion above " +
                        "is vacuous (Vespa's default moved, or the log is no longer under $LOG_DIR)",
                )
            }
    }

    /**
     * The outcome of a traffic-driving window: what was logged, and how much of
     * the traffic the container actually answered. Both halves are needed —
     * either claim made over requests that never landed says nothing about the
     * access log, so [served] is what keeps the claim from being vacuous.
     */
    private data class Drive(
        /** Access-log files found at the end of the window, newline-separated; "" when none. */
        val logs: String,
        /** How many of the requests sent came back with a status code. */
        val served: Int,
    )

    /**
     * Send traffic for [window], then report what has been logged. Used for the
     * NEGATIVE claim, where there is nothing to poll for — absence only means
     * something after giving the container as long to write as the positive
     * control gets.
     */
    private fun driveFor(
        vespa: GenericContainer<*>,
        queryUrl: String,
        window: Duration,
    ): Drive {
        val deadline = System.nanoTime() + window.toNanos()
        var served = 0
        do {
            repeat(REQUESTS) { if (tryQuery(queryUrl) != null) served++ }
            Thread.sleep(POLL.toMillis())
        } while (System.nanoTime() < deadline)
        return Drive(accessLogFiles(vespa), served)
    }

    /**
     * Send traffic until an access log appears, or [LOG_TIMEOUT] passes.
     * Polling the EVIDENCE rather than a readiness endpoint is deliberate: the
     * container answers /ApplicationStatus throughout a reconfiguration, so
     * there is nothing else that reliably says "the new package is live".
     */
    private fun awaitAccessLog(
        vespa: GenericContainer<*>,
        queryUrl: String,
    ): Drive {
        val deadline = System.nanoTime() + LOG_TIMEOUT.toNanos()
        var served = 0
        while (true) {
            repeat(REQUESTS) { if (tryQuery(queryUrl) != null) served++ }
            val files = accessLogFiles(vespa)
            if (files.isNotBlank()) return Drive(files, served)
            if (System.nanoTime() >= deadline) return Drive("", served)
            Thread.sleep(POLL.toMillis())
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
        HTTP
            .send(
                HttpRequest
                    .newBuilder(URI.create("$queryUrl/search/?yql=select+*+from+event+where+true&hits=1"))
                    .timeout(REQUEST_TIMEOUT)
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.discarding(),
            ).statusCode()

    /**
     * One search that tolerates not being answered, returning null when it was
     * not. This is the OTHER face of the reconfiguration race the polling above
     * exists for: activating a package takes the query container's HTTP endpoint
     * down and back up, so a request in flight is refused or dropped mid-response
     * ("HTTP/1.1 header parser received no bytes" — which is how this test failed
     * in CI). Such a request is not evidence about the access log in either
     * direction; it is traffic that never landed, so it is counted out and
     * retried on the next round rather than failing the run. Both callers assert
     * on [Drive.served] so a window that landed NO traffic still fails loudly.
     */
    private fun tryQuery(queryUrl: String): Int? = runCatching { query(queryUrl) }.getOrNull()

    companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** Vespa's access logs live here; the container's own is `JsonAccessLog.default`. */
        const val LOG_DIR = "/opt/vespa/logs"

        /** Requests per round. A working access log writes one line each, so any round is enough to show up. */
        const val REQUESTS = 10

        /** Gap between rounds. */
        val POLL: Duration = Duration.ofSeconds(3)

        /** How long the NEGATIVE claim drives traffic before concluding nothing is being logged. */
        val SETTLE: Duration = Duration.ofSeconds(30)

        /** How long the positive control waits for the reconfiguration to take effect. Generous: it is a restart. */
        val LOG_TIMEOUT: Duration = Duration.ofMinutes(3)

        /**
         * Per-request cap. A container mid-reconfiguration can accept a connection
         * and then answer nothing; without this, one such request would sit on the
         * default (unbounded) response timeout and eat the polling window it was
         * supposed to be probing.
         */
        val REQUEST_TIMEOUT: Duration = Duration.ofSeconds(10)

        /**
         * One client for the whole test. `HttpClient.newHttpClient()` per request
         * would spawn a selector thread and pool per call — hundreds of them over
         * a three-minute poll, all held until GC.
         */
        val HTTP: HttpClient =
            HttpClient
                .newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build()

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
