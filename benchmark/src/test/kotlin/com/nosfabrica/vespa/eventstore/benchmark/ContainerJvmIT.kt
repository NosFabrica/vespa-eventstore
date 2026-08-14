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
import java.io.ByteArrayInputStream
import java.time.Duration
import java.util.zip.ZipInputStream
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The `<jvm options>` the bundled package declares for the query container
 * REACH the JVM serving :8080, and win over the flags Vespa derives.
 *
 * WHY THIS NEEDS A REAL VESPA: nothing else can tell the fix from a no-op.
 * [com.nosfabrica.vespa.eventstore.engine.VespaAppTest] proves the attribute is
 * in services.xml and survives the access-log rewrite — but an attribute Vespa
 * ignores, or places BEFORE its own derived flags, looks identical from the unit
 * side, while the symptom it exists to prevent (issue #77: the container JVM
 * dying on direct-buffer exhaustion — 8 deaths in 18 minutes, taking every query
 * AND every feed operation down each time) only shows up in someone else's
 * cluster, weeks later, under load. Same gap `AccessLogIT` was written for: the
 * build validates the package's SHAPE, only a config server validates its
 * MEANING.
 *
 * Three claims, in the order they can fail:
 *
 *  1. **The container still boots.** A malformed JVM flag is not a deploy error
 *     — `prepareandactivate` returns 200 and the JVM then refuses to start — so
 *     it surfaces only as a container that never serves. `awaitServing` is that
 *     assertion, and it is the one that catches a typo.
 *  2. **The flags are on the command line** of the process the sentinel calls
 *     `container`: Vespa passed them through rather than dropping them.
 *  3. **Ours are the ones in effect.** HotSpot reads flags left to right and the
 *     LAST occurrence wins, so what matters is not that our
 *     `-XX:MaxDirectMemorySize` appears but that nothing appears after it —
 *     Vespa derives a ceiling of its own from the heap and emits it on the same
 *     line.
 *
 * Deliberately NOT pinned: the numbers. They and their reasoning live in
 * `engine/app/services.xml` and are read back from the shipped package here, so
 * re-tuning stays a one-file edit; the invariant under test is "what the package
 * declares is what the JVM runs". The one exception is the DIRECTION of the
 * change — a ceiling at or below Vespa's derived default is #77 again.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; skips
 * cleanly without a Docker daemon.
 */
@Tag("integration")
class ContainerJvmIT {
    @Test
    fun `the package's JVM options reach the query container and override Vespa's derived flags`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the container JVM IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val deployer = SchemaDeployer("http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}")

                // CLAIM 1. The package AS BUILT — not a rewritten one; these
                // flags have to hold on the default path everybody deploys.
                deployer.deploy(VespaApp.zipBytes(null))
                // A JVM that will not start with the options we gave it fails
                // HERE, and nowhere else: the deploy already returned 200.
                deployer.awaitServing(queryUrl)

                val cmdline = containerJvmCommandLine(vespa)

                // CLAIM 2. Both halves of the #77 fix survived the trip from
                // services.xml through the config model into the process table.
                // The cache cap is the half that fixes the MECHANISM (per-thread
                // temporary direct buffers retained at their high-water size);
                // without it the raised ceiling only moves the wall.
                assertTrue(
                    "-Djdk.nio.maxCachedBufferSize=${declared(CACHE_CAP)}" in cmdline,
                    "the retained-buffer cap never reached the JVM — direct memory saturates again (#77): $cmdline",
                )

                // CLAIM 3. Last one wins, so last one is the only one that
                // counts. If the config model ever emits a cluster's <jvm
                // options> BEFORE its own derived flags, ours become decoration
                // and this is the assertion that says so.
                val effective =
                    MAX_DIRECT
                        .findAll(cmdline)
                        .lastOrNull()
                        ?.groupValues
                        ?.get(1)
                        ?: error("no -XX:MaxDirectMemorySize on the query container's command line at all: $cmdline")
                assertEquals(
                    declared(MAX_DIRECT),
                    effective,
                    "the JVM runs a direct-memory ceiling the package did not ask for — Vespa's derived flag wins over " +
                        "<jvm options>, so the #77 fix is a no-op: $cmdline",
                )
                assertTrue(
                    mebibytes(effective) > DERIVED_CEILING_MIB,
                    "the effective ceiling ($effective) is no larger than the derived default that saturated in #77",
                )
            }
    }

    /**
     * The command line of the process serving :8080, found through Vespa's OWN
     * bookkeeping rather than by pattern-matching `ps`: this image runs several
     * JVMs (config server, cluster controller, metrics proxy) and asserting
     * against the wrong one would make every claim above quietly vacuous.
     * `vespa-sentinel-cmd list` names them, and `container` is the same service
     * whose `stopped/1` lines reported the deaths in #77.
     */
    private fun containerJvmCommandLine(vespa: GenericContainer<*>): String {
        val listing = exec(vespa, "$VESPA_HOME/bin/vespa-sentinel-cmd list")
        val pid =
            SENTINEL_PID.find(listing)?.groupValues?.get(1)
                ?: error("the sentinel runs no service named '$SERVICE' — cannot locate the query container's JVM:\n$listing")

        // /proc, not `ps`: this command line is long enough that ps truncates
        // it, and a truncated line reads exactly like a missing flag.
        //
        // The sentinel supervises a WRAPPER, which may or may not have exec'd
        // into the JVM by the time we look, so descend one level when what we
        // find is not a java process — the same two cases Vespa's own find-pid
        // handles.
        val direct = cmdlineOf(vespa, pid)
        if (JAVA.containsMatchIn(direct)) return direct
        val child =
            exec(vespa, "pgrep --parent $pid | head -1").ifBlank {
                error("pid $pid ('$SERVICE') is not a JVM and has no child to descend to: $direct")
            }
        val nested = cmdlineOf(vespa, child)
        require(JAVA.containsMatchIn(nested)) { "neither $pid nor its child $child is the container JVM: $direct / $nested" }
        return nested
    }

    private fun cmdlineOf(
        vespa: GenericContainer<*>,
        pid: String,
    ): String =
        exec(vespa, "tr '\\0' ' ' < /proc/$pid/cmdline").ifBlank {
            error("pid $pid has no command line — it exited between the sentinel listing and the read")
        }

    private fun exec(
        vespa: GenericContainer<*>,
        command: String,
    ): String {
        val res = vespa.execInContainer("bash", "-c", command)
        require(res.exitCode == 0) { "`$command` failed (${res.exitCode}) inside the Vespa container: ${res.stderr}" }
        return res.stdout.trim()
    }

    /**
     * What `engine/app/services.xml` asks for, read out of the SHIPPED package
     * rather than restated as a constant here — the claim is that the JVM runs
     * what the package declares, and restating the value would reduce that to
     * two copies of a literal agreeing with each other.
     */
    private fun declared(flag: Regex): String {
        val services =
            ZipInputStream(ByteArrayInputStream(VespaApp.zipBytes(null))).use { zis ->
                generateSequence { zis.nextEntry }
                    .firstOrNull { it.name == "services.xml" }
                    ?.let { zis.readBytes().decodeToString() }
            } ?: error("the bundled package has no services.xml")
        return flag.find(services)?.groupValues?.get(1)
            ?: error("services.xml no longer declares ${flag.pattern} for the container — #77 is unfixed:\n$services")
    }

    /** `512m` / `1g` / `536870912` as MiB, so the DIRECTION of a re-tune stays checkable. */
    private fun mebibytes(size: String): Long {
        val unit = size.last()
        if (unit.isDigit()) return size.toLong() / (1024 * 1024)
        val digits = size.dropLast(1).toLong()
        return when (unit.lowercaseChar()) {
            'k' -> digits / 1024
            'm' -> digits
            'g' -> digits * 1024
            else -> error("unrecognized size unit in '$size'")
        }
    }

    private fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071

        /** Where the official image installs Vespa (same assumption `AccessLogIT` makes about the log directory). */
        const val VESPA_HOME = "/opt/vespa"

        /** The sentinel's name for the query/feed container — the service that died in #77. */
        const val SERVICE = "container"

        /** `container state=RUNNING mode=AUTO pid=455 exitstatus=0 id="default/container.0"` */
        val SENTINEL_PID = Regex("""^$SERVICE state=\S+\s+mode=\S+\s+pid=(\d+)""", RegexOption.MULTILINE)

        val MAX_DIRECT = Regex("""-XX:MaxDirectMemorySize=(\S+)""")
        val CACHE_CAP = Regex("""-Djdk\.nio\.maxCachedBufferSize=(\S+)""")

        /** A command line belonging to a JVM rather than to the shell wrapper that starts one. */
        val JAVA = Regex("""(^|/)java\s""")

        /**
         * Vespa's derived ceiling with its default 1536m heap — measured in #77
         * as the exact limit the pool saturated against (218,103,808 bytes). A
         * fix that lands at or below it is not a fix.
         */
        const val DERIVED_CEILING_MIB = 208L
    }
}
