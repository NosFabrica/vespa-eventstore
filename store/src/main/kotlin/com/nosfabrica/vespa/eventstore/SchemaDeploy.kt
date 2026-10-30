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

import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import java.time.Duration

/**
 * WHAT [VespaEventStore.open] (or [VespaEventStore.deploySchema]) DOES WITH THE
 * BUNDLED APPLICATION PACKAGE — the schema ships with this code, and a
 * deployment decides how it reaches Vespa.
 *
 * `open()` used to know one answer, deploy-if-absent, so vespa-relay replaced
 * it with its own deploy (compare, deploy on a difference, wait for every
 * service to converge, tolerate a failure while an old schema serves) built on
 * the engine's deployer, and its read-only sync with its own wait. Each mode
 * below is one of those, here.
 */
enum class SchemaDeploy {
    /**
     * Deploy only when NO application serves, then wait until one does. A
     * Vespa already serving any package is left alone, so a schema upgrade is
     * someone else's explicit act. `open()`'s default, as it always was.
     */
    IF_ABSENT,

    /**
     * Deploy unless Vespa already has THIS build's package active, file for
     * file (~80 ms to compare, where a needless redeploy took 2-3 s and a new
     * config generation every service reloads). After a deploy, wait until
     * every service runs the new generation: activation returns before the
     * content nodes do, and a write carrying a field the old generation lacks
     * is refused in that gap. Unchanged still owes that wait when an earlier
     * process died waiting on its own deploy, which is
     * [SchemaDeployment.awaitConvergence]. For the one process that OWNS the
     * schema (a relay that is the only writer).
     */
    IF_CHANGED,

    /** Touch nothing and wait for nothing: an operator deploys the package (a multi-node cluster, split containers). */
    NEVER,

    /**
     * Deploy nothing, but wait until SOME application serves: for a process
     * that never owns the schema (a reader, a second process beside the
     * writer). A package deploys whole, so a second process deploying its own
     * build's would replace the owner's, and the owner's writes carrying fields
     * that package lacks would be refused until its next boot.
     */
    AWAIT,
}

/**
 * What a [SchemaDeploy] did. [deployed] is whether a package was posted;
 * [awaitConvergence] is the wait an UNCHANGED [SchemaDeploy.IF_CHANGED] still
 * owes before the first write, returned rather than paid so a caller can run
 * it beside its other boot steps (`VespaEventStore.open` pays it inline).
 */
class SchemaDeployment internal constructor(
    val mode: SchemaDeploy,
    /** A package was posted to the config server by this call. */
    val deployed: Boolean,
    /** The config generation a deploy activated, when its response said; null when nothing was deployed. */
    val generation: Long?,
    /** Whether every service reported [generation] before the wait ran out; null when nothing was waited on. */
    val converged: Boolean?,
    private val deployer: SchemaDeployer?,
) {
    /**
     * Wait until every Vespa service reports the generation the config server
     * wants (`serviceconverge`) — one read when they have, the common case.
     * Only an unchanged [SchemaDeploy.IF_CHANGED] has anything to wait for
     * here (a deploy already waited); every other result answers true at once.
     * False on [timeout], without throwing: a node that never takes the
     * generation is the operator's, and the previous one keeps serving.
     */
    fun awaitConvergence(timeout: Duration = UNCHANGED_CONVERGE_WAIT): Boolean = if (mode == SchemaDeploy.IF_CHANGED && !deployed && deployer != null) deployer.awaitConverged(null, timeout) else true

    override fun toString(): String = "SchemaDeployment($mode, deployed=$deployed, generation=$generation, converged=$converged)"

    companion object {
        /** How long an unchanged package waits for services still converging: the tail of a deploy an earlier boot already waited on. */
        val UNCHANGED_CONVERGE_WAIT: Duration = Duration.ofSeconds(30)
    }
}

/**
 * A deploy that FAILED, with the one fact a caller needs to decide whether
 * that is fatal: [serving], whether Vespa still serves an application. The
 * store does not decide for it — an unreachable config server must not take
 * down a process that ran fine yesterday on the schema Vespa already has, but
 * on a fresh Vespa there is nothing to run on, and only the caller knows which
 * failure it can live with. Tolerating one means writes carrying fields the
 * served schema lacks are refused until a deploy succeeds.
 */
class SchemaDeployException(
    message: String,
    cause: Throwable?,
    /** Whether an application (the old schema) still serves at the query endpoint. */
    val serving: Boolean,
) : IllegalStateException(message, cause)

/** [VespaEventStore.deploySchema]'s body: the deployer's primitives in the order [mode] asks for. */
internal fun runSchemaDeploy(
    deployer: SchemaDeployer,
    url: String,
    mode: SchemaDeploy,
    accessLog: String?,
    awaitTimeout: Duration,
): SchemaDeployment {
    when (mode) {
        SchemaDeploy.NEVER -> {
            return SchemaDeployment(mode, deployed = false, generation = null, converged = null, deployer = null)
        }

        SchemaDeploy.AWAIT -> {
            val deadline = System.nanoTime() + awaitTimeout.toNanos()
            while (!deployer.isServing(url)) {
                check(System.nanoTime() < deadline) {
                    "no application served at $url within $awaitTimeout. This process never deploys the schema (SchemaDeploy.AWAIT): " +
                        "start the process that owns it, or deploy your own package, and this one will find it"
                }
                Thread.sleep(AWAIT_POLL.toMillis())
            }
            return SchemaDeployment(mode, deployed = false, generation = null, converged = null, deployer = null)
        }

        SchemaDeploy.IF_ABSENT, SchemaDeploy.IF_CHANGED -> {
            val serving = deployer.isServing(url)
            if (serving && mode == SchemaDeploy.IF_ABSENT) return SchemaDeployment(mode, deployed = false, generation = null, converged = null, deployer = null)
            // Built only when a deploy is possible: building it validates the access-log choice.
            val zip = VespaApp.zipBytes(accessLog)
            if (serving && deployer.serves(zip)) return SchemaDeployment(mode, deployed = false, generation = null, converged = null, deployer = deployer)
            return try {
                val generation = SchemaDeployer.generationOf(deployer.deploy(zip))
                val converged = if (mode == SchemaDeploy.IF_CHANGED) deployer.awaitConverged(generation) else null
                deployer.awaitServing(url, poll = AWAIT_POLL)
                SchemaDeployment(mode, deployed = true, generation = generation, converged = converged, deployer = deployer)
            } catch (e: Exception) {
                throw SchemaDeployException("schema deploy (${mode.name}) to Vespa failed: ${e.message?.take(300)}", e, serving = deployer.isServing(url))
            }
        }
    }
}

/** How often a wait re-asks whether an application serves: a quarter second is a boot's patience, not a load. */
private val AWAIT_POLL: Duration = Duration.ofMillis(250)
