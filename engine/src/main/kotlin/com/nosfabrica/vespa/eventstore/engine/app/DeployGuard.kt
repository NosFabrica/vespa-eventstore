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

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.system.exitProcess

/**
 * The deploy guard ([SchemaDeployer.checkShardChange]) as a command, for a deployment whose
 * packages are not deployed by the store: an operator-owned package POSTed by a script, as
 * production's is (brainstorm-k8s' `vespa-relay` chart, a Helm hook Job). It ships in the engine
 * jar, so any image carrying the store runs it, with every jar of the store on the class path:
 * `java -cp <the jars> com.nosfabrica.vespa.eventstore.engine.app.DeployGuard --config-url
 * http://vespa-0:19071 --query-url http://vespa-0:8080 /work/app` (the exact command, wildcard
 * and all, is in `docs/author-shards-production.md`, "The guard on production").
 *
 * The package is a directory (an application package's files) or a `.zip`. It is compared with
 * what the config server has ACTIVE, read-only, and refused when deploying it would delete
 * documents: a content cluster keeping fewer author buckets over data, or a content node holding
 * data (not `retired="true"` in the live package) dropped. The second matters to a one-cluster
 * deployment too: at redundancy 1, lowering a node count deletes that node's share of the corpus,
 * and nothing in Vespa stops it. `--allow-narrowing` is the bucket move's cut-over, given only
 * after its diff is clean, and excuses buckets only, never nodes.
 *
 * Exit status: 0 the package may deploy, 2 refused (the reason on stderr), 1 anything else (bad
 * arguments, an unreadable package): a script deploys only on 0.
 */
object DeployGuard {
    @JvmStatic
    fun main(args: Array<String>) {
        exitProcess(run(args, System.out, System.err))
    }

    /** [main] without the exit, for tests: the status it would exit with. */
    fun run(
        args: Array<String>,
        out: PrintStream,
        err: PrintStream,
    ): Int {
        var configUrl: String? = null
        var queryUrl: String? = null
        var allowNarrowing = false
        var path: String? = null
        var i = 0
        while (i < args.size) {
            when (val a = args[i]) {
                "--config-url" -> configUrl = args.getOrNull(++i)
                "--query-url" -> queryUrl = args.getOrNull(++i)
                "--allow-narrowing" -> allowNarrowing = true
                else -> if (a.startsWith("--") || path != null) return usage(err, "unexpected argument '$a'") else path = a
            }
            i++
        }
        if (configUrl.isNullOrBlank() || queryUrl.isNullOrBlank() || path == null) return usage(err, "--config-url, --query-url and a package are required")
        val zip =
            try {
                packageBytes(File(path))
            } catch (e: Exception) {
                err.println("deploy guard: cannot read the package at $path: ${e.message}")
                return 1
            }
        return try {
            SchemaDeployer(configUrl).checkShardChange(queryUrl, zip, allowNarrowing)
            out.println("deploy guard: the package keeps every bucket and every node holding data: it may deploy")
            0
        } catch (e: IllegalStateException) {
            err.println("deploy guard: ${e.message}")
            2
        }
    }

    private fun usage(
        err: PrintStream,
        problem: String,
    ): Int {
        err.println("deploy guard: $problem")
        err.println("usage: DeployGuard --config-url <url> --query-url <url> [--allow-narrowing] <package dir or .zip>")
        return 1
    }

    /** [file] as an application package zip: a zip as it is, a directory zipped with paths relative to it. */
    private fun packageBytes(file: File): ByteArray {
        if (file.isFile) return file.readBytes()
        require(file.isDirectory) { "no such file or directory" }
        require(File(file, "services.xml").isFile) { "no services.xml in it" }
        val bytes = ByteArrayOutputStream()
        ZipOutputStream(bytes).use { z ->
            file.walkTopDown().filter { it.isFile }.forEach { f ->
                z.putNextEntry(ZipEntry(f.relativeTo(file).invariantSeparatorsPath))
                z.write(f.readBytes())
                z.closeEntry()
            }
        }
        return bytes.toByteArray()
    }
}
