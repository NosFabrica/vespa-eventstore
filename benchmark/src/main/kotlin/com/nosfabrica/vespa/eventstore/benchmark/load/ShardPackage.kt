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
package com.nosfabrica.vespa.eventstore.benchmark.load

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import java.io.ByteArrayOutputStream
import java.io.File
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.system.exitProcess

/**
 * AN OPERATOR'S OWN PACKAGE, SPLIT BY AUTHOR (`docs/author-shards.md`, "A multi-node package"):
 * for a deployment that deploys its own application package (`AUTO_DEPLOY=false` in vespa-relay,
 * a multi-host cluster) rather than the store's bundled single-host one. Reads APP_DIR, writes the
 * same package to OUT_DIR with its `services.xml` split into the store's clusters
 * ([VespaApp.shardServices]), each keeping only the nodes SHARD_NODES gives it, and nothing else
 * touched (`hosts.xml`, `schemas/`, `search/`, `components/` are copied as they are).
 *
 * Then, when VESPA_CONFIG_URL is set, the deploy guard the store runs on its own deploys
 * (`SchemaDeployer.checkShardChange`) is run against the LIVE deployment for the written package:
 * a package that would make a cluster keep fewer buckets while data exists is refused here, before
 * anyone deploys it by hand, where the store's guard never sees it. Read-only: it reads the active
 * package and the document count, and deploys nothing.
 *
 * Env:
 *  - APP_DIR, OUT_DIR (must not exist or be empty);
 *  - VESPA_AUTHOR_SHARDS / VESPA_AUTHOR_BUCKETS, parsed as the store parses them;
 *  - SHARD_NODES: distribution keys per cluster, `0,1,2;3,4` (cluster `content` gets 0, 1, 2);
 *    required when the content cluster lists more than one node;
 *  - VESPA_CONFIG_URL and VESPA_URL (the query container) for the guard; unset skips it.
 */
object ShardPackage {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = System.getenv()
        val app = File(env["APP_DIR"] ?: fail("APP_DIR is required: the application package to split"))
        val out = File(env["OUT_DIR"] ?: fail("OUT_DIR is required: where the split package is written"))
        val services = File(app, "services.xml").takeIf { it.isFile } ?: fail("$app has no services.xml")
        if (out.exists() && (out.list()?.isNotEmpty() == true)) fail("$out exists and is not empty")
        val tuning = EngineTuning.fromEnv(env)
        val map = tuning.authorBuckets?.let(AuthorShards.BucketMap::parse) ?: AuthorShards.BucketMap.even(tuning.authorShards ?: 1)
        val groups = env["SHARD_NODES"]?.trim()?.takeIf { it.isNotEmpty() }?.let(VespaApp::parseNodeGroups)

        val split = VespaApp.shardServices(services.readText(), map, groups)
        app.copyRecursively(out, overwrite = false)
        File(out, "services.xml").writeText(split)
        println("wrote $out: ${map.clusters} content cluster(s), bucket map ${map.spec()}" + (groups?.let { ", nodes ${env["SHARD_NODES"]}" } ?: ""))

        val configUrl = env["VESPA_CONFIG_URL"]?.trim()?.takeIf { it.isNotEmpty() }
        if (configUrl == null) {
            println("guard: VESPA_CONFIG_URL unset, not checked against a live deployment")
            return
        }
        val queryUrl = env["VESPA_URL"]?.trim()?.takeIf { it.isNotEmpty() } ?: fail("VESPA_URL is required with VESPA_CONFIG_URL: the guard counts what the deployment holds")
        try {
            SchemaDeployer(configUrl).checkShardChange(queryUrl, zip(out))
            println("guard: this package keeps every bucket each live cluster holds: it may deploy")
        } catch (e: IllegalStateException) {
            System.err.println("guard: ${e.message}")
            exitProcess(2)
        }
    }

    /** [dir] as an application package zip, paths relative to it. */
    private fun zip(dir: File): ByteArray {
        val bytes = ByteArrayOutputStream()
        ZipOutputStream(bytes).use { z ->
            dir.walkTopDown().filter { it.isFile }.forEach { f ->
                z.putNextEntry(ZipEntry(f.relativeTo(dir).invariantSeparatorsPath))
                z.write(f.readBytes())
                z.closeEntry()
            }
        }
        return bytes.toByteArray()
    }

    private fun fail(message: String): Nothing {
        System.err.println("shardPackage: $message")
        exitProcess(1)
    }
}
