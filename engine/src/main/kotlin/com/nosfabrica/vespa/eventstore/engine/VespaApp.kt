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
package com.nosfabrica.vespa.eventstore.engine

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

/**
 * The Vespa application package (event + reputation schemas, rank profiles),
 * bundled into this jar as a zip by the build — shipped with the code so
 * schema and code can never drift.
 *
 * The package is deployed VERBATIM apart from one deployment-shaped choice:
 * whether the container writes an access log. That cannot be a compile-time
 * constant because it depends on who is talking to the engine. A store fed by
 * its own mirroring router logs mostly itself — the write path is read-heavy,
 * so ~500 req/s of dedup existence checks become ~3.2 GB/hour of JSON nobody
 * reads — while a store answering real client queries wants exactly that
 * record. Hence [ACCESS_LOG_ENV] rather than a fork of services.xml.
 */
object VespaApp {
    /** Classpath location of the bundled package (see vespa/build.gradle.kts). */
    const val RESOURCE = "/vespa-app.zip"

    /** Env var controlling the container's access log. Unset keeps Vespa's default. */
    const val ACCESS_LOG_ENV = "VESPA_ACCESS_LOG"

    /**
     * Accepted values. Deliberately an OFF SWITCH rather than a passthrough of
     * every `<accesslog type=…>` Vespa has: configuring the log at all obliges
     * the package to restate `fileNamePattern` (Vespa fails the deploy without
     * it), so anything other than "off" means owning defaults that are better
     * inherited. A deployment that wants to shape the log should say so in
     * services.xml, where the whole element is visible.
     */
    val ACCESS_LOG_VALUES = setOf("default", "json", "disabled")

    private const val SERVICES = "services.xml"

    /** Insert as the container's first child; `<nodes>` stays last, as Vespa's examples have it. */
    private val CONTAINER_OPEN = Regex("""(<container\b[^>]*>)""")
    private val COMMENT = Regex("""<!--.*?-->""", RegexOption.DOT_MATCHES_ALL)
    private const val DISABLE_ELEMENT = """
    <!-- injected by VespaApp: VESPA_ACCESS_LOG=disabled -->
    <accesslog type="disabled" />"""

    /**
     * The zipped application package bytes, ready to POST to a Vespa config
     * server. Honors [ACCESS_LOG_ENV]; unset ships the package as built.
     */
    fun zipBytes(): ByteArray = zipBytes(System.getenv(ACCESS_LOG_ENV))

    /** [zipBytes] with the access-log choice passed in, so it is testable without the environment. */
    internal fun zipBytes(accessLog: String?): ByteArray {
        val raw =
            VespaApp::class.java.getResourceAsStream(RESOURCE)?.use { it.readBytes() }
                ?: error("bundled Vespa application package not found on the classpath ($RESOURCE) - is :engine on the runtime classpath?")

        val value = accessLog?.trim()?.lowercase()?.takeIf { it.isNotEmpty() } ?: return raw
        require(value in ACCESS_LOG_VALUES) {
            "$ACCESS_LOG_ENV='$accessLog' is not one of ${ACCESS_LOG_VALUES.sorted().joinToString(", ")}"
        }
        if (value != "disabled") return raw

        return rewrite(raw) { name, body ->
            if (name != SERVICES) {
                body
            } else {
                // A silent no-op here would be the worst outcome: the operator
                // sets the variable, the deploy succeeds, and the log keeps
                // filling the disk. Fail instead if the anchor ever moves.
                val open =
                    CONTAINER_OPEN.find(body)
                        ?: error("$SERVICES has no <container> element to configure — $ACCESS_LOG_ENV would be silently ignored")
                // Comments stripped first: services.xml documents this very
                // knob, and matching the tag name inside prose would refuse a
                // perfectly good package.
                require(!COMMENT.replace(body, "").contains("<accesslog")) {
                    "$SERVICES already declares an access log; configure it there rather than via $ACCESS_LOG_ENV"
                }
                val at = open.range.last + 1
                body.substring(0, at) + DISABLE_ELEMENT + body.substring(at)
            }
        }
    }

    /** Stream the package through [transform], applied to each entry's text by name. */
    private fun rewrite(
        zip: ByteArray,
        transform: (name: String, body: String) -> String,
    ): ByteArray {
        val out = ByteArrayOutputStream(zip.size)
        ZipOutputStream(out).use { zos ->
            ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
                var entry = zis.nextEntry
                while (entry != null) {
                    val bytes = zis.readBytes()
                    val body = transform(entry.name, bytes.decodeToString()).encodeToByteArray()
                    // A fresh entry, so the sizes and CRC are recomputed for
                    // the rewritten bytes rather than copied from the source.
                    zos.putNextEntry(ZipEntry(entry.name))
                    if (!entry.isDirectory) zos.write(body)
                    zos.closeEntry()
                    entry = zis.nextEntry
                }
            }
        }
        return out.toByteArray()
    }
}
