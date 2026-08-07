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

    /** Env var selecting the container's access-log type. Unset keeps what services.xml ships. */
    const val ACCESS_LOG_ENV = "VESPA_ACCESS_LOG"

    /** Vespa's accepted `<accesslog type=…>` values. `disabled` writes nothing. */
    val ACCESS_LOG_TYPES = setOf("json", "vespa", "disabled")

    private const val SERVICES = "services.xml"
    private val ACCESS_LOG_TYPE = Regex("""(<accesslog\s+type=")[^"]*(")""")

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

        val type = accessLog?.trim()?.lowercase()?.takeIf { it.isNotEmpty() } ?: return raw
        require(type in ACCESS_LOG_TYPES) {
            "$ACCESS_LOG_ENV='$accessLog' is not a Vespa access-log type (${ACCESS_LOG_TYPES.sorted().joinToString(", ")})"
        }

        return rewrite(raw) { name, body ->
            if (name != SERVICES) {
                body
            } else {
                // A silent no-op here would be the worst outcome: the operator
                // sets the variable, the deploy succeeds, and the log keeps
                // filling the disk. Fail instead if the anchor ever moves.
                require(ACCESS_LOG_TYPE.containsMatchIn(body)) {
                    "$SERVICES has no <accesslog type=\"…\"> to configure — $ACCESS_LOG_ENV would be silently ignored"
                }
                ACCESS_LOG_TYPE.replace(body) { m -> "${m.groupValues[1]}$type${m.groupValues[2]}" }
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
