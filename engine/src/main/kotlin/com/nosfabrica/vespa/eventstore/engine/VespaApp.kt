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

import org.w3c.dom.Element
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import javax.xml.XMLConstants
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult

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
     * it), so anything else means owning defaults better left inherited. A
     * deployment that wants to shape the log should say so in services.xml.
     *
     * "json" is NOT accepted even though it is what Vespa writes today: it would
     * be a no-op coinciding with an upstream default, so the day that default
     * moves the variable becomes a lie the operator cannot detect.
     */
    val ACCESS_LOG_VALUES = setOf("default", "disabled")

    private const val SERVICES = "services.xml"
    private const val CONTAINER = "container"
    private const val ACCESSLOG = "accesslog"

    /**
     * The zipped application package bytes, ready to POST to a Vespa config
     * server. Honors [ACCESS_LOG_ENV]; unset ships the package as built.
     */
    fun zipBytes(): ByteArray = zipBytes(System.getenv(ACCESS_LOG_ENV))

    /**
     * [zipBytes] with the access-log choice passed in rather than read from the
     * environment — the programmatic form of [ACCESS_LOG_ENV], for an embedder
     * that configures the store in code and for the integration test that has
     * to DEPLOY the rewritten package (an env var cannot be set from inside the
     * JVM that reads it). `null` or blank means "as built".
     */
    fun zipBytes(accessLog: String?): ByteArray {
        val raw =
            VespaApp::class.java.getResourceAsStream(RESOURCE)?.use { it.readBytes() }
                ?: error("bundled Vespa application package not found on the classpath ($RESOURCE) - is :engine on the runtime classpath?")

        val value = accessLog?.trim()?.lowercase()?.takeIf { it.isNotEmpty() } ?: return raw
        require(value in ACCESS_LOG_VALUES) {
            "$ACCESS_LOG_ENV='$accessLog' is not one of ${ACCESS_LOG_VALUES.sorted().joinToString(", ")}"
        }
        if (value != "disabled") return raw

        return rewrite(raw, SERVICES, ::disableAccessLog)
    }

    /**
     * Return [servicesXml] with `<accesslog type="disabled" />` as the
     * container's first child (`<nodes>` stays last, as Vespa's examples have
     * it). Parsed rather than string-spliced: services.xml documents this knob
     * in prose, so a textual search for the tag name reads its own comment and
     * refuses a perfectly good package. The parser checks the structure that
     * actually matters — this container has no accesslog CHILD.
     *
     * The serializer reformats: comments and indentation survive, but the XML
     * declaration is rewritten and attribute quoting normalized, so the deployed
     * services.xml is not byte-equal to `engine/app/services.xml`. Only Vespa
     * reads it, and the unset path ships the package untouched.
     */
    internal fun disableAccessLog(servicesXml: String): String {
        val doc =
            DocumentBuilderFactory
                .newInstance()
                .apply {
                    // Our own file, but the parser is hardened anyway: nothing
                    // in an application package needs a doctype or an entity.
                    setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
                    setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
                    isXIncludeAware = false
                    isExpandEntityReferences = false
                }.newDocumentBuilder()
                .parse(ByteArrayInputStream(servicesXml.encodeToByteArray()))

        // A silent no-op here would be the worst outcome: the operator sets the
        // variable, the deploy succeeds, and the log keeps filling the disk.
        // Fail instead if the element this hangs off ever moves.
        val containers = doc.getElementsByTagName(CONTAINER)
        val container =
            when (containers.length) {
                1 -> containers.item(0) as Element

                0 -> error("$SERVICES has no <$CONTAINER> element to configure — $ACCESS_LOG_ENV would be silently ignored")

                // Which one to disable is a real choice, not a default to guess.
                else -> error("$SERVICES declares ${containers.length} <$CONTAINER> elements — $ACCESS_LOG_ENV cannot pick one")
            }
        require(container.getElementsByTagName(ACCESSLOG).length == 0) {
            "$SERVICES already declares an access log; configure it there rather than via $ACCESS_LOG_ENV"
        }

        // Indentation is written by hand rather than by the serializer: it only
        // has to make the injected pair read like the file around it, and
        // OutputKeys.INDENT would reflow every other element to get there.
        val first = container.firstChild
        container.insertBefore(doc.createTextNode("\n    "), first)
        container.insertBefore(doc.createComment(" injected by VespaApp: $ACCESS_LOG_ENV=disabled "), first)
        container.insertBefore(doc.createTextNode("\n    "), first)
        container.insertBefore(doc.createElement(ACCESSLOG).apply { setAttribute("type", "disabled") }, first)

        val out = ByteArrayOutputStream(servicesXml.length + 128)
        TransformerFactory
            .newInstance()
            .apply { setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true) }
            .newTransformer()
            .transform(DOMSource(doc), StreamResult(out))
        return out.toByteArray().decodeToString()
    }

    /**
     * Copy [zip] entry by entry, passing only [target] through [transform].
     * Every other entry is copied as RAW BYTES — decoding them as text would
     * silently replace anything that is not valid UTF-8 with U+FFFD, and the
     * package carries schemas, not just this one file.
     */
    private fun rewrite(
        zip: ByteArray,
        target: String,
        transform: (body: String) -> String,
    ): ByteArray {
        val out = ByteArrayOutputStream(zip.size)
        ZipOutputStream(out).use { zos ->
            ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
                var entry = zis.nextEntry
                while (entry != null) {
                    val bytes = zis.readBytes()
                    val body = if (entry.name == target) transform(bytes.decodeToString()).encodeToByteArray() else bytes
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
