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

import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.FuzzyWordGroup
import org.w3c.dom.Element
import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream
import javax.xml.parsers.DocumentBuilderFactory
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** The Vespa application package is bundled into the jar and deployable — the deploy artifact ships with the code. */
class VespaAppTest {
    private fun entries(zip: ByteArray): Set<String> =
        buildSet {
            ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
                var e = zis.nextEntry
                while (e != null) {
                    add(e.name)
                    e = zis.nextEntry
                }
            }
        }

    /**
     * The package AS BUILT. Deliberately not `zipBytes()`: the no-arg overload
     * reads VESPA_ACCESS_LOG, so every assertion built on it would quietly
     * change meaning — and the shipped-package tests would FAIL — in a shell
     * that happens to export the variable. The baseline has to be pinned.
     */
    private val shipped = VespaApp.zipBytes(null)

    private fun entry(
        name: String,
        zip: ByteArray = shipped,
    ): String {
        ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
            var e = zis.nextEntry
            while (e != null) {
                if (e.name == name) return zis.readBytes().decodeToString()
                e = zis.nextEntry
            }
        }
        error("$name is not in the bundled application package")
    }

    /**
     * Assertions go through a PARSER, not a substring search: services.xml
     * documents the VESPA_ACCESS_LOG knob in prose, and matching the tag name
     * inside that prose is precisely the bug that shipped once already.
     */
    private fun parse(xml: String): Element =
        DocumentBuilderFactory
            .newInstance()
            .newDocumentBuilder()
            .parse(ByteArrayInputStream(xml.encodeToByteArray()))
            .documentElement

    private fun Element.child(tag: String): Element? = getElementsByTagName(tag).let { if (it.length == 0) null else it.item(0) as Element }

    @Test
    fun `the shipped package leaves the access log to Vespa`() {
        // Configuring an access log at all obliges the package to restate
        // fileNamePattern — Vespa fails the deploy without one. Inheriting the
        // default is what keeps services.xml free of that.
        assertEquals(
            0,
            parse(entry("services.xml")).getElementsByTagName("accesslog").length,
            "services.xml must not declare an access log; VESPA_ACCESS_LOG=disabled injects it",
        )
    }

    @Test
    fun `VESPA_ACCESS_LOG=disabled injects the element and leaves the rest of the package alone`() {
        val off = VespaApp.zipBytes("disabled")
        val services = parse(entry("services.xml", off))

        // Inside the container, not loose in <services> — Vespa would reject it.
        val container = assertNotNull(services.child("container"), "the package must still declare a container")
        val log = assertNotNull(container.child("accesslog"), "the disable element must be injected inside <container>")
        assertEquals("disabled", log.getAttribute("type"), "only the disabled type is safe without a fileNamePattern")
        assertEquals(1, services.getElementsByTagName("accesslog").length, "exactly one, and it is the one inside <container>")

        // The rewrite copies every entry through a new zip; the schemas must
        // come out the other side byte-identical or the deploy is broken.
        assertEquals(entries(shipped), entries(off), "rewriting must not add or drop entries")
        assertEquals(entry("schemas/event.sd"), entry("schemas/event.sd", off), "only services.xml may change")
    }

    @Test
    fun `the injected package keeps the tuning that services xml exists to carry`() {
        // The serializer reformats, so this pins that reformatting is all it
        // does: an injection that dropped the proton config or the resource
        // limits would deploy a differently-behaving engine, silently.
        val shippedXml = parse(entry("services.xml"))
        val offXml = parse(entry("services.xml", VespaApp.zipBytes("disabled")))
        for (tag in listOf("document", "config", "concurrency", "numthreadspersearch", "maxtlssize", "disk", "memory", "search", "document-api", "nodes", "jvm")) {
            assertEquals(
                shippedXml.getElementsByTagName(tag).length,
                offXml.getElementsByTagName(tag).length,
                "injecting the access log must not change <$tag>",
            )
        }
        assertEquals(
            shippedXml
                .child("content")
                ?.child("config")
                ?.child("concurrency")
                ?.textContent,
            offXml
                .child("content")
                ?.child("config")
                ?.child("concurrency")
                ?.textContent,
            "feed concurrency is a measured value, not something a rewrite may touch",
        )
    }

    /**
     * The container's direct-buffer flags are load-bearing AVAILABILITY config,
     * not tuning: without them Vespa's derived 208 MiB ceiling saturates under
     * the store's own read-heavy write path and -XX:+ExitOnOutOfMemoryError
     * takes :8080 down with it (issue #77 — 8 deaths in 18 minutes on staging).
     *
     * Two ways to lose them, both silent: dropping the element from
     * services.xml, and the access-log rewrite failing to carry the attribute
     * through the DOM round-trip. Neither shows up as a failed deploy — the
     * package activates happily and the container just dies again under load,
     * weeks later, in someone else's cluster. The shipped values themselves are
     * argued in services.xml; asserted here is only that they are THERE and
     * survive, and ContainerJvmIT is where they are proven to reach the JVM.
     */
    @Test
    fun `the container declares its direct-memory flags and the rewrite carries them through`() {
        fun jvmOptions(zip: ByteArray): String =
            assertNotNull(
                parse(entry("services.xml", zip)).child("container")?.child("jvm"),
                "the container must declare <jvm> — without it Vespa's derived direct-buffer ceiling kills :8080 under load (#77)",
            ).getAttribute("options")

        val shippedOptions = jvmOptions(shipped)
        // Bounding the per-thread cache is the half that fixes the mechanism;
        // the ceiling alone would only move the wall.
        assertTrue(
            "-Djdk.nio.maxCachedBufferSize=" in shippedOptions,
            "the retained-buffer cap is what stops direct memory saturating; without it the ceiling only delays #77: $shippedOptions",
        )
        assertTrue(
            "-XX:MaxDirectMemorySize=" in shippedOptions,
            "the derived 208 MiB ceiling is too small for this workload: $shippedOptions",
        )
        assertEquals(
            shippedOptions,
            jvmOptions(VespaApp.zipBytes("disabled")),
            "disabling the access log must not cost the container its JVM options",
        )
    }

    @Test
    fun `blank and the keep-default value ship the package as built`() {
        // No `zipBytes() == zipBytes(null)` case: with the variable unset those
        // are the same call, and with it set to disabled they differ by design,
        // so the assertion is either vacuous or wrong. What the no-arg overload
        // adds — reading the environment — is not assertable from inside the
        // JVM that reads it; the integration test deploys the rewritten package
        // instead, which is the half that actually broke.
        assertContentEquals(shipped, VespaApp.zipBytes("   "), "blank must not rewrite")
        assertContentEquals(shipped, VespaApp.zipBytes("default"), "default must not rewrite")
    }

    @Test
    fun `an unknown VESPA_ACCESS_LOG fails loudly rather than deploying something else`() {
        val e = assertFailsWith<IllegalArgumentException> { VespaApp.zipBytes("off") }
        assertTrue("disabled" in (e.message ?: ""), "the error must name the valid values: ${e.message}")
    }

    @Test
    fun `json is rejected rather than silently meaning whatever Vespa defaults to`() {
        // It IS what Vespa writes today, which is the problem: accepting it
        // would be a no-op that coincides with an upstream default, so the day
        // that default moves the variable lies and nobody can tell.
        assertFailsWith<IllegalArgumentException> { VespaApp.zipBytes("json") }
    }

    @Test
    fun `a services xml that already declares an access log is refused, but prose about one is not`() {
        val declared =
            """
            <?xml version="1.0" encoding="UTF-8" ?>
            <services version="1.0">
              <container id="default" version="1.0">
                <accesslog type="vespa" fileNamePattern="logs/access" />
              </container>
            </services>
            """.trimIndent()
        val e = assertFailsWith<IllegalArgumentException> { VespaApp.disableAccessLog(declared) }
        assertTrue("already declares" in (e.message ?: ""), "the error must say why: ${e.message}")

        // The regression: the shipped services.xml explains this very knob, and
        // the tag name appearing inside that explanation used to refuse it.
        val documented =
            """
            <?xml version="1.0" encoding="UTF-8" ?>
            <services version="1.0">
              <container id="default" version="1.0">
                <!-- No <accesslog> here on purpose; VESPA_ACCESS_LOG=disabled injects one. -->
                <search />
              </container>
            </services>
            """.trimIndent()
        assertEquals("disabled", parse(VespaApp.disableAccessLog(documented)).child("accesslog")?.getAttribute("type"))
    }

    @Test
    fun `a services xml with no container fails rather than deploying an unchanged package`() {
        // The one outcome worth engineering against: the operator sets the
        // variable, the deploy succeeds, and the disk fills anyway.
        val e = assertFailsWith<IllegalStateException> { VespaApp.disableAccessLog("""<?xml version="1.0" ?><services version="1.0" />""") }
        assertTrue("container" in (e.message ?: ""), "the error must name what is missing: ${e.message}")
    }

    @Test
    fun `the application package is on the classpath and carries the schemas`() {
        val names = entries(shipped)
        assertTrue("services.xml" in names, "package must declare its services: $names")
        assertTrue("schemas/event.sd" in names, "package must carry the event schema: $names")
        assertTrue("schemas/reputation.sd" in names, "package must carry the reputation schema: $names")
    }

    /**
     * THE LADDER INVARIANT, checked statically against the SHIPPED schema:
     * every column the query builder searches must be recallable (a `fieldset
     * default` entry) AND accounted for by `real_match()`, the guard that
     * floors a genuine match at text_score_cutoff instead of letting the
     * cutoff delete it.
     *
     * This exists because the invariant used to live only in comments, and
     * adding a column is one edit while giving it a rung is a different edit
     * in a different part of a different file. It was missed three times (the
     * 2026-08-05 audit: search_text and search_location had no rung at all,
     * search_secondary's read an attribute that can withhold a word the index
     * field holds, nip05/lud16 still ride the floor). A column added to
     * SEARCH_FIELDS and nowhere else now fails the BUILD, with no Docker and
     * no Vespa — the integration gate is where the rung itself gets proven
     * (RankRegressionIT's recall-floor matrix), but this is what catches the
     * omission at the moment it is written.
     *
     * Not asserted here: that the column has its own RUNG. That is a ranking
     * property only a live engine can evaluate, and real_match() is
     * deliberately the weaker, checkable half — the net, not the ladder.
     */
    @Test
    fun `every searched column is in the default fieldset and the real-match guard`() {
        val sd = entry("schemas/event.sd")
        val fieldset = sd.substringAfter("fieldset default {").substringBefore("}")
        val guard = sd.substringAfter("function real_match() {").substringBefore("}")
        for (field in FuzzyWordGroup.SEARCH_FIELDS) {
            // Word-bounded: "name" must not be satisfied by "display_name",
            // nor "search_primary" by "search_primary_near".
            assertTrue(
                Regex("\\b${Regex.escape(field)}\\b").containsMatchIn(fieldset),
                "`$field` is searched but missing from `fieldset default` — it can never be recalled: $fieldset",
            )
            assertTrue(
                "matchCount($field)" in guard,
                "`$field` is searched but missing from real_match() — a match there is deleted by text_score_cutoff, not ranked: $guard",
            )
        }
    }

    /**
     * The near columns are named in THREE places — the schema, the query
     * builder ([FuzzyWordGroup.ALL_NEAR_FIELDS]) and the feed side
     * ([SearchFields.nearFields]) — and nothing but a real Vespa used to notice
     * when they disagreed. That is a bad failure to leave to the integration
     * gate: a query naming a column the schema lacks is a 400 the read-side
     * fallback swallows PERMANENTLY (it demotes for the life of the client, so
     * prefix and fuzzy recall just stop, silently), and a document naming one
     * is a 400 that fails the insert outright.
     *
     * So: the two Kotlin sides must agree with each other, and both with the
     * shipped schema. Renaming any one of the three now fails the build.
     */
    @Test
    fun `the near columns agree across the schema, the query builder and the feed`() {
        val sd = entry("schemas/event.sd")
        val declared = Regex("""^\s*field\s+(\w+)\s+type""", RegexOption.MULTILINE).findAll(sd).map { it.groupValues[1] }.toSet()
        // Every column the feed can write, from a SearchFields that fills all of them.
        val written =
            SearchFields(
                name = "a",
                displayName = "b",
                about = "c",
                nip05 = "d@e.f",
                lud16 = "g@h.i",
                website = "https://j.k",
                primary = "l",
                secondary = "m",
            ).nearFields()
                .keys
        assertEquals(
            FuzzyWordGroup.ALL_NEAR_FIELDS.toSet(),
            written,
            "the near columns the feed writes and the ones the query builder searches must be the same set",
        )
        for (field in written) {
            assertTrue(field in declared, "`$field` is fed and searched but not declared in event.sd: $declared")
        }
    }
}
