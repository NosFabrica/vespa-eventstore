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

import com.nosfabrica.vespa.eventstore.engine.query.FuzzyWordGroup
import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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

    private fun entry(
        name: String,
        zip: ByteArray = VespaApp.zipBytes(),
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

    @Test
    fun `the shipped package declares an access log explicitly`() {
        // Left to Vespa's default, access logging is on and invisible in the
        // package — which is how it grew to 3.2 GB/hour unnoticed.
        assertTrue(
            """<accesslog type="json"""" in entry("services.xml"),
            "services.xml must declare <accesslog> so the choice is reviewable and VESPA_ACCESS_LOG has an anchor",
        )
    }

    @Test
    fun `VESPA_ACCESS_LOG rewrites the type and leaves the rest of the package alone`() {
        val off = VespaApp.zipBytes("disabled")
        assertTrue("""<accesslog type="disabled" """ in entry("services.xml", off), "the type must be swapped")
        assertTrue(""""json"""" !in entry("services.xml", off), "the old type must not survive")
        // The rewrite streams every entry through a new zip; the schemas must
        // come out the other side byte-identical or the deploy is broken.
        assertEquals(entries(VespaApp.zipBytes()), entries(off), "rewriting must not add or drop entries")
        assertEquals(entry("schemas/event.sd"), entry("schemas/event.sd", off), "only services.xml may change")
    }

    @Test
    fun `an unset or blank VESPA_ACCESS_LOG ships the package as built`() {
        assertContentEquals(VespaApp.zipBytes(), VespaApp.zipBytes(null), "unset must not rewrite")
        assertContentEquals(VespaApp.zipBytes(), VespaApp.zipBytes("   "), "blank must not rewrite")
    }

    @Test
    fun `an unknown VESPA_ACCESS_LOG fails loudly rather than deploying something else`() {
        val e = assertFailsWith<IllegalArgumentException> { VespaApp.zipBytes("off") }
        assertTrue("disabled" in (e.message ?: ""), "the error must name the valid types: ${e.message}")
    }

    @Test
    fun `the application package is on the classpath and carries the schemas`() {
        val names = entries(VespaApp.zipBytes())
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
            // nor "search_primary" by "search_primary_parts".
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
}
