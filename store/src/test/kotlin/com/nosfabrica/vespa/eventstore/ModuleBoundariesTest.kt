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

import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * THE LAYERING, ASSERTED — the one rule set nothing else can hold.
 *
 * The modules and their packages are a strict order: a leaf never reaches up,
 * `:engine` never learns about `:store`, and no internal package imports the
 * facade its callers hold. That order is what makes the tree navigable — you
 * can read `engine/text` without reading anything else, and `mapping` without
 * reading the store — and it held here purely by discipline, which is to say
 * it held until someone added an import in a hurry.
 *
 * This reads the SOURCE, not the classpath: package structure is the thing
 * being asserted, and by the time it is bytecode the packages are just names.
 * The layer table below is therefore the specification, and a package missing
 * from it fails the last test rather than being silently unconstrained — a new
 * package is a layering decision, and this is where it gets made.
 */
class ModuleBoundariesTest {
    /**
     * Where each `:engine` package sits. Lower may not import higher, so:
     * shared leaves (text/async/app), then the document shapes, then the query
     * compiler they feed, then the PORT that composes them, then the decorators
     * and implementations of that port.
     */
    private val engineLayer =
        mapOf(
            "engine.text" to 0,
            "engine.async" to 0,
            "engine.app" to 0,
            "engine.doc" to 1,
            "engine.query" to 2,
            "engine" to 3,
            "engine.metrics" to 4,
            "engine.memory" to 5,
            "engine.client" to 5,
        )

    /** `:store` packages that must stand alone: no import of any other `:store` package. */
    private val storeLeaves = setOf("runtime", "mapping")

    /** The facade types a consumer holds. Nothing the facade is built FROM may import one. */
    private val facades = setOf("NostrSemanticsStore", "VespaEventStore")

    /**
     * NO EXEMPTIONS — and that is the assertion, not an omission. The one that
     * used to be here was the bulk mixed path building a whole
     * `NostrSemanticsStore` over its replay snapshot; it now shares
     * `EventAdmission` with the per-event path instead, so the cycle is gone
     * rather than declared. Anything added here should come with the same
     * plan for removing it.
     */
    private val facadeExemptions = emptySet<String>()

    /**
     * The module graph makes this unbuildable today — `:engine` has no
     * dependency on `:store`, so such an import would not compile. That is
     * exactly why the rule is written down: the day someone adds the
     * dependency to `engine/build.gradle.kts` for one convenient type, the
     * compiler goes quiet and this does not.
     */
    @Test
    fun `the engine never imports the store`() {
        val leaks =
            sources("engine").filter { (_, file) ->
                imports(file).any { it.startsWith(OWN) && !it.removePrefix("$OWN.").startsWith("engine.") }
            }
        assertTrue(leaks.isEmpty(), "the engine layer must not know the store exists: ${leaks.map { it.first }}")
    }

    @Test
    fun `engine packages import only their own layer or below`() {
        for ((name, file) in sources("engine")) {
            val from = engineLayer[packageOf(file).removePrefix("$OWN.")] ?: continue
            for (imported in enginePackagesIn(file)) {
                val to = engineLayer[imported] ?: fail("$name imports $imported, which the layer table does not name")
                assertTrue(to <= from, "$name (layer $from) imports $imported (layer $to) — that is upward")
            }
        }
    }

    @Test
    fun `the store's leaf packages import no other store package`() {
        for ((name, file) in sources("store")) {
            val own = packageOf(file).removePrefix("$OWN.")
            if (own !in storeLeaves) continue
            val reached = storePackagesIn(file).filter { it != own }
            assertTrue(reached.isEmpty(), "$name is in leaf package '$own' but imports $reached")
        }
    }

    @Test
    fun `nothing below the facade imports the facade`() {
        for ((name, file) in sources("store")) {
            if (packageOf(file) == OWN || name in facadeExemptions) continue
            val held = imports(file).map { it.substringAfterLast('.') }.filter { it in facades }
            assertTrue(held.isEmpty(), "$name imports the facade ($held); the facade is composed OF these packages")
        }
    }

    @Test
    fun `every engine package is named by the layer table`() {
        val unnamed = sources("engine").map { packageOf(it.second).removePrefix("$OWN.") }.toSet() - engineLayer.keys
        assertTrue(unnamed.isEmpty(), "new engine package(s) $unnamed — place them in the layer table, deliberately")
    }

    private fun sources(module: String): List<Pair<String, File>> {
        val root = File(module, "src/main/kotlin/com/nosfabrica/vespa/eventstore").let { if (it.isDirectory) it else File("..", it.path) }
        assertTrue(root.isDirectory, "cannot find $module sources at ${root.absolutePath}")
        return root
            .walkTopDown()
            .filter { it.extension == "kt" }
            .map { it.relativeTo(root).path to it }
            .toList()
    }

    private fun packageOf(file: File): String = file.useLines { lines -> lines.first { it.startsWith("package ") } }.removePrefix("package ").trim()

    private fun imports(file: File): List<String> = file.readLines().filter { it.startsWith("import ") }.map { it.removePrefix("import ").trim() }

    /** The `:engine` packages [file] imports FROM (an import names a symbol, so drop its last segment). */
    private fun enginePackagesIn(file: File): Set<String> =
        imports(file)
            .filter { it.startsWith("$OWN.engine.") }
            .map { it.removePrefix("$OWN.").substringBeforeLast('.') }
            .toSet()

    /** The `:store` packages [file] imports from — everything under the root package that is not `engine`. */
    private fun storePackagesIn(file: File): Set<String> =
        imports(file)
            .filter { it.startsWith("$OWN.") && !it.removePrefix("$OWN.").startsWith("engine.") }
            .map { it.removePrefix("$OWN.").substringBeforeLast('.', missingDelimiterValue = "") }
            .filter { it.isNotEmpty() }
            .toSet()

    private companion object {
        const val OWN = "com.nosfabrica.vespa.eventstore"
    }
}
