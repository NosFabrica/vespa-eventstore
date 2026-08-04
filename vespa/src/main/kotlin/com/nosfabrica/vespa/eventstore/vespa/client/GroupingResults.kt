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
package com.nosfabrica.vespa.eventstore.vespa.client

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Readers for Vespa grouping responses. Grouping output nests `group:` nodes
 * at varying depths, so each reader walks the tree rather than assuming a shape.
 */
internal object GroupingResults {
    /** The first `count()` grouping output anywhere under [node] — flat for a plain count, nested under the group list for a distinct count. */
    fun firstCount(node: JsonElement): Int? =
        when (node) {
            is JsonObject -> {
                node["fields"]
                    ?.jsonObject
                    ?.get("count()")
                    ?.jsonPrimitive
                    ?.intOrNull
                    ?: node["children"]?.let { firstCount(it) }
            }

            is JsonArray -> {
                node.firstNotNullOfOrNull { firstCount(it) }
            }

            else -> {
                null
            }
        }

    /** Every leaf group's (int value -> count()) pair anywhere under [node] — the kind histogram. */
    fun intCountsInto(
        node: JsonElement,
        out: MutableMap<Int, Int>,
    ) {
        when (node) {
            is JsonObject -> {
                val value = node["value"]?.jsonPrimitive?.intOrNull
                val count =
                    node["fields"]
                        ?.jsonObject
                        ?.get("count()")
                        ?.jsonPrimitive
                        ?.intOrNull
                if (value != null && count != null) out[value] = count
                node["children"]?.let { intCountsInto(it, out) }
            }

            is JsonArray -> {
                node.forEach { intCountsInto(it, out) }
            }

            else -> {}
        }
    }

    /** Every leaf `group:` node's string value under [root] — e.g. the distinct pubkeys of a `group(pubkey)`. */
    fun groupValues(root: JsonObject): LinkedHashSet<String> {
        val values = LinkedHashSet<String>()

        fun collect(node: JsonObject) {
            (node["value"] as? JsonPrimitive)?.let { if (node["id"]?.jsonPrimitive?.content?.startsWith("group:") == true) values += it.content }
            node["children"]?.jsonArray?.forEach { collect(it.jsonObject) }
        }
        collect(root)
        return values
    }

    /** [groupValues] keeping each group's `count()` — the same tree, value -> doc count. */
    fun groupCounts(root: JsonObject): LinkedHashMap<String, Int> {
        val out = LinkedHashMap<String, Int>()

        // Leaf `group:` nodes only — an intermediate node carrying an aggregate
        // can never be mistaken for a group value.
        fun collect(node: JsonObject) {
            if (node["id"]?.jsonPrimitive?.content?.startsWith("group:") == true) {
                val value = (node["value"] as? JsonPrimitive)?.content
                val count =
                    node["fields"]
                        ?.jsonObject
                        ?.get("count()")
                        ?.jsonPrimitive
                        ?.intOrNull
                if (value != null && count != null) out[value] = count
            }
            node["children"]?.jsonArray?.forEach { collect(it.jsonObject) }
        }
        collect(root)
        return out
    }
}
