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
package com.nosfabrica.vespa.eventstore.engine.client

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Readers for Vespa grouping responses. Grouping output nests `group:` nodes
 * at varying depths, so each reader walks the tree rather than assuming a shape.
 */
internal object GroupingResults {
    /** The first `count()` grouping output anywhere under [node] — flat for a plain `all(output(count()))`. */
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

    /**
     * `group(tag_index) each(all(group(pubkey) each(all(group(kind) each(output(count(), sum(relevance())))))))`
     * — [EventYql.buildLinkPeople]' shape — as key -> person -> kind ->
     * (count, summed relevance), the sum null where the response carries none.
     *
     * Read by grouplist LABEL rather than by depth: [groupCounts] collects every
     * `group:` node it meets, which on this shape would fold authors and kinds
     * ("1", "7") into the same map as the keys they sit under.
     */
    fun keyPersonKinds(root: JsonObject): Map<String, Map<String, Map<Int, Pair<Int, Double?>>>> {
        val out = LinkedHashMap<String, Map<String, Map<Int, Pair<Int, Double?>>>>()
        for (keyGroup in groupsUnder(root, "tag_index")) {
            val key = (keyGroup["value"] as? JsonPrimitive)?.content ?: continue
            val people = LinkedHashMap<String, Map<Int, Pair<Int, Double?>>>()
            for (personGroup in groupsUnder(keyGroup, "pubkey")) {
                val person = (personGroup["value"] as? JsonPrimitive)?.content ?: continue
                val byKind = LinkedHashMap<Int, Pair<Int, Double?>>()
                for (kindGroup in groupsUnder(personGroup, "kind")) {
                    val kind = (kindGroup["value"] as? JsonPrimitive)?.content?.toIntOrNull() ?: continue
                    val fields = kindGroup["fields"]?.jsonObject ?: continue
                    val count = fields["count()"]?.jsonPrimitive?.intOrNull ?: continue
                    byKind[kind] = count to fields[EventYql.LINK_TRUST_SUM]?.jsonPrimitive?.doubleOrNull
                }
                if (byKind.isNotEmpty()) people[person] = byKind
            }
            if (people.isNotEmpty()) out[key] = people
        }
        return out
    }

    /**
     * [EventYql.buildNewestVersions]' shape — `group(pubkey)` -> `group(created_at)`
     * (one, the newest) -> `group(id)` — as author -> the winning (id, created_at).
     *
     * The tiebreak happens HERE: the grouping hands back every id at the newest
     * second because it cannot order groups by a string, and NIP-01 breaks the
     * tie to the lowest id ([com.nosfabrica.vespa.eventstore.engine.doc.EventDoc.NEWEST_FIRST]).
     * The newest second is re-taken as a max rather than trusted to be the only
     * group, so a response that ever carries more than `max(1)` asked for still
     * answers right. An author whose groups carry no readable value is absent —
     * "nothing to beat" — which is the safe direction for a caller that only
     * drops what a stored version provably beats.
     */
    fun newestPerAuthor(root: JsonObject): Map<String, DocRef> {
        val out = LinkedHashMap<String, DocRef>()
        for (authorGroup in groupsUnder(root, "pubkey")) {
            val author = (authorGroup["value"] as? JsonPrimitive)?.content ?: continue
            val newest =
                groupsUnder(authorGroup, "created_at")
                    .mapNotNull { g -> (g["value"] as? JsonPrimitive)?.content?.toLongOrNull()?.let { it to g } }
                    .maxByOrNull { it.first } ?: continue
            val id =
                groupsUnder(newest.second, "id")
                    .mapNotNull { (it["value"] as? JsonPrimitive)?.content }
                    .minOrNull() ?: continue
            out[author] = DocRef(id, newest.first)
        }
        return out
    }

    /** The groups of the first grouplist labelled [label] at or below [node]'s children. */
    private fun groupsUnder(
        node: JsonObject,
        label: String,
    ): List<JsonObject> =
        node["children"]?.jsonArray.orEmpty().flatMap { child ->
            val c = child.jsonObject
            if (c["label"]?.jsonPrimitive?.content == label) c["children"]?.jsonArray.orEmpty().map { it.jsonObject } else groupsUnder(c, label)
        }

    /** Every leaf `group:` node's string value under [root], with its `count()` — the distinct pubkeys of a `group(pubkey)` and their doc counts. */
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
