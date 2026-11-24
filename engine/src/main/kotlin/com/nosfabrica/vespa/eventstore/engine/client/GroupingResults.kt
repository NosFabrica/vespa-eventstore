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
import com.nosfabrica.vespa.eventstore.engine.VersionCensus
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
import kotlinx.serialization.json.longOrNull

/**
 * Readers for Vespa grouping responses. Grouping output nests `group:` nodes
 * at varying depths, so each reader walks the tree rather than assuming a shape.
 */
internal object GroupingResults {
    /**
     * [root] with its grouping trees merged into ONE `group:root`: under author sharding
     * ([com.nosfabrica.vespa.eventstore.engine.app.AuthorShards]) the container answers a grouping
     * with one root per content cluster — measured: it merges a query's HITS across clusters, and
     * leaves the groupings side by side. Every reader here took the first count it met or let a
     * later group overwrite an earlier one, so on two clusters a COUNT read half the corpus.
     *
     * Groups are matched by id (`group:string:<value>`) under grouplists matched by label, at
     * every depth, and their outputs combined by what the aggregator is: `count()` and `sum(…)`
     * ADD, `max(…)` and `min(…)` take the extreme. Any other output (`avg`, `summary()`, …) keeps
     * the first cluster's value, since it cannot be merged from the outputs alone. Hit lists under
     * a group are concatenated, highest relevance first.
     *
     * What a merge CANNOT restore: a `max(n)` a cluster applied before answering. Each cluster keeps
     * its own top n, so a group that is n+1st everywhere is absent from the merge, and the merged
     * list keeps first-seen order rather than re-sorting (a group's `relevance` is its rank within
     * one cluster's list, not a value). Exact for every grouping the store itself runs: the
     * author-keyed ones never meet across clusters (a pubkey lives in one), and the rest are
     * uncapped counts and sums.
     *
     * One root — every one-cluster answer — is returned as it came.
     */
    fun mergeRoots(root: JsonObject): JsonObject {
        val children = root["children"] as? JsonArray ?: return root
        val roots = children.filter { (it as? JsonObject)?.isGroupingRoot() == true }
        if (roots.size < 2) return root
        val merged = roots.map { it.jsonObject }.reduce(::mergeGroup)
        val first = children.indexOfFirst { (it as? JsonObject)?.isGroupingRoot() == true }
        val rest = children.filterNot { (it as? JsonObject)?.isGroupingRoot() == true }
        val out = rest.toMutableList<JsonElement>().apply { add(first.coerceAtMost(size), merged) }
        return JsonObject(root + ("children" to JsonArray(out)))
    }

    private fun JsonObject.isGroupingRoot(): Boolean = (this["id"] as? JsonPrimitive)?.content?.startsWith("group:root") == true

    private fun JsonObject.idOf(): String? = (this["id"] as? JsonPrimitive)?.content

    /** Two groups with the same id: outputs combined, child lists merged by id. */
    private fun mergeGroup(
        a: JsonObject,
        b: JsonObject,
    ): JsonObject {
        val out = LinkedHashMap<String, JsonElement>(a)
        val fa = a["fields"] as? JsonObject
        val fb = b["fields"] as? JsonObject
        if (fa != null || fb != null) out["fields"] = mergeOutputs(fa ?: JsonObject(emptyMap()), fb ?: JsonObject(emptyMap()))
        val ca = (a["children"] as? JsonArray).orEmpty()
        val cb = (b["children"] as? JsonArray).orEmpty()
        if (ca.isNotEmpty() || cb.isNotEmpty()) out["children"] = JsonArray(mergeLists(ca, cb))
        return JsonObject(out)
    }

    /** A group's children are grouplists and hitlists, each matched by id; a list's members merge by id too. */
    private fun mergeLists(
        a: List<JsonElement>,
        b: List<JsonElement>,
    ): List<JsonElement> {
        val byId = LinkedHashMap<String, JsonObject>()
        val anonymous = ArrayList<JsonElement>()
        for (element in a + b) {
            val node = element as? JsonObject
            val id = node?.idOf()
            if (node == null || id == null) {
                anonymous += element
                continue
            }
            val seen = byId[id]
            byId[id] =
                when {
                    seen == null -> node
                    id.startsWith("hitlist:") -> mergeHitList(seen, node)
                    id.startsWith("grouplist:") -> mergeGroupList(seen, node)
                    else -> mergeGroup(seen, node)
                }
        }
        return byId.values.toList<JsonElement>() + anonymous
    }

    private fun mergeGroupList(
        a: JsonObject,
        b: JsonObject,
    ): JsonObject {
        val out = LinkedHashMap<String, JsonElement>(a)
        out["children"] = JsonArray(mergeLists((a["children"] as? JsonArray).orEmpty(), (b["children"] as? JsonArray).orEmpty()))
        return JsonObject(out)
    }

    private fun mergeHitList(
        a: JsonObject,
        b: JsonObject,
    ): JsonObject {
        val hits = (a["children"] as? JsonArray).orEmpty() + (b["children"] as? JsonArray).orEmpty()
        val out = LinkedHashMap<String, JsonElement>(a)
        out["children"] = JsonArray(hits.sortedByDescending { ((it as? JsonObject)?.get("relevance") as? JsonPrimitive)?.doubleOrNull ?: Double.NEGATIVE_INFINITY })
        return JsonObject(out)
    }

    private fun mergeOutputs(
        a: JsonObject,
        b: JsonObject,
    ): JsonObject {
        val out = LinkedHashMap<String, JsonElement>(a)
        for ((key, vb) in b) {
            val va = a[key]
            out[key] =
                if (va == null) {
                    vb
                } else {
                    when {
                        key == "count()" || key.startsWith("sum(") -> combine(va, vb, Long::plus, Double::plus)
                        key.startsWith("max(") -> combine(va, vb, ::maxOf, ::maxOf)
                        key.startsWith("min(") -> combine(va, vb, ::minOf, ::minOf)
                        else -> va
                    }
                }
        }
        return JsonObject(out)
    }

    /** Two numeric outputs combined, kept integral when both are; anything not numeric keeps [a]. */
    private fun combine(
        a: JsonElement,
        b: JsonElement,
        whole: (Long, Long) -> Long,
        real: (Double, Double) -> Double,
    ): JsonElement {
        val pa = a as? JsonPrimitive ?: return a
        val pb = b as? JsonPrimitive ?: return a
        if (pa.isString || pb.isString) return a
        val la = pa.longOrNull
        val lb = pb.longOrNull
        if (la != null && lb != null) return JsonPrimitive(whole(la, lb))
        val da = pa.doubleOrNull ?: return a
        val db = pb.doubleOrNull ?: return a
        return JsonPrimitive(real(da, db))
    }

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
            out[author] = newestIn(authorGroup) ?: continue
        }
        return out
    }

    /**
     * [EventYql.buildVersionCensus]' shape: [newestPerAuthor]'s, with each
     * author group's `count()` — author -> (its winner, how many versions it
     * holds). An author whose count or winner is unreadable is absent, and the
     * caller reads that author in full.
     */
    fun versionCensus(root: JsonObject): Map<String, VersionCensus> {
        val out = LinkedHashMap<String, VersionCensus>()
        for (authorGroup in groupsUnder(root, "pubkey")) {
            val author = (authorGroup["value"] as? JsonPrimitive)?.content ?: continue
            val count =
                authorGroup["fields"]
                    ?.jsonObject
                    ?.get("count()")
                    ?.jsonPrimitive
                    ?.intOrNull ?: continue
            out[author] = VersionCensus(newestIn(authorGroup) ?: continue, count)
        }
        return out
    }

    /** The winning (id, created_at) under one author group: the newest second, its lowest id. */
    private fun newestIn(authorGroup: JsonObject): DocRef? {
        val newest =
            groupsUnder(authorGroup, "created_at")
                .mapNotNull { g -> (g["value"] as? JsonPrimitive)?.content?.toLongOrNull()?.let { it to g } }
                .maxByOrNull { it.first } ?: return null
        val id =
            groupsUnder(newest.second, "id")
                .mapNotNull { (it["value"] as? JsonPrimitive)?.content }
                .minOrNull() ?: return null
        return DocRef(id, newest.first)
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
