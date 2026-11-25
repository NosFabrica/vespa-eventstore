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

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * What a caller's grouping pipeline needs before its per-cluster answers can be merged under
 * author sharding ([GroupingResults.mergeRoots]).
 *
 * A GROUP-LEVEL `count()` — an `output(count())` beside a `group(F)` in one `all(…)`/`each(…)`,
 * which Vespa answers with the number of distinct values of F — does not merge by adding: a value
 * present in two clusters is counted in each (vespa-relay's `distinct(kind)` read 16 for 8 kinds on
 * two clusters). It merges exactly only when the values are partitioned by cluster: when F is the
 * author (`pubkey`), or when an enclosing `each(…)` already sits under a `group(pubkey)` (the groups
 * then live inside one author, so inside one cluster).
 *
 * The one shape statistics use, `all(group(F) output(count()))`, is answered exactly instead: it
 * is run as `all(group(F) output(count()) each(output(count())))`, the groups are merged by value
 * across clusters, and their number replaces the count in the answer, which keeps the shape the
 * original pipeline has. That is EXACT where Vespa's own one-cluster answer is not: it estimates
 * (2–4% off at ~55 k pubkeys), and for a multi-value field such as `tag_index` it answered 1
 * where a cluster held 2 values (measured, `AuthorShardsIT`). It costs one group per distinct
 * value, so it is CAPPED: each cluster returns at most [DISTINCT_CAP] + 1 groups, and a merged
 * list longer than [DISTINCT_CAP] is refused rather than answered. A field with millions of
 * values (`tag_index` on production) would otherwise materialize every value in the container
 * and here, where one cluster pays nothing for the same question. Every other group-level count
 * over a non-author field is refused, as a group `max(n)` is: a merged answer would only look
 * right.
 *
 * So is an `order(…)` over groups that come from several clusters: the merge lists groups as it
 * first meets them (cluster by cluster), so "the most common kind first" could name the wrong
 * one. Only groups inside an `each(…)` under a `group(pubkey)` all come from one cluster, and
 * their order stands.
 */
internal object ShardedGrouping {
    /** How a pipeline is answered across clusters. */
    sealed interface Plan {
        /** Merges exactly as it is. */
        data object AsIs : Plan

        /** Run [pipeline] and [rewrite] its merged root ([exactDistinct]). */
        data class Distinct(
            val pipeline: String,
        ) : Plan

        /** Cannot be answered exactly: [reason] says why. */
        data class Refused(
            val reason: String,
        ) : Plan
    }

    /** The author field: its values never meet across clusters. */
    const val AUTHOR_FIELD = "pubkey"

    fun plan(pipeline: String): Plan {
        val top = calls(pipeline.trim()).singleOrNull() ?: return Plan.AsIs
        if (top.name != "all" && top.name != "each") return Plan.AsIs
        unpartitionedOrder(top, partitioned = false)?.let { field ->
            return Plan.Refused(
                "an order() over `$field` groups cannot be kept across author-shard clusters: the merged list holds each cluster's groups as they came; " +
                    "drop the order() and sort the merged groups by their outputs",
            )
        }
        distinctOf(top)?.let { field ->
            return if (field == AUTHOR_FIELD) Plan.AsIs else Plan.Distinct("all(group($field) max(${DISTINCT_CAP + 1}) output(count()) each(output(count())))")
        }
        val offender = groupLevelCount(top, authorKeyed = false) ?: return Plan.AsIs
        return Plan.Refused(
            "a count() of distinct `$offender` values cannot be answered exactly across author-shard clusters (a value present in two clusters is counted in each); " +
                "count the groups of all(group($offender) each(output(count()))) instead",
        )
    }

    /** F, when [top] is exactly `all(group(F) output(count()))`. */
    private fun distinctOf(top: Call): String? {
        if (top.name != "all") return null
        val items = calls(top.args)
        if (items.size != 2) return null
        val group = items.singleOrNull { it.name == "group" } ?: return null
        val output = items.singleOrNull { it.name == "output" } ?: return null
        if (output.args.replace(WS, "") != "count()") return null
        return group.args.trim().takeIf { it.isNotEmpty() }
    }

    /**
     * The first group expression under [scope] whose distinct values are counted while not
     * partitioned by author, or null. A scope's groups are partitioned by author when it groups by
     * [AUTHOR_FIELD] or sits in an `each(…)` of a scope that is ([authorKeyed]).
     */
    private fun groupLevelCount(
        scope: Call,
        authorKeyed: Boolean,
    ): String? {
        val items = calls(scope.args)
        val group = items.firstOrNull { it.name == "group" }?.args?.trim()
        val keyed = authorKeyed || group == AUTHOR_FIELD
        if (group != null && !keyed && items.any { it.name == "output" && COUNT.containsMatchIn(it.args) }) return group
        for (child in items) {
            val found =
                when (child.name) {
                    "each" -> groupLevelCount(child, keyed)
                    "all" -> groupLevelCount(child, authorKeyed)
                    else -> null
                }
            if (found != null) return found
        }
        return null
    }

    /**
     * The group expression of the first scope under [scope] that orders groups merged from several
     * clusters, or null. Groups inside an `each(…)` of a `group(pubkey)` scope ([partitioned]) all
     * come from one cluster.
     */
    private fun unpartitionedOrder(
        scope: Call,
        partitioned: Boolean,
    ): String? {
        val items = calls(scope.args)
        val group = items.firstOrNull { it.name == "group" }?.args?.trim()
        if (group != null && !partitioned && items.any { it.name == "order" }) return group
        val inner = partitioned || group == AUTHOR_FIELD
        for (child in items) {
            val found =
                when (child.name) {
                    "each" -> unpartitionedOrder(child, inner)
                    "all" -> unpartitionedOrder(child, partitioned)
                    else -> null
                }
            if (found != null) return found
        }
        return null
    }

    /** The most distinct values [Plan.Distinct] counts; more are refused, not materialized. */
    const val DISTINCT_CAP = 100_000

    /**
     * [root] (a merged answer to [Plan.Distinct.pipeline]) as Vespa answers the original
     * `all(group(F) output(count()))`: a bare `group:root` whose `count()` is the number of
     * distinct groups, here counted off the merged list, and no children (measured, Vespa 8: the
     * original answers no grouplist at all).
     */
    fun exactDistinct(root: JsonObject): JsonObject {
        val children = root["children"] as? JsonArray ?: return root
        return JsonObject(
            root +
                (
                    "children" to
                        JsonArray(
                            children.map { child ->
                                val node = child as? JsonObject
                                if (node == null || (node["id"] as? JsonPrimitive)?.content?.startsWith("group:root") != true) child else distinctRoot(node)
                            },
                        )
                ),
        )
    }

    private fun distinctRoot(groupRoot: JsonObject): JsonObject {
        val groups =
            (groupRoot["children"] as? JsonArray)
                .orEmpty()
                .mapNotNull { it as? JsonObject }
                .filter { (it["id"] as? JsonPrimitive)?.content?.startsWith("grouplist:") == true }
                .sumOf { ((it["children"] as? JsonArray)?.size ?: 0).toLong() }
        // Each cluster sent at most DISTINCT_CAP + 1: a merged list within the cap was cut nowhere.
        require(groups <= DISTINCT_CAP) {
            "more than $DISTINCT_CAP distinct values: too many to count exactly across author-shard clusters without materializing every one"
        }
        val fields = (groupRoot["fields"] as? JsonObject).orEmpty() + ("count()" to JsonPrimitive(groups))
        return JsonObject(groupRoot - "children" + ("fields" to JsonObject(fields)))
    }

    private val WS = Regex("""\s+""")
    private val COUNT = Regex("""\bcount\(\s*\)""")

    /** One `name(args)` item of a grouping expression. */
    private data class Call(
        val name: String,
        val args: String,
    )

    /**
     * The top-level `name(args)` items of [s], in order: parentheses balanced, quoted strings
     * (a regex `filter`) skipped whole. Text between items that is not a call is ignored.
     */
    private fun calls(s: String): List<Call> {
        val out = ArrayList<Call>()
        var i = 0
        while (i < s.length) {
            if (!(s[i].isLetter() || s[i] == '_')) {
                i++
                continue
            }
            val start = i
            while (i < s.length && (s[i].isLetterOrDigit() || s[i] == '_' || s[i] == '.')) i++
            val name = s.substring(start, i)
            while (i < s.length && s[i].isWhitespace()) i++
            if (i >= s.length || s[i] != '(') continue
            val open = i
            var depth = 0
            var quoted = false
            while (i < s.length) {
                val c = s[i]
                when {
                    c == '\\' && quoted -> i++
                    c == '"' -> quoted = !quoted
                    !quoted && c == '(' -> depth++
                    !quoted && c == ')' -> if (--depth == 0) break
                }
                i++
            }
            out += Call(name, s.substring(open + 1, minOf(i, s.length)))
            i++
        }
        return out
    }
}
