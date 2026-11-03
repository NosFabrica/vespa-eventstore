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
package com.nosfabrica.vespa.eventstore.search

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.async.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.async.forEachBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql

/** Ids asked about in one membership query: `buildExistence`'s chunk, whose YQL stays near 35 KB. */
internal const val MEMBERSHIP_CHUNK = 500

/**
 * The size of the union of [queries]' served sets (STORE-C01), holding one page of ids rather than the union.
 *
 * Filters that cannot share an event are counted apart by [countOne] and summed. Within a group that can, a filter
 * whose limit binds is held as ids (at most its limit), the largest of the rest is counted by the engine, and every
 * other one is walked a page at a time, keeping only the ids no earlier filter serves.
 */
internal suspend fun countUnion(
    index: EventIndex,
    queries: List<EventQuery>,
    countOne: suspend (EventQuery) -> Int,
): Int {
    var total = 0
    overlapGroups(queries).forEachBounded(
        QUERY_FANOUT,
        produce = { group -> if (group.size == 1) countOne(group[0]) else countOverlapping(index, group) },
    ) { total += it }
    return total
}

private suspend fun countOverlapping(
    index: EventIndex,
    group: List<EventQuery>,
): Int {
    // A positive limit bounds hits, never a count, so this is each filter's whole served set.
    val matched = group.map { index.count(it) }
    val held = HashSet<String>()
    val open = mutableListOf<Pair<EventQuery, Int>>()
    group.zip(matched).forEach { (q, n) ->
        val limit = q.limit
        if ((limit != null && limit < n) || q.idWeights.isNotEmpty() || q.authorWeights.isNotEmpty()) {
            servedIds(index, q) { held += it }
        } else {
            open += q.copy(limit = null) to n
        }
    }
    if (open.isEmpty()) return held.size
    open.sortByDescending { it.second }
    val (largest, largestCount) = open[0]
    var total = held.size + largestCount - servedAmong(index, largest, held.toList()).size
    for (k in 1 until open.size) {
        val walked = open[k].first
        val earlier = open.subList(0, k).map { it.first }.filter { it.mayShareAMatchWith(walked) }
        servedIds(index, walked) { page ->
            var fresh = page.filterNot { it in held }
            for (q in earlier) {
                if (fresh.isEmpty()) break
                val served = servedAmong(index, q, fresh)
                fresh = fresh.filterNot { it in served }
            }
            total += fresh.size
        }
    }
    return total
}

/** [q]'s served ids, page by page: an id walk where its order is recency, else its ranked recall. */
private suspend fun servedIds(
    index: EventIndex,
    q: EventQuery,
    onPage: suspend (List<String>) -> Unit,
) {
    if (!q.isRanked() || EventYql.walksInOrder(q)) {
        index.visitIds(q) { page ->
            onPage(page.map { it.id })
            true
        }
    } else {
        onPage(index.rawSearch(q).map { it.id })
    }
}

/** Which of [ids] an unlimited [q] serves, asked [MEMBERSHIP_CHUNK] at a time as [q] narrowed to them. */
private suspend fun servedAmong(
    index: EventIndex,
    q: EventQuery,
    ids: List<String>,
): Set<String> {
    val own = q.ids.toSet()
    val asked = if (own.isEmpty()) ids else ids.filter { it in own }
    val served = HashSet<String>()
    asked.chunked(MEMBERSHIP_CHUNK).forEach { chunk -> servedIds(index, q.copy(ids = chunk)) { served += it } }
    return served
}

/** [queries] split so that no event can match filters in two different groups. */
internal fun overlapGroups(queries: List<EventQuery>): List<List<EventQuery>> {
    val root = IntArray(queries.size) { it }

    fun find(i: Int): Int {
        var r = i
        while (root[r] != r) r = root[r]
        return r
    }
    for (i in queries.indices) {
        for (j in i + 1 until queries.size) {
            if (queries[i].mayShareAMatchWith(queries[j])) root[find(i)] = find(j)
        }
    }
    return queries.indices
        .groupBy(::find)
        .values
        .map { members -> members.map { queries[it] } }
}

/** False only when no event can match both: disjoint kinds, ids or authors, or windows that do not meet. */
internal fun EventQuery.mayShareAMatchWith(other: EventQuery): Boolean =
    !(
        apart(kinds, other.kinds) ||
            apart(ids, other.ids) ||
            apart(authors, other.authors) ||
            before(until, other.since) ||
            before(other.until, since)
    )

private fun <T> apart(
    a: List<T>,
    b: List<T>,
): Boolean = a.isNotEmpty() && b.isNotEmpty() && b.toSet().let { other -> a.none { it in other } }

private fun before(
    until: Long?,
    since: Long?,
): Boolean = until != null && since != null && until < since
