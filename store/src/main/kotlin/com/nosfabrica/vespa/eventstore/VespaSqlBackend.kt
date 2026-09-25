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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.withActivity
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.tags.isIndexableTagName
import com.vitorpamplona.quartz.nipXXSql.AggregatePlan
import com.vitorpamplona.quartz.nipXXSql.ScanSpec
import com.vitorpamplona.quartz.nipXXSql.SqlProfile
import com.vitorpamplona.quartz.nipXXSql.SqlStoreBackend

/**
 * How this store answers Quartz's SQL profile (`IEventStore.sql`, and a
 * relay's `SQL` / `FETCH`) without running SQL: Quartz's pushdown executor
 * hands it plans and scans, and each becomes the cheapest engine read that
 * answers it.
 *
 *  - **Aggregates, natively** — `count(*)` is one engine count; `count(*)
 *    GROUP BY pubkey` is the server-side author grouping
 *    ([EventIndex.countByAuthor]); `SELECT DISTINCT value FROM tags WHERE
 *    name = '<letter>' AND value <> ''` is the `tag_index` grouping behind
 *    [NostrSemanticsStore.distinctTagValues]. Anything else returns null and
 *    Quartz falls back to scans.
 *  - **Scans** — every match of the spec's filter, walked with
 *    [EventIndex.visitIds] (no hit cap) and fetched in id batches; a
 *    newest-first listing's LIMIT goes to a plain search instead. A
 *    reference the query reads only for ids and times is the walk alone.
 *  - **Refusal** — a reference with no condition on id, author, kind or a
 *    single-letter tag would walk the whole corpus, so it is refused and the
 *    query fails `unsupported:`.
 *
 * Reads are PLAIN, like a NIP-77 snapshot: expiry is honored ([NostrSemanticsStore.plainQuery]),
 * no observer lens or ranking applies, so SQL sees what a mirror sees, and
 * scans and aggregates agree with each other.
 */
internal class VespaSqlBackend(
    private val store: NostrSemanticsStore,
    private val index: EventIndex,
) : SqlStoreBackend {
    override suspend fun events(
        spec: ScanSpec,
        onEvent: (Event) -> Unit,
    ) = withActivity(Activity.Query) {
        val q = store.plainQuery(spec.toFilter()) ?: return@withActivity
        if (q.limit != null) {
            index.search(q).forEach { onEvent(it.toEvent()) }
            return@withActivity
        }
        index.visitIds(q) { page ->
            for (chunk in page.chunked(ID_BATCH)) {
                index.search(EventQuery(ids = chunk.map { it.id })).forEach { onEvent(it.toEvent()) }
            }
            true
        }
    }

    /**
     * An id listing (`FilterSql.ids`, every NIP-77 snapshot read over SQL): the same
     * [EventIndex.visitIds] walk the store's own snapshots take, no documents fetched.
     */
    override suspend fun idsAndTimes(
        spec: ScanSpec,
        onEach: (id: String, createdAt: Long) -> Unit,
    ): Boolean =
        withActivity(Activity.Query) {
            val q = store.plainQuery(spec.toFilter()) ?: return@withActivity true
            index.visitIds(q) { page ->
                page.forEach { onEach(it.id, it.createdAt) }
                true
            }
            true
        }

    override fun acceptsScan(spec: ScanSpec) = spec.isSelective

    override suspend fun aggregate(plan: AggregatePlan): List<List<Any?>>? {
        val spec = plan.scan
        val onlyCounts = plan.aggregates.all { it.function == "count" && it.column == null }
        return when {
            spec.table == SqlProfile.EVENTS && plan.groupBy.isEmpty() && onlyCounts && plan.aggregates.isNotEmpty() -> {
                val n = store.plainQuery(spec.toFilter())?.let { withActivity(Activity.Count) { index.count(it) } } ?: 0
                listOf(plan.aggregates.map { n.toLong() })
            }

            spec.table == SqlProfile.EVENTS && plan.groupBy == listOf("pubkey") && onlyCounts -> {
                val byAuthor = store.plainQuery(spec.toFilter())?.let { withActivity(Activity.Count) { index.countByAuthor(it) } } ?: emptyMap()
                byAuthor.map { (pubkey, n) -> listOf<Any?>(pubkey) + plan.aggregates.map { n.toLong() } }
            }

            // tag_index holds tag[1] of every single-letter tag, minus empty
            // values: exactly the rows `name = <letter> AND value <> ''` keeps.
            spec.table == SqlProfile.TAGS && plan.groupBy == listOf("value") && plan.aggregates.isEmpty() &&
                spec.tagName?.let(::isIndexableTagName) == true && spec.tagValues == null && spec.valueNonEmpty -> {
                store.distinctTagValues(spec.toFilter(), spec.tagName!!, 1, unconditional = true).map { listOf(it) }
            }

            else -> {
                null
            }
        }
    }

    private companion object {
        /** Ids per batched fetch, as the relay's own id lookups chunk them. */
        const val ID_BATCH = 500
    }
}
