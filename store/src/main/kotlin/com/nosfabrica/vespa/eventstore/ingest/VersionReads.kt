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
package com.nosfabrica.vespa.eventstore.ingest

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.versionStub
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

/**
 * THE STORED VERSIONS OF AN ADDRESS, WITHOUT THEIR DOCUMENTS. Supersession
 * needs a version's id, created_at and d alone, and a full-document read of a
 * stored kind 3 is 50-500 KB of tags, read under the writer lock to be thrown
 * away. These reads answer with STUBS ([versionStub]: id, author, kind,
 * created_at, d, owner = author) — enough to compare, to bucket by address and
 * to hand to `removeDocs` — for kinds whose removal no decorator reacts to by
 * reading the document (`BulkRecordInsert.WHOLE_DOC_KINDS` keep full reads).
 *
 * Shared by [BulkRecordInsert]'s stage D and [BulkMixedInsert]'s preload, so
 * the two bulk paths read a version the same way.
 */
internal class VersionReads(
    private val index: EventIndex,
) {
    /**
     * Every stored version of [kind] for [authors], plain replaceable, off ONE
     * grouping ([EventIndex.versionCensus]): per author, its winner and how
     * many versions it holds. An author holding exactly one is answered by its
     * winner's stub; one holding more (drift: a sweep that failed) is read in
     * full, so the drift repair still sweeps
     * every version. It used to be two groupings side by side (newest versions,
     * count by author): two queries per chunk, two snapshots a write could land
     * between.
     */
    suspend fun replaceable(
        kind: Int,
        authors: List<String>,
        /** The caller's engine-query bound, one permit per query; see [mapGated]. */
        gate: Semaphore? = null,
    ): List<EventDoc> {
        val census = gated(gate) { index.versionCensus(kind, authors) }
        val out = ArrayList<EventDoc>()
        val unsure = ArrayList<String>()
        for (author in authors) {
            val held = census[author] ?: continue
            if (held.versions == 1) out += stub(kind, author).versionStub(held.newest) else unsure += author
        }
        if (unsure.isNotEmpty()) out += gated(gate) { index.search(EventQuery(kinds = listOf(kind), authors = unsure)) }
        return out
    }

    /** Every stored version [q] recalls for one (kind, author), addressable: the id walk with each version's FIRST d tag, as stubs. */
    suspend fun addressable(
        q: EventQuery,
        kind: Int,
        author: String,
        /** The caller's engine-query bound: one permit for the walk, whose pages run one after another. */
        gate: Semaphore? = null,
    ): List<EventDoc> {
        val template = stub(kind, author)
        val out = ArrayList<EventDoc>()
        gated(gate) {
            index.visitIds(q, withDTag = true) { page ->
                page.mapTo(out) { template.versionStub(it) }
                true
            }
        }
        return out
    }

    private suspend inline fun <T> gated(
        gate: Semaphore?,
        crossinline body: suspend () -> T,
    ): T = if (gate == null) body() else gate.withPermit { body() }

    /** The (kind, author) a [versionStub] is built against: no id, no time, no tags of its own. */
    private fun stub(
        kind: Int,
        author: String,
    ) = EventDoc(id = "", pubkey = author, createdAt = 0, kind = kind, tags = emptyList(), content = "", sig = "")
}
