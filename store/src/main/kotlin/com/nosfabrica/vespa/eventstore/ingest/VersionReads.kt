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
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope

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
     * Every stored version of [kind] for [authors], plain replaceable: the
     * winner per author off one grouping, how many versions each holds off
     * another, run together. An author holding exactly one is answered by its
     * winner's stub; any other answer — drift (two or more), or two groupings
     * that disagree (another process wrote between them) — reads that author's
     * versions in full, as before.
     */
    suspend fun replaceable(
        kind: Int,
        authors: List<String>,
    ): List<EventDoc> =
        coroutineScope {
            val counts = async { index.countByAuthor(EventQuery(kinds = listOf(kind), authors = authors)) }
            val newest = index.newestVersions(kind, authors)
            val held = counts.await()
            val out = ArrayList<EventDoc>()
            val unsure = ArrayList<String>()
            for (author in authors) {
                val n = held[author] ?: 0
                val winner = newest[author]
                when {
                    n == 0 && winner == null -> Unit
                    n == 1 && winner != null -> out += stub(kind, author).versionStub(winner)
                    else -> unsure += author
                }
            }
            if (unsure.isNotEmpty()) out += index.search(EventQuery(kinds = listOf(kind), authors = unsure))
            out
        }

    /** Every stored version [q] recalls for one (kind, author), addressable: the id walk with each version's FIRST d tag, as stubs. */
    suspend fun addressable(
        q: EventQuery,
        kind: Int,
        author: String,
    ): List<EventDoc> {
        val template = stub(kind, author)
        val out = ArrayList<EventDoc>()
        index.visitIds(q, withDTag = true) { page ->
            page.mapTo(out) { template.versionStub(it) }
            true
        }
        return out
    }

    /** The (kind, author) a [versionStub] is built against: no id, no time, no tags of its own. */
    private fun stub(
        kind: Int,
        author: String,
    ) = EventDoc(id = "", pubkey = author, createdAt = 0, kind = kind, tags = emptyList(), content = "", sig = "")
}
