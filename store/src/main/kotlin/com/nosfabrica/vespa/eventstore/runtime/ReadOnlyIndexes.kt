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
package com.nosfabrica.vespa.eventstore.runtime

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationCells
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.store.RawEvent

/**
 * Why a store opened with `readOnly = true` refuses [operation]: another process is the one that
 * writes this index (see `VespaEventStore.open`), and a write from here would make that process's
 * single-writer assertion false without a word.
 */
internal fun readOnlyRefusal(operation: String): Nothing =
    throw IllegalStateException(
        "$operation refused: this store was opened read-only (readOnly = true); another process is the writer of this index",
    )

/**
 * THE BACKSTOP of a read-only store: every read passes through, every write throws. The store
 * refuses its own write entry points before any work (`NostrSemanticsStore.checkWritable`), so
 * nothing should reach here; this is what makes a write path added later, or one missed there,
 * fail at the port instead of writing. Every member is overridden, defaults included, as
 * PortDecoratorsTest holds every decorator to: an inherited `putIfNewer` would call [put] and
 * be refused, but an inherited read default would answer slowly instead of reaching the engine.
 */
internal class ReadOnlyEventIndex(
    private val inner: EventIndex,
) : EventIndex {
    override val supersedesViaPut: Boolean get() = inner.supersedesViaPut

    override suspend fun get(id: String): EventDoc? = inner.get(id)

    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query)

    override suspend fun existingIds(ids: List<String>): Set<String> = inner.existingIds(ids)

    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = inner.rawSearch(query)

    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = inner.searchRanked(query)

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = inner.rawSearchRanked(query)

    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) = inner.visitIds(query, withDTag, onPage)

    override suspend fun distinctTagIndexValues(
        query: EventQuery,
        tagName: String,
    ): Set<String>? = inner.distinctTagIndexValues(query, tagName)

    override suspend fun visitTags(
        query: EventQuery,
        onPage: suspend (List<List<List<String>>>) -> Boolean,
    ) = inner.visitTags(query, onPage)

    override suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage = inner.visitDocsPage(query, resumeFrom, maxDocs)

    override suspend fun count(query: EventQuery): Int = inner.count(query)

    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> = inner.countByAuthor(query)

    override suspend fun countLinks(
        keys: Collection<String>,
        linkers: EventQuery,
    ): Map<String, Map<Int, Int>> = inner.countLinks(keys, linkers)

    override suspend fun scanAuthors(query: EventQuery): Set<String> = inner.scanAuthors(query)

    override suspend fun put(doc: EventDoc) = readOnlyRefusal("put")

    override suspend fun putAll(docs: List<EventDoc>) = readOnlyRefusal("putAll")

    override suspend fun putIfNewer(doc: EventDoc): Boolean = readOnlyRefusal("putIfNewer")

    override suspend fun remove(id: String) = readOnlyRefusal("remove")

    override suspend fun removeAll(ids: List<String>) = readOnlyRefusal("removeAll")

    override suspend fun removeDocs(docs: List<EventDoc>) = readOnlyRefusal("removeDocs")

    override fun close() = inner.close()
}

/** [ReadOnlyEventIndex]'s twin for the reputation documents: the trust projection's writes. */
internal class ReadOnlyReputationIndex(
    private val inner: ReputationIndex,
) : ReputationIndex {
    override suspend fun get(pubkey: String): ReputationDoc? = inner.get(pubkey)

    override suspend fun visitPubkeys(onPage: suspend (List<String>) -> Boolean) = inner.visitPubkeys(onPage)

    override suspend fun visitDocs(onPage: suspend (List<ReputationDoc>) -> Boolean) = inner.visitDocs(onPage)

    override suspend fun put(reputation: ReputationDoc) = readOnlyRefusal("reputation put")

    override suspend fun putAll(reputations: List<ReputationDoc>) = readOnlyRefusal("reputation putAll")

    override suspend fun updateCells(updates: List<ReputationCells>) = readOnlyRefusal("reputation updateCells")

    override suspend fun removeCells(removals: List<CellRemoval>) = readOnlyRefusal("reputation removeCells")

    override suspend fun remove(pubkey: String) = readOnlyRefusal("reputation remove")

    override suspend fun removeAll(pubkeys: List<String>) = readOnlyRefusal("reputation removeAll")

    override fun close() = inner.close()
}
