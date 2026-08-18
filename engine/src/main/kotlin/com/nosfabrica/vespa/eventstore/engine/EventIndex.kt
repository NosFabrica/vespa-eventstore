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
package com.nosfabrica.vespa.eventstore.engine
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.store.RawEvent

/**
 * The engine port: document-keyed get/put/remove plus [EventQuery] recall.
 * Two implementations: the real Vespa client and the in-memory reference —
 * the executable spec of [EventQuery]'s matching semantics.
 *
 * CONTRACT: [get]/[put]/[remove] are read-your-writes consistent per document,
 * and an acked [put] is visible to [search] (proton updates the memory index
 * on the write path). That is what makes query-then-write sound under a
 * single writer.
 */
interface EventIndex : AutoCloseable {
    /**
     * True when the engine enforces replaceable/addressable supersession via
     * [putIfNewer]'s address-keyed conditional put (the bulk path then skips
     * its version-read stage). Default false: read-then-supersede. A decorator
     * that REACTS to writes (the trust projection) intentionally keeps this
     * false so its hooks see both old and new versions; see [putIfNewer].
     */
    val supersedesViaPut: Boolean get() = false

    suspend fun get(id: String): EventDoc?

    suspend fun put(doc: EventDoc)

    /** Bulk [put]: same contract (all acked and visible on return); the real client pipelines them all at once. */
    suspend fun putAll(docs: List<EventDoc>) = docs.forEach { put(it) }

    suspend fun remove(id: String)

    /** Bulk [remove]; the real client pipelines the deletes over HTTP/2 (a big sweep is O(1) round trips, not O(N)). */
    suspend fun removeAll(ids: List<String>) = ids.forEach { remove(it) }

    /**
     * [removeAll] for callers already HOLDING the doomed docs. Overrides use
     * the docs to skip reads: a reacting decorator (trust projection) learns
     * what each removal invalidates without a get per id, and the
     * address-keyed client resolves each docid locally.
     */
    suspend fun removeDocs(docs: List<EventDoc>) = removeAll(docs.map { it.id })

    /** Docs matching [query]: newest first (`created_at` desc, id asc tiebreak) unless ranked by a search term. */
    suspend fun search(query: EventQuery): List<EventDoc>

    /**
     * Which of [ids] the index holds — the bulk-dedup EXISTENCE check, the
     * hottest read on a mirroring relay (~99% of offered ids already held).
     * Semantically `search(EventQuery(ids)).map { it.id }` — RAW presence, no
     * expiry filter — but implementations may answer without materializing
     * summaries, the single largest ingest cost at mirror hit rates (measured;
     * see benchmark/README.md). Exactness is the contract: a short answer is a
     * wrong write. A decorator MUST delegate to its inner index or it silently
     * loses the engine-side summary-free path.
     */
    suspend fun existingIds(ids: List<String>): Set<String> {
        // EventQuery treats an empty ids list as "no constraint", so riding
        // search() would answer a membership question about NOTHING with
        // EVERYTHING. The real client short-circuits identically.
        if (ids.isEmpty()) return emptySet()
        return search(EventQuery(ids = ids)).mapTo(HashSet()) { it.id }
    }

    /**
     * [search] with each match projected to a Quartz [RawEvent] (`tags` kept as
     * its canonical JSON string) — the read path a relay serves straight to a
     * client. The real client builds each [RawEvent] from the decoded summary,
     * so the tag string passes through verbatim with no [EventDoc] and no tag
     * parse. Ordering matches [search].
     */
    suspend fun rawSearch(query: EventQuery): List<RawEvent> = search(query).map { it.toRawEvent() }

    /**
     * [search] with the engine's relevance per hit — for callers that must MERGE
     * several RANKED queries into one order (see [Ranked]). The default answers
     * with null scores, the honest answer for an engine that does not rank.
     *
     * It costs a wrapper per hit, so the store stays on [search] for every
     * single-query recall — everything a relay serves that is not a
     * multi-filter REQ.
     */
    suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = search(query).map { Ranked(it, null) }

    /** [rawSearch] with the scores [searchRanked] carries — the same merge, on the raw read path. */
    suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = rawSearch(query).map { Ranked(it, null) }

    /**
     * Stream EVERY match's (id, created_at) — the full-corpus walk behind
     * negentropy snapshots and sync diffs. No result cap; the real client
     * pages a document-API visit, and order across pages is engine-defined.
     * [onPage] returns whether to CONTINUE (false stops early); [withDTag]
     * also projects the `d` tag an addressable-corpus walk keys on. This
     * default rides [search] and hands everything as ONE page — only the
     * in-memory reference should use it.
     */
    suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean = false,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        onPage(search(query).map { DocRef(it.id, it.createdAt, if (withDTag) it.dTag() else null) })
    }

    /**
     * Stream every match's exact TAG ARRAY (distinct-tag-value discovery), same
     * walk contract as [visitIds]. Deliberately a projection of the stored
     * `tags` field, NOT a grouping over the lossy `tag_index` (single-letter
     * names, FIRST values only — see [EventDoc.tagIndex]), which would silently
     * widen or miss the asked-for set. A decorator MUST delegate to its inner
     * index or it loses the streaming projection.
     */
    suspend fun visitTags(
        query: EventQuery,
        onPage: suspend (List<List<List<String>>>) -> Boolean,
    ) {
        onPage(search(query).map { it.tags })
    }

    /**
     * One PAGE of FULL docs from a resumable, engine-ordered, exhaustive walk
     * — the corpus-rewrite (reindex) primitive, O(page) memory. [resumeFrom]
     * is the continuation the previous page returned (null starts the walk; a
     * null continuation in the result ends it). The default emulates it by
     * id-ordered paging over [search] — correct but re-lists the match set per
     * call, so only for the in-memory reference; the real client uses the
     * document API's visit.
     */
    suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage {
        val batch =
            search(query)
                .sortedBy { it.id }
                .filter { resumeFrom == null || it.id > resumeFrom }
                .take(maxDocs)
        return DocsPage(batch, batch.lastOrNull()?.id?.takeIf { batch.size == maxDocs })
    }

    suspend fun count(query: EventQuery): Int

    /**
     * The DISTINCT authors of [query]'s matches, each with its DOC COUNT. The
     * real client answers it with ONE server-side grouping, so the orphan-score
     * sweep gets distinct 30382 authors — and the per-service card counts its
     * dry run reports — out of millions of docs without reconstructing them
     * (which times search out). A decorator MUST delegate to its inner index,
     * not this default, or it loses that server-side aggregation.
     */
    suspend fun countByAuthor(query: EventQuery): Map<String, Int> = search(query).groupingBy { it.pubkey }.eachCount()

    /**
     * Every distinct author, STREAMED: complete like [countByAuthor]'s key set,
     * but paged through visit continuations instead of one engine response. The
     * guard-owner preload needs full-corpus completeness without that
     * single-response peak — a missed author is a false negative in the guard
     * filter. A decorator MUST delegate to its inner index.
     */
    suspend fun scanAuthors(query: EventQuery): Set<String> = countByAuthor(query).keys

    /**
     * Store [doc] IFF it wins its NIP-01 address (highest `created_at`, ties to
     * the LOWEST id): true when stored (older versions removed), false when a
     * same-or-newer version already holds the address. Non-replaceable docs
     * store unconditionally.
     *
     * The default searches-compares-supersedes; the real client OVERRIDES it
     * with an address-keyed conditional put (engine-atomic, no read). A REACTING
     * decorator (trust projection) must RIDE this default rather than forward to
     * inner: it supersedes through the decorator's own [put]/[remove], firing
     * reactions for old AND new versions, which the engine's atomic put exposes
     * for neither (hence [supersedesViaPut] stays false there).
     */
    suspend fun putIfNewer(doc: EventDoc): Boolean {
        val address =
            doc.addressOrNull() ?: run {
                put(doc)
                return true
            }
        val dTag = doc.dTagOrEmpty()
        val q =
            // Narrow by non-empty d so a prolific author's other addresses of
            // this kind don't push the target past the search page. The
            // addressOrNull filter below is the exact match either way, and it
            // normalizes missing == empty d.
            if (doc.kind.isAddressable() && dTag.isNotEmpty()) {
                EventQuery(kinds = listOf(doc.kind), authors = listOf(doc.pubkey), tags = mapOf("d" to listOf(dTag)))
            } else {
                EventQuery(kinds = listOf(doc.kind), authors = listOf(doc.pubkey))
            }
        val existing = search(q).filter { it.addressOrNull() == address }
        // The NIP-01 winner is the MINIMUM of the serving order (newest first,
        // ties to the lowest id) — see [EventDoc.NEWEST_FIRST].
        val incumbent = existing.minWithOrNull(EventDoc.NEWEST_FIRST)
        // Incumbent wins or is identical -> reject the incoming (stale) version.
        if (incumbent != null && EventDoc.NEWEST_FIRST.compare(doc, incumbent) >= 0) return false
        existing.forEach { remove(it.id) }
        put(doc)
        return true
    }
}

/** One page of [EventIndex.visitDocsPage]: the docs plus the continuation for the next call (null = the walk is complete). */
data class DocsPage(
    val docs: List<EventDoc>,
    val continuation: String?,
)

/** The (id, created_at[, d tag]) projection [EventIndex.visitIds] streams — all a sync diff or projection walk needs. */
data class DocRef(
    val id: String,
    val createdAt: Long,
    val dTag: String? = null,
)
