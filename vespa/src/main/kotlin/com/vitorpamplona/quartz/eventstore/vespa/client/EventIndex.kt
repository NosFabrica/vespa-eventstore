/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.vespa.client
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.store.RawEvent

/**
 * The engine port an event store talks to: document-keyed get/put/remove plus
 * [EventQuery] recall. There are two implementations: the real Vespa client
 * (document API + feed + `/search/`) and the in-memory reference in this
 * module's testFixtures. The reference also serves as the executable spec of
 * [EventQuery]'s matching semantics.
 *
 * [get]/[put]/[remove] must be read-your-writes consistent per document, and an
 * acked [put] must be visible to [search]. Vespa's proton gives both, because
 * the memory index is updated on the write path. That is what makes
 * query-then-write semantics sound under a single writer.
 */
interface EventIndex : AutoCloseable {
    /**
     * When true, replaceable/addressable supersession is enforced by the engine
     * via [putIfNewer] (an address-keyed conditional put) rather than the client
     * reading the current versions first. The bulk path checks this to skip its
     * version-read stage. Default false — the read-then-supersede behavior.
     *
     * A decorator that REACTS to writes (e.g. the trust projection) intentionally
     * keeps this false so supersession stays read-based and its react hooks see
     * both the old and new versions; see [putIfNewer].
     */
    val supersedesViaPut: Boolean get() = false

    suspend fun get(id: String): EventDoc?

    suspend fun put(doc: EventDoc)

    /**
     * Bulk [put]: same contract (all acked and visible on return), but an
     * implementation may pipeline the writes. The real client keeps them all in
     * flight at once, which is what makes million-event ingest feasible.
     */
    suspend fun putAll(docs: List<EventDoc>) = docs.forEach { put(it) }

    suspend fun remove(id: String)

    /** Bulk [remove]; the real client pipelines the deletes over HTTP/2 (a big sweep is O(1) round trips, not O(N)). */
    suspend fun removeAll(ids: List<String>) = ids.forEach { remove(it) }

    /**
     * [removeAll] for callers that already HOLD the doomed docs (a sweep just
     * searched them; the mixed bulk path preloaded them). The default discards
     * the extra information; overrides use it to skip reads — a reacting
     * decorator (the trust projection) learns what each removal invalidates
     * without a get per id, and the address-keyed client resolves each docid
     * locally instead of reading it back.
     */
    suspend fun removeDocs(docs: List<EventDoc>) = removeAll(docs.map { it.id })

    /** Docs matching [query]: newest first (`created_at` desc, id asc tiebreak) unless ranked by a search term. */
    suspend fun search(query: EventQuery): List<EventDoc>

    /**
     * Which of [ids] the index holds — the bulk-dedup EXISTENCE check, and the
     * hottest read of a mirroring relay (a store that syncs the network re-sees
     * the network: ~99% of offered ids are already held). Semantically exactly
     * `search(EventQuery(ids)).map { it.id }` — RAW presence, no expiry filter,
     * finding docs at any docid — but an implementation may answer WITHOUT
     * materializing documents: the caller reads nothing but membership, so
     * filling ~ids.size full summaries to learn it is the single largest ingest
     * cost at mirror hit rates (measured — see benchmark/README.md).
     *
     * Exactness is the contract, same as every batch-stage read: a short answer
     * here is a wrong write, not a small answer. The default rides [search], so
     * the in-memory reference stays the executable spec of what "held" means.
     * A decorator MUST delegate to its inner index or it silently loses the
     * engine-side summary-free path.
     */
    suspend fun existingIds(ids: List<String>): Set<String> {
        // Guarded HERE, not left to search(): EventQuery treats an empty ids
        // list as "no constraint", so riding it would answer a membership
        // question about NOTHING with EVERYTHING — the wrong direction for an
        // exactness contract (a future caller would see every id "already
        // stored"). The real client short-circuits identically.
        if (ids.isEmpty()) return emptySet()
        return search(EventQuery(ids = ids)).mapTo(HashSet()) { it.id }
    }

    /**
     * The same recall as [search], but each match projected to a Quartz
     * [RawEvent] — the wire event with `tags` kept as its canonical JSON string.
     * This is the read path a relay serves straight to a client: it never needs
     * the per-tag object model [EventDoc] carries, so a raw path can hand each
     * hit's `tags` through verbatim rather than parse-then-re-serialize it.
     *
     * The default rides [search] and re-serializes each doc's tags, which keeps
     * the in-memory reference correct. The real Vespa client overrides it to
     * build the [RawEvent] from the decoded summary directly, so a hit's tag
     * string is decoded once off the wire and passed straight through — no
     * [EventDoc], no tag parse. Ordering matches [search].
     */
    suspend fun rawSearch(query: EventQuery): List<RawEvent> = search(query).map { it.toRawEvent() }

    /**
     * Stream EVERY match's (id, created_at). This is the full-corpus walk
     * behind negentropy snapshots and sync reconcile diffs. Unlike [search]
     * there is no result cap: the real client pages through Vespa's
     * document-API visit (a streaming scan, not a query), calling [onPage] per
     * page. Order across pages is engine-defined, so callers must not assume
     * recency.
     *
     * [onPage] returns whether to CONTINUE; false stops the walk early. (A
     * capped snapshot needn't scan a 10M corpus to learn it exceeds the cap.)
     * [withDTag] also projects each doc's `d` tag, which is what an
     * addressable-corpus walk (rebuilding the trust projection) keys on.
     *
     * This default rides [search] and hands the caller ONE page containing
     * everything — complete, but with none of the streaming that makes a
     * corpus-sized walk affordable. Only the in-memory reference should use it.
     */
    suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean = false,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        onPage(search(query).map { DocRef(it.id, it.createdAt, if (withDTag) it.dTag() else null) })
    }

    /**
     * One PAGE of FULL docs from a resumable, engine-ordered walk over
     * [query]'s matches — the corpus-rewrite primitive (reindex). [resumeFrom]
     * is the continuation a previous page returned (null starts the walk); a
     * null continuation in the result means the walk is complete. Order is
     * engine-defined but the walk is exhaustive, with O(page) memory — what
     * [visitIds] gives a sync diff, for callers that need the whole document.
     *
     * The default emulates it by id-ordered paging over [search] — correct,
     * but it re-lists the match set per call, so it is only for the in-memory
     * reference; the real client overrides it with the document API's visit.
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
     * The number of DISTINCT authors (pubkeys) among the matches — what a
     * status/metrics caller reports as "pubkeys with content". The default rides
     * [search] — exact, but it reconstructs every matching doc to count their
     * pubkeys; the real client overrides it with a server-side grouping count
     * that never leaves the engine.
     *
     * The real client's answer is an ESTIMATE: Vespa's `count()` over a group
     * list is sketch-based, measured ~3% high against a known author set
     * (510 for 496 true). Fine for the metrics this feeds; a caller that needs
     * the exact figure should size [scanAuthors]'s set instead (a corpus walk —
     * exact, but seconds-to-minutes where this is milliseconds).
     */
    suspend fun countDistinctAuthors(query: EventQuery): Int = search(query).map { it.pubkey }.distinct().size

    /**
     * How many docs match [query] per kind (kind -> count) — the corpus shape a
     * status/metrics caller prints as "top kinds". The default rides [search]
     * (exact, but it materializes every match to bucket them); the real client
     * overrides it with a server-side grouping.
     */
    suspend fun countByKind(query: EventQuery): Map<Int, Int> = search(query).groupingBy { it.kind }.eachCount()

    /**
     * The DISTINCT `pubkey`s (event authors) across [query]'s match set — the
     * actual author set, not just its size ([countDistinctAuthors]). The default
     * rides [search]; the real client overrides it with a server-side grouping
     * over the full match set, so the orphan-score sweep gets the distinct 30382
     * authors out of millions of docs without reconstructing them (which times
     * search out). A decorator MUST delegate to its inner index, not this
     * default, or it loses that server-side aggregation.
     */
    suspend fun distinctAuthors(query: EventQuery): Set<String> = search(query).mapTo(HashSet()) { it.pubkey }

    /**
     * Every distinct author of [query]'s match set, STREAMED. Both this and
     * [distinctAuthors] are complete (neither caps groups), but the grouping
     * builds its whole answer in one engine response, while this pages a visit
     * through continuations. The guard-owner preload runs over the entire corpus
     * and needs completeness without that single-response peak — a missed author
     * would be a false negative in the guard filter (a skipped-but-needed
     * tombstone probe). A decorator MUST delegate to its inner index.
     */
    suspend fun scanAuthors(query: EventQuery): Set<String> = distinctAuthors(query)

    /**
     * Store [doc] IFF it wins its NIP-01 address (highest `created_at`; ties
     * broken by the LOWEST id) — replaceable/addressable supersession as a single
     * call. Returns true when [doc] was stored (and any older version at the
     * address removed), false when a same-or-newer version already held it.
     * Non-replaceable docs are stored unconditionally (true).
     *
     * The default realizes it the obvious way — search the address, compare,
     * supersede — which is what the in-memory reference and today's store do, so
     * outcomes match the per-event rules exactly. The real client OVERRIDES it
     * with an address-keyed conditional put, letting the engine enforce
     * newest-wins atomically and reject stale versions server-side with no read.
     *
     * A REACTING decorator (the trust projection) does the opposite of the pure
     * read paths: it must RIDE this default rather than forward to inner, because
     * the default supersedes through the decorator's own [put]/[remove] — firing
     * its reactions for the removed old version AND the new one. The engine's
     * atomic conditional put exposes neither the old doc nor the fact that it also
     * keeps [supersedesViaPut] false there.
     */
    suspend fun putIfNewer(doc: EventDoc): Boolean {
        val address =
            doc.addressOrNull() ?: run {
                put(doc)
                return true
            }
        val dTag = doc.dTagOrEmpty()
        val q =
            // Addressable with a non-empty d: narrow by d so a prolific author's
            // other addresses of this kind don't push the target past the search
            // page. Replaceable, or addressable with an empty/missing d (nothing to
            // recall on), stay broad by (kind, author) — the addressOrNull filter
            // below is the exact match and normalizes missing == empty d.
            if (doc.kind.isAddressable() && dTag.isNotEmpty()) {
                EventQuery(kinds = listOf(doc.kind), authors = listOf(doc.pubkey), tags = mapOf("d" to listOf(dTag)))
            } else {
                EventQuery(kinds = listOf(doc.kind), authors = listOf(doc.pubkey))
            }
        val existing = search(q).filter { it.addressOrNull() == address }
        val incumbent = existing.minWithOrNull(REPLACEABLE_WINNER)
        // Incumbent wins or is identical -> reject the incoming (stale) version.
        if (incumbent != null && REPLACEABLE_WINNER.compare(doc, incumbent) >= 0) return false
        existing.forEach { remove(it.id) }
        put(doc)
        return true
    }
}

/** NIP-01 replaceable winner order: newest `created_at` first, ties to the LOWEST id. */
internal val REPLACEABLE_WINNER: Comparator<EventDoc> =
    compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id }

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
