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
package com.nosfabrica.vespa.eventstore.trust

import com.nosfabrica.vespa.eventstore.engine.DocRef
import com.nosfabrica.vespa.eventstore.engine.DocsPage
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.LinkScore
import com.nosfabrica.vespa.eventstore.engine.LinkTarget
import com.nosfabrica.vespa.eventstore.engine.LinkWeights
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.async.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.async.mapBounded
import com.nosfabrica.vespa.eventstore.engine.doc.CellRemoval
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * Maintains the `reputation` parent documents (per-pubkey trust tensors the
 * schema imports into ranking, keyed by the SERVICE that signed each score)
 * as an [EventIndex] DECORATOR: every deletion style — supersession, kind-5,
 * vanish, sweeps — funnels into [put]/[remove] here, so the tensors stay
 * current with ZERO deletion-specific code.
 *
 * A card is ONE cell update on its subject, a removed card one cell remove,
 * both applied at the end of the store COMMIT that wrote them, one pipelined
 * pass for the whole commit ([commit], [TrustRecompute.applyCards]). The only
 * deferred reaction is a 10040 naming a service nobody named before, whose
 * stored cards become cells through a walk ([TrustRecompute.projectServices])
 * — declared as [ProjectionWork] and settled inline (read-your-writes; what
 * the unit tests assert) or by the background drain ([ProjectionLedger.drainInBackground]).
 * Every trust-mutating op runs [ProjectionLedger.insuring]: the event and projection
 * writes are separate acks and dedup fires a trigger only once, so a crash
 * between them would be PERMANENT drift (the retry comes back all-duplicates)
 * — the ledger persists what the op could leave stale before it starts, and
 * the exact derive repairs it at the next settle, drain, or reconcile. Drift
 * no write trigger can see is [TrustReconciler]'s job.
 *
 * The observer never appears in a cell: their kind 10040 is resolved to a
 * service key per query (NostrSemanticsStore.lensed), so a list re-signed or
 * re-pointed writes nothing here. See docs/service-keyed-trust.md.
 */
class TrustProjection(
    private val inner: EventIndex,
    internal val reputations: ReputationIndex,
    nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
) : EventIndex {
    /** The recompute engine the ledger's drains drive; [TrustReconciler] shares it. */
    internal val recompute = TrustRecompute(inner, reputations, nowSecs)

    /** The work ledger: crash marker + (optionally deferred) projection queue; [TrustReconciler] drains it at startup. */
    internal val backlog = ProjectionLedger(reputations, recompute)

    override suspend fun get(id: String): EventDoc? = inner.get(id)

    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query)

    override suspend fun existingIds(ids: List<String>): Set<String> = inner.existingIds(ids)

    // MUST delegate, not ride the interface default, which would route through
    // this decorator's search() and lose the raw passthrough (see EventIndex.rawSearch).
    override suspend fun rawSearch(query: EventQuery): List<RawEvent> = inner.rawSearch(query)

    // Same rule, and this is the production stack's only decorator: riding the
    // default would route through search()/rawSearch() above and hand the store
    // null scores for every hit, so a multi-filter REQ would silently fall back
    // to recency and the relevance merge would never run against Vespa.
    override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = inner.searchRanked(query)

    override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = inner.rawSearchRanked(query)

    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) = inner.visitIds(query, withDTag, onPage)

    override suspend fun visitTags(
        query: EventQuery,
        onPage: suspend (List<List<List<String>>>) -> Boolean,
    ) = inner.visitTags(query, onPage)

    override suspend fun count(query: EventQuery): Int = inner.count(query)

    // MUST forward: the interface default re-lists the ENTIRE corpus through
    // this decorator's search() per page — O(corpus²). Pure read; nothing to react to.
    override suspend fun visitDocsPage(
        query: EventQuery,
        resumeFrom: String?,
        maxDocs: Int,
    ): DocsPage = inner.visitDocsPage(query, resumeFrom, maxDocs)

    // The aggregates below MUST forward to inner: the interface defaults
    // materialize the whole match set via search() where the real client groups
    // or streams server-side. scanAuthors backs the guard-owner Bloom preload
    // over the ENTIRE corpus — materializing that is an OOM, not a paged walk.

    /**
     * FORWARDED, like every other read: the projection decorates WRITES, and
     * an aggregate it does not forward silently becomes the port's `= null`
     * default — which reads as "this engine has no such aggregate" and sends
     * the caller down the paged walk instead (~157s against ~1s on the corpus
     * [EventIndex.distinctTagIndexValues] measures). A decorator that does not
     * override a defaulted port member does not merely fail to decorate it; it
     * ANSWERS it, wrongly.
     */
    override suspend fun distinctTagIndexValues(
        query: EventQuery,
        tagName: String,
    ): Set<String>? = inner.distinctTagIndexValues(query, tagName)

    override suspend fun countByAuthor(query: EventQuery): Map<String, Int> = inner.countByAuthor(query)

    override suspend fun scoreLinks(
        targets: List<LinkTarget>,
        linkers: EventQuery,
        weights: LinkWeights,
    ): Map<String, LinkScore> = inner.scoreLinks(targets, linkers, weights)

    override suspend fun scanAuthors(query: EventQuery): Set<String> = inner.scanAuthors(query)

    override fun close() {
        inner.close()
        reputations.close()
    }

    // The engine's atomic conditional put IS enough now: a card's cell is a
    // function of the newest version at its address alone (the same key is
    // overwritten), so the projection no longer needs to see the version it
    // replaced. Forwarding lets the bulk path skip its version-read stage.
    override val supersedesViaPut: Boolean get() = inner.supersedesViaPut

    /**
     * ONE STORE COMMIT, ONE REACTION. Run [body] — a commit under the trust
     * gate, which may make any number of port calls on this decorator, from
     * any number of coroutines — and apply what its trust writes change ONCE,
     * at the end: one crash-ledger bracket and one pipelined cell write for
     * the commit, however its writes were split into calls.
     *
     * The reaction used to be per CALL, and calls are not shaped by trust: the
     * bulk path's address-keyed supersession is one `putIfNewer` per event,
     * its read-then-supersede one `removeDocs` then one `putAll`. Measured on
     * one GrapeRank assistant run (20,000 cards by one service, batch 1000,
     * address-keyed), every card paid its own bracket and cell write — 1,000
     * `proj.write` calls and 3,000 marker round trips per batch, the marker
     * ones all updating ONE document 32 callers deep (78 ms each): 139 cards/s,
     * ~18 minutes for the 150,000-card run the publisher re-signs every five.
     * And the remove-then-put retracted every superseded card's cell for the
     * put to write it straight back. Batching at the commit fixes every path at
     * once, including the ones no one has written yet, with no port member to
     * add — the alternative tried first (a batch form of each call) had to be
     * rediscovered by every caller that loops.
     *
     * WHAT IS RECORDED, AND WHY THE NET IS EXACT. A card's cell is a function
     * of the newest stored version at its (service, subject) address alone, so
     * each call records, per address, only the last thing that happened to it
     * — stored, or removed — and the flush applies exactly that. Calls on ONE
     * address arrive in order (the store sequences an address's run; distinct
     * addresses are distinct cells), so last-recorded is last-happened. A
     * removal followed by its winner is therefore one update, not a retraction
     * and a re-write; a card stored and then deleted in the same commit is one
     * retraction. A 10040's reaction stays INLINE — the provider map dropped
     * the moment the list is stored — so a later call in the commit judges
     * fresh services, and the flush applies cards, against the map that
     * includes it.
     *
     * CRASH SAFETY is the ledger's, one bracket wide: [planned] (the events the
     * store is about to commit) is insured before anything is written, and
     * each call extends the cover with whatever it was not told about before
     * its own write ([ProjectionLedger.Cover]) — so a pre-declared batch writes
     * the marker once, and the commit's 32 concurrent conditional puts write it
     * not at all. A failure anywhere queues the whole cover.
     *
     * Atomic against the drain for the same reason the per-call bracket was:
     * the whole commit, flush included, runs under the caller's trust gate. A
     * call made OUTSIDE any commit (a test, a direct caller) opens its own, so
     * it behaves exactly as the per-call reaction did. Nested commits join the
     * outer one. A long commit flushes every [FLUSH_AT] addresses, so its buffer
     * stays bounded; flushes are serialized and drain the buffer in order.
     */
    internal suspend fun <T> commit(
        planned: List<Event> = emptyList(),
        body: suspend () -> T,
    ): T {
        if (currentCoroutineContext()[CommitScope] != null) return body()
        val insurance = insuranceForPlanned(planned)
        return backlog.insuring(insurance) { cover ->
            val scope = CommitScope(cover, coarse = insurance.toRewalk.isNotEmpty())
            val result = withContext(scope) { body() }
            flush(scope)
            Outcome(result, scope.workLeft())
        }
    }

    /** Run one call's write [op] inside the current commit — or a commit of its own — insured by [puts] and [removals] before it writes. */
    private suspend fun <T> reacting(
        puts: List<EventDoc>,
        removals: List<EventDoc>,
        fresh: Set<String>,
        op: suspend (CommitScope) -> T,
    ): T {
        val scope = currentCoroutineContext()[CommitScope] ?: return commit { reacting(puts, removals, fresh, op) }
        // A REMOVAL is the one point that settles what the commit buffered so
        // far, mid-commit: every removal path is a single sequential call (a
        // supersession's sweep, a deletion, a vanish page), never part of the
        // conditional puts' fan-out, so nothing else is usually in flight —
        // which is what [ProjectionLedger.Cover.retire] needs to shrink the
        // marker. A long removal commit (a vanish of a scoring service, page by
        // page) thereby holds one page of insurance at a time instead of all of
        // it. Puts never trigger this: a put cannot be undone by a later call
        // in a way that costs a write, while a removal can be refilled by the
        // winner the NEXT call stores — the read-then-supersede path's
        // removeDocs-then-putAll — and settling between the two is exactly the
        // retract-then-rewrite the commit exists to avoid.
        if (removals.isNotEmpty()) settle(scope)
        // A commit insured by SERVICE (a planned batch past DIRT_SUBJECT_CAP)
        // keeps its puts insured by service: per-subject cover here would be a
        // marker write per card, the very cost the commit exists to remove.
        // [fresh] counts as named: this call's own list is about to name it.
        val insures = insurableSigners(fresh)
        val putCover = if (scope.coarse) servicesOf(puts, insures) else insuranceForPuts(puts, insures)
        scope.cover.extend(putCover + insuranceForRemovals(removals) + ProjectionWork(emptySet(), fresh), call = true)
        val result =
            try {
                op(scope)
            } finally {
                scope.cover.landed()
            }
        // The buffer's bound, for puts: their cells written now, removals kept
        // for a winner that may still refill them (see above).
        if (scope.pendingStored() >= FLUSH_AT) flush(scope, storedOnly = true)
        return result
    }

    /** Apply everything [scope] buffered, then retire the insurance it no longer needs — see [reacting]. */
    private suspend fun settle(scope: CommitScope) {
        val written = flush(scope, storedOnly = false)
        // Not the subjects still owed a derive: they stay insured until the drain.
        scope.cover.retire(written - scope.rederiving())
    }

    /**
     * Supersession of a trust doc is ONE reaction: the winner's cells. The
     * versions it replaces need no hook — the cell is keyed by the address
     * they share, so the winner's `add` overwrites it and its `remove` of a
     * dropped tag retracts it — so the read-then-supersede default, which
     * would route each replaced version through [removeDocs], is replayed here
     * on the INNER index and only the winner is recorded. Non-trust kinds go
     * straight through.
     */
    override suspend fun putIfNewer(doc: EventDoc): Boolean {
        if (doc.kind !in TRUST_KINDS) return inner.putIfNewer(doc)
        // A 10040's fresh services must be judged against the map BEFORE the
        // write lands, else a first-ever map read would already hold the new
        // list and the walk that projects its service would never run.
        val fresh = if (doc.kind == TrustProviderListEvent.KIND) freshServicesOf(listOf(doc)) else emptySet()
        return reacting(listOf(doc), emptyList(), fresh) { scope ->
            val stored = if (inner.supersedesViaPut) inner.putIfNewer(doc) else supersedeReading(doc)
            // A loser records nothing: its fresh services were never written.
            if (stored) stored(scope, listOf(doc), fresh)
            stored
        }
    }

    /** [EventIndex.putIfNewer]'s read-then-supersede, against [inner] alone (see [putIfNewer]). */
    private suspend fun supersedeReading(doc: EventDoc): Boolean {
        val address =
            doc.addressOrNull() ?: run {
                inner.put(doc)
                return true
            }
        val dTag = doc.dTagOrEmpty()
        val q =
            if (doc.kind.isAddressable() && dTag.isNotEmpty()) {
                EventQuery(kinds = listOf(doc.kind), authors = listOf(doc.pubkey), tags = mapOf("d" to listOf(dTag)))
            } else {
                EventQuery(kinds = listOf(doc.kind), authors = listOf(doc.pubkey))
            }
        val existing = inner.search(q).filter { it.addressOrNull() == address }
        val incumbent = existing.minWithOrNull(EventDoc.NEWEST_FIRST)
        if (incumbent != null && EventDoc.NEWEST_FIRST.compare(doc, incumbent) >= 0) return false
        if (existing.isNotEmpty()) inner.removeDocs(existing)
        inner.put(doc)
        return true
    }

    override suspend fun put(doc: EventDoc) {
        val fresh = if (doc.kind == TrustProviderListEvent.KIND) freshServicesOf(listOf(doc)) else emptySet()
        reacting(listOf(doc), emptyList(), fresh) { scope ->
            inner.put(doc)
            stored(scope, listOf(doc), fresh)
        }
    }

    /**
     * The bulk path: cards land as tensor-cell UPDATES with ZERO reads, one
     * update per card (its service's cell on its subject), written when the
     * commit flushes. Supersession guarantees each card here is the newest for
     * its (service, subject) address, and the cell is keyed by exactly that
     * service, so the update IS the derivation. A card that lost a tag drops
     * that dimension's cell in the same update (a tensor `remove`), so the one
     * thing left for the read-based derive is a card that fails to reconstruct.
     */
    override suspend fun putAll(docs: List<EventDoc>) {
        val lists = docs.filter { it.kind == TrustProviderListEvent.KIND }
        val fresh = if (lists.isEmpty()) emptySet() else freshServicesOf(lists)
        reacting(docs, emptyList(), fresh) { scope ->
            IngestStats.timed("write") { inner.putAll(docs) }
            stored(scope, docs, fresh)
        }
    }

    /**
     * Stored trust docs, recorded: the provider map dropped NOW after any
     * 10040 (so the rest of the commit sees the list), its [fresh] services
     * owed a walk, and the cards left for the flush.
     */
    private fun stored(
        scope: CommitScope,
        docs: List<EventDoc>,
        fresh: Set<String>,
    ) {
        if (docs.any { it.kind == TrustProviderListEvent.KIND }) recompute.invalidateProviders()
        // HERE, once the list is stored and its walk is about to be queued —
        // not where [fresh] is judged: a list that loses supersession queues
        // nothing, and releasing its services' skips then would lose them.
        recompute.walkQueued(fresh)
        scope.walk(fresh)
        scope.record(docs.filter { it.kind == UserAssertionEvent.KIND }, stored = true)
    }

    /**
     * Removed trust docs, recorded. A removed 10040 drops the provider map
     * now; the cells its services own stay — no lens resolves to an unnamed
     * service, so they are dead weight for the orphan sweep.
     */
    private fun removed(
        scope: CommitScope,
        docs: List<EventDoc>,
    ) {
        if (docs.any { it.kind == TrustProviderListEvent.KIND }) recompute.invalidateProviders()
        scope.record(docs.filter { it.kind == UserAssertionEvent.KIND }, stored = false)
    }

    /**
     * Apply what the commit recorded since the last flush, in ONE pass: the
     * stored cards as cell updates ([TrustRecompute.applyCards], under the
     * provider map as it stands now), the removed ones as cell removes. Each
     * address appears once, so the updates and the removes touch disjoint
     * cells and their order does not matter.
     *
     * A removed card is exact for ranking: the store holds one version per
     * (service, subject) address, so the removed card was the one backing the
     * cell. What a removal leaves behind is bookkeeping, not drift: a parent
     * with no cells left, which may now read high — an upper bound still, so
     * the descent stays sound and only pays a rung; both are tightened by a
     * derive (the crash marker, a verify with repair).
     *
     * A winner that REFILLED an address this commit emptied writes the cell
     * itself, so no retraction is sent — unless its signer is one no 10040
     * maps, which writes no cell at all: then the predecessor's cell (left by
     * a service that went unnamed since) is retracted, as its removal alone
     * would have. A winner that fails to reconstruct is derive work, and the
     * derive writes the subject's exact parent, stale cell included.
     */
    private suspend fun flush(
        scope: CommitScope,
        storedOnly: Boolean = false,
    ): Set<String> {
        val written = LinkedHashSet<String>()
        scope.flushing(storedOnly) { ops ->
            val puts = ops.filter { it.stored }
            val map = if (puts.isEmpty()) null else recompute.providerMap()
            if (map != null) scope.rederive(recompute.applyCards(puts.map { it.doc }, map))
            val removals = ArrayList<CellRemoval>()
            for (op in ops) {
                val subject = subjectOf(op.doc) ?: continue
                written += subject
                if (op.stored && (!op.refills || map?.maps(op.doc.pubkey) == true)) continue
                removals += CellRemoval(subject, ServiceKey(op.doc.pubkey), influence = true, followers = true)
            }
            if (removals.isNotEmpty()) IngestStats.timed("proj.write") { reputations.removeCells(removals) }
        }
        return written
    }

    /** The services [lists] name that the CURRENT map does not — the ones whose stored cards have no cells yet. Read before the lists are written. */
    private suspend fun freshServicesOf(lists: List<EventDoc>): Set<String> {
        val named = ProviderMap.trustServicesOf(lists)
        if (named.isEmpty()) return emptySet()
        val before = recompute.providerMap()
        return named.filterNot(before::maps).toSet()
    }

    /**
     * CATCH UP WITH 10040s ANOTHER PROCESS STORED — the provider refresher's
     * tick (`NostrSemanticsStore.refreshTrustProviders`).
     *
     * The rebuild runs unlocked, like any pass. What it may leave is WORK:
     * cards this process skipped because their signer was unnamed here, whose
     * signer the pass now names (the sync mirroring cards for a service first
     * named on the relay's socket). Their walk is queued exactly as a local
     * 10040 write queues one — through [ProjectionLedger.insuring], under
     * [underTrustLock], which must hold BOTH writer locks: the ledger's stamps
     * are only ordered under the writes mutex, and an inline settle drains
     * under the gate. Checked even when the rebuild throws, against whatever
     * pass is cached — a cold read may have built one — and the lock is
     * skipped when there is nothing to queue, so a quiet tick never waits
     * behind a drain.
     */
    internal suspend fun refreshProviders(underTrustLock: suspend (suspend () -> Unit) -> Unit) {
        try {
            recompute.refreshProviders()
        } finally {
            if (recompute.hasSkippedNowNamed()) underTrustLock { queueSkippedWalks() }
        }
    }

    private suspend fun queueSkippedWalks() {
        val found = recompute.takeSkippedNowNamed()
        if (found.isEmpty()) return
        val walk = ProjectionWork(emptySet(), found)
        try {
            backlog.insuring(walk) { Outcome(Unit, walk) }
        } catch (t: Throwable) {
            // Only what never reached the ledger goes back. [insuring] queues
            // before an inline settle can throw, and a queued walk that then
            // fails stays in the ledger for the drain to retry; handing it back
            // too would re-queue it every tick, each time with a new stamp that
            // drops the provider cache again.
            recompute.giveBackSkipped(found - backlog.queuedRewalks())
            throw t
        }
    }

    override suspend fun remove(id: String) {
        // The doomed doc says what the removal invalidates — read before deleting.
        val doc = inner.get(id)
        if (doc != null) removeDocs(listOf(doc)) else inner.remove(id)
    }

    /**
     * Bulk remove by id: only TRUST docs (30382/10040) can invalidate the
     * projection, so read back just those via chunked kind-filtered searches —
     * never a get per id — then delete pipelined, recorded once for the batch.
     */
    override suspend fun removeAll(ids: List<String>) {
        val docs =
            ids
                .chunked(REMOVE_CHUNK)
                // `complete`, like every read that feeds a write: a short answer
                // here is a removed card whose subject is never re-derived, so its
                // cells linger after a kind-5 — refusing beats silent drift.
                .mapBounded(QUERY_FANOUT) { chunk -> inner.search(EventQuery(ids = chunk, kinds = TRUST_KINDS, complete = true)) }
                .flatten()
        if (docs.isEmpty()) return inner.removeAll(ids)
        val byId = docs.associateBy { it.id }
        val plain = ids.filter { it !in byId }
        reacting(emptyList(), docs, emptySet()) { scope ->
            if (plain.isNotEmpty()) inner.removeAll(plain)
            inner.removeDocs(docs)
            removed(scope, docs)
        }
    }

    /**
     * Bulk remove for callers that already HOLD the doomed docs: ZERO reads —
     * the docs themselves say what each removal invalidates.
     */
    override suspend fun removeDocs(docs: List<EventDoc>) {
        if (docs.none { it.kind in TRUST_KINDS }) return inner.removeDocs(docs)
        reacting(emptyList(), docs, emptySet()) { scope ->
            // Timed for symmetry with putAll's `write`: this is a supersession's
            // other half — the sweep of the versions the winner replaced — on
            // the same bulk path inside the same writer lock, so leaving it
            // untimed reads as "the write is all there is".
            IngestStats.timed("remove") { inner.removeDocs(docs) }
            removed(scope, docs)
        }
    }

    /**
     * Crash insurance for a PUT of [docs]: the cards whose signer [insures]
     * admits ([insurableSigners]). Card subjects are recorded exactly
     * while they fit [DIRT_SUBJECT_CAP]; a bigger batch records the cards'
     * SERVICES instead — few keys however large the batch, repaired by
     * re-walking each service (safe for puts: the stored cards make the walk
     * reach every touched subject). A 10040 insures nothing — its walk is
     * declared as work, not insured.
     */
    private fun insuranceForPuts(
        docs: List<EventDoc>,
        insures: (String) -> Boolean,
    ): ProjectionWork = putCover(docs, { it.kind }, ::subjectOf, { it.pubkey }, insures)

    /** [insuranceForPuts] coarsened outright: the services of [docs]' insurable cards. */
    private fun servicesOf(
        docs: List<EventDoc>,
        insures: (String) -> Boolean,
    ): ProjectionWork = ProjectionWork(emptySet(), docs.filter { it.kind == UserAssertionEvent.KIND && insures(it.pubkey) }.mapTo(LinkedHashSet()) { it.pubkey })

    /**
     * [insuranceForPuts] for the EVENTS a commit is about to write, before any
     * is converted — the same rule and cap, by construction ([putCover]), so
     * the planned cover and each call's own cover name the same keys. A 10040
     * among them counts its services as named, as a call's `fresh` does: the
     * commit is about to store it, and its cards would then write cells.
     */
    private fun insuranceForPlanned(events: List<Event>): ProjectionWork {
        val lists = events.mapNotNull { it as? TrustProviderListEvent }
        val named = if (lists.isEmpty()) emptySet() else ProviderMap.providersIn(lists).services
        return putCover(events, { it.kind }, ::subjectOf, { it.pubKey }, insurableSigners(named))
    }

    /**
     * WHOSE CARDS NEED INSURANCE: only a signer some 10040 maps, or one in
     * [named]. [TrustRecompute.applyCards] writes no cell for any other
     * signer, so a crash between such a card's write and the flush leaves
     * nothing to repair — insuring it bought a marker write-ahead and its
     * clear, two serial round trips under the trust gate, for no cell at all.
     * A mirror's by-kind card ingest is mostly this (issue #183).
     *
     * Read off the provider map's PEEK ([TrustRecompute.liveProviderMap]): no
     * I/O on the write path, and no new way for a commit to fail. Everything
     * is insured, as before, whenever the peek cannot answer: no pass built
     * yet, one dropped by a 10040 write since, or a pass naming no service at
     * all — which is a relay holding no 10040s OR an engine not yet serving
     * them, and only the second needs the insurance ([ProviderMap.get] refuses
     * to cache that emptiness for the same reason).
     *
     * THE WINDOW THIS OPENS, stated so nobody reads it as a guarantee: a pass
     * that newly names a signer can land between a card's cover and the
     * commit's flush — a refresher tick carrying another process's 10040 —
     * and a crash in that gap leaves the card's cell unwritten and unnamed.
     * That is the drift a commit already leaves when the map is stale at its
     * FLUSH (the skip is remembered only in memory, [ProviderMap.noteSkipped]),
     * a gap one commit wide beside it, and the reconciler's to repair. This
     * process's own 10040s never open it: their services are [named] here,
     * and each insures its walk before the list is written.
     */
    private fun insurableSigners(named: Set<String>): (String) -> Boolean {
        val map = recompute.liveProviderMap()
        if (map == null || map.services.isEmpty()) return { true }
        return { map.maps(it) || it in named }
    }

    /** The one put-insurance rule, for docs and events alike: insurable card subjects under [DIRT_SUBJECT_CAP], else their services. */
    private inline fun <T> putCover(
        items: List<T>,
        kind: (T) -> Int,
        subject: (T) -> String?,
        service: (T) -> String,
        insures: (String) -> Boolean,
    ): ProjectionWork {
        val subjects = LinkedHashSet<String>()
        for (item in items) if (kind(item) == UserAssertionEvent.KIND && insures(service(item))) subject(item)?.let(subjects::add)
        if (subjects.size <= DIRT_SUBJECT_CAP) return ProjectionWork(subjects, emptySet())
        val services = LinkedHashSet<String>()
        for (item in items) if (kind(item) == UserAssertionEvent.KIND && insures(service(item))) services += service(item)
        return ProjectionWork(emptySet(), services)
    }

    /** What a REMOVE of [docs] insures: card subjects exactly — a lost cell remove is repaired by the exact derive, which also drops an emptied parent. */
    private fun insuranceForRemovals(docs: List<EventDoc>): ProjectionWork {
        val subjects = LinkedHashSet<String>()
        for (doc in docs) if (doc.kind == UserAssertionEvent.KIND) subjectOf(doc)?.let(subjects::add)
        return ProjectionWork(subjects, emptySet())
    }

    /** The last thing a commit did to one card address: stored [doc], or removed it; [refills] when a removal preceded the store. */
    private class CardOp(
        val doc: EventDoc,
        val stored: Boolean,
        val refills: Boolean,
    )

    /**
     * One commit's recorded reactions — a coroutine-context element, so the
     * calls of the commit find it from whichever child coroutine makes them
     * (the bulk path fans its conditional puts out [com.nosfabrica.vespa.eventstore.engine.async.PUT_FANOUT] wide).
     */
    private class CommitScope(
        val cover: ProjectionLedger.Cover,
        /** Insured by service (a planned batch past [DIRT_SUBJECT_CAP]): later puts are too. */
        val coarse: Boolean,
    ) : AbstractCoroutineContextElement(CommitScope) {
        companion object Key : CoroutineContext.Key<CommitScope>

        private val lock = Any()
        private var ops = LinkedHashMap<String, CardOp>()
        private var storedCount = 0
        private val walks = LinkedHashSet<String>()
        private val rederive = LinkedHashSet<String>()
        private val flushLock = Mutex()

        fun record(
            cards: List<EventDoc>,
            stored: Boolean,
        ) {
            if (cards.isEmpty()) return
            synchronized(lock) {
                for (doc in cards) {
                    val address = doc.addressOrNull() ?: continue
                    val prior = ops[address]
                    if (prior?.stored == true) storedCount--
                    if (stored) storedCount++
                    ops[address] = CardOp(doc, stored, refills = stored && prior != null && (!prior.stored || prior.refills))
                }
            }
        }

        /** Stored ops waiting — the buffer [FLUSH_AT] bounds; removals wait for a refill (see [reacting]). */
        fun pendingStored(): Int = synchronized(lock) { storedCount }

        fun rederiving(): Set<String> = synchronized(lock) { rederive.toSet() }

        fun walk(services: Set<String>) {
            if (services.isNotEmpty()) synchronized(lock) { walks += services }
        }

        fun rederive(subjects: Set<String>) {
            if (subjects.isNotEmpty()) synchronized(lock) { rederive += subjects }
        }

        fun workLeft(): ProjectionWork = synchronized(lock) { ProjectionWork(rederive.toSet(), walks.toSet()) }

        /**
         * Take what is recorded so far — every op, or with [storedOnly] the
         * stored ones, leaving removals for a refill — and [apply] it.
         * Serialized, so two flushes land in the order they took.
         */
        suspend fun flushing(
            storedOnly: Boolean,
            apply: suspend (List<CardOp>) -> Unit,
        ) = flushLock.withLock {
            val batch =
                synchronized(lock) {
                    if (storedOnly) {
                        val taken = ops.values.filter { it.stored }
                        ops.values.removeAll { it.stored }
                        storedCount = 0
                        taken
                    } else {
                        ops.values.toList().also {
                            ops = LinkedHashMap()
                            storedCount = 0
                        }
                    }
                }
            if (batch.isNotEmpty()) apply(batch)
        }
    }

    internal companion object {
        /** The only kinds whose write or removal can touch the projection — what the store's trust gate is taken for. */
        val TRUST_KINDS = listOf(UserAssertionEvent.KIND, TrustProviderListEvent.KIND)

        /** Ids per removeAll read-back query — round-trip width, not a result cap. */
        const val REMOVE_CHUNK = 500

        /**
         * Max subjects persisted per put insurance before coarsening to services
         * — bounds the marker write to noise against the batch it brackets while
         * keeping the precise, cheaper repair for normally-sized batches.
         */
        const val DIRT_SUBJECT_CAP = 5_000

        /**
         * Card addresses a commit buffers before it flushes mid-way — a bound
         * on the buffer, not a batching knob: a relay's ingest batch (1000 by
         * default, the mirror lane's 20,000 at most) flushes once at its end,
         * and only a commit bigger than that splits. Past ~1000 cells a
         * flush is throughput-bound (5000 cells measured 16% cheaper per cell
         * than 1000), so a bigger buffer would buy nothing.
         */
        internal const val FLUSH_AT = 20_000
    }
}
