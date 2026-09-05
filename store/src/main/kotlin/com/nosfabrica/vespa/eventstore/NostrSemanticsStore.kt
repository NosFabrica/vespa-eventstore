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
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.forEachBounded
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.ingest.BulkMixedInsert
import com.nosfabrica.vespa.eventstore.ingest.BulkRecordInsert
import com.nosfabrica.vespa.eventstore.ingest.GuardOwners
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.nosfabrica.vespa.eventstore.mapping.VespaText
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.nosfabrica.vespa.eventstore.mapping.toEventQuery
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.nosfabrica.vespa.eventstore.search.SearchReferenceExpansion
import com.nosfabrica.vespa.eventstore.search.SearchReferences
import com.nosfabrica.vespa.eventstore.search.SubjectKeys
import com.nosfabrica.vespa.eventstore.trust.Delegations
import com.nosfabrica.vespa.eventstore.trust.Enrolment
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip01Core.store.owner
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip40Expiration.isExpired
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.coroutineContext

/**
 * The Nostr-semantics layer: a Quartz [IEventStore] backed by the search
 * engine itself — one copy of the data, queryable with full NIP-01 filters
 * plus NIP-50 search. It is engine-agnostic (any [EventIndex] works, including
 * the in-memory one); [VespaEventStore.open] assembles it over Vespa.
 *
 * [insertLocked] enforces the Nostr write rules: dedup ("duplicate:"),
 * replaceable/addressable supersession with the NIP-01 tiebreak — same
 * created_at, LOWEST id wins — ("replaced:"), NIP-09 deletions and NIP-62
 * vanishes ("blocked:", enforcement in [Deletions], keyed on the event's
 * OWNER — the gift-wrap recipient for kind 1059, else the author),
 * already-expired events rejected and due expirations swept (NIP-40), and
 * ephemeral kinds accepted WITHOUT storing (an observable wrapper still
 * broadcasts them live).
 *
 * NIP-50: only kinds implementing SearchableEvent are searchable, via
 * [SearchExtractors]. Filters arrive with `search` verbatim; this store
 * interprets the `sort:`/`filter:rank:`/`include:spam`/`observer:` extensions
 * and the `-word` / `"exact phrase"` term syntax, and ignores extensions it
 * doesn't know. A resolved observer also trust-gates PLAIN recall — the
 * observer gate, see [toExpiryQuery].
 *
 * Correctness rests on two properties: all writes serialize behind one
 * [Mutex], so query-then-write is atomic against other writers in this
 * process; and [EventIndex] guarantees an acked put is visible to search.
 * There are no cross-document transactions — [transaction] buffers and applies
 * sequentially without rollback. Both properties are about writes THIS process
 * makes; when another process feeds the same index, say so with [writers] —
 * the guard-owner cache is the one piece of state that would otherwise trust
 * this process's writes to be all of them.
 *
 * Events are NOT verified here; verification is the ingest path's job, once,
 * before insert.
 */
class NostrSemanticsStore(
    private val index: EventIndex,
    override val relay: NormalizedRelayUrl? = null,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
    /**
     * Whether anything ELSE writes [index] — an assertion only the caller can
     * make, since the store cannot see a second feeder. The guard-owner cache is
     * self-maintaining only for a writer that sees all of its owners' guards, so
     * the default [WriterTopology.SHARED_STRICT] caches nothing and probes every
     * insert. See [WriterTopology].
     */
    writers: WriterTopology = WriterTopology.SHARED_STRICT,
    /** Guard-cache rebuild cadence under [WriterTopology.SHARED]; 0 disables the refresher. */
    guardRefreshMillis: Long = DEFAULT_GUARD_REFRESH_MILLIS,
    /** Events removed per sweep round (see [Deletions]). Internal — a test seam, like [nowSecs]. */
    internal val sweepPage: Int = 10_000,
    /**
     * How much of a searching read's answer may be events it POINTS AT rather
     * than events it matched — see [SearchReferenceExpansion]. Always applied,
     * because it only ever engages on a read carrying TERMS: a plain NIP-01
     * recall, a mirror's paging, a NIP-77 catch-up and this store's own
     * provider-list read all carry none, and are left exactly as they were.
     * [SearchExpansionLimits.Off] turns it off outright.
     */
    private val searchExpansion: SearchExpansionLimits = SearchExpansionLimits.Default,
    /**
     * HOW MANY HITS ONE AUTHOR MAY HOLD ON A RANKED PAGE, or null — the
     * default — for no cap at all.
     *
     * Ranking answers "how good is this document"; nothing in it answers "how
     * many of these does one page need". A single Wikipedia-mirror bot took 27
     * of the top 50 for `Verified Human` (measured 2026-09-01), and
     * `rank_cases.json`'s `jack` row has the same shape from a different
     * author — 13 of 40 slots, one account's identical kind-20 pictures. Both
     * are one document being right many times, which is a DIVERSITY problem
     * and not a relevance one: no rung can fix it, because every one of those
     * documents genuinely earned its rung.
     *
     * OFF BY DEFAULT, and this is a deliberate refusal rather than caution. A
     * cap DROPS events a filter matched, so a page can come back shorter than
     * the limit the client asked for — a NIP-01 answer with rows missing, which
     * is the operator's call to make and not a library's. The store adds no
     * cap of its own anywhere else either (see [count]); bounding a read is the
     * FILTER's job.
     *
     * RANKED PAGES ONLY when it is on. A recency-ordered recall is a mirror
     * paging a corpus or a NIP-77 catch-up, where dropping an author's events
     * is data loss rather than an editorial choice; only a relevance page is
     * making a judgement that a cap can participate in.
     */
    private val maxHitsPerAuthor: Int? = null,
) : IEventStore {
    private val writes = Mutex()

    /**
     * THE TRUST GATE, separate from [writes] since 2026-09-04.
     *
     * One mutex used to serialise every write in this store, and the hazard it
     * was documented against is recompute-versus-recompute: "repairs must not
     * race live inserts' recomputes". A plain kind-1 note has NO recompute —
     * [TrustProjection.opDirt] returns `Dirt.NONE` for every kind but 30382 and
     * 10040 — so it was excluded against work it cannot conflict with. Measured
     * on staging: an ephemeral event, which takes the lock and returns without
     * storing anything, took 35-41 SECONDS to answer OK while the trust drain
     * held the lock re-deriving reputation documents.
     *
     * What a plain insert genuinely needs exclusion for is the DELETION race —
     * check `isDeleted`, then put, with a kind-5 landing in between would
     * resurrect a deleted event. That is event-document work and stays on
     * [writes]. Reputation-document work moves here.
     *
     * LOCK ORDER, where both are needed (a card insert does inline projection):
     * [trustGate] FIRST, then [writes] — never the reverse. The order was
     * writes-then-gate when the split shipped, and that leaked the drain's
     * stall back onto every plain writer: a card took [writes] and then waited
     * for the gate WHILE HOLDING IT, so for the length of a drain slice every
     * kind-1 in the process queued behind the card. Gate first means a card
     * waits for the drain holding nothing, and once it has the gate it takes
     * [writes] for one short hold. The drain and the reconciler only ever
     * hold the gate, so the pair cannot deadlock as long as every two-lock
     * path here goes through [gated].
     */
    private val trustGate = Mutex()

    // Owners with any stored tombstone/vanish; everyone else's inserts skip the
    // NIP-09/62 guard probes entirely (see GuardOwners for the safety argument).
    private val guards = GuardOwners(index, writers, guardRefreshMillis)

    private val deletions = Deletions(index, relay, sweepPage)

    /**
     * WHO THIS READER ASKED TO COMPUTE WHAT — the gate the search expansion
     * checks before unpacking a Trusted List or a NIP-85 assertion.
     *
     * Read off [TrustProjection] rather than derived here, and the type check is
     * the honest statement of the dependency: the projection is the decorator
     * every write already passes through, so its [ProviderMap] is invalidated by
     * every 10040 put and remove with no write-path code of its own. A second
     * cache in this class would need its own hook on four separate write entry
     * points and would still be a second answer to one question.
     *
     * A store assembled WITHOUT the projection — a bare index in a test — has no
     * Map to read and so admits no declaration. Labels are unaffected: NIP-32 is
     * ungated by design, and this gate never applied to them.
     */
    private suspend fun delegations(): Delegations = (index as? TrustProjection)?.recompute?.delegations() ?: Delegations.NONE

    /**
     * THE LENS, RESOLVED: the reputation tensors are keyed by SERVICE key, so
     * a query carrying an observer is handed the service their kind 10040
     * names per dimension ([EventQuery.rankKey], [EventQuery.followersKey])
     * off the projection's cached provider map — the same pass the write
     * side and the search gate read, invalidated by every 10040 write. An
     * observer with no stored list resolves to no key and ranks as trusting
     * nobody, which is what an observer with no cells ranked as before. A
     * store assembled without the projection has no map and resolves nothing.
     */
    private suspend fun lensed(queries: List<EventQuery>): List<EventQuery> {
        if (queries.none { it.observer != null }) return queries
        val recompute = (index as? TrustProjection)?.recompute ?: return queries
        val providers = recompute.providerMap()
        return queries.map { q ->
            val lens = providers.lensOf(q.observer ?: return@map q)
            q.copy(rankKey = lens.rank, followersKey = lens.followers)
        }
    }

    private val bulkRecords = BulkRecordInsert(index, relay, guards)

    private val bulkMixed = BulkMixedInsert(index, relay, nowSecs, guards)

    /** A writer-lock label's two [IngestStats] stage names, interned at construction. */
    private class LockStage(
        name: String,
    ) {
        val wait = "$name.wait"
        val hold = "$name.hold"
    }

    /**
     * Take [writes], booking the WAIT and the HOLD under separate [IngestStats]
     * stages named for [stage].
     *
     * Every other stage timer starts once the lock is already held, so a writer
     * starved by another holder would otherwise show up as fast stages and a
     * stalled pipeline with nothing naming the reason. The deferred trust
     * projection makes that real: it re-derives off the ingest path but INSIDE
     * this lock (DirtLedger.drain's gate), so `proj.fetch` and an ingest commit
     * contend for one mutex while both look cheap individually. `lock.*.wait`
     * makes that visible; `lock.*.hold` attributes it.
     */
    private suspend fun <T> locked(
        stage: LockStage,
        body: suspend () -> T,
    ): T = lockedOn(writes, stage, body)

    /** [locked], on a named mutex — see [trustGate] for why there are two. */
    private suspend fun <T> lockedOn(
        mutex: Mutex,
        stage: LockStage,
        body: suspend () -> T,
    ): T {
        val requested = System.nanoTime()
        var acquired = 0L
        try {
            return mutex.withLock {
                acquired = System.nanoTime()
                // Live holder, for the question the cumulative stages cannot
                // answer: not "the gate was held for 24 minutes since boot"
                // but "the gate is held RIGHT NOW, by this, for this long".
                // Two volatile writes per critical section, against a section
                // that is measured in seconds.
                IngestStats.beginHold(stage.hold)
                try {
                    body()
                } finally {
                    IngestStats.endHold(stage.hold)
                }
            }
        } finally {
            // Booked AFTER release: recording inside would put two map lookups
            // and two atomic adds in the critical section this exists to
            // measure, and `hold` would stop short of the actual release.
            // acquired == 0 means the lock was never taken (cancelled while
            // waiting) — nothing to attribute.
            if (acquired != 0L) {
                val released = System.nanoTime()
                IngestStats.add(stage.wait, acquired - requested)
                IngestStats.add(stage.hold, released - acquired)
            }
        }
    }

    /**
     * Trust-relevant writes take BOTH gates; everything else takes only
     * [writes].
     *
     * CONSERVATIVE BY CONSTRUCTION — the question asked is "could this write
     * change a reputation document?", and anything that might answers yes:
     * a contact card (30382) and a provider list (10040) are the two kinds
     * [TrustProjection.opDirt] books work for, and a deletion or a
     * request-to-vanish can REMOVE one, which is trust work through
     * `TrustProjection.remove`. A kind-1 note, a reaction, a repost and a zap
     * are none of those, and they are the overwhelming majority of what a
     * relay is asked to store.
     */
    private fun touchesTrust(event: Event): Boolean =
        event.kind == ContactCardEvent.KIND ||
            event.kind == TrustProviderListEvent.KIND ||
            event is DeletionEvent ||
            event is RequestToVanishEvent

    /**
     * THE ONE TWO-LOCK SHAPE: [trustGate] when [trust], then [writes] under
     * [stage]. Every path that needs both goes through here, which is what
     * makes the order (see [trustGate]) a property of the file rather than of
     * each call site.
     *
     * The gate wait is charged to its OWN stage, not to LOCK_GATE: `lock.gate.*`
     * is the drain's, and folding an insert's wait for the drain into the same
     * name would make "the drain is slow" and "a card is waiting for the
     * drain" one number. They have different remedies.
     */
    private suspend fun <T> gated(
        trust: Boolean,
        stage: LockStage,
        /** The stage the GATE wait is booked under — ingest's by default; a sweep or a reindex names its own, so a sweep waiting on the drain does not read as "a card is waiting". */
        gateStage: LockStage = LOCK_INGEST_TRUST,
        body: suspend () -> T,
    ): T = if (trust) lockedOn(trustGate, gateStage) { locked(stage) { body() } } else locked(stage) { body() }

    private suspend fun <T> lockedForWrite(
        event: Event,
        body: suspend () -> T,
    ): T = gated(touchesTrust(event), LOCK_INGEST, body = body)

    override suspend fun insert(event: Event) = lockedForWrite(event) { insertLocked(event) }

    /**
     * Run [body] under this store's TRUST writer lock. For the trust
     * reconciler's mutating batches: its repairs derive from a read of the
     * corpus, and racing a live insert would let a derivation from pre-write
     * state land after the insert's own recompute. NOT reentrant (a plain
     * [Mutex]): never call from a path that already holds the lock.
     *
     * Booked under [LOCK_GATE], separately from ingest's own acquisitions: the
     * callers are the projection drain and the reconciler, and telling their
     * hold apart from ingest's is the entire point of the split.
     *
     * ON [trustGate], NOT [writes], since 2026-09-04: this is
     * reputation-document work, so holding it no longer stalls a kind-1 insert
     * that has no reputation work to do. Writes that DO touch reputation still
     * queue for it — see [touchesTrust].
     */
    internal suspend fun <T> withWriteLock(body: suspend () -> T): T = lockedOn(trustGate, LOCK_GATE) { body() }

    /**
     * [lockedForWrite] for a BATCH that must stay whole: the trust gate is
     * taken when ANY event in it touches reputation.
     *
     * The bulk path writes reputation state inline — `TrustProjection.putAll`
     * ends in `reputations.updateCells` for the cards in the batch — so a batch
     * carrying one card mutates the same documents the drain re-derives.
     * Missing this is how the trust-gate split first shipped: `insert` took
     * both locks and `batchInsert` took neither, which left the mirror's bulk
     * card ingest racing the drain with no mutual exclusion at all. The
     * single-lock design could not have this bug; the split has to earn its
     * exclusion at every write entry point, and there are several.
     */
    private suspend fun <T> lockedForBatch(
        events: List<Event>,
        body: suspend () -> T,
    ): T = gated(events.any { touchesTrust(it) }, LOCK_INGEST, body = body)

    /**
     * Batches take a BULK path — the per-event path costs 3–5 index round
     * trips each, which caps ingest in the low thousands per second. Two
     * shapes, by whether the batch mutates via deletions:
     *
     *  - PURE RECORDS: [BulkRecordInsert] chunks the read checks and pipelines
     *    one [EventIndex.putAll]; its dedup reads run outside the writer lock
     *    so parallel relays overlap them (a raced duplicate is an idempotent
     *    re-put); guards and supersession stay under it.
     *  - CONTAINS kind 5/62: [BulkMixedInsert] batch-reads the working set and
     *    replays the per-event rules in memory, order preserved, then writes
     *    the diff.
     *
     * Sub-[BULK_MIN] batches aren't worth the setup and just loop [insertLocked].
     *
     * A PURE-RECORD BATCH IS SPLIT BY TRUST. The events that touch reputation
     * (cards, provider lists — see [touchesTrust]) commit under the trust gate;
     * everything else commits under [writes] alone, first. The two halves
     * cannot interact: a card's or a list's supersession address is
     * `(kind, pubkey, d)`, which no other kind in the batch can share; dedup is
     * per id; the guard probes are per owner and read-only. So the split
     * changes no outcome (they are merged back by position) and it takes the
     * mirror's 999 notes out from behind the drain slice their one card has to
     * wait for — and holds the gate for one small commit instead of the whole
     * write stage. A MIXED batch (any kind 5/62) stays whole: [BulkMixedInsert]
     * replays it in order because a deletion may target an event earlier in
     * the same batch, and a kind 5 by id may be pointing at a card.
     */
    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
        if (events.any { it is DeletionEvent || it is RequestToVanishEvent }) {
            return lockedForBatch(events) { if (events.size < BULK_MIN) events.map { tryInsertLocked(it) } else bulkMixed.run(events) }
        }
        // Bulk-or-loop is decided on the batch the CALLER sent, not on a
        // half: a 30-event batch split 15/15 must not fall to the per-event
        // loop on both sides for having been split.
        val bulk = events.size >= BULK_MIN
        val trustAt = BooleanArray(events.size) { touchesTrust(events[it]) }
        val trustCount = trustAt.count { it }
        if (trustCount == 0) return insertRecords(events, trust = false, bulk = bulk)
        if (trustCount == events.size) return insertRecords(events, trust = true, bulk = bulk)
        val plainOut = insertRecords(events.filterIndexed { i, _ -> !trustAt[i] }, trust = false, bulk = bulk)
        val trustOut = insertRecords(events.filterIndexed { i, _ -> trustAt[i] }, trust = true, bulk = bulk)
        var p = 0
        var t = 0
        return events.indices.map { i -> if (trustAt[i]) trustOut[t++] else plainOut[p++] }
    }

    /**
     * One run of plain records (no kind 5/62) — the bulk path when [bulk], a
     * loop otherwise — under [writes], and under the trust gate first when
     * [trust] (every event in the run touches reputation, or none does).
     */
    private suspend fun insertRecords(
        events: List<Event>,
        trust: Boolean,
        bulk: Boolean,
    ): List<IEventStore.InsertOutcome> {
        if (!bulk) return gated(trust, LOCK_INGEST) { events.map { tryInsertLocked(it) } }
        // PLANNED OUTSIDE THE LOCKS, as before: the plan is reads only.
        val plan = bulkRecords.plan(events)
        return gated(trust, LOCK_INGEST) { bulkRecords.commit(plan) }
    }

    private suspend fun tryInsertLocked(event: Event): IEventStore.InsertOutcome =
        try {
            insertLocked(event)
            IEventStore.InsertOutcome.Accepted
        } catch (e: RejectedException) {
            // Only a SEMANTIC rejection becomes a Rejected outcome. A transient
            // engine failure must PROPAGATE — swallowing it would silently DROP
            // a valid event and let the sync cursor advance past it.
            IEventStore.InsertOutcome.Rejected(e.message ?: Rejections.INSERT_FAILED)
        }

    /** No rollback: buffered inserts apply in order; the first rejection propagates and aborts the rest. */
    override suspend fun transaction(body: IEventStore.ITransaction.() -> Unit) {
        val buffered = ArrayList<Event>()
        object : IEventStore.ITransaction {
            override fun insert(event: Event) {
                buffered += event
            }
        }.body()
        lockedForBatch(buffered) { buffered.forEach { insertLocked(it) } }
    }

    /**
     * The per-event rules with NO lock and NO lock accounting: the caller
     * holds whatever it needs. Internal for [BulkMixedInsert]'s replay, which
     * runs these rules against an in-memory snapshot under the real store's
     * locks — going through [insert] there booked a phantom `lock.ingest`
     * sample per replayed event into the process-wide [IngestStats].
     */
    internal suspend fun insertLocked(event: Event) {
        if (event.kind.isEphemeral()) return
        if (event.isExpired()) throw RejectedException(Rejections.EXPIRED)
        // Text the engine refuses is a property of the event, so it is settled
        // here with the other no-I/O checks rather than surfacing as a feed
        // exception three round trips later. See [VespaText].
        if (VespaText.firstIllegalField(event) != null) throw RejectedException(Rejections.UNSTORABLE_TEXT)
        // The admission reads — dedup, NIP-09 tombstone, NIP-62 vanish — are
        // independent, so fire them together and check in the original
        // precedence (duplicate > deleted > vanished). The dup GET deliberately
        // stays a read: folding it into a conditional put was A/B-measured
        // 15-35% slower (see docs/server-side-constraints.md). Guard probes run
        // only when this owner HAS a stored tombstone/vanish (GuardOwners).
        val owner = event.owner()
        val probeDeleted = guards.mightBeDeleted(owner)
        val probeVanished = guards.mightHaveVanished(owner)
        if (!probeDeleted && !probeVanished) {
            // The common case reads just the dup get — skip the fan-out
            // machinery, which allocates per call.
            if (index.get(event.id) != null) throw RejectedException(Rejections.DUPLICATE)
        } else {
            coroutineScope {
                val existing = async { index.get(event.id) }
                val deleted = if (probeDeleted) async { deletions.isDeleted(event) } else null
                val vanished = if (probeVanished) async { deletions.isVanished(event) } else null
                if (existing.await() != null) throw RejectedException(Rejections.DUPLICATE)
                if (deleted?.await() == true) throw RejectedException(Rejections.DELETED)
                if (vanished?.await() == true) throw RejectedException(Rejections.VANISHED)
            }
        }
        when {
            event is DeletionEvent -> {
                deletions.applyDeletion(event)
                index.put(event.toDoc())
                guards.noteDeletionStored(event.pubKey)
            }

            event is RequestToVanishEvent -> {
                deletions.applyVanish(event)
                index.put(event.toDoc())
                guards.noteVanishStored(event.pubKey)
            }

            // Replaceable/addressable newest-wins in ONE call: false == a
            // same-or-newer version holds the address, so this insert is
            // REPLACED (see EventIndex.putIfNewer).
            event.kind.isReplaceable() || event.kind.isAddressable() -> {
                if (!index.putIfNewer(event.toDoc())) throw RejectedException(Rejections.REPLACED)
            }

            else -> {
                index.put(event.toDoc())
            }
        }
    }

    // ---- queries ------------------------------------------------------------

    /**
     * Map a filter to an [EventQuery] stamped with the request's clock (one
     * [cutoffSecs] per request, so sibling filters can't disagree about an
     * event expiring on the boundary — nor, since it is also the RECENCY
     * ranking instant, about how old the same document is) and the ranking
     * observer. An explicit
     * `observer:` search token wins over the connection [observer] — scores
     * are public, so a client may rank through any lens.
     *
     * THE OBSERVER GATE: supplying an observer opts the whole request into
     * that lens — including plain recall. A non-search query with a resolved
     * observer keeps its NIP-01 recency order but drops authors below the
     * trust floor ([EventQuery.minRank] if the query set one via
     * `filter:rank:…`, else [DEFAULT_MIN_RANK]). `include:spam` opts out of
     * the DEFAULT floor only — an explicit floor survives it. Queries that
     * chose a profile (`sort:`) or carry terms already gate through their own
     * profile; reads that never resolve an observer are untouched — recall
     * without a lens is never gated.
     */
    private fun Filter.toExpiryQuery(
        cutoffSecs: Long,
        observer: String? = null,
    ): EventQuery? =
        toEventQuery()?.let {
            // nowSecs: the same instant the expiry cutoff uses. recallOrdered
            // MERGES sibling filters' hits by engine score, and with recency in
            // the profile a score is a function of the instant it was asked at
            // — so letting each filter stamp its own clock would put one merge
            // on two scales, which is the exact thing cutoffSecs exists to stop.
            // It also puts the store's injectable clock in charge of ranking.
            val q = it.copy(notExpiredAt = cutoffSecs, nowSecs = it.nowSecs ?: cutoffSecs, observer = it.observer ?: observer)
            val floor = q.minRank ?: DEFAULT_MIN_RANK.takeUnless { q.includeSpam }
            // Phrases count as search text (they gate through the search
            // profiles); an exclusion-only (notSearch) query is plain recall.
            val termless = q.search == null && q.phrases.isEmpty()
            when {
                q.observer != null && termless && q.ranking == null && floor != null -> {
                    q.copy(ranking = EventYql.RANK_RECENCY_GATED, minRank = floor)
                }

                // `sort:recent` with nothing to gate through AND nothing to
                // search IS a plain NIP-01 filter — the gated profile would
                // order it identically while forgoing the recency profile and
                // the count-probe planner (both key on a ranking-free query),
                // and would rank every match to say so. Handed back to the
                // plain path. A `sort:recent` carrying TERMS keeps the profile:
                // there, ranking-free means the RELEVANCE profiles, which is
                // the order the caller just asked us not to use.
                q.observer == null && termless && q.ranking == EventYql.RANK_RECENCY_GATED -> {
                    q.copy(ranking = null, minRank = null)
                }

                else -> {
                    q
                }
            }
        }

    /** Whether this query recalls through a rank profile (its trust gates apply engine-side). */
    private fun EventQuery.isRanked(): Boolean = search != null || phrases.isNotEmpty() || ranking != null

    /**
     * Whether the ENGINE's hit order is the serving order. Ranked queries keep
     * relevance order (NIP-50) — except the observer gate's profile, whose
     * order is defined as NIP-01 recency: the engine's created_at score order
     * is re-sorted client-side (engine score ties are arbitrary). That covers
     * `sort:recent` SEARCHES too — the point of the token is that its hits
     * come back in the same `created_at desc, id asc` order a plain filter's do.
     */
    private fun EventQuery.keepsEngineOrder(): Boolean = isRanked() && ranking != EventYql.RANK_RECENCY_GATED && ranking != EventYql.RANK_RECENCY_GATED_EXACT

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    /**
     * NOTE ON [T]: the reference expansion adds rows the caller's own recall
     * never returned — the subjects a matched label, assertion or Trusted List
     * nominates — so a SEARCHING read can serve more events than it matched.
     * What it cannot do is serve a KIND the filters did not name: [servedKinds]
     * holds the page to the caller's own kinds, so `query<MetadataEvent>(
     * kinds=[0], search=…)` is still all kind 0 and the unchecked cast below —
     * the interface's own idiom — stays honest. A read that named NO kinds
     * admits everything by definition, and there a mixed page is what was
     * asked for.
     */
    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = lensed(filters.mapNotNull { it.toExpiryQuery(cutoff, observer) })
        val expansion = expansionOf(queries)
        val recalled =
            recallOrdered(
                queries + companionsOf(expansion, queries),
                EventDoc.NEWEST_FIRST,
                EventDoc::id,
                { index.search(it) },
                { index.searchRanked(it) },
                expansion != null,
            )
        val page = spliced(expansion, recalled, DOC_KEYS, { if (it.kind in SearchReferences.KINDS) it.toEvent() else null }, { index.searchRanked(it) })
        // Reconstruct via Quartz's by-kind factory straight from the stored
        // fields, skipping the serialize+parse round trip; see [toEvent].
        // Narrowed to what the caller's own filters admit BEFORE the diversity
        // cap, so a row about to be dropped cannot spend an author's slots.
        return page.asked(expansion, filters, EventDoc::kind, EventDoc::id, EventDoc::pubkey).diverse(queries.all { it.keepsEngineOrder() }, EventDoc::pubkey).map { it.toEvent() } as List<T>
    }

    /**
     * The page narrowed to what the caller's filters ADMIT — the same list,
     * not a copy, whenever nothing expanded or every row passes, which is
     * every plain recall and every search whose expansion added only rows the
     * REQ's filters match.
     *
     * A kind-restricted search recalls MORE than its own kinds on purpose: the
     * pointer families that convert into them are fetched as companion queries
     * ([SearchReferenceExpansion.companions]), because a label, assertion or
     * Trusted List is the only route to the subjects it names. That is a recall
     * device, and it stops at recall. A REQ that asked for `kinds:[0]` asked a
     * NIP-01 question with a NIP-01 answer, and a 30382 on that page is a kind
     * the client said it did not want — it has no parser for it, it did not
     * budget a slot for it, and on a relay it is a protocol violation rather
     * than a bonus. So the pointer does its job (it names subjects, and those
     * subjects ARE of an asked-for kind) and is then dropped from the answer.
     *
     * JUDGED ON KINDS, IDS AND AUTHORS — the three exact keys — each row
     * against ANY filter, since a REQ ORs its filters and answers with one
     * page. This used to be a kinds-only check that treated a filter with no
     * `kinds` as admitting every kind, which served a companion pointer to a
     * REQ like `[{kinds:[0], search:…}, {ids:[e1]}]` — the second filter has
     * no kinds but admits exactly one event, and the 30392 matched neither.
     * Not tags or the time window, deliberately: the engine matches tag values
     * uncased and a client-side matcher would not, so re-judging those here
     * could drop a hit the engine rightly served; the exact keys have one
     * answer on both sides. Everything else the expansion nominates was looked
     * up under the finding query with its terms stripped, so it passed the
     * same keys the hits did and passes here too; the pointer kinds are the
     * one constraint the companions deliberately step outside of.
     */
    private fun <R> List<R>.asked(
        expansion: SearchReferenceExpansion?,
        filters: List<Filter>,
        kindOf: (R) -> Int,
        idOf: (R) -> String,
        authorOf: (R) -> String,
    ): List<R> {
        if (expansion == null) return this
        val keys = filters.map { Triple(it.kinds?.toSet(), it.ids?.mapTo(HashSet()) { id -> id.lowercase() }, it.authors?.mapTo(HashSet()) { a -> a.lowercase() }) }

        fun admitted(row: R): Boolean =
            keys.any { (kinds, ids, authors) ->
                (kinds == null || kindOf(row) in kinds) && (ids == null || idOf(row) in ids) && (authors == null || authorOf(row) in authors)
            }
        if (all(::admitted)) return this
        return filter(::admitted)
    }

    /**
     * The page with no author holding more than [maxHitsPerAuthor] rows —
     * the same list, not a copy, whenever the cap is off or nothing exceeds it,
     * which is every read on a default store.
     *
     * STABLE and FIRST-WINS: the rows an author keeps are the ones the ranking
     * put highest, and everything else stays exactly where it was. A spliced
     * member cannot be dropped by this in practice — a person has one profile,
     * so one row — which is the right asymmetry: the cap exists to stop one
     * author's BULK from taking a page, not to ration the people a list names.
     */
    private fun <R> List<R>.diverse(
        /**
         * Read off the QUERIES, not off whether the page came back with scores.
         * The two differ on an engine that does not rank — the in-memory
         * reference reports a null score per hit — and which pages an operator
         * capped must not depend on which engine answered them.
         */
        ranked: Boolean,
        authorOf: (R) -> String,
    ): List<R> {
        val cap = maxHitsPerAuthor ?: return this
        if (!ranked || size <= cap) return this
        val seen = HashMap<String, Int>()
        var dropped = false
        val kept = ArrayList<R>(size)
        for (hit in this) {
            val n = seen.merge(authorOf(hit), 1, Int::plus)!!
            if (n <= cap) kept.add(hit) else dropped = true
        }
        return if (dropped) kept else this
    }

    /**
     * The page narrowed to [kinds] — the same list, not a copy, whenever there
     * is nothing to narrow (see [servedKinds]) or nothing fell outside, which
     * is every plain recall and every search whose expansion added only
     * asked-for kinds.
     */
    private fun <R> List<R>.asked(
        kinds: Set<Int>?,
        kindOf: (R) -> Int,
    ): List<R> {
        if (kinds == null || all { kindOf(it) in kinds }) return this
        return filter { kindOf(it) in kinds }
    }

    /**
     * The reference expansion for this read, or null where nothing can ever
     * expand: the feature switched off, or no query carrying TERMS. Terms, not
     * "carries a search field" — every anonymous read on a lens-requiring
     * relay stamps `include:spam`, and a mirror's paging carries it too, so
     * gating on the field would put all of that traffic behind an expansion
     * none of it asked for.
     *
     * Created BEFORE the recall, because the expansion now shapes it: a
     * searching read also recalls the pointer kinds it would otherwise miss
     * ([SearchReferenceExpansion.companions]) — the ones a kind-restricted
     * query cannot return at all, and the enrolled signers' declarations an
     * unrestricted one ranks below its own page. Without that, the lists,
     * assertions and labels whose text matched are never returned, and neither
     * this store nor the client ever learns there was anything to unpack.
     *
     * The GATE READ STAYS LAZY. [ProviderMap] never caches an empty pass, so
     * on a relay holding no 10040s the delegations read is one small engine
     * query every time it is asked — which is why the enrolment goes in as a
     * supplier the expansion resolves at most once, and only on the paths
     * that consult the gate: building a declaration companion, or meeting a
     * declaration pointer in the page. An anonymous read resolves to
     * [Enrolment.NONE] without ever querying, as before, and a termless one
     * never reaches the gate at all. An UNRESTRICTED-kind read with an
     * observer now does resolve it — it builds a declaration companion like
     * any other searching read — which is one small query per such read on a
     * relay with no 10040s to cache, the price of the reader's own lists
     * reaching a page they were ranked off.
     */
    private fun expansionOf(queries: List<EventQuery>): SearchReferenceExpansion? {
        if (!searchExpansion.enabled) return null
        val searching = queries.filter { it.search != null || it.phrases.isNotEmpty() }
        if (searching.isEmpty()) return null
        // An ANONYMOUS read can unpack no declaration at all, and asking the
        // gate to say so would cost a provider-list query on a relay that holds
        // no Treasure Maps — where the answer is never cached, by design. Labels
        // are ungated, so the expansion still runs; it just runs with nothing
        // enrolled.
        //
        // PER OBSERVER, never pooled: the supplier takes the lens's own observer
        // and the expansion memoizes per key, so one filter's `observer:` can
        // never unpack a declaration only the filter beside it enrolled. The
        // resolution is the same one either way — `delegations()` caches the
        // parsed Maps, and `of()` is a pure fold over that cache — so a
        // two-lens read costs a second fold, not a second query.
        return SearchReferenceExpansion(
            searching,
            { observer -> if (observer == null) Enrolment.NONE else delegations().of(observer) },
            searchExpansion,
        )
    }

    /**
     * [SearchReferenceExpansion.companions] minus any query the caller already
     * sent: a REQ can legitimately carry the very filter a companion would
     * duplicate (`kinds:[1]` beside `kinds:[1985]`, same terms), and running
     * the identical query twice buys nothing the id-dedup doesn't already
     * guarantee.
     */
    private suspend fun companionsOf(
        expansion: SearchReferenceExpansion?,
        queries: List<EventQuery>,
    ): List<EventQuery> = expansion?.companions()?.filterNot { it in queries } ?: emptyList()

    /**
     * Recall every query concurrently (bounded), dedup across queries, and
     * order the result — ONE order over the union, not one order per filter.
     *
     * NIP-50 asks for relevance order and NIP-01 for recency, and a REQ can
     * carry both kinds of filter at once, so there are three cases:
     *
     *  - **No query ranked.** The union is sorted `created_at desc, id asc` —
     *    the NIP-01 order, applied across filters.
     *  - **Every query ranked, by the SAME profile.** The union is merged on the
     *    engine's relevance, ties broken by recency: the filters are several
     *    ways of asking one question, and their answers belong in one order.
     *    It costs the scores, which is why [EventIndex.searchRanked] exists.
     *  - **Mixed, or ranked by different profiles.** Each query's hits keep
     *    their own order and the runs are concatenated. A relevance score and a
     *    timestamp share no scale, and neither do two profiles' scores, so
     *    interleaving them would be inventing a comparison. The honest floor,
     *    not a good answer — a client that wants one order should ask one
     *    question.
     *
     * Dedup is by id and keeps the BEST copy: sorting before [distinctBy] means
     * an event that answered two filters survives at its higher score.
     */
    private suspend fun <R> recallOrdered(
        queries: List<EventQuery>,
        newestFirst: Comparator<R>,
        idOf: (R) -> String,
        searchOne: suspend (EventQuery) -> List<R>,
        searchRankedOne: suspend (EventQuery) -> List<Ranked<R>>,
        /**
         * Whether the CALLER needs the per-hit relevance kept — the splice does,
         * to place a subject by the confidence its pointer expressed.
         *
         * IT COSTS NOTHING ON A RANKED QUERY, which is the only kind it applies
         * to: `recallSummaries` already goes through `rankedHits` there, one
         * `recallRoot` call, and the ranked path differs only in keeping the
         * `relevance` Vespa already returned. What the single-query fast path
         * avoids is wrapping every hit of an ORDINARY REQ — and an ordinary REQ
         * is recency-ordered, so `keepsEngineOrder()` sends it down the
         * score-free branch regardless of this flag.
         */
        wantScores: Boolean = false,
    ): Page<R> {
        if (queries.isEmpty()) return Page(emptyList(), null)
        // One filter is the ordinary REQ and never needs a score: its engine
        // order IS the answer, and asking for scores would wrap every hit on
        // the hottest read a relay serves.
        if (queries.size == 1) {
            if (wantScores && queries[0].keepsEngineOrder()) return Page.of(searchRankedOne(queries[0]))
            val hits = searchOne(queries[0])
            return Page(if (queries[0].keepsEngineOrder()) hits else hits.sortedWith(newestFirst), null)
        }
        if (queries.none { it.keepsEngineOrder() }) {
            return Page(
                queries
                    .mapBounded(QUERY_FANOUT) { searchOne(it) }
                    .flatten()
                    .distinctBy(idOf)
                    .sortedWith(newestFirst),
                null,
            )
        }
        // [EventYql.profileOf], not `ranking`: the field is null for every
        // ordinary search and the profile is picked from the query's shape, so
        // two filters where only one carries `observer:` read as "same profile"
        // by the field while running on `search` and `text` — two scales,
        // interleaved.
        if (queries.all { it.keepsEngineOrder() } && queries.mapTo(HashSet()) { EventYql.profileOf(it) }.size == 1) {
            val scored = queries.mapBounded(QUERY_FANOUT) { searchRankedOne(it) }.flatten()
            // An engine that does not rank (the in-memory reference) reports a
            // null rather than a fabricated constant; its hits are already
            // newest-first, so recency is the merge that keeps them coherent.
            if (scored.any { it.score == null }) {
                return Page(
                    scored
                        .map { it.hit }
                        .distinctBy(idOf)
                        .sortedWith(newestFirst),
                    null,
                )
            }
            return Page.of(
                scored
                    .sortedWith(compareByDescending<Ranked<R>> { it.score }.thenBy(newestFirst) { it.hit })
                    .distinctBy { idOf(it.hit) },
            )
        }
        val results = queries.mapBounded(QUERY_FANOUT) { searchOne(it) }
        val ordered = queries.zip(results).flatMap { (q, hits) -> if (q.keepsEngineOrder()) hits else hits.sortedWith(newestFirst) }
        // Two scales already, which is why these runs are concatenated rather
        // than merged — so there is no coherent score to carry out of here.
        return Page(ordered.distinctBy(idOf), null)
    }

    /**
     * ONE ORDERED PAGE, AND THE RELEVANCE BEHIND IT — [scores] index-aligned
     * with [hits], or NULL for the pages that have none: a recency-ordered
     * recall, two ranking profiles concatenated, an engine that does not rank.
     *
     * A nullable parallel list rather than a `List<Ranked<R>>` because the
     * unscored page is the hot one. A plain NIP-01 recall, a mirror's paging and
     * a NIP-77 catch-up all land here, and wrapping every hit of those in a
     * score-carrying object — then unwrapping it again in [spliced] — would be
     * two copies of the page and an allocation per event to carry a null.
     */
    private class Page<R>(
        val hits: List<R>,
        val scores: List<Double?>?,
        /**
         * The TEXT band behind each score — `Ranked.textScore`, index-aligned
         * with [hits] and null wherever [scores] is. The splice places a
         * Trusted List's member by the band the LIST earned times what the list
         * says about that MEMBER, so it needs the pointer's text apart from the
         * signer's trust that [scores] multiplies in.
         */
        val texts: List<Double?>? = null,
    ) {
        companion object {
            fun <R> of(ranked: List<Ranked<R>>) = Page(ranked.map { it.hit }, ranked.map { it.score }, ranked.map { it.textScore })
        }
    }

    /**
     * THE ORDERED PAGE, PLUS WHAT IT POINTS AT — a label's subject behind the
     * label, a Trusted List's members behind the list.
     *
     * Runs after [recallOrdered] rather than inside it, but over the [Page] it
     * produced rather than over a bare list: a scored member is placed by the
     * relevance the ENGINE gave it on the member rung, and its pointer rises to
     * sit just above the best of them, so the placement needs both the finished
     * order AND the scores that produced it. Splicing inside the recall would
     * have to answer that question once per ordering case; here it is answered
     * once.
     *
     * Returns the page's hits UNTOUCHED — the same list, not a copy — whenever
     * [expansion] is null, which is every plain recall this store serves
     * (see [expansionOf] for what qualifies). There is no cheaper early-out
     * left to take here: since the conversion recall, EVERY searching read can
     * splice — a kind-restricted one reaches its pointers through the
     * companion queries — so "can serve no pointer kind" no longer exists as
     * a shape.
     */
    private suspend fun <R> spliced(
        expansion: SearchReferenceExpansion?,
        page: Page<R>,
        keys: SubjectKeys<R>,
        pointerOf: (R) -> Event?,
        recall: suspend (EventQuery) -> List<Ranked<R>>,
    ): List<R> {
        val hits = page.hits
        if (expansion == null || hits.isEmpty()) return hits
        val expanded = expansion.expand(hits, page.scores, page.texts, keys, pointerOf, recall)

        // THE POINTER'S OWN ORDER FIRST, always — the sort below is a stable
        // re-sort of it, so a tie between a subject and its own pointer resolves
        // the only way it can read: the reason above the result. It is also the
        // answer whenever there are no scores to sort by.
        val placed = ArrayList<Placed<R>>(hits.size)
        hits.forEachIndexed { i, hit ->
            val pointer = page.scores?.get(i)
            // THE POINTER RISES TO ITS BEST MEMBER — it does not hold them down.
            //
            // "A reason cannot rank below the thing it explains" is still the
            // invariant, and this is the direction that satisfies it without
            // deciding the page. The other direction — clamping each member to
            // its pointer's score — made the SIGNER's trust the ceiling for
            // everyone the list names, and a trust service is a key nobody
            // follows: on the staging relay a `Verified Human` list signed by a
            // service scored 26 pinned sixteen members scored 65..100 to one
            // number (550 x wot(26)), so they came back in the publisher's tag
            // order, three orders of magnitude below their own relevance,
            // beneath organic hits from authors trusted 7. Member trust and
            // publisher confidence — the two things `event.sd` §13 computes —
            // could not move a member at all.
            //
            // Lifting instead keeps the pill row's reading exactly: the raised
            // score is a MAX over the pointer's own members, so no subject can
            // pass it, and a tie between the pointer and its best member
            // resolves to the pointer because the stable sort sees it first.
            //
            // Only SCORED members lift, which is why a label is untouched by
            // this: it expresses no confidence, none of its subjects is fetched
            // under a member profile, `lifted` collapses to `pointer`, and the
            // placement is bit-identical to before.
            //
            // A LOOP, not filterNotNull().maxOrNull(): this runs for every hit
            // of every scored page, and the overwhelming majority of them carry
            // no subjects at all — a throwaway ArrayList per row to reduce an
            // empty list is the wrong price for a page of 500.
            val lifted =
                if (pointer == null) {
                    null
                } else {
                    var best: Double = pointer
                    for (score in expanded.scores[i]) {
                        if (score != null && score > best) best = score
                    }
                    best
                }
            if (expanded.fresh[i]) placed.add(Placed(hit, lifted))
            expanded.subjects[i].forEachIndexed { j, subject ->
                val own = expanded.scores[i][j]
                placed.add(
                    Placed(
                        subject,
                        when {
                            // NO RUNG, SO NO MOVE — it sits WITH its pointer.
                            // A reference that expressed no confidence (a
                            // NIP-32 label, a NIP-85 assertion) was never
                            // fetched under the member profile, so it takes the
                            // pointer's score and a stable sort puts it right
                            // behind it. That is the placement those two
                            // families have always had, and it is right:
                            // neither claim is probabilistic, so there is no
                            // doubt for a rung to express.
                            //
                            // The LIFTED score, not the raw one, because
                            // adjacency is the whole point of this branch. On a
                            // list mixing scored and unscored members the raw
                            // pointer score would strand the unscored ones
                            // where the block used to be — on the staging
                            // numbers, ~340x below the siblings they were named
                            // beside. For a label, which has no scored member
                            // to lift anything, `lifted` IS `pointer` and this
                            // is bit-identical to before.
                            own == null -> lifted

                            // An UNSCORED pointer on a scored member is still a
                            // null, exactly as it was under the ceiling: it is
                            // the signal that this page cannot be sorted at all
                            // and must keep the pointer's own order. Answering
                            // `own` here would let one row's missing score turn
                            // a fallback page into a sorted one.
                            pointer == null -> null

                            // THE ENGINE'S OWN NUMBER, unclamped — see the
                            // lift above for what used to happen here.
                            else -> own
                        },
                    ),
                )
            }
        }
        // ONE SCALE, THE ENGINE'S. Hits carry the relevance their rank profile
        // gave them; members carry the relevance a MEMBER profile gave them on
        // the same ladder (event.sd §13), so the two sort together without this
        // class doing arithmetic on either. Nothing to normalize, nothing to
        // shift: a number computed here could only ever be a guess about a
        // scale the engine owns, and the guess is what broke.
        //
        // A page missing any score cannot be sorted at all — the in-memory
        // reference reports null rather than fabricating a constant, and a
        // recency-ordered read has no relevance to give — so it keeps the
        // pointer's own order.
        if (placed.any { it.score == null }) return placed.map { it.row }
        return placed.sortedByDescending { it.score }.map { it.row }
    }

    /** A row and the relevance the ENGINE placed it by — its rank profile's for a hit, the member profile's for a subject. */
    private class Placed<R>(
        val row: R,
        val score: Double?,
    )

    /**
     * Raw read path: recall matches as Quartz [RawEvent]s, skipping the
     * per-hit tag parse and the Event object model — a relay serving REQs
     * straight to the wire re-serializes each event anyway (see
     * [EventIndex.rawSearch]). Same recall, expiry, dedup, and ordering as
     * [query]; only the projection differs.
     */
    override suspend fun rawQuery(
        filters: List<Filter>,
        onEach: (RawEvent) -> Unit,
    ) {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries = lensed(filters.mapNotNull { it.toExpiryQuery(cutoff, observer) })
        val expansion = expansionOf(queries)
        val ordered =
            recallOrdered(
                queries + companionsOf(expansion, queries),
                RAW_NEWEST_FIRST,
                RawEvent::id,
                { index.rawSearch(it) },
                { index.rawSearchRanked(it) },
                expansion != null,
            )
        spliced(expansion, ordered, RAW_KEYS, { if (it.kind in SearchReferences.KINDS) it.toEvent() else null }, { index.rawSearchRanked(it) })
            .asked(expansion, filters, RawEvent::kind, RawEvent::id, RawEvent::pubKey)
            .diverse(queries.all { it.keepsEngineOrder() }, RawEvent::pubKey)
            .forEach(onEach)
    }

    override suspend fun <T : Event> query(
        filter: Filter,
        onEach: (T) -> Unit,
    ) = query<T>(filter).forEach(onEach)

    override suspend fun <T : Event> query(
        filters: List<Filter>,
        onEach: (T) -> Unit,
    ) = query<T>(filters).forEach(onEach)

    /**
     * NIP-45 COUNT == the REQ's MATCHED feed, exactly: the same observer
     * (ranking gates included), the same per-filter limits, the same
     * cross-filter id dedup — the number a client could verify by running the
     * REQ and counting what it MATCHED. One deliberate exception: the search
     * expansion's ADDED rows — converted pointers and their spliced subjects —
     * are not counted, so a searching COUNT can undercount the frames its REQ
     * serves. Counting them would cost the full ranked recall plus the
     * expansion's subject lookups on a path whose whole point is to be cheaper
     * than the REQ, to count rows that exist to be *extra*; a client sizing a
     * slice wants the match set, and that is what it gets.
     *
     * A GATED count is therefore a recall, not a grouping: the trust floor
     * reads the observer's cell in the author's reputation tensor, which no YQL
     * `count()` can express, so an observer's count costs a full ranked pass
     * over the match set. Only the ungated shapes reach the cheap engine count
     * below. Bounding that cost is the FILTER's job, as with every other query
     * here — the store adds no cap of its own (see [VespaEventStore.open]).
     */
    override suspend fun count(filter: Filter): Int = count(listOf(filter))

    override suspend fun count(filters: List<Filter>): Int {
        val observer = coroutineContext[StoreQueryContext]?.observer
        val cutoff = nowSecs()
        val queries =
            lensed(filters.mapNotNull { it.toExpiryQuery(cutoff, observer) })
                // A present limit <= 0 is the "matches nothing" sentinel on the
                // feed, so it contributes nothing to the count either.
                .filterNot { (it.limit ?: 1) <= 0 }
        if (queries.isEmpty()) return 0
        if (queries.size == 1) {
            val q = queries[0]
            return when {
                // Ranked/searching: only the search path applies the observer's
                // trust floor and spam gate, so the count must be of what the
                // feed would SERVE, not of what the filter matches. That used to
                // mean recalling the page and taking its size, which fetched a
                // full document summary per counted event to produce an integer
                // — 76.0s for a "bitcoin" COUNT on the production relay against
                // 4.9s for the same search (2026-09-01). [EventIndex.count]
                // answers the same gated number from the engine's own
                // `totalCount` with zero hits; the clamp below is unchanged, so
                // the ANSWER is identical (STORE-C01: a count honours its
                // filter's limit).
                q.isRanked() || q.limit != null -> {
                    val served = index.count(q.copy(limit = null))
                    q.limit?.let { minOf(served, it) } ?: served
                }

                // Plain, unbounded: the engine's exact grouping count.
                else -> {
                    index.count(q)
                }
            }
        }
        // Multi-filter: the feed dedups across filters, so collect each
        // filter's SERVED ids and count the union. Plain unbounded filters
        // stream ids through the visit (no doc materialization).
        //
        // Fanned out at [QUERY_FANOUT], like the recall path: a COUNT summarizes
        // the same feed a REQ serves and a relay allows as many filters here as
        // there (20), so running them one after another made COUNT several times
        // slower than the REQ it describes, for identical work. [forEachBounded]
        // serializes the fold, which is what lets the id set stay a plain
        // HashSet while the queries overlap.
        val ids = HashSet<String>()
        queries.forEachBounded(
            QUERY_FANOUT,
            produce = { q ->
                // Every PLAIN filter streams ids, limited or not — the engine
                // honours a limit on the id walk itself, so the relay's
                // `limit: 100000` no longer turns a count into a full-summary
                // recall (measured: 40 s for two window filters over 51k
                // events, against 0.2 s counted singly). Only a ranked filter
                // has to recall through the search path, where its ids ARE
                // the ranking.
                if (!q.isRanked()) {
                    val seen = ArrayList<String>()
                    index.visitIds(q) { page ->
                        page.forEach { seen += it.id }
                        true
                    }
                    seen
                } else {
                    index.rawSearch(q).map { it.id }
                }
            },
        ) { found -> ids += found }
        return ids.size
    }

    /**
     * Every distinct value of [tagName] at position [valueIndex] across
     * [filter]'s matches, optionally narrowed by [where] — which sees the
     * WHOLE tag, so a positional condition on another element is expressible
     * (NIP-65's write marker, NIP-85's relay position).
     *
     * It rides the tags-only visit projection ([EventIndex.visitTags]) BY
     * DEFAULT, not a grouping over `tag_index`: that field is a derived, lossy
     * view (single-letter names, first values only), so a grouping never sees a
     * multi-character name and cannot apply a positional condition — it would
     * return a SUPERSET.
     *
     * WHERE THE SUPERSET IS THE ANSWER, though, the grouping is exactly right,
     * and [unconditional] is how a caller says so: a one-letter tag read at
     * position 1 with no condition on the rest of it. Then the engine
     * aggregates and the corpus is never walked — measured at ~1s against
     * ~157s for every relay url in 3.27M NIP-65 lists. Any other shape takes
     * the walk and gets the exact set.
     *
     * Empty values are skipped, and expiry is honored like [count].
     */
    suspend fun distinctTagValues(
        filter: Filter,
        tagName: String,
        valueIndex: Int = 1,
        /**
         * Whether the caller's [where] actually looks at the tag. It cannot be
         * inferred — a lambda is opaque — and guessing wrong would answer a
         * SUPERSET silently, so the default is the safe one and only a caller
         * that knows it has no positional condition opts in.
         *
         * BEFORE [where], not after: `where` is function-typed, so a parameter
         * added past it captures every trailing-lambda call site as this
         * Boolean instead. The compiler caught it; the ordering keeps it caught
         * for good.
         */
        unconditional: Boolean = false,
        where: (List<String>) -> Boolean = { true },
    ): Set<String> {
        val q = filter.toExpiryQuery(nowSecs()) ?: return emptySet()
        // THE FAST PATH, and every one of these conditions is load-bearing.
        // `tag_index` is lossy in three ways at once — single-letter names,
        // first values only, nothing of the rest of the tag — so a grouping
        // over it answers this question and no other. Drop any condition and
        // the answer silently widens.
        if (unconditional && valueIndex == 1 && tagName.length == 1) {
            index.distinctTagIndexValues(q, tagName)?.let { return it }
        }
        val out = HashSet<String>()
        index.visitTags(q) { page ->
            for (tags in page) {
                for (tag in tags) {
                    if (tag.size > valueIndex && tag[0] == tagName && where(tag)) {
                        tag[valueIndex].takeIf(String::isNotEmpty)?.let(out::add)
                    }
                }
            }
            true
        }
        return out
    }

    /**
     * (created_at, id) pairs straight off the docs — no Event materialization.
     * Plain filters walk the corpus through the engine's visit, so a
     * negentropy session sees the COMPLETE match set even when it dwarfs the
     * search page limit. Searching or limit'd filters keep the search path,
     * since their semantics live there.
     *
     * [maxEntries] returns at most `maxEntries + 1` — one over, so the caller
     * can tell "at budget" from "over budget" — and STOPS the walk there: a
     * caller sizing a sync window only needs to learn the set exceeds its
     * budget, not scan a 10M corpus to prove it.
     *
     * That cap counts UNIQUE ids, which is why cross-filter dedup runs INLINE
     * rather than over the collected list at the end. Capping RAW hits would
     * stop the walk while the deduped union was still under budget, handing back
     * a partial set indistinguishable from a complete one.
     *
     * [onProgress] fires after every page — the walk is the longest silent phase
     * a mirror has.
     */
    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
        onProgress: ((collected: Int) -> Unit)?,
    ): List<IdAndTime> {
        val all = ArrayList<IdAndTime>()
        // Only a multi-filter snapshot can repeat an id — one query never
        // returns a doc twice — so the single-filter walk pays nothing here.
        val seen = if (filters.size > 1) HashSet<String>() else null
        val cap = maxEntries?.plus(1)

        fun collect(
            id: String,
            createdAt: Long,
        ) {
            if (seen == null || seen.add(id)) all += IdAndTime(createdAt, id)
        }

        // Exclude already-expired events (NIP-40), exactly as query/count do —
        // otherwise a peer keeps trying to reconcile events we refuse to serve.
        val cutoff = nowSecs()
        for (q in filters.mapNotNull { it.toExpiryQuery(cutoff) }) {
            // Already over budget: the filters left can only add to the union.
            if (cap != null && all.size >= cap) break
            if (q.search == null && q.limit == null) {
                index.visitIds(q) { page ->
                    page.forEach { collect(it.id, it.createdAt) }
                    onProgress?.invoke(all.size)
                    cap == null || all.size < cap
                }
            } else {
                index.search(q).forEach { collect(it.id, it.createdAt) }
                onProgress?.invoke(all.size)
            }
        }
        // A page (or a search, which has no page hook to stop on) can carry
        // the count past the cap — trim to the sentinel the contract promises.
        return if (cap != null && all.size > cap) all.subList(0, cap) else all
    }

    // ---- deletes ------------------------------------------------------------

    override suspend fun delete(filter: Filter) = delete(listOf(filter))

    /**
     * BOTH LOCKS, unconditionally — a sweep is defined by a filter, so what it
     * will delete is not known until it runs, and it may well be cards. A
     * removal does not write reputation inline (it marks dirt and the drain
     * re-derives), but `DirtLedger.guarded` PERSISTS that mark as a reputation
     * document, and the drain loads and rewrites the same marker under the
     * trust gate. Racing it would drop work the drain had already snapshotted.
     *
     * Cheap where it matters: a sweep is periodic, not the client write path
     * the gate split exists to keep clear.
     */
    override suspend fun delete(filters: List<Filter>) {
        // An EMPTY filter deletes NOTHING (STORE-F10, the reference's deliberate
        // asymmetry with query): as a query it means "everything", and a stray
        // one here would sweep the corpus 10k at a time until the round cap
        // threw, half-wiped.
        val queries = filters.filterNot { it.isEmpty() }.mapNotNull { it.toEventQuery() }
        if (queries.isEmpty()) return
        gated(trust = true, LOCK_SWEEP, LOCK_SWEEP_TRUST) {
            queries.forEach { deletions.sweep(it) }
        }
    }

    override suspend fun deleteExpiredEvents() {
        // expiresBefore is strict (<): +1 makes "expires exactly now" due, per NIP-40.
        // Both locks, for the reason on [delete]: NIP-40 expiry does not ask
        // what kind it is reaping, so it can reap cards.
        gated(trust = true, LOCK_SWEEP, LOCK_SWEEP_TRUST) { deletions.sweep(EventQuery(expiresBefore = nowSecs() + 1)) }
    }

    // ---- full-text reindex --------------------------------------------------

    /**
     * Re-derive the search fields for every stored event. Which kinds are
     * searchable — and how [SearchExtractors] decomposes them — is baked into
     * this build, so docs indexed under old code can be stale until this runs.
     * It also clears fields for kinds that LOST searchability, and it is the
     * RE-FEED that backfills the near-tier prefix/fuzzy attributes on a corpus
     * fed before they existed — a Vespa reindex cannot, because fed fields
     * only change on a put.
     *
     * ORDER MATTERS on an upgraded deployment: deploy the bundled schema
     * BEFORE running this — a serving schema that predates the near fields
     * rejects the backfill puts outright, failing loudly instead of
     * backfilling nothing.
     *
     * THIS DOES NOT BACKFILL `search_text_gram`, and the symmetry with the near
     * tier is exactly inverted. That column is DERIVED BY VESPA
     * (`indexing: input search_text | index`), not fed, so nothing this method
     * compares can see it missing: `columnsChanged` reads the search columns,
     * which are identical, and `nearStale` reads the near arrays, which are
     * identical too — so no document is re-put and the walk reports success
     * having repaired nothing. The corpus keeps working, minus body
     * partial-word reach, with no error anywhere. VERIFIED on Vespa 8
     * (2026-08-15): deploying the column onto a populated cluster leaves it
     * empty for every existing document while `search_text` keeps serving.
     *
     * Two things do repair it, and an operator must pick one:
     *   - Vespa REINDEXING, which the deploy response asks for by name
     *     ("Non-document field 'search_text_gram' added; this may be populated
     *     by reindexing"). Triggered on the config server at
     *     `POST …/reindex?clusterId=content&documentType=event`. It is
     *     asynchronous — it goes `pending` and is dispatched by a maintenance
     *     job, not on the call — so do not treat the 200 as completion.
     *   - A full RE-FEED. A plain put re-derives the column at index time, even
     *     with byte-identical content (verified). This method cannot be that
     *     re-feed, by the drift check above.
     */
    override suspend fun reindexFullTextSearch() {
        var cursor: String? = null
        do {
            val progress = reindexFullTextSearch(cursor)
            cursor = progress.cursor
        } while (!progress.done)
    }

    /**
     * Resumable batch: one page of the engine's document walk, with the walk's
     * continuation carried in the opaque [FtsReindexProgress.cursor]. O(page)
     * memory and O(corpus) total.
     */
    override suspend fun reindexFullTextSearch(
        resumeFrom: String?,
        batchSize: Int,
    ): FtsReindexProgress {
        val (progress, trustDocs) =
            locked(LOCK_REINDEX) {
                val page = index.visitDocsPage(EventQuery(), resumeFrom, batchSize)
                // ONE pipelined write per page: serial awaited puts pay per-op ack
                // latency — hours of it on a churny reindex.
                val changed = ArrayList<EventDoc>()
                for (doc in page.docs) {
                    val fields = SearchExtractors.extract(doc.toEvent())
                    // The near-tier arrays are FED data derived from the search
                    // columns at put time, so identical columns can still hide a
                    // stale or MISSING near tier (a corpus fed before those fields
                    // existed). storedNearFields is the visit's evidence of what the
                    // engine holds (null = no evidence). Checked second, since a
                    // changed column already forces the re-put.
                    val columnsChanged = fields != doc.search
                    val nearStale = !columnsChanged && doc.storedNearFields?.let { it != fields.nearFieldsWritten() } == true
                    if (columnsChanged || nearStale) changed += doc.copy(search = fields)
                }
                // A page can carry cards, and the projection applies their
                // cells INLINE on putAll — the same documents the drain
                // re-derives — so those queue for the trust gate like every
                // other write that touches reputation. The gate comes BEFORE
                // [writes] (see [trustGate]), and this page was read under
                // [writes] alone, so they are handed out and re-taken in order
                // below rather than stalling the page (and every writer behind
                // it) on a drain slice. Values agree either way — the card is
                // the newest for its address — so this is the split's rule
                // kept at its fifth entry point, not a repair.
                val (trust, plain) = changed.partition { it.kind in TrustProjection.TRUST_KINDS }
                if (plain.isNotEmpty()) index.putAll(plain)
                FtsReindexProgress(cursor = page.continuation, processedThisBatch = page.docs.size, done = page.continuation == null) to trust
            }
        if (trustDocs.isNotEmpty()) {
            gated(trust = true, LOCK_REINDEX, LOCK_REINDEX_TRUST) {
                // [writes] was released to take the gate in order, so a
                // supersession may have landed since the page was read, and
                // re-putting the page's copy of a replaced card would roll the
                // newer version back. Events are immutable, so an id that is
                // STILL stored is exactly the doc the page holds — near-tier
                // evidence included, which a re-read would not carry.
                val alive = index.existingIds(trustDocs.map { it.id })
                val still = trustDocs.filter { it.id in alive }
                if (still.isNotEmpty()) index.putAll(still)
            }
        }
        return progress
    }

    /**
     * Rebuild the guard-owner cache from the corpus NOW — the explicit barrier
     * for the staleness [WriterTopology.SHARED] otherwise bounds by its refresh
     * interval. Worth calling after a known foreign write (a sync round that
     * mirrored kinds 5/62 from upstream), or to make a test deterministic
     * instead of interval-bound. Costs one distinct-author scan per guard kind;
     * union-only, so it can never unflag an owner.
     */
    suspend fun refreshGuardOwners() = guards.refresh()

    override fun close() {
        // Before the index its background walks read through goes away.
        guards.close()
        index.close()
    }

    private companion object {
        /**
         * Writer-lock stage labels, named for the CALLER rather than the
         * operation: these exist to say which side of the contention a stall is
         * on ("ingest waited 40s while the gate held 40s"). Built once per
         * label, since `insert()` takes this lock per event and a String
         * allocation is not what a measurement should cost.
         */
        val LOCK_INGEST = LockStage("lock.ingest")
        val LOCK_GATE = LockStage("lock.gate")

        /** A trust-relevant insert queueing for [trustGate] — see [touchesTrust]. */
        val LOCK_INGEST_TRUST = LockStage("lock.ingest.trust")
        val LOCK_SWEEP = LockStage("lock.sweep")
        val LOCK_REINDEX = LockStage("lock.reindex")

        /** A sweep's / a reindex page's wait for the trust gate, apart from ingest's — different holders, different remedies. */
        val LOCK_SWEEP_TRUST = LockStage("lock.sweep.trust")
        val LOCK_REINDEX_TRUST = LockStage("lock.reindex.trust")

        /** Batches this size or larger take the bulk path; smaller ones aren't worth its setup. */
        const val BULK_MIN = 16

        /** [EventDoc.NEWEST_FIRST] for the raw read path — the same order over [RawEvent]s. */
        val RAW_NEWEST_FIRST = compareByDescending(RawEvent::createdAt).thenBy(RawEvent::id)

        /** NIP-01's parameterized-replaceable range — the kinds an `a` tag can name. */
        private val ADDRESSABLE = 30_000..39_999

        /** How the expansion reads a subject out of a document. */
        private val DOC_KEYS =
            SubjectKeys<EventDoc>(
                idOf = { it.id },
                authorOf = { it.pubkey },
                addressOf = { if (it.kind in ADDRESSABLE) "${it.kind}:${it.pubkey}:${it.dTagOrEmpty()}" else null },
            )

        /**
         * The same, off a raw row. The address costs a tag parse, so it is paid
         * only for the addressable kinds — the raw path exists to skip exactly
         * that parse on the hits, and a spliced addressable is a rounding error
         * beside them.
         */
        private val RAW_KEYS =
            SubjectKeys<RawEvent>(
                idOf = { it.id },
                authorOf = { it.pubKey },
                addressOf = { raw -> if (raw.kind in ADDRESSABLE) raw.toEvent<Event>().let { "${it.kind}:${it.pubKey}:${it.tags.dTag()}" } else null },
            )
    }
}
