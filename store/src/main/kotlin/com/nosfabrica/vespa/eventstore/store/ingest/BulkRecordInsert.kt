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
package com.nosfabrica.vespa.eventstore.store.ingest

import com.nosfabrica.vespa.eventstore.store.Rejections
import com.nosfabrica.vespa.eventstore.store.mapping.VespaText
import com.nosfabrica.vespa.eventstore.store.mapping.addressOrNull
import com.nosfabrica.vespa.eventstore.store.mapping.owner
import com.nosfabrica.vespa.eventstore.store.mapping.toDoc
import com.nosfabrica.vespa.eventstore.store.mapping.toEvent
import com.nosfabrica.vespa.eventstore.vespa.IngestStats
import com.nosfabrica.vespa.eventstore.vespa.PUT_FANOUT
import com.nosfabrica.vespa.eventstore.vespa.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.vespa.client.EventIndex
import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc
import com.nosfabrica.vespa.eventstore.vespa.mapBounded
import com.nosfabrica.vespa.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip40Expiration.isExpired
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent

/**
 * The bulk insert fast path for one run of plain events (no kind 5/62) —
 * batches that CONTAIN deletions/vanishes take [BulkMixedInsert] instead. It
 * enforces the same Nostr rules as the per-event [NostrSemanticsStore] path, but
 * with BATCHED I/O. The per-event path costs 3–5 engine round trips per event,
 * which is useless against a million-event sync. Stages:
 *
 *  A. local checks (ephemeral accepted-not-stored, expired rejected, later
 *     copies of an id already in this run rejected as duplicates);
 *  B. one `id in (…)` duplicate query per [CHECK_CHUNK], fanned out bounded;
 *  C. per-owner tombstone/vanish guards (one query per owner chunk) — read in
 *     [commit], UNDER the writer lock, never in [plan]: a kind 5/62 committed
 *     by a neighbouring batch between a lock-free guard read and this batch's
 *     writes would resurrect the events it covers. The [GuardOwners] gate
 *     keeps the locked cost at zero for the common batch with no flagged
 *     owners;
 *  D. per-address supersession resolved IN RUN ORDER. Existing versions are
 *     fetched per (kind, author), and losers inside the run are
 *     Accepted-then-superseded exactly as sequential inserts would end up;
 *  E. one pipelined [EventIndex.putAll] of the survivors.
 *
 * Every stage read is unbounded — batched I/O may not trade exactness for
 * speed, and a short page here would be a wrong write, not a small answer.
 */
internal class BulkRecordInsert(
    private val index: EventIndex,
    private val relay: NormalizedRelayUrl?,
    private val guards: GuardOwners,
) {
    /**
     * What [plan] resolved from its LOCK-FREE reads (stages A–B), handed to
     * [commit] to finish under the single writer lock. Dedup can race here —
     * a duplicate that slips past is an idempotent re-put of identical bytes.
     * The guard and supersession reads canNOT race: both decide against the
     * store's current state, so they run in [commit], atomic with the writes.
     */
    internal class Plan(
        val events: List<Event>,
        val outcome: Array<IEventStore.InsertOutcome?>,
    )

    /** Plan then commit in one call, for callers that already hold the writer lock across both. */
    suspend fun run(events: List<Event>): List<IEventStore.InsertOutcome> = commit(plan(events))

    /**
     * The LOCK-FREE half: stages A–B (local checks, dedup) — reads whose staleness
     * is harmless (see [Plan]) — so parallel relays' batches overlap them. The
     * guard and supersession reads belong to [commit], under the lock.
     */
    suspend fun plan(events: List<Event>): Plan {
        val outcome = arrayOfNulls<IEventStore.InsertOutcome>(events.size)

        fun alive() = events.indices.filter { outcome[it] == null }

        // Stage A — no I/O: ephemeral accepted-not-stored, expired rejected,
        // later copies of an id already in this run rejected as duplicates, and
        // text the engine will not accept rejected before it can throw.
        val seen = HashSet<String>()
        events.forEachIndexed { i, e ->
            when {
                e.kind.isEphemeral() -> {
                    outcome[i] = IEventStore.InsertOutcome.Accepted
                }

                e.isExpired() -> {
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.EXPIRED)
                }

                !seen.add(e.id) -> {
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.DUPLICATE)
                }

                // Last of the free checks: it walks the content, where the others
                // only read a field. Still cheap next to the round trip it saves —
                // and far cheaper than the alternative, which is the feed client
                // throwing mid-batch and taking every event beside it down. See
                // [VespaText].
                VespaText.firstIllegalField(e) != null -> {
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.UNSTORABLE_TEXT)
                }
            }
        }

        // Stage B — ids already stored, via the EXISTENCE check (membership
        // only, no summaries: at mirror hit rates ~99% of these ids come back,
        // and materializing each one's full document to read its id back was
        // the single largest ingest cost — see EventIndex.existingIds). The
        // chunk queries are independent reads, so they fan out with BOUNDED
        // concurrency. Serialized round trips starve the batch, but unbounded
        // fan-out measurably 504s the engine's summary stage.
        val stored = HashSet<String>()
        IngestStats.timed("dedup") {
            alive()
                .map { events[it].id }
                .chunked(DEDUP_CHUNK)
                .mapBounded(QUERY_FANOUT) { chunk -> index.existingIds(chunk) }
                .forEach { stored += it }
        }
        alive().forEach { i -> if (events[i].id in stored) outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.DUPLICATE) }
        return Plan(events, outcome)
    }

    /**
     * The LOCKED half: the tombstone/vanish guards and the supersession
     * read+resolve — both must see every prior commit's writes, so they run
     * under the single writer lock — then the pipelined writes. Kept as short
     * as the semantics allow: the guard queries vanish for unflagged owners
     * (the common batch), and an empty remove or put set skips its round trip.
     */
    suspend fun commit(plan: Plan): List<IEventStore.InsertOutcome> {
        val events = plan.events
        val outcome = plan.outcome

        fun alive() = events.indices.filter { outcome[it] == null }

        // Stage C — tombstone + vanish guards, BATCHED by owner: one deletion query
        // and one vanish query per CHECK_CHUNK of owners (then bucketed by author),
        // NOT one pair per owner. A content batch touches ~500 owners; per-owner that
        // was ~1000 round trips at QUERY_FANOUT=4 — the ingest's real bottleneck —
        // now it is a handful. Nothing caps the guard queries, so the batched view
        // is exact by construction — there is no "the page may have cut this short"
        // case left to fall back from.
        val owners = alive().groupBy { events[it].owner() }
        val guardSets =
            IngestStats.timed("guards") {
                // Only owners with a stored tombstone/vanish can have guard
                // docs at all (GuardOwners); everyone else's sets are provably
                // empty — usually ALL of a content batch, skipping both queries.
                // Gated independently: deleters vastly outnumber vanishers, so
                // the vanish query usually disappears even in a flagged batch.
                val flaggedDeleters = guards.filterFlaggedDeleters(owners.keys)
                val flaggedVanishers = guards.filterFlaggedVanishers(owners.keys)
                val tombs = if (flaggedDeleters.isEmpty()) emptyMap() else guardDocs(flaggedDeleters, DeletionEvent.KIND)
                val vanishes = if (flaggedVanishers.isEmpty()) emptyMap() else guardDocs(flaggedVanishers, RequestToVanishEvent.KIND)
                owners.keys.associateWith { (tombs[it].orEmpty() to vanishes[it].orEmpty()) }
            }
        for ((owner, idxs) in owners) {
            val (tombs, vanishes) = guardSets.getValue(owner)
            // target -> the newest guarding tombstone's created_at.
            val byId = HashMap<String, Long>()
            val byAddress = HashMap<String, Long>()
            tombs.forEach { doc ->
                doc.tags.forEach { t ->
                    if (t.size > 1) {
                        when (t[0]) {
                            "e" -> byId.merge(t[1], doc.createdAt, ::maxOf)
                            "a" -> byAddress.merge(t[1], doc.createdAt, ::maxOf)
                        }
                    }
                }
            }
            val vanishAt =
                vanishes
                    .mapNotNull { doc -> (doc.toEvent() as? RequestToVanishEvent)?.takeIf { it.shouldVanishFrom(relay) }?.createdAt }
                    .maxOrNull() ?: Long.MIN_VALUE
            for (i in idxs) {
                val e = events[i]
                val guard = maxOf(byId[e.id] ?: Long.MIN_VALUE, e.addressOrNull()?.let { byAddress[it] } ?: Long.MIN_VALUE)
                if (guard >= e.createdAt) {
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.DELETED)
                } else if (e.createdAt <= vanishAt) {
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.VANISHED)
                }
            }
        }

        // Group survivors by replaceable address; plain events go straight to
        // toPut. Local work — after stage C, so a guard-rejected event can
        // never reach the writes below.
        val toPut = LinkedHashMap<String, Event>() // id -> event scheduled for storage
        val groups = LinkedHashMap<Triple<Int, String, String?>, MutableList<Int>>()
        alive().forEach { i ->
            val e = events[i]
            if (e.kind.isReplaceable() || e.kind.isAddressable()) {
                // Missing d normalizes to "" (one address per NIP-01), matching
                // the doc-side dTagOrEmpty() bucketing below.
                val d = if (e.kind.isAddressable()) e.tags.dTag().orEmpty() else null
                groups.getOrPut(Triple(e.kind, e.pubKey, d)) { mutableListOf() } += i
            } else {
                toPut[e.id] = e
            }
        }

        // Stage D — supersession per replaceable address.
        if (index.supersedesViaPut) {
            // The address-keyed engine enforces newest-wins per put, so replay
            // each address's run through putIfNewer IN ORDER — identical to the
            // per-event path, so outcomes match the read path exactly: an event
            // that loses to the stored version OR to an earlier same-batch version
            // comes back false and is REPLACED. Different addresses run
            // concurrently; a single address stays sequential (each put depends on
            // the previous). Replaceable winners are written by putIfNewer itself;
            // toPut carries only the regular events.
            IngestStats.timed("versions") {
                // Conditional puts are writes — fan out much wider than QUERY_FANOUT
                // (which is capped for searches) so they pipeline like the raw feed.
                groups.entries.toList().mapBounded(PUT_FANOUT) { (_, idxs) ->
                    for (i in idxs) {
                        if (!index.putIfNewer(events[i].toDoc())) {
                            outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.REPLACED)
                        }
                    }
                }
            }
            if (toPut.isNotEmpty()) index.putAll(toPut.values.map { it.toDoc() })
            alive().forEach { i -> outcome[i] = IEventStore.InsertOutcome.Accepted }
            return outcome.map { it ?: IEventStore.InsertOutcome.Rejected(Rejections.INSERT_FAILED) }
        }

        // Read-then-supersede (default): existing versions for every touched
        // address, chunked. Replaceables are fetched by (kind, authors…).
        // Addressables are fetched by (kind, author, d-tags…) via tag_index recall,
        // then bucketed doc-side (the d filter is exact there).
        val existing = HashMap<Triple<Int, String, String?>, MutableList<EventDoc>>()
        val addressable = groups.keys.filter { it.third != null }
        val replaceable = groups.keys.filter { it.third == null }
        val versionQueries =
            buildList {
                for ((kind, keys) in replaceable.groupBy { it.first }) {
                    keys.map { it.second }.distinct().chunked(CHECK_CHUNK).forEach { authors ->
                        add(EventQuery(kinds = listOf(kind), authors = authors))
                    }
                }
                // Addressables recall PER (kind, author), never across authors. A
                // multi-author (authors x d-tags) query is a CROSS PRODUCT. In a
                // dense corpus (dozens of service keys scoring the same subjects)
                // that recalls authors×ds real docs — an unbounded response on
                // the ingest hot path, and missed existing versions on any
                // deployment that capped hits (it truncates silently). One
                // author's d-set is bounded.
                for ((ka, keys) in addressable.groupBy { it.first to it.second }) {
                    val (kind, author) = ka
                    val ds = keys.mapNotNull { it.third }.filter { it.isNotEmpty() }.distinct()
                    ds.chunked(CHECK_CHUNK).forEach { chunk ->
                        add(EventQuery(kinds = listOf(kind), authors = listOf(author), tags = mapOf("d" to chunk)))
                    }
                    // Empty d: a stored version with NO d tag at all carries no
                    // "d:" pair in tag_index, so the d-keyed query above can never
                    // recall it. Go broad by (kind, author) — the same fallback
                    // the per-event path (EventIndex.putIfNewer) and the mixed
                    // preload apply. Bounded: one author's docs of one kind.
                    if (keys.any { it.third.isNullOrEmpty() }) add(EventQuery(kinds = listOf(kind), authors = listOf(author)))
                }
            }
        IngestStats
            .timed("versions") {
                versionQueries.mapBounded(QUERY_FANOUT) { q -> index.search(q) }
            }.forEach { docs ->
                docs.forEach { doc ->
                    val d = if (doc.kind.isAddressable()) doc.dTagOrEmpty() else null
                    existing.getOrPut(Triple(doc.kind, doc.pubkey, d)) { mutableListOf() } += doc
                }
            }
        val removeFromStore = ArrayList<EventDoc>()
        val removeIds = HashSet<String>()

        fun scheduleRemove(doc: EventDoc) {
            if (removeIds.add(doc.id)) removeFromStore += doc
        }
        for ((key, idxs) in groups) {
            val versions = existing[key].orEmpty()
            // The run competes against the store's best. Every stored version
            // strictly older than the final winner is swept.
            var bestDoc: EventDoc? = versions.maxWithOrNull(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id })
            var bestAt = versions.maxOfOrNull { it.createdAt } ?: Long.MIN_VALUE
            var bestId = versions.filter { it.createdAt == bestAt }.minOfOrNull { it.id }
            var bestInRun: Int? = null
            for (i in idxs) {
                val e = events[i]
                val lost = bestId != null && (bestAt > e.createdAt || (bestAt == e.createdAt && bestId!! < e.id))
                if (lost) {
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.REPLACED)
                } else {
                    // The previous best is superseded. An in-run best stays
                    // Accepted but never lands; a stored best is removed.
                    bestInRun?.let { toPut.remove(events[it].id) }
                    bestDoc?.let { scheduleRemove(it) }
                    bestDoc = null
                    bestInRun = i
                    bestAt = e.createdAt
                    bestId = e.id
                    toPut[e.id] = e
                }
            }
            // Older stored versions beyond the single best also fall (drift repair).
            versions.forEach { doc -> if (doc.id != bestDoc?.id && doc.id !in removeIds) scheduleRemove(doc) }
        }
        // Skip the round trip when nothing supersedes — the common case for a
        // fresh corpus (first-seen addresses remove nothing). The docs are in
        // hand, so removeDocs lets the projection react without a get per id.
        if (removeFromStore.isNotEmpty()) index.removeDocs(removeFromStore)

        // Stage E — one pipelined write for everything that survived. (Timing
        // is booked by the layers below: the projection decorator splits it
        // into write / proj.fetch / proj.write.)
        if (toPut.isNotEmpty()) index.putAll(toPut.values.map { it.toDoc() })
        alive().forEach { i -> outcome[i] = IEventStore.InsertOutcome.Accepted }
        return outcome.map { it ?: IEventStore.InsertOutcome.Rejected(Rejections.INSERT_FAILED) }
    }

    /**
     * Every guard event of [kind] (deletion or vanish) for [owners], bucketed by
     * author. One query per [CHECK_CHUNK] of owners rather than one per owner.
     * The chunking is purely about how wide one round trip is: no query here
     * carries a limit, so a chunk always comes back whole and one prolific
     * deleter cannot crowd out the other owners in its chunk.
     */
    private suspend fun guardDocs(
        owners: Collection<String>,
        kind: Int,
    ): Map<String, List<EventDoc>> =
        owners
            .toList()
            .chunked(CHECK_CHUNK)
            .mapBounded(QUERY_FANOUT) { chunk ->
                index.search(EventQuery(kinds = listOf(kind), authors = chunk))
            }.flatten()
            .groupBy { it.pubkey }

    private companion object {
        // Ids/authors/d-tags per check query. Not a result cap — no query here
        // carries a limit — just how wide one round trip is built.
        const val CHECK_CHUNK = 500

        /**
         * Stage B's dedup chunk width alone — the guard/version queries keep
         * [CHECK_CHUNK]; their shapes were never measured at other widths.
         * The measured curve (benchmark/README.md, dedup A/B): existence
         * throughput still climbs at 2000 ids/chunk (~1.5x the 500 default at
         * fan-out 4), but each query occupies the engine longer, which is the
         * read-starvation lever — so the DEFAULT stays at the width the
         * REQ-latency A/B was measured at, and `VESPA_DEDUP_CHUNK` lets a
         * deployment that values sync speed over read latency widen it.
         */
        val DEDUP_CHUNK: Int = System.getenv("VESPA_DEDUP_CHUNK")?.toIntOrNull()?.coerceAtLeast(1) ?: CHECK_CHUNK
    }
}
