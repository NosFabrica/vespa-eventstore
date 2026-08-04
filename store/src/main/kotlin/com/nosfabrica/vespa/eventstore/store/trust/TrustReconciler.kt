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
package com.nosfabrica.vespa.eventstore.store.trust

import com.nosfabrica.vespa.eventstore.store.ingest.GuardBloom
import com.nosfabrica.vespa.eventstore.store.mapping.toEvent
import com.nosfabrica.vespa.eventstore.vespa.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.vespa.client.EventIndex
import com.nosfabrica.vespa.eventstore.vespa.client.ReputationIndex
import com.nosfabrica.vespa.eventstore.vespa.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.vespa.mapBounded
import com.nosfabrica.vespa.eventstore.vespa.query.EventQuery
import com.nosfabrica.vespa.eventstore.vespa.query.EventYql
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex

/**
 * Repair for projection drift no write can reach — [TrustProjection] derives
 * on WRITE and a trigger only fires once, so a corpus stored before its
 * provider lists stays unprojected with nothing left to trip. [reconcile]
 * repairs per affected service (worth running at startup); [rebuildAll] is the
 * operator's hammer. Also owns the one trust-side DELETION: [sweepOrphanScores]
 * drops the 30382s signed by services no 10040 names.
 *
 * Every MUTATING pass runs through [gate] — the store's writer lock — else a
 * reconcile racing live ingest can overwrite a fresh derivation with one from
 * pre-write state, drift no later trigger repairs. The gate wraps each bounded
 * batch, not the whole walk, so a long rebuild shares the lock with ingest.
 */
class TrustReconciler internal constructor(
    private val index: EventIndex,
    private val reputations: ReputationIndex,
    private val recompute: TrustRecompute,
    private val dirt: DirtLedger,
    private val gate: suspend (suspend () -> Unit) -> Unit = { it() },
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    /** What [reconcile] found: services examined, and the ones it had to re-derive. */
    data class Reconciliation(
        val services: Int,
        val rebuilt: List<String>,
    ) {
        fun isClean() = rebuilt.isEmpty()
    }

    /** What [sweepOrphanScores] found — and, unless [dryRun], removed. */
    data class OrphanSweep(
        /** Distinct kind-30382 signers stored when the sweep started. */
        val servicesSeen: Int,
        /** The signers no stored 10040 names: swept, or (under [dryRun]) the ones that would be. Complete. */
        val orphans: List<String>,
        /** Cards deleted; under [dryRun], how many WOULD be. */
        val scoresSwept: Int,
        /** Candidates a 10040 claimed mid-sweep: left alone, and absent from [orphans]. */
        val remapped: List<String>,
        val dryRun: Boolean,
        /** Whether the sweep refused to run because no 10040 attribution was readable — see [sweepOrphanScores]. */
        val refused: Boolean,
    ) {
        /**
         * Nothing to do AND nothing skipped. A REFUSED sweep is never clean —
         * reporting it as "no orphans" would turn the one state the guardrail
         * exists for into a silent all-clear.
         */
        fun isClean() = !refused && orphans.isEmpty() && remapped.isEmpty()
    }

    /**
     * One subject whose stored parent doc does not match what the 10040+30382
     * records derive: [actual] null = MISSING (records score it, no doc);
     * [expected] null = ORPHAN or leftover (a doc with no records behind it);
     * both present = STALE cells.
     */
    data class TrustDrift(
        val subject: String,
        val expected: ReputationDoc?,
        val actual: ReputationDoc?,
    )

    /** What [verify] found across BOTH corpora. [drift] holds the first examples; [driftCount] is complete. */
    data class TrustAudit(
        /** Subjects with stored 30382s whose expected-vs-actual doc was compared. */
        val subjectsChecked: Int,
        /** Stored reputation parents examined for the orphan check. */
        val parentsChecked: Int,
        /** Every mismatch found (complete count; [drift] samples the first [VERIFY_DRIFT_SAMPLES]). */
        val driftCount: Int,
        val drift: List<TrustDrift>,
        /** Whether the drifted subjects were re-derived in place. */
        val repaired: Boolean,
    ) {
        fun isClean() = driftCount == 0
    }

    /**
     * FULL audit: does every reputation doc match its 10040+30382 records?
     * Queued deferred work is drained first — that is lag, not drift. Then:
     *
     *  1. COMPLETENESS: every subject with stored 30382s is re-derived
     *     ([TrustRecompute.deriveBatch]) and compared against the stored parent
     *     — catches missing docs, stale cells, wrong observers, leftovers.
     *  2. ORPHANS: every stored parent's pubkey is checked against a
     *     [GuardBloom] of the subjects phase 1 saw. No false negatives, so
     *     "not seen" proves the doc had no records; a false positive (1e-6)
     *     can only HIDE an orphan, never invent drift.
     *
     * Both phases SCREEN lock-free — millions of clean subjects take no lock —
     * and only suspects are re-judged under the gate ([confirm]) after another
     * drain, re-derived and re-read atomically against writers, so a live
     * store cannot produce false positives. Follower cells compare at float32
     * (the stored precision; doubles would report engine rounding as drift).
     * [repair] re-derives exactly the confirmed subjects in place — the
     * targeted alternative to [rebuildAll].
     */
    suspend fun verify(
        repair: Boolean = false,
        onProgress: ((subjectsChecked: Int) -> Unit)? = null,
    ): TrustAudit {
        dirt.drain(gate)
        val seen = GuardBloom(expectedInsertions = index.count(EventQuery(kinds = listOf(ContactCardEvent.KIND))).coerceAtLeast(1024), fpp = 1e-6)
        val samples = ArrayList<TrustDrift>()
        var driftCount = 0
        var subjectsChecked = 0

        // The gated re-judgement of screened suspects; also repairs, when asked.
        suspend fun confirm(suspects: List<String>) {
            if (suspects.isEmpty()) return
            dirt.drain(gate) // settle work queued since the last drain before judging it
            gate {
                val expected = recompute.deriveBatch(suspects, recompute.providerMap())
                val drifted = ArrayList<String>()
                suspects
                    .mapBounded(QUERY_FANOUT) { it to reputations.get(it) }
                    .forEach { (subject, actual) ->
                        if (!matches(expected[subject], actual)) {
                            driftCount++
                            drifted += subject
                            if (samples.size < VERIFY_DRIFT_SAMPLES) samples += TrustDrift(subject, expected[subject], actual)
                        }
                    }
                if (repair && drifted.isNotEmpty()) recompute.recomputeBatch(drifted, recompute.providerMap(), removeEmpties = true)
            }
        }

        // Phase 1 — completeness: expected (from the records) vs actual (stored),
        // per batch of subjects streamed off the 30382 corpus.
        val buffer = LinkedHashSet<String>()

        suspend fun screenBatch() {
            val batch = buffer.filter { !seen.mightContain(it) }
            buffer.clear()
            if (batch.isEmpty()) return
            batch.forEach(seen::add)
            val expected = recompute.deriveBatch(batch, recompute.providerMap())
            val suspects =
                batch
                    .mapBounded(QUERY_FANOUT) { it to reputations.get(it) }
                    .filter { (subject, actual) -> !matches(expected[subject], actual) }
                    .map { it.first }
            confirm(suspects)
            subjectsChecked += batch.size
            onProgress?.invoke(subjectsChecked)
        }
        index.visitIds(EventQuery(kinds = listOf(ContactCardEvent.KIND)), withDTag = true) { page ->
            page.forEach { it.dTag?.takeIf(Hex::isHex64)?.let(buffer::add) }
            if (buffer.size >= VERIFY_BATCH) screenBatch()
            true
        }
        screenBatch()

        // Phase 2 — orphans: stored parents phase 1 never derived. The non-hex
        // filter keeps the ledger's dirt marker out. Candidates go through the
        // same gated confirm — a subject whose first records landed mid-audit
        // is judged there, not miscalled an orphan.
        var parentsChecked = 0
        reputations.visitPubkeys { page ->
            parentsChecked += page.count { Hex.isHex64(it) }
            confirm(page.filter { Hex.isHex64(it) && !seen.mightContain(it) })
            true
        }
        return TrustAudit(subjectsChecked, parentsChecked, driftCount, samples, repair)
    }

    /**
     * Re-derive the services whose scores are not projected under every
     * observer currently mapped to them. Drains the [DirtLedger] first: a
     * marker left by a crashed process names drift EXACTLY, so a restart heals
     * before ranked search serves stale cells.
     *
     * Needed because derivation happens on WRITE and a trigger only fires once:
     * dedup rejects an already-held event before the projection sees it, so a
     * card skipped because its service had no 10040 yet stays unprojected as
     * long as both events remain stored. Mirrors hit this as a matter of course
     * (scores outnumber lists by ~4 orders of magnitude and arrive first),
     * leaving a projection that is empty, correct-looking, and unable to repair
     * itself.
     *
     * Check: per mapped service, sample a few cards and ask, per observer PER
     * DIMENSION ([TrustProviders]), whether a sampled subject carries that
     * observer's cell in the tensor the mapping owns. Only cards that ASSERT a
     * dimension can prove it unprojected (else a corpus that never carries the
     * mapped tag would re-walk on every startup); checking observers rather
     * than mere existence also catches a RE-MAPPED service. Sampling settles it
     * because the never-triggered failure is all-or-nothing per (service,
     * observer); PARTIAL drift is the dirty marker's job, repaired exactly by
     * [DirtLedger.drain]. A sample landing on retracted subjects just costs one
     * idempotent walk.
     *
     * [onProgress] reports before and after the fanned-out sampling; the serial
     * rebuild walks report `(total, total, rebuilt, derivedInService)` so a
     * caller can show a real fraction through exactly the slow part.
     */
    suspend fun reconcile(
        samplesPerService: Int = DEFAULT_RECONCILE_SAMPLES,
        onProgress: ((inspected: Int, total: Int, rebuilt: Int, derivedInService: Int) -> Unit)? = null,
    ): Reconciliation {
        dirt.drain(gate)
        val providers = recompute.providerMap()
        if (providers.isEmpty()) return Reconciliation(0, emptyList())
        val total = providers.services.size
        onProgress?.invoke(0, total, 0, 0)

        // Phase 1 — sampling, fanned out: pure reads, no ordering dependency.
        // null = nothing stored for the service; true/false = sampled
        // projected/unprojected.
        val cutoff = nowSecs()
        val verdicts =
            providers.services.toList().mapBounded(QUERY_FANOUT) { service ->
                val sample =
                    index.search(
                        EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service), limit = samplesPerService, notExpiredAt = cutoff),
                    )
                if (sample.isEmpty()) return@mapBounded null
                // Only sampled cards that CARRY a tag can prove that dimension
                // unprojected — else every startup would re-walk the service.
                val cards = sample.mapNotNull { doc -> subjectOf(doc)?.let { s -> (doc.toEvent() as? ContactCardEvent)?.let { s to it } } }
                val rankSubjects = cards.filter { it.second.boundedRank() != null }.map { it.first }.distinct()
                val followerSubjects = cards.filter { it.second.followerCount() != null }.map { it.first }.distinct()
                val parents = (rankSubjects + followerSubjects).distinct().mapNotNull { s -> reputations.get(s)?.let { s to it } }.toMap()
                val rankProjected =
                    rankSubjects.isEmpty() ||
                        providers.rank[service].orEmpty().all { o -> rankSubjects.any { parents[it]?.influenceScores?.containsKey(o) == true } }
                val followersProjected =
                    followerSubjects.isEmpty() ||
                        providers.followers[service].orEmpty().all { o -> followerSubjects.any { parents[it]?.followerCounts?.containsKey(o) == true } }
                service to (rankProjected && followersProjected)
            }
        val examined = verdicts.count { it != null }
        onProgress?.invoke(total, total, 0, 0)

        // Phase 2 — rebuilds: heavy walks whose mutating batches take the
        // writer lock through the gate.
        val rebuilt = mutableListOf<String>()
        for (service in verdicts.mapNotNull { v -> v?.takeIf { !it.second }?.first }) {
            recompute.recomputeWalk(
                EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service)),
                onSubjects = { derived -> onProgress?.invoke(total, total, rebuilt.size, derived) },
                gate = gate,
            )
            rebuilt += service
            onProgress?.invoke(total, total, rebuilt.size, 0)
        }
        return Reconciliation(examined, rebuilt)
    }

    /**
     * DELETE the orphan scores: every stored kind-30382 whose SIGNER no stored
     * 10040 names, either dimension. Unmapped cards project nothing, rank
     * nothing, gate nothing — a relay mirroring 30382s by kind accumulates them
     * by the hundreds of thousands per service. A DELETION, not a repair, and
     * deliberately never automatic — [reconcile] never calls it.
     *
     * REFUSAL invariant: "no 10040 names it" and "no 10040 was readable"
     * produce the identical candidate list, and the second is a real, observed
     * state (a corpus mirrored before its provider lists — the same ambiguity
     * behind [ProviderMap.get]'s no-empty-cache rule). Sweeping there would
     * irreversibly delete the ENTIRE score corpus, so an empty attribution map
     * sweeps nothing and reports [OrphanSweep.refused].
     *
     * Safety: the candidate list is a snapshot, so membership is RE-CHECKED
     * under [gate] before every page — a 10040 landing mid-sweep drops its
     * service ([OrphanSweep.remapped]); deleting a mapped service's card is the
     * one outcome this must never produce. Pages deleted before that 10040 need
     * no repair: the list's arrival queues its service walk, which derives from
     * post-deletion state. The orphans themselves need no re-derivation
     * (unmapped signers contribute no cells), which is why this removes through
     * the raw index, not the projection; stale cells from a formerly-mapped
     * service are [verify]'s job. Expired 10040s attribute nothing, matching
     * the projection. The re-check assumes ONE writer — this process's lock
     * (docs/multi-node-consistency.md).
     *
     * A deletion is not a tombstone: a by-kind mirror re-downloads what this
     * freed — a companion to narrowing that sync, not a substitute. [dryRun]
     * answers "how much would this free" from the one grouping query, no writes.
     */
    suspend fun sweepOrphanScores(
        dryRun: Boolean = false,
        onProgress: ((servicesDone: Int, totalServices: Int, scoresSwept: Int, totalScores: Int) -> Unit)? = null,
    ): OrphanSweep {
        val providers = recompute.providerMap()
        // The refusal above: no attribution readable = every service looks
        // orphaned. Nothing is deleted and the caller is told why.
        if (providers.isEmpty()) return OrphanSweep(0, emptyList(), 0, emptyList(), dryRun, refused = true)

        // ONE server-side grouping (EventIndex.countByAuthor): every signer and
        // its card count with no doc reconstruction, so the dry run needs no
        // per-service query. A signer the grouping missed keeps its cards —
        // completeness only matters in the harmless direction.
        val cardsBySigner = index.countByAuthor(EventQuery(kinds = listOf(ContactCardEvent.KIND)))
        val candidates = cardsBySigner.keys.filterNot(providers::maps)
        val doomed = candidates.sumOf { cardsBySigner[it] ?: 0 }
        onProgress?.invoke(0, candidates.size, 0, doomed)
        if (candidates.isEmpty()) return OrphanSweep(cardsBySigner.size, emptyList(), 0, emptyList(), dryRun, refused = false)

        // The dry run is answered entirely by the grouping above — no further
        // reads, no lock taken anywhere.
        if (dryRun) return OrphanSweep(cardsBySigner.size, candidates, doomed, emptyList(), dryRun = true, refused = false)

        val swept = ArrayList<String>()
        val remapped = ArrayList<String>()
        var scores = 0
        for (service in candidates) {
            val page =
                EventQuery(
                    // No expiry cutoff: an expired orphan card is still stored,
                    // and this pass exists to reclaim exactly that storage.
                    kinds = listOf(ContactCardEvent.KIND),
                    authors = listOf(service),
                    limit = SWEEP_PAGE,
                    // A sweep wants ANY page: explicit ranking opts out of the
                    // recency planner's count probes, compiling to identical YQL.
                    ranking = EventYql.RANK_UNRANKED,
                )
            var stillOrphan = true
            var rounds = 0
            var drained = false
            var lastPage: Set<String>? = null
            while (!drained && rounds++ < MAX_SWEEP_ROUNDS) {
                gate {
                    // Re-read INSIDE the lock, per page: a 10040 committed since
                    // the snapshot must stop the deletion at the first page
                    // boundary, not at the end.
                    if (recompute.providerMap().maps(service)) {
                        stillOrphan = false
                        drained = true
                        return@gate
                    }
                    val docs = index.search(page)
                    if (docs.isEmpty()) {
                        drained = true
                        return@gate
                    }
                    val ids = docs.mapTo(HashSet()) { it.id }
                    // An acked remove is visible to search (EventIndex contract),
                    // so an identical page means deletes are not landing. Fail
                    // LOUD rather than report freed storage that is still stored.
                    check(ids != lastPage) { "orphan sweep is not shrinking: ${ids.size} scores by $service survived their own removal" }
                    index.removeDocs(docs)
                    scores += docs.size
                    lastPage = ids
                }
                // PROCESSED services, not swept ones: a progress bar must still
                // reach the total when a candidate turns out remapped.
                onProgress?.invoke(swept.size + remapped.size, candidates.size, scores, doomed)
            }
            check(drained) { "orphan sweep did not drain $service after $MAX_SWEEP_ROUNDS rounds of $SWEEP_PAGE" }
            if (stillOrphan) swept += service else remapped += service
            onProgress?.invoke(swept.size + remapped.size, candidates.size, scores, doomed)
        }
        return OrphanSweep(cardsBySigner.size, swept, scores, remapped, dryRun = false, refused = false)
    }

    /**
     * Re-derive every parent doc from scratch, THEN sweep the parents the card
     * walk cannot reach: a doc whose subject has no cards left (its last card's
     * removal crashed pre-recompute) is enumerated from the REPUTATION corpus
     * and re-derived to empty, which removes it — the card walk by construction
     * only visits subjects that still have cards. Bounded only by the corpus.
     */
    suspend fun rebuildAll() {
        recompute.recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND)), gate = gate)
        val buffer = ArrayList<String>(ORPHAN_BATCH)

        suspend fun flush() {
            if (buffer.isNotEmpty()) {
                gate { recompute.recomputeBatch(buffer.toList(), recompute.providerMap(), removeEmpties = true) }
                buffer.clear()
            }
        }
        reputations.visitPubkeys { page ->
            // Only real subjects (64-hex): the projection's own bookkeeping doc
            // (DirtLedger's marker) must not be "repaired" away mid-crash.
            page.filterTo(buffer) { Hex.isHex64(it) }
            if (buffer.size >= ORPHAN_BATCH) flush()
            true
        }
        flush()
    }

    private companion object {
        // Cards sampled per service by [reconcile]: the never-triggered failure
        // is all-or-nothing per (service, observer), so a handful settles it.
        const val DEFAULT_RECONCILE_SAMPLES = 3

        // Subjects per orphan-sweep re-derive round (memory-bounded, like
        // TrustRecompute's walk batches).
        const val ORPHAN_BATCH = 20_000

        // Subjects compared per gated [verify] batch (memory- and lock-hold-bounded).
        const val VERIFY_BATCH = 20_000

        // Drift examples carried in the [TrustAudit] report; the COUNT is always complete.
        const val VERIFY_DRIFT_SAMPLES = 100

        // Cards deleted per [sweepOrphanScores] round — one page read, one bulk
        // delete, one lock hold. Same size the store's own sweep pages at.
        const val SWEEP_PAGE = 10_000

        // Runaway backstop per service, not a delete cap (100M cards at the page
        // above); only reachable when a service keeps publishing INTO the sweep.
        const val MAX_SWEEP_ROUNDS = 10_000

        /**
         * Expected-vs-stored equality for [verify]. Influence cells (int8) are
         * exact; follower cells compare at their stored float32 precision — a
         * double compare would call engine rounding "drift". An EMPTY
         * derivation matches exactly a missing doc.
         */
        fun matches(
            expected: ReputationDoc?,
            actual: ReputationDoc?,
        ): Boolean {
            if (expected == null || actual == null) return (expected == null) && (actual == null)
            return expected.influenceScores == actual.influenceScores &&
                expected.followerCounts.keys == actual.followerCounts.keys &&
                expected.followerCounts.all { (observer, count) -> actual.followerCounts[observer]?.toFloat() == count.toFloat() }
        }
    }
}
