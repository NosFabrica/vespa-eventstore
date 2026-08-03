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
package com.vitorpamplona.quartz.eventstore.store.trust

import com.vitorpamplona.quartz.eventstore.store.ingest.GuardBloom
import com.vitorpamplona.quartz.eventstore.store.mapping.toEvent
import com.vitorpamplona.quartz.eventstore.vespa.QUERY_FANOUT
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.ReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.ReputationDoc
import com.vitorpamplona.quartz.eventstore.vespa.mapBounded
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex

/**
 * The repair tool for a projection no write can reach — [TrustProjection]
 * derives on WRITE, and a trigger only fires once, so a corpus stored before
 * its provider lists stays unprojected with nothing left to trip. This class
 * finds and re-derives that drift: [reconcile] per affected service (worth
 * running at startup), [rebuildAll] as the operator's hammer.
 *
 * It also owns the one trust-side operation that DELETES rather than repairs:
 * [sweepOrphanScores] drops the kind-30382s signed by services no 10040 names,
 * which can never become a tensor cell for anyone.
 *
 * Every MUTATING pass runs through [gate] — the store's writer lock. Without
 * it a reconcile racing live ingest can overwrite a concurrent write's fresh
 * derivation with one computed from pre-write state, drift no later trigger
 * repairs. The gate wraps each bounded batch, not the whole walk, so a
 * minutes-long rebuild shares the lock with ingest instead of stalling it.
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
        fun isClean() = orphans.isEmpty() && remapped.isEmpty()
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
     * FULL audit: does every reputation doc match its 10040+30382 counterparts?
     *
     * Answers it from both directions. First the queued deferred work is
     * drained — that is lag, not drift, and auditing through it would report
     * every in-flight subject. Then:
     *
     *  1. COMPLETENESS: every subject with stored 30382s is re-derived from the
     *     records ([TrustRecompute.deriveBatch] — the same pure derivation the
     *     projection writes, including the current 10040 attribution) and
     *     compared against the stored parent. Catches missing docs, stale
     *     cells, wrong observers, and leftovers for retracted subjects.
     *  2. ORPHANS: every stored parent's pubkey is streamed
     *     ([ReputationIndex.visitPubkeys]) and checked against a Bloom filter
     *     of the subjects phase 1 saw. [GuardBloom] has NO false negatives, so
     *     "not seen" proves the doc had no records when phase 1 passed. (A
     *     false positive can only HIDE an orphan — at the configured 1e-6 rate
     *     — never invent drift; the same filter also dedups subjects whose
     *     cards span visit pages.)
     *
     * Both phases SCREEN lock-free — millions of clean subjects must not take
     * the writer lock at all — and only the suspects are re-judged under the
     * gate ([confirm]): queued work is drained first (a write racing the
     * screen is lag, not drift), then the suspect is re-derived and re-read
     * atomically against writers. Only confirmed drift is counted, so a live
     * store cannot produce false positives; the cost is a short lock hold per
     * suspect batch, zero on a clean store.
     *
     * Follower cells are compared at float32 precision — that is how the engine
     * stores them, and comparing doubles would report storage rounding as drift.
     *
     * [repair] re-derives exactly the confirmed subjects in place (same gate),
     * the targeted alternative to [rebuildAll]. Cost of the audit itself: one
     * read of every 30382 plus one get per scored subject — no writes beyond
     * the drains (and the repairs, when asked).
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
        // filter keeps the ledger's dirt marker out of the audit. Candidates go
        // through the same gated confirm — a subject whose first records landed
        // mid-audit re-derives non-empty there and is judged like any other,
        // not miscalled an orphan.
        var parentsChecked = 0
        reputations.visitPubkeys { page ->
            parentsChecked += page.count { Hex.isHex64(it) }
            confirm(page.filter { Hex.isHex64(it) && !seen.mightContain(it) })
            true
        }
        return TrustAudit(subjectsChecked, parentsChecked, driftCount, samples, repair)
    }

    /**
     * Re-derive the services whose scores are not projected under every observer
     * currently mapped to them. Drains the [DirtLedger] first: a marker left by
     * a crashed process — or by a deferred-mode shutdown with work still
     * queued — names drift EXACTLY, and repairing it here means a restart heals
     * before ranked search serves stale cells.
     *
     * ## Why this is needed at all
     *
     * Derivation happens on WRITE: [TrustProjection.putAll] projects the cards
     * in the batch, and a 10040 arriving re-walks the services it names. Both
     * are triggers, and a trigger only fires once. Dedup rejects an event the
     * store already holds BEFORE the projection sees it, so once a corpus is
     * stored neither trigger can fire again — a card skipped because its
     * service had no 10040 yet stays unprojected for as long as both events
     * remain in the store.
     *
     * A mirror hits this as a matter of course: scores outnumber provider lists
     * by four orders of magnitude and arrive first, and the run that finally
     * writes the 10040 may be one in which every score is already a duplicate.
     * The result is a projection that is empty, correct-looking and unable to
     * repair itself — every ranked search returns nothing, with no error anywhere.
     *
     * ## What it checks
     *
     * A service is projected when its subjects carry a cell for EACH of its
     * observers — the observer set, because a popular provider is named by many
     * 10040s and every one of those users must rank through it. So for each
     * mapped service this samples a few of its cards and asks, per observer PER
     * DIMENSION ([TrustProviders]), whether a sampled subject has that
     * observer's cell in the tensor the mapping owns — rank-mapped observers
     * are checked against influence cells, followers-mapped ones against
     * follower cells. Only sampled cards that ASSERT a dimension (carry its
     * tag) can prove it unprojected: a corpus whose cards never carry the
     * mapped tag projects nothing, and treating that as drift would re-walk
     * the service on every startup. Checking observers rather than mere
     * existence is what also catches a RE-MAPPED service: its subjects have
     * docs, but the cells belong to the previous observer.
     *
     * Sampling, not counting, because the alternative is reading every card. A
     * service whose sample happens to land on retracted subjects is re-derived
     * needlessly, which costs one walk and is idempotent. The opposite error —
     * calling an unprojected service clean — would need a sampled subject to be
     * projected while the rest are not. The never-triggered failure this hunts
     * is all-or-nothing per (service, observer), so the sample settles it;
     * PARTIAL drift (a batch that failed mid-corpus) is not left to sampling at
     * all — the dirty marker names it and [DirtLedger.drain] repairs it exactly.
     *
     * ## Progress
     *
     * Sampling is FANNED OUT (bounded), so [onProgress] reports once before it
     * and once after; the expensive phase — the per-service rebuild walks, one
     * of which can cost a full six-figure visit — stays serial and reports
     * `(total, total, rebuilt, derivedInService)` as its subjects are
     * re-derived, so a caller shows movement through exactly the slow part.
     * `total` is known up front, so a caller can show a real fraction rather
     * than a spinner.
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

        // Phase 1 — sampling, fanned out: pure reads with no ordering
        // dependency, so hundreds of mapped services cost a few round-trip
        // waves instead of ~4 serial round trips each, on every startup,
        // in front of ranked search working. null = nothing stored for the
        // service; true/false = sampled projected/unprojected.
        val cutoff = nowSecs()
        val verdicts =
            providers.services.toList().mapBounded(QUERY_FANOUT) { service ->
                val sample =
                    index.search(
                        EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service), limit = samplesPerService, notExpiredAt = cutoff),
                    )
                if (sample.isEmpty()) return@mapBounded null
                // Per dimension: only sampled cards that CARRY a tag can prove
                // that tag's tensor unprojected — a mapped corpus whose cards
                // never assert the dimension derives nothing there, and calling
                // that drift would re-walk the service on every startup.
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
     * 10040 names, for either dimension.
     *
     * ## What makes them dead weight
     *
     * A card only becomes a tensor cell through the attribution map — the
     * signer must appear in some observer's 10040 under `30382:rank` or
     * `30382:followers` ([ProviderMap]). Cards from a service nobody named
     * project nothing, rank nothing and gate nothing; they are storage and
     * ingest cost with no read path. A mirroring relay accumulates them as a
     * matter of course: it syncs 30382s by kind, so every service publishing on
     * the network lands its whole corpus (hundreds of thousands of cards per
     * service) whether or not a single user here trusts it.
     *
     * Unlike everything else in this class this is a DELETION, not a repair,
     * and it is deliberately not automatic — [reconcile] never calls it.
     *
     * ## Why an empty attribution map REFUSES instead of deleting everything
     *
     * "No 10040 names it" and "no 10040 was readable" produce the identical
     * candidate list, and the second is a real, observed state: a relay that
     * mirrored its scores before its provider lists, or one asked one second
     * too early, reads zero 10040s while holding millions of cards (see
     * [ProviderMap.get], where the same ambiguity dictates the no-empty-cache
     * rule). Sweeping there would delete the ENTIRE score corpus — irreversibly,
     * on exactly the store the lists are still arriving at. So an empty map
     * sweeps nothing and reports [OrphanSweep.refused]; an operator who really
     * means it must first have at least one usable 10040 stored.
     *
     * ## Safety of the deletion itself
     *
     * The candidate list is a snapshot, so membership is RE-CHECKED under the
     * writer lock ([gate]) before every page: a 10040 naming the service may
     * have landed since, and its cards would then be carrying live cells. Such
     * a service is dropped mid-sweep and reported as [OrphanSweep.remapped],
     * keeping whatever pages already went — deleting a card of a service that IS
     * mapped is the one outcome this must never produce. Pages that landed
     * BEFORE that 10040 need no repair either: the list's own arrival queues its
     * service walk ([TrustProjection]), which derives from what is stored when
     * it runs, i.e. after those deletions.
     *
     * No re-derivation is needed for the orphans themselves — that is why this
     * removes through the index it was given (the raw engine index in the
     * shipped wiring) rather than through the projection: by definition an
     * unmapped signer's cards contribute no cells, so deleting them cannot
     * change any parent doc. (Stale cells from a service that WAS mapped and no
     * longer is are drift, not a consequence of this sweep — [verify] finds and
     * repairs them.)
     *
     * Expiry follows the same rule the projection applies: an already-expired
     * 10040 attributes nothing (it is not served as a record), so a service
     * named only by expired lists counts as an orphan here too.
     *
     * [dryRun] answers "how much would this free" — the candidate list plus a
     * count per service, no writes at all.
     */
    suspend fun sweepOrphanScores(
        dryRun: Boolean = false,
        onProgress: ((sweptServices: Int, totalOrphans: Int, scoresSwept: Int) -> Unit)? = null,
    ): OrphanSweep {
        val providers = recompute.providerMap()
        // The refusal above: no attribution readable = every service looks
        // orphaned. Nothing is deleted and the caller is told why.
        if (providers.isEmpty()) return OrphanSweep(0, emptyList(), 0, emptyList(), dryRun, refused = true)

        // Server-side grouping over the whole card corpus (see
        // EventIndex.distinctAuthors): the distinct signers out of millions of
        // docs without reconstructing one of them. Completeness is load-bearing
        // in the harmless direction only — a signer the grouping missed keeps
        // its cards, it never causes a wrong deletion.
        val signers = index.distinctAuthors(EventQuery(kinds = listOf(ContactCardEvent.KIND)))
        val candidates = signers.filterNot(providers::maps)
        onProgress?.invoke(0, candidates.size, 0)
        if (candidates.isEmpty()) return OrphanSweep(signers.size, emptyList(), 0, emptyList(), dryRun, refused = false)

        if (dryRun) {
            // Counts only — engine-side, fanned out, and no lock taken anywhere.
            val counted = candidates.mapBounded(QUERY_FANOUT) { index.count(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(it))) }
            return OrphanSweep(signers.size, candidates, counted.sum(), emptyList(), dryRun = true, refused = false)
        }

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
                    // A sweep wants ANY page, so the explicit ranking opts out of
                    // the recency planner's count probes (as NostrSemanticsStore's
                    // sweep does) while compiling to identical YQL.
                    ranking = EventYql.RANK_UNRANKED,
                )
            var stillOrphan = true
            var rounds = 0
            var drained = false
            var lastPage: Set<String>? = null
            while (!drained && rounds++ < MAX_SWEEP_ROUNDS) {
                gate {
                    // Re-read INSIDE the lock, per page: cached when unchanged,
                    // and a 10040 committed since the snapshot must stop the
                    // deletion at the first page boundary, not at the end.
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
                    // An acked remove is visible to search (the EventIndex
                    // contract), so an identical page means the deletes are not
                    // landing. Fail LOUD: reporting freed storage that is still
                    // stored is worse than stopping.
                    check(ids != lastPage) { "orphan sweep is not shrinking: ${ids.size} scores by $service survived their own removal" }
                    index.removeDocs(docs)
                    scores += docs.size
                    lastPage = ids
                }
                onProgress?.invoke(swept.size, candidates.size, scores)
            }
            check(drained) { "orphan sweep did not drain $service after $MAX_SWEEP_ROUNDS rounds of $SWEEP_PAGE" }
            if (stillOrphan) swept += service else remapped += service
            onProgress?.invoke(swept.size, candidates.size, scores)
        }
        return OrphanSweep(signers.size, swept, scores, remapped, dryRun = false, refused = false)
    }

    /**
     * Re-derive every parent doc from scratch (bootstrap over an existing
     * index), THEN sweep the parents the card walk cannot reach: a doc whose
     * subject has no stored cards left (its last card's removal crashed before
     * the recompute) is enumerated from the REPUTATION corpus and re-derived to
     * empty, which removes it. Without that second pass an orphan survives even
     * this hammer — the card walk, by construction, only visits subjects that
     * still have cards. Bounded only by the corpus — [reconcile] does the same
     * repair per affected service and normally finds nothing to do.
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
        // Cards sampled per service by [reconcile]. The question it answers is
        // "did this service's scores get projected at all", and the
        // never-triggered failure is all-or-nothing per (service, observer), so
        // a handful settles it; the cost is one small query plus a few key
        // lookups per mapped service.
        const val DEFAULT_RECONCILE_SAMPLES = 3

        // Subjects per orphan-sweep re-derive round (memory-bounded, like
        // TrustRecompute's walk batches).
        const val ORPHAN_BATCH = 20_000

        // Subjects compared per gated [verify] batch (memory- and lock-hold-bounded).
        const val VERIFY_BATCH = 20_000

        // Drift examples carried in the [TrustAudit] report; the COUNT is always complete.
        const val VERIFY_DRIFT_SAMPLES = 100

        // Cards deleted per [sweepOrphanScores] round — one page read, one
        // pipelined bulk delete, one lock hold. Same size the store's own sweep
        // pages at; a 30382 is a small doc.
        const val SWEEP_PAGE = 10_000

        // Runaway backstop per service, not a delete cap (100M cards at the page
        // above). Only reachable when a service keeps publishing INTO the sweep;
        // deletes that stop landing are caught after one repeated page.
        const val MAX_SWEEP_ROUNDS = 10_000

        /**
         * Expected-vs-stored equality for [verify]. Influence cells are int8 —
         * exact. Follower cells are stored as float32, so they are compared at
         * that precision; a double-precision compare would call the engine's
         * own rounding "drift". A subject whose derivation is EMPTY matches
         * exactly a missing doc.
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
