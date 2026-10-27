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
package com.nosfabrica.vespa.eventstore.engine.query
import com.nosfabrica.vespa.eventstore.engine.text.IndexableChars
import com.nosfabrica.vespa.eventstore.engine.text.WHITESPACE
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.tags.isIndexableTagName
import com.vitorpamplona.quartz.utils.Hex

/**
 * Builds YQL over the `event` schema from an [EventQuery]. Returns null when
 * the query provably matches nothing (no valid 64-hex id/author, a
 * non-single-letter tag name, limit 0), so the caller answers empty (EOSE)
 * without asking Vespa.
 *
 * Injection safety: ids and authors reach the YQL only after 64-hex
 * validation; every other caller-supplied string is escaped ([quote]) or
 * passed out-of-band as a query parameter; trigram literals are filtered to
 * alphanumeric characters only.
 */
object EventYql {
    /**
     * Kinds whose documents NEVER carry `search_text`, so `search_text_gram`
     * ([FuzzyWordGroup.PHRASE_GRAM_FIELDS], derived from it) is empty on every one of them and a body phrase
     * restricted to them is provably no match.
     *
     * Provably no match is not free. MEASURED on production (#161, 445M docs,
     * 2026-10-08): `kind = 0 and search_text_gram contains phrase(…7 grams…)`
     * took 1.77 s to match 0 documents. Vespa drives an AND from its smallest
     * estimate, and the phrase's (its rarest trigram, ~10^6) undercuts kind 0's
     * 60M, so every position check runs BEFORE the kind is consulted; the kind
     * filter can only lead for kinds holding fewer documents than that. A
     * `kinds:[0]` search paid 1.4-1.9 s for this clause alone, against ~40 ms
     * for every other clause of the word group together.
     *
     * Kind 0 alone, deliberately, and pinned rather than mirrored: which kinds
     * fill which column is Quartz's call (`SearchFieldExtractor` dispatches on
     * the event CLASS, so there is no per-kind table to derive from), and
     * benchmark/README.md rejects copying that table for the profile columns.
     * This is narrower on both counts. A kind 0 extracts to the PROFILE shape,
     * which has no body slot at all — `SearchExtractorsTest` asserts every kind
     * here still comes out with `text == null`, through the factory, so the day
     * Quartz gives one a body the build fails. And a stale entry would cost only
     * partial-word body reach: the exact `search_text` clause still runs.
     */
    val BODYLESS_KINDS: Set<Int> = setOf(MetadataEvent.KIND)

    /** Vespa's built-in no-scoring profile — filters without a search term. */
    const val RANK_UNRANKED = "unranked"

    /** The DEFAULT search profile in event.sd: text relevance combined with concave trust. */
    const val RANK_SEARCH = "search"

    /** Pure text relevance, no trust (`sort:text`). */
    const val RANK_TEXT = "text"

    /**
     * WHERE A SPLICED MEMBER IS SCORED — the profile that places a Trusted
     * List's member on the affiliation rung rather than deriving its score from
     * the list's.
     *
     * ONE profile, for [RANK_SEARCH] only, and the reason is the trust gate
     * rather than the ranking: a Trusted List and a NIP-85 assertion unpack
     * only for a reader whose own kind-10040 named their signer, so a read with
     * no `observer:` expands no declaration at all — and an `observer:` is
     * exactly what sends a query to [RANK_SEARCH]. A scored member therefore
     * cannot reach the [RANK_TEXT] ladder. The only thing an observerless read
     * expands is a NIP-32 label, which is ungated and carries no confidence, so
     * its subject wants no rung: it keeps its pointer's own score.
     */
    const val RANK_SPLICED_MEMBER = "spliced_member"

    /**
     * The rank feature carrying a member's 0..1 confidence to those profiles —
     * ONE number for the whole lookup, which is all a recall keyed by
     * (kind, author, d) can carry. The keyed shapes send [F_DOC_CONF] instead
     * and let each member's own weight ride in with its key.
     */
    const val F_MEMBER_CONF = "member_conf"

    /** Read the confidence per DOCUMENT (`rawScore`) rather than from [F_MEMBER_CONF]. */
    const val F_DOC_CONF = "doc_conf"

    /**
     * The reader's own trust floor as a HARD GATE on a spliced subject — see
     * [EventQuery.memberFloor] for why it cannot ride `min_rank` here. Sent only
     * on [RANK_SPLICED_MEMBER], the one profile that declares it.
     */
    const val F_MEMBER_FLOOR = "member_floor"

    /** The finding pointer's own relevance — the floor a subject is placed at a share of. 0 = no floor. */
    const val F_POINTER_REL = "pointer_rel"

    /**
     * The finding pointer's TEXT band — its relevance with the signer's trust
     * and the recency multiplier left out (`event.sd`'s text_score).
     *
     * Sent beside [F_POINTER_REL] because they answer different questions: the
     * relevance says where the POINTER belongs on the page, this says how much
     * the pointer's own words earned, and only the second may be handed to a
     * member. A member's trust is its own — the list's score for it, else its
     * own rank — never the trust of the key that signed the list.
     */
    const val F_POINTER_TEXT = "pointer_text"

    /** How far below its pointer a doubted subject may fall, as a fraction of the pointer's relevance. */
    const val F_SUBJECT_FLOOR_SPAN = "w_subject_floor_span"

    /**
     * The member profile matching what [q] itself ranked on — trust-multiplied
     * when the finding query carried a usable observer, plain text otherwise.
     *
     * Null when [q] ranks on neither ladder (a recency read, a plain recall):
     * those pages have no relevance to be comparable WITH, and the splice falls
     * back to the pointer's own order.
     */
    fun memberProfileOf(q: EventQuery): String? = if (profileOf(q) == RANK_SEARCH) RANK_SPLICED_MEMBER else null

    /**
     * NIP-01 recency order with the trust floor: score IS created_at,
     * below-floor authors dropped — the always-on spam gate for feeds, the
     * no-terms `filter:rank:` match-all, and the store's `sort:recent` search
     * (a search's own match set, ordered chronologically).
     *
     * MATCH-PHASE variant, keeping only the
     * newest ~[MATCH_PHASE_MAX_HITS] candidates per node before gating; [build]
     * demotes shapes the cut can't serve exactly to [RANK_RECENCY_GATED_EXACT],
     * and a degraded-and-unproven page reruns exact (VespaEventIndex.recallRoot).
     * The count-probe planner excludes both variants — its windows are proven
     * against the UNGATED match set, which the gate breaks.
     */
    const val RANK_RECENCY_GATED = "recency_gated"

    /** The full-scan variant of [RANK_RECENCY_GATED]: exact for every shape, but ranks every match — the fallback and the unlimited/deep shape, not the hot path. */
    const val RANK_RECENCY_GATED_EXACT = "recency_gated_exact"

    /** [buildLinkPeople]' lensed profile: the observer gate, scoring each link by its author's trust. */
    const val RANK_LINK_TRUST = "link_trust"

    /** `reputation.sd`'s profile scoring a pubkey by its rank cell under the lens ([buildTrustCells]). */
    const val RANK_TRUST_CELL = "trust_cell"

    /** Trust-sorted within each match tier, descending (`sort:rank`). */
    const val RANK_DESC = "rank_desc"

    /** Ascending trust within each (still-descending) match tier (`sort:rank:asc`). */
    const val RANK_ASC = "rank_asc"

    /** Verified-follower-count order within match tiers (`sort:followers`). */
    const val RANK_FOLLOWERS = "sort_followers"

    /**
     * Match-phase profile for LIMIT'D unranked recall: keeps only the
     * ~[MATCH_PHASE_MAX_HITS] newest candidates during matching. Selected only
     * when the limit sits [MATCH_PHASE_HEADROOM]x or more under max-hits, so
     * the true top-`limit` always survives the cut; the client accepts a
     * match-phase-degraded response only for this profile and [RANK_RECENCY_GATED].
     */
    const val RANK_RECENCY = "recency"

    /**
     * `max-hits` in event.sd's `recency` / `recency_gated` match-phase — the
     * per-CONTENT-NODE candidate depth, and the one number here an operator may
     * need to move: it is what decides [MATCH_PHASE_BAND], and a deployment
     * whose engine cuts harder than this build assumes can raise both.
     *
     * `VESPA_MATCH_PHASE_MAX_HITS` overrides it, and MUST be set to whatever the
     * SERVING schema says — the two are mirrors, not independent knobs, and a
     * value above the schema's would claim a depth the engine does not have.
     */
    val MATCH_PHASE_MAX_HITS: Int = System.getenv("VESPA_MATCH_PHASE_MAX_HITS")?.toIntOrNull()?.coerceAtLeast(1) ?: 20_000

    /**
     * A limit may use [RANK_RECENCY] only with this safety factor under
     * [MATCH_PHASE_MAX_HITS]. Vespa treats max-hits as a TARGET, not a
     * guarantee — a node may cut somewhat short of it — so the band leaves an
     * order of magnitude rather than riding the nominal depth.
     */
    const val MATCH_PHASE_HEADROOM = 10

    /**
     * The largest `limit` the match-phase profiles may serve. Past it a recall
     * is PAGED at this width (VespaEventIndex.pagedRecency) rather than asked
     * for in one oversized query — the caller's limit is never capped by it.
     *
     * It is stated once, here, because it is the number the client's overfetch
     * must respect: letting [VespaEventIndex]'s TIE_SLACK push a query over the
     * band silently moved the real ceiling to `band - TIE_SLACK` AND moved the
     * query onto a profile whose degradation the client refuses instead of
     * reconciling. MEASURED against a production cluster (2026-08-18): limit
     * 1936 served, limit 1937 refused outright.
     */
    val MATCH_PHASE_BAND: Int get() = MATCH_PHASE_MAX_HITS / MATCH_PHASE_HEADROOM

    /**
     * Vespa's per-query match-thread override. The cluster config
     * (`numthreadspersearch` in services.xml) is the CEILING; this parameter
     * can only select a value at or below it, never above — which is what makes
     * it safe to send from a client that cannot see the serving config.
     *
     * WHY THE PARALLELISM IS OPT-OUT RATHER THAN OPT-IN. A relevance search for
     * a common word matches millions of postings and pays for every one of them
     * TWICE — once to match it, once to score it — so its latency is a straight
     * function of the match set and nothing about the request can shrink it
     * (measured on the production relay 2026-09-01: "nostr", kind 1, the limit
     * makes NO difference — 19.2s at limit 50, 19.4s at limit 1, 23.0s at limit
     * 500). Splitting that work across match threads is the one lever that cuts
     * it without cutting recall. The cheap shapes — plain recall, the
     * match-phase profiles, counts, the snapshot walk — are the opposite case:
     * they are already fast, they are what a relay serves thousands of per
     * second, and services.xml's own reasoning (one thread per query keeps the
     * cores saturated with INDEPENDENT queries) is about exactly them. So they
     * ask for one thread and the ranked search takes the ceiling.
     *
     * The match-phase profiles have a second, harder reason: their `max-hits`
     * cut is a PER-NODE depth this client mirrors ([MATCH_PHASE_MAX_HITS]) to
     * decide which limits it may serve. Whether Vespa applies that cut per node
     * or per thread is not something a query may be uncertain about, so these
     * profiles stay on the one thread the mirrored number was measured against.
     */
    const val MATCH_THREADS = "ranking.matching.numThreadsPerSearch"

    /** What [MATCH_THREADS] carries on every shape that is not a relevance search. */
    const val SINGLE_MATCH_THREAD = "1"

    /**
     * Vespa's query-time SORTING DEGRADER. When a query's primary sort key is a
     * single-value numeric fast-search attribute — `created_at` here — Vespa
     * activates match-phase degradation BY DEFAULT, on EVERY rank profile
     * (docs.vespa.ai/en/reference/sorting.html; query parameter
     * `sorting.degrading`, default true). `unranked` is not exempt, which
     * [buildIdTime] assumed: a profile with no schema match phase still gets
     * one from the sort.
     *
     * MEASURED on staging 2026-09-12, the walk's exact shape (kind 1, unranked,
     * summary idtime, hits 20064, one match thread), only the window varied:
     *
     *     1d / 7d                           coverage 100%            0.07 to 0.21 s
     *     30d                               coverage   3%, cut       0.38 s  (refused)
     *     30d,  sorting.degrading=false     coverage 100%, 12.8M     0.93 to 1.0 s
     *     365d, sorting.degrading=false     coverage 100%, 104.9M    6.6 s
     *
     * The same flag on `recency` / `recency_gated`: identical degraded output,
     * coverage and latency — their schema `match-phase max-hits` governs, so
     * the parameter changes nothing there and is not sent there.
     *
     * What the refusal cost before this, per hour on the sync pod: 121 to 150
     * `walk.refused.match-phase`, 131 `walk.cut.narrowed`, 15 to 19
     * `walk.scan` at a mean of 17.9 minutes, 5 to 7 running at any moment,
     * 140 to 245 MB/s of document-store reads and 5 to 8 proton cores per
     * content node.
     *
     * SENT BY THE ID WALK ONLY. [build]'s unranked recall carries the same
     * sort and is refused the same way (VespaEventIndex.searchRoot), and stays
     * on the degrader by decision, not oversight: a deep-past client REQ over a
     * huge match set fails fast today (all-time kinds 1/6/7 at limit 100:
     * 0.03 s refused) and would instead run to completion (4.05 s, measured)
     * with the flag. Widening the scope is one line at the `order` clause
     * there; it wants its own measurement of what clients then pay.
     */
    const val SORT_DEGRADING = "sorting.degrading"

    /** What [SORT_DEGRADING] carries on the id walk. */
    const val SORT_DEGRADING_OFF = "false"

    /**
     * The profiles that ask for [SINGLE_MATCH_THREAD]: unranked recall and both
     * match-phase profiles. Everything else here ranks text over a match set the
     * caller cannot bound, which is the shape parallelism exists for.
     *
     * `recency_gated_exact` is deliberately NOT here. It carries no match phase
     * — it gates and scores EVERY match — and it is the shape a `sort:recent`
     * search past the band falls to, measured at 14.5s against the production
     * relay where the match-phase variant of the same query took 1.4s.
     */
    private val SINGLE_THREADED_PROFILES = setOf(RANK_UNRANKED, RANK_RECENCY, RANK_RECENCY_GATED)

    /**
     * Whether [q] asks for one match thread — the profile says so, or the query
     * has already bounded its own match set by NAMING the documents it wants.
     *
     * The keyed shapes are the reference expansion's lookups: a label's subject,
     * a Trusted List's members, fetched by id or by (author, kind, d) with the
     * finding query's TERMS STRIPPED (SearchReferenceExpansion). They land on a
     * relevance profile — `spliced_member` places a member on its own rung — so
     * the profile alone would hand them the ceiling, and there is nothing there
     * to parallelise: a handful of keys is a handful of postings, and the
     * threads would cost more to start than the query costs to run. Several of
     * them run per searching REQ, so that is not a rounding error.
     */
    private fun singleMatchThread(
        q: EventQuery,
        ranking: String,
    ): Boolean = ranking in SINGLE_THREADED_PROFILES || q.ids.isNotEmpty() || q.idWeights.isNotEmpty() || q.authorWeights.isNotEmpty()

    /**
     * The summary fields needed to reconstruct an event
     * ([com.nosfabrica.vespa.eventstore.engine.doc.EventDoc.fromSummary]).
     * Selecting these instead of `*` omits the BM25 index fields — ~35% fewer
     * bytes on a plain 200-hit note scan, far more on long-form. The omitted
     * fields are index/ranking inputs, never part of the served event.
     */
    const val SUMMARY_FIELDS = "id, pubkey, created_at, kind, tags, content, sig, owner"

    /**
     * The attribute-only document-summary in event.sd serving the existence
     * check ([buildExistence]) — keep in sync with the schema.
     */
    const val SUMMARY_DEDUP = "dedup"

    /**
     * [vq] projected to (id, created_at) off the attribute-only [SUMMARY_IDTIME]
     * summary: the same match, the same order, the same hits — but served from
     * memory, never the document store. The id-first fetch resolves a page with
     * this and reads full documents only for the ids that made it
     * (VespaEventIndex.recallSummaries).
     */
    fun idTimeOnly(vq: VespaQuery): VespaQuery =
        vq.copy(
            yql = vq.yql.replaceFirst("select $SUMMARY_FIELDS from ", "select id, created_at from "),
            params = vq.params + ("presentation.summary" to SUMMARY_IDTIME),
        )

    // Attribute-only projections for snapshot walks — see buildIdTime.
    const val SUMMARY_IDTIME = "idtime"
    const val SUMMARY_IDTIME_TAG = "idtimetag"

    /**
     * The (id, created_at[, tag_index]) projection a snapshot walk pages on:
     * attributes only, newest first, always UNRANKED, and with the engine's
     * sorting degrader turned OFF ([SORT_DEGRADING]).
     *
     * Unranked is load-bearing: the recency profile's match-phase caps
     * `totalCount` and can drop hits below its cut (see [buildCount]), which on
     * a walk that must be COMPLETE would lose events silently. Unranked
     * declares no match phase in the schema — but the `order by created_at
     * desc` alone activates Vespa's query-time degrader, and that is what cut
     * this walk to 3% coverage on staging. This comment used to say the
     * opposite ("unranked has no match phase, so the sort keeps full
     * coverage"), on the strength of a 6.0M-match filter over 42.8M docs that
     * answered whole. Both observations are real; what separates a 6.0M match
     * set that answered whole from a 12.8M one that was cut was not measured,
     * which is why the walk sends the flag rather than relying on a theory of
     * when the degrader engages.
     */
    fun buildIdTime(
        q: EventQuery,
        withDTag: Boolean = false,
    ): VespaQuery? {
        val params = LinkedHashMap<String, String>()
        val clauses = filterClauses(q, params) ?: return null
        val limit = q.limit?.let { if (it <= 0) return null else " limit $it" } ?: ""
        params["presentation.summary"] = if (withDTag) SUMMARY_IDTIME_TAG else SUMMARY_IDTIME
        params[MATCH_THREADS] = SINGLE_MATCH_THREAD
        params[SORT_DEGRADING] = SORT_DEGRADING_OFF
        // A GATED WALK GATES. The walk is how a query's whole id set leaves the
        // engine (NIP-77 snapshots above all), and on `unranked` a lensed read
        // walked every author the lens drops: a reconcile declared through an
        // observer handed over exactly the corpus the observer gate exists to
        // withhold. `recency_gated_exact` drops below-floor authors engine-side
        // — the same `user_score >= min_rank` every trust profile gates on — and
        // scores by created_at alone, so the `order by` the cursor pages on is
        // unchanged; it has no match-phase, so nothing cuts the walk. A walk is
        // termless (walksGated), so no text feature was emitted to strip.
        val gated = walksGated(q)
        if (gated) lensParams(q, params)
        return VespaQuery(
            yql = "select ${if (withDTag) "id, created_at, tag_index" else "id, created_at"} from event where ${whereOf(clauses)} order by created_at desc$limit",
            params = params,
            ranking = if (gated) RANK_RECENCY_GATED_EXACT else RANK_UNRANKED,
            complete = q.complete,
            sampled = q.sampled,
            shape = clauseShape(q),
        )
    }

    /**
     * Existence-only recall for the bulk-dedup preload: `select id` under
     * [SUMMARY_DEDUP], answered from the id ATTRIBUTE in memory — the disk
     * summary store never runs. No `order by` (membership is unordered), no
     * `limit` (an existence answer must be complete: a short page would be a
     * wrong write upstream). Null when no valid 64-hex id remains — the
     * constraint is unsatisfiable, so nothing exists (same contract as [build]).
     */
    fun buildExistence(ids: List<String>): VespaQuery? {
        val clause = hexIn("id", ids) ?: return null
        return VespaQuery(
            yql = "select id from event where $clause",
            params = mapOf("presentation.summary" to SUMMARY_DEDUP, MATCH_THREADS to SINGLE_MATCH_THREAD),
            ranking = RANK_UNRANKED,
            shape = "ids",
        )
    }

    /**
     * True when [build] would auto-select [RANK_RECENCY] for [q]. The index
     * keys two behaviors on this: skipping the count-probe planner and
     * demoting to [RANK_UNRANKED] against a serving schema that predates the
     * profile. A deep-past `until` is EXCLUDED: the newest-first match-phase
     * cut lands above the wanted window and forces the full-scan rerun; those
     * shapes fall to the count-probe planner, whose windows anchor at `until`.
     */
    fun usesRecencyProfile(q: EventQuery): Boolean =
        q.ranking == null &&
            q.search.isNullOrBlank() &&
            q.phrases.isEmpty() &&
            (q.limit ?: 0) in 1..MATCH_PHASE_BAND &&
            (q.until == null || q.until >= nowOf(q) - RECENT_UNTIL_HORIZON)

    /**
     * The request's clock: [EventQuery.nowSecs] when the caller stamped one
     * (the store stamps one per REQ), else the wall clock.
     *
     * One clock per query, for the same reason the store stamps one per
     * request: profile SELECTION here and the recency RANKING in the schema
     * both ask "how old is this", and a query whose two answers disagree is a
     * query whose plan does not match its score. It also makes selection a
     * function of the request, so a test can pin a deep-past `until` without
     * pinning the machine's clock.
     */
    private fun nowOf(q: EventQuery): Long = q.nowSecs ?: (System.currentTimeMillis() / 1000)

    /** How far back an `until` may sit and still ride [RANK_RECENCY] — beyond it, pagination anchors take the planner path. */
    const val RECENT_UNTIL_HORIZON = 2_592_000L

    /**
     * True when [build] keeps a [RANK_RECENCY_GATED] query on the match-phase
     * variant instead of demoting it to [RANK_RECENCY_GATED_EXACT]. Same shape
     * gate as [usesRecencyProfile], but the headroom must also absorb the
     * gate's drops: a qualifying shape can still come back degraded-and-short
     * when too few of the newest candidates are trusted — that case reruns
     * exact (VespaEventIndex.recallRoot), paid only on heavily-spammed corpora
     * or near-empty trust graphs.
     *
     * SEARCH TERMS are admitted here (unlike [usesRecencyProfile], where a term
     * means a relevance profile owns the order): a `sort:recent` search ranks
     * by created_at like any other gated query, so the cut keeps exactly the
     * newest candidates it wants. In practice a term-bearing match set rarely
     * reaches max-hits, so the phase never engages.
     */
    fun usesGatedMatchPhase(q: EventQuery): Boolean =
        q.ranking == RANK_RECENCY_GATED &&
            (q.limit ?: 0) in 1..MATCH_PHASE_BAND &&
            (q.until == null || q.until >= nowOf(q) - RECENT_UNTIL_HORIZON)

    /**
     * The rank profile [build] will run [q] on. Stated separately because
     * `EventQuery.ranking` is NOT that answer: it is null for every ordinary
     * search, and the profile is then chosen from the query's SHAPE — above all
     * from whether an observer resolved, which picks [RANK_SEARCH] over
     * [RANK_TEXT]. A caller comparing the field instead of this reads two
     * different profiles as one, and the only thing that turns on that question
     * is whether two queries' scores share a scale
     * (NostrSemanticsStore.recallOrdered) — where being wrong interleaves them.
     */
    fun profileOf(q: EventQuery): String {
        // Trust ranking needs an observer: without one, an unguarded min_rank
        // would gate every hit against a zero score and return nothing — so a
        // search with no observer defaults to pure text and emits neither
        // feature. An explicit sort:/filter: keeps its profile but loses trust.
        val requested =
            q.ranking ?: when {
                usesRecencyProfile(q) -> RANK_RECENCY

                // Phrases are search text: a phrase-only query ranks like any
                // search. Only notSearch-free-and-text-free recall is plain.
                q.search.isNullOrBlank() && q.phrases.isEmpty() -> RANK_UNRANKED

                q.observer?.lowercase()?.takeIf(Hex::isHex64) != null -> RANK_SEARCH

                else -> RANK_TEXT
            }
        // The match-phase cut is only sound for shapes [usesGatedMatchPhase]
        // admits — others would silently lose every hit older than the newest
        // ~max-hits candidates, so they demote to the full-scan variant.
        return if (requested == RANK_RECENCY_GATED && !q.keepMatchPhase && !usesGatedMatchPhase(q)) RANK_RECENCY_GATED_EXACT else requested
    }

    /**
     * THE FLOOR [q]'s LENS ENFORCES, or null when its reads gate nobody — the
     * one answer to "who is gated" that the stored page, the id walk, a NIP-77
     * snapshot and a relay's live gate all read, so they cannot disagree.
     *
     * Gated: a hex observer to read the lens through, a floor above 0, and a
     * profile whose first phase drops `user_score < min_rank` — the recency-gated
     * pair, the three trust sorts, and the default `search` profile (whose
     * `wot_mult()` is 0 below the floor). Not gated: no observer ([build] sends
     * no lens), `sort:text` (the `text` profile reads none), plain recall, and a
     * floor of 0 or less — every rank cell is 0..100, so `include:spam`'s 0 admits
     * every author and a gate there could only cost reads.
     */
    fun gateFloor(q: EventQuery): Double? {
        if (q.observer?.lowercase()?.takeIf(Hex::isHex64) == null) return null
        val floor = q.minRank?.takeIf { it > 0.0 } ?: return null
        return when (profileOf(q)) {
            RANK_RECENCY_GATED, RANK_RECENCY_GATED_EXACT, RANK_DESC, RANK_ASC, RANK_FOLLOWERS, RANK_SEARCH -> floor
            else -> null
        }
    }

    /**
     * Whether [buildIdTime] walks [q] through the observer gate: a query that
     * gates ([gateFloor]) with no search text — the walk's where clause has no
     * text ranking to offer, and a searching read is a relevance question the
     * walk does not answer. A trust SORT walks gated too: the walk's order is
     * created_at whatever the profile, but its SET is the sort's, whose gate is
     * the same floor — so an unlimited walk of it is exactly its gated set.
     */
    fun walksGated(q: EventQuery): Boolean = q.search.isNullOrBlank() && q.phrases.isEmpty() && gateFloor(q) != null

    /**
     * Whether a LIMIT'D walk of [q] is the page its read serves: plain recall,
     * or the recency-gated pair, whose score IS created_at — the newest N either
     * way. A trust sort's first N are its most trusted, not its newest, so a
     * limit'd one is a search, never a walk.
     */
    fun walksInOrder(q: EventQuery): Boolean = q.search.isNullOrBlank() && q.phrases.isEmpty() && (q.ranking == null || q.ranking == RANK_UNRANKED || q.ranking == RANK_RECENCY || q.ranking == RANK_RECENCY_GATED || q.ranking == RANK_RECENCY_GATED_EXACT)

    /**
     * The lens a trust profile reads, as query features. The tensors are keyed
     * by SERVICE: the lens is the observer's resolved provider per dimension,
     * and an unresolved one is the empty tensor — "trusts nobody", score 0 —
     * never the observer's own key, which no card is keyed under.
     */
    private fun lensParams(
        q: EventQuery,
        params: MutableMap<String, String>,
    ) {
        params["ranking.features.query(user_q)"] = q.rankKey?.takeIf(Hex::isHex64)?.let { "{$it:1.0}" } ?: "{}"
        params["ranking.features.query(followers_q)"] = q.followersKey?.takeIf(Hex::isHex64)?.let { "{$it:1.0}" } ?: "{}"
        q.minRank?.let { params["ranking.features.query(min_rank)"] = it.toString() }
    }

    /**
     * THE PROFILE A COUNT MUST RUN ON, or null when the query needs no profile
     * at all and the unranked grouping counts it exactly ([buildCount]).
     *
     * A count is "how many documents would this query serve", and for a ranked
     * query that is NOT the size of the match set: the trust profiles map a
     * below-floor author to the sentinel and `rank-score-drop-limit` deletes the
     * hit, so the served count is strictly smaller. Vespa reports the served
     * number as the response's own `totalCount` ([SearchRootFields]) — once
     * the ranking pass has run, which a `hits=0` query may skip (it does on
     * Vespa 8.731, see VespaEventIndex.count) — which is why a count on those
     * profiles is one ONE-hit query rather than a page the caller has to
     * materialize and measure.
     *
     * The two MATCH-PHASE profiles are the exception, and they split:
     *
     *  - [RANK_RECENCY] gates nothing — its score IS `created_at` and it drops
     *    no hit — so the served count equals the match count and the unranked
     *    grouping already answers it exactly, for less. It maps to null rather
     *    than to itself because its match phase would CAP `totalCount`
     *    (10x+ undercount; the same trap [buildCount]'s missing `order by`
     *    avoids).
     *  - [RANK_RECENCY_GATED] does gate, so it cannot fall back to the grouping
     *    — but its own `totalCount` is capped by the same match phase. It counts
     *    on the full-scan twin ([RANK_RECENCY_GATED_EXACT]), which applies the
     *    identical gate and carries no cut.
     */
    fun countProfileOf(q: EventQuery): String? =
        when (val profile = profileOf(q)) {
            RANK_UNRANKED, RANK_RECENCY -> null
            RANK_RECENCY_GATED -> RANK_RECENCY_GATED_EXACT
            else -> profile
        }

    fun build(q: EventQuery): VespaQuery? {
        val params = LinkedHashMap<String, String>()
        val ranking = profileOf(q)
        val clauses = filterClauses(q, params, bodyPhrase = ranking == RANK_SEARCH) ?: return null

        val observer = q.observer?.lowercase()?.takeIf(Hex::isHex64)
        if (ranking != RANK_UNRANKED && ranking != RANK_RECENCY && observer != null) lensParams(q, params)
        // A MEMBER PROFILE WITHOUT AN OBSERVER WOULD SATURATE, not degrade.
        // wot_mult() reads `min_rank`, which defaults to -1e9 so the gate in
        // the rank_* profiles is a no-op; with no user_q the member's
        // user_score() is 0, and `0 - (-1e9)` clamps to 100 — the TOP of the
        // trust curve, for every member equally. Pinning the floor to 0 makes
        // an unlensed member score its rung times 1.0, which is what "no trust
        // signal" should mean.
        if (ranking == RANK_SPLICED_MEMBER && observer == null) {
            params["ranking.features.query(min_rank)"] = "0.0"
        }
        // The query instant for recency ranking (docs/recency-ranking.md).
        // Stamped by the CLIENT rather than read from Vespa's own `now` so a
        // score is a function of the request and not of the second it arrived
        // in: positions stay pinnable as the corpus ages, every content node
        // scores one query against one instant, and a reported ranking can be
        // replayed later. Sent on everything that ranks text; the two profiles
        // excluded here rank by created_at ALONE, where an instant means
        // nothing (and the gated pair strips it below with the text features).
        if (ranking != RANK_UNRANKED && ranking != RANK_RECENCY) {
            params[F_NOW_SECS] = nowOf(q).toString()
        }
        // The gated profiles are STANDALONE (see event.sd): they declare the two
        // trust inputs, score by created_at, and read no text signal at all. The
        // term-weighting features [filterClauses] emits alongside a search term
        // therefore have nothing to weigh here — `sort:recent` is the one shape
        // that brings terms to this profile — so they are dropped rather than
        // shipped to a profile that never declares them.
        if (ranking == RANK_RECENCY_GATED || ranking == RANK_RECENCY_GATED_EXACT) {
            params.keys.removeAll(TEXT_RANK_FEATURES)
        }
        // The store's per-shape recency weight (EventQuery.recencyWeight) — on the
        // one profile whose recency_mult() reads it, so no other profile is sent
        // an input it does not declare.
        if (ranking == RANK_SEARCH) q.recencyWeight?.let { params[F_W_RECENCY] = it.toString() }
        // Two-phase profiles only; the engine ignores it elsewhere.
        q.rerankCount?.let { params["ranking.rerankCount"] = it.toString() }
        // Harness overrides last, so a sweep can also move a feature the
        // builder just set (min_rank, now_secs) — that is what a sweep is for.
        // The NAME is validated here, not trusted: every other caller-supplied
        // string in this builder is escaped or travels out-of-band, and a
        // parameter name must not be the one hole in that.
        q.rankFeatures.forEach { (name, value) ->
            require(RANK_FEATURE_NAME.matches(name)) { "illegal rank feature name: $name" }
            params["ranking.features.query($name)"] = value.toString()
        }

        // Only the member profile declares it, and only a reader who asked for a
        // floor sends one (EventQuery.memberFloor).
        if (ranking == RANK_SPLICED_MEMBER) {
            q.memberFloor?.let { params["ranking.features.query($F_MEMBER_FLOOR)"] = it.toString() }
        }

        if (singleMatchThread(q, ranking)) params[MATCH_THREADS] = SINGLE_MATCH_THREAD

        val where = whereOf(clauses)
        // Plain recall orders newest first; anything ranked keeps Vespa's
        // score order. created_at ONLY — an engine-side id tiebreak (compound
        // sort on the id STRING attribute) paid UCA collation over the whole
        // match set (measured 0.22s -> 1.3s on 2M matches). The client
        // restores the exact `created_at desc, id asc` contract from the
        // RETURNED page instead — see VespaEventIndex.recallSummaries.
        // This sort also invites Vespa's own match-phase cut on `unranked`,
        // which the client refuses; only the id walk opts out — see
        // [SORT_DEGRADING] for the measurement and why the scope stops there.
        val order = if (ranking == RANK_UNRANKED || ranking == RANK_RECENCY) " order by created_at desc" else ""
        val limit = q.limit?.let { if (it <= 0) return null else " limit $it" } ?: ""
        return VespaQuery(
            yql = "select $SUMMARY_FIELDS from event where $where$order$limit",
            params = params,
            ranking = ranking,
            complete = q.complete,
            sampled = q.sampled,
            shape = clauseShape(q),
        )
    }

    /**
     * An EXACT-count query: same filters, a grouping `count()`, NO `order by` —
     * attribute sorting trips Vespa's match-phase on a large corpus and caps
     * `totalCount` (10x+ undercount; the mechanism is the sorting degrader,
     * [SORT_DEGRADING]). Grouping over the unranked match set is exact.
     */
    fun buildCount(q: EventQuery): VespaQuery? = grouping(q, "all(output(count()))")

    /**
     * DISTINCT authors of the match set, aggregated server-side, each leaf group
     * carrying its doc count. No `max()`: EVERY distinct author comes back;
     * [grouping] and the bundled query profile disable the engine's group
     * ceilings, since a truncated author set would make the orphan-score sweep
     * silently under-delete.
     */
    fun buildDistinctAuthors(q: EventQuery): VespaQuery? = grouping(q, "all(group(pubkey) each(output(count())))")

    /**
     * DISTINCT values of a single-letter tag across the match set, aggregated
     * server-side off `tag_index`, each leaf group carrying its doc count.
     *
     * GROUPS EVERY LETTER, and takes no tag name for that reason: `tag_index`
     * holds derived `"<letter>:<value>"` pairs and Vespa groups the attribute
     * whole, so the one-letter narrowing is the CALLER's, done on the prefixed
     * keys that come back. It used to take a `tagName` it never read, which
     * read as a server-side constraint that was never there — and a caller
     * that believed the signature and dropped its own prefix filter would have
     * served every letter's values. It is a LOSSY projection — single-letter names, first values only, and nothing of
     * the tag beyond the value — which is exactly why this is only sound for a
     * one-letter tag read at position 1 with no condition on the rest of the
     * tag. [EventIndex.distinctTagIndexValues] owns those preconditions;
     * anything else must take the tags projection instead and read whole tags.
     *
     * No `max()`, as with [buildDistinctAuthors]: a truncated url set would
     * silently narrow whatever asked for it.
     */
    fun buildDistinctTagValues(q: EventQuery): VespaQuery? = grouping(q, """all(group(tag_index) each(output(count())))""")

    /**
     * The PEOPLE half of [EventIndex.scoreLinks]: the documents [linkers] would
     * serve that carry any of [keys] in `tag_index`, grouped key -> author ->
     * kind, each kind group carrying its count and `sum(relevance())` — under the
     * lens, the author's summed trust. Zap receipts are left out
     * ([buildLinkZaps] reads them): their author is the recipient's LNURL server,
     * not a person who engaged.
     *
     * The key clause is an OR over every letter at once (`tag_index in ("e:…",
     * "a:…")`), which no [EventQuery] can say — its `tags` AND across names, as
     * NIP-01 does — so it is written here rather than smuggled through [linkers].
     *
     * The grouping carries a `filter(regex())` naming exactly [keys]. Without it
     * `group(tag_index)` builds a group for EVERY element of every matched
     * document's array — a reply's whole `p` list, its thread root, its client
     * tag — and that is where the time goes: measured on real search pages
     * (benchmark/README.md, "Trusted links per search hit"), 6,137 groups for
     * the 26 that mattered, 117 ms against 17 ms with the filter. (Inside a key's
     * group, `tag_index` is that key alone — Vespa binds a grouped multivalue
     * attribute to the group's value — which is why the zapper, another element
     * of the same array, cannot be grouped here and has a query of its own.)
     *
     * GATED AND TRUST-SCORED: a lensed [linkers] ([isLensed]) runs on
     * [RANK_LINK_TRUST] — the exact gate of [RANK_RECENCY_GATED_EXACT], whose
     * `rank-score-drop-limit` removes a below-floor author before the grouping
     * sees it, with the author's rank cell as the score. Anything else runs
     * unranked, where the sum is 0 and means nothing. That the drop happens
     * before grouping even at `hits=0` is the engine's behavior, not a promise of
     * this builder — `TrustedLinksIT` pins it.
     *
     * Null when nothing can match: no key a document could hold, or [linkers]
     * provably empty.
     */
    fun buildLinkPeople(
        keys: Collection<String>,
        linkers: EventQuery,
    ): VespaQuery? {
        val usable = keys.filter(::isLinkKey).distinct()
        if (usable.isEmpty()) return null
        val pattern = usable.joinToString("|", prefix = "^(?:", postfix = ")$") { regexLiteral(it) }
        val pipeline =
            "all(group(tag_index) filter(regex(${quote(pattern)}, tag_index)) " +
                "each(all(group(pubkey) each(all(group(kind) each(output(count(), $LINK_TRUST_SUM)))))))"
        val q = linkers.copy(limit = null)
        val clause = "${keyClause(usable)} and !(kind = $ZAP_RECEIPT_KIND)"
        return grouping(q, pipeline, extra = clause, ranking = if (isLensed(q)) RANK_LINK_TRUST else RANK_UNRANKED)
    }

    /**
     * The ZAP half of [EventIndex.scoreLinks]: the zap receipts [linkers] would
     * serve that carry any of [keys], as hits on the attribute-only `idtimetag`
     * summary — `tag_index` is where the receipt's `P` (the zapper) and the keys
     * it links to both live, and only a document read has both at once. Under
     * the lens the same [RANK_LINK_TRUST] gate applies to the receipt's SIGNER
     * (an LNURL server the observer does not trust is a receipt anyone could
     * have forged), and the hit's relevance is that signer's cell. No `order by`
     * and no limit: every receipt, one round trip.
     */
    fun buildLinkZaps(
        keys: Collection<String>,
        linkers: EventQuery,
    ): VespaQuery? {
        val usable = keys.filter(::isLinkKey).distinct()
        if (usable.isEmpty()) return null
        val q = linkers.copy(limit = null)
        if (q.kinds.isNotEmpty() && ZAP_RECEIPT_KIND !in q.kinds) return null
        val params = LinkedHashMap<String, String>()
        val clauses = filterClauses(q, params) ?: return null
        val ranking = if (isLensed(q)) RANK_LINK_TRUST else RANK_UNRANKED
        if (ranking != RANK_UNRANKED) {
            lensParams(q, params)
            params.keys.removeAll(TEXT_RANK_FEATURES)
        }
        params["presentation.summary"] = SUMMARY_IDTIME_TAG
        params[MATCH_THREADS] = SINGLE_MATCH_THREAD
        return VespaQuery(
            yql = "select id, tag_index from event where ${whereOf(clauses + keyClause(usable) + "kind = $ZAP_RECEIPT_KIND")}",
            params = params,
            ranking = ranking,
            shape = clauseShape(q),
        )
    }

    /**
     * [pubkeys]' rank cells under [rankKey], as the hits' relevance — the
     * `trust_cell` profile of `reputation.sd`, one query for a whole page's
     * zappers. An author with no reputation document is simply not a hit.
     */
    fun buildTrustCells(
        pubkeys: Collection<String>,
        rankKey: String,
    ): VespaQuery? {
        val hexes = pubkeys.map { it.lowercase() }.filter(Hex::isHex64).distinct()
        if (hexes.isEmpty() || !Hex.isHex64(rankKey)) return null
        return VespaQuery(
            yql = "select pubkey from reputation where pubkey in (${hexes.joinToString(", ") { "\"$it\"" }})",
            params = mapOf("ranking.features.query(user_q)" to "{${rankKey.lowercase()}:1.0}", MATCH_THREADS to SINGLE_MATCH_THREAD),
            ranking = RANK_TRUST_CELL,
        )
    }

    /** Whether a link query reads trust: a hex observer, through the observer gate's ranking. */
    fun isLensed(q: EventQuery): Boolean = q.observer?.lowercase()?.takeIf(Hex::isHex64) != null && (q.ranking == RANK_RECENCY_GATED || q.ranking == RANK_RECENCY_GATED_EXACT)

    private fun keyClause(keys: List<String>): String = keys.joinToString(", ", prefix = "tag_index in (", postfix = ")") { quote(it) }

    /** NIP-57's zap receipt — the link kind whose person is not its author ([buildLinkZaps]). */
    const val ZAP_RECEIPT_KIND = 9735

    /** The grouping output [buildLinkPeople] sums each kind's linker trust with — also its key in the response's `fields`. */
    const val LINK_TRUST_SUM = "sum(relevance())"

    /** A `"<letter>:<value>"` key `tag_index` could hold: an indexable one-letter name, a value, nothing [quote] cannot render. */
    private fun isLinkKey(key: String): Boolean = key.length > 2 && key[1] == ':' && isIndexableTagName(key.substring(0, 1)) && isQuotable(key)

    /**
     * [s] as a regex matching only itself. ASCII punctuation is escaped and
     * nothing else: a hex id needs no escape at all, an address only its
     * colons' neighbours (`kind:pubkey:d`, where `d` is caller text), and
     * escaping a letter or a non-ASCII character would turn it INTO a regex
     * escape sequence rather than out of one.
     */
    private fun regexLiteral(s: String): String =
        buildString(s.length) {
            for (c in s) {
                if (c.code < 128 && !c.isLetterOrDigit() && c != ' ') append('\\')
                append(c)
            }
        }

    /**
     * The shared shape of every aggregation query: the filter WHERE clause,
     * `limit 0`, the [pipeline] grouping, NO `order by` (attribute sorting
     * trips match-phase and caps totals), unranked. Null when the filter
     * provably matches nothing.
     *
     * The per-request group ceilings are disabled ([UNLIMITED_GROUPS]) so an
     * aggregation answers over the WHOLE match set — a `max()`-less pipeline
     * otherwise returns `grouping.defaultMaxGroups` groups (TEN). The third
     * ceiling, `grouping.globalMaxGroups`, must be disabled too but CANNOT be
     * sent per-request (Vespa 400s any request carrying it): it lives in the
     * bundled query profile (`engine/app/search/query-profiles/default.xml`),
     * and a deployment that replaces that profile must carry the field over.
     */
    private fun grouping(
        q: EventQuery,
        pipeline: String,
        /** One more WHERE clause the [EventQuery] cannot express ([buildLinkPeople]' cross-letter key OR). */
        extra: String? = null,
        /**
         * The profile the grouping runs under — [RANK_UNRANKED] for every
         * aggregation but a gated one ([buildLinkPeople]), whose drop-limit is
         * what removes below-floor documents before they are grouped.
         */
        ranking: String = RANK_UNRANKED,
    ): VespaQuery? {
        // A present limit <= 0 is the "matches nothing" sentinel (as in [build]);
        // a positive limit is about hits, not the grouping, so it is ignored.
        if (q.limit != null && q.limit <= 0) return null
        val params = LinkedHashMap<String, String>()
        val clauses = filterClauses(q, params) ?: return null
        val where = whereOf(if (extra == null) clauses else clauses + extra)
        if (ranking != RANK_UNRANKED && q.observer?.lowercase()?.takeIf(Hex::isHex64) != null) {
            lensParams(q, params)
            // The gated profiles read no text signal (see [build]).
            params.keys.removeAll(TEXT_RANK_FEATURES)
        }
        params["grouping.defaultMaxGroups"] = UNLIMITED_GROUPS
        params["grouping.defaultMaxHits"] = UNLIMITED_GROUPS
        params[MATCH_THREADS] = SINGLE_MATCH_THREAD
        return VespaQuery(
            yql = "select * from event where $where limit 0 | $pipeline",
            params = params,
            ranking = ranking,
            shape = clauseShape(q),
        )
    }

    /** The trigram-net weight for the query's words (text_relevance's `w_gram`). */
    private const val F_W_GRAM = "ranking.features.query(w_gram)"

    /** The typed words as one body phrase, for the rank-only item behind event.sd's body_phrase(). */
    private const val P_BODY_PHRASE = "bp"

    /** How many TOKENS that phrase is to Vespa — body_phrase()'s threshold; see [phraseTokens]. */
    private const val F_PHRASE_TERMS = "ranking.features.query(phrase_terms)"

    /** How many things the user asked for — words plus quoted phrases (text_relevance's `n_words`). */
    private const val F_N_WORDS = "ranking.features.query(n_words)"

    /** The query instant recency ranking reads (text_relevance's `now_secs`). */
    private const val F_NOW_SECS = "ranking.features.query(now_secs)"

    /** [EventQuery.recencyWeight]'s parameter: the `search` profile's `query(w_recency)`. */
    private const val F_W_RECENCY = "ranking.features.query(w_recency)"

    /** The text-weighting features a search term brings — meaningless on the recency profiles, which read no text signal. */
    private val TEXT_RANK_FEATURES = setOf(F_W_GRAM, F_N_WORDS, F_NOW_SECS)

    /** What [EventQuery.rankFeatures] may name: the shape every `query()` input in event.sd has. */
    private val RANK_FEATURE_NAME = Regex("[a-z][a-z0-9_]*")

    /** Vespa's "disable this ceiling" sentinel for the `grouping.*Max*` settings. */
    const val UNLIMITED_GROUPS = "-1"

    /**
     * The one grouping ceiling Vespa only accepts from a query profile — NOT
     * in [grouping]'s params. Named here for the guard that keeps it out.
     */
    const val GLOBAL_MAX_GROUPS = "grouping.globalMaxGroups"

    /**
     * How many tokens Vespa cuts [phrase] into: event.sd body_phrase()'s
     * threshold, which the typed-word count `n_words` is not. Vespa splits a
     * word at punctuation and drops it ("don't" is `don t`, "e-cash" `e cash`,
     * "21.5" `21 5`, "foo_bar" `foo bar`), so against `n_words` a body holding
     * only "don't" read as the whole of `don't trust` — 58 of the 74 matches
     * on the evaluation corpus took the band, and 1 of them held the phrase
     * (2026-10-23). Read off Vespa's own query tokenization that day: a run of
     * letters, digits and combining marks is ONE token whatever its scripts
     * ("web3", "比特币", "bitcoin比特币", "नमस्ते", "café"), and an emoji is one
     * of its own. Counted to err only HIGH — every symbol code point is a
     * token, so "❤️" counts 2 — because the errors are not symmetric: an
     * over-count withholds a tie-breaker band, an under-count hands it to a
     * body that lacks the phrase.
     */
    internal fun phraseTokens(phrase: String): Int {
        var tokens = 0
        var inRun = false
        var i = 0
        while (i < phrase.length) {
            val cp = phrase.codePointAt(i)
            when (Character.getType(cp).toByte()) {
                Character.UPPERCASE_LETTER, Character.LOWERCASE_LETTER, Character.TITLECASE_LETTER,
                Character.MODIFIER_LETTER, Character.OTHER_LETTER, Character.DECIMAL_DIGIT_NUMBER,
                Character.LETTER_NUMBER, Character.OTHER_NUMBER, Character.NON_SPACING_MARK,
                Character.ENCLOSING_MARK, Character.COMBINING_SPACING_MARK,
                -> {
                    if (!inRun) tokens++
                    inRun = true
                }

                Character.MATH_SYMBOL, Character.CURRENCY_SYMBOL, Character.MODIFIER_SYMBOL, Character.OTHER_SYMBOL -> {
                    tokens++
                    inRun = false
                }

                else -> {
                    inRun = false
                }
            }
            i += Character.charCount(cp)
        }
        return tokens
    }

    /**
     * WHICH CLAUSE KINDS [q] carries, for [VespaQuery.shape] — read off the
     * QUERY, which is the only place that knows without guessing.
     *
     * MIRRORS [filterClauses] BRANCH FOR BRANCH, and sits beside it so the two
     * are edited together: a clause added there wants a name here, or a
     * degraded-read tally starts under-describing the shape that produced it.
     *
     * DELIBERATELY COARSER than that list, because this is a COUNTER KEY and
     * the key space has to stay small enough to budget (see `Activity`). The
     * weighted forms fold into their plain ones (`idWeights` is still an id
     * recall), `tagsAll` into `tags`, `phrases` into `search`, and the two
     * expiry stamps are dropped outright — the store puts `notExpiredAt` on
     * nearly every read, so it separates nothing and would double the space.
     * Eight clause kinds, so 256 shapes at most, against the 512 the substring
     * table could produce.
     */
    internal fun clauseShape(q: EventQuery): String {
        val parts = ArrayList<String>(4)
        if (q.ids.isNotEmpty() || q.idWeights.isNotEmpty()) parts += "ids"
        if (q.kinds.isNotEmpty()) parts += "kinds"
        if (q.authors.isNotEmpty() || q.authorWeights.isNotEmpty()) parts += "authors"
        if (q.owners.isNotEmpty()) parts += "owners"
        if (q.tags.isNotEmpty() || q.tagsAll.isNotEmpty()) parts += "tags"
        if (!q.search.isNullOrBlank() || q.phrases.isNotEmpty()) parts += "search"
        if (q.since != null) parts += "since"
        if (q.until != null) parts += "until"
        return if (parts.isEmpty()) SHAPE_PLAIN else parts.joinToString(",")
    }

    /**
     * The shared WHERE clauses (filters + optional search term + exclusions); null when the filter provably matches nothing.
     * [bodyPhrase] adds the rank-only body phrase, which only [RANK_SEARCH]'s second phase reads — see the clause.
     */
    private fun filterClauses(
        q: EventQuery,
        params: MutableMap<String, String>,
        bodyPhrase: Boolean = false,
    ): List<String>? {
        val clauses = ArrayList<String>()

        if (q.ids.isNotEmpty()) clauses += hexIn("id", q.ids) ?: return null
        if (q.idWeights.isNotEmpty()) clauses += hexDotProduct("id", q.idWeights) ?: return null
        if (q.kinds.isNotEmpty()) clauses += "kind in (${q.kinds.joinToString(", ")})"
        if (q.authors.isNotEmpty()) clauses += hexIn("pubkey", q.authors) ?: return null
        if (q.authorWeights.isNotEmpty()) clauses += hexDotProduct("pubkey", q.authorWeights) ?: return null
        if (q.owners.isNotEmpty()) clauses += hexIn("owner", q.owners) ?: return null
        for ((name, values) in q.tags) {
            clauses += tagClause(name, values, "or") ?: return null
        }
        for ((name, values) in q.tagsAll) {
            clauses += tagClause(name, values, "and") ?: return null
        }
        q.since?.let { clauses += "created_at >= $it" }
        q.until?.let { clauses += "created_at <= $it" }
        q.expiresBefore?.let { clauses += "expires_at < $it" }
        q.notExpiredAt?.let { clauses += "expires_at > $it" }

        // Every word the caller typed goes into the query — never silently
        // dropped for speed. Exception: a word with nothing an index holds
        // ([IndexableChars]: no letter, digit or emoji — "₿", "∞", "#") is
        // erased by tokenization on the doc side too, and a query of only such
        // terms is an HTTP 400 (NullItem); it is dropped, and a query that is
        // ONLY such words is provably no match. An emoji is NOT such a word:
        // "⚡" is indexed, and searches for itself. (SearchFields.matches
        // applies the same rule.)
        //
        // Default-grammar syntax is cleaned from what is SENT
        // ([IndexableChars.queryText]: quotes and stars become spaces, a lone
        // variation selector and skin tones go), each of which made a typed
        // word a different query or a 400. Not from [words]: a search that is
        // only syntax must stay "provably no match", never a search for
        // everything.
        val words =
            q.search
                ?.trim()
                .orEmpty()
                .split(WHITESPACE)
                .filter { it.isNotEmpty() }
        val matchable =
            IndexableChars
                .queryText(q.search?.trim().orEmpty())
                .split(WHITESPACE)
                .filter(IndexableChars::hasIndexable)
        if (words.isNotEmpty() && matchable.isEmpty()) return null
        if (matchable.isNotEmpty()) {
            // The body phrase only where a body can exist: on body-less kinds it
            // matches nothing and still costs seconds ([BODYLESS_KINDS]).
            val bodyReachable = FuzzyWordGroup.bodyReachable(q.kinds)
            val bodyGram = q.bodyGramMatching && bodyReachable
            val stem = q.stemLanguage?.takeIf { bodyReachable }
            val words = FuzzyWordGroup.clause(matchable, params, nearFields = q.nearMatching, bodyGram = bodyGram, bodyEdge = q.bodyEdgeMatching, stemLanguage = stem)
            // The body PHRASE, as a RANK-ONLY item: `rank(match, extra)` matches
            // exactly what `match` matches and only adds `extra`'s match data.
            // event.sd's body_phrase() reads fieldMatch(search_text).longestSequence,
            // which counts a run of query terms adjacent in the FIELD and in the
            // QUERY — and in the word groups the synthetic joined pairs
            // ("bookof") sit between the typed words, so without this item no
            // body ever read as holding the phrase (measured 2026-10-23: 0 of
            // the 28 notes saying "book of daniel"; with it, 28 of 28 and no
            // false positive among the 14 that hold the words apart). Only
            // where it can mean something: two or more words, and a kind that
            // can carry a body. Quoted phrases need none — they are required.
            // And only a phrase Vespa ACCEPTS as typed: one word repeated past
            // [PhraseRuns.MAX_RUN] in a row is refused with a 400 that fails the
            // whole REQ (SearchExactTextIT's "x no no no no no no" caught this),
            // so such a query simply gets no phrase band — a tie-breaker it can
            // do without — rather than the cut pieces a REQUIRED phrase needs.
            // And only for the `search` profile ([bodyPhrase]): the item is not
            // ranking-neutral elsewhere — it is a search_text term, so it feeds
            // bm25(search_text) and reordered observer-less `text` pages that no
            // evaluation measured — and every other read (counts, id walks,
            // recency) would pay a positional phrase iterator for nothing.
            val phrase = matchable.joinToString(" ")
            clauses +=
                if (bodyPhrase && matchable.size > 1 && bodyReachable && PhraseRuns.pieces(phrase) == listOf(phrase)) {
                    params[P_BODY_PHRASE] = phrase
                    params[F_PHRASE_TERMS] = phraseTokens(phrase).toString()
                    "rank($words, ({label:\"body_phrase\",defaultIndex:\"search_text\",grammar:\"phrase\"}userInput(@$P_BODY_PHRASE)))"
                } else {
                    words
                }
            // Short queries lean harder on the trigram safety net — judged by
            // the words that HAVE grams: an emoji beside "extraordinary" is no
            // reason to weight its trigrams up.
            val gramWords = matchable.filter(IndexableChars::hasLetterOrDigit)
            params[F_W_GRAM] = if (gramWords.isNotEmpty() && FuzzyWordGroup.leansOnGrams(gramWords)) "8.0" else "2.0"
        }

        // Quoted phrases ([EventQuery.phrases]): one REQUIRED phrase-grammar
        // term per entry against the `default` fieldset, text out-of-band. No
        // fuzzy word group — exact and adjacent is the point of quoting. The
        // phrase rides RAW (the tokenizer drops what indexing dropped), so
        // only an ALL-erased phrase needs the unsatisfiable-requirement rule —
        // and RAW but for [PhraseRuns.pieces]: a phrase repeating one word too
        // often is one Vespa refuses outright, failing the whole REQ, so it
        // rides as the pieces Vespa accepts, every one REQUIRED. Nearly every
        // phrase is one piece, `@p$i`, exactly as typed.
        var phraseItems = 0
        q.phrases.forEachIndexed { i, phrase ->
            if (!IndexableChars.hasIndexable(phrase)) return null
            PhraseRuns.pieces(phrase).forEachIndexed { j, piece ->
                val name = if (j == 0) "p$i" else "p${i}c$j"
                params[name] = piece
                clauses += "({defaultIndex:\"default\",grammar:\"phrase\"}userInput(@$name))"
                phraseItems++
            }
        }

        // How many things the USER asked for — matchable words PLUS quoted
        // phrases — for the schema's perfect_match() rung, which cannot count
        // them itself. fieldMatch's queryCompleteness divides by every term in
        // the tree, including the SYNTHETIC ones [FuzzyWordGroup] adds (a
        // whole-field match read 2/3 at two words, 1/4 at three). A quoted
        // phrase is ONE matchCount item however many words it spans, so phrases
        // count 1 each; omitting them left the feature unsent on a phrase-only
        // query, where the schema default of 1 is wrong for two phrases.
        // Both measured on a live Vespa, 2026-08-05. A phrase split into
        // pieces (PhraseRuns) is that many matchCount items, so it counts each.
        val queryItems = matchable.size + phraseItems
        if (queryItems > 0) params[F_N_WORDS] = queryItems.toString()

        // Exclusions ([EventQuery.notSearch]): one negated term per word,
        // out-of-band, deliberately NOT the fuzzy word group — exclusion must
        // never out-reach what the user literally typed. grammar:"phrase"
        // keeps a punctuated word ("e-cash") one adjacent unit. A tokenization-
        // erased word ("-₿") is vacuous here (no index holds it, so nothing
        // can be excluded by it) and is simply dropped — the mirror of the
        // positive-side rule; "-🔥" is not erased, and excludes. Phrase grammar is also why [PhraseRuns.pieces]:
        // "-no-no-no-no-no-no" is six "no"s in one phrase, an HTTP 400. A word
        // in pieces drops the documents holding ALL of them.
        q.notSearch
            .filter(IndexableChars::hasIndexable)
            .forEachIndexed { i, word ->
                val pieces =
                    PhraseRuns.exclusionPieces(word).mapIndexed { j, piece ->
                        val name = if (j == 0) "n$i" else "n${i}c$j"
                        params[name] = piece
                        "({defaultIndex:\"default\",grammar:\"phrase\"}userInput(@$name))"
                    }
                clauses += "!(${pieces.joinToString(" and ")})"
            }
        return clauses
    }

    /**
     * The WHERE text for [clauses]. A negation-only list gets an explicit
     * `true` companion: YQL's `!` is AND-NOT sugar and needs a positive side
     * to subtract from.
     */
    private fun whereOf(clauses: List<String>): String =
        when {
            clauses.isEmpty() -> "true"
            clauses.all { it.startsWith("!(") } -> joined(listOf("true") + clauses, " and ")
            else -> joined(clauses, " and ")
        }

    /** [parts] joined by [separator] into one buffer sized up front: an id list's clause runs to tens of kilobytes. */
    private fun joined(
        parts: List<String>,
        separator: String,
    ): String {
        if (parts.size == 1) return parts[0]
        var length = separator.length * (parts.size - 1)
        parts.forEach { length += it.length }
        val out = StringBuilder(length)
        parts.forEachIndexed { i, part -> (if (i > 0) out.append(separator) else out).append(part) }
        return out.toString()
    }

    /**
     * One tag constraint: values joined with [op] ("or" = NIP-01 tags, "and" =
     * tagsAll). Null when it can't match: tag_index only holds single-letter
     * names, and a present-but-empty value list matches nothing.
     *
     * The OR case compiles to `in`, not an OR-chain of `contains`: one
     * dictionary-backed iterator vs per-term iterators plus the OR merge — the
     * gap grows with the list, and relay tag lists run to hundreds of values.
     * Semantics on an array attribute are identical. AND has no `in` form.
     */
    private fun tagClause(
        name: String,
        values: List<String>,
        op: String,
    ): String? {
        if (!isIndexableTagName(name)) return null
        if (values.isEmpty()) return null
        // A value [quote] cannot render is also a value no document can hold
        // (see [isQuotable]), so it is unmatchable rather than an escaping
        // problem. OR DROPS them — the same treatment [hexIn] gives invalid
        // hex. AND cannot: every value must be present, so one unmatchable
        // value makes the conjunction unsatisfiable, and dropping it would
        // WIDEN the query into matching documents that lack the term.
        val usable = values.filter(::isQuotable)
        if (op == "and" && usable.size != values.size) return null
        if (usable.isEmpty()) return null
        if (op == "or" && usable.size > 1) {
            var length = 16
            usable.forEach { length += name.length + it.length + 5 }
            val out = StringBuilder(length).append("tag_index in (")
            usable.forEachIndexed { i, v -> appendQuoted(if (i > 0) out.append(", ") else out, name, v) }
            return out.append(')').toString()
        }
        return usable.joinToString(" $op ", prefix = "(", postfix = ")") { v -> "tag_index contains ${quote("$name:$v")}" }
    }

    /**
     * Whether [quote] can render [s] into a YQL literal at all.
     *
     * Of the C0 block only tab, LF and CR have an escape in [quote] — and those
     * are exactly the three the engine will STORE, so a tag value carrying any
     * other C0 character can never sit in `tag_index` (`VespaText`
     * .firstIllegalField refuses the whole event first). Unmatchable either way.
     *
     * DEL and the C1 block are deliberately NOT excluded: Vespa stores them
     * (mojibake'd Latin-1 lands there constantly), documents really do carry
     * them, and they need no escape.
     */
    private fun isQuotable(s: String): Boolean = s.none { it < ' ' && it != '\t' && it != '\n' && it != '\r' }

    /**
     * `field in (…)` over the valid 64-hex entries of [values] (normalized to
     * lowercase). Invalid entries can never match and are dropped — but if
     * nothing valid remains the constraint is unsatisfiable: null.
     */
    private fun hexIn(
        field: String,
        values: List<String>,
    ): String? {
        val hexes = LinkedHashSet<String>(values.size * 2)
        values.forEach { v -> v.lowercase().let { if (Hex.isHex64(it)) hexes += it } }
        if (hexes.isEmpty()) return null
        val out = StringBuilder(field.length + 6 + hexes.size * 68).append(field).append(" in (")
        hexes.forEachIndexed { i, hex -> (if (i > 0) out.append(", ") else out).append('"').append(hex).append('"') }
        return out.append(')').toString()
    }

    /**
     * The same recall as [hexIn], with a NUMBER attached to each key that the
     * ranking reads back as `rawScore(field)` — see [EventQuery.authorWeights].
     *
     * `dotProduct`, not `weightedSet`, and that is a measured distinction rather
     * than a stylistic one: against a real Vespa both recall exactly the keys on
     * a single-value `fast-search` string attribute, but only `dotProduct` sets
     * `rawScore` — with `weightedSet` every matched document reads 0 and the
     * per-key number is simply lost. The weights are INTEGERS by construction
     * (quartz's 0..100 member scale), which is also what the operator takes.
     *
     * Same unsatisfiability rule as [hexIn]: nothing valid left, no constraint
     * that could match, null. A weight of zero is kept — it recalls its document
     * and scores it zero, which is what a publisher who wrote 0 said.
     */
    private fun hexDotProduct(
        field: String,
        weights: Map<String, Int>,
    ): String? {
        val entries =
            weights.entries
                .mapNotNull { (key, weight) -> key.lowercase().takeIf(Hex::isHex64)?.let { it to weight } }
                .distinctBy { it.first }
        if (entries.isEmpty()) return null
        return "dotProduct($field, {${entries.joinToString(", ") { (key, weight) -> "\"$key\": $weight" }}})"
    }

    /** YQL string literal with backslash/quote/control escaping — for caller-supplied text. */
    private fun quote(s: String): String =
        StringBuilder(s.length + 8)
            .append('"')
            .escapedYql(s)
            .append('"')
            .toString()

    /** `"[prefix]:[value]"` as one YQL literal, without building the joined string first. */
    private fun appendQuoted(
        out: StringBuilder,
        prefix: String,
        value: String,
    ): StringBuilder =
        out
            .append('"')
            .escapedYql(prefix)
            .append(':')
            .escapedYql(value)
            .append('"')

    /** [s] with backslash, quote, LF, CR and tab escaped: [quote]'s body. */
    private fun StringBuilder.escapedYql(s: String): StringBuilder {
        for (c in s) {
            when (c) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else -> append(c)
            }
        }
        return this
    }
}
