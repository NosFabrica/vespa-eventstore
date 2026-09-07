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
package com.nosfabrica.vespa.eventstore.search

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery

/**
 * HOW ONE PAGE'S REFERENCES BECOME QUERIES — the grouping between "these are
 * the records the hits point at" and "these are the round trips that fetch
 * them".
 *
 * Its whole job is deciding what may share a query. Two keys can when the
 * query would be IDENTICAL, and only then: the per-key confidences never force
 * a split (each key carries its own into the batch), while the pointer's own
 * relevance does, because a weighted lookup sends it. Get that wrong in the
 * cheap direction and members are ranked under the wrong pointer's
 * confidence; get it wrong in the expensive one and a ranked page of Trusted
 * Lists becomes one engine query per list.
 *
 * FIRST WRITER WINS, throughout. Rows arrive in page order — relevance order
 * on any page that can be sorted — so a key two pointers name is fetched under
 * the better-ranked one, which is also the pointer the placement will file it
 * under. The address buckets say the same thing in their own order (unscored
 * first, then descending confidence), because [SearchReferenceExpansion] files
 * found subjects with `putIfAbsent`.
 *
 * Split out of [SearchReferenceExpansion] because it is the one part of the
 * expansion that is pure: references and numbers in, groups out, no index and
 * no suspension — so it can be asserted directly rather than through a page.
 */
internal class SubjectBatches(
    private val limits: SearchExpansionLimits,
) {
    /** One lens's id and pubkey batches, in the order their first row created them. */
    class Grouped(
        val ids: List<Batch>,
        val pubKeys: List<Batch>,
    )

    /**
     * Group every reference this [lens] claims into the fewest queries that
     * can carry them, ids and pubkeys apart.
     *
     * [pointerRel] and [pointerText] read the row's two engine scores; a row
     * the caller cannot score passes 0.0, which lands every scored reference in
     * one batch — the floor-off shape.
     */
    fun group(
        planned: List<References>,
        lensOfRow: IntArray,
        lens: Int,
        pointerRel: (Int) -> Double,
        pointerText: (Int) -> Double,
    ): Grouped {
        val idBatches = LinkedHashMap<Any, Batch>()
        val keyBatches = LinkedHashMap<Any, Batch>()
        val claimedIds = HashSet<String>()
        val claimedKeys = HashSet<String>()
        for (row in planned.indices) {
            if (lensOfRow[row] != lens) continue
            val refs = planned[row]
            val rel = pointerRel(row)
            val text = pointerText(row)
            for ((weighted, ids) in refs.eventIds.filterNot { it in claimedIds }.splitByScored(refs)) {
                claimedIds += ids
                idBatches.batch(weighted, rel, text).take(ids, refs)
            }
            for ((weighted, pubKeys) in refs.pubKeys.filterNot { it in claimedKeys }.splitByScored(refs)) {
                claimedKeys += pubKeys
                keyBatches.batch(weighted, rel, text).take(pubKeys, refs)
            }
        }
        return Grouped(idBatches.values.toList(), keyBatches.values.toList())
    }

    /** [bucketed], as this lens's address lookups: the public name for what the buckets are FOR. */
    fun addressBuckets(
        planned: List<References>,
        lensOfRow: IntArray,
        lens: Int,
    ): List<Pair<Double?, LinkedHashSet<String>>> = bucketed(planned, lensOfRow, lens)

    /**
     * ONE ROUND TRIP'S WORTH OF KEYS: everything that can be asked for in a
     * single query, with each key's own confidence.
     *
     * Two rows share a batch when they would send the SAME query, which is the
     * only thing that has to force them apart — the per-key weights never do,
     * since each key carries its own. Unscored references send the caller's
     * lens untouched, so they all share one; scored ones differ only in
     * [pointerRel], and not even in that when the floor is off.
     */
    class Batch(
        val weighted: Boolean,
        val pointerRel: Double,
        val pointerText: Double,
    ) {
        /** Key -> the 0..100 confidence its pointer gave it; 0 and unread on an unscored batch. */
        val keys = LinkedHashMap<String, Int>()

        fun take(
            batched: List<String>,
            refs: References,
        ) {
            batched.forEach { keys[it] = refs.confidence[it] ?: 0 }
        }

        fun query(
            under: EventQuery,
            profile: String?,
            limits: SearchExpansionLimits,
        ): EventQuery =
            if (weighted) {
                under.withWeightedMember(profile, limits.confidenceGamma, pointerRel, pointerText, limits.subjectFloorSpan)
            } else {
                under
            }
    }

    /**
     * The batch this row's half belongs in — created on first use, so the map's
     * insertion order is page order and the best-ranked pointer keeps a
     * contested key.
     *
     * The identity is what the QUERY would carry: nothing at all for the
     * unscored, and for the scored either the pointer's relevance or, with the
     * floor off, one shared batch — [withWeightedMember] does not send the
     * relevance then, so splitting on it would buy identical queries.
     */
    private fun MutableMap<Any, Batch>.batch(
        weighted: Boolean,
        rel: Double,
        text: Double,
    ): Batch {
        val floored = weighted && limits.subjectFloorSpan != null
        // BOTH numbers are the identity now: two pointers that happen to share
        // a relevance may still have earned it differently (one on a title
        // match under a trusted signer, one on a weaker match under a better
        // one), and their members are placed by the TEXT half.
        val identity: Any = if (floored) listOf(rel, text) else weighted
        return getOrPut(identity) { Batch(weighted, if (floored) rel else 0.0, if (floored) text else 0.0) }
    }

    /**
     * One row's keys split into the SCORED and the UNSCORED, in that order,
     * dropping whichever half is empty.
     *
     * The two cannot share a lookup: a scored member is ranked on the member
     * rung by the number its list gave it, while a reference that expressed no
     * confidence "is as sure as the pointer itself" and must come back with NO
     * member score at all, so the placement can hand it the pointer's own. That
     * is the same split the buckets drew with a null key, and it is one query
     * each in the overwhelmingly common case where a list scores everybody or
     * nobody.
     */
    private fun List<String>.splitByScored(refs: References): List<Pair<Boolean, List<String>>> {
        val (scored, unscored) = partition { refs.weightOf(it) != null }
        return listOfNotNull(
            (true to scored).takeIf { scored.isNotEmpty() },
            (false to unscored).takeIf { unscored.isNotEmpty() },
        )
    }

    /**
     * This lens's COORDINATE references, grouped by quantized confidence.
     *
     * Ordered HIGHEST FIRST so that a member two lists disagree about is looked
     * up under the higher one — `putIfAbsent` above then keeps that first
     * answer. The generous reading is the right one for a disagreement between
     * two publishers the reader delegated: they both vouched, and the reader
     * asked for both.
     *
     * COORDINATES ONLY, because they are the only shape left that buckets. This
     * used to collect all three and hand back a `Shapes` holding each, from when
     * the buckets were how EVERY member reached its rung; the keyed shapes moved
     * to weighted batches ([Batch]) and their two sets became write-only. They
     * were not free: a page is walked here for every scored searching read, so a
     * list of 1,000 members cost 1,000 boxed bucket keys, hash lookups and
     * `LinkedHashSet` inserts that nothing ever read — and the addressable
     * family it all fed has no instances at all on the staging corpus. Buckets
     * that only ids or pubkeys created are gone with them, which changes
     * nothing: they reached the loop below with no addresses and issued no
     * query.
     */
    private fun bucketed(
        planned: List<References>,
        lensOfRow: IntArray,
        lens: Int,
    ): List<Pair<Double?, LinkedHashSet<String>>> {
        val out = HashMap<Double?, LinkedHashSet<String>>()
        planned.forEachIndexed { i, refs ->
            if (lensOfRow[i] != lens || refs.addresses.isEmpty()) return@forEachIndexed
            refs.addresses.forEach { out.getOrPut(bucketOf(refs.weightOf(it))) { LinkedHashSet() }.add(it) }
        }
        if (out.isEmpty()) return emptyList()
        // UNSCORED FIRST, THEN DESCENDING CONFIDENCE, and the order is
        // load-bearing: [lookUp] files each found subject with `putIfAbsent`, so
        // whichever bucket runs first wins a member that two pointers name. An
        // unscored reference is not a doubted one — it is a claim with no
        // confidence attached — so it must not lose its subject to a scored
        // duplicate; and between two publishers the reader delegated, both of
        // whom vouched, the generous reading is the right one.
        //
        // `compareBy(nullsFirst())` sorted ASCENDING, which handed every
        // contested member to the publisher that doubted it most.
        val (unscored, scored) = out.entries.partition { it.key == null }
        return (unscored + scored.sortedByDescending { it.key!! }).map { it.key to it.value }
    }

    /**
     * A 0..1 weight quantized to [BUCKETS] steps, or NULL where the pointer
     * expressed no confidence at all.
     *
     * Null is not zero and not one: it means the member rung does not apply.
     * A NIP-32 label has no confidence field in the NIP and a NIP-85
     * assertion's `d` IS its subject, so neither claim is probabilistic —
     * there is no doubt for a rung to express, and those subjects keep the
     * placement they have always had, their POINTER's own score, which puts
     * them directly behind it. Only a Trusted List member is scored, and only
     * a scored member goes on a rung.
     */
    private fun bucketOf(weight: Double?): Double? = weight?.let { Math.round(it * BUCKETS) / BUCKETS.toDouble() }

    private companion object {
        /**
         * Confidence steps one page may distinguish, FOR THE ADDRESSABLE SHAPE
         * ALONE — and, because a rank feature is a property of the QUERY, the
         * round trips it can cost: one lookup per occupied bucket per owner.
         *
         * THE KEYED SHAPES NO LONGER COME THROUGH HERE. A weighted recall
         * carries each member's confidence on its own key
         * ([EventQuery.authorWeights]), so a whole list is ONE query at the
         * publisher's own resolution — which is what quantizing was buying its
         * way out of. A coordinate is (kind, author, d) and no single attribute
         * holds it, so 30394 members have no key to hang a weight on and keep
         * the buckets; `tag_index` (`d:<value>`, fast-search) could carry them
         * inside one owner's group if that family ever earns the work.
         *
         * FOUR, not twenty, for the reason it always was: twenty gave 5%
         * resolution and let one well-scored list cost twenty round trips.
         * Measured against the Tapestry corpus, every confidence from 0.10 to
         * 1.00 placed a member in the SAME position relative to the page,
         * because the member band spans x7.3 while a page spans x367 — a page
         * that cannot resolve two ends of the range cannot resolve twentieths
         * of it. The floor is what undoes that argument for the keyed shapes: a
         * member is placed against its POINTER now, and a x367 page has room
         * for the difference between 0.10 and 1.00.
         *
         * Members that land in one bucket tie, and a stable sort then keeps
         * them in the order their list named them — which for a
         * descending-sorted list is the publisher's own ranking.
         */
        const val BUCKETS = 4
    }
}
