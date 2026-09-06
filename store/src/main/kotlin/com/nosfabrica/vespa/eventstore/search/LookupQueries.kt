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
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.vitorpamplona.quartz.nip01Core.core.Event

/*
 * The query shapes a subject lookup runs under — every one of them a
 * transformation of the FINDING query rather than a query of its own.
 *
 * That is the invariant this file exists to keep in one place: a subject is
 * served only if it satisfies everything the finding query asked for except
 * the words, and the way that is guaranteed is that the lookup IS that query
 * with its terms stripped and the subject keys intersected in. A second
 * matcher would be a second answer to "does this belong in this read", so
 * there is none — [accepts] is the one exception and it is deliberately the
 * mirror image, applied to a POINTER the engine already served.
 */

/**
 * Whether this query accepts [event] on everything EXCEPT its words.
 *
 * Used only to attribute a pointer to the lens that found it, so it checks the
 * structural constraints and not the trust floor: the floor already decided
 * whether the pointer was served at all, and re-applying it here would need the
 * rank this row was scored with, which the read no longer carries.
 *
 * Two modes on the KIND, and the caller runs them as two PASSES. With
 * [converted] false, the pointer must be of a kind this query asked for
 * outright — the attribution that has always held. With [converted] true, a
 * pointer whose kind merely CONVERTS into this query's kinds is accepted —
 * that is what attributes a companion-fetched pointer
 * ([SearchReferenceExpansion.companions]) to the query whose kinds it was
 * fetched for, and its subjects are then admitted under THAT query's kinds,
 * never under the companion's own. The converting pass may only run after the
 * asked-for pass found nothing: every kind-restricted lens converts from the
 * id-shaped families, so a single merged pass would let whichever lens came
 * first take a pointer another lens explicitly asked for.
 */
internal fun EventQuery.accepts(
    event: Event,
    converted: Boolean,
): Boolean =
    (ids.isEmpty() || event.id in ids) &&
        (
            if (converted) {
                kinds.isNotEmpty() && SearchReferences.converts(event.kind, kinds)
            } else {
                admitsKind(event.kind)
            }
        ) &&
        (authors.isEmpty() || event.pubKey in authors) &&
        (since == null || event.createdAt >= since!!) &&
        (until == null || event.createdAt <= until!!) &&
        tags.all { (name, values) -> values.any { event.has(name, it) } } &&
        tagsAll.all { (name, values) -> values.all { event.has(name, it) } }

internal fun Event.has(
    name: String,
    value: String,
): Boolean = tags.any { it.size > 1 && it[0] == name && it[1] == value }

/** Whether a read asking for these kinds could serve one of [kind]. */
internal fun EventQuery.admitsKind(kind: Int): Boolean = kinds.isEmpty() || kind in kinds

/**
 * This query with its TERMS stripped — what a subject lookup runs under.
 *
 * Everything that decides which corpus is visible survives (observer, floor,
 * spam waiver, expiry); everything about what was being looked FOR goes,
 * including the ranking profile the terms selected, since what remains is a
 * keyed recall. The `limit` goes too: a limit is the caller's budget for HITS,
 * and the expansion's own caps already bound the subjects.
 *
 * The floor survives TWICE OVER, because on the member profile `min_rank` no
 * longer gates: it anchors the trust curve inside the member's placement, and
 * `max(member_rung(), …)` floors an untrusted member back up (event.sd §13). An
 * EXPLICIT floor therefore also travels as [EventQuery.memberFloor], which that
 * profile reads as a hard gate. "Explicit" is read the way [companions] already
 * reads it — a floor that is not the default one the store stamps on every
 * lensed read — because that is the only signal left by the time a query gets
 * here, and the two decisions must not disagree about what the reader asked for.
 * A reader who explicitly asks for exactly [DEFAULT_MIN_RANK] is indistinguishable
 * from one who asked for nothing, and gets the default's behaviour.
 */
internal fun EventQuery.forLookup(): EventQuery =
    copy(
        search = null,
        phrases = emptyList(),
        notSearch = emptyList(),
        ranking = null,
        limit = null,
        memberFloor = minRank?.takeIf { it != DEFAULT_MIN_RANK },
    )

/**
 * The same lookup, asked to SCORE what it finds as a member of a list this
 * confident.
 *
 * [profile] null means the finding query ranks on no ladder — a recency read, a
 * plain recall — so there is nothing for a synthesized score to be comparable
 * with and the lookup stays unranked. The splice then falls back to the
 * pointer's own order, which is the same degradation an unscored page gets.
 */
internal fun EventQuery.withMember(
    profile: String?,
    confidence: Double,
    gamma: Double,
): EventQuery =
    if (profile == null) {
        this
    } else {
        copy(
            ranking = profile,
            rankFeatures = rankFeatures + mapOf(EventYql.F_MEMBER_CONF to confidence, "w_member_gamma" to gamma),
        )
    }

/**
 * The same lookup, asked to score what it finds as a member whose confidence
 * rides WITH ITS KEY, under a pointer this relevant.
 *
 * The two numbers arrive by different routes because they are different kinds
 * of fact. The confidence is per (list, member) and travels as a weight on the
 * key — one query for a whole list, at the publisher's own resolution. The
 * pointer's relevance is per query because the lookup is per pointer, which is
 * exactly what the weights bought.
 *
 * [profile] null means the finding query ranks on no ladder — a recency read, a
 * plain recall — so there is nothing for a member score to be comparable with,
 * and the lookup stays unranked. The splice then falls back to the pointer's own
 * order, as it always has.
 */
internal fun EventQuery.withWeightedMember(
    profile: String?,
    gamma: Double,
    pointerRelevance: Double,
    pointerText: Double,
    floorSpan: Double?,
): EventQuery =
    if (profile == null) {
        this
    } else {
        copy(
            ranking = profile,
            rankFeatures =
                rankFeatures +
                    mapOf(EventYql.F_DOC_CONF to 1.0, "w_member_gamma" to gamma) +
                    // No span, no floor: the pointer's relevance is simply not
                    // sent, and the profile's own default of 0 leaves a subject
                    // on its rung exactly as it was before the floor existed.
                    (
                        floorSpan?.let {
                            mapOf(
                                EventYql.F_POINTER_REL to pointerRelevance.coerceAtLeast(0.0),
                                EventYql.F_POINTER_TEXT to pointerText.coerceAtLeast(0.0),
                                EventYql.F_SUBJECT_FLOOR_SPAN to it,
                            )
                        } ?: emptyMap()
                    ),
        )
    }

/** The same lookup, narrowed to these ids — null when the read cannot serve them anyway. */
internal fun EventQuery.narrowIds(chunk: List<String>): EventQuery? {
    val wanted = if (ids.isEmpty()) chunk else chunk.filter { it in ids }
    return if (wanted.isEmpty()) null else copy(ids = wanted)
}

/** Narrowed to these authors' profiles — null when the read admits no kind 0, or none of these authors. */
internal fun EventQuery.narrowProfiles(chunk: List<String>): EventQuery? {
    if (!admitsKind(0)) return null
    val wanted = if (authors.isEmpty()) chunk else chunk.filter { it in authors }
    // `ids` survives rather than being cleared: a read that named specific ids
    // may only serve those, and the engine ANDs the two constraints. Clearing
    // it would let a keyed read hand back a profile it never asked for, which
    // is the one thing "admission is the engine's own job" promises it cannot.
    return if (wanted.isEmpty()) null else copy(kinds = listOf(0), authors = wanted)
}

/**
 * [narrowIds] with each id's confidence attached — the same recall, scored per
 * document. The caller's own `ids` constraint still intersects, exactly as it
 * does unweighted: admission is the engine's job either way.
 */
internal fun EventQuery.narrowIdWeights(
    chunk: List<String>,
    weights: Map<String, Int>,
): EventQuery? {
    val wanted = (if (ids.isEmpty()) chunk else chunk.filter { it in ids }).weighedBy(weights)
    return if (wanted.isEmpty()) null else copy(ids = emptyList(), idWeights = wanted)
}

/** [narrowProfiles] with each member's confidence attached. */
internal fun EventQuery.narrowProfileWeights(
    chunk: List<String>,
    weights: Map<String, Int>,
): EventQuery? {
    if (!admitsKind(0)) return null
    val wanted = (if (authors.isEmpty()) chunk else chunk.filter { it in authors }).weighedBy(weights)
    // `ids` survives for the reason [narrowProfiles] keeps it.
    return if (wanted.isEmpty()) null else copy(kinds = listOf(0), authors = emptyList(), authorWeights = wanted)
}

/**
 * These keys with the 0..100 score their pointer gave them — quartz's own
 * scale, unrounded, which is also the integer scale a weighted recall takes.
 *
 * [weights] is the BATCH's map rather than one row's, because a batch pools the
 * members of every pointer that sends the same query — so a key must carry the
 * confidence ITS OWN list expressed, not the confidence of whichever list is
 * being read at the time.
 */
internal fun List<String>.weighedBy(weights: Map<String, Int>): Map<String, Int> = mapNotNull { key -> weights[key]?.let { key to it } }.toMap()

/** Narrowed to one owner's events of one kind — the coarsest key an addressable has. */
internal fun EventQuery.narrowAddresses(
    kind: Int,
    pubkey: String,
    dTags: List<String>,
): EventQuery? {
    if (!admitsKind(kind)) return null
    if (authors.isNotEmpty() && pubkey !in authors) return null
    // A `d` the read ALREADY constrained is intersected, never replaced: `tags`
    // is a map, so `+` would drop the caller's own `#d` and serve coordinates it
    // had excluded. Same reason `ids` survives in [narrowProfiles].
    val wanted = tags["d"]?.let { asked -> dTags.filter { it in asked } } ?: dTags
    if (wanted.isEmpty()) return null
    // `d` is a tag the index answers on, so the filter goes to the engine rather
    // than being applied to the answer — a publisher with 10,000 articles must
    // not be read whole to find the three a list named.
    return copy(kinds = listOf(kind), authors = listOf(pubkey), tags = tags + ("d" to wanted))
}
