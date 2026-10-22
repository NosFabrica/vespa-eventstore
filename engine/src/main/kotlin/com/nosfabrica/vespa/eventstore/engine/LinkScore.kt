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

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlin.math.pow

/**
 * One hit whose links [EventIndex.scoreLinks] scores: its id (linked by `e`),
 * its address when it has one (linked by `a`), and its AUTHOR — whose own links
 * to it are not engagement and never count (a reply in one's own thread).
 */
data class LinkTarget(
    val id: String,
    val author: String,
    /** `kind:pubkey:d` for an addressable hit, else null. */
    val address: String? = null,
) {
    /** The `tag_index` entries that link to this hit. */
    val keys: List<String> get() = if (address == null) listOf("e:$id") else listOf("e:$id", "a:$address")
}

/**
 * One PERSON's links to one hit, as the engine saw them: their trust under the
 * lens (0..1, the rank cell over [LinkScore.TRUST_SCALE]) and how many links of
 * each kind they made.
 */
data class Linker(
    val trust: Double,
    val byKind: Map<Int, Int>,
)

/**
 * HOW LINKS BECOME A SCORE — the policy [EventIndex.scoreLinks] applies, DATA
 * handed in by the caller: which links help a hit and which hurt it is relay
 * POLICY (the store owns it; see store/search/LinkRerank), while the engine
 * only applies it. Three layers, each with one job:
 *
 *  1. WITHIN ONE PERSON AND ONE KIND, DIMINISHING: the first link counts its
 *     kind's weight, each further one [repeatDecay] times the one before — at
 *     0.5 a person's ten replies in a thread are worth ~2 replies, their second
 *     like barely more than one.
 *  2. WITHIN ONE PERSON, ACROSS KINDS: summed, then clamped to ±[personCap] —
 *     a like, a repost and a zap from one person (3) beat a like alone (1), and
 *     nothing past [personCap] counts. Then TIMES that person's trust.
 *  3. ACROSS PEOPLE: summed. The caller's own curve (LinkRerank's multiplier)
 *     is where many people saturate; a third decay here would double it.
 *
 * The hit's own author is not a person here at all ([LinkTarget.author]).
 *
 * WHO A "PERSON" IS: a link's author — except a zap receipt (9735), which is
 * signed by the RECIPIENT's LNURL server, so its person is the zapper the
 * receipt's `P` tag names (NIP-57; it equals the zap request's signer on every
 * receipt that carries it). A receipt without `P` (~half of them, measured) has
 * no nameable person: it counts once, on its own, at its SIGNER's trust.
 *
 * Per kind and nothing finer below the person, because that is what the
 * engine can see: `tag_index` keeps a tag's VALUE, so a NIP-56 report's reason
 * and a zap's amount are invisible to it — weighing those needs an attribute
 * written at feed time.
 */
data class LinkWeights(
    /** A link's worth for each listed kind. */
    val byKind: Map<Int, Double> = emptyMap(),
    /** A link's worth for every kind not in [byKind]; 0 makes [byKind] an allow-list. */
    val otherwise: Double = 1.0,
    /** Each further same-kind link from one person is worth this times the one before (0..1; 1 = plain count). */
    val repeatDecay: Double = DEFAULT_REPEAT_DECAY,
    /** One person's links to one hit, in weight units, are clamped to ±this before their trust multiplies in. */
    val personCap: Double = DEFAULT_PERSON_CAP,
) {
    init {
        require(otherwise.isFinite() && byKind.values.all { it.isFinite() }) { "link weights must be finite" }
        require(repeatDecay in 0.0..1.0) { "repeatDecay must be within 0..1" }
        require(personCap > 0.0 && personCap.isFinite()) { "personCap must be positive" }
    }

    fun of(kind: Int): Double = byKind[kind] ?: otherwise

    /**
     * [linkers] narrowed to the kinds that can move a score: with [otherwise]
     * at 0, only [byKind]'s non-zero kinds are worth matching, so the engine is
     * not asked to group links that would all weigh nothing. An existing kinds
     * filter on [linkers] is intersected, never widened.
     */
    fun narrow(linkers: EventQuery): EventQuery {
        if (otherwise != 0.0) return linkers
        val weighed = byKind.filterValues { it != 0.0 }.keys
        val kinds = if (linkers.kinds.isEmpty()) weighed.toList() else linkers.kinds.filter { it in weighed }
        // Nothing can score: a limit of 0 is the port's "matches nothing" sentinel.
        return if (kinds.isEmpty()) linkers.copy(limit = 0) else linkers.copy(kinds = kinds.sorted())
    }

    /** What [n] same-kind links from one person add up to, in units of one: 1 + d + d² + … (n terms). */
    fun repeats(n: Int): Double = if (repeatDecay >= 1.0) n.toDouble() else (1.0 - repeatDecay.pow(n)) / (1.0 - repeatDecay)

    /** One person's worth to one hit (layers 1 and 2), trust included. */
    fun person(linker: Linker): Double {
        val raw = linker.byKind.entries.sumOf { (kind, n) -> of(kind) * repeats(n) }
        return raw.coerceIn(-personCap, personCap) * linker.trust
    }

    /**
     * [target]'s score from the people who linked to it and the links no person
     * could be named for (each its own one-link [Linker]): layer 3, with the
     * author left out.
     */
    fun score(
        target: LinkTarget,
        people: Map<String, Linker>,
        unattributed: List<Linker> = emptyList(),
    ): LinkScore {
        val counted = people.filterKeys { it != target.author }
        val byKind = HashMap<Int, Int>()
        (counted.values + unattributed).forEach { l -> l.byKind.forEach { (k, n) -> byKind.merge(k, n, Int::plus) } }
        val score = counted.values.sumOf(::person) + unattributed.sumOf(::person)
        return LinkScore(score, byKind, counted.size + unattributed.size)
    }

    companion object {
        /** Plain counting: every link of every kind worth 1, no decay, no cap that a count could reach. */
        val COUNT = LinkWeights(repeatDecay = 1.0, personCap = Double.MAX_VALUE)

        /** Each repeat worth half the one before: at most 2 per person per kind. */
        const val DEFAULT_REPEAT_DECAY = 0.5

        /** Five weight units per person per hit — "a like, a repost, a zap, a reply" and then no more. */
        const val DEFAULT_PERSON_CAP = 5.0

        /** NIP-57: the receipt a zapper's LNURL server signs — the one kind whose person is not its author. */
        const val ZAP_RECEIPT_KIND = 9735
    }
}

/**
 * One hit's links, scored ([LinkWeights] says how): possibly NEGATIVE — a note
 * that drew only reports scores below one that drew nothing.
 *
 * Trust is the lens's rank cell over [TRUST_SCALE]: 0..1, so a person fully
 * trusted is worth exactly their (capped) weight. Below the floor a linker is not
 * counted at all. An engine reading NO lens — the in-memory reference, which
 * holds no reputation data, or an un-lensed [EventIndex.scoreLinks] — counts
 * every person fully trusted (1.0), the same way it gates nobody.
 *
 * [byKind] (the links that counted, by kind) and [people] (how many people, plus
 * unattributed zaps, they came from) are what [score] was computed from — kept
 * because a ranking nobody can explain is a ranking nobody can debug.
 */
data class LinkScore(
    val score: Double,
    val byKind: Map<Int, Int>,
    val people: Int,
) {
    companion object {
        /** The NIP-85 rank scale a lens's cells are written on (0..100); trust = cell / this. */
        const val TRUST_SCALE = 100.0
    }
}
