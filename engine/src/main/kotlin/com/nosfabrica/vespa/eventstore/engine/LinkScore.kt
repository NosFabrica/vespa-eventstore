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

/**
 * WHAT ONE LINK IS WORTH, by the LINKING event's kind — the input
 * [EventIndex.scoreLinks] folds a key's links with. A link scores its kind's
 * weight TIMES its author's trust under the lens ([LinkScore]): a like from an
 * author the observer trusts at 90 is worth 0.9, a report from one at 40 is
 * worth −0.4 under a weight of −1. DATA, handed in by the
 * caller: which links help a hit and which hurt it is relay POLICY (the store
 * owns it; see store/search/LinkRerank), while the engine only applies it.
 *
 * Per kind and nothing finer, because that is what the engine can see:
 * `tag_index` keeps a tag's VALUE, so a NIP-56 report's reason (`["e", id,
 * "spam"]`) and a zap's amount are invisible to it. Weighing those needs an
 * attribute written at feed time (the amount from the receipt's `bolt11`, a
 * rating from its tag), summed by the same grouping — a schema change, and
 * the reason this is a type rather than a bare map: it is where those
 * per-unit weights will go.
 */
data class LinkWeights(
    /** A link's worth for each listed kind. */
    val byKind: Map<Int, Double> = emptyMap(),
    /** A link's worth for every kind not in [byKind]; 0 makes [byKind] an allow-list. */
    val otherwise: Double = 1.0,
) {
    init {
        require(otherwise.isFinite() && byKind.values.all { it.isFinite() }) { "link weights must be finite" }
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

    /** One key's score: each kind's summed linker trust ([trustByKind]), weighed by [of]. */
    fun score(
        byKind: Map<Int, Int>,
        trustByKind: Map<Int, Double>,
    ): LinkScore = LinkScore(trustByKind.entries.sumOf { (kind, trust) -> of(kind) * trust }, byKind, trustByKind)

    companion object {
        /** Every link weighs 1: the score is the linkers' summed trust. */
        val COUNT = LinkWeights()
    }
}

/**
 * One key's links, scored: Σ over its linking kinds of the kind's weight times
 * the TRUST its linkers carry under the lens, and so possibly NEGATIVE — a note
 * that drew only reports scores below one that drew nothing.
 *
 * Trust is the lens's rank cell over [TRUST_SCALE]: 0..1, so a link from a
 * fully trusted author is worth exactly its weight. Below the floor a linker is
 * not in the sum at all (the gate drops it before the grouping). An engine
 * reading NO lens — the in-memory reference, which holds no reputation data, or
 * an un-lensed [EventIndex.scoreLinks] — counts every linker fully trusted (1.0
 * each), the same way it gates nobody: there the score is the weighed count.
 *
 * WHOSE trust: the linking EVENT's author. For most kinds that is the person
 * who engaged; a zap receipt (9735) is signed by the recipient's LNURL server,
 * so a zap is weighed by the wallet service's trust, not the zapper's (NIP-57
 * puts the sender in the receipt's `P` tag, which needs a feed-time attribute
 * before the engine can read it).
 *
 * [byKind] and [trustByKind] are what [score] was computed from, kept because a
 * ranking nobody can explain is a ranking nobody can debug, and the grouping
 * hands them back for free.
 */
data class LinkScore(
    val score: Double,
    /** How many links of each kind. */
    val byKind: Map<Int, Int>,
    /** The summed trust (0..1 each) of each kind's linkers. */
    val trustByKind: Map<Int, Double>,
) {
    companion object {
        /** The NIP-85 rank scale a lens's cells are written on (0..100); trust = cell / this. */
        const val TRUST_SCALE = 100.0
    }
}
