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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * WHY THIS PUBKEY SEES WHAT IT SEES — every fact behind one ranked read, in
 * one call.
 *
 * Answering "an observer cannot find their own profile" took a dozen
 * hand-written YQL queries against the document API, one of which was silently
 * broken and produced four confident false negatives, and another of which hit
 * `grouping.defaultMaxGroups` (TEN) and under-reported 1,060 services as 10.
 * Both traps are avoided here for the same reason: this asks through
 * [EventIndex], which sets the grouping ceilings and refuses partial answers,
 * rather than through a curl an operator assembles under pressure.
 *
 * Read-only. Every field is a fact, not a verdict — [Explanation.summary] says
 * which fact is the reason, and says nothing when none of them is.
 */
internal class TrustExplain(
    private val index: EventIndex,
    private val reputations: ReputationIndex,
    private val recompute: TrustRecompute,
) {
    class Explanation(
        val pubkey: String,
        val profiles: Int,
        val providerLists: Int,
        val rankService: String?,
        val followersService: String?,
        val cardsAbout: Int,
        val parentExists: Boolean,
        val influenceCells: Int,
        val followerCells: Int,
        val rankCellFromLens: Int?,
        val followersCellFromLens: Double?,
    ) {
        /**
         * The first fact that would empty this observer's ranked page, or a
         * clean line. Ordered the way the read fails: no lens beats no cell,
         * and no cell beats no cards.
         */
        val summary: String
            get() =
                when {
                    providerLists == 0 -> {
                        "no kind-10040: this observer has no lens, so a ranked read has nothing to resolve"
                    }

                    rankService == null && followersService == null -> {
                        "kind-10040 names no 30382 service"
                    }

                    rankCellFromLens == null && followersCellFromLens == null && !parentExists -> {
                        "no reputation parent: nothing has ever scored this subject"
                    }

                    rankCellFromLens == null && followersCellFromLens == null -> {
                        "parent carries $influenceCells cell(s) but NONE from this observer's own lens " +
                            "(${(rankService ?: followersService)?.take(16)}...) — that service's cards are unprojected"
                    }

                    else -> {
                        "lens resolves and the parent carries its cell; a ranked read should return this subject"
                    }
                }

        fun line(): String =
            "trust-explain ${pubkey.take(16)}...: profiles=$profiles lists=$providerLists " +
                "lens(rank=${rankService?.take(16) ?: "-"}, followers=${followersService?.take(16) ?: "-"}) " +
                "cardsAbout=$cardsAbout parent=$parentExists cells=$influenceCells/$followerCells " +
                "fromLens(rank=${rankCellFromLens ?: "-"}, followers=${followersCellFromLens ?: "-"}) — $summary"
    }

    suspend fun explain(pubkey: String): Explanation {
        val profiles = index.search(EventQuery(kinds = listOf(0), authors = listOf(pubkey), limit = 1)).size
        val lists = index.search(EventQuery(kinds = listOf(TrustProviderListEvent.KIND), authors = listOf(pubkey), limit = 1))
        val lens = recompute.providerMap().lensOf(pubkey)
        val cards = index.search(EventQuery(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to listOf(pubkey)), limit = MAX_CARDS)).size
        val parent = reputations.get(pubkey)
        return Explanation(
            pubkey = pubkey,
            profiles = profiles,
            providerLists = lists.size,
            rankService = lens.rank,
            followersService = lens.followers,
            cardsAbout = cards,
            parentExists = parent != null,
            influenceCells = parent?.influenceScores?.size ?: 0,
            followerCells = parent?.followerCounts?.size ?: 0,
            rankCellFromLens = lens.rank?.let { parent?.influenceScores?.get(ServiceKey(it)) },
            followersCellFromLens = lens.followers?.let { parent?.followerCounts?.get(ServiceKey(it)) },
        )
    }

    private companion object {
        /** Enough to say whether cards exist and roughly how many; this is a diagnostic, not a walk. */
        const val MAX_CARDS = 1000
    }
}
