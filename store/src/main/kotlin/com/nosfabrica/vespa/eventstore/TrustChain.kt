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

/**
 * ONE READER'S TRUST CHAIN AS THIS STORE FOLLOWS IT: their kind-10040, the
 * `30382:rank` service it resolves to, and whether that service has signed
 * anything here — the facts behind "why is my ranked search empty", as values
 * ([LensedEventStore.lensOf]).
 *
 * The store's rule, not a consumer's: a list naming several rank services
 * resolves to the FIRST (`trust/ProviderMap`: NIP-85 prescribes no merge, and
 * Amethyst models one slot per metric). vespa-relay's sign-in notice re-derived
 * the chain and checked ANY of the first eight services for cards, so a reader
 * whose first provider had signed nothing here — an empty ranked page — got no
 * notice. Reading this, it cannot disagree with the lens the reads use.
 *
 * Two lenses, because the store keeps two: [rankService] / [followersService]
 * are the CACHED provider pass every read uses right now; [storedRankService] /
 * [storedFollowersService] are the stored 10040 read fresh. They differ while
 * the pass has not caught up with a list another process stored (#145) —
 * [passAgeSecs] says how old it is — and the stored one is what the reads will
 * use after the next refresh.
 */
class TrustChain(
    val pubkey: String,
    /** A live (unexpired) kind-10040 by [pubkey] is stored. */
    val hasProviderList: Boolean,
    /** The rank service the cached pass resolves [pubkey] to; null when it resolves none. */
    val rankService: String?,
    /** The followers service the cached pass resolves [pubkey] to; null when it resolves none. */
    val followersService: String?,
    /** The stored list's first `30382:rank` provider; null without a list, or with one naming none. */
    val storedRankService: String?,
    /** The stored list's first `30382:followers` provider. */
    val storedFollowersService: String?,
    /**
     * Whether any kind-30382 signed by the rank service is held — the stored
     * list's, else the cached pass's; null when there is no rank service to ask
     * about. False means every ranked read under this lens is empty until that
     * provider's cards are mirrored here.
     */
    val rankServiceHasCards: Boolean?,
    /** Seconds since the cached provider pass was issued; null while none is cached. */
    val passAgeSecs: Long?,
) {
    /** The first link of the chain this store cannot follow, in the order a read fails; null when the chain is whole. */
    enum class Gap {
        /** No kind-10040 by this reader is stored. */
        NO_PROVIDER_LIST,

        /** The stored 10040 names no usable `30382:rank` service. */
        NO_RANK_SERVICE,

        /** The rank service has signed no kind-30382 held here. */
        NO_RANK_CARDS,
    }

    /** See [Gap]: the first missing link, the one a notice should name. */
    val gap: Gap?
        get() =
            when {
                !hasProviderList -> Gap.NO_PROVIDER_LIST
                (storedRankService ?: rankService) == null -> Gap.NO_RANK_SERVICE
                rankServiceHasCards == false -> Gap.NO_RANK_CARDS
                else -> null
            }

    /** Whether the cached pass resolves this reader differently than their stored 10040 does. */
    val passDisagrees: Boolean get() = hasProviderList && (rankService != storedRankService || followersService != storedFollowersService)

    override fun toString(): String = "TrustChain(${pubkey.take(16)}…, list=$hasProviderList, rank=${rankService?.take(16)}, stored=${storedRankService?.take(16)}, cards=$rankServiceHasCards, gap=$gap)"
}
