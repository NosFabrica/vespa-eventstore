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

import com.vitorpamplona.quartz.experimental.trustedLists.treasureMap.TrustedListProviderTag
import com.vitorpamplona.quartz.experimental.trustedLists.treasureMap.trustedListProvider
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.serviceProviders

/**
 * WHICH SIGNERS A READER HAS ASKED FOR, KIND BY KIND — the read-time face of
 * the same kind-10040 relation [ProviderMap] resolves for the write side.
 *
 * A Trusted List (30392-30395) and a NIP-85 assertion (30382-30385) are a trust
 * service's computed OUTPUT, and NIP-85 says how a reader picks services: by
 * publishing a Treasure Map that names them. So a search may only unpack one of
 * those for a reader whose own Map named its signer — otherwise a stranger's
 * computation arrives in a feed as if it had been asked for.
 *
 * ## Per kind, not one flat set
 *
 * Every entry in a Map names a kind, and the kind is WHAT was delegated:
 * `["30382:rank", …]` appoints a service to rank users, `["30393", …]` appoints
 * a publisher to curate lists of events. Collapsing the entries into one set of
 * admitted signers loses that — a reader who appointed one service to rank
 * users would have that service's event lists, address lists and external-id
 * assertions unpacked too, none of which they asked for, and one delegation
 * would hand a careless publisher a free run of three more families.
 *
 * ## Two shapes, and both are read here
 *
 * A Map names a NIP-85 provider per kind AND metric — `["30382:rank", <pubkey>,
 * <relay>]` — which is what `serviceProviders()` reads. Tapestry's Trusted Lists
 * delegate through the same event in a different shape: a GENERIC BARE-KIND
 * entry, `["30392", <pubkey>, <relay>]`, one of which delegates every list of
 * that kind (ADR `tl-treasure-map/0001`). A bare kind carries no `:`, so NIP-85's
 * parser has never returned one — read only that side and the Trusted List half
 * of the gate has no key at all, and those kinds unpack only for a reader who
 * signed the lists themselves.
 *
 * NAMED `3039x:<name>` entries are deliberately NOT admitted: the ADR reserves
 * them and says they must drive no behavior until it defines them, which is why
 * [TrustedListProviderTag.parseGeneric] exists upstream. A gate is the last
 * place to act on a reservation.
 *
 * ## The public half only
 *
 * Half a Map's delegations may be NIP-44 encrypted to its owner, and a store
 * holds no signer. A reader who keeps a delegation private gets no expansion
 * from it. That is not new — [ProviderMap] reads `tags` and has always had the
 * same blind spot — and it is the safe direction for a gate.
 */
internal data class Delegations(
    /** observer -> (delegated kind -> the signers named for it). */
    private val byObserver: Map<String, Map<Int, Set<String>>>,
) {
    /**
     * What ONE observer has asked for: themselves, plus each kind's delegated
     * signers. The anonymous read has no observer and so expands no declaration
     * at all — its caller passes [Enrolment.NONE] rather than a key.
     *
     * ONE OBSERVER, DELIBERATELY. This took a `Set` and unioned across it, so a
     * read carrying two `observer:` lenses unpacked any declaration EITHER had
     * enrolled — defensible for a client sending its own two points of view,
     * and wrong for anything else: a relay multiplexing two people's filters
     * into one store call would let B's trust service place rows on A's page,
     * when the entire premise of the gate is that A chose their services in A's
     * own 10040. An observer's service key may never unpack for another, so the
     * union is gone and [SearchReferenceExpansion] keys the gate by the lens
     * that found each pointer.
     */
    fun of(observer: String): Enrolment = Enrolment(setOf(observer), byObserver[observer] ?: emptyMap())

    companion object {
        val NONE = Delegations(emptyMap())

        /**
         * Both delegation shapes off every stored 10040, keyed by its owner.
         *
         * Derived in the SAME pass [ProviderMap] uses for its own projection:
         * one query for the 10040s, one parse each, two maps out. A second pass
         * would be a second place the signer→observer link is resolved, which
         * is the duplication this file exists to avoid.
         */
        fun delegationsOf(maps: List<TrustProviderListEvent>): Delegations {
            val out = LinkedHashMap<String, MutableMap<Int, MutableSet<String>>>()
            maps
                .forEach { map ->
                    val mine = out.getOrPut(map.pubKey) { LinkedHashMap() }
                    // The kind an entry names IS the delegation, in both shapes:
                    // a `30382:rank` service is appointed to rank users and
                    // nothing more, a bare `30393` publisher to curate lists of
                    // events and nothing more.
                    map.tags.serviceProviders().forEach { mine.getOrPut(it.service.kind) { LinkedHashSet() }.add(it.pubkey) }
                    TrustedListProviderTag.KINDS.forEach { kind ->
                        map.tags.trustedListProvider(kind)?.let { mine.getOrPut(kind) { LinkedHashSet() }.add(it.pubkey) }
                    }
                }
            return Delegations(out)
        }
    }
}

/**
 * ONE READ'S ANSWER: whose declaration of kind K this read may unpack.
 *
 * Immutable and cheap to hold, so a search resolves it once and asks it per row.
 */
internal class Enrolment(
    /** Admitted on EVERY kind — a reader's own declarations need no delegation. */
    private val observers: Set<String>,
    /** Delegated signers, keyed by the kind the Map entry appointed them for. */
    private val byKind: Map<Int, Set<String>>,
) {
    /**
     * Whether [pubKey]'s declaration of [kind] is one this read asked for.
     *
     * The kind is the DECLARATION's, not the subject's: a 30392 list of pubkeys
     * is admitted by a `30392` delegation, never by the `30382:rank` one that
     * appoints a service to make assertions about those same pubkeys. Two
     * different things to have asked for.
     *
     * A reader is always their own signer, on every kind: their own computations
     * need no delegation, and nobody publishes a Map appointing themselves.
     */
    fun admits(
        kind: Int,
        pubKey: String,
    ): Boolean = pubKey in observers || pubKey in (byKind[kind] ?: emptySet())

    /**
     * Every signer whose declaration of [kind] this read asked for — the
     * mirror of [admits], as a set: the authors a companion fetch for that
     * kind may name ([SearchReferenceExpansion.companions] is the caller).
     * Empty exactly when [admits] can admit nobody, which is the anonymous
     * read — so a companion built on this fetches nothing a gate would then
     * refuse to unpack.
     */
    fun signersOf(kind: Int): Set<String> = if (observers.isEmpty()) emptySet() else observers + (byKind[kind] ?: emptySet())

    companion object {
        /** The anonymous read: no observer, so no declaration of any kind. */
        val NONE = Enrolment(emptySet(), emptyMap())
    }
}
