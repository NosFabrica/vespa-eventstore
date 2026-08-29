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
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes

/**
 * The NIP-85 attribution maps, PER DIMENSION: which observers named a service
 * for which tensor (`30382:rank` vs `30382:followers`). A user may pick
 * DIFFERENT services for the two, so a card's tag only counts for observers
 * who named its signer for THAT dimension — a rank provider's followers tag
 * must not overwrite the chosen follower provider's value.
 */
internal data class TrustProviders(
    /** rank service key -> observers whose 10040 names it under `30382:rank`. */
    val rank: Map<String, Set<String>>,
    /** followers service key -> observers whose 10040 names it under `30382:followers`. */
    val followers: Map<String, Set<String>>,
) {
    fun isEmpty() = rank.isEmpty() && followers.isEmpty()

    /** Whether [service] is mapped for either dimension — its cards can project. */
    fun maps(service: String) = rank.containsKey(service) || followers.containsKey(service)

    /** Every mapped service key, either dimension. */
    val services: Set<String> get() = rank.keys + followers.keys
}

/**
 * Derives [TrustProviders] from every stored kind-10040 — the one place the
 * signer->observer link is resolved: a 30382 is SIGNED by a service key but
 * credited to the observers who named that service. Each map's value is a SET:
 * popular providers are the NIP-85 norm, and each naming user must see the
 * scores under their own key (a `toMap()` here once kept one arbitrary winner
 * per service, silently unranking everyone else trusting it).
 *
 * [get] is CACHED across a pass — the maps only change on a 10040
 * write/removal, each of which [invalidate]s. Safe as a plain @Volatile field
 * because every caller that can observe a stale value runs under the store's
 * single-writer lock (the reconciler's mutating passes take it via gate).
 */
internal class ProviderMap(
    private val inner: EventIndex,
    private val nowSecs: () -> Long,
) {
    /**
     * Both projections of one pass. The write side wants service -> observers
     * per tensor; the READ side wants observer -> signers per kind, to gate
     * which declarations a search may unpack ([Delegations]). Same query, same
     * parse, same invalidation — deriving them separately would be a second
     * place the signer/observer link is resolved, and the two would drift the
     * first time a Map shape changed.
     */
    private class Pass(
        val trust: TrustProviders,
        val delegations: Delegations,
    )

    @Volatile private var cached: Pass? = null

    /**
     * The maps, rebuilt once per pass. Already-expired 10040s (NIP-40) are
     * excluded, matching every read path — a list not served as a record must
     * not keep attributing (its cells stand until the expiry sweep fires the
     * projection's react).
     *
     * An EMPTY result is never cached. A relay with no 10040s and one whose
     * engine has not finished serving its corpus return the same empty list, and
     * caching it makes the second permanent: [invalidate] fires only on a 10040
     * WRITE, and dedup drops an already-held 10040 before any write (observed:
     * 271 queryable 10040s while ten minutes of reconcile retries read the same
     * cached emptiness). Not caching costs one small query per pass on a
     * genuinely providerless relay.
     */
    suspend fun get(): TrustProviders = pass().trust

    /**
     * The read-time gate, off the same pass. See [Delegations] for what a Map
     * entry's kind means and why a named `3039x:<name>` entry is not admitted.
     */
    suspend fun delegations(): Delegations = pass().delegations

    private suspend fun pass(): Pass {
        cached?.let { return it }
        val docs = inner.search(EventQuery(kinds = listOf(TrustProviderListEvent.KIND), notExpiredAt = nowSecs()))
        val fresh = Pass(providersOf(docs), Delegations.delegationsOf(docs))
        // Emptiness is judged on the DOCUMENTS, not on either projection: the
        // ambiguity this rule records is "no 10040s" versus "the engine has not
        // finished serving them", and only the query's own answer distinguishes
        // those. Judging it on the trust half instead would also refuse to cache
        // a relay whose Maps carry nothing but bare-kind Trusted List entries —
        // a real shape, and one with no rank or followers provider in it.
        if (docs.isNotEmpty()) cached = fresh
        return fresh
    }

    /** Drop the cache; the next [get] rebuilds. Call after any 10040 write/remove. */
    fun invalidate() {
        cached = null
    }

    companion object {
        /** Both dimensions' `service key -> observers` maps from [listDocs]' typed entries. */
        fun providersOf(listDocs: List<EventDoc>): TrustProviders {
            val rank = LinkedHashMap<String, MutableSet<String>>()
            val followers = LinkedHashMap<String, MutableSet<String>>()
            listDocs
                .mapNotNull { it.toEvent() as? TrustProviderListEvent }
                .forEach { list ->
                    list.serviceProviders().forEach { entry ->
                        when (entry.service) {
                            ProviderTypes.rank -> rank.getOrPut(entry.pubkey) { LinkedHashSet() }.add(list.pubKey)
                            ProviderTypes.followerCount -> followers.getOrPut(entry.pubkey) { LinkedHashSet() }.add(list.pubKey)
                        }
                    }
                }
            return TrustProviders(rank, followers)
        }

        /** The distinct service keys (either dimension) named across a batch of 10040 lists. */
        fun trustServicesOf(listDocs: List<EventDoc>): List<String> = providersOf(listDocs).services.toList()
    }
}
