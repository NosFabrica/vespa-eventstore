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
package com.vitorpamplona.quartz.eventstore.store.trust

import com.vitorpamplona.quartz.eventstore.store.mapping.toEvent
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes

/**
 * The NIP-85 attribution maps, PER DIMENSION: which observers named a service
 * for which tensor. A 10040 entry is typed — `30382:rank` picks a provider for
 * influence scores, `30382:followers` one for follower counts — and a user may
 * pick DIFFERENT services for the two. A card's tag therefore only counts for
 * the observers who named its signer for THAT tag's dimension: a rank
 * provider's `followers` tag must not overwrite the value the user's chosen
 * follower provider asserts (and vice versa).
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
 * The NIP-85 observer-attribution maps ([TrustProviders]), derived from every
 * stored kind-10040's `30382:rank` and `30382:followers` entries. A 30382 is
 * SIGNED by a service key, but its scores are credited to the OBSERVERS: every
 * 10040 author who named that service — for the dimension they named it for.
 * This is the one place that link is resolved.
 *
 * Each map's value is a SET, not one observer. Popular providers are the norm
 * on NIP-85 — many users' 10040s name the same service — and each of those
 * users must see the service's scores under their own key. (A `toMap()` here
 * once kept a single arbitrary winner per service, which silently unranked
 * every other user trusting that provider.)
 *
 * [get] is CACHED across a pass. The maps only change when a 10040 is written or
 * removed, so a run of single-30382 publishes (each re-deriving its subject) pays
 * the full 10040 scan ONCE, not per event. Every mutation path that touches a
 * 10040 [invalidate]s it. It is safe as a plain @Volatile field because every
 * caller that can observe a stale value runs under [TrustProjection]'s store
 * single-writer lock (the reconciler's mutating passes take the same lock
 * through its gate).
 */
internal class ProviderMap(
    private val inner: EventIndex,
    private val nowSecs: () -> Long,
) {
    @Volatile private var cached: TrustProviders? = null

    /**
     * The maps, rebuilding them once per pass. Already-expired 10040s (NIP-40)
     * are excluded, matching what every read path would serve — a mapping the
     * store refuses to return as a record must not keep attributing scores. (The
     * cells it produced still stand until the expiry sweep removes the list and
     * fires the projection's react.)
     *
     * An EMPTY result is never cached, and that exception is the whole point. A
     * relay with no 10040s and a relay whose engine has not finished serving its
     * corpus return the identical empty list, and caching it makes the second
     * one permanent: [invalidate] fires only on a 10040 WRITE, and a 10040 the
     * store already holds is dropped by dedup before any write happens. A relay
     * that mirrored its corpus before its 10040s — or simply asked one second too
     * early — then serves empty rankings forever, with a cache that never looks
     * again. Observed: 271 kind-10040s queryable at full coverage while ten
     * minutes of reconcile retries all read the same cached emptiness.
     *
     * The cost of not caching it is one small query per pass on a relay that
     * genuinely has no providers. The cost of caching it is a relay that can
     * never rank anything until it restarts.
     */
    suspend fun get(): TrustProviders {
        cached?.let { return it }
        val fresh = providersOf(inner.search(EventQuery(kinds = listOf(TrustProviderListEvent.KIND), notExpiredAt = nowSecs())))
        if (!fresh.isEmpty()) cached = fresh
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
