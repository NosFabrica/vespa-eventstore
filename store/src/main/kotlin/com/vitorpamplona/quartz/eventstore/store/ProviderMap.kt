/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store

import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes

/**
 * The NIP-85 observer-attribution map: `service key -> observer`, derived from
 * every stored kind-10040's `30382:rank` entries. A 30382 is SIGNED by a service
 * key, but its score is credited to the OBSERVER: the 10040 author who named that
 * service. This is the one place that link is resolved.
 *
 * [get] is CACHED across a pass. The map only changes when a 10040 is written or
 * removed, so a run of single-30382 publishes (each re-deriving its subject) pays
 * the full 10040 scan ONCE, not per event. Every mutation path that touches a
 * 10040 [invalidate]s it. It is safe as a plain @Volatile field because every
 * caller runs under [TrustProjection]'s store single-writer lock.
 */
internal class ProviderMap(
    private val inner: EventIndex,
) {
    @Volatile private var cached: Map<String, String>? = null

    /**
     * The map, rebuilding it once per pass.
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
    suspend fun get(): Map<String, String> {
        cached?.let { return it }
        val fresh =
            rankProviders(inner.search(EventQuery(kinds = listOf(TrustProviderListEvent.KIND)))).toMap()
        if (fresh.isNotEmpty()) cached = fresh
        return fresh
    }

    /** Drop the cache; the next [get] rebuilds. Call after any 10040 write/remove. */
    fun invalidate() {
        cached = null
    }

    companion object {
        /** Every 10040 doc's `30382:rank` entries as `service key -> observer (the 10040 author)` pairs. */
        private fun rankProviders(listDocs: List<EventDoc>): List<Pair<String, String>> =
            listDocs
                .mapNotNull { Event.fromJsonOrNull(it.toEventJson()) as? TrustProviderListEvent }
                .flatMap { list -> list.serviceProviders().filter { it.service == ProviderTypes.rank }.map { it.pubkey to list.pubKey } }

        /** The distinct rank-service keys named across a batch of 10040 lists. */
        fun rankServicesOf(listDocs: List<EventDoc>): List<String> = rankProviders(listDocs).map { it.first }.distinct()
    }
}
