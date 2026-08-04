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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.addressOrNull
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.owner
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent

/**
 * NIP-09 (deletion request) and NIP-62 (request to vanish) enforcement, plus
 * the paged [sweep] every bulk erase runs on. Every method must be called
 * under the store's writer lock — query-then-write is only atomic there.
 */
internal class Deletions(
    private val index: EventIndex,
    private val relay: NormalizedRelayUrl?,
    /**
     * Events removed per [sweep] round. A sweep re-runs its query until it
     * comes back empty, so this bounds how many matches are held at once, NOT
     * how many get deleted — without it a vanish over a prolific author would
     * materialize every doomed event in one list.
     */
    private val sweepPage: Int,
) {
    /**
     * NIP-09: a kind 5 authored by this event's OWNER, e/a-tagging it, with
     * created_at >= the event's, blocks the insert. Both target styles are
     * time-guarded.
     */
    suspend fun isDeleted(event: Event): Boolean {
        // Deletion requests and vanish requests are immune to kind-5 tombstones.
        if (event is DeletionEvent || event is RequestToVanishEvent) return false
        val owner = event.owner()

        suspend fun deletionExists(
            tagKey: String,
            value: String,
        ): Boolean = index.search(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = listOf(owner), tags = mapOf(tagKey to listOf(value)), since = event.createdAt, limit = 1)).isNotEmpty()
        if (deletionExists("e", event.id)) return true
        val address = event.addressOrNull() ?: return false
        return deletionExists("a", address)
    }

    /** NIP-62: a stored vanish request by this event's OWNER covering [relay] blocks their events up to its time. */
    suspend fun isVanished(event: Event): Boolean {
        val vanishes = index.search(EventQuery(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(event.owner()), since = event.createdAt))
        return vanishes.any { doc -> (doc.toEvent() as? RequestToVanishEvent)?.shouldVanishFrom(relay) == true }
    }

    /**
     * NIP-09 enforcement: erase this kind 5's targets — by id when the doc's
     * OWNER is the deletion author (a recipient deletes gift-wraps sent to
     * them), by address (addressable AND replaceable kinds) up to the
     * deletion's created_at, same author only. The caller stores the event
     * itself afterwards, as the tombstone.
     */
    suspend fun applyDeletion(ev: DeletionEvent) {
        // Deletions routinely carry dozens of e-tags: resolve them with
        // bounded-concurrent gets and remove the victims in ONE pipelined
        // removeDocs — which also hands the trust projection its batch react.
        val byId =
            ev
                .deleteEventIds()
                .distinct()
                .mapBounded(TARGET_GET_FANOUT) { index.get(it) }
                .filterNotNull()
                // Kind 5 against a kind 5 or a kind 62 has no effect.
                .filter { it.kind != DeletionEvent.KIND && it.kind != RequestToVanishEvent.KIND }
                .filter { it.owner == ev.pubKey }
        if (byId.isNotEmpty()) index.removeDocs(byId)
        for (address in ev.deleteAddresses()) {
            if (address.pubKeyHex != ev.pubKey) continue
            if (!address.kind.isAddressable() && !address.kind.isReplaceable()) continue
            val victims =
                index
                    .search(EventQuery(kinds = listOf(address.kind), authors = listOf(address.pubKeyHex), until = ev.createdAt))
                    // Replaceable kinds have ONE address regardless of the a-tag's d part.
                    .filter { !address.kind.isAddressable() || it.dTagOrEmpty() == address.dTag }
            if (victims.isNotEmpty()) index.removeDocs(victims)
        }
    }

    /**
     * NIP-62 enforcement: when the request covers [relay], erase the owner's
     * history "until its created_at" — INCLUSIVE, per the spec. The request
     * itself is only stored after this runs, so it survives its own sweep.
     */
    suspend fun applyVanish(ev: RequestToVanishEvent) {
        if (!ev.shouldVanishFrom(relay)) return
        sweep(EventQuery(owners = listOf(ev.pubKey), until = ev.createdAt))
    }

    /**
     * Remove every match, [sweepPage] at a time, until the query comes back
     * empty. No offset: each round re-runs the SAME query, and the removes
     * shrink the match set, so the next round naturally sees the next batch.
     * (Offset paging would be wrong — deleting under an offset skips rows.)
     */
    suspend fun sweep(q: EventQuery) {
        // The read is paged; the caller's own limit, if any, still decides
        // whether one page is the whole job, so read q.limit, not sweepPage.
        // Plain sweeps stamp RANK_UNRANKED: a sweep wants ANY page, so the
        // recency planner's count probes would answer a question it never
        // asked — the explicit ranking opts out while compiling to identical YQL.
        val paged =
            if (q.search == null && q.ranking == null) {
                q.copy(limit = q.limit ?: sweepPage, ranking = EventYql.RANK_UNRANKED)
            } else {
                q.copy(limit = q.limit ?: sweepPage)
            }
        var rounds = 0
        var lastPage: Set<String>? = null
        while (rounds++ < MAX_SWEEP_ROUNDS) {
            val page = index.search(paged)
            if (page.isEmpty()) return
            val ids = page.mapTo(HashSet()) { it.id }
            // An acked remove is visible to search (the EventIndex contract),
            // so a page identical to the one just removed means the deletes are
            // not landing. Fail NOW: silently returning would report a
            // vanish/delete as enforced while the events are still served.
            check(ids != lastPage) { "sweep is not shrinking: ${ids.size} matches for $q survived their own removal" }
            // The docs are already in hand — removeDocs lets the trust
            // projection react without a get per id.
            index.removeDocs(page)
            // A limit'd delete is satisfied by its first page.
            if (q.limit != null) return
            lastPage = ids
        }
        // Loud, not silent: the caller (a vanish, an expiry pass, an admin
        // delete) must not believe the sweep completed.
        error("sweep did not drain after $MAX_SWEEP_ROUNDS rounds of ${paged.limit} for $q")
    }

    private companion object {
        // Runaway backstop, not a delete cap: the non-shrinking case is caught
        // earlier, after ONE repeated page.
        const val MAX_SWEEP_ROUNDS = 10_000

        // Concurrent doc-API gets when resolving a kind 5's e-tag targets.
        // Gets are light, so this floats above QUERY_FANOUT.
        const val TARGET_GET_FANOUT = 16
    }
}
