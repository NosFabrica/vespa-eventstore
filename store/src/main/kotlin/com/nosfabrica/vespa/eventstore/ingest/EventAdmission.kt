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
package com.nosfabrica.vespa.eventstore.ingest

import com.nosfabrica.vespa.eventstore.RejectedException
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.mapping.VespaText
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.owner
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip40Expiration.isExpired
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope

/**
 * THE PER-EVENT WRITE RULES — dedup, NIP-09 / NIP-62 guards, NIP-01
 * supersession, NIP-40 expiry, ephemeral-never-stored — as one component,
 * holding no locks and owning no policy about when to take them.
 *
 * It is its own class because it has TWO callers that must not disagree:
 * `NostrSemanticsStore.insert` under the real writer lock, and
 * [BulkMixedInsert], which replays the same rules against an in-memory
 * snapshot to fold a batch containing deletions into one diff. That second
 * caller used to build a whole throwaway `NostrSemanticsStore` over the
 * snapshot — a package cycle, and a store with its own mutexes, guard cache
 * and metrics ledger where three collaborators were wanted. Same rules, same
 * code, no cycle.
 *
 * Rejections are SEMANTIC and typed ([Rejections]); a transient engine failure
 * propagates instead, because swallowing it would silently drop a valid event.
 */
internal class EventAdmission(
    private val index: EventIndex,
    private val deletions: Deletions,
    /** Whether this owner can have a stored tombstone/vanish at all — see [GuardOwners]. */
    private val guards: GuardOwners,
) {
    /**
     * The rules, applied to one event. NO lock and NO lock accounting: the
     * caller holds whatever it needs — `NostrSemanticsStore` its writer lock,
     * [BulkMixedInsert] the real store's while it replays against a snapshot.
     */
    suspend fun admit(event: Event) {
        if (event.kind.isEphemeral()) return
        if (event.isExpired()) throw RejectedException(Rejections.EXPIRED)
        // Text the engine refuses is a property of the event, so it is settled
        // here with the other no-I/O checks rather than surfacing as a feed
        // exception three round trips later. See [VespaText].
        if (VespaText.firstIllegalField(event) != null) throw RejectedException(Rejections.UNSTORABLE_TEXT)
        // The admission reads — dedup, NIP-09 tombstone, NIP-62 vanish — are
        // independent, so fire them together and check in the original
        // precedence (duplicate > deleted > vanished). The dup GET deliberately
        // stays a read: folding it into a conditional put was A/B-measured
        // 15-35% slower (see docs/server-side-constraints.md). Guard probes run
        // only when this owner HAS a stored tombstone/vanish (GuardOwners).
        val owner = event.owner()
        val probeDeleted = guards.mightBeDeleted(owner)
        val probeVanished = guards.mightHaveVanished(owner)
        if (!probeDeleted && !probeVanished) {
            // The common case reads just the dup get — skip the fan-out
            // machinery, which allocates per call.
            if (index.get(event.id) != null) throw RejectedException(Rejections.DUPLICATE)
        } else {
            coroutineScope {
                val existing = async { index.get(event.id) }
                val deleted = if (probeDeleted) async { deletions.isDeleted(event) } else null
                val vanished = if (probeVanished) async { deletions.isVanished(event) } else null
                if (existing.await() != null) throw RejectedException(Rejections.DUPLICATE)
                if (deleted?.await() == true) throw RejectedException(Rejections.DELETED)
                if (vanished?.await() == true) throw RejectedException(Rejections.VANISHED)
            }
        }
        when {
            event is DeletionEvent -> {
                deletions.applyDeletion(event)
                index.put(event.toDoc())
                guards.noteDeletionStored(event.pubKey)
            }

            event is RequestToVanishEvent -> {
                deletions.applyVanish(event)
                index.put(event.toDoc())
                guards.noteVanishStored(event.pubKey)
            }

            // Replaceable/addressable newest-wins in ONE call: false == a
            // same-or-newer version holds the address, so this insert is
            // REPLACED (see EventIndex.putIfNewer).
            event.kind.isReplaceable() || event.kind.isAddressable() -> {
                if (!index.putIfNewer(event.toDoc())) throw RejectedException(Rejections.REPLACED)
            }

            else -> {
                index.put(event.toDoc())
            }
        }
    }

    /** [admit], with a SEMANTIC rejection reported as an outcome rather than thrown. */
    suspend fun tryAdmit(event: Event): IEventStore.InsertOutcome =
        try {
            admit(event)
            IEventStore.InsertOutcome.Accepted
        } catch (e: RejectedException) {
            // Only a SEMANTIC rejection becomes a Rejected outcome. A transient
            // engine failure must PROPAGATE — swallowing it would silently DROP
            // a valid event and let the sync cursor advance past it.
            IEventStore.InsertOutcome.Rejected(e.message ?: Rejections.INSERT_FAILED)
        }
}
