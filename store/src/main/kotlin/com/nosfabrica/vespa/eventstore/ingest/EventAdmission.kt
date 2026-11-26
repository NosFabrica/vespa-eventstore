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
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip40Expiration.isExpirationBefore
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
    /**
     * The STORE's clock, which NIP-40 admission reads — never quartz's
     * `TimeUtils.now()`. The expiry sweep's cutoff is read off the same clock,
     * and its termination argument ("nothing already due at the cutoff can be
     * admitted after it") holds only if admission and the sweep agree on what
     * "now" is (`NostrSemanticsStore.deleteExpiredEvents`).
     */
    private val nowSecs: () -> Long,
    /**
     * Addresses a failed write left owing a sweep ([OwedSweeps]) — the real
     * store's, shared with its bulk paths. Null for [BulkMixedInsert]'s replay,
     * whose snapshot is thrown away.
     */
    private val owed: OwedSweeps? = null,
) {
    /**
     * The rules, applied to one event. NO lock and NO lock accounting: the
     * caller holds whatever it needs — `NostrSemanticsStore` its writer lock,
     * [BulkMixedInsert] the real store's while it replays against a snapshot.
     */
    suspend fun admit(event: Event) {
        if (!settle(event)) admitStored(event)
    }

    /**
     * The verdicts that need NO I/O — ephemeral (accepted, never stored),
     * NIP-40 expired, text the engine cannot store — as an outcome, or null
     * when the event needs the store's reads. A property of the event alone,
     * so it is safe WITHOUT the writer lock, and `NostrSemanticsStore` asks it
     * before taking one: an ephemeral OK used to queue behind the lock's holder
     * (35-41 s measured on staging, see `WriteLocks`) to learn that nothing
     * would be written. Same precedence as the bulk planner's stage A.
     */
    fun localOutcome(event: Event): IEventStore.InsertOutcome? =
        when {
            event.kind.isEphemeral() -> IEventStore.InsertOutcome.Accepted

            event.isExpirationBefore(nowSecs()) -> IEventStore.InsertOutcome.Rejected(Rejections.EXPIRED)

            // Text the engine refuses is a property of the event, so it is
            // settled with the other no-I/O checks rather than surfacing as a
            // feed exception three round trips later. See [VespaText].
            VespaText.firstIllegalField(event) != null -> IEventStore.InsertOutcome.Rejected(Rejections.UNSTORABLE_TEXT)

            else -> null
        }

    /**
     * [localOutcome] in [admit]'s idiom: true when the event is accepted with
     * nothing to store, a [RejectedException] when it is rejected, false when
     * it needs [admitStored].
     */
    fun settle(event: Event): Boolean {
        val outcome = localOutcome(event) ?: return false
        if (outcome is IEventStore.InsertOutcome.Rejected) throw RejectedException(outcome.reason)
        return true
    }

    /**
     * The rules that READ THE STORE, for an event [settle] left undecided —
     * the half that needs the caller's writer lock. Not a public shortcut past
     * [settle]: an ephemeral event handed straight here would be stored.
     */
    suspend fun admitStored(event: Event) {
        // The admission reads — dedup, NIP-09 tombstone, NIP-62 vanish — are
        // independent, so fire them together and check in the original
        // precedence (duplicate > deleted > vanished). The dup read deliberately
        // stays a read: folding it into a conditional put was A/B-measured
        // 15-35% slower (see docs/server-side-constraints.md). Guard probes run
        // only when this owner HAS a stored tombstone/vanish (GuardOwners).
        val owner = event.owner()
        val probeDeleted = guards.mightBeDeleted(owner)
        val probeVanished = guards.mightHaveVanished(owner)
        if (!probeDeleted && !probeVanished) {
            if (supersedesBesideDupRead(event)) return
            // The common case reads just the dup get — skip the fan-out
            // machinery, which allocates per call.
            if (isStored(event)) duplicate(event)
        } else {
            coroutineScope {
                val existing = async { isStored(event) }
                val deleted = if (probeDeleted) async { deletions.isDeleted(event) } else null
                val vanished = if (probeVanished) async { deletions.isVanished(event) } else null
                if (existing.await()) duplicate(event)
                if (deleted?.await() == true) throw RejectedException(Rejections.DELETED)
                if (vanished?.await() == true) throw RejectedException(Rejections.VANISHED)
            }
        }
        // A guard's owner is noted BEFORE its write: a write that lands and then fails (a timeout
        // after the feed took it) is a stored guard all the same, and noted after, it was never
        // noted at all; its re-offer is a duplicate, which notes nothing. Over-flagging, for a
        // write that never landed, only costs the owner's events a probe ([GuardOwners]).
        when {
            event is DeletionRequestEvent -> {
                guards.writingGuards {
                    guards.noteDeletionStored(event.pubKey)
                    deletions.applyDeletion(event)
                    index.put(event.toDoc())
                }
            }

            event is RequestToVanishEvent -> {
                guards.writingGuards {
                    guards.noteVanishStored(event.pubKey)
                    deletions.applyVanish(event)
                    index.put(event.toDoc())
                }
            }

            // Replaceable/addressable newest-wins in ONE call: false == a
            // same-or-newer version holds the address, so this insert is
            // REPLACED (see EventIndex.putIfNewer, and Rejections.REPLACED for the OK false).
            //
            // A failure is remembered against the address ([OwedSweeps]): the
            // winner may have landed without its sweep, and its re-offer is a
            // duplicate. A win swept every version it read, so the address owes
            // nothing after it; a loss is a write to the address too, and pays.
            event.kind.isReplaceable() || event.kind.isAddressable() -> {
                replacedUnless(supersede(event), event)
            }

            else -> {
                index.put(event.toDoc())
            }
        }
    }

    /**
     * The dup probe: whether [event]'s id is stored. An EXISTENCE question, so it is asked as one
     * ([EventIndex.existingIds]), never as a document read: the engine answers one id with a GET
     * of its id field alone, where `get` brought the whole stored document back, search columns
     * and parsed tags included, under the writer lock, to answer one bit.
     */
    private suspend fun isStored(event: Event): Boolean = event.id in index.existingIds(listOf(event.id))

    /**
     * A plain replaceable/addressable event with no guard to probe: its dup GET
     * and its supersession ([supersede], whose version read is the round trip
     * that matters) run TOGETHER, so the insert waits for one round trip under
     * the writer lock instead of two. True when the event was decided here.
     *
     * The outcome is the sequential one's. A stored event can never win its own
     * address — it IS the incumbent, or an older version beside a newer one —
     * so a supersession that WROTE proves the event was not stored, and the
     * GET's answer (which may have read that very write) is moot; one that
     * wrote nothing leaves the GET to decide duplicate against `replaced:`, as
     * it would have decided first. Only where no guard is probed: a
     * guard's verdict must be in before anything is written. Not for the trust
     * kinds either, whose supersession opens crash insurance (a marker write)
     * that a duplicate card would pay for nothing.
     *
     * The price is the supersession's read (or the address-keyed conditional
     * put, refused) for an event that turns out to be a duplicate — which a
     * batch's dedup, not this path, is for.
     */
    private suspend fun supersedesBesideDupRead(event: Event): Boolean {
        if (!(event.kind.isReplaceable() || event.kind.isAddressable()) || event.kind in BulkRecordInsert.WHOLE_DOC_KINDS) return false
        val stored =
            coroutineScope {
                val existing = async { isStored(event) }
                val written = runCatching { supersede(event) }
                if (written.getOrNull() == true) {
                    existing.cancel()
                    return@coroutineScope true
                }
                if (existing.await()) duplicate(event)
                written.getOrThrow()
            }
        replacedUnless(stored, event)
        return true
    }

    /**
     * NIP-01 newest-wins in ONE call ([EventIndex.putIfNewer]); a failure is
     * remembered against the address ([OwedSweeps]), and a win swept every
     * version it read, so the address owes nothing after it.
     */
    private suspend fun supersede(event: Event): Boolean {
        val address = listOfNotNull(OwedSweeps.addressOf(event))
        val stored =
            try {
                index.putIfNewer(event.toDoc())
            } catch (t: Throwable) {
                owed?.owe(address)
                throw t
            }
        if (stored) owed?.clear(address)
        return stored
    }

    /** `replaced:` unless [stored] — a loss is a write to the address too, and pays what it owes ([OwedSweeps]). */
    private suspend fun replacedUnless(
        stored: Boolean,
        event: Event,
    ) {
        if (stored) return
        owed?.settle(index, listOf(event))
        throw RejectedException(Rejections.REPLACED)
    }

    /**
     * The duplicate verdict — after finishing a sweep the event's address owes
     * ([OwedSweeps]): a re-offer of a winner whose sweep failed is exactly a
     * duplicate, and the one write that will ever name that address again.
     */
    private suspend fun duplicate(event: Event): Nothing {
        owed?.settle(index, listOf(event))
        throw RejectedException(Rejections.DUPLICATE)
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
