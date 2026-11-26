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

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.async.retryingSweep
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag

/**
 * ADDRESSES OWING A SWEEP: replaceable/addressable addresses where a write may
 * have landed a winner without removing the versions it replaced — the batch
 * or insert threw (the removal failed past its retries, the put failed part
 * way, the commit was cancelled) after its writes began.
 *
 * Why remember them: the caller re-offers what failed, and by then the winner
 * is stored, so the re-offer comes back a DUPLICATE (the bulk plan's dedup,
 * [EventAdmission]'s dup probe, the mixed replay) and never reaches supersession
 * again. The stale version then stays stored indefinitely: two versions served,
 * and a later kind 5 of the winner, or its expiry, RESURRECTS the stale one —
 * for a kind 10040, `newestPerObserver` would then promote a stale provider
 * list. So the write paths [owe] such an address when they throw, and when the
 * address is next written or re-offered as a duplicate, [settle] finishes the
 * sweep: every stored version but the NIP-01 newest is removed. A write that
 * supersedes the address itself sweeps every version it reads, and [clear]s it.
 *
 * IN MEMORY AND BOUNDED ([capacity], oldest forgotten first): a process restart
 * — or more failed addresses than fit — in that window still leaves the stale
 * version stored until the next write that supersedes the address, which
 * sweeps every version older than its winner (stage D's drift repair, the
 * default putIfNewer). That is the state before this class, now confined to a
 * crash in the window rather than to every failed sweep.
 *
 * Unused on an address-keyed index ([EventIndex.supersedesViaPut]): an address
 * is one document there, so no write can leave two versions.
 */
internal class OwedSweeps(
    private val capacity: Int = DEFAULT_CAPACITY,
) {
    /** Insertion-ordered, and [owe] re-inserts, so the oldest-owed address is forgotten first; guarded by itself. */
    private val owed = LinkedHashSet<Address>()

    /** How many addresses owe a sweep now (tests, telemetry). */
    val size: Int get() = synchronized(owed) { owed.size }

    fun owe(addresses: Collection<Address>) {
        if (addresses.isEmpty()) return
        synchronized(owed) {
            for (a in addresses) {
                owed.remove(a)
                owed.add(a)
            }
            while (owed.size > capacity) owed.remove(owed.first())
        }
    }

    /** These addresses were swept by a write that read every version: they owe nothing now. */
    fun clear(addresses: Collection<Address>) {
        if (addresses.isEmpty()) return
        synchronized(owed) { if (owed.isNotEmpty()) owed.removeAll(addresses.toSet()) }
    }

    /** Whether any of [events]' addresses owes a sweep — what makes an all-duplicates batch worth its commit. */
    fun owesAny(events: Collection<Event>): Boolean = synchronized(owed) { owed.isNotEmpty() && events.any { e -> addressOf(e)?.let { it in owed } == true } }

    private fun owedAmong(addresses: Collection<Address>): List<Address> = synchronized(owed) { if (owed.isEmpty()) emptyList() else addresses.filter { it in owed }.distinct() }

    /**
     * Finish the sweep of every owed address among [events]' addresses: read
     * its stored versions and remove all but the NIP-01 newest. Called under
     * the writer lock, after the caller's own writes. Usually free — nothing
     * owed — and otherwise one read per owed address, which happens only after
     * a failure. A sweep that fails again stays owed, and throws.
     */
    suspend fun settle(
        index: EventIndex,
        events: Collection<Event>,
    ) {
        if (index.supersedesViaPut) return
        val due = owedAmong(events.mapNotNull { addressOf(it) })
        for (address in due) {
            sweep(index, address)
            clear(listOf(address))
        }
    }

    /**
     * One address's sweep. FULL documents, as every removal the trust
     * projection reacts to needs them (a 10040's services, a card's subject),
     * and because this runs only after a failure.
     */
    private suspend fun sweep(
        index: EventIndex,
        address: Address,
    ) {
        val (kind, author, d) = address
        val base = EventQuery(kinds = listOf(kind), authors = listOf(author))
        val q = if (d.isNullOrEmpty()) base else base.copy(tags = mapOf("d" to listOf(d)))
        val versions = index.search(q).filter { addressOf(it) == address }
        val newest = versions.minWithOrNull(EventDoc.NEWEST_FIRST) ?: return
        val stale = versions.filter { it.id != newest.id }
        if (stale.isNotEmpty()) retryingSweep { index.removeDocs(stale) }
    }

    internal companion object {
        /** Addresses remembered at most. Each entry is a few hundred bytes; only failures add one. */
        const val DEFAULT_CAPACITY = 4_096

        /** The supersession key of [e] ((kind, author, d) — null d for a plain replaceable), or null for a regular event. */
        fun addressOf(e: Event): Address? =
            when {
                e.kind.isAddressable() -> Address(e.kind, e.pubKey, e.tags.dTag().orEmpty())
                e.kind.isReplaceable() -> Address(e.kind, e.pubKey, null)
                else -> null
            }

        /** [addressOf] for a stored document: its FIRST d tag, missing == empty, as the address rule reads it. */
        fun addressOf(doc: EventDoc): Address? =
            when {
                doc.kind.isAddressable() -> Address(doc.kind, doc.pubkey, doc.dTagOrEmpty())
                doc.kind.isReplaceable() -> Address(doc.kind, doc.pubkey, null)
                else -> null
            }
    }
}

/** A replaceable/addressable address: (kind, author, d) — null d for a plain replaceable. */
internal typealias Address = Triple<Int, String, String?>
