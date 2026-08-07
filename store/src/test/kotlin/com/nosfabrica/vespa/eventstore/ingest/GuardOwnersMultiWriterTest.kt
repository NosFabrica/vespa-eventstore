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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.RejectedException
import com.nosfabrica.vespa.eventstore.Rejections
import com.nosfabrica.vespa.eventstore.WriterTopology
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * TWO STORES OVER ONE INDEX — the topology the guard-owner cache's original
 * safety argument excluded, and the one a relay deployment actually runs (a
 * serving relay plus a sync router feeding the same Vespa, split by role, so
 * both touch the same authors).
 *
 * The failure it guards against: instance A stores a kind 5, only A's bloom
 * learns the author, and B — whose sets were loaded once, lazily — proves to
 * itself that the author has no tombstone, skips both admission probes, and
 * admits an event the tombstone covers. Nothing converges: the miss lasts as
 * long as B's process. It needs no concurrency; the two writes can be hours
 * apart.
 *
 * So these tests pin what each [WriterTopology] promises about a SECOND
 * writer's guards, and that a rebuild is union-only — a refresh that dropped a
 * guard noted while it was scanning would re-introduce the same false negative
 * it exists to close.
 */
class GuardOwnersMultiWriterTest {
    private val relay = "wss://sot.test/".normalizeRelayUrl()

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun pk(tag: String) = tag.repeat(32).take(64)

    private fun note(
        author: String,
        id: String = id(),
    ) = Event(id, author, 1_000, 1, emptyArray(), "hi", "")

    private fun tombstone(
        author: String,
        vararg targets: String,
    ) = DeletionEvent(id(), author, 2_000, targets.map { arrayOf("e", it) }.toTypedArray(), "", "")

    /**
     * The acceptance case, made deterministic by the explicit barrier: the
     * tombstone A stored must block B's insert once B rebuilds. Before the
     * rebuild existed there was no call that could make this pass — B's answer
     * was "accepted", forever.
     */
    @Test
    fun aSecondWritersTombstoneBlocksAfterARefresh() =
        runBlocking {
            val index = InMemoryEventIndex()
            val a = NostrSemanticsStore(index, relay = relay)
            val b = NostrSemanticsStore(index, relay = relay, writers = WriterTopology.SHARED)
            val author = pk("a1")
            val covered = note(author)

            // Force b's guard load FIRST, so its sets predate a's tombstone —
            // the lazy load is what makes the miss permanent.
            b.insert(note(pk("c1")))
            a.insert(tombstone(author, covered.id))

            b.refreshGuardOwners()

            val rejected = assertFailsWith<RejectedException> { b.insert(covered) }
            assertEquals(Rejections.DELETED, rejected.message, "b admitted an event a's tombstone covers")
            assertNull(index.get(covered.id), "the covered event reached the index")
        }

    /**
     * The same, left to the background refresher: staleness is the interval,
     * not the process lifetime. The interval is tiny here; production cadence
     * is minutes (see DEFAULT_GUARD_REFRESH_MILLIS).
     *
     * Each attempt uses a fresh covered event because the first attempts are
     * expected to SUCCEED — that admission is the very bug, and re-offering an
     * admitted event would come back "duplicate" instead of "deleted".
     */
    @Test
    fun aSecondWritersTombstoneBlocksWithinARefreshInterval() =
        runBlocking {
            val index = InMemoryEventIndex()
            val a = NostrSemanticsStore(index, relay = relay)
            val b = NostrSemanticsStore(index, relay = relay, writers = WriterTopology.SHARED, guardRefreshMillis = 20)
            val author = pk("a2")
            val covered = (0 until 200).map { note(author) }

            b.insert(note(pk("c2")))
            a.insert(tombstone(author, *covered.map { it.id }.toTypedArray()))

            withTimeout(30_000) {
                var blocked = false
                for (event in covered) {
                    val outcome = runCatching { b.insert(event) }
                    if (outcome.exceptionOrNull()?.message == Rejections.DELETED) {
                        blocked = true
                        break
                    }
                    outcome.getOrThrow() // anything but DELETED/accepted is a real failure
                    delay(20)
                }
                assertTrue(blocked, "the refresher never picked up the second writer's tombstone")
            }
        }

    /**
     * THE DEFAULT, pinned as a correctness property rather than a config
     * detail: a caller who says nothing about writers must not be silently
     * trading "a deleted event is never served" for read capacity. Constructed
     * with no `writers` argument on purpose — if the default ever drifts back
     * to a caching mode, this fails.
     */
    @Test
    fun theDefaultTopologyBlocksImmediately() =
        runBlocking {
            val index = InMemoryEventIndex()
            val a = NostrSemanticsStore(index, relay = relay)
            val b = NostrSemanticsStore(index, relay = relay)
            val author = pk("a6")
            val covered = note(author)

            b.insert(note(pk("c6")))
            a.insert(tombstone(author, covered.id))

            val rejected = assertFailsWith<RejectedException> { b.insert(covered) }
            assertEquals(Rejections.DELETED, rejected.message, "the DEFAULT topology served a deleted event")
            assertNull(index.get(covered.id))
        }

    /**
     * The same floor named explicitly — what `GUARD_OWNERS_DISABLE=1` forces,
     * and the behaviour a deployment must not lose by moving off that switch.
     */
    @Test
    fun strictTopologyBlocksImmediately() =
        runBlocking {
            val index = InMemoryEventIndex()
            val a = NostrSemanticsStore(index, relay = relay)
            val b = NostrSemanticsStore(index, relay = relay, writers = WriterTopology.SHARED_STRICT)
            val author = pk("a3")
            val covered = note(author)

            b.insert(note(pk("c3")))
            a.insert(tombstone(author, covered.id))

            val rejected = assertFailsWith<RejectedException> { b.insert(covered) }
            assertEquals(Rejections.DELETED, rejected.message)
        }

    /**
     * SINGLE_WRITER is an assertion, and this pins what it buys: no rebuilds,
     * so the loaded sets stand for the process's lifetime. The second writer
     * here VIOLATES that assertion, and the admission below is the documented
     * consequence — the reason the default is [WriterTopology.SHARED] and the
     * process-lifetime cache is something a deployment opts into.
     */
    @Test
    fun singleWriterKeepsItsLoadedSetsForTheProcessLifetime() =
        runBlocking {
            val index = InMemoryEventIndex()
            val a = NostrSemanticsStore(index, relay = relay)
            val b = NostrSemanticsStore(index, relay = relay, writers = WriterTopology.SINGLE_WRITER, guardRefreshMillis = 20)
            val author = pk("a4")
            val covered = note(author)

            b.insert(note(pk("c4")))
            a.insert(tombstone(author, covered.id))
            delay(200) // several refresh intervals, if one were running

            b.insert(covered) // admitted: no rebuild ever runs in this mode
            assertTrue(index.get(covered.id) != null)

            // The explicit barrier still works — it is the mode's manual repair.
            b.refreshGuardOwners()
            val next = note(author)
            a.insert(tombstone(author, next.id))
            b.refreshGuardOwners()
            assertEquals(Rejections.DELETED, assertFailsWith<RejectedException> { b.insert(next) }.message)
        }

    /**
     * The rebuild is UNION-ONLY. A guard stored while the scan is in flight can
     * be invisible to that scan (it may already have paged past the author) and
     * lands in blooms the swap is about to discard — so it is buffered and
     * folded in under the same lock that publishes the replacement. Losing it
     * would be a false negative introduced BY the fix.
     */
    @Test
    fun aGuardNotedDuringARebuildSurvivesTheSwap() =
        runBlocking {
            val index = GatedScanIndex(InMemoryEventIndex())
            val guards = GuardOwners(index, WriterTopology.SHARED, refreshMillis = 0)
            val author = pk("a5")

            assertFalse(guards.mightBeDeleted(author)) // initial load, ungated

            val scanning = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            index.scanning = scanning
            index.hold = release

            val rebuild = launch { guards.refresh() }
            scanning.await() // the rebuild has taken its view of the corpus
            guards.noteDeletionStored(author) // ...and only now is the guard stored
            release.complete(Unit)
            rebuild.join()

            assertTrue(guards.mightBeDeleted(author), "the swap dropped a guard noted during the rebuild")
        }

    /**
     * The switch reaches operators as a documented string, and it was silently
     * inert once — the docs said `=1` while the parse returned null for "1", so
     * every deployment that followed the instructions kept the cache. Anything
     * it cannot read now fails loudly instead of reading as "cache on".
     */
    @Test
    fun theDisableSwitchParsesItsDocumentedValuesAndRefusesTheRest() {
        listOf("1", "true", "TRUE", "yes", "on", " 1 ").forEach {
            assertEquals(WriterTopology.SHARED_STRICT, GuardOwners.parseDisable(it), "$it did not disable the cache")
        }
        listOf(null, "", "0", "false", "no", "off").forEach {
            assertNull(GuardOwners.parseDisable(it), "$it wrongly disabled the cache")
        }
        listOf("maybe", "2", "trve").forEach {
            assertFailsWith<IllegalArgumentException>("$it was accepted") { GuardOwners.parseDisable(it) }
        }
    }

    /** Lets a test hold [scanAuthors] open AFTER it has read the corpus, so a write can land mid-rebuild. */
    private class GatedScanIndex(
        private val inner: EventIndex,
    ) : EventIndex by inner {
        @Volatile var hold: CompletableDeferred<Unit>? = null

        @Volatile var scanning: CompletableDeferred<Unit>? = null

        override suspend fun scanAuthors(query: EventQuery): Set<String> {
            val snapshot = inner.scanAuthors(query)
            scanning?.complete(Unit)
            hold?.await()
            return snapshot
        }
    }
}
