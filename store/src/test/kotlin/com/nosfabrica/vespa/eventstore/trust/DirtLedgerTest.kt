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

import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.ReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The ledger's bookkeeping under a drain that runs BESIDE writes — the
 * deferred mode of production, where the drain holds only the trust gate and
 * the writes it interleaves with hold their own locks or none. The [DirtLedger.drain]
 * gate is injectable, so a test can put a write exactly where a live one would
 * land: after a subject's slice has been derived, before the round is over.
 */
class DirtLedgerTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private val subject = "ab".repeat(32)

    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun list10040() = TrustProviderListEvent(id(), observer, next(), arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")

    private fun card(rank: Int) = ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString())), "", "")

    private fun note() = MetadataEvent(id(), "cc".repeat(32), next(), emptyArray(), """{"name":"n"}""", "")

    private class Deferred {
        val reputations = InMemoryReputationIndex()
        val projection = TrustProjection(InMemoryEventIndex(), reputations)

        init {
            projection.dirt.deferTo { }
        }

        suspend fun rankOf(subject: String) =
            reputations
                .get(subject)
                ?.influenceScores
                ?.values
                ?.singleOrNull()

        suspend fun marker() = reputations.get(DirtLedger.MARKER_KEY)
    }

    /**
     * THE MID-DRAIN CARD. The old ledger subtracted the round's snapshot at the
     * end, so a subject re-dirtied while its own slice was already derived was
     * subtracted along with the snapshot — marker rewritten clean, newer card
     * never projected, and the drain returned with the OLD rank served. The
     * gate lambda here is the live writer: it lands the card right after the
     * slice that derived the subject, and the drain's next round must find it.
     */
    @Test
    fun `a card written after its subject's slice was derived is projected by the next round`() =
        runBlocking {
            val d = Deferred()
            d.projection.put(list10040().toDoc())
            // The provider list is drained FIRST, so the round under test holds
            // the subject alone: with the service still dirty, its walk (which
            // runs after the subject slices) would re-derive the subject from
            // the newer card by coincidence and hide the ledger's loss.
            d.projection.dirt.drain { it() }
            d.projection.put(card(40).toDoc())
            assertNull(d.rankOf(subject), "deferred: nothing projected yet")

            var landed = false
            d.projection.dirt.drain { body ->
                body()
                // Once, after the FIRST slice: the derive just wrote rank 40 from
                // the cards it read; this card post-dates that read.
                if (!landed && d.rankOf(subject) == 40) {
                    landed = true
                    d.projection.put(card(90).toDoc())
                    // Covered on disk from the moment it is written: the marker
                    // is only narrowed once a round completes.
                    assertTrue(subject in assertNotNull(d.marker()).influenceScores, "the marker names the re-dirtied subject")
                }
            }
            assertTrue(landed, "the interleaved card was written mid-drain")
            // The drain loops until a round finds nothing, so the newer card is
            // projected before it returns — not lost to the snapshot it was in.
            assertEquals(90, d.rankOf(subject), "the next round projected the newer card")
            assertNull(d.marker(), "and then the ledger is clean")
        }

    /**
     * THE KIND-1 BESIDE THE DRAIN. A plain note's [DirtLedger.guarded] adds no
     * work; it must also not WRITE BACK the dirt it read, or a drain that took
     * that dirt out in between is undone in memory — after which the next card
     * for that subject finds its subject "already pending", persists no
     * write-ahead, and a crash before the drain loses it. Reproduced by
     * pinning the note's read before the drain and its write after.
     */
    @Test
    fun `a plain write interleaved with a drain does not resurrect drained dirt`() =
        runBlocking {
            val d = Deferred()
            d.projection.put(list10040().toDoc())
            d.projection.put(card(40).toDoc())

            // The note's guarded() runs to completion on its own; the
            // interleaving is created by draining INSIDE its block, between
            // the ledger read (before the block) and the write (after it).
            d.projection.dirt.guarded(DirtLedger.Dirt.NONE) {
                d.projection.dirt.drain { it() }
                assertEquals(40, d.rankOf(subject))
                assertNull(d.marker(), "the drain rewrote the marker clean")
                d.projection.put(note().toDoc()) // the plain write itself
                Unit to DirtLedger.Dirt.NONE
            }

            // A clean ledger: a fresh card must persist its own write-ahead.
            // Under the lost-update bug `before` still named the subject, the
            // delta was empty and nothing was persisted.
            d.projection.put(card(90).toDoc())
            val marker = d.marker()
            assertNotNull(marker, "the card's write-ahead was persisted")
            assertTrue(subject in marker.influenceScores)
            d.projection.dirt.drain { it() }
            assertEquals(90, d.rankOf(subject))
        }

    /**
     * A BULK write-ahead mid-round rewrites the marker as a whole document
     * (past DELTA_ADD_MAX the write-ahead is one put, not cell adds). The
     * round's own snapshot must survive that rewrite: a version of this ledger
     * that TOOK its snapshot out of memory computed the write-ahead without
     * it, so the marker stopped naming the subjects still being derived — a
     * crash before the round's final rewrite would have lost them for good.
     */
    @Test
    fun `a bulk write-ahead during a round keeps the marker covering the round's subjects`() =
        runBlocking {
            val d = Deferred()
            d.projection.put(list10040().toDoc())
            d.projection.dirt.drain { it() }
            d.projection.put(card(40).toDoc())

            var landed = false
            d.projection.dirt.drain { body ->
                body()
                if (!landed && d.rankOf(subject) == 40) {
                    landed = true
                    // 100 cards for OTHER subjects: a delta past DELTA_ADD_MAX,
                    // persisted as one marker-doc put.
                    d.projection.putAll((1..100).map { i -> ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", i.toString(16).padStart(64, 'e')), arrayOf("rank", "1")), "", "").toDoc() })
                    val marker = assertNotNull(d.marker(), "the marker still stands")
                    assertTrue(subject in marker.influenceScores, "the marker still names the subject the round is deriving")
                }
            }
            assertTrue(landed)
            assertNull(d.marker(), "everything drained in the end")
        }

    /**
     * INSURANCE THAT PRODUCED NO WORK must not leave the marker standing. A
     * batch of cards by a signer no 10040 maps insures every subject (one
     * marker put past DELTA_ADD_MAX) and then writes no cells; with nothing
     * pending no drain ever rewrote the marker, so the next boot inherited
     * every one of those subjects as drift to re-derive to nothing.
     */
    @Test
    fun `a batch that insures subjects but leaves no work clears its marker`() =
        runBlocking {
            val d = Deferred()
            // No 10040 at all: every card is by an unmapped signer.
            d.projection.putAll((1..100).map { i -> ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", i.toString(16).padStart(64, 'a')), arrayOf("rank", "1")), "", "").toDoc() })
            assertNull(d.marker(), "no work was left, so no marker stands")
            // The per-event path queues its subject as WORK (the bulk path is
            // the one that applies cells inline), so its marker stands until
            // the drain re-derives it — and is then gone.
            d.projection.put(card(40).toDoc())
            assertNotNull(d.marker(), "a single card's subject is queued work, and the marker names it")
            d.projection.dirt.drain { it() }
            assertNull(d.marker())
        }

    /** A failed round leaves the snapshot pending in memory as well as on disk, so the retry re-derives it. */
    @Test
    fun `a failed drain puts its snapshot back`() =
        runBlocking {
            val d = Deferred()
            d.projection.put(list10040().toDoc())
            d.projection.put(card(40).toDoc())
            assertFailsWith<IllegalStateException> {
                d.projection.dirt.drain { error("engine down") }
            }
            assertNotNull(d.marker(), "the marker survived the failure")
            d.projection.dirt.drain { it() }
            assertEquals(40, d.rankOf(subject), "the retry derived the snapshot")
            assertNull(d.marker())
        }

    /** A previous process's marker is discovered by the first drain and healed with the provider map dropped. */
    @Test
    fun `an inherited marker is drained once and cleared`() =
        runBlocking {
            val index = InMemoryEventIndex()
            val reputations: ReputationIndex = InMemoryReputationIndex()
            val first = TrustProjection(index, reputations)
            first.put(list10040().toDoc())
            first.put(card(40).toDoc())
            // The crashed process: its marker names the subject, its projection never ran.
            reputations.remove(subject)
            reputations.put(ReputationDoc(DirtLedger.MARKER_KEY, mapOf(subject to 1), emptyMap()))

            val restarted = TrustProjection(index, reputations)
            restarted.dirt.deferTo { }
            restarted.dirt.drain { it() }
            assertEquals(mapOf(observer to 40), reputations.get(subject)?.influenceScores)
            assertNull(reputations.get(DirtLedger.MARKER_KEY))
        }
}
