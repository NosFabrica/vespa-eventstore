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

import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.serviceCells
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.mapping.toDoc
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals

/**
 * The one call that answers "why can this observer not find their own
 * profile". It exists because assembling those facts by hand took a dozen
 * queries, one silently broken and one truncated at ten groups.
 */
class TrustExplainTest {
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private val index = InMemoryEventIndex()
    private val reputations = InMemoryReputationIndex()
    private val projection = TrustProjection(index, reputations)
    private val explain = TrustExplain(index, reputations, projection.recompute)

    private fun list10040() =
        TrustProviderListEvent(
            id(),
            observer,
            1_000L + seq,
            arrayOf(arrayOf("30382:rank", service, "wss://s.example/"), arrayOf("30382:followers", service, "wss://s.example/")),
            "",
            "",
        )

    private fun card(about: String) = ContactCardEvent(id(), service, 1_000L + seq, arrayOf(arrayOf("d", about), arrayOf("rank", "87"), arrayOf("followers", "12")), "", "")

    private fun profile(of: String) = MetadataEvent(id(), of, 1_000L + seq, emptyArray(), """{"name":"n"}""", "")

    /**
     * THE STAGING CASE, EXACTLY. Everything present — profile, lens, cards,
     * a parent with cells — and still an empty ranked page, because none of
     * the cells is from the service this observer's own lens resolves to.
     * That took a dozen hand-built queries to establish; here it is one call.
     */
    @Test
    fun `names the lens with no cell as the reason, when everything else is present`() =
        runBlocking {
            index.put(profile(observer).toDoc())
            index.put(list10040().toDoc())
            index.put(card(observer).toDoc())
            // A parent rich in cells from OTHER services — never from this lens.
            reputations.put(ReputationDoc(observer, serviceCells("aa".repeat(32) to 90, "bb".repeat(32) to 80)))

            val e = explain.explain(observer)
            assertEquals(1, e.profiles, "the profile is present — an empty page is not a missing event")
            assertEquals(1, e.providerLists)
            assertEquals(service, e.rankService)
            assertEquals(2, e.influenceCells)
            assertEquals(null, e.rankCellFromLens, "and none of them is the lens's")
            assertContains(e.summary, "NONE from this observer's own lens")
        }

    /** The lens resolving and carrying its cell reads as healthy, so the line is usable as an all-clear. */
    @Test
    fun `says so when the lens resolves and the cell is there`() =
        runBlocking {
            index.put(profile(observer).toDoc())
            index.put(list10040().toDoc())
            index.put(card(observer).toDoc())
            reputations.put(ReputationDoc(observer, serviceCells(service to 87), serviceCells(service to 12.0)))

            val e = explain.explain(observer)
            assertEquals(87, e.rankCellFromLens)
            assertContains(e.summary, "should return this subject")
        }

    /** No lens at all is a different fact from a lens with no cell, and must not read the same. */
    @Test
    fun `an observer with no provider list is told that, not blamed on the projection`() =
        runBlocking {
            index.put(profile(observer).toDoc())
            val e = explain.explain(observer)
            assertEquals(0, e.providerLists)
            assertContains(e.summary, "no kind-10040")
        }
}
