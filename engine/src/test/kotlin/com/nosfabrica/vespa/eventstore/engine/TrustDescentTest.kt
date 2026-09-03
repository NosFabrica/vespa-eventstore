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
package com.nosfabrica.vespa.eventstore.engine

import com.nosfabrica.vespa.eventstore.engine.client.TrustDescent
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * THE BOUND IS ONLY A BOUND IF ITS CONSTANTS ARE THE SCHEMA'S. Every weight
 * the `search` profile multiplies into a score is mirrored in [TrustDescent]
 * and read back here from the bundled event.sd — so a retuned rung fails
 * this test until the mirror moves with it, instead of quietly serving a
 * page the bound no longer covers.
 */
class TrustDescentTest {
    private val schema: String by lazy {
        ZipInputStream(ByteArrayInputStream(VespaApp.zipBytes())).use { zip ->
            generateSequence { zip.nextEntry }.first { it.name.endsWith("schemas/event.sd") }
            zip.readBytes().decodeToString()
        }
    }

    private fun input(name: String): Double {
        val m = Regex("""query\($name\) double: ([0-9.]+)""").find(schema) ?: error("event.sd declares no query($name)")
        return m.groupValues[1].toDouble()
    }

    @Test
    fun `the mirrored weights are the schema's`() {
        assertEquals(input("w_name_tier"), TrustDescent.W_NAME_TIER)
        assertEquals(input("w_split_tier"), TrustDescent.W_SPLIT_TIER)
        assertEquals(input("w_near_tier"), TrustDescent.W_NEAR_TIER)
        assertEquals(input("w_weak_tier"), TrustDescent.W_WEAK_TIER)
        assertEquals(input("w_perfect_tier"), TrustDescent.W_PERFECT_TIER)
        assertEquals(input("w_affil_tier_text"), TrustDescent.W_AFFIL_TIER_TEXT)
        assertEquals(input("gram_cap"), TrustDescent.GRAM_CAP)
        assertEquals(input("w_identity"), TrustDescent.W_IDENTITY)
        assertEquals(input("w_secondary"), TrustDescent.W_SECONDARY)
        assertEquals(input("w_content"), TrustDescent.W_CONTENT)
        assertEquals(input("w_location"), TrustDescent.W_LOCATION)
        assertEquals(input("w_about"), TrustDescent.W_ABOUT)
        assertEquals(input("w_words_pop"), TrustDescent.W_WORDS_POP)
        assertEquals(input("w_exactness_pop"), TrustDescent.W_EXACTNESS_POP)
        assertEquals(input("w_perfect_pop"), TrustDescent.W_PERFECT_POP)
        assertEquals(input("w_wot"), TrustDescent.W_WOT)
        assertEquals(input("w_wot_pow"), TrustDescent.W_WOT_POW)
        assertEquals(input("w_recency"), TrustDescent.W_RECENCY)
    }

    /**
     * The profile's `wot_of(r)`, transcribed: 0 under the floor, then
     * `1 + w_wot * min(max(0, r - floor), 100)^w_wot_pow`. The values the
     * schema's own comments quote pin the transcription.
     */
    @Test
    fun `wot_mult is the profile's curve`() {
        assertEquals(0.0, TrustDescent.wotMult(1.0, 2.0), "below the floor is deleted")
        assertEquals(1.0, TrustDescent.wotMult(2.0, 2.0), "at the floor the curve is 1")
        assertEquals(648.6, TrustDescent.wotMult(13.0, 2.0), 1.0, "the schema quotes wot_mult(13) ≈ 648.6")
        assertEquals(218601.0, TrustDescent.wotMult(97.0, 2.0), 500.0, "…and wot_mult(97) ≈ 218601")
    }

    /**
     * What the bound is FOR: a rung is exact when the page's K-th hit beats
     * the most an excluded document could score. The floor rung excludes only
     * what the gate deletes, so it is always proven; a K-th hit that scores
     * under the ceiling proves nothing above it; a K-th hit a top author's
     * body match reaches proves a rung in the teens, which is where the
     * measured pages stopped (docs/search-latency.md in the relay).
     */
    @Test
    fun `the rung a page proves`() {
        assertEquals(2, TrustDescent.floorRung(2.0))
        assertEquals(3, TrustDescent.floorRung(2.5), "a fractional floor rounds UP: the rung must not admit what the gate deletes")
        assertEquals(0.0, TrustDescent.bound(2, 2.0, 1), "nothing excluded by the floor rung can score at all")
        assertEquals(2, TrustDescent.provenRung(0.0, 2.0, 1), "an empty or weak page proves only the floor")
        assertEquals(2, TrustDescent.provenRung(TrustDescent.scoreCeiling(1) * 0.99, 2.0, 1))
        // A weak-band hit (4000) by a rank-51 author — the 40th hit of the measured `the` page.
        val kth = 4000.0 * TrustDescent.wotMult(51.0, 2.0)
        val rung = TrustDescent.provenRung(kth, 2.0, 1)
        assertTrue(rung in 8..20, "a top author's body hit proves a rung in the teens, got $rung")
        assertTrue(TrustDescent.bound(rung, 2.0, 1) <= kth, "the proven rung's bound is met")
        assertTrue(TrustDescent.bound(rung + 1, 2.0, 1) > kth, "…and the next rung's is not: it is the HIGHEST proven rung")
        assertEquals(TrustDescent.FIRST_RUNG, TrustDescent.provenRung(1e30, 2.0, 1), "an unbeatable page stops at the first rung")
        assertTrue(TrustDescent.scoreCeiling(3) > TrustDescent.scoreCeiling(1), "more words, more tail, a looser ceiling")
    }

    @Test
    fun `which shapes descend`() {
        val hexA = "a".repeat(64)
        val base = EventQuery(kinds = listOf(1), search = "bitcoin", observer = hexA, minRank = 2.0, limit = 40)
        assertTrue(TrustDescent.descends(base), "the relay's ranked search")
        assertFalse(TrustDescent.descends(base.copy(minRank = 0.0)), "include:spam has no gate, so no rung is exact")
        assertFalse(TrustDescent.descends(base.copy(minRank = null)))
        assertFalse(TrustDescent.descends(base.copy(observer = null)), "no observer: the text profile, no trust to descend")
        assertFalse(TrustDescent.descends(base.copy(limit = null)), "no page to prove")
        assertFalse(TrustDescent.descends(base.copy(ranking = EventYql.RANK_DESC)), "an explicit sort is another score")
        assertFalse(TrustDescent.descends(base.copy(search = null)), "plain recall")
        assertFalse(TrustDescent.descends(base.copy(trustFloor = 90)), "a rung does not descend again")
        assertFalse(TrustDescent.descends(base.copy(ids = listOf("b".repeat(64)))), "a keyed lookup names its own match set")
        assertEquals(2, TrustDescent.words(base.copy(search = "bitcoin lightning ⚡")), "the erased word is not a term")
        assertEquals(2, TrustDescent.words(base.copy(phrases = listOf("the standard"))))
    }
}
