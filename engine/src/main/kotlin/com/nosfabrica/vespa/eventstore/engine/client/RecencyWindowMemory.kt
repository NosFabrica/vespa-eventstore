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
package com.nosfabrica.vespa.eventstore.engine.client

import com.nosfabrica.vespa.eventstore.engine.query.EventQuery

/**
 * WHERE THE LAST READ OF THIS SHAPE FOUND ITS PAGE, so the next one starts
 * there instead of at [RecencyPlanner.FIRST_WINDOW].
 *
 * [RecencyStrategy.SPECULATIVE] pays one round trip per window it has to
 * guess. Relay traffic repeats its guesses: every client of a global feed
 * asks the identical filter, a follow feed is re-asked on every reconnect and
 * every page, a notification subscription on every open. What the first read
 * learned — "a page of this shape fits in W seconds", or "this shape is too
 * thin to window at all" — answers the next one's guess in advance:
 *
 *  - a remembered WINDOW is the first attempt (scaled to the new limit), so a
 *    shape that needed a second window last time proves out in one query;
 *  - a remembered NARROW skips the attempts and runs the read unwindowed
 *    immediately — the shipped cost, exactly, with nothing speculated.
 *
 * Correctness never depends on it: a window only changes what the engine
 * WALKS, never which page is served (a full window proves itself; a short
 * one widens as before), so a stale entry costs a round trip, not an answer.
 * Entries expire after [ttlMillis] so a feed that changes character — a
 * hashtag that starts trending — is re-probed rather than remembered wrong,
 * and the table is an LRU of [capacity] shapes, so its memory is bounded
 * whatever the traffic.
 *
 * The KEY is everything that sets a shape's event RATE, and nothing that
 * doesn't: kinds, authors, tags, owners, exclusions, the search terms and
 * phrases (a gated `sort:recent` search is windowable, and a rare word fills
 * at its own rate, not the busy one's), and the gate's lens and floor — but
 * not `since`/`until` (windows are anchor-relative) or `limit` (a remembered
 * window scales linearly with it). One coarse term does carry time: how far
 * back the anchor sits, in powers of two of a day, because a page ten months
 * back does not fill at the rate the newest page does.
 *
 * A read computes its [Shape] ONCE ([shapeOf]) and hands it to [recall] and
 * then [remember]/[forget]: the key walks the whole author list, which is
 * tens of kilobytes for a follow feed.
 *
 * ONE TABLE FOR WHAT A SHAPE TAUGHT: the window above and, for the reads' own
 * table, the tie slack ([TieSlackMemory] keeps its entries HERE, not in a map
 * of its own), so a read finds both under one lock acquisition ([recallWith])
 * and the two share one LRU bound.
 */
internal class RecencyWindowMemory(
    private val capacity: Int = 4_096,
    private val ttlMillis: Long = 600_000L,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    /** What one shape's last speculative read learned. */
    sealed interface Recall {
        /** A page fit in [window] seconds at [limit]; start the next attempt there, scaled. */
        data class Window(
            val window: Long,
            val limit: Int,
        ) : Recall

        /** The shape fell back as too thin to window; run it unwindowed. */
        data object Narrow : Recall
    }

    /**
     * Everything one shape taught, each part with its own age: a window (or
     * none), and a tie slack (0 = none) with the epoch of the write that set
     * it ([TieSlackMemory.learn]). Mutated only under the table's lock.
     */
    private class Slot {
        var recall: Recall? = null
        var recallAt = 0L
        var slack = 0
        var slackAt = 0L
        var slackEpoch = 0L

        fun isEmpty() = recall == null && slack == 0
    }

    /**
     * A shape, as a FIXED-SIZE key: 128 bits hashed from the canonical shape.
     * Holding the shape itself would hold its author list — a 1,000-author
     * follow feed is ~100 KB of strings per entry, ~400 MB at [capacity] — for
     * a table whose whole value is being cheap. A collision is harmless by the
     * class's own argument: it can only hand a read the wrong starting guess
     * (a window, a tie slack), which costs a round trip, never a page — so the
     * key is a performance hint, and its hash only has to be good, not
     * adversary-proof ([shapeOf]).
     */
    class Shape internal constructor(
        private val hi: Long,
        private val lo: Long,
    ) {
        override fun equals(other: Any?): Boolean = other is Shape && other.hi == hi && other.lo == lo

        override fun hashCode(): Int = (hi xor lo).toInt()
    }

    /** Bumped by every slack write, so an entry's epoch names that write. 0 is "none". */
    private var epochs = 0L

    // Access-ordered: the eldest entry is the least recently USED shape.
    private val entries =
        object : LinkedHashMap<Shape, Slot>(256, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Shape, Slot>?): Boolean = size > capacity
        }

    /** What [q] (anchored at [anchor], with the request clock [now]) should start from, or null to start cold. */
    fun recall(
        q: EventQuery,
        anchor: Long,
        now: Long,
    ): Recall? = recall(shapeOf(q, anchor, now))

    /** What [shape] should start from, or null to start cold. */
    fun recall(shape: Shape): Recall? = synchronized(entries) { liveRecall(shape) }

    /**
     * [recall] and the tie slack ([TieSlackMemory.slackFor]) of [shape] under
     * ONE lock acquisition — what a speculative read asks first.
     */
    fun recallWith(
        shape: Shape,
        coldSlack: Int,
    ): Pair<Recall?, TieSlackMemory.Lease> = synchronized(entries) { liveRecall(shape) to lease(shape, coldSlack) }

    fun remember(
        q: EventQuery,
        anchor: Long,
        now: Long,
        recall: Recall,
    ) = remember(shapeOf(q, anchor, now), recall)

    fun remember(
        shape: Shape,
        recall: Recall,
    ) {
        synchronized(entries) {
            val slot = entries.getOrPut(shape) { Slot() }
            slot.recall = recall
            slot.recallAt = clock()
        }
    }

    fun forget(
        q: EventQuery,
        anchor: Long,
        now: Long,
    ) = forget(shapeOf(q, anchor, now))

    /** Forget [shape]'s WINDOW; a tie slack it learned stays. */
    fun forget(shape: Shape) {
        synchronized(entries) {
            val slot = entries[shape] ?: return
            slot.recall = null
            if (slot.isEmpty()) entries.remove(shape)
        }
    }

    /** Shapes currently remembered (expired ones included until touched). */
    val size: Int get() = synchronized(entries) { entries.size }

    /** [shape]'s window, or null when absent or expired. Call holding the lock. */
    private fun liveRecall(shape: Shape): Recall? {
        val slot = entries[shape] ?: return null
        if (slot.recall != null && clock() - slot.recallAt > ttlMillis) {
            slot.recall = null
            if (slot.isEmpty()) entries.remove(shape)
        }
        return slot.recall
    }

    /** [TieSlackMemory.slackFor]'s answer, from this table. Call holding the lock. */
    private fun lease(
        shape: Shape,
        cold: Int,
    ): TieSlackMemory.Lease {
        val slot = entries[shape] ?: return TieSlackMemory.Lease(cold, 0)
        if (slot.slack != 0 && clock() - slot.slackAt > ttlMillis) {
            slot.slack = 0
            slot.slackEpoch = 0
            if (slot.isEmpty()) entries.remove(shape)
        }
        return if (slot.slack == 0) TieSlackMemory.Lease(cold, 0) else TieSlackMemory.Lease(slot.slack, slot.slackEpoch)
    }

    /** [TieSlackMemory.slackFor]: the slack [shape] learned, or [cold]. */
    internal fun slackLease(
        shape: Shape,
        cold: Int,
    ): TieSlackMemory.Lease = synchronized(entries) { lease(shape, cold) }

    /**
     * [TieSlackMemory.learn]'s write, decided against what is stored now:
     * [decide] gets the live slack and its epoch (0, 0 when none) and answers
     * the slack to store, or null to leave it.
     */
    internal fun learnSlack(
        shape: Shape,
        decide: (stored: Int, epoch: Long) -> Int?,
    ) {
        synchronized(entries) {
            val live = lease(shape, 0)
            val value = decide(live.value, live.epoch) ?: return
            val slot = entries.getOrPut(shape) { Slot() }
            slot.slack = value
            slot.slackAt = clock()
            slot.slackEpoch = ++epochs
        }
    }

    companion object {
        /** [q]'s shape, anchored at [anchor] under the request clock [now] — compute once per read. */
        fun shapeOf(
            q: EventQuery,
            anchor: Long,
            now: Long,
        ): Shape {
            val gated = q.usesGatedProfile()
            val k = ShapeKey()
            k.field(1).ints(q.kinds)
            k.field(2).set(q.authors, lowerAscii = true)
            k.field(3).set(q.owners, lowerAscii = true)
            k.field(4).tags(q.tags)
            k.field(5).tags(q.tagsAll)
            k.field(6).set(q.notSearch, lowerAscii = false)
            // Raw, not normalized: two spellings of one term are two shapes, which
            // costs at most a learning read, never a page.
            k.field(7).text(q.search?.trim().orEmpty())
            k.field(8).set(q.phrases, lowerAscii = false)
            k.field(9).long(if (gated) 1 else 0)
            if (gated) {
                k.field(10).text(q.rankKey.toString())
                k.field(11).text(q.minRank.toString())
            }
            k.field(12).long(ageBucket(now - anchor).toLong())
            return k.key()
        }

        /**
         * How far back an anchor sits, coarsely: 0 within a day of the clock,
         * then one bucket per doubling (2 days, 4, 8, … ). Deep pagination
         * walks through the buckets and re-learns at each, which is the point:
         * the rate a page meets depends on how old it is.
         */
        fun ageBucket(ageSecs: Long): Int {
            val days = ageSecs / 86_400L
            return if (days <= 0) 0 else 64 - java.lang.Long.numberOfLeadingZeros(days)
        }
    }
}

/**
 * A shape's 128-bit key, built WITHOUT copying, sorting or digesting its lists.
 *
 * It used to lowercase, copy and sort the author list and SHA-256 the joined
 * text, on every windowable read: ~50-200 µs for a 1,000-author feed. A LIST
 * is now an ORDER-INDEPENDENT combination — the sum, in each of two 64-bit
 * lanes, of its items' hashes, plus its size — so no sort is needed, and each
 * item hashes its characters in place (lowercasing ASCII as it goes, which is
 * all a hex key needs). Every item is FRAMED (its length is hashed first) and
 * every field is tagged with its number, so values cannot join into another
 * shape's (see the framing fix this keeps). The lanes are seeded apart and
 * finished with a full avalanche (MurmurHash3's fmix64), so the key is as good
 * as a performance hint needs: a collision only costs a read its warm start.
 */
private class ShapeKey {
    private var hi = SEED_HI
    private var lo = SEED_LO

    fun field(n: Int): ShapeKey {
        mix(n.toLong())
        return this
    }

    fun long(v: Long) {
        mix(v)
    }

    fun ints(values: List<Int>) {
        var sh = 0L
        var sl = 0L
        for (v in values) {
            sh += fmix(v.toLong() xor SEED_HI)
            sl += fmix(v.toLong() xor SEED_LO)
        }
        combine(values.size, sh, sl)
    }

    fun text(s: String) {
        mix2(hashOf(s, SEED_HI, false), hashOf(s, SEED_LO, false))
    }

    fun set(
        values: List<String>,
        lowerAscii: Boolean,
    ) {
        var sh = 0L
        var sl = 0L
        for (v in values) {
            sh += hashOf(v, SEED_HI, lowerAscii)
            sl += hashOf(v, SEED_LO, lowerAscii)
        }
        combine(values.size, sh, sl)
    }

    fun tags(tags: Map<String, List<String>>) {
        // Each (name, values) pair hashed whole, the pairs combined order-independently.
        var sh = 0L
        var sl = 0L
        for ((name, values) in tags) {
            var vh = 0L
            var vl = 0L
            for (v in values) {
                vh += hashOf(v, SEED_HI, false)
                vl += hashOf(v, SEED_LO, false)
            }
            sh += fmix(hashOf(name, SEED_HI, false) * K1 + vh * K2 + values.size)
            sl += fmix(hashOf(name, SEED_LO, false) * K2 + vl * K1 + values.size)
        }
        combine(tags.size, sh, sl)
    }

    fun key(): RecencyWindowMemory.Shape = RecencyWindowMemory.Shape(fmix(hi), fmix(lo))

    private fun combine(
        size: Int,
        sh: Long,
        sl: Long,
    ) {
        mix(size.toLong())
        mix2(sh, sl)
    }

    private fun mix(v: Long) = mix2(v, v)

    private fun mix2(
        h: Long,
        l: Long,
    ) {
        hi = fmix(hi * K1 + h)
        lo = fmix(lo * K2 + l)
    }

    private companion object {
        const val SEED_HI = -0x61c8864680b583ebL
        const val SEED_LO = 0x2545f4914f6cdd1dL
        const val K1 = -0x40a7b892e31b1a47L
        const val K2 = -0x6b2fb644ecceee15L

        /** [s] framed (its length first) and hashed char by char; ASCII lowercased when [lowerAscii]. */
        fun hashOf(
            s: String,
            seed: Long,
            lowerAscii: Boolean,
        ): Long {
            var h = seed xor (s.length.toLong() * K1)
            for (i in s.indices) {
                var c = s[i].code
                if (lowerAscii && c in 'A'.code..'Z'.code) c += 32
                h = (h xor c.toLong()) * K2
            }
            return fmix(h)
        }

        /** MurmurHash3's 64-bit finalizer: every input bit reaches every output bit. */
        fun fmix(k0: Long): Long {
            var k = k0
            k = k xor (k ushr 33)
            k *= -0xae502812aa7333L
            k = k xor (k ushr 33)
            k *= -0x3b314601e57a13adL
            return k xor (k ushr 33)
        }
    }
}
