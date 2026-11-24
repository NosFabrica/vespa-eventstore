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

/**
 * WHAT A READ'S FAMILY USUALLY IS, for the read no table has seen yet.
 *
 * [RecencyWindowMemory] keys a shape by its VALUES, so it answers the second
 * read of a `#p` notification feed and never the first. The first reads are
 * most of them: one per user after every expiry, one per new thread's `#e`,
 * one per profile opened. A cold [RecencyStrategy.SPECULATIVE] read spends a
 * windowed attempt before it learns that its shape is narrow, and for a narrow
 * shape that attempt is pure overhead: two engine queries where the unwindowed
 * read alone answers. Measured on nostr-relay-bench's idle test (a fresh
 * `{kinds:[1],#p:[key]}` per connection), every REQ paid exactly that.
 *
 * So the cold reads of a FAMILY ([RecencyWindowMemory.familyOf]: the shape
 * with its values counted, not named) vote. Once at least [minSamples] have,
 * and at least [threshold] of them ended narrow, a cold read of that family
 * skips the attempt and runs the unwindowed read, as a remembered narrow shape
 * does. Correctness never depends on it, by the window memory's own argument:
 * the unwindowed read IS the answer, so a wrong vote costs cost, never a page.
 *
 * What a wrong vote costs is bounded. A BUSY value in a narrow family (the
 * popular account among the quiet ones) pays the unwindowed read once, the
 * shipped match-phase cost, and its full page names the window that would have
 * held it: the exact shape remembers that window, and its next read speculates
 * from it. The vote counts that read as not narrow, so a family whose values
 * turn busy loses its majority and starts speculating again.
 *
 * Votes are a decaying count ([window]: past it both counts halve, so the
 * recent reads outweigh the old) that expires with the window memory's
 * [ttlMillis], and the table is an LRU of [capacity] families.
 */
internal class NarrowPrior(
    private val capacity: Int = 1_024,
    private val ttlMillis: Long = 600_000L,
    private val minSamples: Int = MIN_SAMPLES,
    private val threshold: Double = THRESHOLD,
    private val window: Int = WINDOW,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private class Votes {
        var narrow = 0
        var total = 0
        var at = 0L
    }

    private val entries =
        object : LinkedHashMap<RecencyWindowMemory.Shape, Votes>(64, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<RecencyWindowMemory.Shape, Votes>?): Boolean = size > capacity
        }

    /** Whether a cold read of [family] should skip the speculative attempt. */
    fun isNarrow(family: RecencyWindowMemory.Shape): Boolean =
        synchronized(entries) {
            val v = live(family) ?: return false
            v.total >= minSamples && v.narrow >= threshold * v.total
        }

    /** One cold read of [family] ended [narrow] (or proved a window). */
    fun record(
        family: RecencyWindowMemory.Shape,
        narrow: Boolean,
    ) {
        synchronized(entries) {
            val v = live(family) ?: Votes().also { entries[family] = it }
            if (v.total >= window) {
                v.total /= 2
                v.narrow /= 2
            }
            v.total++
            if (narrow) v.narrow++
            v.at = clock()
        }
    }

    /** Families currently held (expired ones included until touched). */
    val size: Int get() = synchronized(entries) { entries.size }

    /** [family]'s votes, or null when absent or expired. Call holding the lock. */
    private fun live(family: RecencyWindowMemory.Shape): Votes? {
        val v = entries[family] ?: return null
        if (clock() - v.at > ttlMillis) {
            entries.remove(family)
            return null
        }
        return v
    }

    companion object {
        /** Cold reads a family needs before its vote counts. */
        const val MIN_SAMPLES = 8

        /**
         * The share of them that must have ended narrow. High on purpose: a
         * busy value sent unwindowed pays the match-phase cost, a narrow one sent
         * through an attempt pays one small query, so the prior acts only where
         * almost every cold read was narrow.
         */
        const val THRESHOLD = 0.9

        /** Past this many votes both counts halve: the recent reads decide. */
        const val WINDOW = 32
    }
}
