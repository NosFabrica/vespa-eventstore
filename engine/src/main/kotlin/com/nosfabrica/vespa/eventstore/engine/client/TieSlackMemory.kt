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
 * HOW WIDE A TIE SLACK EACH READ SHAPE NEEDS, learned from its last pages.
 *
 * The engine sorts a recency page by `created_at` alone, so a page of `limit`
 * overfetches a slack to hold the boundary second's whole tie group
 * ([VespaEventIndex.recallSummaries]); a group that reaches `limit + slack`
 * pays a second, unbounded `[T,T]` query instead. The two costs are lopsided:
 * an extra query is ~800 µs of query-container CPU whatever its size, an extra
 * hit ~4.4 µs (benchmark/README.md, "The tie slack is learned per shape"), so
 * one avoided query buys ~180 hits of slack. What a shape needs is set by its
 * event RATE, which only the shape knows: a one-author or one-tag feed holds a
 * handful of events per second and wastes nearly all of a wide slack, while a
 * global kind-1 or kind-7 feed on an aggregating relay holds tens per second,
 * and a narrow slack there makes the extra query the common case.
 *
 * So the slack is per SHAPE ([RecencyWindowMemory.Shape], the key the window
 * memory uses), starts at [VespaEventIndex.TIE_SLACK] — the flat width every
 * read had before, and what a shape seen for the first time gets — and moves
 * by what each full page showed ([next]): `over`, the members of the boundary
 * second past the page's last slot.
 *
 *  - a page whose group the slack CUT (it paid the `[T,T]` query) puts the
 *    shape straight back to [VespaEventIndex.TIE_SLACK];
 *  - a page whose boundary group asks for more than the current slack RAISES
 *    it at once, to `2 * over + MIN_TIE_SLACK` (the doubling is headroom for
 *    the next boundary second holding twice this one's ties, a Poisson feed's
 *    ordinary spread);
 *  - a page that needed less NARROWS it by a quarter of the gap, never in one
 *    step: one quiet second on a busy feed must not open the next read to a
 *    cut, and a quarter is where a simulation of Poisson feeds put a busy
 *    shape's cost at the flat slack's while a sparse one still narrows within
 *    about ten reads (see the README section).
 *
 * Exactness never depends on it: the slack only decides whether the boundary
 * group arrives in the first query or by its `[T,T]` read, which is untouched.
 * Bounded like the window memory (an LRU of [capacity] shapes whose entries
 * expire after [ttlMillis]), and absent when `VESPA_RECENCY_MEMORY=0`, which
 * leaves every read at the flat slack.
 */
internal class TieSlackMemory(
    private val capacity: Int = 4_096,
    private val ttlMillis: Long = 600_000L,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private class Entry(
        val slack: Int,
        val atMillis: Long,
    )

    // Access-ordered: the eldest entry is the least recently USED shape.
    private val entries =
        object : LinkedHashMap<RecencyWindowMemory.Shape, Entry>(256, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<RecencyWindowMemory.Shape, Entry>?): Boolean = size > capacity
        }

    /** The slack a page of [shape] should overfetch: what it learned, or [VespaEventIndex.TIE_SLACK] cold. */
    fun slackFor(shape: RecencyWindowMemory.Shape): Int =
        synchronized(entries) {
            val entry = entries[shape] ?: return VespaEventIndex.TIE_SLACK
            if (clock() - entry.atMillis > ttlMillis) {
                entries.remove(shape)
                VespaEventIndex.TIE_SLACK
            } else {
                entry.slack
            }
        }

    /**
     * What a full page of [shape] showed: it overfetched [slack], [over] members
     * of its boundary second fell past its last slot, and [cut] when the slack
     * could not hold them and the `[T,T]` query ran.
     */
    fun learn(
        shape: RecencyWindowMemory.Shape,
        slack: Int,
        over: Int,
        cut: Boolean,
    ) {
        val next = next(slack, over, cut)
        synchronized(entries) { entries[shape] = Entry(next, clock()) }
    }

    /** Shapes currently remembered (expired ones included until touched). */
    val size: Int get() = synchronized(entries) { entries.size }

    companion object {
        /** The slack after a page that overfetched [slack] and saw [over] boundary members past its end — see the class KDoc. */
        fun next(
            slack: Int,
            over: Int,
            cut: Boolean,
        ): Int {
            if (cut) return VespaEventIndex.TIE_SLACK
            val target = VespaEventIndex.plusSaturated(VespaEventIndex.plusSaturated(over, over), VespaEventIndex.MIN_TIE_SLACK).coerceIn(VespaEventIndex.MIN_TIE_SLACK, VespaEventIndex.TIE_SLACK)
            if (target >= slack) return target
            return maxOf(target, slack - maxOf(1, (slack - target) / 4))
        }
    }
}
