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
 * memory uses). A shape with nothing learned starts from what the read
 * already knows ([coldSlack]): the wide [VespaEventIndex.TIE_SLACK] for a
 * shape that can be busy (a bare kind, a global or gated termless feed), the
 * page's own width for a selective one (authors, tags, ids, terms), the least
 * for a shape the window memory recalls as narrow, and the rate a speculative
 * read's short attempts measured before its fallback ([measuredSlack]). Then
 * it moves by what each full page showed ([next]): `over`, the members of the
 * boundary second past the page's last slot.
 *
 *  - a page whose group the slack CUT (it paid the `[T,T]` query, which
 *    counted the group exactly) learns that group's width with headroom, and
 *    never less than [VespaEventIndex.TIE_SLACK] — the busy-feed protection;
 *  - a page whose boundary group asks for more than the current slack RAISES
 *    it at once, to `2 * over + MIN_TIE_SLACK` (the doubling is headroom for
 *    the next boundary second holding twice this one's ties, a Poisson feed's
 *    ordinary spread), at most [MAX_LEARNED_SLACK];
 *  - a page that needed less NARROWS it by a quarter of the gap, never in one
 *    step: one quiet second on a busy feed must not open the next read to a
 *    cut, and a quarter is where a simulation of Poisson feeds put a busy
 *    shape's cost at the flat slack's while a sparse one still narrows within
 *    about ten reads (see the README section) — and only while no other page
 *    of the shape has written since this one leased its slack ([learn]).
 *
 * A page served off part of the corpus teaches nothing.
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
    /**
     * The slack a page should overfetch, as [slackFor] handed it out: its
     * [value], and the [epoch] of the entry it came from, which [learn] needs
     * to tell a narrowing still current from one another page overtook.
     */
    class Lease internal constructor(
        val value: Int,
        internal val epoch: Long,
    )

    private class Entry(
        val slack: Int,
        val atMillis: Long,
        val epoch: Long,
    )

    /** Bumped by every write, so an entry's epoch names that write. 0 is "no entry". */
    private var epochs = 0L

    // Access-ordered: the eldest entry is the least recently USED shape.
    private val entries =
        object : LinkedHashMap<RecencyWindowMemory.Shape, Entry>(256, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<RecencyWindowMemory.Shape, Entry>?): Boolean = size > capacity
        }

    /**
     * The slack a page of [shape] should overfetch: what it learned, or [cold]
     * for a shape with nothing live — [VespaEventIndex.TIE_SLACK] unless the
     * caller has evidence the shape is thinner ([coldSlack]).
     */
    fun slackFor(
        shape: RecencyWindowMemory.Shape,
        cold: Int = VespaEventIndex.TIE_SLACK,
    ): Lease =
        synchronized(entries) {
            val entry = live(shape) ?: return Lease(cold, 0)
            Lease(entry.slack, entry.epoch)
        }

    /**
     * What a full page of [shape] showed: it overfetched [lease], [over] members
     * of its boundary second fell past its last slot (exactly, even on a cut:
     * the `[T,T]` query counted them), and [cut] when the slack could not hold
     * them and that query ran.
     *
     * Decided INSIDE the lock, against what is stored NOW, not only against the
     * slack the page started with: pages of one shape run concurrently, and a
     * last-writer-wins table let a narrowing computed from a stale slack
     * overwrite the cut or the raise another page had just learned — reopening
     * a busy shape to the very cut that raised it. A cut or a raise never
     * lowers what is stored; a narrowing lands only while the entry is still
     * the one the page leased (same epoch), so any write since overtakes it.
     */
    fun learn(
        shape: RecencyWindowMemory.Shape,
        lease: Lease,
        over: Int,
        cut: Boolean,
    ) {
        val proposed = next(lease.value, over, cut)
        synchronized(entries) {
            val stored = live(shape)
            val value =
                when {
                    stored == null -> proposed
                    proposed >= lease.value -> maxOf(proposed, stored.slack)
                    stored.epoch == lease.epoch -> proposed
                    else -> return
                }
            entries[shape] = Entry(value, clock(), ++epochs)
        }
    }

    /** [shape]'s entry, or null when absent or expired (and then dropped). Call holding the lock. */
    private fun live(shape: RecencyWindowMemory.Shape): Entry? {
        val entry = entries[shape] ?: return null
        if (clock() - entry.atMillis <= ttlMillis) return entry
        entries.remove(shape)
        return null
    }

    /** Shapes currently remembered (expired ones included until touched). */
    val size: Int get() = synchronized(entries) { entries.size }

    companion object {
        /**
         * The highest slack a shape may learn: ~180 hits cost what one avoided
         * `[T,T]` query costs (~800 µs against ~4.4 µs a hit; benchmark/README.md,
         * "The tie slack is learned per shape"), so past it the query is cheaper
         * than the slack that would avoid it.
         */
        const val MAX_LEARNED_SLACK = 180

        /** The slack after a page that overfetched [slack] and saw [over] boundary members past its end — see the class KDoc. */
        fun next(
            slack: Int,
            over: Int,
            cut: Boolean,
        ): Int {
            val target = VespaEventIndex.plusSaturated(VespaEventIndex.plusSaturated(over, over), VespaEventIndex.MIN_TIE_SLACK).coerceIn(VespaEventIndex.MIN_TIE_SLACK, MAX_LEARNED_SLACK)
            // A CUT knows its boundary group exactly (the `[T,T]` read counted it),
            // so it learns that group's width with the usual headroom — and never
            // less than the wide slack, which is the busy-feed protection: a shape
            // whose boundary second routinely runs past 64 stops paying the extra
            // query on every read, and one that cut once does not narrow below 64
            // on the strength of the very page that cut it.
            if (cut) return maxOf(VespaEventIndex.TIE_SLACK, target)
            if (target >= slack) return target
            return maxOf(target, slack - maxOf(1, (slack - target) / 4))
        }

        /**
         * The slack a read of [q] with this [limit] starts from when its shape
         * has learned nothing: the wide [VespaEventIndex.TIE_SLACK] for a shape
         * that can be BUSY — a bare kind, a global feed, a gated termless read —
         * and the page's own width (within [VespaEventIndex.MIN_TIE_SLACK] ..
         * [VespaEventIndex.TIE_SLACK]) for a SELECTIVE one: authors, owners,
         * tags, ids or search terms. A selective shape's boundary second rarely
         * holds a page's worth of ties, and such shapes — a `#p` notification
         * feed, an `#e` thread, one author's profile — are the high-cardinality
         * ones a shape table rarely holds warm: started at 64, they paid ~64
         * extra hits on every read, most of a narrow page's cost. A cut still
         * sends one to the wide slack at once.
         */
        fun coldSlack(
            q: EventQuery,
            limit: Int,
        ): Int {
            val selective = q.authors.isNotEmpty() || q.owners.isNotEmpty() || q.tags.isNotEmpty() || q.tagsAll.isNotEmpty() || q.ids.isNotEmpty() || !q.search.isNullOrBlank() || q.phrases.isNotEmpty()
            return if (selective) limit.coerceIn(VespaEventIndex.MIN_TIE_SLACK, VespaEventIndex.TIE_SLACK) else VespaEventIndex.TIE_SLACK
        }

        /**
         * The slack a read measured to hold [events] in [windowSecs] seconds
         * should start from: its rate per second, doubled (rounded down: a few
         * events an hour hold no ties), plus the least slack, within the cold range. A speculative read whose attempts came
         * back short knows exactly this before its unwindowed fallback runs.
         */
        fun measuredSlack(
            events: Int,
            windowSecs: Long,
        ): Int {
            val doubled = 2L * events / maxOf(1L, windowSecs)
            return (doubled + VespaEventIndex.MIN_TIE_SLACK).coerceIn(VespaEventIndex.MIN_TIE_SLACK.toLong(), VespaEventIndex.TIE_SLACK.toLong()).toInt()
        }
    }
}
