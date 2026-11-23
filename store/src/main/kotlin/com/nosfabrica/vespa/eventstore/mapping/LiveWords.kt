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
package com.nosfabrica.vespa.eventstore.mapping

import com.nosfabrica.vespa.eventstore.engine.text.IndexableChars
import com.vitorpamplona.quartz.nip01Core.core.Event
import java.util.concurrent.atomic.LongAdder

/**
 * AN EVENT'S INDEXED WORDS, AS THE LIVE GATE READS THEM, ONCE PER EVENT PER THREAD.
 *
 * Every live subscription has its own `LiveGate`, and each text rule in it
 * checks the event's words. Read per gate, a note reaching N open NIP-50 feeds
 * was extracted, joined, lower-cased and tokenized N times on the fan-out path.
 * One instance per store, shared by every gate it builds, remembers the words
 * of the LAST event each thread asked about — the same trick as the relay's
 * per-loop shared frame body. Fan-out hands every subscription the same
 * [Event] instance, one event at a time per delivery thread, so identity finds
 * it and the slot never needs more than one entry.
 *
 * THREAD-CONFINED because fan-out runs on several threads at once (quartz's
 * `FanoutExecutor`s, one per event loop), each on its own event. One shared
 * slot would be race-free (an immutable entry behind a volatile reference),
 * but those threads would keep evicting each other's entry — a thread
 * resuming its walk over its subscriptions would find another loop's event and
 * extract again. Per thread, a slot is only ever replaced by that thread's
 * next event.
 *
 * A miss computes exactly [read], which is what the gate computed on every
 * call before the memo, and [read] is a pure function of the event, so a hit
 * answers the same. The memo is still asked only by a text rule whose NIP-01
 * part matched: a reaction passing a `kinds:[1]` search feed is never read.
 *
 * Construct with [textOf] only in tests; production uses [text].
 */
internal class LiveWords(
    private val textOf: (Event) -> String = ::text,
) {
    private class Last(
        val event: Event,
        val words: List<String>,
    )

    private val last = ThreadLocal<Last>()

    // A LongAdder, not an AtomicLong: every delivery thread increments it on each miss, and one
    // shared counter would be one contended cache line across all of them.
    private val misses = LongAdder()

    /** How many times [of] had to extract and tokenize: the number the sharing exists to keep at one per event per thread. */
    val computed: Long get() = misses.sum()

    /** [event]'s words, from this thread's memo when it last asked about this same instance. */
    fun of(event: Event): List<String> {
        last.get()?.let { if (it.event === event) return it.words }
        val text = textOf(event)
        val words = tokens(text)
        misses.increment()
        // Past the cap the words are not kept: the slot outlives the fan-out (until this thread's
        // next event), so it must not pin a long-form article and every word of it per thread.
        if (retainedChars(event, text) <= MAX_MEMO_CHARS) last.set(Last(event, words))
        return words
    }

    companion object {
        /**
         * The most chars a memo entry may pin: the event's content and tags, which the slot keeps
         * alive by holding the event, plus the text its words were read from — every indexed field
         * ([text]), tags included, which the words' strings roughly add up to. A larger event is
         * read on every call, as before the memo. Each delivery thread keeps one entry alive until
         * its next event: at the relay's 128 KiB `maxContentLength` a note's would be ~256 Ki
         * chars, estimated (not measured) at ~1.5 MB per thread with its word objects. 32 Ki
         * chars keeps it near 200 KB per thread, while a note — the kind a live search feed
         * streams — sits far below it: the 10,961-event staging slice in benchmark/README.md has a
         * median of 501 content chars, which counts about twice (content, then its words).
         *
         * It measured `content.length` alone until a review found what that missed: a short note
         * may carry thousands of tags, all pinned with the event, and an article's words may come
         * from a long title or summary tag, none of it content.
         */
        const val MAX_MEMO_CHARS = 32 * 1024

        /** What a memo entry for [event] pins, in chars: see [MAX_MEMO_CHARS]. Stops counting once past it. */
        internal fun retainedChars(
            event: Event,
            text: String,
        ): Int {
            var total = event.content.length + text.length
            for (tag in event.tags) {
                for (value in tag) total += value.length
                if (total > MAX_MEMO_CHARS) return total
            }
            return total
        }

        /** The text the live check reads: every indexed field of [event] ([SearchExtractors]), joined. */
        fun text(event: Event): String =
            SearchExtractors
                .extract(event)
                .fields()
                .values
                .joinToString(" ")

        /** The engine's tokens ([IndexableChars.tokens]) of lower-cased [text]: letter/digit runs, and each emoji a word of its own. */
        fun tokens(text: String?): List<String> = text?.lowercase()?.let(IndexableChars::tokens).orEmpty()

        /** [event]'s words with no memo: what [of] computes on a miss. */
        fun read(event: Event): List<String> = tokens(text(event))
    }
}
