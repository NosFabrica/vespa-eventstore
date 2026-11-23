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

import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * THE MEMO KEEPS ONE EVENT PER THREAD, BY IDENTITY, AND NEVER A LARGE ONE.
 * (That every gate of a store shares it, and that quartz's fan-out hands them
 * one instance, is LiveGateTest's.)
 */
class LiveWordsTest {
    private val alice = "a1".repeat(32)

    private fun note(
        n: Int,
        text: String,
    ) = TextNoteEvent(n.toString(16).padStart(64, '0'), alice, 1_700_000_000L + n, emptyArray(), text, "")

    @Test
    fun `a hit is the same instance, and answers what a fresh read does`() {
        val words = LiveWords()
        val e = note(1, "Pizza ⚡ night")
        assertEquals(LiveWords.read(e), words.of(e))
        assertEquals(listOf("pizza", "⚡", "night"), words.of(e))
        assertEquals(1L, words.computed, "the second ask was a hit")
        val twin = note(1, "Pizza ⚡ night")
        words.of(twin)
        assertEquals(2L, words.computed, "an equal event is not the same instance: identity, never equality, finds it")
    }

    @Test
    fun `each thread keeps its own last event`() {
        val words = LiveWords()
        val e = note(1, "pizza")
        words.of(e)
        thread { words.of(e) }.join()
        words.of(e)
        assertEquals(2L, words.computed, "once here, once on the other thread, and this thread's slot survived it")
    }

    /**
     * PAST [LiveWords.MAX_MEMO_CONTENT] NOTHING IS KEPT: the slot outlives the
     * fan-out, so a long-form article would stay pinned, with every word of
     * it, on every delivery thread until that thread's next event.
     */
    @Test
    fun `an event past the size cap is read every time and not kept`() {
        val words = LiveWords()
        val atCap = note(1, "a".repeat(LiveWords.MAX_MEMO_CONTENT))
        words.of(atCap)
        words.of(atCap)
        assertEquals(1L, words.computed, "at the cap: kept")
        val past = note(2, "a".repeat(LiveWords.MAX_MEMO_CONTENT + 1))
        words.of(past)
        words.of(past)
        assertEquals(3L, words.computed, "past it: read on each ask")
        words.of(atCap)
        assertEquals(3L, words.computed, "and the slot still holds the last event it could keep")
    }
}
