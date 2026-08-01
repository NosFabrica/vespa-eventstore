/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store.ingest

import java.util.concurrent.atomic.AtomicLongArray

/**
 * A fixed-capacity, concurrent Bloom filter over owner keys — the scalable
 * backing for [GuardOwners], replacing an exact `HashSet` that had to give up
 * (and disable the guard-skip) past 10k distinct deleters.
 *
 * The ONLY property [GuardOwners] relies on for correctness is **no false
 * negatives**: [mightContain] returns `false` only for a key that was provably
 * never [add]ed, so skipping the guard probe on a `false` can never skip a
 * needed one. A Bloom filter guarantees exactly that — `false` means at least
 * one of the key's bits is unset, which is impossible after [add]. A false
 * POSITIVE (all bits collide) only costs a wasted probe — the same over-flag
 * cost the exact set's design already accepted — so a higher fill just trades
 * memory for probe count, NEVER for correctness.
 *
 * Thread-safe by construction: bits live in an [AtomicLongArray]; [add] ORs each
 * word atomically and [mightContain] reads words plainly (AtomicLongArray reads
 * carry volatile visibility). Concurrent add-of-X / test-of-Y is safe — Y's bits
 * that X does not touch are unaffected, and per the store's one-write-lane-per-
 * owner model no other lane tests X while X's own guard is being recorded.
 *
 * Sizing follows the standard Bloom optima for [expectedInsertions] at [fpp]:
 *   m = -n ln p / (ln 2)^2 bits,  k = round(m/n · ln 2) hashes.
 * Overfilling past [expectedInsertions] degrades gracefully — the false-positive
 * rate climbs (more wasted probes) but never yields a false negative.
 */
internal class GuardBloom(
    expectedInsertions: Int,
    fpp: Double = 0.01,
) {
    private val bitCount: Long
    private val words: AtomicLongArray
    private val hashes: Int

    init {
        val n = expectedInsertions.coerceAtLeast(1).toDouble()
        val p = fpp.coerceIn(1e-6, 0.5)
        val ln2 = Math.log(2.0)
        val m = Math.ceil(-n * Math.log(p) / (ln2 * ln2)).toLong().coerceIn(64L, 1L shl 34)
        bitCount = ((m + 63) / 64) * 64 // round up to whole words
        words = AtomicLongArray((bitCount / 64).toInt())
        hashes = Math.max(1, Math.round(bitCount / n * ln2).toInt()).coerceAtMost(32)
    }

    /** Record [key]. Idempotent; only ever sets bits. Safe under concurrent [mightContain]. */
    fun add(key: String) {
        forEachBit(key) { word, mask ->
            var cur = words.get(word)
            while (cur and mask == 0L && !words.compareAndSet(word, cur, cur or mask)) {
                cur = words.get(word)
            }
        }
    }

    /** `false` ⟹ [key] was NEVER added (no false negatives). `true` ⟹ probably added (may be a false positive). */
    fun mightContain(key: String): Boolean {
        forEachBit(key) { word, mask ->
            if (words.get(word) and mask == 0L) return false
        }
        return true
    }

    private inline fun forEachBit(
        key: String,
        body: (word: Int, mask: Long) -> Unit,
    ) {
        // Double hashing (Kirsch–Mitzenmacher): k indices from two independent
        // 64-bit hashes, gi = h1 + i·h2. Both derived from the key's bytes.
        val h1 = fnv1a64(key, -0x340d631b7bdddcdbL) // FNV offset basis
        val h2 = fnv1a64(key, 0x100000001b3L) or 1L // odd, so the step never degenerates
        var combined = h1
        for (i in 0 until hashes) {
            val bit = (combined and Long.MAX_VALUE) % bitCount
            body((bit ushr 6).toInt(), 1L shl (bit and 63L).toInt())
            combined += h2
        }
    }

    private fun fnv1a64(
        s: String,
        seed: Long,
    ): Long {
        var h = seed
        for (i in s.indices) {
            h = h xor (s[i].code.toLong() and 0xffL)
            h *= 0x100000001b3L
        }
        // final avalanche so short/similar hex keys spread across all words
        h = h xor (h ushr 33)
        h *= -0xae502812aa7333L
        h = h xor (h ushr 29)
        return h
    }
}
