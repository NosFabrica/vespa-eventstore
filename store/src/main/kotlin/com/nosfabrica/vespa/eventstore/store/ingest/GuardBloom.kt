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
package com.nosfabrica.vespa.eventstore.store.ingest

import java.util.concurrent.atomic.AtomicLongArray

/**
 * A fixed-capacity, concurrent Bloom filter over owner keys — the scalable
 * backing for [GuardOwners] (an exact `HashSet` gave up past 10k deleters).
 *
 * The ONLY correctness property [GuardOwners] relies on is **no false
 * negatives**: [mightContain] returns `false` only for a key provably never
 * [add]ed, so skipping a guard probe on `false` can never skip a needed one.
 * A false POSITIVE only costs a wasted probe; overfill trades memory for probe
 * count, never correctness.
 *
 * Thread-safe: [add] ORs words atomically in an [AtomicLongArray], reads carry
 * volatile visibility, and per the store's one-write-lane-per-owner model no
 * other lane tests X while X's own guard is being recorded.
 *
 * Sizing follows the standard Bloom optima for [expectedInsertions] at [fpp]:
 *   m = -n ln p / (ln 2)^2 bits,  k = round(m/n · ln 2) hashes.
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
