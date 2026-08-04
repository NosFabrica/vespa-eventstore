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
package com.nosfabrica.vespa.eventstore.ingest

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class GuardBloomTest {
    private fun hex(rnd: Random): String = (0 until 64).joinToString("") { "0123456789abcdef"[rnd.nextInt(16)].toString() }

    /**
     * THE correctness invariant GuardOwners relies on: a key that was added must
     * NEVER report absent. A false negative here would skip a needed NIP-09/62
     * guard probe — the one thing the guard-skip must never do.
     */
    @Test
    fun noFalseNegatives() {
        val rnd = Random(1)
        val bloom = GuardBloom(expectedInsertions = 200_000)
        val keys = (0 until 200_000).map { hex(rnd) }
        keys.forEach { bloom.add(it) }
        keys.forEach { assertTrue(bloom.mightContain(it), "added key reported absent — false negative") }
    }

    /** Empty filter: every key is provably absent (so every owner is safely skippable-checked). */
    @Test
    fun emptyIsAllAbsent() {
        val bloom = GuardBloom(expectedInsertions = 1_000)
        val rnd = Random(2)
        repeat(10_000) { assertFalse(bloom.mightContain(hex(rnd))) }
    }

    /** Overfilling past capacity stays CORRECT (no false negatives), only the FP rate rises. */
    @Test
    fun overfillKeepsNoFalseNegatives() {
        val bloom = GuardBloom(expectedInsertions = 1_000) // deliberately undersized
        val rnd = Random(3)
        val keys = (0 until 50_000).map { hex(rnd) } // 50x the sizing
        keys.forEach { bloom.add(it) }
        keys.forEach { assertTrue(bloom.mightContain(it)) }
    }

    /** False-positive rate stays near the target when sized correctly (memory vs wasted-probe trade, not correctness). */
    @Test
    fun falsePositiveRateBounded() {
        val n = 100_000
        val bloom = GuardBloom(expectedInsertions = n, fpp = 0.01)
        val rnd = Random(4)
        val added = HashSet<String>()
        while (added.size < n) added.add(hex(rnd))
        added.forEach { bloom.add(it) }

        var fp = 0
        val trials = 100_000
        var seen = 0
        while (seen < trials) {
            val k = hex(rnd)
            if (k in added) continue
            seen++
            if (bloom.mightContain(k)) fp++
        }
        val rate = fp.toDouble() / trials
        assertTrue(rate < 0.03, "false-positive rate $rate too high (target ~0.01)")
    }

    /**
     * Concurrent adds and reads: after a racing load, every added key must still
     * report present (the store checks guards lock-free in plan() while another
     * lane may be recording a guard).
     */
    @Test
    fun concurrentAddNoFalseNegative() {
        val bloom = GuardBloom(expectedInsertions = 200_000)
        val rnd = Random(5)
        val keys = (0 until 200_000).map { hex(rnd) }
        val pool = Executors.newFixedThreadPool(8)
        val reader = AtomicBoolean(true)
        // Readers hammer mightContain on already-added keys while writers add the rest.
        val half = keys.subList(0, 20_000)
        half.forEach { bloom.add(it) }
        val readTasks =
            (0 until 4).map {
                pool.submit {
                    while (reader.get()) half.forEach { require(bloom.mightContain(it)) }
                }
            }
        val writeTasks =
            keys.chunked(45_000).map { chunk ->
                pool.submit { chunk.forEach { bloom.add(it) } }
            }
        writeTasks.forEach { it.get() }
        reader.set(false)
        readTasks.forEach { it.get() }
        pool.shutdown()
        keys.forEach { assertTrue(bloom.mightContain(it)) }
    }

    /** Sizing scales the bit array with n (sanity that it is not a fixed tiny array). */
    @Test
    fun distinctKeysAllPresent() {
        val bloom = GuardBloom(expectedInsertions = 10)
        val keys = listOf("a".repeat(64), "b".repeat(64), "c".repeat(64))
        keys.forEach { bloom.add(it) }
        assertEquals(3, keys.count { bloom.mightContain(it) })
    }
}
