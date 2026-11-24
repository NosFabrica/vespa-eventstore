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
package com.nosfabrica.vespa.eventstore.engine.app

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class AuthorShardsTest {
    private val hex = "0123456789abcdef"

    private fun key(
        random: Random,
        middle: String? = null,
    ): String {
        val k = CharArray(64) { hex[random.nextInt(16)] }
        middle?.forEachIndexed { i, c -> k[32 + i] = c }
        return String(k)
    }

    @Test
    fun `the bucket is the pubkey's middle byte, whatever its ends are`() {
        val middle = "00000000000000000000000000000000" + "7f" + "000000000000000000000000000000"
        assertEquals(0x7f, AuthorShards.bucketOf(middle))
        // A vanity key: the same middle under a ground prefix and suffix lands in the same bucket.
        val vanity = "deadbeefdeadbeefdeadbeefdeadbeef" + "7f" + "c0ffeec0ffeec0ffeec0ffeec0ffee"
        assertEquals(0x7f, AuthorShards.bucketOf(vanity))
        assertEquals(AuthorShards.shardOf(middle, 4), AuthorShards.shardOf(vanity, 4))
    }

    @Test
    fun `a key with no hex at the middle byte goes to shard 0`() {
        assertEquals(null, AuthorShards.bucketOf("short"))
        assertEquals(null, AuthorShards.bucketOf("0".repeat(32) + "7F" + "0".repeat(30)), "uppercase is not the stored spelling")
        assertEquals(0, AuthorShards.shardOf("dirt-ledger-marker", 8))
        assertEquals(0, AuthorShards.shardOf("0".repeat(32) + "zz" + "0".repeat(30), 8))
    }

    @Test
    fun `every bucket belongs to exactly one shard, and every shard owns a contiguous run`() {
        for (shards in listOf(1, 2, 3, 5, 7, 16, 100, 255, 256)) {
            val owned = (0 until shards).map { AuthorShards.bucketsOf(it, shards) }
            assertEquals((0 until 256).toList(), owned.flatMap { it.toList() }, "$shards shards")
            for (b in 0 until 256) assertEquals(b * shards / 256, owned.indexOfFirst { b in it })
        }
        assertFailsWith<IllegalArgumentException> { AuthorShards.shardOf("x", 0) }
        assertFailsWith<IllegalArgumentException> { AuthorShards.shardOf("x", 257) }
    }

    /**
     * The selection Vespa routes by and [AuthorShards.shardOf] must name the same shard for every
     * key, or an event and its author's reputation document could part (both selections are the
     * same expression over the same string, so one agreement covers both). Java's regex stands in
     * for the container's, which is Java too; the content node's GC re-evaluation is checked by
     * `AuthorShardsIT` against a real Vespa.
     */
    @Test
    fun `the cluster selections claim exactly the keys shardOf assigns them`() {
        val random = Random(7)
        val keys =
            (0 until 2_000).map { key(random) } +
                (0 until 256).map { b -> key(random, "%02x".format(b)) } +
                listOf("dirt-ledger-marker", "", "0".repeat(32) + "G0" + "0".repeat(30), "0".repeat(33))
        for (shards in listOf(2, 3, 4, 16, 256)) {
            val patterns = (0 until shards).map { selectionMatcher(AuthorShards.selection("v", it, shards)) }
            for (k in keys) {
                val claimed = patterns.indices.filter { patterns[it](k) }
                assertEquals(listOf(AuthorShards.shardOf(k, shards)), claimed, "key '$k' under $shards shards")
            }
        }
    }

    @Test
    fun `cluster names keep the bundled cluster as shard 0`() {
        assertEquals(listOf("content"), AuthorShards.clusters(1))
        assertEquals(listOf("content", "content1", "content2"), AuthorShards.clusters(3))
    }

    /** Evaluate the two selection shapes [AuthorShards.selection] emits over the value `v`. */
    private fun selectionMatcher(selection: String): (String) -> Boolean {
        val regexes = Regex("v =~ \"([^\"]*)\"").findAll(selection).map { Regex(it.groupValues[1]) }.toList()
        return when (regexes.size) {
            1 -> { v -> regexes[0].containsMatchIn(v) }
            2 -> { v -> regexes[0].containsMatchIn(v) || !regexes[1].containsMatchIn(v) }
            else -> error("unexpected selection $selection")
        }
    }
}
