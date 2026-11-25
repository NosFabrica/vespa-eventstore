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
package com.nosfabrica.vespa.eventstore.benchmark

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.app.VespaApp
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.utils.EventFactory
import java.security.MessageDigest

/**
 * `-DitAuthorShards=N`: run an integration test against the PROTOTYPE author-sharded topology
 * (`docs/author-shards.md`) — N content clusters in the one Vespa container — instead of the
 * bundled single cluster. Unset is 1, the package as built, so the CI gates are unchanged:
 *
 * ```bash
 * ./gradlew :benchmark:test -Pintegration -DitAuthorShards=2 --tests '*VespaParityIT' --tests '*FilterMatrixIT'
 * ```
 *
 * The deployed package and the client must agree on the count, so a test takes both from here.
 */
internal object ItAuthorShards {
    val shards: Int = VespaApp.authorShardsOf(System.getProperty("itAuthorShards"))

    fun tuning(): EngineTuning = EngineTuning(authorShards = shards)

    fun zip(): ByteArray = VespaApp.zipBytes(null, shards)

    /**
     * [corpus] with every author's MIDDLE byte rewritten, on a sharded run only: quartz's
     * conformance corpus builds its keys as a tag, zero padding and a counter, so every one has
     * `00` at [AuthorShards.BUCKET_BYTE] and the whole corpus would sit in shard 0 — a parity run
     * that never crossed a cluster. Each key becomes the same key with that byte replaced by one
     * derived from the key, wherever the key appears (author, every tag value, the content: `p`
     * and `a` tags, deletions, vanishes), so the corpus keeps its shape and both stores under
     * comparison load the same rewritten corpus. Ids and signatures are left alone; neither store
     * verifies them.
     */
    fun spreadAuthors(corpus: List<Event>): List<Event> {
        if (shards == 1) return corpus
        val authors = corpus.map { it.pubKey }.toSet()
        val mapping =
            authors.associateWith { pk ->
                val byte = MessageDigest.getInstance("SHA-256").digest(pk.encodeToByteArray())[0].toInt() and 0xff
                pk.substring(0, 32) + "%02x".format(byte) + pk.substring(34)
            }
        check(mapping.values.toSet().size == mapping.size) { "the middle-byte rewrite collided two authors" }
        check(mapping.values.none { it in authors && mapping[it] != it }) { "the rewrite named another author's key" }

        fun rewrite(text: String): String {
            if (text.length < 64) return mapping[text] ?: text
            var out = text
            for ((from, to) in mapping) if (from in out) out = out.replace(from, to)
            return out
        }
        return corpus.map { e ->
            EventFactory.create(
                e.id,
                mapping.getValue(e.pubKey),
                e.createdAt,
                e.kind,
                Array(e.tags.size) { i -> Array(e.tags[i].size) { j -> rewrite(e.tags[i][j]) } },
                rewrite(e.content),
                e.sig,
            )
        }
    }

    /**
     * Refuse a fixture whose [authors] do not reach every shard: a corpus that lands in one
     * cluster passes on N clusters without testing anything about them. Prints the spread, since
     * how even it is says how much of the run crossed clusters.
     */
    fun requireSpread(authors: Collection<String>) {
        if (shards == 1) return
        val distinct = authors.toSet()
        val perShard = distinct.groupingBy { AuthorShards.shardOf(it, shards) }.eachCount().toSortedMap()
        println("author shards: ${distinct.size} authors over $shards clusters, per cluster ${(0 until shards).map { perShard[it] ?: 0 }}")
        check(perShard.size == shards) { "the fixture's authors reach only clusters ${perShard.keys} of $shards" }
    }
}
