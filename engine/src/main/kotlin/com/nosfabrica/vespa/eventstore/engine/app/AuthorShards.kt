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

/**
 * PROTOTYPE: the corpus split by AUTHOR across several Vespa content clusters, so each node holds
 * only the `reputation` documents of the authors whose events it holds (`docs/author-shards.md`).
 *
 * Why clusters and not one cluster's buckets: `reputation` must be `global` for `event.sd`'s
 * `author_ref` to import it, and a global document is copied to every node of ITS content
 * cluster, not of the deployment. Split the authors into K clusters, put each author's events and
 * reputation document in the same one, and the import still resolves locally while each node
 * carries 1/K of the trust tensors. Nothing about a query changes: the container fans every
 * search out to all clusters and merges the hits (measured; groupings it does NOT merge, see
 * `GroupingResults.mergeRoots`).
 *
 * Routing is SERVER-SIDE, by each cluster's `<document selection=…>`: an event by its `pubkey`
 * field, a reputation document by its id (the subject, so a cell update that carries no `pubkey`
 * field still routes). The client never picks a cluster for a write; only a visit must name one.
 *
 * The shard key is ONE MIDDLE BYTE of the pubkey, byte [BUCKET_BYTE] of 32 (hex characters
 * 32..33), never the first or the last: vanity keys are ground for a chosen prefix (an npub that
 * spells a word) or suffix, so the ends of a key are not uniform. A byte is 256 buckets; shard i of
 * K owns the contiguous buckets `b * K / 256 == i`. A key that is not lowercase hex through that
 * byte — a non-hex marker document the trust ledger keeps among the reputations, a malformed
 * pubkey — goes to shard 0, in both document types alike, so an event and its author's reputation
 * document never part. Vespa answers 200 for a document NO cluster's selection takes and stores
 * nothing, which is why the catch-all is not optional.
 *
 * "Hex THROUGH that byte", not "anything, then hex there": the selection is evaluated by two
 * regex engines — Java's in the container that routes, and the content node's when garbage
 * collection re-reads the selection — and `.` is not the same character set in both (Java's skips
 * `\r`, `\u0085`, `\u2028`, `\u2029`; the other skips only `\n`; and they count UTF-16 units against
 * code points differently). A key the router sent to shard 0 that the collector reads as another
 * shard's is a document garbage collection deletes. ASCII character classes mean one thing
 * everywhere, so the selections use nothing else.
 */
object AuthorShards {
    /** `VESPA_AUTHOR_SHARDS`: how many content clusters the authors are split across. Unset is 1, the package as built. */
    const val ENV = "VESPA_AUTHOR_SHARDS"

    /** The most shards the one-byte key can split into. */
    const val MAX = 256

    /** The pubkey byte that picks the bucket: the middle of 32. */
    const val BUCKET_BYTE = 16

    /** Where [BUCKET_BYTE] starts in the 64-character hex key. */
    private const val CHAR = BUCKET_BYTE * 2

    /** The content cluster the bundled package declares; shard 0 keeps its name, so one shard is the package unchanged. */
    const val BASE_CLUSTER = "content"

    private const val HEX = "0123456789abcdef"

    /** The bucket [pubkey] falls in, 0..255; null unless its first `2 * BUCKET_BYTE + 2` characters are all lowercase hex. */
    fun bucketOf(pubkey: String): Int? {
        if (pubkey.length < CHAR + 2) return null
        for (i in 0 until CHAR + 2) if (HEX.indexOf(pubkey[i]) < 0) return null
        return HEX.indexOf(pubkey[CHAR]) * 16 + HEX.indexOf(pubkey[CHAR + 1])
    }

    /** The shard of [shards] that holds [pubkey]'s events and reputation document. */
    fun shardOf(
        pubkey: String,
        shards: Int,
    ): Int {
        require(shards in 1..MAX) { "shards $shards is outside 1..$MAX" }
        return bucketOf(pubkey)?.let { it * shards / 256 } ?: 0
    }

    /** Shard [index]'s content cluster id: shard 0 is [BASE_CLUSTER], the rest `content1`, `content2`, … */
    fun clusterName(index: Int): String = if (index == 0) BASE_CLUSTER else "$BASE_CLUSTER$index"

    /** Every shard's cluster id, in shard order. */
    fun clusters(shards: Int): List<String> {
        require(shards in 1..MAX) { "shards $shards is outside 1..$MAX" }
        return (0 until shards).map(::clusterName)
    }

    /** The buckets shard [index] of [shards] owns. */
    fun bucketsOf(
        index: Int,
        shards: Int,
    ): IntRange {
        require(shards in 1..MAX && index in 0 until shards) { "shard $index of $shards" }
        val first = (0 until 256).first { it * shards / 256 == index }
        val last = (0 until 256).last { it * shards / 256 == index }
        return first..last
    }

    /**
     * The document-selection test that [value] (`event.pubkey`, `id.specific`) belongs to shard
     * [index]: hex through [BUCKET_BYTE] with that byte in the shard's run, plus, on shard 0,
     * everything that is not hex through it. Only ASCII classes, a counted repeat and
     * alternation — see the class KDoc for why never `.`.
     */
    fun selection(
        value: String,
        index: Int,
        shards: Int,
    ): String {
        val owned = bucketsOf(index, shards)
        val alternatives =
            owned
                .groupBy { it / 16 }
                .map { (hi, buckets) -> "${HEX[hi]}[${buckets.joinToString("") { HEX[it % 16].toString() }}]" }
        val mine = "$value =~ \"^[$HEX]{$CHAR}(${alternatives.joinToString("|")})\""
        return if (index == 0) "($mine or not ($value =~ \"^[$HEX]{${CHAR + 2}}\"))" else "($mine)"
    }
}
