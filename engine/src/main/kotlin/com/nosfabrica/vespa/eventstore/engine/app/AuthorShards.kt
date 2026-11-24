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

    /** `VESPA_AUTHOR_BUCKETS`: an explicit [BucketMap], for an uneven split or a move in flight. */
    const val BUCKETS_ENV = "VESPA_AUTHOR_BUCKETS"

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

    /** The shard of [shards] (an even split) that holds [pubkey]'s events and reputation document. */
    fun shardOf(
        pubkey: String,
        shards: Int,
    ): Int = BucketMap.even(shards).shardOf(pubkey)

    /** Shard [index]'s content cluster id: shard 0 is [BASE_CLUSTER], the rest `content1`, `content2`, … */
    fun clusterName(index: Int): String = if (index == 0) BASE_CLUSTER else "$BASE_CLUSTER$index"

    /** Every shard's cluster id, in shard order. */
    fun clusters(shards: Int): List<String> {
        require(shards in 1..MAX) { "shards $shards is outside 1..$MAX" }
        return (0 until shards).map(::clusterName)
    }

    /** The buckets shard [index] of [shards] owns under an even split. */
    fun bucketsOf(
        index: Int,
        shards: Int,
    ): IntRange {
        require(shards in 1..MAX && index in 0 until shards) { "shard $index of $shards" }
        val owned = BucketMap.even(shards).bucketsOf(index)
        return owned.first()..owned.last()
    }

    /** [BucketMap.selection] under an even split of [shards]. */
    fun selection(
        value: String,
        index: Int,
        shards: Int,
    ): String = BucketMap.even(shards).selection(value, index)

    /**
     * Which content cluster holds each of the 256 buckets: an explicit table, so a split need not
     * be even (three nodes in one cluster, two in another take 60% and 40% of the buckets) and a new
     * cluster takes only the buckets given to it, moving about 1/(K+1) of the authors instead of
     * the most that `b * K / 256` would move.
     *
     * [owner] is the cluster that SERVES a bucket: writes, reads, scoped queries. [also] is a second
     * cluster that RECEIVES the bucket's writes while a move fills it, or -1. A cluster that owns no
     * bucket outright and only receives is PENDING: it takes writes and is read by no one, so the
     * copy being built never shows up twice. A move onto an existing serving cluster would be read
     * twice, so moves go onto pending clusters (`docs/author-shards.md`, "Moving buckets").
     *
     * Written as `00-99=0,9a-ff=1`, an overlap as `9a-ff=1+2` (owner 1, also 2), hex ranges
     * inclusive, every bucket exactly once.
     */
    class BucketMap private constructor(
        private val owner: IntArray,
        private val also: IntArray,
    ) {
        init {
            require(owner.size == 256 && also.size == 256) { "a bucket map has 256 buckets" }
            for (b in 0 until 256) require(also[b] != owner[b]) { "bucket ${hex(b)} lists cluster ${owner[b]} twice" }
        }

        /** How many content clusters the map names, pending ones included. */
        val clusters: Int = maxOf(owner.max(), also.max()) + 1

        init {
            require(clusters <= MAX) { "a bucket map names at most $MAX clusters" }
            for (c in 0 until clusters) require(owner.any { it == c } || also.any { it == c }) { "cluster $c holds no bucket" }
            require(owner.any { it == 0 }) { "cluster 0 must serve: it holds the keys that are not hex" }
        }

        /** Clusters that serve at least one bucket, in index order. */
        val serving: List<Int> = (0 until clusters).filter { c -> owner.any { it == c } }

        /** Clusters that only receive: being filled by a move, read by no one. */
        val pending: List<Int> = (0 until clusters).filter { it !in serving }

        /** Whether any bucket is being written to two clusters. */
        val moving: Boolean get() = also.any { it >= 0 }

        /** The cluster that serves [pubkey]: its bucket's owner, or 0 for a key that is not hex through the bucket byte. */
        fun shardOf(pubkey: String): Int = bucketOf(pubkey)?.let { owner[it] } ?: 0

        /** The cluster that serves [bucket]. */
        fun ownerOf(bucket: Int): Int = owner[bucket]

        /** Every bucket cluster [index] keeps — serves or receives — ascending. */
        fun bucketsOf(index: Int): List<Int> = (0 until 256).filter { owner[it] == index || also[it] == index }

        /** The buckets cluster [index] keeps under this map and does NOT under [next]: what [next] would make its garbage collection delete. */
        fun narrowedBy(
            next: BucketMap,
            index: Int,
        ): List<Int> = bucketsOf(index).filter { b -> next.owner[b] != index && next.also[b] != index }

        /**
         * The document-selection test that [value] (`event.pubkey`, `id.specific`) belongs to
         * cluster [index]: hex through [BUCKET_BYTE] with that byte among the cluster's buckets,
         * plus, on cluster 0, everything that is not hex through it. Only ASCII classes, a counted
         * repeat and alternation — see [AuthorShards] for why never `.`.
         */
        fun selection(
            value: String,
            index: Int,
        ): String {
            val alternatives =
                bucketsOf(index)
                    .groupBy { it / 16 }
                    .map { (hi, buckets) -> "${HEX[hi]}[${buckets.joinToString("") { HEX[it % 16].toString() }}]" }
            val mine = "$value =~ \"^[$HEX]{$CHAR}(${alternatives.joinToString("|")})\""
            return if (index == 0) "($mine or not ($value =~ \"^[$HEX]{${CHAR + 2}}\"))" else "($mine)"
        }

        /** The map as [parse] reads it: runs of consecutive buckets with the same owners. */
        fun spec(): String {
            val out = ArrayList<String>()
            var start = 0
            for (b in 1..256) {
                if (b == 256 || owner[b] != owner[start] || also[b] != also[start]) {
                    val range = if (start == b - 1) hex(start) else "${hex(start)}-${hex(b - 1)}"
                    out += "$range=${owner[start]}" + (if (also[start] >= 0) "+${also[start]}" else "")
                    start = b
                }
            }
            return out.joinToString(",")
        }

        override fun equals(other: Any?): Boolean = other is BucketMap && owner.contentEquals(other.owner) && also.contentEquals(other.also)

        override fun hashCode(): Int = owner.contentHashCode() * 31 + also.contentHashCode()

        override fun toString(): String = spec()

        companion object {
            /** [clusters] clusters, each owning a contiguous, near-equal run: shard `b * K / 256`. */
            fun even(clusters: Int): BucketMap {
                require(clusters in 1..MAX) { "shards $clusters is outside 1..$MAX" }
                return BucketMap(IntArray(256) { it * clusters / 256 }, IntArray(256) { -1 })
            }

            /** Read a map written as [spec] writes it; refused unless every bucket is named exactly once. */
            fun parse(spec: String): BucketMap {
                val owner = IntArray(256) { -1 }
                val also = IntArray(256) { -1 }
                for (part in spec.split(',').map { it.trim() }.filter { it.isNotEmpty() }) {
                    val (range, who) = part.split('=').map { it.trim() }.takeIf { it.size == 2 } ?: throw IllegalArgumentException("'$part' is not range=cluster")
                    val (lo, hi) = range.split('-').map { parseBucket(it.trim(), part) }.let { if (it.size == 1) it[0] to it[0] else it[0] to it[1] }
                    require(lo <= hi) { "'$part': $range runs backwards" }
                    val owners = who.split('+').map { it.trim().toIntOrNull() ?: throw IllegalArgumentException("'$part': '$who' is not a cluster index") }
                    require(owners.size in 1..2) { "'$part': a bucket has one owner and at most one cluster receiving it" }
                    for (b in lo..hi) {
                        require(owner[b] < 0) { "bucket ${hex(b)} is named twice" }
                        owner[b] = owners[0]
                        also[b] = owners.getOrElse(1) { -1 }
                    }
                }
                val missing = (0 until 256).filter { owner[it] < 0 }
                require(missing.isEmpty()) { "buckets ${missing.take(5).joinToString { hex(it) }}${if (missing.size > 5) " …" else ""} have no cluster" }
                return BucketMap(owner, also)
            }

            private fun parseBucket(
                text: String,
                part: String,
            ): Int = text.lowercase().takeIf { it.length == 2 }?.toIntOrNull(16) ?: throw IllegalArgumentException("'$part': '$text' is not a two-digit hex bucket")

            private fun hex(b: Int) = "%02x".format(b)
        }
    }
}
