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
package com.nosfabrica.vespa.eventstore.benchmark.load

import com.nosfabrica.vespa.eventstore.EngineTuning
import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import com.nosfabrica.vespa.eventstore.engine.client.ClientTuning
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import kotlinx.coroutines.runBlocking

/**
 * The corpus by AUTHOR BUCKET (the pubkey's middle byte, `AuthorShards`), read-only, before a
 * split is chosen (`docs/author-shards.md`): events and reputation documents per bucket, how
 * evenly the middle byte spreads them, how many reputation documents score a subject with no
 * stored event (the ones "trust only for authors" would drop), and, for each cluster count asked
 * for, a bucket map that splits the events evenly.
 *
 * Two full walks: every reputation document (fast, ~10^6) and every event projected to its
 * pubkey (on a production corpus, hours of background visiting). Run it off-peak.
 *
 * Env: VESPA_URL (default http://localhost:8080), BENCH_SPLITS (cluster counts to suggest maps
 * for, default "2,3,5"), VESPA_AUTHOR_SHARDS / VESPA_AUTHOR_BUCKETS for a sharded deployment.
 */
object BucketReport {
    @JvmStatic
    fun main(args: Array<String>): Unit =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val splits = (System.getenv("BENCH_SPLITS") ?: "2,3,5").split(",").map { it.trim().toInt() }
            // The store's own strict parse (AGENTS.md, "Operator settings"): a bad VESPA_AUTHOR_SHARDS
            // or VESPA_AUTHOR_BUCKETS is named and refused, never read as one cluster.
            val store = EngineTuning.fromEnv(System.getenv())
            val tuning = ClientTuning(authorShards = store.authorShards, authorBuckets = store.authorBuckets)

            val subjects = HashMap<String, Boolean>() // subject -> has an event
            val reputations = LongArray(BUCKETS + 1)
            VespaReputationIndex(url, tuning = tuning, eagerFeed = false).use { rep ->
                rep.visitPubkeys { page ->
                    page.forEach { p ->
                        subjects[p] = false
                        reputations[slot(p)]++
                    }
                    true
                }
            }
            println("reputation documents: ${subjects.size}")

            val events = LongArray(BUCKETS + 1)
            var walked = 0L
            VespaEventIndex(url, tuning = tuning, eagerFeed = false).use { index ->
                index.visitAuthors { page ->
                    page.forEach { a ->
                        events[slot(a)]++
                        if (a in subjects) subjects[a] = true
                    }
                    walked += page.size
                    if (walked % 10_000_000 < page.size) println("  events walked: $walked")
                    true
                }
            }
            val orphans = LongArray(BUCKETS + 1)
            subjects.forEach { (s, has) -> if (!has) orphans[slot(s)]++ }

            println(render(events, reputations, orphans))
            for (k in splits) println("even split of EVENTS over $k clusters: VESPA_AUTHOR_BUCKETS=${suggest(events, k).spec()}")
        }

    const val BUCKETS = 256

    /** The bucket of [pubkey], or [BUCKETS] for a key that is not hex through the bucket byte (always cluster 0). */
    fun slot(pubkey: String): Int = AuthorShards.bucketOf(pubkey) ?: BUCKETS

    /**
     * A map giving each of [k] clusters a contiguous run of buckets holding as close to 1/k of the
     * [events] as whole buckets allow. Cluster 0 also keeps the keys that are not hex.
     */
    fun suggest(
        events: LongArray,
        k: Int,
    ): AuthorShards.BucketMap {
        require(k in 1..BUCKETS) { "k $k" }
        val total = events.sum().coerceAtLeast(1)
        val owner = IntArray(BUCKETS)
        var cluster = 0
        var acc = events[BUCKETS].toDouble() // the non-hex keys sit in cluster 0
        for (b in 0 until BUCKETS) {
            val left = BUCKETS - b
            val clustersLeft = k - cluster
            // Move on when this cluster has its share, keeping at least one bucket for every cluster after it.
            // Never before bucket 0: cluster 0 must own one, however many non-hex keys it already holds.
            if (b > 0 && cluster < k - 1 && (acc >= total.toDouble() * (cluster + 1) / k || left == clustersLeft - 1)) cluster++
            owner[b] = cluster
            acc += events[b]
        }
        return AuthorShards.BucketMap.parse(spec(owner))
    }

    private fun spec(owner: IntArray): String {
        val out = ArrayList<String>()
        var start = 0
        for (b in 1..BUCKETS) {
            if (b == BUCKETS || owner[b] != owner[start]) {
                out += "%02x-%02x=%d".format(start, b - 1, owner[start])
                start = b
            }
        }
        return out.joinToString(",")
    }

    fun render(
        events: LongArray,
        reputations: LongArray,
        orphans: LongArray,
    ): String {
        val sb = StringBuilder()
        val hexEvents = events.take(BUCKETS)
        val mean = hexEvents.average()
        sb.appendLine("events: ${events.sum()} (${events[BUCKETS]} by keys that are not hex through the bucket byte)")
        sb.appendLine("events per bucket: min ${hexEvents.min()} / mean ${"%.0f".format(mean)} / max ${hexEvents.max()}  (max/mean ${"%.2f".format(hexEvents.max() / mean.coerceAtLeast(1.0))})")
        sb.appendLine("reputation documents: ${reputations.sum()}, of which ${orphans.sum()} score a subject with no stored event")
        sb.appendLine("bucket  events  reputations  without-events")
        for (b in 0..BUCKETS) {
            val label = if (b == BUCKETS) "other" else "%02x".format(b)
            sb.appendLine("%-6s %8d %11d %14d".format(label, events[b], reputations[b], orphans[b]))
        }
        return sb.toString()
    }
}
