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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.doc.SearchFields
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.system.measureNanoTime

/**
 * NIP-50 search latency across the shapes a relay actually serves: rare and
 * common terms, multi-word queries, the trigram/fuzzy net (short and
 * misspelled words), search combined with kind/window/author filters, profile
 * (kind-0) directory lookups, and the `text` vs `text2` two-phase profile A/B
 * with a rerank-count sweep.
 *
 * Feeds its own DETERMINISTIC text corpus once (idempotent — a completion
 * marker doc is written last and checked first): [BENCH_SEARCH_DOCS] kind-1
 * notes whose content is zipfian sentences over a pronounceable synthetic
 * vocabulary (so BM25 sees realistic document-frequency spread and the
 * trigram fields see real substrings), 15% carrying a hashtag, 25% a #p
 * mention, plus 5k kind-0 profiles with searchable names. created_at sits
 * BELOW the visit corpus's BASE so the two bands stay distinguishable.
 *
 * Correctness gate: 37 sentinel docs contain a token that appears nowhere
 * else; the sentinel search must return EXACTLY those 37 — a full-recall
 * assertion through the entire search stack.
 *
 * Env: VESPA_URL (default http://localhost:8080), BENCH_SEARCH_DOCS (200000),
 * BENCH_SEARCH_REPS (15), BENCH_SEED (42).
 */
object SearchBench {
    private const val BASE = 1_600_000_000L // below VisitBench's 1.7e9 band
    private const val PROFILES = 5_000
    private const val SENTINELS = 37
    private const val SENTINEL_TOKEN = "zqsentinelmark"

    private var failures = 0

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val docs = System.getenv("BENCH_SEARCH_DOCS")?.toIntOrNull() ?: 200_000
            val reps = System.getenv("BENCH_SEARCH_REPS")?.toIntOrNull() ?: 15
            val seed = System.getenv("BENCH_SEED")?.toLongOrNull() ?: 42L
            val index = VespaEventIndex(url)
            try {
                val vocab = vocabulary(seed)
                feedOnce(index, docs, vocab, seed)

                println()
                println(String.format("%-34s %10s %10s %10s %8s", "search shape", "p50", "p90", "max", "hits"))

                val common = vocab[0]
                val mid = vocab[40]
                val rare = vocab[vocab.size - 5]
                val short = "zap" // <= 3 chars -> leans on the trigram net
                val typo = common.dropLast(1) + "x" // not in the vocab; trigram fallback only

                shape(index, "rare term", reps, EventQuery(kinds = listOf(1), search = rare, limit = 50), expectHits = true)
                shape(index, "common term", reps, EventQuery(kinds = listOf(1), search = common, limit = 50), expectHits = true)
                shape(index, "two words", reps, EventQuery(kinds = listOf(1), search = "$common $mid", limit = 50), expectHits = true)
                shape(
                    index,
                    "four words",
                    reps,
                    EventQuery(kinds = listOf(1), search = "$common $mid ${vocab[7]} ${vocab[90]}", limit = 50),
                    expectHits = true,
                )
                shape(index, "short word (gram-lean)", reps, EventQuery(kinds = listOf(1), search = short, limit = 50), expectHits = false)
                shape(index, "misspelled (trigram net)", reps, EventQuery(kinds = listOf(1), search = typo, limit = 50), expectHits = false)
                shape(
                    index,
                    "search + window",
                    reps,
                    EventQuery(kinds = listOf(1), search = common, since = BASE, until = BASE + docs / 2, limit = 50),
                    expectHits = true,
                )
                shape(
                    index,
                    "search + author",
                    reps,
                    EventQuery(kinds = listOf(1), search = common, authors = listOf(author(3)), limit = 50),
                    expectHits = false,
                )
                shape(index, "common term limit=1000", reps, EventQuery(kinds = listOf(1), search = common, limit = 1_000), expectHits = true)
                shape(index, "profile directory (kind 0)", reps, EventQuery(kinds = listOf(0), search = "name${seed}17", limit = 20), expectHits = false)

                // Full-recall gate: the sentinel token exists in EXACTLY 37 docs.
                val found = index.search(EventQuery(kinds = listOf(1), search = SENTINEL_TOKEN, limit = 100))
                expect(found.size == SENTINELS) { "sentinel recall: got ${found.size}, expected $SENTINELS" }
                println(String.format("%-34s %40s", "sentinel full recall", "${found.size}/$SENTINELS docs found"))

                // --- text vs text2 (two-phase) profile A/B, same query ---------
                println()
                println("profile A/B (same two-word query, limit=50)")
                for ((label, q) in listOf(
                    // Both pinned explicitly — the DEFAULT is text2 now, so the
                    // single-phase side must name its profile to stay measured.
                    "text (single-phase)" to EventQuery(kinds = listOf(1), search = "$common $mid", limit = 50, ranking = "text"),
                    "text2 (two-phase, rerank 1000)" to EventQuery(kinds = listOf(1), search = "$common $mid", limit = 50, ranking = "text2"),
                    "text2 rerank=100" to EventQuery(kinds = listOf(1), search = "$common $mid", limit = 50, ranking = "text2", rerankCount = 100),
                )) {
                    shape(index, label, reps, q, expectHits = true)
                }

                println()
                if (failures == 0) println("all search gates passed") else error("$failures search correctness failure(s)")
            } finally {
                index.close()
            }
        }

    /** Pronounceable deterministic vocabulary — real substrings for the trigram fields, zipf-ranked by index. */
    private fun vocabulary(seed: Long): List<String> {
        val rnd = Random(seed)
        val cons = listOf("b", "d", "f", "g", "k", "l", "m", "n", "p", "r", "s", "t", "v", "z", "ch", "st", "tr")
        val vow = listOf("a", "e", "i", "o", "u", "ai", "ou")
        return (0 until 1_500)
            .map {
                val syllables = 2 + rnd.nextInt(3)
                buildString { repeat(syllables) { append(cons.random(rnd)).append(vow.random(rnd)) } }
            }.distinct()
    }

    private fun author(k: Int) = k.toString(16).padStart(64, '5')

    private fun zipf(
        rnd: Random,
        size: Int,
    ): Int {
        val r = rnd.nextDouble()
        return ((r * r * r) * size).toInt().coerceAtMost(size - 1)
    }

    private suspend fun feedOnce(
        index: VespaEventIndex,
        docs: Int,
        vocab: List<String>,
        seed: Long,
    ) {
        val marker = "5eac".padEnd(64, 'f')
        if (index.get(marker) != null) {
            println("search corpus already loaded ($docs docs)")
            return
        }
        println("feeding search corpus: $docs notes + $PROFILES profiles ...")
        val rnd = Random(seed)
        val hashtags = (0 until 300).map { "tag${vocab[zipf(rnd, vocab.size)]}$it" }
        val sentinelAt = (0 until docs).shuffled(Random(seed)).take(SENTINELS).toHashSet()
        var fed = 0
        val batch = ArrayList<EventDoc>(2_000)

        suspend fun flush() {
            if (batch.isNotEmpty()) {
                index.putAll(batch)
                fed += batch.size
                batch.clear()
                if (fed % 50_000 < 2_000) println("  fed $fed")
            }
        }
        repeat(docs) { i ->
            val words = (8 + rnd.nextInt(13)).let { n -> List(n) { vocab[zipf(rnd, vocab.size)] } }
            val content = (if (i in sentinelAt) words + SENTINEL_TOKEN else words).joinToString(" ")
            val tags = ArrayList<List<String>>(2)
            if (rnd.nextInt(100) < 15) tags += listOf("t", hashtags[zipf(rnd, hashtags.size)])
            if (rnd.nextInt(100) < 25) tags += listOf("p", author(rnd.nextInt(300)))
            batch +=
                EventDoc(
                    id = "5eac" + i.toString(16).padStart(60, '0'),
                    pubkey = author(i % 300),
                    createdAt = BASE + i,
                    kind = 1,
                    tags = tags,
                    content = content,
                    sig = "e".repeat(128),
                    search = SearchFields(text = content),
                )
            if (batch.size >= 2_000) flush()
        }
        repeat(PROFILES) { i ->
            batch +=
                EventDoc(
                    id = "5eac" + (0x10000000 + i).toString(16).padStart(60, '0'),
                    pubkey = author(300 + i),
                    createdAt = BASE + i,
                    kind = 0,
                    tags = emptyList(),
                    content = "{}",
                    sig = "e".repeat(128),
                    search = SearchFields(name = "name$seed$i", about = List(6) { vocab[zipf(Random(seed + i), vocab.size)] }.joinToString(" ")),
                )
            if (batch.size >= 2_000) flush()
        }
        flush()
        // Completion marker LAST: a crashed partial feed re-feeds (dedup makes it cheap).
        index.put(
            EventDoc(marker, author(0), BASE, 1, emptyList(), "search bench feed complete", "e".repeat(128)),
        )
        println("search corpus loaded")
    }

    private suspend fun shape(
        index: VespaEventIndex,
        label: String,
        reps: Int,
        q: EventQuery,
        expectHits: Boolean,
    ) {
        var hits = 0
        val lat = LongArray(reps) { measureNanoTime { runBlocking { hits = index.search(q).size } } }
        lat.sort()
        if (expectHits) expect(hits > 0) { "$label returned no hits" }
        println(
            String.format(
                "%-34s %8.2fms %8.2fms %8.2fms %8d",
                label,
                lat[reps / 2] / 1e6,
                lat[(reps * 9) / 10] / 1e6,
                lat[reps - 1] / 1e6,
                hits,
            ),
        )
    }

    private inline fun expect(
        ok: Boolean,
        msg: () -> String,
    ) {
        if (!ok) {
            failures++
            println("  !! ${msg()}")
        }
    }
}
