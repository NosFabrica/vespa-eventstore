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
package com.vitorpamplona.quartz.eventstore.benchmark

import com.vitorpamplona.quartz.nip01Core.core.Event
import kotlinx.serialization.json.add
import kotlinx.serialization.json.addJsonArray
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.random.Random

/**
 * A reproducible, relay-shaped corpus of Nostr events — the "list of events" the
 * benchmarks hammer. Both stores under test are fed the SAME [Event] instances,
 * so any throughput difference is the store's, not the input's.
 *
 * Why generate rather than replay a capture: the benchmark must be deterministic
 * (a [Config.seed] fixes every id, author, timestamp and kind) and must exercise
 * the paths that actually cost the store — replaceable supersession, addressable
 * d-tag resolution, and NIP-09 deletions of real prior targets — not just a flat
 * stream of inserts. Quartz has no throughput benchmark to borrow (its closest,
 * `ParallelInsertTest`, is a 1,600-event concurrency-correctness test with no
 * timing), so the mix here is modelled on a general relay's kind distribution.
 *
 * Events are built as canonical JSON and parsed through Quartz's [Event.fromJson]
 * so each lands as its proper subclass (kind 5 -> DeletionEvent, kind 0 ->
 * MetadataEvent, ...). That matters: the store dispatches on the runtime type,
 * so a bare `Event(kind = 5, ...)` would skip deletion handling entirely.
 *
 * The events carry a real-length (128-hex) but fake signature: relay traffic is
 * overwhelmingly signed, and neither store verifies signatures (that is the
 * ingest path's job, upstream). An empty `sig` would be a valid rumor too, but
 * real Vespa omits empty summary fields — see BUG in the module README — so a
 * signed-shaped corpus is both more representative and reads back cleanly.
 */
object NostrCorpus {
    /** A general-relay kind mix (weights are relative, not percentages). */
    private val MIX =
        listOf(
            Kind.NOTE to 50, // kind 1  text note — the bulk of a relay
            Kind.REACTION to 25, // kind 7  reaction (e/p tag a prior note)
            Kind.REPOST to 4, // kind 6  repost
            Kind.METADATA to 4, // kind 0  profile (REPLACEABLE — supersession churn)
            Kind.CONTACTS to 3, // kind 3  follow list (REPLACEABLE)
            Kind.DELETION to 4, // kind 5  deletion of a prior same-author event
            Kind.RELAY_LIST to 2, // kind 10002 relay list (REPLACEABLE)
            Kind.LONG_FORM to 3, // kind 30023 article (ADDRESSABLE by d tag)
        )

    private enum class Kind { NOTE, REACTION, REPOST, METADATA, CONTACTS, DELETION, RELAY_LIST, LONG_FORM }

    /** Words that show up in note/article content, so NIP-50 search has real terms to match. */
    private val VOCAB =
        (
            "nostr bitcoin lightning coffee freedom relay zap note purple ostrich " +
                "signal privacy sovereign mesh garden protocol client vespa quartz search " +
                "trust graph web ranking payload consensus block mempool node wallet key"
        ).split(" ")

    data class Config(
        val size: Int,
        val authorCount: Int = (size / 20).coerceIn(50, 50_000),
        val seed: Long = 42L,
        /** First created_at; each event advances the clock by a small jittered step. */
        val startTime: Long = 1_700_000_000L,
        /**
         * High nibble of every event id. Vary it to mint a second, id-disjoint
         * corpus against a shared store (e.g. the per-event insert probe on a real
         * Vespa already loaded with the main corpus) without dedup collisions.
         */
        val idBand: Long = 0x1L,
    )

    /** The parts of an event this generator varies; [event] serializes them to canonical JSON. */
    private class Draft(
        val kind: Int,
        val tags: List<List<String>>,
        val content: String,
    )

    /**
     * Build [Config.size] events. Deterministic for a given config. Replaceable
     * and addressable kinds deliberately revisit earlier (author) / (author,d)
     * pairs so the store's supersession path runs; deletions target a real prior
     * event of the same author so NIP-09 erasure runs.
     */
    fun generate(config: Config): List<Event> {
        val rnd = Random(config.seed)
        val authors = List(config.authorCount) { pubkey(it) }
        // Per-author recent event ids (for deletions to target their own history).
        val recentByAuthor = HashMap<String, ArrayDeque<String>>()
        // (author, id) for cross-author references (replies, reactions, reposts).
        val allRecent = ArrayDeque<Pair<String, String>>()
        val dTagsByAuthor = HashMap<String, MutableList<String>>()

        val out = ArrayList<Event>(config.size)
        var clock = config.startTime
        var counter = 0L
        val picker = WeightedPicker(MIX, rnd)

        while (out.size < config.size) {
            clock += rnd.nextInt(1, 8)
            val author = authors[rnd.nextInt(authors.size)]
            val id = hex64(config.idBand, counter++)

            val draft =
                when (picker.next()) {
                    Kind.NOTE -> {
                        note(rnd, allRecent)
                    }

                    Kind.REACTION -> {
                        reaction(rnd, allRecent)
                    }

                    Kind.REPOST -> {
                        repost(rnd, allRecent)
                    }

                    Kind.METADATA -> {
                        metadata(rnd)
                    }

                    Kind.CONTACTS -> {
                        contacts(rnd, authors)
                    }

                    Kind.RELAY_LIST -> {
                        relayList()
                    }

                    Kind.LONG_FORM -> {
                        longForm(rnd, dTagsByAuthor.getOrPut(author) { ArrayList() })
                    }

                    Kind.DELETION -> {
                        // No prior same-author target yet -> emit a note instead, so the
                        // corpus size stays exact and deletions always erase something real.
                        recentByAuthor[author]?.lastOrNull()?.let { deletion(it) } ?: note(rnd, allRecent)
                    }
                }

            out += Event.fromJson(event(id, author, clock, draft))

            // Deletions target a prior NON-deletion event by the same author.
            // A deletion targeting a deletion is a NIP-09 no-op ("a deletion request
            // against a deletion request has no effect") — which THIS store honors
            // but Quartz's SQLite store does not (it erases the earlier deletion).
            // Keeping that case out of the corpus lets store<->SQLite parity be a
            // clean pass; the divergence is called out in the module README.
            if (draft.kind != 5) {
                val q = recentByAuthor.getOrPut(author) { ArrayDeque() }
                q.addLast(id)
                if (q.size > 8) q.removeFirst()
            }
            allRecent.addLast(author to id)
            if (allRecent.size > 4096) allRecent.removeFirst()
        }
        return out
    }

    // ---- per-kind drafts ----------------------------------------------------

    private fun note(
        rnd: Random,
        recent: ArrayDeque<Pair<String, String>>,
    ): Draft {
        val tags = ArrayList<List<String>>()
        // ~30% of notes are replies: e-tag a prior note + p-tag its author.
        if (rnd.nextInt(100) < 30 && recent.isNotEmpty()) {
            val (a, i) = recent.elementAt(rnd.nextInt(recent.size))
            tags += listOf("e", i)
            tags += listOf("p", a)
        }
        // ~20% carry a Capitalized hashtag. NIP-01 tag matching is byte-exact
        // (SQLite hashes the exact bytes), and Vespa string attributes match
        // UNCASED unless the schema says `match: cased` — so the corpus keeps
        // mixed-case tag values in play and the parity battery asserts both
        // stores treat case identically. The lowercase form never occurs as a
        // t-tag, so a lowercased filter must match nothing on both stores.
        if (rnd.nextInt(100) < 20) {
            tags += listOf("t", VOCAB[rnd.nextInt(VOCAB.size)].replaceFirstChar { it.uppercase() })
        }
        return Draft(1, tags, sentence(rnd, 6, 24))
    }

    private fun reaction(
        rnd: Random,
        recent: ArrayDeque<Pair<String, String>>,
    ): Draft {
        val tags = targetTags(rnd, recent)
        return Draft(7, tags, if (rnd.nextBoolean()) "+" else "🤙")
    }

    private fun repost(
        rnd: Random,
        recent: ArrayDeque<Pair<String, String>>,
    ): Draft = Draft(6, targetTags(rnd, recent), "")

    private fun metadata(rnd: Random): Draft {
        val name = VOCAB[rnd.nextInt(VOCAB.size)] + rnd.nextInt(1000)
        return Draft(0, emptyList(), """{"name":"$name","about":"${sentence(rnd, 4, 10)}"}""")
    }

    private fun contacts(
        rnd: Random,
        authors: List<String>,
    ): Draft {
        val follows = 5 + rnd.nextInt(40)
        val tags = List(follows) { listOf("p", authors[rnd.nextInt(authors.size)]) }
        return Draft(3, tags, "")
    }

    private fun relayList(): Draft = Draft(10002, listOf(listOf("r", "wss://relay.example.com"), listOf("r", "wss://nos.lol")), "")

    private fun longForm(
        rnd: Random,
        priorDTags: MutableList<String>,
    ): Draft {
        // ~40% of articles are edits of an existing slug (addressable supersession).
        val d =
            if (priorDTags.isNotEmpty() && rnd.nextInt(100) < 40) {
                priorDTags[rnd.nextInt(priorDTags.size)]
            } else {
                ("slug-" + VOCAB[rnd.nextInt(VOCAB.size)] + "-" + rnd.nextInt(10_000)).also { priorDTags += it }
            }
        val tags = listOf(listOf("d", d), listOf("title", sentence(rnd, 2, 5)))
        return Draft(30023, tags, sentence(rnd, 40, 120))
    }

    private fun deletion(targetId: String): Draft = Draft(5, listOf(listOf("e", targetId)), "deleted")

    /** An e/p tag pair pointing at a random recent event (for reactions/reposts). */
    private fun targetTags(
        rnd: Random,
        recent: ArrayDeque<Pair<String, String>>,
    ): List<List<String>> {
        if (recent.isEmpty()) return emptyList()
        val (a, i) = recent.elementAt(rnd.nextInt(recent.size))
        return listOf(listOf("e", i), listOf("p", a))
    }

    // ---- primitives ---------------------------------------------------------

    private fun event(
        id: String,
        pubkey: String,
        createdAt: Long,
        draft: Draft,
    ): String =
        buildJsonObject {
            put("id", id)
            put("pubkey", pubkey)
            put("created_at", createdAt)
            put("kind", draft.kind)
            put(
                "tags",
                buildJsonArray {
                    draft.tags.forEach { tag ->
                        addJsonArray { tag.forEach { add(it) } }
                    }
                },
            )
            put("content", draft.content)
            // A real 128-hex (64-byte schnorr) signature shape. Events on a relay are
            // overwhelmingly signed, so the corpus is signed too; the store never
            // verifies it. (Deterministic, derived from the id — no real crypto needed.)
            put("sig", id + id)
        }.toString()

    private fun sentence(
        rnd: Random,
        min: Int,
        max: Int,
    ): String {
        val n = min + rnd.nextInt((max - min).coerceAtLeast(1))
        return (0 until n).joinToString(" ") { VOCAB[rnd.nextInt(VOCAB.size)] }
    }

    /** Author pubkeys live in a high hex band so they never collide with event ids. */
    private fun pubkey(i: Int): String = hex64(0xAL, i.toLong())

    private fun hex64(
        band: Long,
        n: Long,
    ): String {
        val tail = n.toString(16)
        val head = band.toString(16)
        return head + "0".repeat(64 - head.length - tail.length) + tail
    }

    /** Weighted choice from a fixed table (small table, linear scan is fine). */
    private class WeightedPicker<T>(
        weighted: List<Pair<T, Int>>,
        private val rnd: Random,
    ) {
        private val items = weighted.map { it.first }
        private val cumulative = IntArray(weighted.size)
        private val total: Int

        init {
            var acc = 0
            weighted.forEachIndexed { i, (_, w) ->
                acc += w
                cumulative[i] = acc
            }
            total = acc
        }

        fun next(): T {
            val r = rnd.nextInt(total)
            return items[cumulative.indexOfFirst { r < it }]
        }
    }
}
