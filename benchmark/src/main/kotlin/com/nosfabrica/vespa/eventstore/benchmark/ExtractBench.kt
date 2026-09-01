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

import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip30CustomEmoji.emojis
import com.vitorpamplona.quartz.nip50Search.SearchFieldExtractor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import java.nio.file.Files
import java.nio.file.Path
import kotlin.system.measureNanoTime

/**
 * THE WRITE PATH'S DERIVATION, IN ISOLATION: how long `SearchExtractors.extract`
 * takes per event, on a real corpus and on a corpus where every event carries a
 * NIP-30 badge.
 *
 * Every other bench here times a round trip to Vespa, which is milliseconds and
 * hides everything the JVM does on the way in. This one times nothing but the
 * derivation `EventDoc` needs — the per-kind decomposition, Vespa sanitization
 * and the shortcode rewrite — because that cost is paid once per event by every
 * ingest, and a batch feed does 500 of them between round trips.
 *
 * TWO CORPORA, because the question "are shortcodes expensive?" has two answers:
 *
 *  - `--corpus <capture.json>`: what a relay actually holds. Measured on a
 *    10 961-event staging slice, SEVEN events carry an `emoji` tag (0.06%),
 *    against 16.5 tags per event overall — so what the rewrite costs a real
 *    ingest is almost entirely the cost of DECIDING it has nothing to do.
 *  - `--badges N`: the same corpus with N declared badges bolted onto every
 *    event and written into its indexed text — the "emoji everywhere" corpus
 *    that does not exist yet but is the one a bridge could create.
 *
 *     ./gradlew :benchmark:extractBench --args="--corpus /path/capture.json"
 *     ./gradlew :benchmark:extractBench --args="--corpus /path/capture.json --badges 3"
 *
 * `--rounds` (default 7) times that many passes over the whole corpus after two
 * warm-up passes, and reports the MEDIAN — a mean over a JIT-warming JVM
 * measures the compiler, not the code.
 */
object ExtractBench {
    @JvmStatic
    fun main(args: Array<String>) {
        val opts =
            args
                .toList()
                .chunked(2)
                .filter { it.size == 2 }
                .associate { it[0] to it[1] }
        val corpus = opts["--corpus"]
        if (corpus == null) {
            System.err.println("usage: --corpus <events.json> [--badges N] [--rounds 7] [--stage upstream|emojis|declared|full]")
            return
        }
        val badges = opts["--badges"]?.toIntOrNull() ?: 0
        val rounds = opts["--rounds"]?.toIntOrNull() ?: 7
        // `upstream` times quartz's per-kind decomposition ALONE — the floor
        // this store cannot go below, and the number every wrapper cost has to
        // be read against.
        val stage = opts["--stage"] ?: "full"

        val events = load(Path.of(corpus)).let { if (badges > 0) it.map { e -> badged(e, badges) } else it }
        val declaring = events.count { e -> e.tags.any { it.size > 2 && it[0] == "emoji" } }
        println("corpus ${events.size} events, $declaring declaring an emoji (${"%.2f".format(100.0 * declaring / events.size)}%)")

        repeat(2) { pass(events, stage) }
        val times = (0 until rounds).map { measureNanoTime { pass(events, stage) } }.sorted()
        val median = times[times.size / 2]
        println(
            "$stage: %.0f ns/event  (%,d events/s)  median of $rounds rounds, spread %.0f-%.0f ns".format(
                median.toDouble() / events.size,
                (1_000_000_000.0 * events.size / median).toLong(),
                times.first().toDouble() / events.size,
                times.last().toDouble() / events.size,
            ),
        )
    }

    /** One pass over the corpus. The checksum exists so nothing here is dead code the JIT can delete. */
    private fun pass(
        events: List<Event>,
        stage: String,
    ): Int {
        var sum = 0
        when (stage) {
            // Quartz's per-kind decomposition alone: the floor this store
            // cannot go below, and the number every wrapper cost reads against.
            "upstream" -> for (event in events) sum += SearchFieldExtractor.extract(event).hashCode()

            // The NIP-30 declaration lookup alone — what asking "does this
            // event carry a badge?" costs, 10 961 times.
            "emojis" -> for (event in events) sum += event.tags.emojis().size

            // The GUARD the store asks first (SearchExtractors.Emoji): the same
            // question answered by an inlined array scan that allocates
            // nothing, for the 99.94% of events whose answer is no.
            "declared" -> for (event in events) sum += if (event.tags.any { it.size > 2 && it[0] == "emoji" }) 1 else 0

            else -> for (event in events) sum += SearchExtractors.extract(event).hashCode()
        }
        return sum
    }

    private fun load(path: Path): List<Event> =
        Json
            .parseToJsonElement(Files.readString(path))
            .jsonArray
            .map { Event.fromJson(it.toString()) }

    /**
     * The worst case, built honestly: [n] DECLARED codes per event, each also
     * written into the content so the rewrite has to find and replace it —
     * declaring a badge the text never wears would measure the cheap path
     * while looking like the expensive one.
     */
    private fun badged(
        event: Event,
        n: Int,
    ): Event {
        val codes = (0 until n).map { "badge$it" }
        val tags = event.tags + codes.map { arrayOf("emoji", it, "https://static/$it.png") }
        val worn = codes.joinToString(" ") { ":$it:" }
        val content =
            if (event.content.startsWith("{")) {
                // A kind 0 is JSON: wear the badges inside the name, where a
                // bridged profile really carries them.
                event.content.replaceFirst(Regex("\"name\"\\s*:\\s*\""), "\"name\":\"$worn ")
            } else {
                "$worn ${event.content}"
            }
        // THROUGH THE FACTORY, not the Event constructor: `SearchFieldExtractor`
        // dispatches on the parsed TYPE, and a base `Event` is not a
        // `SearchableEvent` at all — rebuilding one directly measures the
        // extractor's None branch at full speed and calls it a badge corpus.
        return Event.fromJson(Event(event.id, event.pubKey, event.createdAt, event.kind, tags, content, event.sig).toJson())
    }
}
