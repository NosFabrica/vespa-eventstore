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
package com.nosfabrica.vespa.eventstore.benchmark.bench

import com.nosfabrica.vespa.eventstore.LiveGate
import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.mapping.LiveWords
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import java.nio.file.Files
import java.nio.file.Path
import kotlin.system.measureNanoTime

/**
 * THE LIVE GATE'S TEXT WORK, PER EVENT, AS OPEN SEARCH FEEDS GROW.
 *
 * Every live subscription has its own `LiveGate`, and a searching one checks
 * each matching event's words. The store's gates share one words memo
 * (`mapping/LiveWords`: one read per event per thread), which this bench
 * prices against the cost it replaced by walking two sets of the same gates
 * the same way — each event through every gate, as fan-out walks them:
 *
 *  - SHARED: the store's own gates. The first gate reads the words, every
 *    other gate is a memo hit.
 *  - PER GATE: the same filters built around the plain, uncached read
 *    (`LiveWords.read`, through the store's internal `liveGateWith`), which is
 *    exactly what each gate paid on every call before the memo. NOT the memo
 *    missing on every call: a miss also pays the ThreadLocal store, the entry
 *    allocation and the miss counter, which an earlier version of this bench
 *    charged to the baseline and so overstated the speed-up.
 *
 * Both walks must give the same answers for every (gate, event), or the run
 * fails: the timing is also the proof that sharing changes no admission.
 * Anonymous one-word searches (no lens, so no trust read) over `kinds:[1]`,
 * the words cycled across gates, through `admitsNow` — the ingest-path call.
 *
 *     ./gradlew :benchmark:liveGateBench
 *     ./gradlew :benchmark:liveGateBench --args="--corpus /path/capture.json --gates 1,10,100,500"
 *
 * Without `--corpus` the events are 2,000 synthetic ~250-character notes.
 * With it, the capture's kind-1 events. `--rounds` (default 7) timed rounds
 * after a warm-up, the two walks alternating round by round; the MEDIAN is
 * reported.
 */
object LiveGateBench {
    private val WORDS = listOf("bitcoin", "nostr", "pizza", "relay", "lightning", "zap", "music", "art", "coffee", "weather")

    @JvmStatic
    fun main(args: Array<String>) {
        val opts =
            args
                .toList()
                .chunked(2)
                .filter { it.size == 2 }
                .associate { it[0] to it[1] }
        val rounds = opts["--rounds"]?.toIntOrNull() ?: 7
        val sizes = opts["--gates"]?.split(",")?.map { it.trim().toInt() } ?: listOf(1, 10, 100, 500)
        val events = opts["--corpus"]?.let { load(Path.of(it)) } ?: synthetic()
        require(events.isNotEmpty()) { "no kind-1 events to fan out" }
        println("${events.size} kind-1 events, mean ${events.sumOf { it.content.length } / events.size} content chars; median of $rounds rounds")

        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
        // One whole warm-up at a mid size first: without it the FIRST size timed (1 gate) paid the
        // JIT's compilation, and read 2.5x where both walks do identical work.
        runBlocking { (0 until 100).map { checkNotNull(store.liveGate(filtersOf(it))) to checkNotNull(store.liveGateWith(filtersOf(it), LiveWords::read)) } }.let { warm ->
            repeat(3) {
                walk(warm.map { it.first }, events)
                walk(warm.map { it.second }, events)
            }
        }
        for (n in sizes) {
            val gates = runBlocking { (0 until n).map { checkNotNull(store.liveGate(filtersOf(it))) } }
            val uncached = runBlocking { (0 until n).map { checkNotNull(store.liveGateWith(filtersOf(it), LiveWords::read)) } }
            val shared = walk(gates, events)
            check(shared == walk(uncached, events)) { "$n gates: the two walks disagree on an admission" }
            check(shared.any { row -> row.any { it == true } }) { "$n gates: nothing admitted, so nothing was measured" }
            repeat(2) {
                walk(gates, events)
                walk(uncached, events)
            }
            // Interleaved round by round, so neither walk is the one timed while the JIT is still compiling.
            val perGateRounds = ArrayList<Long>()
            val sharedRounds = ArrayList<Long>()
            repeat(rounds) {
                perGateRounds += measureNanoTime { walk(uncached, events) }
                sharedRounds += measureNanoTime { walk(gates, events) }
            }
            val perGate = median(perGateRounds) / events.size
            val sharedNs = median(sharedRounds) / events.size
            println(
                "%4d gates: words per gate %,9.1f us/event   shared %,8.1f us/event   %.1fx".format(
                    n,
                    perGate / 1000.0,
                    sharedNs / 1000.0,
                    perGate / sharedNs,
                ),
            )
        }
    }

    /** One anonymous one-word `kinds:[1]` search, the words cycled across gates. */
    private fun filtersOf(i: Int) = listOf(Filter(kinds = listOf(1), search = WORDS[i % WORDS.size]))

    /** Each event through every gate, as fan-out walks them: answers[event][gate]. */
    private fun walk(
        gates: List<LiveGate>,
        events: List<Event>,
    ): List<List<Boolean?>> = events.map { e -> gates.map { it.admitsNow(e) } }

    private fun median(times: List<Long>): Double = times.sorted()[times.size / 2].toDouble()

    private fun synthetic(): List<Event> {
        val body =
            "Just set up a new relay for the community, running on a small box with plenty of headroom. " +
                "Lightning zaps work, the feed is quick, and search over trust is live. #nostr #bitcoin https://example.com/post/123 "
        return (0 until 2000).map { i ->
            TextNoteEvent(i.toString(16).padStart(64, '0'), "a1".repeat(32), 1_700_000_000L + i, emptyArray(), body + WORDS[i % WORDS.size] + " " + i, "")
        }
    }

    private fun load(path: Path): List<Event> =
        Json
            .parseToJsonElement(Files.readString(path))
            .jsonArray
            .map { Event.fromJson(it.toString()) }
            .filter { it.kind == 1 }
}
