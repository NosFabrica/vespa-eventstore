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
 * prices against the cost it replaced by walking the SAME gates two ways:
 *
 *  - EVENT-MAJOR, the way fan-out runs (each event through every gate): the
 *    first gate reads the words, every other gate is a memo hit.
 *  - GATE-MAJOR (each gate through every event): consecutive calls never see
 *    the same event, so every call is a miss — exactly what each gate paid
 *    on every call before the memo, when the words were read per gate.
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
        runBlocking { (0 until 100).map { checkNotNull(store.liveGate(listOf(Filter(kinds = listOf(1), search = WORDS[it % WORDS.size])))) } }.let { warm ->
            repeat(3) {
                eventMajor(warm, events)
                gateMajor(warm, events)
            }
        }
        for (n in sizes) {
            val gates = runBlocking { (0 until n).map { checkNotNull(store.liveGate(listOf(Filter(kinds = listOf(1), search = WORDS[it % WORDS.size])))) } }
            val shared = eventMajor(gates, events)
            check(shared == gateMajor(gates, events)) { "$n gates: the two walks disagree on an admission" }
            check(shared.any { row -> row.any { it == true } }) { "$n gates: nothing admitted, so nothing was measured" }
            repeat(2) {
                eventMajor(gates, events)
                gateMajor(gates, events)
            }
            // Interleaved round by round, so neither walk is the one timed while the JIT is still compiling.
            val perGateRounds = ArrayList<Long>()
            val sharedRounds = ArrayList<Long>()
            repeat(rounds) {
                perGateRounds += measureNanoTime { gateMajor(gates, events) }
                sharedRounds += measureNanoTime { eventMajor(gates, events) }
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

    /** Each event through every gate, as fan-out walks them: answers[event][gate]. */
    private fun eventMajor(
        gates: List<LiveGate>,
        events: List<Event>,
    ): List<List<Boolean?>> = events.map { e -> gates.map { it.admitsNow(e) } }

    /** Each gate through every event — a miss on every call — transposed to answers[event][gate]. */
    private fun gateMajor(
        gates: List<LiveGate>,
        events: List<Event>,
    ): List<List<Boolean?>> {
        val byGate = gates.map { g -> events.map { g.admitsNow(it) } }
        return events.indices.map { e -> byGate.map { it[e] } }
    }

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
