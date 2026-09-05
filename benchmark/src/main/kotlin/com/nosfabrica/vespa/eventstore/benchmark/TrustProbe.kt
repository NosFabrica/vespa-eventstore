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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.trust.TrustKeyingMigration
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import java.nio.file.Files
import java.nio.file.Path
import kotlin.random.Random

/**
 * THE TRUST WRITE PATH UNDER A REAL LENS, timed — the harness behind the
 * service-keyed tensor study. Feeds a captured set of kind-10040 lists and two
 * providers' kind-30382 corpora through the store's real write path, then
 * measures the four operations a relay actually sees:
 *
 *  1. bulk card ingest (the mirror's `batchInsert`, relay batch size) and the
 *     projection settling behind it;
 *  2. a single superseding card insert with the drain idle — the client path;
 *  3. an observer re-signing an UNCHANGED 10040 (new id, same entries) — the
 *     commonest 10040 write on a live relay;
 *  4. an observer SWAPPING providers (the 10040 now names the second key).
 *
 * During 3 and 4 the probe keeps inserting fresh cards on a clock and records
 * how long each waits on the trust gate — the number a client feels — while
 * the drain's own counters say how far the walk got, so an hours-long walk
 * can be measured for a bounded time and extrapolated.
 *
 * Signatures are never verified by the store, so the fabricated events carry
 * an empty signature and a random id; every other field is a real one's.
 *
 *     VESPA_URL=http://localhost:8080 ./gradlew :benchmark:trustProbe \
 *       --args="lists.json cards_a.json cards_b.json [walkSeconds]"
 */
object TrustProbe {
    private val rnd = Random(42)

    private fun hexId() = ByteArray(32).also { rnd.nextBytes(it) }.joinToString("") { "%02x".format(it) }

    private fun load(path: String): List<Event> =
        Json
            .parseToJsonElement(Files.readString(Path.of(path)))
            .jsonArray
            .map { Event.fromJson(it.toString()) }

    private suspend fun feed(
        store: IEventStore,
        events: List<Event>,
        batch: Int,
        label: String,
    ) {
        val t0 = System.nanoTime()
        var accepted = 0
        val rejected = HashMap<String, Int>()
        events.chunked(batch).forEach { chunk ->
            store.batchInsert(chunk).forEach { o ->
                when (o) {
                    is IEventStore.InsertOutcome.Accepted -> accepted++
                    is IEventStore.InsertOutcome.Rejected -> rejected.merge(o.reason.substringBefore(':'), 1, Int::plus)
                    is IEventStore.InsertOutcome.Failed -> rejected.merge("FAILED", 1, Int::plus)
                }
            }
        }
        val secs = (System.nanoTime() - t0) / 1e9
        println("$label: ${events.size} events, $accepted accepted, rejected $rejected, %.1fs (%.0f ev/s)".format(secs, events.size / secs))
    }

    private fun freshCard(
        template: ContactCardEvent,
        rank: Int,
    ): ContactCardEvent {
        val tags = template.tags.map { t -> if (t.isNotEmpty() && t[0] == "rank") arrayOf("rank", rank.toString()) else t }.toTypedArray()
        return ContactCardEvent(hexId(), template.pubKey, template.createdAt + 1_000, tags, "", "")
    }

    private fun resigned(
        list: TrustProviderListEvent,
        at: Long,
        rankKey: String? = null,
        followersKey: String? = null,
    ): TrustProviderListEvent {
        val tags =
            list.tags
                .map { t ->
                    when {
                        t.size >= 3 && t[0] == "30382:rank" && rankKey != null -> arrayOf(t[0], rankKey, t[2])
                        t.size >= 3 && t[0] == "30382:followers" && followersKey != null -> arrayOf(t[0], followersKey, t[2])
                        else -> t
                    }
                }.toTypedArray()
        return TrustProviderListEvent(hexId(), list.pubKey, at, tags, list.content, "")
    }

    private fun stage(name: String): IngestStats.Stage = IngestStats.snapshot()[name] ?: IngestStats.Stage(0, 0, 0)

    private fun statsLine(vararg names: String): String =
        names.joinToString("  ") { n ->
            val s = stage(n)
            "$n=%.1fs/%d calls (max %.1fs)".format(s.totalNanos / 1e9, s.calls, s.maxNanos / 1e9)
        }

    /** Insert [cards] one by one through the client path; returns each insert's wall time in ms. */
    private suspend fun timedInserts(
        store: IEventStore,
        cards: List<ContactCardEvent>,
    ): List<Long> =
        cards.map { c ->
            val t0 = System.nanoTime()
            runCatching { store.insert(c) }.onFailure { println("  insert failed: ${it.message?.take(120)}") }
            (System.nanoTime() - t0) / 1_000_000
        }

    /**
     * Run [mutation] (a 10040 write), then for [walkSeconds] keep inserting a
     * superseding card every few seconds and report the gate wait each pays,
     * plus the drain's progress counters, then report whether the projection
     * settled in time.
     */
    private suspend fun watchWalk(
        store: VespaEventStore,
        label: String,
        walkSeconds: Long,
        templates: List<ContactCardEvent>,
        mutation: suspend () -> Unit,
    ) {
        println("== $label")
        val derive0 = stage("proj.fetch.derive")
        val write0 = stage("proj.write")
        val t0 = System.nanoTime()
        mutation()
        println("  10040 insert returned in %d ms".format((System.nanoTime() - t0) / 1_000_000))
        val settled =
            CoroutineScope(Dispatchers.Default).async {
                withTimeoutOrNull(walkSeconds * 1_000) {
                    store.awaitTrustProjection()
                    (System.nanoTime() - t0) / 1e9
                }
            }
        var i = 0
        val waits = ArrayList<Long>()
        while (!settled.isCompleted && (System.nanoTime() - t0) / 1e9 < walkSeconds) {
            delay(5_000)
            val card = freshCard(templates[i++ % templates.size], rank = 40 + (i % 50))
            val ms = timedInserts(store, listOf(card)).first()
            waits += ms
            val d = stage("proj.fetch.derive")
            val w = stage("proj.write")
            println(
                "  t=%3.0fs card insert %5d ms | derive calls +%d (%.1fs) | proj.write +%d (%.1fs) | gate held now: %s".format(
                    (System.nanoTime() - t0) / 1e9,
                    ms,
                    d.calls - derive0.calls,
                    (d.totalNanos - derive0.totalNanos) / 1e9,
                    w.calls - write0.calls,
                    (w.totalNanos - write0.totalNanos) / 1e9,
                    IngestStats.heldNow()?.let { "${it.stage} ${it.heldForMillis()}ms ${it.detail ?: ""}" } ?: "none",
                ),
            )
        }
        val secs = settled.await()
        if (secs == null) {
            println("  NOT SETTLED after ${walkSeconds}s")
        } else {
            println("  settled in %.1fs".format(secs))
        }
        if (waits.isNotEmpty()) {
            println("  card inserts during the walk: n=${waits.size} median=${waits.sorted()[waits.size / 2]}ms max=${waits.max()}ms")
        }
        println("  " + statsLine("lock.ingest.trust.wait", "lock.gate.hold", "proj.fetch.derive", "proj.write", "proj.fetch.maxrank"))
    }

    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            require(args.size >= 3) { "usage: trustProbe <lists.json> <cardsA.json> <cardsB.json> [walkSeconds]" }
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val batch = System.getenv("BENCH_BATCH")?.toIntOrNull() ?: 1000
            val walkSeconds = args.getOrNull(3)?.toLongOrNull() ?: 180L
            val observer = System.getenv("PROBE_OBSERVER") ?: "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

            if (args[0] == "--republish-only" || args[0] == "--load-then-republish") {
                // A provider re-publishes its whole corpus: every card supersedes
                // the stored version at its address — the mirror's steady state.
                val cardsA = load(args[1])
                VespaEventStore.open(url).use { store ->
                    if (args[0] == "--load-then-republish") {
                        feed(store, load(args[2]), batch, "lists")
                        feed(store, cardsA, batch, "cards A")
                        store.awaitTrustProjection()
                        println("  loaded and settled")
                    }
                    val stored = cardsA.filterIsInstance<ContactCardEvent>().associateBy { it.pubKey + (it.aboutUser() ?: "") }.values
                    val republished = stored.map { c -> ContactCardEvent(hexId(), c.pubKey, c.createdAt + 100_000, c.tags, "", "") }
                    println("== 6. the provider republishes ${republished.size} cards (every one a supersession)")
                    val derive0 = stage("proj.fetch.derive")
                    val t0 = System.nanoTime()
                    feed(store, republished, batch, "republished cards")
                    val fed = (System.nanoTime() - t0) / 1e9
                    val settled =
                        withTimeoutOrNull(walkSeconds * 1_000) {
                            store.awaitTrustProjection()
                            (System.nanoTime() - t0) / 1e9
                        }
                    val d = stage("proj.fetch.derive")
                    println("  fed in %.1fs; projection %s; derive calls during it: %d (%.1fs)".format(fed, settled?.let { "settled at %.1fs".format(it) } ?: "NOT settled after ${walkSeconds}s", d.calls - derive0.calls, (d.totalNanos - derive0.totalNanos) / 1e9))
                    println("  " + statsLine("write", "remove", "versions", "proj.write", "proj.fetch.maxrank", "proj.fetch.derive", "lock.gate.hold", "lock.ingest.trust.wait"))
                }
                return@runBlocking
            }
            if (args[0] == "--query-only") {
                val cardsA = load(args[1])
                val cardsB = load(args[2])
                VespaEventStore.open(url).use { store -> lensedQuery(store, observer, cardsA, cardsB) }
                return@runBlocking
            }
            val lists = load(args[0])
            val cardsA = load(args[1])
            val cardsB = load(args[2])
            val providerA = cardsA.first().pubKey
            val providerB = cardsB.first().pubKey
            println("lists=${lists.size} cardsA=${cardsA.size} (${providerA.take(8)}) cardsB=${cardsB.size} (${providerB.take(8)}) batch=$batch observer=${observer.take(8)}")

            VespaEventStore.open(url).use { store ->
                println("== 1. bulk ingest")
                feed(store, lists, batch, "lists")
                val t0 = System.nanoTime()
                feed(store, cardsA, batch, "cards A")
                feed(store, cardsB, batch, "cards B")
                store.awaitTrustProjection()
                println("  projection settled %.1fs after the feeds started".format((System.nanoTime() - t0) / 1e9))
                println("  " + statsLine("write", "proj.write", "proj.fetch.maxrank", "proj.fetch.derive", "lock.ingest.hold", "lock.ingest.trust.wait", "lock.gate.hold"))

                val templates = cardsA.filterIsInstance<ContactCardEvent>().shuffled(rnd).take(400)
                println("== 2. single card inserts, drain idle (client path)")
                val single = timedInserts(store, templates.take(20).map { freshCard(it, rank = 55) })
                val t1 = System.nanoTime()
                store.awaitTrustProjection()
                println("  20 inserts: median=${single.sorted()[10]}ms max=${single.max()}ms; settle after them %.2fs".format((System.nanoTime() - t1) / 1e9))
                println("  " + statsLine("proj.fetch.derive", "proj.write", "lock.ingest.trust.wait"))

                val myList = lists.filterIsInstance<TrustProviderListEvent>().first { it.pubKey == observer }
                var at = myList.createdAt + 10
                watchWalk(store, "3. observer re-signs an UNCHANGED 10040", walkSeconds, templates.drop(20)) {
                    store.insert(resigned(myList, at++))
                }
                watchWalk(store, "4. observer SWAPS providers (${providerA.take(8)} -> ${providerB.take(8)})", walkSeconds, templates.drop(100)) {
                    store.insert(resigned(myList, at++, rankKey = providerB, followersKey = providerB))
                }
                lensedQuery(store, observer, cardsA, cardsB)

                // ---- 6. a swap to a provider NOBODY has named: the one walk left.
                // The second corpus re-signed under a fresh key, stored while unnamed.
                val providerC = "7".repeat(64)
                val cardsC = cardsB.filterIsInstance<ContactCardEvent>().map { c -> ContactCardEvent(hexId(), providerC, c.createdAt, c.tags, "", "") }
                feed(store, cardsC, batch, "cards C (${providerC.take(8)}, named by nobody)")
                val page0 = stage("proj.fetch.page")
                watchWalk(store, "6. observer SWAPS to a NEVER-named provider (${providerB.take(8)} -> ${providerC.take(8)}): the service walk", walkSeconds, templates.drop(200)) {
                    store.insert(resigned(myList, at++, rankKey = providerC, followersKey = providerC))
                }
                val page1 = stage("proj.fetch.page")
                println("  walk pages: %d (%.1fs); cells: %s".format(page1.calls - page0.calls, (page1.totalNanos - page0.totalNanos) / 1e9, statsLine("proj.write")))
                val sampleC = cardsC.take(5).map { it.aboutUser()!! }
                val cellsC =
                    VespaReputationIndex(url).use { reps ->
                        sampleC.map { s ->
                            s.take(8) + "->" + (
                                reps
                                    .get(s)
                                    ?.influenceScores
                                    ?.get(providerC)
                                    ?.toString() ?: "none"
                            )
                        }
                    }
                println("  cells under the new key: " + cellsC.joinToString())

                if (System.getenv("PROBE_STOP_AFTER") == "6") {
                    println(IngestStats.dump())
                    return@use
                }
                // ---- 7. the other lensed reads: followers, the gated feed, count.
                println("== 7. the other lensed reads")
                val followers: List<Event> = store.query(Filter(kinds = listOf(1), limit = 10, search = "observer:$observer sort:followers"))
                println("  sort:followers: ${followers.size} hits")
                val feedT0 = System.nanoTime()
                val gated: List<Event> = store.query(Filter(kinds = listOf(1), limit = 10, search = "observer:$observer filter:rank:gte:50"))
                println("  filter:rank:gte:50 feed: ${gated.size} hits in %d ms".format((System.nanoTime() - feedT0) / 1_000_000))
                val countT0 = System.nanoTime()
                val counted = store.count(Filter(kinds = listOf(1), search = "observer:$observer"))
                println("  COUNT under the observer: $counted in %d ms".format((System.nanoTime() - countT0) / 1_000_000))

                // ---- 8. deletions: the assertions stream's deleteMissing shape (by id, 500 a call).
                println("== 8. kind-5 style deletions: 2,000 of the current provider's cards, 500 per call")
                val doomed = cardsC.drop(10).take(2_000)
                val delT0 = System.nanoTime()
                doomed.chunked(500).forEach { chunk -> store.delete(Filter(ids = chunk.map { it.id })) }
                store.awaitTrustProjection()
                println("  deleted and settled in %.1fs; derive calls total now %d".format((System.nanoTime() - delT0) / 1e9, stage("proj.fetch.derive").calls))
                val gone = doomed.first().aboutUser()!!
                println("  a deleted card's cell: ${VespaReputationIndex(url).use { it.get(gone) }?.influenceScores?.get(providerC) ?: "gone"}")

                // ---- 9. reconcile (the relay's boot repair) on a loaded, healthy store.
                println("== 9. reconcileTrust() on the loaded store")
                val recT0 = System.nanoTime()
                val rec = store.reconcileTrust()
                println("  services=${rec.services} rebuilt=${rec.rebuilt.size} in %.1fs".format((System.nanoTime() - recT0) / 1e9))

                // ---- 10. the keying migration at scale: marker down, reopen, wait.
                println("== 10. the keying migration on the loaded store (marker removed, store reopened)")
                VespaReputationIndex(url).use { it.remove(TrustKeyingMigration.MARKER_KEY) }
                println(IngestStats.dump())
                store.close()
                val migT0 = System.nanoTime()
                VespaEventStore.open(url).use { reopened ->
                    val done = reopened.awaitTrustKeying()
                    println("  migration: $done in %.1fs".format((System.nanoTime() - migT0) / 1e9))

                    // ---- 11. verify: every parent against the exact derive — the correctness net.
                    println("== 11. verifyTrust(): every stored parent against the exact derive")
                    val verT0 = System.nanoTime()
                    var last = 0
                    val audit =
                        reopened.verifyTrust { n ->
                            if (n - last >= 100_000) {
                                last = n
                                println("    checked $n subjects, %.0fs".format((System.nanoTime() - verT0) / 1e9))
                            }
                        }
                    println("  subjects=${audit.subjectsChecked} parents=${audit.parentsChecked} drift=${audit.driftCount} in %.1fs".format((System.nanoTime() - verT0) / 1e9))
                    audit.drift.take(5).forEach { d -> println("    drift ${d.subject.take(8)}: expected=${d.expected?.influenceScores} actual=${d.actual?.influenceScores}") }
                }
            }
        }

    private fun rankOf(card: Event): Int? = (card as? ContactCardEvent)?.rank()

    /**
     * WHAT THE LENS SERVES after the swap: one kind-1 note per probe author —
     * five subjects the swapped-to provider ranks at distinct levels, five the
     * old provider ranks — then `sort:rank` under the observer. The page must
     * order by the NEW provider's ranks and hold nobody it does not rank.
     */
    private suspend fun lensedQuery(
        store: IEventStore,
        observer: String,
        cardsA: List<Event>,
        cardsB: List<Event>,
    ) {
        println("== 5. a lensed query after the swap")

        fun pick(cards: List<Event>): List<Pair<String, Int>> {
            val byRank = LinkedHashMap<Int, String>()
            for (c in cards.sortedBy { it.id }) {
                val r = rankOf(c) ?: continue
                val subject = (c as ContactCardEvent).aboutUser() ?: continue
                if (r in listOf(95, 70, 45, 20, 5) && r !in byRank) byRank[r] = subject
                if (byRank.size == 5) break
            }
            return byRank.map { (r, s) -> s to r }
        }
        val fromB = pick(cardsB)
        val fromA = pick(cardsA).filter { (s, _) -> fromB.none { it.first == s } }
        val ranksB = cardsB.associate { ((it as ContactCardEvent).aboutUser() ?: "") to rankOf(it) }
        val ranksA = cardsA.associate { ((it as ContactCardEvent).aboutUser() ?: "") to rankOf(it) }
        val authors = (fromB + fromA).map { it.first }
        val notes =
            authors.mapIndexed { i, a ->
                com.vitorpamplona.quartz.nip10Notes
                    .TextNoteEvent(hexId(), a, 1_700_000_000L + i, emptyArray(), "probe note $i", "")
            }
        val outcomes = store.batchInsert(notes)
        println("  notes stored: ${outcomes.count { it is IEventStore.InsertOutcome.Accepted }} of ${notes.size}")
        val hits: List<Event> = store.query(Filter(kinds = listOf(1), limit = 20, search = "observer:$observer sort:rank"))
        println("  sort:rank under the observer: ${hits.size} hits (expected: the ${fromB.size} authors the new provider ranks above the floor, in rank order)")
        hits.forEach { h -> println("    ${h.pubKey.take(8)}  rank under new provider=${ranksB[h.pubKey]}  under old=${ranksA[h.pubKey]}") }
        val unranked = store.query<Event>(Filter(kinds = listOf(1), limit = 20))
        println("  unlensed recall: ${unranked.size} of ${notes.size} notes")
    }
}
