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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.app.SchemaDeployer
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip25Reactions.ReactionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.test.assertTrue

/**
 * EVERY FILTER SHAPE, THROUGH THE FRONT DOOR, AGAINST AN ORACLE.
 *
 * The COUNT that ignored its lens (a term-less `observer:… filter:rank:gte:50`
 * counted the whole match set, whatever the floor) got past every engine-level
 * IT because each one hands the engine an `EventQuery` it built by hand — and
 * the decision that bit was made ABOVE that, where a Quartz [Filter] becomes a
 * query and a term-less observer read is stamped onto the gated profile. So
 * this test enters where a relay does: [VespaEventStore.open], real signed-
 * shape 10040 and 30382 events resolving the lens, and a [Filter] per case.
 *
 * The cases are a cross product — NIP-01 shapes x lens modes (token observer,
 * connection observer, both, a lens-less observer, `include:spam`, every
 * `filter:rank` form) x `sort:` modes x search shapes (terms, a phrase, an
 * exclusion, none) — plus limits and multi-filter requests. Each is checked
 * against an ORACLE computed here from the fixture alone: Quartz's own
 * [Filter.match] for the NIP-01 part, a token scan for the text, and the
 * fixture's rank table for the gate. For every case:
 *
 *  - the REQ (the raw path a relay serves, [NostrSemanticsStore.rawQuery])
 *    serves exactly the oracle's set;
 *  - COUNT equals what that REQ served (NIP-45, STORE-C01's clamp included);
 *  - a chronological read is in NIP-01 order, and a termless trust sort is
 *    monotone in the key it names.
 *
 * Failures are COLLECTED, not thrown at the first, and printed as one table:
 * the point is the whole matrix's state, which one assertion cannot show.
 *
 * Tagged `integration`, excluded from the default `:benchmark:test`; run with
 * `-Pintegration` where Docker is available. Skips cleanly without a daemon.
 */
@Tag("integration")
class FilterMatrixIT {
    @Test
    fun `every filter shape serves what the lens admits, and counts what it serves`() {
        assumeTrue(dockerAvailable(), "Docker not available — skipping the filter matrix IT")

        GenericContainer("vespaengine/vespa:latest")
            .withExposedPorts(QUERY_PORT, CONFIG_PORT)
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(CONFIG_PORT).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(5))
            .use { vespa ->
                vespa.start()
                val queryUrl = "http://${vespa.host}:${vespa.getMappedPort(QUERY_PORT)}"
                val configUrl = "http://${vespa.host}:${vespa.getMappedPort(CONFIG_PORT)}"
                SchemaDeployer(configUrl).deployIfAbsent(queryUrl)
                VespaEventStore.open(url = queryUrl, autoDeploy = false, configUrl = configUrl).use { front ->
                    runBlocking {
                        front.batchInsert(CARDS + NOTES + REACTIONS)
                        front.insert(LIST_10040)
                        front.awaitTrustProjection()
                        awaitCorpus(front.store)

                        val failures = ArrayList<String>()
                        var checked = 0
                        for (case in cases()) {
                            checked++
                            runCatching { check(front.store, case) }
                                .onSuccess { failures += it }
                                .onFailure { failures += "${case.name}\n    THREW ${it::class.simpleName}: ${it.message?.take(300)}" }
                        }

                        println("filter matrix: $checked cases, ${failures.size} failing")
                        failures.forEach { println("  FAIL $it") }
                        assertTrue(failures.isEmpty(), "${failures.size} of $checked filter cases failed:\n" + failures.joinToString("\n"))
                    }
                }
            }
    }

    // ---- one case -------------------------------------------------------------

    /** Run [case] as a REQ and as a COUNT; every way it disagrees with the oracle. */
    private suspend fun check(
        store: NostrSemanticsStore,
        case: Case,
    ): List<String> {
        val expected = case.expected()
        val served = ArrayList<Event>()
        val count: Int
        if (case.connection != null) {
            withContext(StoreQueryContext(setOf(case.connection))) {
                store.rawQuery(case.filters) { served += it.toEvent<Event>() }
            }
            count = withContext(StoreQueryContext(setOf(case.connection))) { store.count(case.filters) }
        } else {
            store.rawQuery(case.filters) { served += it.toEvent<Event>() }
            count = store.count(case.filters)
        }

        val problems = ArrayList<String>()
        val servedIds = served.map { it.id }
        if (servedIds.size != servedIds.toSet().size) problems += "served duplicates"

        val limit = case.limit
        if (limit == null) {
            val want = expected.map { it.id }.toSet()
            val got = servedIds.toSet()
            if (got != want) {
                problems += "served ${got.size}, oracle ${want.size}" +
                    (want - got).takeIf { it.isNotEmpty() }?.let { " | missing ${describe(it)}" }.orEmpty() +
                    (got - want).takeIf { it.isNotEmpty() }?.let { " | extra ${describe(it)}" }.orEmpty()
            }
        } else {
            val want = minOf(limit, expected.size)
            if (servedIds.size != want) problems += "limit $limit: served ${servedIds.size}, oracle ${expected.size} -> want $want"
            val stray = servedIds.toSet() - expected.map { it.id }.toSet()
            if (stray.isNotEmpty()) problems += "limit $limit: served outside the oracle ${describe(stray)}"
        }

        if (count != servedIds.size) problems += "COUNT $count != served ${servedIds.size}"
        val oracleCount = limit?.let { minOf(it, expected.size) } ?: expected.size
        if (count != oracleCount) problems += "COUNT $count != oracle $oracleCount"

        // Order: only where the shape promises one.
        when (case.order) {
            Order.CHRONOLOGICAL -> {
                val want = expected.sortedWith(NEWEST_FIRST).map { it.id }.let { if (limit != null) it.take(limit) else it }
                if (servedIds != want && problems.isEmpty()) problems += "not in NIP-01 order"
            }

            Order.TRUST_DESC -> {
                if (!served.map { RANK[it.pubKey] ?: 0 }.zipWithNext().all { (a, b) -> a >= b }) problems += "sort:rank not trust-descending"
            }

            Order.TRUST_ASC -> {
                if (!served.map { RANK[it.pubKey] ?: 0 }.zipWithNext().all { (a, b) -> a <= b }) problems += "sort:rank:asc not trust-ascending"
            }

            Order.FOLLOWERS_DESC -> {
                if (!served.map { FOLLOWERS[it.pubKey] ?: 0 }.zipWithNext().all { (a, b) -> a >= b }) problems += "sort:followers not follower-descending"
            }

            Order.ANY -> {}
        }

        return if (problems.isEmpty()) emptyList() else listOf("${case.name}\n    " + problems.joinToString("\n    "))
    }

    // ---- the matrix -------------------------------------------------------------

    private fun cases(): List<Case> {
        val out = ArrayList<Case>()
        for (lens in LENSES) {
            for (sort in SORTS) {
                for (text in TEXTS) {
                    for (base in BASES) {
                        out += Case(base.name, listOf(base.filter), lens, sort, text, limit = null)
                    }
                    // Limits: the clamp (STORE-C01) and the "matches nothing" sentinel.
                    for (limit in listOf(3, 0)) {
                        out += Case("kind1 limit=$limit", listOf(Filter(kinds = listOf(1), limit = limit)), lens, sort, text, limit = limit)
                    }
                }
            }
            // Multi-filter: the feed dedups across filters, and so must COUNT.
            // The lens rides every filter; the text rides only where named.
            for ((name, filters) in MULTI) {
                out += Case(name, filters, lens, Sort.NONE, Text.NONE, limit = null)
            }
            out += Case("multi: kind 7 + kind1 'pizza'", listOf(Filter(kinds = listOf(7)), Filter(kinds = listOf(1), search = "pizza")), lens, Sort.NONE, Text.NONE, limit = null)
        }
        return out
    }

    /** One request: its filters (lens, sort and text appended to each search), and its oracle. */
    private inner class Case(
        baseName: String,
        baseFilters: List<Filter>,
        val lens: Lens,
        val sort: Sort,
        val text: Text,
        val limit: Int?,
    ) {
        val connection: String? = lens.connection
        val filters: List<Filter> =
            baseFilters.map { f ->
                val search = listOfNotNull(f.search, text.query, lens.tokens, sort.token).joinToString(" ").ifBlank { null }
                f.copy(search = search)
            }
        val name =
            "[${lens.name}] [${sort.name.lowercase()}] [${text.name.lowercase()}] $baseName  " +
                filters.joinToString(" + ") { it.toJson() } + (connection?.let { "  (AUTH ${it.take(6)}…)" } ?: "")

        /** The observer this request reads through: an explicit token wins over the connection. */
        private val observer: String? = lens.tokenObserver ?: lens.connection

        /** The lens's rank table: the fixture's, or nobody's for an observer whose 10040 names no one. */
        private fun trustOf(author: String): Int = if (observer == OBSERVER) RANK[author] ?: 0 else 0

        val order: Order
            get() {
                // Over EVERY filter: one searching filter in the request makes
                // the merged page a ranked one, whatever this case's own text.
                val termless = filters.all { f -> parse(f.search).let { it.terms.isEmpty() && it.phrases.isEmpty() } }
                return when {
                    sort == Sort.RECENT -> Order.CHRONOLOGICAL
                    termless && sort == Sort.NONE -> Order.CHRONOLOGICAL
                    termless && observer == OBSERVER && sort == Sort.RANK -> Order.TRUST_DESC
                    termless && observer == OBSERVER && sort == Sort.RANK_ASC -> Order.TRUST_ASC
                    termless && observer == OBSERVER && sort == Sort.FOLLOWERS -> Order.FOLLOWERS_DESC
                    else -> Order.ANY
                }
            }

        /** What the whole request should serve, before any limit: the union over its filters. */
        fun expected(): List<Event> = ALL.filter { e -> filters.any { f -> matches(f, e) } }

        private fun matches(
            f: Filter,
            e: Event,
        ): Boolean {
            if (!f.copy(search = null, limit = null).match(e)) return false
            val parsed = parse(f.search)
            // Text: every positive term and phrase must be in the event's
            // indexed text; a searchable kind is the only thing that has any.
            if (parsed.terms.isNotEmpty() || parsed.phrases.isNotEmpty()) {
                if (e.kind != 1) return false
                val tokens = tokens(e.content)
                if (!parsed.terms.all { it in tokens }) return false
                if (!parsed.phrases.all { p -> tokens.windowed(p.size).any { it == p } }) return false
            }
            if (parsed.notTerms.any { e.kind == 1 && it in tokens(e.content) }) return false
            // `sort:text` is the documented opt-out of the lens (README: "force
            // pure-text relevance, ignoring the observer"). It takes an
            // EXPLICIT `filter:rank:` floor with it too — the `text` profile
            // gates nothing — although `include:spam` is documented never to
            // drop one. That is today's behavior, pinned here as found; should
            // the floor come to survive `sort:text`, this line moves with it.
            if (sort == Sort.TEXT) return true
            // The gate: only a resolved observer gates, and `include:spam`
            // lifts the DEFAULT floor only — an explicit floor survives it.
            val floor = parsed.floor ?: if (parsed.includeSpam) null else DEFAULT_FLOOR
            if (observer != null && floor != null && trustOf(e.pubKey) < floor) return false
            return true
        }
    }

    /** The oracle's reading of a search string: terms, phrases, exclusions, the floor and the spam switch. */
    private class Parsed(
        val terms: List<String>,
        val phrases: List<List<String>>,
        val notTerms: List<String>,
        val floor: Int?,
        val includeSpam: Boolean,
    )

    private fun parse(search: String?): Parsed {
        if (search == null) return Parsed(emptyList(), emptyList(), emptyList(), null, false)
        val phrases = Regex("\"([^\"]+)\"").findAll(search).map { tokens(it.groupValues[1]) }.toList()
        val words = search.replace(Regex("\"[^\"]*\""), " ").split(' ').filter { it.isNotBlank() }
        val floor =
            words.firstNotNullOfOrNull { w ->
                Regex("filter:rank:(gte|gt):(\\d+)").matchEntire(w)?.let { m -> m.groupValues[2].toInt() + if (m.groupValues[1] == "gt") 1 else 0 }
            }
        return Parsed(
            terms = words.filter { ':' !in it && !it.startsWith("-") },
            phrases = phrases,
            notTerms = words.filter { it.startsWith("-") && ':' !in it }.map { it.drop(1) },
            floor = floor,
            includeSpam = "include:spam" in words,
        )
    }

    private fun tokens(text: String): List<String> = text.lowercase().split(Regex("[^a-z0-9]+")).filter { it.isNotEmpty() }

    private fun describe(ids: Set<String>): String =
        ids
            .mapNotNull { id -> ALL.find { it.id == id } }
            .groupBy { "k${it.kind}/${LABEL[it.pubKey] ?: it.pubKey.take(6)}" }
            .entries
            .joinToString(", ") { (k, v) -> "$k x${v.size}" }

    private suspend fun awaitCorpus(store: NostrSemanticsStore) {
        repeat(120) {
            if (store.count(Filter()) >= ALL.size && store.count(Filter(kinds = listOf(1), search = "pizza")) > 0) return
            delay(500)
        }
        error("corpus never became searchable (${ALL.size} events)")
    }

    // ---- the dimensions ----------------------------------------------------------

    /** How a request names its lens: search tokens, an authenticated connection, or both. */
    private class Lens(
        val name: String,
        val tokens: String? = null,
        val connection: String? = null,
    ) {
        val tokenObserver: String? = tokens?.let { Regex("observer:([0-9a-f]{64})").find(it)?.groupValues?.get(1) }
    }

    private enum class Sort(
        val token: String?,
    ) {
        NONE(null),
        RANK("sort:rank"),
        RANK_ASC("sort:rank:asc"),
        FOLLOWERS("sort:followers"),
        TEXT("sort:text"),
        RECENT("sort:recent"),
    }

    private enum class Text(
        val query: String?,
    ) {
        NONE(null),
        PIZZA("pizza"),
        BITCOIN("bitcoin"),
        PHRASE("\"pizza party\""),
        EXCLUDE("-bitcoin"),
        PIZZA_NOT_BITCOIN("pizza -bitcoin"),
        ;

        val terms: List<String> get() = query?.split(' ')?.filter { !it.startsWith("-") }.orEmpty()
    }

    private enum class Order { CHRONOLOGICAL, TRUST_DESC, TRUST_ASC, FOLLOWERS_DESC, ANY }

    private class Base(
        val name: String,
        val filter: Filter,
    )

    private companion object {
        const val QUERY_PORT = 8080
        const val CONFIG_PORT = 19071
        const val DEFAULT_FLOOR = 2

        /** The staging observer; its 10040 names [PROVIDER] for rank and followers. */
        const val OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

        /** An observer with no 10040 at all: resolves to no lens, "trusts nobody". */
        val STRANGER = "d".repeat(64)
        val PROVIDER = "5e".repeat(32)

        /** Seven authors, one trust tier each; the last has no card at all. */
        val AUTHORS = (1..7).map { it.toString(16).padStart(64, 'b') }
        val RANKS = listOf(90, 60, 50, 10, 2, 1)
        val FOLLOWER_COUNTS = listOf(5, 50, 500, 5000, 1, 2)
        val RANK = AUTHORS.zip(RANKS).toMap()
        val FOLLOWERS = AUTHORS.zip(FOLLOWER_COUNTS).toMap()
        val LABEL = AUTHORS.mapIndexed { i, a -> a to "A${i + 1}(${RANKS.getOrNull(i) ?: "-"})" }.toMap() + mapOf(PROVIDER to "provider", OBSERVER to "observer")

        private var seq = 0

        fun hexId() = (++seq).toString(16).padStart(64, 'a')

        val CARDS =
            AUTHORS.zip(RANKS.zip(FOLLOWER_COUNTS)).map { (subject, rf) ->
                UserAssertionEvent(hexId(), PROVIDER, 1_600_000_000L + rf.first, arrayOf(arrayOf("d", subject), arrayOf("rank", rf.first.toString()), arrayOf("followers", rf.second.toString())), "", "")
            }

        val LIST_10040 = TrustProviderListEvent(hexId(), OBSERVER, 1_600_000_100L, arrayOf(arrayOf("30382:rank", PROVIDER, "wss://scores.test/"), arrayOf("30382:followers", PROVIDER, "wss://scores.test/")), "", "")

        /** Six notes per author, interleaved in time so every order crosses authors. */
        val TEXTS_AND_TAGS: List<Pair<String, Array<Array<String>>>> =
            listOf(
                "pizza party tonight" to arrayOf(arrayOf("t", "food")),
                "party pizza leftovers" to arrayOf(arrayOf("t", "food"), arrayOf("p", AUTHORS[0])),
                "bitcoin price today" to arrayOf(arrayOf("t", "money")),
                "pizza with bitcoin" to arrayOf(arrayOf("t", "food"), arrayOf("t", "money")),
                "quokka sighting" to emptyArray(),
                "zebra crossing" to arrayOf(arrayOf("p", AUTHORS[1])),
            )

        val NOTES: List<Event> =
            TEXTS_AND_TAGS.flatMapIndexed { j, (text, tags) ->
                AUTHORS.mapIndexed { i, author -> TextNoteEvent(hexId(), author, 1_700_000_000L + j * AUTHORS.size + i, tags, text, "e".repeat(128)) }
            }

        val REACTIONS: List<Event> =
            AUTHORS.mapIndexed { i, author ->
                val target = NOTES[(i + 1) % NOTES.size]
                ReactionEvent(hexId(), author, 1_700_000_100L + i, arrayOf(arrayOf("e", target.id), arrayOf("p", target.pubKey)), "+", "e".repeat(128))
            }

        val ALL: List<Event> = CARDS + LIST_10040 + NOTES + REACTIONS

        val NEWEST_FIRST = compareByDescending<Event> { it.createdAt }.thenBy { it.id }

        private val MID = NOTES.sortedBy { it.createdAt }[NOTES.size / 2].createdAt
        private val EARLY = NOTES.minOf { it.createdAt } + 5

        val BASES =
            listOf(
                Base("everything", Filter()),
                Base("kind 1", Filter(kinds = listOf(1))),
                Base("kinds 1,7", Filter(kinds = listOf(1, 7))),
                Base("kind 7", Filter(kinds = listOf(7))),
                Base("kind 30382", Filter(kinds = listOf(30382))),
                Base("authors A1,A4,A7", Filter(authors = listOf(AUTHORS[0], AUTHORS[3], AUTHORS[6]))),
                Base("kind 1 by A3", Filter(kinds = listOf(1), authors = listOf(AUTHORS[2]))),
                Base("kind 1 by A5,A6 (floor edge)", Filter(kinds = listOf(1), authors = listOf(AUTHORS[4], AUTHORS[5]))),
                Base("#t food", Filter(tags = mapOf("t" to listOf("food")))),
                Base("#t food|money", Filter(tags = mapOf("t" to listOf("food", "money")))),
                Base("&t food+money", Filter(tagsAll = mapOf("t" to listOf("food", "money")))),
                Base("#p A1", Filter(tags = mapOf("p" to listOf(AUTHORS[0])))),
                Base("since mid", Filter(since = MID)),
                Base("until mid", Filter(until = MID)),
                Base("kind 1 window", Filter(kinds = listOf(1), since = EARLY, until = MID)),
                Base("ids x4", Filter(ids = listOf(NOTES[0].id, NOTES[9].id, NOTES[20].id, REACTIONS[3].id))),
                Base("kind 1 #t food since early", Filter(kinds = listOf(1), tags = mapOf("t" to listOf("food")), since = EARLY)),
            )

        val MULTI: List<Pair<String, List<Filter>>> =
            listOf(
                "multi: kind1 by A1 + kind1 #t food (overlap)" to listOf(Filter(kinds = listOf(1), authors = listOf(AUTHORS[0])), Filter(kinds = listOf(1), tags = mapOf("t" to listOf("food")))),
                "multi: kind 7 + kind1 by A7" to listOf(Filter(kinds = listOf(7)), Filter(kinds = listOf(1), authors = listOf(AUTHORS[6]))),
            )

        val LENSES =
            listOf(
                Lens("no lens"),
                Lens("observer", tokens = "observer:$OBSERVER"),
                Lens("AUTH observer", connection = OBSERVER),
                Lens("observer include:spam", tokens = "observer:$OBSERVER include:spam"),
                Lens("include:spam", tokens = "include:spam"),
                Lens("observer gte:50", tokens = "observer:$OBSERVER filter:rank:gte:50"),
                Lens("observer gte:90", tokens = "observer:$OBSERVER filter:rank:gte:90"),
                Lens("observer gt:49", tokens = "observer:$OBSERVER filter:rank:gt:49"),
                Lens("observer gte:0", tokens = "observer:$OBSERVER filter:rank:gte:0"),
                Lens("AUTH observer gte:50", tokens = "filter:rank:gte:50", connection = OBSERVER),
                Lens("gte:50, no observer", tokens = "filter:rank:gte:50"),
                Lens("observer gte:50 include:spam", tokens = "observer:$OBSERVER filter:rank:gte:50 include:spam"),
                Lens("lens-less observer", tokens = "observer:$STRANGER"),
                Lens("lens-less observer include:spam", tokens = "observer:$STRANGER include:spam"),
                Lens("token beats AUTH", tokens = "observer:$OBSERVER filter:rank:gte:50", connection = STRANGER),
            )

        val SORTS = Sort.entries
        val TEXTS = Text.entries

        fun dockerAvailable(): Boolean = runCatching { DockerClientFactory.instance().isDockerAvailable }.getOrDefault(false)
    }
}
