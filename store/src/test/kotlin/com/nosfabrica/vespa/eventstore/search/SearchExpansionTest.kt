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
package com.nosfabrica.vespa.eventstore.search

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * WHAT A SEARCH ANSWERS WITH BESIDES ITS HITS: the record a label describes,
 * the profile an assertion is about, the members a Trusted List names.
 *
 * The full stack, because the gate is part of it — `NostrSemanticsStore` over
 * `TrustProjection`, which is what `VespaEventStore.open()` assembles and the
 * only shape where a 10040 write invalidates the delegation map it is read
 * through. A store over a bare index admits no declaration at all, which is its
 * own case below.
 *
 * Every search here declares `include:spam`: the terms are what matters and the
 * default trust floor would otherwise gate an unranked corpus to nothing.
 */
class SearchExpansionTest {
    private val relayUrl = "wss://sot.test/".normalizeRelayUrl()
    private val reader = key("a1")
    private val curator = key("b2")
    private val stranger = key("c3")
    private val subject = key("d4")

    private val index = InMemoryEventIndex()
    private val store = NostrSemanticsStore(TrustProjection(index, InMemoryReputationIndex()), relay = relayUrl)

    private var t = 1_000_000L
    private var seq = 0

    private fun next() = t++

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun key(prefix: String) = prefix.repeat(32)

    private fun event(
        kind: Int,
        tags: Array<Array<String>>,
        content: String = "",
        author: String = curator,
        at: Long = next(),
    ): Event = EventFactory.create(id(), author, at, kind, tags, content, "")

    /** The reader's Treasure Map: `curator` computes for them, and per kind. */
    private fun treasureMap(vararg entries: Array<String>) = event(10040, arrayOf(*entries), author = reader)

    /** A kind-0 for [subject] — what a list of pubkeys and a contact card both splice. */
    private val profile = event(0, emptyArray(), """{"name":"Ada Bramble"}""", author = subject)

    /** The note a label points at. None of the searched words are in it. */
    private val note = event(1, emptyArray(), "the third episode is up", author = subject)

    private fun userList(
        title: String,
        members: List<String> = listOf(subject),
        author: String = curator,
        listId: String = "roster",
    ) = event(
        30392,
        buildList {
            add(arrayOf("d", listId))
            add(arrayOf("title", title))
            members.forEach { add(arrayOf("p", it, "", "80")) }
        }.toTypedArray(),
        author = author,
    )

    private suspend fun page(vararg filters: Filter): List<String> {
        val out = store.query<Event>(filters.toList()).map { it.id }
        // A feature whose whole job is to ADD events to a page is the one most
        // likely to send one twice, so every page in this file is checked.
        assertEquals(out.distinct(), out, "an event was served twice on one read")
        return out
    }

    private fun search(
        terms: String,
        kinds: List<Int>,
        observer: String? = reader,
    ) = Filter(kinds = kinds, search = "$terms include:spam" + (observer?.let { " observer:$it" } ?: ""))

    // ------------------------------------------------------------------
    // The three families
    // ------------------------------------------------------------------

    @Test
    fun `a label brings the record it describes, ungated`() =
        runBlocking {
            store.insert(note)
            val label = event(1985, arrayOf(arrayOf("L", "#health"), arrayOf("l", "medical", "#health"), arrayOf("e", note.id)))
            store.insert(label)

            // No observer at all: a NIP-32 label is a description anyone may
            // publish, so it expands for an anonymous read the way it expands
            // for anybody. Gating it would make most of a relay's traffic blind
            // to its own corpus.
            assertEquals(listOf(label.id, note.id), page(search("medical", listOf(1, 1985), observer = null)))
        }

    @Test
    fun `an assertion brings the profile it is about, once its signer is named`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30382:rank", curator, "wss://provider.example")))
            val card = event(30382, arrayOf(arrayOf("d", subject), arrayOf("petname", "Bramblecast"), arrayOf("rank", "90")))
            store.insert(card)

            assertEquals(listOf(card.id, profile.id), page(search("bramblecast", listOf(0, 30382))))
        }

    @Test
    fun `a trusted list brings its members`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            assertEquals(listOf(list.id, profile.id), page(search("podcaster", listOf(0, 30392))))
        }

    // ------------------------------------------------------------------
    // The gate
    // ------------------------------------------------------------------

    @Test
    fun `a list from a signer the reader never named expands nothing`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val theirs = userList("Podcaster Trust List")
            val strangers = userList("Podcastical Roster", author = stranger, listId = "outsiders")
            store.insert(theirs)
            store.insert(strangers)

            assertEquals(listOf(theirs.id, profile.id), page(search("podcaster", listOf(0, 30392))))
            assertEquals(listOf(strangers.id), page(search("podcastical", listOf(0, 30392))))
        }

    @Test
    fun `a delegation opens the kind it names and no other`() =
        runBlocking {
            store.insert(profile)
            // Appointed to RANK users, and nothing else. The contact card is a
            // computation this reader asked for; the same publisher's Trusted
            // List of those same users is not.
            store.insert(treasureMap(arrayOf("30382:rank", curator, "wss://provider.example")))
            val card = event(30382, arrayOf(arrayOf("d", subject), arrayOf("petname", "Bramblecast")))
            val list = userList("Podcaster Trust List")
            store.insert(card)
            store.insert(list)

            assertEquals(listOf(card.id, profile.id), page(search("bramblecast", listOf(0, 30382))))
            assertEquals(listOf(list.id), page(search("podcaster", listOf(0, 30392))))
        }

    @Test
    fun `a reserved named entry drives nothing, and a bare kind drives everything`() =
        runBlocking {
            store.insert(profile)
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // `30392:<name>` is RESERVED by the ADR and must drive no behavior.
            store.insert(treasureMap(arrayOf("30392:podcasters", curator, "wss://lists.example")))
            assertEquals(listOf(list.id), page(search("podcaster", listOf(0, 30392))))

            // The generic bare-kind entry is the one that delegates. A 10040 is
            // replaceable, so this supersedes the reserved one above — and the
            // projection is invalidated by the write, which is the whole reason
            // the gate lives beside it.
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            assertEquals(listOf(list.id, profile.id), page(search("podcaster", listOf(0, 30392))))
        }

    @Test
    fun `a reader's own list needs no delegation, and an anonymous read expands none`() =
        runBlocking {
            store.insert(profile)
            val mine = userList("Podcaster Trust List", author = reader)
            store.insert(mine)

            assertEquals(listOf(mine.id, profile.id), page(search("podcaster", listOf(0, 30392))))
            // No observer: nothing to have delegated, so nothing unpacks.
            assertEquals(listOf(mine.id), page(search("podcaster", listOf(0, 30392), observer = null)))
        }

    @Test
    fun `a store with no trust projection admits no declaration`() =
        runBlocking {
            val bare = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            bare.insert(profile)
            bare.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            bare.insert(list)

            // The 10040 is stored and served; there is simply no projection to
            // read it through, and a gate that cannot resolve admits nothing.
            val out = bare.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id }
            assertEquals(listOf(list.id), out)
        }

    // ------------------------------------------------------------------
    // What a read did NOT ask for
    // ------------------------------------------------------------------

    @Test
    fun `a termless read expands nothing at all`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // Plain NIP-01 recall, and the `include:spam` lens on its own is not
            // a term — which is what makes the expansion safe to run on every
            // read: a mirror's paging carries exactly this shape.
            assertEquals(listOf(list.id), page(Filter(kinds = listOf(30392), search = "include:spam")))
            assertEquals(listOf(list.id), page(Filter(kinds = listOf(30392))))
            // Tag recall still serves the membership, which is what it is for.
            assertEquals(listOf(list.id), page(Filter(kinds = listOf(30392), tags = mapOf("p" to listOf(subject)))))
        }

    @Test
    fun `a subject the read cannot admit is not added`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // The read asks for lists only. The member's kind 0 satisfies the
            // pointer but not the read, and the engine — not a second matcher —
            // is what refuses it.
            assertEquals(listOf(list.id), page(search("podcaster", listOf(30392))))
            // Same for an author constraint that the subject fails.
            val onlyCurator = Filter(kinds = listOf(0, 30392), authors = listOf(curator), search = "podcaster include:spam observer:$reader")
            assertEquals(listOf(list.id), page(onlyCurator))
        }

    // ------------------------------------------------------------------
    // Once, and in order
    // ------------------------------------------------------------------

    @Test
    fun `a subject that is also a hit is served once, at its own place`() =
        runBlocking {
            // The profile itself matches the search, so it is a HIT — and the
            // list names it too. It must go out once, at the rank it earned,
            // not a second time behind the list.
            val named = event(0, emptyArray(), """{"name":"Podcaster Ada"}""", author = subject)
            store.insert(named)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            val out = page(search("podcaster", listOf(0, 30392)))
            assertEquals(2, out.size, "the hit and the list, and nothing spliced twice: $out")
            assertTrue(named.id in out && list.id in out)
        }

    @Test
    fun `many pointers naming one subject send it once`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val first = userList("Podcaster Trust List", listId = "one")
            val second = userList("Podcaster Roster", listId = "two")
            store.insert(first)
            store.insert(second)

            val out = page(search("podcaster", listOf(0, 30392)))
            assertEquals(3, out.size, "two lists and one profile: $out")
            assertEquals(1, out.count { it == profile.id })
        }

    @Test
    fun `members ride in the order the list names them`() =
        runBlocking {
            // Inserted back to front, so recall order and tag order disagree and
            // only one of them can produce this assertion. A publisher sorts a
            // list by the score it computed, and that ordering is the only thing
            // that makes a spliced member's position mean its rank.
            val members = listOf(key("e5"), key("f6"), key("07"))
            members.reversed().forEach { store.insert(event(0, emptyArray(), """{"name":"m"}""", author = it)) }
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List", members = members)
            store.insert(list)

            val out = page(search("podcaster", listOf(0, 30392)))
            val spliced = out.drop(1)
            val byAuthor = store.query<Event>(listOf(Filter(kinds = listOf(0), authors = members))).associateBy { it.pubKey }
            assertEquals(members.map { byAuthor.getValue(it).id }, spliced, "members in the order the list names them")
        }

    @Test
    fun `the per-event cap truncates the splice and never the page`() =
        runBlocking {
            val capped =
                NostrSemanticsStore(
                    TrustProjection(index, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(maxPerEvent = 1),
                )
            val second = key("e5")
            store.insert(profile)
            store.insert(event(0, emptyArray(), """{"name":"Bo Quill"}""", author = second))
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val pair = userList("Podcaster Trust List", members = listOf(subject, second))
            store.insert(pair)

            val out = capped.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id }
            assertEquals(listOf(pair.id, profile.id), out, "the list, then the FIRST member it names")
        }

    // ------------------------------------------------------------------
    // Weighted placement — the score decides where, not just whether
    // ------------------------------------------------------------------

    /**
     * An index that ranks, which the in-memory reference deliberately does not:
     * it reports a null rather than a fabricated constant, and weighted
     * placement has nothing to discount without a real one.
     *
     * Scores descend with recall order, which is what a relevance-ordered engine
     * hands back — so the anchored page and the weighted page start identical
     * and any difference between them is the weighting.
     */
    private class RankingIndex(
        private val inner: InMemoryEventIndex,
    ) : com.nosfabrica.vespa.eventstore.engine.EventIndex by inner {
        override suspend fun searchRanked(query: com.nosfabrica.vespa.eventstore.engine.query.EventQuery) =
            inner.search(query).mapIndexed { i, doc ->
                com.nosfabrica.vespa.eventstore.engine
                    .Ranked(doc, 100.0 - i)
            }
    }

    @Test
    fun `a doubted member sinks below a confident one, and below the hits between them`() =
        runBlocking {
            val ranking = RankingIndex(InMemoryEventIndex())
            val weighted =
                NostrSemanticsStore(
                    TrustProjection(ranking, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(placement = SplicePlacement.Weighted(gamma = 1.0)),
                )
            val sure = key("e5")
            val doubted = key("f6")
            weighted.insert(event(0, emptyArray(), """{"name":"Sure"}""", author = sure))
            weighted.insert(event(0, emptyArray(), """{"name":"Doubted"}""", author = doubted))
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            // Two OTHER hits the search finds on its own, which the doubted
            // member has to fall past for the weighting to have done anything.
            val filler = (1..2).map { event(1, emptyArray(), "podcaster note $it", author = stranger) }
            filler.forEach { weighted.insert(it) }
            val list =
                event(
                    30392,
                    arrayOf(
                        arrayOf("d", "roster"),
                        arrayOf("title", "Podcaster Trust List"),
                        arrayOf("p", sure, "", "100"),
                        arrayOf("p", doubted, "", "10"),
                    ),
                )
            weighted.insert(list)

            val out = weighted.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
            val sureId = weighted.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(sure)))).single().id
            val doubtedId = weighted.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(doubted)))).single().id

            assertTrue(out.indexOf(sureId) < out.indexOf(doubtedId), "confidence orders the two members: $out")
            assertTrue(out.indexOf(list.id) < out.indexOf(sureId), "a subject never passes its own pointer: $out")
            // The whole point: the doubted member is no longer glued behind its
            // list — organic hits the search found itself now sit above it.
            assertTrue(out.indexOf(doubtedId) > out.indexOf(sureId) + 1, "the doubted member fell past a hit: $out")
        }

    @Test
    fun `an unscored reference is full confidence, not doubt`() =
        runBlocking {
            // A label expresses no confidence — NIP-32 has no such field — and a
            // pointer that says nothing must not be read as unsure. Its subject
            // stays where the anchored placement would have put it.
            val ranking = RankingIndex(InMemoryEventIndex())
            val weighted =
                NostrSemanticsStore(
                    TrustProjection(ranking, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(placement = SplicePlacement.Weighted()),
                )
            weighted.insert(note)
            val label = event(1985, arrayOf(arrayOf("L", "#health"), arrayOf("l", "medical", "#health"), arrayOf("e", note.id)))
            weighted.insert(label)

            val out = weighted.query<Event>(listOf(search("medical", listOf(1, 1985), observer = null))).map { it.id }
            assertEquals(listOf(label.id, note.id), out, "an unscored pointer places its subject exactly as Anchored would")
        }

    @Test
    fun `a page the engine did not score is placed anchored`() =
        runBlocking {
            // The in-memory reference reports null scores. Weighted has nothing
            // to discount, and a fabricated constant would be worse than the
            // anchored order it replaced — so it degrades rather than inventing.
            val unranked =
                NostrSemanticsStore(
                    TrustProjection(index, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(placement = SplicePlacement.Weighted()),
                )
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            assertEquals(listOf(list.id, profile.id), unranked.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id })
        }

    @Test
    fun `gamma above one punishes doubt harder`() {
        val soft = SplicePlacement.Weighted(gamma = 0.5)
        val hard = SplicePlacement.Weighted(gamma = 2.0)
        // The same pointer, the same 25% confidence, three different verdicts.
        assertEquals(50.0, soft.scoreFor(100.0, 0.25))
        assertEquals(25.0, SplicePlacement.Weighted(1.0).scoreFor(100.0, 0.25))
        assertEquals(6.25, hard.scoreFor(100.0, 0.25))
        // Absent confidence is full confidence, at every gamma.
        assertEquals(100.0, hard.scoreFor(100.0, null))
        // Nothing to discount is nothing to place by.
        assertEquals(null, hard.scoreFor(null, 0.25))
    }

    @Test
    fun `the expansion can be switched off outright`() =
        runBlocking {
            val off =
                NostrSemanticsStore(
                    TrustProjection(index, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits.Off,
                )
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            assertEquals(listOf(list.id), off.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id })
        }
}
