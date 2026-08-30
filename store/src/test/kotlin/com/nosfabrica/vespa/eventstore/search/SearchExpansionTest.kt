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
    // Conversion: a read that only wants the subjects' kinds
    // ------------------------------------------------------------------

    @Test
    fun `a search for profiles alone still gets the list that converts to them`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // kinds:[0] — the shape a client hunting people actually sends.
            // The list is not an asked-for kind, but it converts to one, so it
            // is fetched, served (the client needs the pointer to know what to
            // process) and unpacked.
            assertEquals(listOf(list.id, profile.id), page(search("podcaster", listOf(0))))
        }

    @Test
    fun `a search for notes alone still gets the label that converts to them`() =
        runBlocking {
            store.insert(note)
            val label = event(1985, arrayOf(arrayOf("L", "#health"), arrayOf("l", "medical", "#health"), arrayOf("e", note.id)))
            store.insert(label)

            assertEquals(listOf(label.id, note.id), page(search("medical", listOf(1), observer = null)))
        }

    @Test
    fun `an assertion converts to the profile kind once its signer is named`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30382:rank", curator, "wss://provider.example")))
            val card = event(30382, arrayOf(arrayOf("d", subject), arrayOf("petname", "Bramblecast"), arrayOf("rank", "90")))
            store.insert(card)

            assertEquals(listOf(card.id, profile.id), page(search("bramblecast", listOf(0))))
        }

    @Test
    fun `an event list converts to the notes it names`() =
        runBlocking {
            store.insert(note)
            store.insert(treasureMap(arrayOf("30393", curator, "wss://lists.example")))
            val list = event(30393, arrayOf(arrayOf("d", "episodes"), arrayOf("title", "Podcast Episodes"), arrayOf("e", note.id, "", "80")))
            store.insert(list)

            assertEquals(listOf(list.id, note.id), page(search("episodes", listOf(1))))
        }

    @Test
    fun `the conversion fetch never surfaces a stranger's list`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val strangers = userList("Podcaster Roster", author = stranger, listId = "outsiders")
            store.insert(strangers)

            // Asked for explicitly, a stranger's list is a plain NIP-01 hit,
            // expanding nothing. As a conversion — this store's own addition
            // to the feed — only what the gate would unpack is fetched at all.
            assertEquals(listOf(strangers.id), page(search("podcaster", listOf(0, 30392))))
            assertEquals(emptyList(), page(search("podcaster", listOf(0))))
        }

    @Test
    fun `an anonymous kind-restricted search converts labels and nothing else`() =
        runBlocking {
            store.insert(profile)
            store.insert(userList("Podcaster Trust List"))
            val label = event(1985, arrayOf(arrayOf("L", "#trades"), arrayOf("l", "podcaster", "#trades"), arrayOf("p", subject)))
            store.insert(label)

            // No observer: the declaration companion is never even fetched —
            // the matching list stays out of the page — while the ungated
            // label still converts to the profile it describes.
            assertEquals(listOf(label.id, profile.id), page(search("podcaster", listOf(0), observer = null)))
        }

    @Test
    fun `a termless kind-restricted read fetches no pointer`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            store.insert(userList("Podcaster Trust List"))

            // The observer lens alone is not a term: plain recall answers
            // exactly the kinds it was asked, nothing converted, nothing added.
            assertEquals(listOf(profile.id), page(Filter(kinds = listOf(0), search = "include:spam observer:$reader")))
        }

    @Test
    fun `a conversion respects the read's author constraint on its subjects`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // The list converts and is served either way; whether the member
            // rides along is the engine's admission under the read's own
            // authors filter, exactly as with an explicitly asked-for pointer.
            val withSubject = Filter(kinds = listOf(0), authors = listOf(curator, subject), search = "podcaster include:spam observer:$reader")
            assertEquals(listOf(list.id, profile.id), page(withSubject))
            val withoutSubject = Filter(kinds = listOf(0), authors = listOf(curator), search = "podcaster include:spam observer:$reader")
            assertEquals(listOf(list.id), page(withoutSubject))
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

    @Test
    fun `a row the index hands back twice still goes out once`() =
        runBlocking {
            // The store's own recall dedups by id, so nothing else here would
            // notice if this guard went. But the expansion is the thing ADDING
            // events to a page, and a page it builds must be free of duplicates
            // because of what IT does rather than because of what the index
            // below it happens to promise. This is that contract, stated
            // against an index that breaks the promise.
            //
            // It used to live in vespa-relay, against a doubling IEventStore
            // wrapped around this one. There is no wrapper left to put there,
            // which is why the contract came with the code it constrains.
            val doubling =
                object : com.nosfabrica.vespa.eventstore.engine.EventIndex by index {
                    override suspend fun search(query: com.nosfabrica.vespa.eventstore.engine.query.EventQuery) = index.search(query).flatMap { listOf(it, it) }
                }
            val doubled = NostrSemanticsStore(TrustProjection(doubling, InMemoryReputationIndex()), relay = relayUrl)

            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            val out = doubled.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id }
            assertEquals(out.distinct(), out, "a doubled row must not double on the way out: $out")
            assertTrue(list.id in out && profile.id in out, "and the page is still served whole: $out")
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
        // HITS ON THE TOKEN RUNG, members on the affiliation rung — the two
        // scales `event.sd` actually produces, not the flat 100..0 this used to
        // report. A linear ladder is the one shape where the OLD placement
        // (pointer relevance x confidence) looked correct, which is exactly why
        // it hid the bug: on the real banded scale a discounted member leaves
        // its band entirely.
        //
        // A member query is delegated, because InMemoryEventIndex implements
        // the member rungs itself and this class must not be a second answer to
        // where a member sits.
        override suspend fun searchRanked(query: com.nosfabrica.vespa.eventstore.engine.query.EventQuery) =
            if (query.ranking != null) {
                inner.searchRanked(query)
            } else {
                inner.search(query).mapIndexed { i, doc ->
                    // THE `search` LADDER, because every query in this file
                    // carries `observer:` and so ranks on it: rungs 550
                    // (affiliation) / 4 000 (weak) / 23 000 (near) / 130 000
                    // (name). A hit whose content says "weak" is scored INSIDE
                    // the member band (550..4 000) so a test can show a member
                    // interleaving with organic results; on the real engine that
                    // is an ordinary bio-strength match.
                    val band =
                        when {
                            // Inside the member band (550..4 000).
                            doc.content.contains("weak") -> 1000.0

                            // ABOVE the member band, below the name rung — where
                            // a subject that kept its pointer's score outranks it
                            // and one placed on the rung does not.
                            doc.content.contains("mid") -> 20_000.0

                            else -> 130_000.0
                        }
                    com.nosfabrica.vespa.eventstore.engine
                        .Ranked(doc, band - i * 0.01)
                }
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
                    searchExpansion = SearchExpansionLimits(),
                )
            val sure = key("e5")
            val doubted = key("f6")
            weighted.insert(event(0, emptyArray(), """{"name":"Sure"}""", author = sure))
            weighted.insert(event(0, emptyArray(), """{"name":"Doubted"}""", author = doubted))
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            // Two OTHER hits the search finds on its own, which the doubted
            // member has to fall past for the weighting to have done anything.
            // An organic hit INSIDE the member band, which is what a member has
            // to be able to fall past for the placement to mean anything.
            val weak = event(1, emptyArray(), "podcaster note, weak match", author = stranger)
            weighted.insert(weak)
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
            // THE WHOLE POINT: the members are no longer glued behind their
            // list. They sit on the affiliation rung, and the organic hit that
            // scored inside that rung lands BETWEEN them — above the member its
            // publisher doubts, below the one it is sure of.
            assertTrue(out.indexOf(weak.id) > out.indexOf(sureId), "a full-confidence member outranks a bio-strength hit: $out")
            assertTrue(out.indexOf(weak.id) < out.indexOf(doubtedId), "and a doubted one falls below it: $out")
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
                    searchExpansion = SearchExpansionLimits(),
                )
            weighted.insert(note)
            val label = event(1985, arrayOf(arrayOf("L", "#health"), arrayOf("l", "medical", "#health"), arrayOf("e", note.id)))
            weighted.insert(label)

            val out = weighted.query<Event>(listOf(search("medical", listOf(1, 1985), observer = null))).map { it.id }
            assertEquals(listOf(label.id, note.id), out, "an unscored pointer places its subject exactly as Anchored would")
        }

    @Test
    fun `a page the engine did not score keeps the pointer's own order`() =
        runBlocking {
            // The in-memory reference reports null scores, so there is nothing
            // to discount. A fabricated constant would be worse than the
            // pointer's own order, so the placement degrades rather than
            // inventing — which is also what a recency-ordered read gets.
            val unranked = NostrSemanticsStore(TrustProjection(index, InMemoryReputationIndex()), relay = relayUrl)
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            assertEquals(listOf(list.id, profile.id), unranked.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id })
        }

    @Test
    fun `gamma reaches the engine, where the rung applies it`() =
        runBlocking {
            // The exponent used to be applied here, to the pointer's relevance.
            // It is a rank feature now — the store's only remaining part in it
            // is handing it to the member profile — so what this pins is that it
            // still ARRIVES, and that a harsher gamma sinks a doubted member
            // further. The curve itself is `event.sd` §13's to own.
            val ranking = RankingIndex(InMemoryEventIndex())
            val hard =
                NostrSemanticsStore(
                    TrustProjection(ranking, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(confidenceGamma = 4.0),
                )
            val doubted = key("f6")
            hard.insert(event(0, emptyArray(), """{"name":"Doubted"}""", author = doubted))
            hard.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            // Scored inside the member band, between where c=0.5 lands at
            // gamma 1.0 (2275) and where it lands at gamma 4.0 (766).
            val weak = event(1, emptyArray(), "podcaster note, weak match", author = stranger)
            hard.insert(weak)
            val list =
                event(
                    30392,
                    arrayOf(arrayOf("d", "roster"), arrayOf("title", "Podcaster Trust List"), arrayOf("p", doubted, "", "50")),
                )
            hard.insert(list)

            val out = hard.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
            val card = hard.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(doubted)))).single().id
            assertTrue(out.indexOf(card) > out.indexOf(weak.id), "at gamma 4 the doubted member sinks below the weak hit: $out")
        }

    @Test
    fun `a member two lists disagree about is placed by the higher confidence`() =
        runBlocking {
            // One publisher, two rosters, the same person scored 100 on one and
            // 10 on the other — which real curators do, because a list is
            // computed per topic. The lookups are issued per confidence bucket
            // and the first answer wins, so the bucket ORDER decides this. Both
            // lists are from a signer the reader delegated and both vouched, so
            // the generous reading is the right one.
            val ranking = RankingIndex(InMemoryEventIndex())
            val weighted = NostrSemanticsStore(TrustProjection(ranking, InMemoryReputationIndex()), relay = relayUrl)
            val member = key("f6")
            weighted.insert(event(0, emptyArray(), """{"name":"Contested"}""", author = member))
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            // Between where c=0.10 lands (895) and where c=1.00 lands (4000).
            val weak = event(1, emptyArray(), "podcaster note, weak match", author = stranger)
            weighted.insert(weak)
            weighted.insert(
                event(30392, arrayOf(arrayOf("d", "sure"), arrayOf("title", "Podcaster Trust List"), arrayOf("p", member, "", "100"))),
            )
            weighted.insert(
                event(30392, arrayOf(arrayOf("d", "doubt"), arrayOf("title", "Podcaster Roster"), arrayOf("p", member, "", "10"))),
            )

            val out = weighted.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
            val card = weighted.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(member)))).single().id
            assertTrue(card in out, "the contested member is served: $out")
            assertTrue(out.indexOf(card) < out.indexOf(weak.id), "placed by the list that was sure, not the one that doubted: $out")
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

    @Test
    fun `truncation keeps the confidence the survivors were scored with`() =
        runBlocking {
            // The cap and the weighting are independent: a list longer than
            // [SearchExpansionLimits.maxPerEvent] still places the members it
            // DID bring by what the publisher said about them. A 2,000-member
            // list is the motivating case for both, so the one that truncates
            // is exactly the one that must not lose its scores.
            val ranking = RankingIndex(InMemoryEventIndex())
            val weighted =
                NostrSemanticsStore(
                    TrustProjection(ranking, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(maxPerEvent = 2),
                )
            val sure = key("e5")
            val doubted = key("f6")
            val past = key("07")
            weighted.insert(event(0, emptyArray(), """{"name":"Sure"}""", author = sure))
            weighted.insert(event(0, emptyArray(), """{"name":"Doubted"}""", author = doubted))
            weighted.insert(event(0, emptyArray(), """{"name":"Past the cap"}""", author = past))
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
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
                        arrayOf("p", past, "", "100"),
                    ),
                )
            weighted.insert(list)

            val out = weighted.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
            val cards = weighted.query<Event>(listOf(Filter(kinds = listOf(0)))).associateBy { it.pubKey }

            assertTrue(cards.getValue(past).id !in out, "the third member is past the cap and was never looked up: $out")
            assertTrue(out.indexOf(cards.getValue(sure).id) < out.indexOf(cards.getValue(doubted).id), "confidence survives the truncation: $out")
            // The fillers are token-band hits, so BOTH survivors sit below
            // them: a member rides the affiliation rung whatever its list
            // scored, and a note that actually contains the word outranks a
            // person somebody put on a list. What truncation must not lose is
            // the ORDER between the two it kept, which is the assertion above.
            assertTrue(
                filler.all { out.indexOf(it.id) < out.indexOf(cards.getValue(sure).id) },
                "a token match outranks a member of a list: $out",
            )
        }

    @Test
    fun `two filters of one read each expand their own kinds`() =
        runBlocking {
            // Same observer, same waiver — so the two filters share a LENS and
            // differ only in what they ask for. Reading the second one's
            // pointers through the first one's kinds would leave one of the two
            // families unexpanded, which is the whole read half-answered.
            store.insert(note)
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val label = event(1985, arrayOf(arrayOf("L", "#topic"), arrayOf("l", "podcaster", "#topic"), arrayOf("e", note.id)))
            store.insert(label)
            val list = userList("Podcaster Trust List")
            store.insert(list)

            val out = page(search("podcaster", listOf(1, 1985)), search("podcaster", listOf(0, 30392)))
            assertTrue(note.id in out, "the label's subject: $out")
            assertTrue(profile.id in out, "the list's member: $out")
        }

    @Test
    fun `a read that named ids serves no subject outside them`() =
        runBlocking {
            // "Admission is the engine's own job" only holds if the lookup keeps
            // every constraint the finding query carried. `ids` is the one a
            // keyed lookup is most tempted to clear, and clearing it hands back
            // a profile the read explicitly excluded.
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            val onlyTheList = Filter(ids = listOf(list.id), search = "podcaster include:spam observer:$reader")
            assertEquals(listOf(list.id), page(onlyTheList), "the member is not one of the ids the read asked for")
        }

    @Test
    fun `a read that named a d tag serves no coordinate outside it`() =
        runBlocking {
            // Same rule for an addressable subject, where the lookup has to ADD
            // a `#d` of its own: `tags` is a map, so writing it rather than
            // intersecting it would drop the read's own `#d` and serve the
            // article it had excluded.
            val article = { d: String, title: String -> event(30023, arrayOf(arrayOf("d", d), arrayOf("title", title)), author = subject) }
            val wanted = article("kept", "Kept Article")
            val other = article("other", "Other Article")
            store.insert(wanted)
            store.insert(other)
            store.insert(treasureMap(arrayOf("30394", curator, "wss://lists.example")))
            val list =
                event(
                    30394,
                    arrayOf(
                        arrayOf("d", "roster"),
                        arrayOf("title", "Podcaster Trust List"),
                        arrayOf("a", "30023:$subject:kept", "", "90"),
                        arrayOf("a", "30023:$subject:other", "", "90"),
                    ),
                )
            store.insert(list)

            val narrowed = Filter(kinds = listOf(30023, 30394), tags = mapOf("d" to listOf("kept", "roster")), search = "podcaster include:spam observer:$reader")
            val out = page(narrowed)
            assertTrue(wanted.id in out, "the member whose d the read asked for: $out")
            assertTrue(other.id !in out, "the member whose d the read excluded: $out")
        }

    @Test
    fun `an unscored pointer keeps its subject, the rung is only for a scored member`() =
        runBlocking {
            // A NIP-32 label expresses no confidence, so there is no doubt for a
            // rung to express and its subject stays where it always sat: its
            // pointer's own score, directly behind it. Putting a label's subject
            // on the member rung would move it to a fixed place in the page
            // regardless of how well the label itself matched — on a real page
            // that meant a label scoring 1306 handing its note a 610.
            //
            // This is also the only splice an observerless read performs at all:
            // a Trusted List is a DECLARATION and unpacks only for a reader who
            // delegated its signer, so `Enrolment.NONE` admits none of them.
            val ranking = RankingIndex(InMemoryEventIndex())
            val weighted = NostrSemanticsStore(TrustProjection(ranking, InMemoryReputationIndex()), relay = relayUrl)
            weighted.insert(note)
            val label = event(1985, arrayOf(arrayOf("L", "#topic"), arrayOf("l", "medical", "#topic"), arrayOf("e", note.id)))
            weighted.insert(label)
            val mid = event(1, emptyArray(), "medical, mid match", author = stranger)
            weighted.insert(mid)

            // WITH an observer, so the read ranks on the ladder that HAS a
            // member rung — the only place this distinction is observable. A
            // label is ungated, so it expands here too; what must not happen is
            // its subject being scored as if it were a list member.
            val out = weighted.query<Event>(listOf(search("medical", listOf(1, 1985)))).map { it.id }
            assertEquals(listOf(label.id, note.id), out.take(2), "the label, then its note, above the mid-band hit: $out")
            assertTrue(out.indexOf(mid.id) > out.indexOf(note.id), "a rung-placed subject would have fallen below it: $out")
        }

    @Test
    fun `a ladder the member rungs do not cover degrades to the pointer's order`() =
        runBlocking {
            // `sort:rank:asc` and its siblings rank on a THIRD ladder, one whose
            // scores can run below zero (it subtracts the author's trust from
            // the match tiers). There is no member rung defined on it, so
            // `memberProfileOf` returns null, every member comes back unscored,
            // and the page keeps the pointer's own order rather than mixing two
            // scales. That degradation is the contract — an invented placement
            // on an uncovered ladder is how the previous design put the most
            // doubted member of a list at the top of the page.
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            val sorted = Filter(kinds = listOf(0, 30392), search = "podcaster include:spam observer:$reader sort:rank:asc")
            assertEquals(listOf(list.id, profile.id), page(sorted), "the list, then its member, in the order the list named them")
        }
}
