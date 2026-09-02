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

    /** A Treasure Map: `curator` computes for [owner], and per kind. */
    private fun treasureMap(
        vararg entries: Array<String>,
        owner: String = reader,
    ) = event(10040, arrayOf(*entries), author = owner)

    /** A kind-0 for [subject] — what a list of pubkeys and a contact card both splice. */
    private val profile = event(0, emptyArray(), """{"name":"Ada Bramble"}""", author = subject)

    /** The note a label points at. None of the searched words are in it. */
    private val note = event(1, emptyArray(), "the third episode is up", author = subject)

    /** A NIP-51 people list: a title to match on, `p` tags naming members, and nowhere to put a confidence. */
    private fun peopleList(
        title: String,
        members: List<String> = listOf(subject),
        author: String = reader,
        listId: String = "roster",
    ) = event(
        30000,
        buildList {
            add(arrayOf("d", listId))
            add(arrayOf("title", title))
            members.forEach { add(arrayOf("p", it)) }
        }.toTypedArray(),
        author = author,
    )

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
    fun `a search for profiles alone gets the list's members and not the list`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // kinds:[0] — the shape a client hunting people actually sends.
            // The list is not an asked-for kind, but it converts to one, so it
            // is fetched and unpacked: the member arrives because the list
            // vouched for them. The LIST itself does not, because the read said
            // kind 0 and a 30392 is not one — the companion buys recall, not a
            // seat on the answer.
            assertEquals(listOf(profile.id), page(search("podcaster", listOf(0))))

            // Ask for the kind and it is a plain NIP-01 hit again, member and
            // all — the same corpus, the same terms, one more kind.
            assertEquals(listOf(list.id, profile.id), page(search("podcaster", listOf(0, 30392))))
        }

    @Test
    fun `a search for notes alone gets what the label describes and not the label`() =
        runBlocking {
            store.insert(note)
            val label = event(1985, arrayOf(arrayOf("L", "#health"), arrayOf("l", "medical", "#health"), arrayOf("e", note.id)))
            store.insert(label)

            assertEquals(listOf(note.id), page(search("medical", listOf(1), observer = null)))
            assertEquals(listOf(label.id, note.id), page(search("medical", listOf(1, 1985), observer = null)))
        }

    @Test
    fun `an assertion converts to the profile kind once its signer is named`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30382:rank", curator, "wss://provider.example")))
            val card = event(30382, arrayOf(arrayOf("d", subject), arrayOf("petname", "Bramblecast"), arrayOf("rank", "90")))
            store.insert(card)

            // The whole point of the narrowing, in the shape it was reported
            // in: a read for kind 0 gets the profile the card is about, and no
            // 30382 — a kind it has no parser for and reserved no slot for.
            assertEquals(listOf(profile.id), page(search("bramblecast", listOf(0))))
        }

    @Test
    fun `an event list converts to the notes it names`() =
        runBlocking {
            store.insert(note)
            store.insert(treasureMap(arrayOf("30393", curator, "wss://lists.example")))
            val list = event(30393, arrayOf(arrayOf("d", "episodes"), arrayOf("title", "Podcast Episodes"), arrayOf("e", note.id, "", "80")))
            store.insert(list)

            assertEquals(listOf(note.id), page(search("episodes", listOf(1))))
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
            // the matching list unpacks nothing — while the ungated label still
            // converts to the profile it describes. The label is a 1985 and the
            // read asked for kind 0, so what comes back is the profile alone.
            assertEquals(listOf(profile.id), page(search("podcaster", listOf(0), observer = null)))
        }

    // ------------------------------------------------------------------
    // A companion is recall, not an answer
    // ------------------------------------------------------------------

    /**
     * THE RULE THE CONVERSION IS BOUNDED BY: a read answers with the kinds it
     * asked for.
     *
     * The companion queries deliberately step outside the caller's kinds —
     * a pointer of another kind is the only route to the subjects it names —
     * and that is a RECALL device. The answer is still a NIP-01 answer: a
     * client that sent `kinds:[0]` has no parser for a 30382 and budgeted no
     * slot for one, so the assertion does its job (it puts the profile on the
     * page, at the position its own relevance earned) and then steps out.
     *
     * Every family at once, under one read, because the narrowing is not
     * per-family and a regression in any one of them would be invisible in the
     * others.
     */
    @Test
    fun `a kind-restricted search never answers with a kind it excluded`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30382:rank", curator, "wss://p.example"), arrayOf("30392", curator, "wss://l.example")))
            val card = event(30382, arrayOf(arrayOf("d", subject), arrayOf("petname", "Podcaster Ada"), arrayOf("rank", "90")))
            val list = userList("Podcaster Trust List")
            val label = event(1985, arrayOf(arrayOf("L", "#trades"), arrayOf("l", "podcaster", "#trades"), arrayOf("p", subject)))
            listOf(card, list, label).forEach { store.insert(it) }

            // Three pointers, all matched, all unpacked to the same profile —
            // and the answer is that one profile.
            assertEquals(listOf(profile.id), page(search("podcaster", listOf(0))))
        }

    @Test
    fun `a sibling filter that asked for the pointer kind keeps it on the page`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // A REQ ORs its filters and answers with ONE page, so the kinds it
            // asked for are the union: the second filter named 30392, which
            // makes the list a plain NIP-01 hit for the read as a whole. The
            // narrowing is not per filter — that would drop a row one filter
            // asked for because another did not.
            val out = page(search("podcaster", listOf(0)), Filter(kinds = listOf(30392), authors = listOf(curator)))
            assertEquals(setOf(list.id, profile.id), out.toSet())
        }

    @Test
    fun `an unrestricted read narrows nothing`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // No kinds is not "kinds:[]" as a constraint — it admits every kind
            // by definition, so there is nothing the pointer could be excluded
            // from. The mixed page is what this read asked for.
            assertEquals(listOf(list.id, profile.id), page(Filter(search = "podcaster include:spam observer:$reader")))
        }

    @Test
    fun `the raw path narrows to the caller's kinds too`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            store.insert(userList("Podcaster Trust List"))

            // A relay serves REQs off `rawQuery`, so the rule has to hold on
            // the path that actually reaches the wire — the two read paths
            // splice through the same code and would be trivial to narrow in
            // only one of them.
            val out = mutableListOf<String>()
            store.rawQuery(listOf(search("podcaster", listOf(0)))) { out.add(it.id) }
            assertEquals(listOf(profile.id), out)
        }

    @Test
    fun `a pointer asked for outright expands under its own lens, never a converting one`() =
        runBlocking {
            store.insert(note)
            store.insert(treasureMap(arrayOf("30393", curator, "wss://lists.example")))
            val list = event(30393, arrayOf(arrayOf("d", "episodes"), arrayOf("title", "Podcast Episodes"), arrayOf("e", note.id, "", "80")))
            store.insert(list)

            // The first filter could CONVERT a 30393, but its terms never
            // matched this one; the second asked for the kind outright and
            // did. Asked-for beats converted whatever the filter order, so the
            // list's subjects are read through the lens that admits no kind 1
            // — and the note stays out, exactly as it did before conversion
            // existed.
            val out = page(search("unrelatedword", listOf(1)), search("episodes", listOf(30393)))
            assertEquals(listOf(list.id), out)
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

            // The list converts and unpacks either way; whether the member
            // rides along is the engine's admission under the read's own
            // authors filter, exactly as with an explicitly asked-for pointer.
            // The list is never on the answer — kinds:[0] — so the second read,
            // whose author constraint the member fails, comes back empty rather
            // than with the pointer alone.
            val withSubject = Filter(kinds = listOf(0), authors = listOf(curator, subject), search = "podcaster include:spam observer:$reader")
            assertEquals(listOf(profile.id), page(withSubject))
            val withoutSubject = Filter(kinds = listOf(0), authors = listOf(curator), search = "podcaster include:spam observer:$reader")
            assertEquals(emptyList(), page(withoutSubject))
        }

    // ------------------------------------------------------------------
    // Conversion: a read that restricted nothing
    // ------------------------------------------------------------------

    /**
     * The hits a page fills up with: kind 1s that match the terms and are
     * NEWER than the pointer, so a `limit` cuts the pointer off the bottom of
     * a recall no kinds narrow. Newest-first is what this index orders an
     * unscored page by, which makes "ranked out" reproducible without a
     * ranking — the staging shape in miniature, where a Trusted List signed by
     * an unranked service key sat at rank 80 and a page of 40 never saw it.
     */
    private suspend fun crowdOut(
        terms: String,
        n: Int = 2,
    ): List<Event> =
        (1..n).map { i ->
            event(1, emptyArray(), "$terms $i", author = stranger).also { store.insert(it) }
        }

    @Test
    fun `an unrestricted search reaches the list its own page ranked out`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list = userList("Podcaster Trust List")
            store.insert(list)
            val newer = crowdOut("podcaster")

            // No kinds at all — what a search UI's "Everything" sends. Such a
            // read ADMITS the list, which is why it used to be skipped here,
            // but admission is not recall: the page is two hits deep, both are
            // newer, and without the declaration companion the reader's own
            // list and every member it vouches for are simply not on it.
            val out = page(Filter(search = "podcaster include:spam observer:$reader", limit = 2))
            assertEquals(listOf(newer[1].id, newer[0].id, list.id, profile.id), out)
        }

    @Test
    fun `an unrestricted search leaves a label its page ranked out where it fell`() =
        runBlocking {
            store.insert(note)
            val label = event(1985, arrayOf(arrayOf("L", "#trades"), arrayOf("l", "podcaster", "#trades"), arrayOf("e", note.id)))
            store.insert(label)
            val newer = crowdOut("podcaster")

            // The asymmetry is deliberate. A label companion is the caller's
            // own query with the kinds swapped — ungated, no author constraint
            // — so on an unrestricted read it would re-fetch rows the ranking
            // had already placed below the page, second-guessing an order
            // nothing says is wrong. A label has no unranked signer behind it
            // to correct for.
            assertEquals(listOf(newer[1].id, newer[0].id), page(Filter(search = "podcaster include:spam", limit = 2)))

            // Kind-restricted, the same corpus still converts it: there the
            // recall cannot return a 1985 at all, however deep the page — so
            // the note the label describes joins the page, while the label,
            // whose kind the read excluded, does not.
            val restricted = Filter(kinds = listOf(1), search = "podcaster include:spam", limit = 2)
            assertEquals(listOf(newer[1].id, newer[0].id, note.id), page(restricted))
        }

    @Test
    fun `the unrestricted companion never surfaces a stranger's list`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            store.insert(userList("Podcaster Roster", author = stranger, listId = "outsiders"))
            val newer = crowdOut("podcaster")

            // The same gate the kind-restricted companion applies: a companion
            // is this store's own addition to the feed, so it adds only what
            // the reader enrolled. A stranger's list keeps the place the
            // ranking gave it, which here is off the page.
            assertEquals(listOf(newer[1].id, newer[0].id), page(Filter(search = "podcaster include:spam observer:$reader", limit = 2)))
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
    fun `one observer's service key never unpacks for another`() =
        runBlocking {
            store.insert(profile)
            // Only the STRANGER enrolled the curator. The reader enrolled
            // nobody, and the two read together.
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example"), owner = stranger))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // Both filters are points of view on ONE page, and only one of them
            // enrolled the signer — so the list is served as the ordinary hit
            // it is and unpacks nothing. Under the pooled gate the stranger's
            // delegation opened it for the reader too, and `profile` rode a
            // page the reader never asked for it on.
            assertEquals(
                listOf(list.id),
                page(search("podcaster", listOf(0, 30392), observer = reader), search("podcaster", listOf(0, 30392), observer = stranger)),
                "a signer only one observer enrolled must not unpack onto the page they share",
            )
            // ...in either order: `accepts` ignores the terms, so which filter
            // "found" the pointer is not a question the page can answer, and
            // the gate must not depend on the answer.
            assertEquals(
                listOf(list.id),
                page(search("podcaster", listOf(0, 30392), observer = stranger), search("podcaster", listOf(0, 30392), observer = reader)),
                "…whatever order the filters arrived in",
            )

            // The stranger reading ALONE unpacks it: the gate is unpooled, not off.
            assertEquals(
                listOf(list.id, profile.id),
                page(search("podcaster", listOf(0, 30392), observer = stranger)),
                "the enrolling observer's own read still unpacks",
            )
        }

    @Test
    fun `a declaration both observers enrolled unpacks for the read they share`() =
        runBlocking {
            store.insert(profile)
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example"), owner = stranger))
            val list = userList("Podcaster Trust List")
            store.insert(list)

            // Unanimous: every point of view on the read asked for this signer,
            // so the members ride the shared page for both of them.
            assertEquals(
                listOf(list.id, profile.id),
                page(search("podcaster", listOf(0, 30392), observer = reader), search("podcaster", listOf(0, 30392), observer = stranger)),
            )
        }

    @Test
    fun `a reader's own people list brings the people on it, and a stranger's brings nobody`() =
        runBlocking {
            store.insert(profile)
            val mine = peopleList("Podcaster Roster", author = reader)
            val theirs = peopleList("Podcaster Roster", author = stranger, listId = "theirs")
            store.insert(mine)
            store.insert(theirs)

            // A NIP-51 list is not a trust service's output, so nothing
            // delegates it — but a reader is always their own signer, and a
            // list they curated is an answer to their own search.
            assertEquals(
                listOf(theirs.id, mine.id, profile.id),
                page(search("podcaster", listOf(0, 30000))),
                "the reader's own list splices its members; the stranger's is an ordinary hit",
            )

            // ...and the People tab shape: the list itself is a kind the read
            // did not ask for, so only the person comes back.
            assertEquals(listOf(profile.id), page(search("podcaster", listOf(0))))
        }

    @Test
    fun `a follow pack converts to profiles like a people list`() =
        runBlocking {
            store.insert(profile)
            val pack = event(39089, arrayOf(arrayOf("d", "pack"), arrayOf("title", "Podcaster Pack"), arrayOf("p", subject)), author = reader)
            store.insert(pack)

            assertEquals(listOf(pack.id, profile.id), page(search("podcaster", listOf(0, 39089))))
        }

    @Test
    fun `the per-author cap thins a ranked page and never a recency one`() =
        runBlocking {
            val capped = NostrSemanticsStore(TrustProjection(InMemoryEventIndex(), InMemoryReputationIndex()), relay = relayUrl, maxHitsPerAuthor = 2)
            // One author, five matching notes — the mirror-bot shape.
            val bulk = (1..5).map { event(1, arrayOf(arrayOf("subject", "podcaster roundup $it")), author = stranger) }
            bulk.forEach { capped.insert(it) }
            val other = event(1, arrayOf(arrayOf("subject", "podcaster weekly")), author = curator)
            capped.insert(other)

            val ranked = capped.query<Event>(listOf(search("podcaster", listOf(1)))).map { it.id }
            assertEquals(3, ranked.size, "two from the bulk author, plus the other author's one: $ranked")
            assertEquals(2, ranked.count { id -> bulk.any { it.id == id } }, "the cap is per author, not per page")
            assertTrue(other.id in ranked, "and it never costs an author their only row")

            // A RECENCY read is a mirror paging a corpus: dropping rows there
            // is data loss, not an editorial choice.
            assertEquals(6, capped.query<Event>(listOf(Filter(kinds = listOf(1)))).size, "plain recall keeps everything")
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
            // Delegated for a member query AND for a TERMLESS one. The terms are
            // what separates a page query from a subject lookup (forLookup strips
            // them), and a real Vespa answers the termless shape through
            // isRecencyOrdered() -> Ranked(hit, null). Fabricating a band for it
            // here made a reference with no confidence look SCORED, which is the
            // one thing the unscored-subject placement turns on.
            if (query.ranking != null || (query.search == null && query.phrases.isEmpty())) {
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

                            // INSIDE the member band, above where a LOW-scoring
                            // pointer sits — the gap a member has to cross to
                            // reach the rung its own confidence earned it.
                            doc.content.contains("faint") -> 2_000.0

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
            // WHAT THE SPAN CHANGED. Both members are now floored at a share of
            // the list that found them — c=1.00 ties it (130 000), c=0.10 lands
            // a rung below (130 000 x (0.1769 + 0.8231 x 0.1) = 33 700) — so
            // BOTH clear the bio-strength hit inside the member band, and
            // confidence orders them between those two points rather than
            // inside a 550..4 000 rung that no organic hit competes in.
            assertTrue(out.indexOf(weak.id) > out.indexOf(sureId), "a full-confidence member outranks a bio-strength hit: $out")
            assertTrue(out.indexOf(weak.id) > out.indexOf(doubtedId), "and so does a doubted one, within a rung of its list: $out")

            // The same corpus with no floor at all — the placement that came
            // before it, kept under test rather than deleted, because the
            // difference between the two IS the feature: on its own rung
            // (550 + 3450 x 0.1 = 895) the doubted member falls under the hit.
            val unfloored =
                NostrSemanticsStore(
                    TrustProjection(ranking, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(subjectFloorSpan = null),
                )
            val plain = unfloored.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
            assertTrue(plain.indexOf(weak.id) < plain.indexOf(doubtedId), "with no floor the doubted member is back on its rung: $plain")
            assertTrue(plain.indexOf(sureId) < plain.indexOf(weak.id), "and the confident one still clears the hit on the rung alone: $plain")
        }

    /**
     * An index that COUNTS the subject lookups — the round trips the weights
     * exist to remove. A subject lookup is the shape nothing else has: keyed,
     * termless, and ranked on the member profile.
     */
    private class CountingIndex(
        private val inner: com.nosfabrica.vespa.eventstore.engine.EventIndex,
    ) : com.nosfabrica.vespa.eventstore.engine.EventIndex by inner {
        var subjectLookups = 0
            private set

        /**
         * EVERY round trip the expansion spends, the unranked ones included —
         * a label's subject is fetched under no member profile at all, so
         * [subjectLookups] cannot see it, and the cost of a page of labels is
         * exactly what regressed once.
         */
        var keyedLookups = 0
            private set

        override suspend fun searchRanked(query: com.nosfabrica.vespa.eventstore.engine.query.EventQuery): List<com.nosfabrica.vespa.eventstore.engine.Ranked<com.nosfabrica.vespa.eventstore.engine.doc.EventDoc>> {
            if (query.ranking == com.nosfabrica.vespa.eventstore.engine.query.EventYql.RANK_SPLICED_MEMBER) subjectLookups++
            // Keyed and TERMLESS is the shape only a subject lookup has: the
            // page's own queries carry the words, `forLookup` strips them.
            val keyed = query.ids.isNotEmpty() || query.idWeights.isNotEmpty() || query.authorWeights.isNotEmpty() || query.authors.isNotEmpty()
            if (keyed && query.search == null && query.phrases.isEmpty()) keyedLookups++
            return inner.searchRanked(query)
        }
    }

    @Test
    fun `a page of labels costs one lookup, however long the page`() =
        runBlocking {
            // THE COST OF THE FLOOR, BOUNDED. A pointer's relevance is a
            // query-level number, so a floored lookup cannot pool two pointers
            // that ranked differently — and an early cut of this fed EVERY row
            // its own round trip, which turned a page of fifty labels from one
            // lookup into fifty. A label carries no confidence and takes no
            // floor, so nothing about it varies per row and the whole page
            // shares one query. Twenty here; the number must not move with it.
            val counting = CountingIndex(RankingIndex(InMemoryEventIndex()))
            val store = NostrSemanticsStore(TrustProjection(counting, InMemoryReputationIndex()), relay = relayUrl)
            repeat(20) { i ->
                val subject = event(1, emptyArray(), "episode $i", author = key("d4"))
                store.insert(subject)
                store.insert(event(1985, arrayOf(arrayOf("L", "#topic"), arrayOf("l", "podcaster", "#topic"), arrayOf("e", subject.id))))
            }

            val out = store.query<Event>(listOf(search("podcaster", listOf(1, 1985)))).map { it.id }
            assertEquals(40, out.size, "twenty labels and the twenty notes they describe: ${out.size}")
            assertEquals(1, counting.keyedLookups, "one round trip for the whole page, not one per label")
        }

    @Test
    fun `one lookup carries a whole list, whatever its confidences`() =
        runBlocking {
            // WHY THE WEIGHTS EXIST, measured rather than argued. Confidence was
            // a query-level rank feature, so members had to be GROUPED by it and
            // each group cost a round trip — which is also why it was quantized
            // to quarters. The staging `Verified Human` list's seventeen members
            // occupy four buckets; this is that list in miniature, five members
            // across five distinct confidences that used to be four lookups.
            val counting = CountingIndex(InMemoryEventIndex())
            val weighted = NostrSemanticsStore(TrustProjection(counting, InMemoryReputationIndex()), relay = relayUrl)
            val members = listOf("11", "22", "33", "44", "55").map(::key)
            members.forEach { weighted.insert(event(0, emptyArray(), """{"name":"m"}""", author = it)) }
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            weighted.insert(
                event(
                    30392,
                    buildList {
                        add(arrayOf("d", "roster"))
                        add(arrayOf("title", "Podcaster Trust List"))
                        listOf("100", "87", "50", "12", "3").forEachIndexed { i, score -> add(arrayOf("p", members[i], "", score)) }
                    }.toTypedArray(),
                ),
            )

            val out = weighted.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id }
            assertEquals(6, out.size, "the list and all five members: $out")
            assertEquals(1, counting.subjectLookups, "five confidences, ONE keyed lookup")
        }

    /**
     * THE ROUND-TRIP BUDGET CUTS THE BOTTOM OF THE PAGE, NEVER THE TOP.
     *
     * A pointer's own relevance is a query-level rank feature, so two lists that
     * ranked differently cannot share a lookup — which means a wide page of
     * Trusted Lists costs one round trip each, and past
     * [SearchExpansionLimits.maxLookups] some of them get none. That bound is
     * fine; WHICH pointers it drops is not a detail. Lookups are planned in page
     * order — relevance order on any page that can be sorted — so the budget is
     * spent on the best-ranked pointers and the worst-ranked lose their members.
     *
     * Pinned with a budget of one and two lists, because that is the whole
     * property: before, the cut fell wherever the loop nesting happened to
     * reach it, and nothing said so.
     */
    @Test
    fun `the lookup budget spends itself on the best-ranked pointers`() =
        runBlocking {
            val ranking = RankingIndex(InMemoryEventIndex())
            val store =
                NostrSemanticsStore(
                    TrustProjection(ranking, InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(maxLookups = 1),
                )
            val first = key("e5")
            val second = key("f6")
            store.insert(event(0, emptyArray(), """{"name":"First member"}""", author = first))
            store.insert(event(0, emptyArray(), """{"name":"Second member"}""", author = second))
            store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            // Two lists, same text, so only their page POSITION separates them.
            // The reference orders equal matches newest-first and RankingIndex
            // scores by that position, so the list inserted LAST is the one the
            // page ranks first — and the one the single lookup must be spent on.
            val bottom = event(30392, arrayOf(arrayOf("d", "bottom"), arrayOf("title", "Podcaster Trust List"), arrayOf("p", second, "", "90")))
            val top = event(30392, arrayOf(arrayOf("d", "top"), arrayOf("title", "Podcaster Trust List"), arrayOf("p", first, "", "90")))
            store.insert(bottom)
            store.insert(top)

            val out = store.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id }
            val firstCard = store.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(first)))).single().id
            val secondCard = store.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(second)))).single().id

            assertTrue(out.indexOf(top.id) < out.indexOf(bottom.id), "the better-ranked list leads: $out")
            assertTrue(firstCard in out, "its member is the one the single lookup bought: $out")
            assertTrue(secondCard !in out, "and the budget stopped before the lower-ranked list's: $out")
        }

    @Test
    fun `a member of a matched list rides within one rung of it`() =
        runBlocking {
            // THE CASE THIS FEATURE EXISTS FOR. On staging the member a list is
            // 87% sure of — ranked 100 by that very reader — sat 30 rows under
            // the list, because the member rung's ceiling (4 000 x wot) cannot
            // reach a title match (130 000 x wot) from below. Here the `mid` hit
            // at 20 000 stands in for everything in that gap: unreachable from
            // the rung, cleared by the floor.
            val ranking = RankingIndex(InMemoryEventIndex())
            val weighted = NostrSemanticsStore(TrustProjection(ranking, InMemoryReputationIndex()), relay = relayUrl)
            val member = key("e5")
            weighted.insert(event(0, emptyArray(), """{"name":"Vouched"}""", author = member))
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val mid = event(1, emptyArray(), "podcaster note, mid match", author = stranger)
            weighted.insert(mid)
            val list =
                event(
                    30392,
                    arrayOf(arrayOf("d", "roster"), arrayOf("title", "Podcaster Trust List"), arrayOf("p", member, "", "87")),
                )
            weighted.insert(list)

            val out = weighted.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
            val card = weighted.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(member)))).single().id
            assertEquals(list.id, out.first(), "the list still leads on its own text: $out")
            assertEquals(card, out[1], "and the member it vouches for rides with it: $out")
            assertTrue(out.indexOf(card) < out.indexOf(mid.id), "over the gap its own rung could never cross: $out")
        }

    @Test
    fun `a scored list carrying an unscored member splits the lookup, not the meaning`() =
        runBlocking {
            // A publisher may score some members and not others. The unscored
            // one is not DOUBTED — "as sure as the pointer itself" — so it must
            // not ride in with weight 0, which would read as a publisher saying
            // nothing good about it. It takes a second, unweighted lookup and
            // the pointer's own placement, exactly as a label's subject does.
            val counting = CountingIndex(RankingIndex(InMemoryEventIndex()))
            val weighted = NostrSemanticsStore(TrustProjection(counting, InMemoryReputationIndex()), relay = relayUrl)
            val scored = key("e5")
            val silent = key("f6")
            weighted.insert(event(0, emptyArray(), """{"name":"Scored"}""", author = scored))
            weighted.insert(event(0, emptyArray(), """{"name":"Silent"}""", author = silent))
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val list =
                event(
                    30392,
                    arrayOf(
                        arrayOf("d", "roster"),
                        arrayOf("title", "Podcaster Trust List"),
                        arrayOf("p", scored, "", "40"),
                        arrayOf("p", silent),
                    ),
                )
            weighted.insert(list)

            val out = weighted.query<Event>(listOf(search("podcaster", listOf(0, 30392)))).map { it.id }
            val cards = weighted.query<Event>(listOf(Filter(kinds = listOf(0)))).associateBy { it.pubKey }
            assertEquals(3, out.size, "the list and both members: $out")
            assertEquals(1, counting.subjectLookups, "the scored half is weighted; the unscored half is not ranked at all")
            // And the meaning survives the split: the SILENT member takes the
            // pointer's own placement and sits directly behind it, while the one
            // scored 40 is floored partway down the span, below it.
            assertEquals(cards.getValue(silent).id, out[1], "the unscored member rides its pointer: $out")
            assertEquals(cards.getValue(scored).id, out[2], "and the doubted one is placed by what its list said: $out")
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
    fun `gamma reaches the engine, where both the rung and the floor apply it`() =
        runBlocking {
            // The exponent is a rank feature — the store's part is handing it
            // over — and it now shapes BOTH halves of the placement: the rung's
            // span, and where inside the floor's span a member lands. What this
            // pins is that it still arrives and still sinks a doubted member.
            //
            // Read with the floor's SPAN set to zero, which isolates the
            // exponent: the floor is then pointer x c^gamma, so c=0.5 lands at
            // 65 000 with gamma 1 and at 8 125 with gamma 4, either side of the
            // `mid` hit at 20 000. With the span at its default the same member
            // never falls that far — see the test below, which is the point of
            // having a span at all.
            fun storeAt(gamma: Double) =
                NostrSemanticsStore(
                    TrustProjection(RankingIndex(InMemoryEventIndex()), InMemoryReputationIndex()),
                    relay = relayUrl,
                    searchExpansion = SearchExpansionLimits(confidenceGamma = gamma, subjectFloorSpan = 0.0),
                )

            suspend fun place(store: NostrSemanticsStore): Pair<List<String>, Pair<String, String>> {
                val doubted = key("f6")
                store.insert(event(0, emptyArray(), """{"name":"Doubted"}""", author = doubted))
                store.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
                val mid = event(1, emptyArray(), "podcaster note, mid match", author = stranger)
                store.insert(mid)
                store.insert(
                    event(30392, arrayOf(arrayOf("d", "roster"), arrayOf("title", "Podcaster Trust List"), arrayOf("p", doubted, "", "50"))),
                )
                val out = store.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
                val card = store.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(doubted)))).single().id
                return out to (card to mid.id)
            }

            val (soft, softIds) = place(storeAt(1.0))
            assertTrue(soft.indexOf(softIds.first) < soft.indexOf(softIds.second), "at gamma 1 the member clears the mid hit: $soft")

            val (hard, hardIds) = place(storeAt(4.0))
            assertTrue(hard.indexOf(hardIds.first) > hard.indexOf(hardIds.second), "at gamma 4 it sinks below it: $hard")
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
            // And the survivors are placed the way an untruncated list's are:
            // the one it is SURE of ties its list at full confidence and comes
            // straight after it, ahead of the token-band fillers; the one it
            // doubts is floored a rung lower (c=0.10 -> 0.2592 x pointer) and
            // falls below them. Truncation decides WHICH members are here,
            // never how the ones that are get placed.
            assertEquals(list.id, out.first(), "the list leads: $out")
            assertEquals(cards.getValue(sure).id, out[1], "its full-confidence member directly behind it: $out")
            assertTrue(
                filler.all { out.indexOf(it.id) < out.indexOf(cards.getValue(doubted).id) },
                "a token match outranks the member its list doubts: $out",
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
    fun `a label reaches an addressable subject, on the unscored bucket`() =
        runBlocking {
            // The coordinate shape is the one family that still groups its
            // subjects by quantized confidence, and a NIP-32 label is how the
            // UNSCORED bucket of that grouping is reached — it expresses no
            // confidence, so it takes the `null` key rather than a number. The
            // scored half has the 30394 test below it; without this one, half
            // of `bucketed` is exercised by nothing.
            val article = event(30023, arrayOf(arrayOf("d", "kept"), arrayOf("title", "Kept Article")), author = subject)
            store.insert(article)
            val label =
                event(
                    1985,
                    arrayOf(arrayOf("L", "#trades"), arrayOf("l", "podcaster", "#trades"), arrayOf("a", "30023:$subject:kept")),
                )
            store.insert(label)

            // Ungated, so no observer is needed — and kind-restricted, so the
            // label itself stays off the answer while the article it describes
            // arrives.
            assertEquals(listOf(article.id), page(search("podcaster", listOf(30023), observer = null)))
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
    fun `a low-scoring pointer rises to its best member instead of holding it down`() =
        runBlocking {
            // THE STAGING BUG, in miniature. A trust service is a key nobody
            // follows, so its lists score near the bottom of the ladder — and
            // while a member was CLAMPED to its pointer (`minOf(own, pointer)`)
            // that one number became the ceiling for everybody the list named.
            // Measured on `search-staging` for the query `Verified Human`: a
            // list signed by a service scored 26 pinned sixteen members scored
            // 65..100 to 550 x wot(26), which put them below organic hits from
            // authors trusted 7 and, because they all tied, back in the
            // publisher's tag order.
            //
            // Here: the list scores 1 000 (the pointer), an unrelated hit
            // scores 2 000, and the member's own confidence earns it 4 000. The
            // member must clear the hit, and the list must come with it —
            // never below it.
            val ranking = RankingIndex(InMemoryEventIndex())
            val weighted = NostrSemanticsStore(TrustProjection(ranking, InMemoryReputationIndex()), relay = relayUrl)
            val member = key("f7")
            weighted.insert(event(0, emptyArray(), """{"name":"Vouched"}""", author = member))
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val faint = event(1, emptyArray(), "podcaster note, faint match", author = stranger)
            weighted.insert(faint)
            val list =
                event(
                    30392,
                    arrayOf(arrayOf("d", "roster"), arrayOf("title", "Podcaster Trust List"), arrayOf("p", member, "", "100")),
                    // Scores the pointer on the weak rung, under the hit above.
                    content = "weak echo",
                )
            weighted.insert(list)

            val out = weighted.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
            val card = weighted.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(member)))).single().id
            assertEquals(listOf(list.id, card), out.take(2), "the list, then the member it is sure about, both above the faint hit: $out")
            assertTrue(out.indexOf(card) < out.indexOf(faint.id), "a clamped member would have sunk to the pointer, under the hit: $out")
        }

    @Test
    fun `a member the list expressed no confidence about rides with the lifted pointer`() =
        runBlocking {
            // A LIST THAT SCORES SOME MEMBERS AND NOT OTHERS. Every member on the
            // eleven staging lists carries a score, but nothing in the family
            // REQUIRES one — a bare `p` tag is a well-formed member — and such a
            // member is looked up without the member profile, so it comes back
            // unscored like a label's subject does.
            //
            // It must ride with its pointer, and the pointer is the LIFTED one.
            // Taking the raw pointer score instead would strand it where the
            // block used to be while its scored siblings moved up without it —
            // here below the faint hit, and on the staging numbers about 340x
            // under the members it was named beside.
            val ranking = RankingIndex(InMemoryEventIndex())
            val weighted = NostrSemanticsStore(TrustProjection(ranking, InMemoryReputationIndex()), relay = relayUrl)
            val scored = key("f8")
            val unscored = key("f9")
            weighted.insert(event(0, emptyArray(), """{"name":"Scored"}""", author = scored))
            weighted.insert(event(0, emptyArray(), """{"name":"Unscored"}""", author = unscored))
            weighted.insert(treasureMap(arrayOf("30392", curator, "wss://lists.example")))
            val faint = event(1, emptyArray(), "podcaster note, faint match", author = stranger)
            weighted.insert(faint)
            val list =
                event(
                    30392,
                    arrayOf(
                        arrayOf("d", "roster"),
                        arrayOf("title", "Podcaster Trust List"),
                        arrayOf("p", scored, "", "100"),
                        // A member with no score at all — the shape this pins.
                        arrayOf("p", unscored),
                    ),
                    content = "weak echo",
                )
            weighted.insert(list)

            val out = weighted.query<Event>(listOf(search("podcaster", listOf(0, 1, 30392)))).map { it.id }
            val scoredCard = weighted.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(scored)))).single().id
            val unscoredCard = weighted.query<Event>(listOf(Filter(kinds = listOf(0), authors = listOf(unscored)))).single().id
            assertEquals(listOf(list.id, scoredCard, unscoredCard), out.take(3), "the list, then both members, in the order it named them: $out")
            assertTrue(out.indexOf(unscoredCard) < out.indexOf(faint.id), "the unscored member rode the lift, it did not stay at the raw pointer: $out")
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
