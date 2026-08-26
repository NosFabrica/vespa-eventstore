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
package com.nosfabrica.vespa.eventstore

import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.SearchExtractors
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.experimental.trustedLists.TrustedListEvent
import com.vitorpamplona.quartz.experimental.trustedLists.addressables.AddressableTrustedListEvent
import com.vitorpamplona.quartz.experimental.trustedLists.events.EventTrustedListEvent
import com.vitorpamplona.quartz.experimental.trustedLists.externalIds.ExternalIdTrustedListEvent
import com.vitorpamplona.quartz.experimental.trustedLists.users.UserTrustedListEvent
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.EventFactory
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The Tapestry Trusted List family (Quartz `experimental/trustedLists`, kinds
 * 30392-30395): an addressable event that publishes a curated set of MEMBERS
 * computed under one point of view — the bulk form of a NIP-85 assertion,
 * bound to it by the `+10` rule, so the kind's last digit names the member tag
 * (30392/`p`, 30393/`e`, 30394/`a`, 30395/`i`).
 *
 * Nothing in this store is kind-aware about them, and that is the point: every
 * rule they need already keys off the kind RANGE (addressable supersession) or
 * off NIP-01's own tag space (`tag_index`). This suite pins that the generic
 * paths really do carry the family, because the family's whole design assumes a
 * relay can serve `#p`/`#e`/`#a`/`#i` recall over a membership that runs to
 * thousands of entries — if `tag_index` ever grew a cap, a kind allow-list, or a
 * per-document ceiling, these lists would be the first thing to break, silently.
 *
 * Runs against [InMemoryEventIndex], the executable specification of EventQuery
 * matching. Nothing here is engine-specific — the tag recall it exercises is the
 * same `tag_index` path `VespaParityIT` already pins against a real Vespa — so it
 * needs no integration gate of its own.
 */
class TrustedListIndexingTest {
    private val publisher = "9c".repeat(32)
    private val observer = "0b".repeat(32)
    private val service = "5e".repeat(32)

    private val index = InMemoryEventIndex()
    private val store = NostrSemanticsStore(index, relay = "wss://sot.test/".normalizeRelayUrl())

    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun key(prefix: String) = prefix.repeat(32)

    /**
     * A list as it arrives on the wire: raw tag arrays, in the order the family's
     * builder emits them (metadata first, membership last, so a reader hits the
     * header before a run of thousands).
     */
    private fun list(
        kind: Int,
        listId: String,
        memberTag: String,
        members: List<String>,
        at: Long = next(),
        eventId: String = id(),
        author: String = publisher,
        title: String? = null,
        extra: List<Array<String>> = emptyList(),
    ): Event {
        val tags =
            buildList {
                add(arrayOf("d", listId))
                if (title != null) add(arrayOf("title", title))
                add(arrayOf("observer", observer))
                add(arrayOf("metric", "pinned-tag-membership"))
                addAll(extra)
                // Members carry `[<tag>, <value>, <hint>, <score>]`: the score
                // sits at index 3 family-wide, so a publisher with a score and no
                // relay hint pads index 2 with "". Only index 1 is indexable.
                members.forEach { m -> add(arrayOf(memberTag, m, "", "80")) }
            }.toTypedArray()
        // Through EventFactory, exactly like the ingress that feeds a relay:
        // SearchFieldExtractor dispatches on the event CLASS, so a fixture built
        // as a bare Event would be unsearchable for the wrong reason.
        return EventFactory.create<Event>(eventId, author, at, kind, tags, "", "")
    }

    private fun userList(
        listId: String = "trusted-nostr-devs",
        members: List<String>,
        at: Long = next(),
        eventId: String = id(),
        author: String = publisher,
        title: String? = null,
        extra: List<Array<String>> = emptyList(),
    ) = list(UserTrustedListEvent.KIND, listId, "p", members, at, eventId, author, title, extra)

    private fun recall(
        kind: Int,
        tag: String,
        value: String,
    ) = runBlocking { store.query<Event>(Filter(kinds = listOf(kind), tags = mapOf(tag to listOf(value)))) }

    // ---- membership recall -------------------------------------------------

    /**
     * The family's reason to exist: a reader asks the relay for the lists a
     * given member appears in. One case per kind, because the member tag is the
     * ONE thing that differs across the four — and it is the one thing a lossy
     * `tag_index` would drop.
     */
    @Test
    fun `every member is recalled by the tag its kind's last digit names`() =
        runBlocking {
            val pubkeys = listOf(key("a1"), key("a2"), key("a3"))
            val eventIds = listOf(key("e1"), key("e2"))
            val addresses = listOf("30023:${key("c3")}:essay-one", "30023:${key("c3")}:essay-two")
            val externalIds = listOf("podcast:guid:c90e609a-df1e-596a-bd5e-57bcc8aad6cc", "isbn:9780765382030")

            store.insert(list(UserTrustedListEvent.KIND, "devs", "p", pubkeys))
            store.insert(list(EventTrustedListEvent.KIND, "notes", "e", eventIds))
            store.insert(list(AddressableTrustedListEvent.KIND, "articles", "a", addresses))
            store.insert(list(ExternalIdTrustedListEvent.KIND, "works", "i", externalIds))

            // EVERY member, not just the first: `tag_index` keeps one entry per
            // tag occurrence, so a "first value per name" projection would pass
            // on pubkeys[0] and lose the rest.
            pubkeys.forEach { assertEquals(1, recall(UserTrustedListEvent.KIND, "p", it).size, "member $it") }
            eventIds.forEach { assertEquals(1, recall(EventTrustedListEvent.KIND, "e", it).size, "member $it") }
            addresses.forEach { assertEquals(1, recall(AddressableTrustedListEvent.KIND, "a", it).size, "member $it") }
            externalIds.forEach { assertEquals(1, recall(ExternalIdTrustedListEvent.KIND, "i", it).size, "member $it") }

            // A pubkey that is in no list is in no list.
            assertTrue(recall(UserTrustedListEvent.KIND, "p", key("ff")).isEmpty())
        }

    /**
     * The membership is indexed WHOLE. The family's own KDoc says these lists
     * "run to thousands of entries", so the interesting assertion is the count:
     * one `tag_index` entry per member plus the `d`, with nothing truncated at
     * any boundary a smaller fixture would sit under.
     */
    @Test
    fun `a membership of thousands is indexed entry for entry`() =
        runBlocking {
            val members = (0 until 2_000).map { it.toString(16).padStart(64, '0') }
            val event = userList(members = members)
            store.insert(event)

            val doc = assertNotNull(index.get(event.id))
            val tagIndex = doc.tagIndex()
            // d + observer/metric are multi-letter (below), so: one `d` + 2000 `p`.
            assertEquals(2_001, tagIndex.size)
            assertEquals(2_000, tagIndex.count { it.startsWith("p:") })
            // Spot the ends and the middle — a cap would take the tail first.
            listOf(members.first(), members[1_000], members.last()).forEach {
                assertEquals(1, recall(UserTrustedListEvent.KIND, "p", it).size, "member $it")
            }

            // And the stored tags are still the exact wire array, hints and
            // scores included: `tag_index` is a derived VIEW, never the record.
            val stored = assertNotNull(store.query<Event>(Filter(ids = listOf(event.id))).firstOrNull())
            assertEquals(event.tags.map { it.toList() }, stored.tags.map { it.toList() })
            assertEquals(listOf("p", members.first(), "", "80"), stored.tags[3].toList())
        }

    /**
     * NIP-01 addresses only single ASCII letters, so the list's own metadata —
     * `observer`, `metric`, `min-rank`, `status`, `truncated`, `source-tag` —
     * is deliberately NOT filterable. It must still survive losslessly in the
     * stored tag array, which is where the typed accessors read it from.
     */
    @Test
    fun `list metadata rides in the tags but never in the tag index`() =
        runBlocking {
            val event =
                userList(
                    members = listOf(key("a1")),
                    extra =
                        listOf(
                            arrayOf("min-rank", "20"),
                            arrayOf("truncated", "94210"),
                            arrayOf("source-tag", key("d4"), publisher, "nostr-devs"),
                        ),
                )
            store.insert(event)

            val doc = assertNotNull(index.get(event.id))
            assertEquals(listOf("d:trusted-nostr-devs", "p:${key("a1")}"), doc.tagIndex())

            val typed = store.query<Event>(Filter(ids = listOf(event.id))).first() as TrustedListEvent
            assertEquals(observer, typed.observer())
            assertEquals("pinned-tag-membership", typed.metric())
            assertEquals(20, typed.minRank())
            assertTrue(typed.isTruncated())
            assertEquals(94_210, typed.truncatedTotal())
            assertEquals("nostr-devs", typed.sourceTag()?.slug)
        }

    /**
     * On 30392 the members are the `p` tags, so `a` is what the family leaves
     * free for "what this list is about"; on 30394 it is the other way round.
     * Both roles ride the same `tag_index`, so a reader can ask either question
     * — and neither answer may leak into the other.
     */
    @Test
    fun `discovery tags stay filterable beside the membership`() =
        runBlocking {
            val about = "34550:${key("c0")}:nostr-devs"
            val aboutPubkey = key("b7")
            val member = key("a1")
            val memberAddress = "30023:${key("c3")}:essay-one"

            store.insert(list(UserTrustedListEvent.KIND, "devs", "p", listOf(member), extra = listOf(arrayOf("a", about))))
            store.insert(
                list(AddressableTrustedListEvent.KIND, "essays", "a", listOf(memberAddress), extra = listOf(arrayOf("p", aboutPubkey))),
            )

            // The 30392 answers on both of its roles, and only on its own values.
            assertEquals(1, recall(UserTrustedListEvent.KIND, "a", about).size)
            assertEquals(1, recall(UserTrustedListEvent.KIND, "p", member).size)
            assertTrue(recall(UserTrustedListEvent.KIND, "p", aboutPubkey).isEmpty())

            assertEquals(1, recall(AddressableTrustedListEvent.KIND, "p", aboutPubkey).size)
            assertEquals(1, recall(AddressableTrustedListEvent.KIND, "a", memberAddress).size)
            assertTrue(recall(AddressableTrustedListEvent.KIND, "a", about).isEmpty())
        }

    // ---- addressable lifecycle --------------------------------------------

    /**
     * The `d` tag identifies the LIST, not a subject, so a recompute replaces
     * in place (NIP-01 addressable supersession, which this store keys off the
     * kind range — 30392 needs no entry anywhere to get it). Dropped members
     * must stop being recalled the moment the new revision lands: a stale `#p`
     * hit is the failure mode that matters here.
     */
    @Test
    fun `a recompute replaces the list in place and drops the members it lost`() =
        runBlocking {
            val kept = key("a1")
            val dropped = key("a2")
            val added = key("a3")

            store.insert(userList(listId = "devs", members = listOf(kept, dropped)))
            store.insert(userList(listId = "devs", members = listOf(kept, added)))
            // A DIFFERENT list by the same publisher is a different address.
            store.insert(userList(listId = "designers", members = listOf(dropped)))

            assertEquals(2, index.count(EventQuery(kinds = listOf(UserTrustedListEvent.KIND))))
            assertEquals(1, recall(UserTrustedListEvent.KIND, "p", kept).size)
            assertEquals(1, recall(UserTrustedListEvent.KIND, "p", added).size)
            // `dropped` survives only through the OTHER list, not the superseded revision.
            val stillTagged = recall(UserTrustedListEvent.KIND, "p", dropped)
            assertEquals(1, stillTagged.size)
            assertEquals("designers", (stillTagged.first() as TrustedListEvent).listId())
        }

    /**
     * The family retracts rather than deletes: an empty-membership replacement
     * carrying `["status", "retracted"]`. To this store that is just a newer
     * revision, which is exactly what makes it work — the members go away
     * because supersession dropped the revision that held them, with no
     * retraction-aware code on the write path.
     */
    @Test
    fun `a retraction supersedes the membership it replaces`() =
        runBlocking {
            val member = key("a1")
            store.insert(userList(listId = "devs", members = listOf(member)))
            store.insert(userList(listId = "devs", members = emptyList(), extra = listOf(arrayOf("status", "retracted"))))

            assertTrue(recall(UserTrustedListEvent.KIND, "p", member).isEmpty())
            val live = store.query<Event>(Filter(kinds = listOf(UserTrustedListEvent.KIND)))
            assertEquals(1, live.size)
            val typed = live.first() as TrustedListEvent
            assertTrue(typed.isRetracted())
            assertEquals(0, typed.memberCount())
            // Still served as a record — a retraction is a statement, not a tombstone.
            assertEquals(1, recall(UserTrustedListEvent.KIND, "d", "devs").size)
        }

    // ---- search -----------------------------------------------------------

    /**
     * The title, and nothing but the title. `TrustedListEvent` implements
     * `SearchableEvent` as `title() ?: ""`, and [SearchExtractors] mirrors that
     * set rather than deciding it, so the four kinds became searchable on the
     * pin bump alone — no store code.
     *
     * The negatives are the interesting half, and they are upstream's own:
     * `metric` names a computation, `d` identifies the list, the member tags are
     * hex ids and `content` is a JSON echo of the same membership. None of it is
     * prose, and a `#p`/`#e`/`#a`/`#i` filter reaches the membership far better
     * than a full-text index could — so a search for a common word must not
     * return every list that ran the same job.
     */
    @Test
    fun `a trusted list is searchable by its title and by nothing else`() =
        runBlocking {
            val member = key("a1")
            val event = userList(listId = "tl-pin-podcaster", members = listOf(member), title = "Podcaster")
            store.insert(event)

            assertEquals(listOf(event.id), store.query<Event>(Filter(search = "podcaster")).map { it.id })
            // Everything else the list carries stays out of the index.
            assertTrue(store.query<Event>(Filter(search = "pinned-tag-membership")).isEmpty(), "metric")
            assertTrue(store.query<Event>(Filter(search = "tl-pin-podcaster")).isEmpty(), "list id")
            assertTrue(store.query<Event>(Filter(search = member)).isEmpty(), "membership")
        }

    /**
     * WHICH tier the title lands in is not ours to choose — [SearchExtractors]
     * applies this schema's weights to the decomposition Quartz hands it, and
     * upstream gave the family no explicit `SearchFieldExtractor` branch. So it
     * takes the generic `is SearchableEvent ->` fallback, which puts the whole
     * `indexableContent()` in the TERTIARY tier: the body column, not
     * `search_primary`.
     *
     * Pinned as observed, not endorsed. A body in this schema is reached by
     * trigram substring rather than the prefix/typo attributes every other
     * titled kind gets, and it carries the body's lower weight — so a list title
     * ranks like note content and forgives no typos. The fix, if wanted, is one
     * branch upstream (`is TrustedListEvent -> tiers(event, event.title(), null,
     * null)`), never a local divergence: which accessor lands in which tier is
     * Quartz's call, and diverging would break the set-diffing this store does at
     * every pin bump.
     */
    @Test
    fun `the title lands in the body tier, not the title tier`() =
        runBlocking {
            val titled = userList(members = listOf(key("a1")), title = "Podcaster")
            store.insert(titled)

            val fields = SearchExtractors.extract(titled)
            assertEquals("Podcaster", fields.text)
            assertNull(fields.primary)
            assertNull(fields.secondary)
            assertEquals(fields, assertNotNull(index.get(titled.id)).search)

            // Substring reaches it (the body route); an anchored-prefix column would not be consulted.
            assertEquals(listOf(titled.id), store.query<Event>(Filter(search = "podcast")).map { it.id })
        }

    /**
     * `title() ?: ""` never throws — it runs inside the store's insert
     * transaction — and an all-empty extraction collapses to
     * [IndexableFields.None], so a titleless list is stored and recallable while
     * carrying no search text at all. Most machine-published lists have no title.
     */
    @Test
    fun `a titleless list is stored with no search text`() =
        runBlocking {
            val untitled = userList(members = listOf(key("a1")))
            store.insert(untitled)

            assertEquals(SearchFields.NONE, SearchExtractors.extract(untitled))
            assertNull(assertNotNull(index.get(untitled.id)).search.text)
            assertEquals(1, recall(UserTrustedListEvent.KIND, "p", key("a1")).size)
        }

    // ---- what a Trusted List must NOT do -----------------------------------

    /**
     * A 30392 is NOT a 30382. The names and the `+10` kinds invite the mistake,
     * but the trust projection is fed by NIP-85 contact cards attributed through
     * a 10040; a list carries per-member scores that no 10040 entry type names
     * ([ProviderTypes] stops at 30385). It must leave the tensors alone — a list
     * that quietly ranked its own members would let any publisher score anyone
     * for every observer that named it for `30382:rank`.
     */
    @Test
    fun `a trusted list never feeds the trust projection`() =
        runBlocking {
            val reputations = InMemoryReputationIndex()
            val projection = TrustProjection(InMemoryEventIndex(), reputations)
            val trustStore = NostrSemanticsStore(projection, relay = "wss://sot.test/".normalizeRelayUrl())
            val subject = key("a1")

            trustStore.insert(
                TrustProviderListEvent(
                    id(),
                    observer,
                    next(),
                    arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")),
                    "",
                    "",
                ),
            )
            // Signed by the very service the observer named for `30382:rank`.
            trustStore.insert(userList(members = listOf(subject), author = service))

            // No cell for the member the list scored 80, and none anywhere else:
            // the ledger settles inline here, so this is read-your-writes.
            assertNull(reputations.get(subject))
            assertTrue(
                reputations
                    .get(service)
                    ?.influenceScores
                    .orEmpty()
                    .isEmpty(),
            )

            // Positive control: the SAME service, the SAME subject, the same
            // score — as a 30382 — does land the cell. So the null above is the
            // list being ignored, not the 10040 attribution being miswired here.
            trustStore.insert(
                ContactCardEvent(id(), service, next(), arrayOf(arrayOf("d", subject), arrayOf("rank", "80")), "", ""),
            )
            assertEquals(mapOf(observer to 80), assertNotNull(reputations.get(subject)).influenceScores)
        }
}
