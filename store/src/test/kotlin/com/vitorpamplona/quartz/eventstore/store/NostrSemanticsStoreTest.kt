/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store

import com.vitorpamplona.quartz.eventstore.store.mapping.addressOrNull
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.normalizeRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip59Giftwrap.wraps.GiftWrapEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The store must enforce the same Nostr semantics as Quartz's SQLite modules —
 * each test names the sqlite ...Module.kt rule it mirrors. Events are unsigned
 * fixtures: like the SQLite store, verification is the ingest path's job.
 */
open class NostrSemanticsStoreTest {
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)

    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    /** Override to run the WHOLE semantics suite against another engine (see NostrSemanticsStoreWireTest). */
    protected open fun newIndex(): EventIndex = InMemoryEventIndex()

    protected val index: EventIndex by lazy { newIndex() }
    private val store by lazy { NostrSemanticsStore(index, relay = "wss://sot.test/".normalizeRelayUrl()) }

    private fun storedDocs() = runBlocking { index.count(EventQuery()) }

    private fun note(
        author: String = alice,
        at: Long = next(),
        content: String = "hello",
        tags: Array<Array<String>> = emptyArray(),
        eventId: String = id(),
    ) = Event(eventId, author, at, 1, tags, content, "")

    private fun metadata(
        at: Long = next(),
        name: String = "alice",
        eventId: String = id(),
    ) = MetadataEvent(eventId, alice, at, emptyArray(), """{"name":"$name"}""", "")

    private fun card(
        subject: String,
        at: Long = next(),
        eventId: String = id(),
    ) = ContactCardEvent(eventId, alice, at, arrayOf(arrayOf("d", subject), arrayOf("rank", "50")), "", "")

    @Test
    fun `rawQuery matches query ids order and wire json`() =
        runBlocking {
            val tagged = note(tags = arrayOf(arrayOf("p", bob), arrayOf("e", id())), content = "for bob")
            val plain = note(author = bob)
            val older = note(at = 1, content = "oldest")
            store.insert(tagged)
            store.insert(plain)
            store.insert(older)

            val filter = Filter(kinds = listOf(1))
            val viaQuery = store.query<Event>(filter)
            val viaRaw = mutableListOf<RawEvent>()
            store.rawQuery(listOf(filter)) { viaRaw.add(it) }

            // Same recall, same newest-first order.
            assertEquals(viaQuery.map { it.id }, viaRaw.map { it.id })
            // Lossless: each raw event rebuilds to the exact wire JSON query() returns,
            // tags included — proving the stored tag string passes straight through.
            assertEquals(viaQuery.map { it.toJson() }, viaRaw.map { it.toEvent<Event>().toJson() })
        }

    @Test
    fun `stores and answers nip01 filters`() =
        runBlocking {
            val tagged = note(tags = arrayOf(arrayOf("p", bob)), content = "for bob")
            val plain = note(author = bob)
            store.insert(tagged)
            store.insert(plain)

            assertEquals(listOf(tagged.id), store.query<Event>(Filter(tags = mapOf("p" to listOf(bob)))).map { it.id })
            assertEquals(listOf(plain.id), store.query<Event>(Filter(authors = listOf(bob))).map { it.id })
            assertEquals(2, store.count(Filter(kinds = listOf(1))))
            // Newest first, and limit applies.
            assertEquals(listOf(plain.id), store.query<Event>(Filter(kinds = listOf(1), limit = 1)).map { it.id })
            // Present-but-empty list matches nothing (NIP-01).
            assertEquals(0, store.count(Filter(kinds = emptyList())))
        }

    @Test
    fun `queries return typed events`() =
        runBlocking {
            store.insert(metadata(name = "alice"))
            val back = store.query<MetadataEvent>(Filter(kinds = listOf(MetadataEvent.KIND)))
            assertEquals("alice", back.single().contactMetaData()?.name)
        }

    @Test
    fun `duplicates are rejected`() =
        runBlocking {
            val ev = note()
            val outcomes = store.batchInsert(listOf(ev, ev))
            assertEquals(IEventStore.InsertOutcome.Accepted, outcomes[0])
            assertTrue((outcomes[1] as IEventStore.InsertOutcome.Rejected).reason.startsWith("duplicate:"))
        }

    /** ReplaceableModule: newer version deletes older; older arrivals are rejected. */
    @Test
    fun `replaceables keep only the newest version`() =
        runBlocking {
            val old = metadata(at = 100, name = "v1")
            val new = metadata(at = 200, name = "v2")
            store.insert(old)
            store.insert(new)
            assertEquals(listOf(new.id), store.query<Event>(Filter(kinds = listOf(0))).map { it.id })

            val stale = metadata(at = 150, name = "late")
            val rejected = assertFailsWith<RejectedException> { store.insert(stale) }
            assertTrue(rejected.message!!.startsWith("replaced:"))
        }

    /** ReplaceableModule tie-break: equal created_at, LOWEST id wins. */
    @Test
    fun `replaceable ties keep the lowest id`() =
        runBlocking {
            val high = MetadataEvent("f".repeat(64), alice, 100, emptyArray(), "{}", "")
            val low = MetadataEvent("0".repeat(64), alice, 100, emptyArray(), "{}", "")
            store.insert(high)
            store.insert(low)
            assertEquals(listOf(low.id), store.query<Event>(Filter(kinds = listOf(0))).map { it.id })
            assertFailsWith<RejectedException> { store.insert(MetadataEvent("e".repeat(64), alice, 100, emptyArray(), "{}", "")) }
        }

    /** AddressableModule: supersession is per d-tag address. */
    @Test
    fun `addressables replace per d tag`() =
        runBlocking {
            val forBob = card(subject = bob, at = 100)
            val forBobNewer = card(subject = bob, at = 200)
            val forOther = card(subject = "c3".repeat(32), at = 150)
            store.insert(forBob)
            store.insert(forOther)
            store.insert(forBobNewer)
            assertEquals(setOf(forBobNewer.id, forOther.id), store.query<Event>(Filter(kinds = listOf(ContactCardEvent.KIND))).map { it.id }.toSet())
        }

    /** DeletionRequestModule: e-tag targets erased, tombstone kept, re-inserts blocked — same author only. */
    @Test
    fun `deletion by event id erases and blocks the target`() =
        runBlocking {
            val target = note()
            val bobs = note(author = bob, eventId = id())
            store.insert(target)
            store.insert(bobs)

            store.insert(DeletionEvent(id(), alice, next(), arrayOf(arrayOf("e", target.id), arrayOf("e", bobs.id)), "", ""))

            // Alice's target is gone; bob's event survives (NIP-09 same-author rule).
            assertEquals(setOf(bobs.id), store.query<Event>(Filter(kinds = listOf(1))).map { it.id }.toSet())
            // The tombstone blocks a re-insert.
            val rejected = assertFailsWith<RejectedException> { store.insert(target) }
            assertTrue(rejected.message!!.startsWith("blocked:"))
        }

    /** DeletionRequestModule: a-tag targets erased up to the deletion's created_at; newer versions still insert. */
    @Test
    fun `deletion by address erases versions up to its time`() =
        runBlocking {
            store.insert(card(subject = bob, at = 100))
            store.insert(DeletionEvent(id(), alice, 200, arrayOf(arrayOf("a", "${ContactCardEvent.KIND}:$alice:$bob")), "", ""))
            assertEquals(0, store.count(Filter(kinds = listOf(ContactCardEvent.KIND))))

            // Older than the deletion: blocked.
            assertFailsWith<RejectedException> { store.insert(card(subject = bob, at = 150)) }
            // Newer than the deletion: accepted.
            store.insert(card(subject = bob, at = 300))
            assertEquals(1, store.count(Filter(kinds = listOf(ContactCardEvent.KIND))))
        }

    /** SQLiteEventStore.insertEvent: ephemeral kinds are ACCEPTED but never persisted (NIP-01). */
    @Test
    fun `ephemeral kinds are accepted without storing`() =
        runBlocking {
            val outcomes = store.batchInsert(listOf(Event(id(), alice, next(), 20_001, emptyArray(), "", "")))
            assertEquals(listOf<IEventStore.InsertOutcome>(IEventStore.InsertOutcome.Accepted), outcomes)
            assertEquals(0, storedDocs())
        }

    /** ExpirationModule: expired inserts rejected; due expirations swept. */
    @Test
    fun `expiration is enforced`() =
        runBlocking {
            val realNow = System.currentTimeMillis() / 1000
            assertFailsWith<RejectedException> {
                store.insert(note(tags = arrayOf(arrayOf("expiration", "${realNow - 10}"))))
            }

            val lateStore = NostrSemanticsStore(index, nowSecs = { realNow + 100_000 })
            val expiring = note(tags = arrayOf(arrayOf("expiration", "${realNow + 50_000}")))
            val keeper = note()
            store.insert(expiring)
            store.insert(keeper)
            lateStore.deleteExpiredEvents()
            assertEquals(setOf(keeper.id), store.query<Event>(Filter(kinds = listOf(1))).map { it.id }.toSet())
        }

    /** RightToVanishModule: a covering kind 62 erases the author's history and blocks older inserts. */
    @Test
    fun `request to vanish erases and blocks the author`() =
        runBlocking {
            val hers = note(at = 100)
            val his = note(author = bob, at = 110)
            store.insert(hers)
            store.insert(his)

            store.insert(RequestToVanishEvent(id(), alice, 200, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))

            // Alice's history is gone, the request itself and bob's event survive.
            assertEquals(setOf(his.id), store.query<Event>(Filter(kinds = listOf(1))).map { it.id }.toSet())
            assertEquals(1, store.count(Filter(kinds = listOf(RequestToVanishEvent.KIND))))

            val rejected = assertFailsWith<RejectedException> { store.insert(note(at = 150)) }
            assertTrue(rejected.message!!.startsWith("blocked:"))
            // Newer than the request: accepted.
            store.insert(note(at = 300))
        }

    @Test
    fun `transaction buffers and applies in order`() =
        runBlocking {
            val a = note()
            val b = note()
            store.transaction {
                insert(a)
                insert(b)
            }
            assertEquals(2, store.count(Filter(kinds = listOf(1))))
        }

    @Test
    fun `negentropy snapshot returns id and time pairs`() =
        runBlocking {
            val ev = note(at = 42)
            store.insert(ev)
            val snapshot = store.snapshotIdsForNegentropy(listOf(Filter(kinds = listOf(1))), maxEntries = null)
            assertEquals(listOf(42L to ev.id), snapshot.map { it.createdAt to it.id })
        }

    @Test
    fun `delete by filter`() =
        runBlocking {
            store.insert(note())
            store.insert(note(author = bob))
            store.delete(Filter(authors = listOf(alice)))
            assertEquals(setOf(bob), store.query<Event>(Filter(kinds = listOf(1))).map { it.pubKey }.toSet())
        }

    /** NIP-01 standardized tags: a replaceable coordinate is "kind:pubkey:" — "(note: include the trailing colon)". */
    @Test
    fun `replaceable addresses keep the trailing colon`() {
        assertEquals("0:$alice:", metadata().addressOrNull())
        assertEquals("${ContactCardEvent.KIND}:$alice:$bob", card(subject = bob).addressOrNull())
    }

    /** FullTextSearchModule: only SearchableEvent kinds are searchable, via indexableContent(). */
    @Test
    fun `search matches searchable kinds only`() =
        runBlocking {
            store.insert(metadata(name = "satoshi"))
            // A base Event never implements SearchableEvent — its content is invisible to search.
            store.insert(note(content = "satoshi wrote this"))
            val hits = store.query<Event>(Filter(search = "satoshi"))
            assertEquals(listOf(MetadataEvent.KIND), hits.map { it.kind })
        }

    /** Multi-word search is AND: every word must be present on the same event. */
    @Test
    fun `multi-word search requires every word on the same event`() =
        runBlocking {
            store.insert(metadata(name = "Vitor Pamplona"))
            store.insert(MetadataEvent(id(), bob, next(), emptyArray(), """{"name":"Vitor"}""", ""))

            val hits = store.query<Event>(Filter(search = "vitor pamplona"))
            assertEquals(listOf(alice), hits.map { it.pubKey }, "a profile matching only one word must not recall")
            // Each word alone still recalls both — AND narrows, it doesn't drop matchers.
            assertEquals(2, store.query<Event>(Filter(search = "vitor")).size)

            // A word tokenization erases ("⚡") cannot be required — it is
            // dropped, not turned into an unsatisfiable conjunct; a query
            // that is ONLY such words matches nothing, not everything.
            assertEquals(2, store.query<Event>(Filter(search = "vitor ⚡")).size)
            assertEquals(0, store.query<Event>(Filter(search = "⚡")).size)
        }

    /** EventIndexesModule pubkey_owner_hash: a gift-wrap is OWNED by its p-tag recipient. */
    @Test
    fun `gift wraps obey their recipient not their signer`() =
        runBlocking {
            val throwaway = "c3".repeat(32)
            val wrap = GiftWrapEvent(id(), throwaway, next(), arrayOf(arrayOf("p", alice)), "sealed", "")
            store.insert(wrap)

            // The recipient's deletion erases it — even though she never signed it.
            store.insert(DeletionEvent(id(), alice, next(), arrayOf(arrayOf("e", wrap.id)), "", ""))
            assertEquals(0, store.count(Filter(kinds = listOf(GiftWrapEvent.KIND))))
            // And the tombstone blocks its return.
            assertFailsWith<RejectedException> { store.insert(wrap) }
        }

    /** RightToVanishModule uses the owner too: vanishing erases wraps addressed to the author. */
    @Test
    fun `vanish erases gift wraps addressed to the author`() =
        runBlocking {
            val wrap = GiftWrapEvent(id(), "c3".repeat(32), 100, arrayOf(arrayOf("p", alice)), "sealed", "")
            store.insert(wrap)
            store.insert(RequestToVanishEvent(id(), alice, 200, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))
            assertEquals(0, store.count(Filter(kinds = listOf(GiftWrapEvent.KIND))))
        }

    /** NIP-62: history is erased "until its created_at" — INCLUSIVE (the spec; Quartz's trigger uses strict <). */
    @Test
    fun `vanish erases events up to and including its timestamp`() =
        runBlocking {
            val atBoundary = note(at = 200)
            val after = note(at = 201)
            store.insert(atBoundary)
            store.insert(after)
            store.insert(RequestToVanishEvent(id(), alice, 200, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))
            assertEquals(setOf(after.id), store.query<Event>(Filter(kinds = listOf(1))).map { it.id }.toSet())
        }

    /** NIP-40: "Relays SHOULD NOT send expired events to clients, even if they are stored." */
    @Test
    fun `expired events are not served even before the sweep`() =
        runBlocking {
            val realNow = System.currentTimeMillis() / 1000
            val expiring = note(tags = arrayOf(arrayOf("expiration", "${realNow + 50_000}")))
            val keeper = note()
            store.insert(expiring)
            store.insert(keeper)

            // Same index, clock past the expiration, no sweep has run:
            val lateStore = NostrSemanticsStore(index, nowSecs = { realNow + 100_000 })
            assertEquals(setOf(keeper.id), lateStore.query<Event>(Filter(kinds = listOf(1))).map { it.id }.toSet())
            assertEquals(1, lateStore.count(Filter(kinds = listOf(1))))
            // The doc is still stored (the sweep hasn't run) — only serving is guarded.
            assertEquals(2, storedDocs())
        }

    /** NIP-09/NIP-62: a deletion request against a kind 5 or a kind 62 has no effect. */
    @Test
    fun `deletion requests cannot erase deletions or vanish requests`() =
        runBlocking {
            val target = note()
            store.insert(target)
            val deletion = DeletionEvent(id(), alice, next(), arrayOf(arrayOf("e", target.id)), "", "")
            store.insert(deletion)

            val vanishOther = RequestToVanishEvent(id(), bob, next(), arrayOf(arrayOf("relay", "wss://elsewhere.example.com/")), "", "")
            store.insert(vanishOther)

            store.insert(DeletionEvent(id(), alice, next(), arrayOf(arrayOf("e", deletion.id)), "", ""))
            store.insert(DeletionEvent(id(), bob, next(), arrayOf(arrayOf("e", vanishOther.id)), "", ""))

            // Both tombstones survive their own deletion requests.
            assertEquals(3, store.count(Filter(kinds = listOf(DeletionEvent.KIND))))
            assertEquals(1, store.count(Filter(kinds = listOf(RequestToVanishEvent.KIND))))
        }

    /** NIP-50: unsupported key:value extensions are ignored, not matched as text. */
    @Test
    fun `search ignores unsupported extensions`() =
        runBlocking {
            store.insert(metadata(name = "satoshi"))
            assertEquals(1, store.query<Event>(Filter(search = "satoshi language:en nsfw:false")).size)
            // An all-extensions query imposes no text constraint at all.
            assertEquals(1, store.query<Event>(Filter(kinds = listOf(0), search = "language:en")).size)
            // But a URL is NOT an extension — scheme://… stays a search term
            // (a naive key:value strip would have turned this into match-all).
            assertEquals(0, store.query<Event>(Filter(kinds = listOf(0), search = "https://unknown.example.com")).size)
        }

    /** reindexFullTextSearch: docs indexed without search_text become searchable after the rebuild. */
    @Test
    fun `reindex re-derives search text`() =
        runBlocking {
            // Simulate a doc fed under old code: correct fields, no derived search_text.
            index.put(EventDoc.fromEventJson(metadata(name = "satoshi").toJson()))
            store.insert(note())
            assertEquals(0, store.count(Filter(search = "satoshi")))

            // The resumable path pages by id cursor; batchSize 1 forces multiple rounds.
            var cursor: String? = null
            do {
                val progress = store.reindexFullTextSearch(cursor, batchSize = 1)
                cursor = progress.cursor
            } while (!progress.done)
            assertEquals(1, store.count(Filter(search = "satoshi")))
        }

    /** A present limit <= 0 is the "matches nothing" sentinel on EVERY recall path — never an exception, never a full result. */
    @Test
    fun `a non-positive limit matches nothing instead of throwing`() =
        runBlocking {
            val n = note(at = 100)
            store.insert(n)
            assertEquals(0, store.query<Event>(Filter(ids = listOf(n.id), limit = -1)).size, "pure-id fast path")
            assertEquals(0, store.query<Event>(Filter(kinds = listOf(1), limit = 0)).size, "search path")
            assertEquals(0, store.count(Filter(kinds = listOf(1), limit = -1)))
            assertEquals(0, store.count(listOf(Filter(kinds = listOf(1), limit = -1), Filter(kinds = listOf(999)))))
        }

    /**
     * NIP-45: COUNT == the REQ's feed, EXACTLY — same limits, same gates, same
     * cross-filter dedup. The number a client could verify by running the REQ.
     */
    @Test
    fun `count equals the REQ feed exactly`() =
        runBlocking {
            (1..5).forEach { store.insert(note(at = 100L + it, content = "hello world $it")) }
            val limited = Filter(kinds = listOf(1), limit = 2)
            val all = Filter(kinds = listOf(1))
            val searching = Filter(kinds = listOf(1), search = "hello")
            assertEquals(store.query<Event>(limited).size, store.count(limited), "limited")
            assertEquals(2, store.count(limited), "the feed caps at the limit, so the count does")
            assertEquals(store.query<Event>(all).size, store.count(all), "unbounded")
            assertEquals(5, store.count(all))
            assertEquals(store.query<Event>(searching).size, store.count(searching), "searching")
            // Multi-filter: the union of the SERVED pages, deduped — filters
            // overlap, so the count is the feed's size, not the sum.
            val filters = listOf(limited, all)
            assertEquals(store.query<Event>(filters).size, store.count(filters), "multi-filter dedup")
            assertEquals(5, store.count(filters))
        }
}
