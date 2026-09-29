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
package com.nosfabrica.vespa.eventstore.engine.observe

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The decorator on its own, against the in-memory reference: what it reports
 * for each write member, and what it must NOT do — report a write the engine
 * refused, or let an observer's failure into the write path. The whole store
 * driving it through every removal style is `IndexObserversOverStoreTest`'s.
 */
class ObservedEventIndexTest {
    /** Everything an observer was told, in order — `+id` a put, `-id` a remove. */
    private class Recorder : IndexObserver {
        val log = ArrayList<String>()
        val events = ArrayList<Event>()

        override fun onPut(events: List<Event>) {
            this.events += events
            events.forEach { log += "+${it.id}" }
        }

        override fun onRemove(ids: List<String>) {
            ids.forEach { log += "-$it" }
        }
    }

    private val alice = "a1".repeat(32)

    private fun doc(
        n: Int,
        kind: Int = 1,
        at: Long = 100L + n,
        tags: List<List<String>> = emptyList(),
    ) = EventDoc(
        id = n.toString(16).padStart(64, '0'),
        pubkey = alice,
        createdAt = at,
        kind = kind,
        tags = tags,
        content = "note $n",
        sig = "5".repeat(128),
    )

    @Test
    fun `no observers installs nothing`() {
        assertNull(ObservedEventIndex.of(InMemoryEventIndex(), emptyList()), "an empty observer list must leave the stack untouched")
    }

    @Test
    fun `every write member is reported after it lands`() =
        runBlocking {
            val rec = Recorder()
            val inner = InMemoryEventIndex()
            val index = ObservedEventIndex(inner, listOf(rec))

            index.put(doc(1))
            index.putAll(listOf(doc(2), doc(3)))
            index.remove(doc(1).id)
            index.removeAll(listOf(doc(2).id))
            index.removeDocs(listOf(doc(3)))
            index.putAll(emptyList())
            index.removeAll(emptyList())

            assertEquals(
                listOf("+${doc(1).id}", "+${doc(2).id}", "+${doc(3).id}", "-${doc(1).id}", "-${doc(2).id}", "-${doc(3).id}"),
                rec.log,
                "each write reported once, in order; an empty batch reports nothing",
            )
            assertEquals(0, inner.count(EventQuery()), "and the writes really reached the inner index")
        }

    /** The seven NIP-01 fields, exactly — a mirror keys on id and re-serves the rest. */
    @Test
    fun `a put is reported as the stored event, tags included`() =
        runBlocking {
            val rec = Recorder()
            val stored = doc(7, tags = listOf(listOf("e", "f".repeat(64), "wss://relay.example.com", "root"), listOf("t", "nostr")))
            ObservedEventIndex(InMemoryEventIndex(), listOf(rec)).put(stored)

            val told = rec.events.single()
            assertEquals(stored.toEventJson(), told.toJson())
        }

    /**
     * READ-THEN-SUPERSEDE, OBSERVED. Over an index that supersedes by reading,
     * the replaced version must leave THROUGH the decorator — a mirror told only
     * of the winner would keep both versions until it applied NIP-01 itself.
     */
    @Test
    fun `supersession over a reading index reports the replaced version and the winner`() =
        runBlocking {
            val rec = Recorder()
            val index = ObservedEventIndex(InMemoryEventIndex(), listOf(rec))
            val old = doc(1, kind = 0, at = 100)
            val new = doc(2, kind = 0, at = 200)

            assertTrue(index.putIfNewer(old))
            assertTrue(index.putIfNewer(new))
            assertFalse(index.putIfNewer(doc(3, kind = 0, at = 150)), "a stale version is refused")

            assertEquals(listOf("+${old.id}", "-${old.id}", "+${new.id}"), rec.log, "a refused version must not be reported")
            assertEquals(listOf(new.id), index.search(EventQuery(kinds = listOf(0))).map { it.id })
        }

    /**
     * ENGINE-ATOMIC supersession (address-keyed Vespa; the reference's test
     * hook stands in): the replaced version never leaves through the port, so
     * only the winner is reported — the documented gap the consumer closes by
     * applying the NIP-01 rule itself.
     */
    @Test
    fun `supersession over an atomic engine reports the winner only`() =
        runBlocking {
            val rec = Recorder()
            val index = ObservedEventIndex(InMemoryEventIndex(supersedesViaPut = true), listOf(rec))
            val old = doc(1, kind = 0, at = 100)
            val new = doc(2, kind = 0, at = 200)

            assertTrue(index.supersedesViaPut, "the flag must forward, or the bulk path changes strategy under observation")
            index.putIfNewer(old)
            index.putIfNewer(new)
            assertFalse(index.putIfNewer(doc(3, kind = 0, at = 150)))

            assertEquals(listOf("+${old.id}", "+${new.id}"), rec.log)
        }

    @Test
    fun `a failed write reports nothing`() =
        runBlocking {
            val rec = Recorder()
            val failing =
                object : EventIndex by InMemoryEventIndex() {
                    override suspend fun put(doc: EventDoc) = error("engine refused")
                }
            val index = ObservedEventIndex(failing, listOf(rec))

            assertFailsWith<IllegalStateException> { index.put(doc(1)) }
            assertTrue(rec.log.isEmpty(), "a mirror must never hold an event the engine refused")
        }

    @Test
    fun `a throwing observer is counted and neither fails the write nor silences the others`() =
        runBlocking {
            val broken =
                object : IndexObserver {
                    override fun onPut(events: List<Event>) = error("mirror down")

                    override fun onRemove(ids: List<String>) = throw IllegalArgumentException("mirror down")
                }
            val rec = Recorder()
            val inner = InMemoryEventIndex()
            val index = ObservedEventIndex(inner, listOf(broken, rec))

            index.put(doc(1))
            index.remove(doc(1).id)
            index.put(doc(2))

            assertEquals(listOf("+${doc(1).id}", "-${doc(1).id}", "+${doc(2).id}"), rec.log, "the healthy observer after the broken one still hears everything")
            assertEquals(3L, index.observerFailures())
            assertEquals(listOf(doc(2).id), inner.search(EventQuery()).map { it.id }, "every write landed regardless")
        }

    @Test
    fun `a write that throws is reported as uncertain, never as done`() =
        runBlocking {
            val uncertain = ArrayList<String>()
            val rec =
                object : IndexObserver {
                    val done = ArrayList<String>()

                    override fun onPut(events: List<Event>) {
                        events.forEach { done += "+${it.id}" }
                    }

                    override fun onRemove(ids: List<String>) {
                        ids.forEach { done += "-$it" }
                    }

                    override fun onUncertain(
                        events: List<Event>,
                        ids: List<String>,
                    ) {
                        events.forEach { uncertain += "+${it.id}" }
                        ids.forEach { uncertain += "-$it" }
                    }
                }
            // A bulk put that lands its first doc, then fails: which part landed is unknown.
            val halfway =
                object : EventIndex by InMemoryEventIndex() {
                    override suspend fun putAll(docs: List<EventDoc>) {
                        put(docs.first())
                        error("timed out after the first chunk")
                    }

                    override suspend fun removeAll(ids: List<String>): Unit = error("timed out")
                }
            val index = ObservedEventIndex(halfway, listOf(rec))
            assertFailsWith<IllegalStateException> { index.putAll(listOf(doc(1), doc(2))) }
            assertFailsWith<IllegalStateException> { index.removeAll(listOf(doc(3).id)) }
            assertEquals(listOf("+${doc(1).id}", "+${doc(2).id}", "-${doc(3).id}"), uncertain)
            assertTrue(rec.done.isEmpty(), "nothing reported as done")
        }

    @Test
    fun `puts under ObserverSilence are not reported, removals still are`() =
        runBlocking {
            val rec = Recorder()
            val index = ObservedEventIndex(InMemoryEventIndex(), listOf(rec))
            withContext(ObserverSilence) {
                index.putAll(listOf(doc(1)))
                index.remove(doc(1).id)
            }
            assertEquals(listOf("-${doc(1).id}"), rec.log)
        }
}
