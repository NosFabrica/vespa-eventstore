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
package com.nosfabrica.vespa.eventstore.store.mapping

import com.nosfabrica.vespa.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.nip01Core.core.Address
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip59Giftwrap.wraps.GiftWrapEvent
import com.vitorpamplona.quartz.utils.EventFactory

/*
 * Event <-> EventDoc, both directions, plus the derived-field helpers: the
 * exact stored form ([toDoc]), the typed reconstruction ([toEvent]), the owner
 * (gift-wrap recipient or author), and the NIP-01 replaceable/addressable
 * address. Pure, with no store state.
 */

/**
 * The event's exact stored form plus two derived fields: [EventDoc.owner] (the
 * gift-wrap recipient or the author) and [EventDoc.search] (the kind-specific
 * decomposition from [SearchExtractors]).
 */
internal fun Event.toDoc(): EventDoc =
    EventDoc(
        id = id,
        pubkey = pubKey,
        createdAt = createdAt,
        kind = kind,
        tags = tags.map { it.toList() },
        content = content,
        sig = sig,
        owner = owner(),
        search = SearchExtractors.extract(this),
    )

/** The pubkey Nostr semantics key off: the gift-wrap recipient, else the author. */
internal fun Event.owner(): String = (this as? GiftWrapEvent)?.recipientPubKey() ?: pubKey

/**
 * The NIP-01 address for replaceable/addressable kinds; null for regular
 * events. Replaceables use the fixed empty d-tag regardless of stray d tags,
 * matching Quartz's BaseReplaceableEvent.FIXED_D_TAG.
 */
internal fun Event.addressOrNull(): String? =
    when {
        kind.isReplaceable() -> Address.assemble(kind, pubKey)
        kind.isAddressable() -> Address.assemble(kind, pubKey, tags.dTag())
        else -> null
    }

/**
 * Rebuild a stored [EventDoc] into its typed Quartz [Event] — the query result path.
 *
 * The obvious `Event.fromJson(toEventJson())` reconstructs by SERIALIZING the doc
 * to a JSON string and PARSING it back, once per returned event — and on a hot
 * query path that string + parse is the single biggest source of garbage
 * (measured ~8x slower and ~5x more allocation per event than building it
 * directly).
 *
 * [EventFactory.create] is Quartz's OWN by-kind dispatch — the same registry
 * `fromJson` uses to pick the subclass (kind 1 -> TextNoteEvent, 0 ->
 * MetadataEvent, …) — but invoked straight from the stored fields, with no JSON
 * in the middle. It covers every known kind and returns a base [Event] for the
 * rest, so this is both faster AND complete: no hand-maintained kind table, and
 * the result is identical to what `fromJson` would have produced (pinned by
 * `EventDocConversionTest`). Those subclass constructors only store the seven
 * NIP-01 fields; every derived view is computed lazily, so a directly-built
 * instance is indistinguishable from a parsed one.
 */
internal fun EventDoc.toEvent(): Event = EventFactory.create(id, pubkey, createdAt, kind, tagsAsArray(), content, sig)

private fun EventDoc.tagsAsArray(): Array<Array<String>> = Array(tags.size) { tags[it].toTypedArray() }
