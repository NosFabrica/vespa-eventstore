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
package com.nosfabrica.vespa.eventstore.mapping

import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.owner
import com.vitorpamplona.quartz.utils.EventFactory

/*
 * Event <-> EventDoc, both directions, plus the derived-field helpers: the
 * exact stored form ([toDoc]), the typed reconstruction ([toEvent]), the owner
 * (gift-wrap recipient or author). Pure, with no store state. The NIP-01
 * replaceable/addressable address an [Event] occupies is quartz's
 * `Event.addressTagOrNull()` (kind-dispatched, the fixed empty d-tag for a
 * replaceable even with a stray `d`); [EventDoc.addressOrNull] is the same
 * rule over the stored fields.
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

/**
 * Rebuild a stored [EventDoc] into its typed Quartz [Event] — the query result
 * path. Uses [EventFactory.create], Quartz's own by-kind dispatch, straight from
 * the stored fields: the obvious `Event.fromJson(toEventJson())` round-trips
 * through a JSON string per event (measured ~8x slower, ~5x more allocation).
 * The result is identical to what `fromJson` would produce (pinned by
 * `EventDocConversionTest`) — subclass constructors store only the seven NIP-01
 * fields and compute derived views lazily.
 */
internal fun EventDoc.toEvent(): Event = EventFactory.create(id, pubkey, createdAt, kind, tagsAsArray(), content, sig)

private fun EventDoc.tagsAsArray(): Array<Array<String>> = Array(tags.size) { tags[it].toTypedArray() }
