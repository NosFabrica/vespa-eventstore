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

import com.nosfabrica.vespa.eventstore.mapping.SearchIntent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/**
 * WHAT A FILTER ASKS THIS STORE FOR, read before the read — for a consumer
 * whose policy depends on it: a relay that refuses an anonymous read with no
 * lens, a ranked-read throttle, a status line.
 *
 * Derived by the SAME code the store compiles the filter with (the mapping's
 * one reading of the NIP-50 extensions), so the answer here and the read the
 * store then serves cannot disagree. They did when the consumer parsed the
 * search string itself: vespa-relay counted any `sort:` as a ranked read,
 * while the store ignores a `sort:` value it does not recognise and serves
 * plain recall. `ReadShapeTest` holds the two to the same answers on
 * recognised, unknown and malformed extensions.
 *
 * Only the search string decides any of this; a filter that can never match
 * (an empty `ids` list) still has a shape, since a policy refusing a read
 * should not depend on whether it would have found anything.
 */
class ReadShape private constructor(
    /**
     * The lens the filter names itself, `observer:<64-hex>` lowercased; null when
     * absent or unusable (an `npub`, short hex), which the store ignores rather
     * than decodes. A connection's NIP-42 key is the OTHER source of a lens, and
     * not the filter's to declare.
     */
    val observer: String?,
    /** `include:spam`: the read asks for the corpus with the default trust floor waived. */
    val includeSpam: Boolean,
    /**
     * Ranked by relevance: the filter carries text (terms or a quoted phrase) or
     * a `sort:` this store recognises (`rank`, `rank:asc`, `followers`, `text`,
     * `recent`). An exclusion alone (`-word`) or an unknown `sort:` is not.
     */
    val ranked: Boolean,
) {
    /**
     * Whether the filter settles its own lens question: it names an [observer],
     * or it waives the floor with [includeSpam]. A read where neither holds is
     * lensed only if the connection supplies a key.
     */
    val declaresLens: Boolean get() = observer != null || includeSpam

    override fun equals(other: Any?): Boolean = other is ReadShape && other.observer == observer && other.includeSpam == includeSpam && other.ranked == ranked

    override fun hashCode(): Int = (observer.hashCode() * 31 + includeSpam.hashCode()) * 31 + ranked.hashCode()

    override fun toString(): String = "ReadShape(observer=${observer?.take(16)}, includeSpam=$includeSpam, ranked=$ranked)"

    companion object {
        /** [filter]'s shape, as the store will read it. */
        fun of(filter: Filter): ReadShape = of(filter.search)

        /** The shape of a NIP-50 [search] string alone; null or blank is a plain NIP-01 read. */
        fun of(search: String?): ReadShape = SearchIntent.of(search).let { ReadShape(it.observer, it.includeSpam, it.ranked) }
    }
}
