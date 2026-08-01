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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * Carries the ORIGINAL request filters past Quartz's extension stripping.
 *
 * Quartz's `LiveEventStore` runs `strippingSearchExtensions` on every REQ's
 * filters before they reach the store. That is the right default for stores
 * that would otherwise match `key:value` tokens as text, but this store HONORS
 * the NIP-50 `sort:`/`filter:rank:`/`include:spam` extensions and must see them.
 * The relay backend stashes the pre-strip filters here. The store then restores
 * each filter's `search` string by position — it is the same list in the same
 * order, only the search field differs — before mapping to the engine query.
 */
class OriginalFilters(
    val filters: List<Filter>,
) : AbstractCoroutineContextElement(OriginalFilters) {
    companion object Key : CoroutineContext.Key<OriginalFilters>
}
